//! Caller-driven gRPC server for unary and server-streaming methods.
//!
//! [`GrpcServer`] runs on a Flux [`Network`](crate::Network) group: forward its
//! events, then call [`GrpcServer::drive`] each iteration. The route callback
//! sees each call once, when its single request message arrives, and returns
//! the [`Response`]: one message, a status, or an open stream. Streams are sent
//! to later through their [`Stream`] handle; how they are grouped is up to the
//! application.
//!
//! Messages are serialized protobuf bytes; the server adds gRPC framing. For
//! fan-out, encode once into a buffer you reuse and pass it to every
//! [`GrpcServer::send`].
//!
//! Requests are uncompressed protobuf over HTTP/2, with plaintext or TLS (h2
//! ALPN). Metadata stays in HTTP wire form; `-bin` values remain base64.
//! Client-streaming, bidirectional calls, compression and `grpc-timeout`
//! enforcement are not supported.
//!
//! Wire format: <https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md>.

mod server;
#[cfg(test)]
mod tests;
use std::{borrow::Cow, fmt, net::SocketAddr};

use bytes::BytesMut;
use flux_timing::{Duration, IngestionTime};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
pub use server::{Closed, GrpcConfig, GrpcServer, Route};

use crate::http2::{Fields, RequestHead};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Code {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    pub code: Code,
    pub message: Cow<'static, str>,
}

impl Status {
    pub fn new(code: Code, message: impl Into<Cow<'static, str>>) -> Self {
        Self { code, message: message.into() }
    }

    pub const fn ok() -> Self {
        Self { code: Code::Ok, message: Cow::Borrowed("") }
    }

    /// Adds the final HEADERS fields to `fields`. A fixed message that needs
    /// no escaping is used in place; others are percent-encoded into
    /// `scratch`, whose capacity is reused once `fields` is cleared.
    fn write_trailers(&self, fields: &mut HeaderMap, scratch: &mut BytesMut) {
        const CODES: [&str; 17] = [
            "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
            "16",
        ];
        let status = HeaderValue::from_static(CODES[self.code as usize]);
        fields.insert(HeaderName::from_static("grpc-status"), status);
        if self.message.is_empty() {
            return;
        }
        let plain = |byte: u8| (0x21..=0x7e).contains(&byte) && byte != b'%';
        let mut message = match &self.message {
            Cow::Borrowed(text) if text.bytes().all(plain) => HeaderValue::from_static(text),
            _ => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                scratch.clear();
                for byte in self.message.bytes() {
                    if plain(byte) {
                        scratch.extend_from_slice(&[byte]);
                    } else {
                        scratch.extend_from_slice(&[
                            b'%',
                            HEX[(byte >> 4) as usize],
                            HEX[(byte & 15) as usize],
                        ]);
                    }
                }
                HeaderValue::from_maybe_shared(scratch.split().freeze())
                    .expect("percent-encoding leaves visible ASCII")
            }
        };
        // Never indexed: HPACK would otherwise retain the scratch bytes, and
        // messages rarely repeat.
        message.set_sensitive(true);
        fields.insert(HeaderName::from_static("grpc-message"), message);
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "gRPC {:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for Status {}

/// One server-streaming call. Stays valid until its call ends; sends then
/// return [`Closed`]. Never reused for another call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Stream {
    token: mio::Token,
    id: u32,
}

/// Path and metadata of one call, copied out of the header block into storage
/// the connection reuses across calls.
#[derive(Debug)]
struct Metadata {
    /// The path, then each field's name and value, back to back.
    bytes: Vec<u8>,
    path_len: usize,
    /// Where each field's name and value end in `bytes`.
    fields: Vec<(u32, u32)>,
    peer: SocketAddr,
    tls: bool,
    stream: Stream,
    received_at: IngestionTime,
    receive_duration: Duration,
}

impl Metadata {
    /// Empty storage; `fill` sets every field before routing.
    fn new(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
            path_len: 0,
            fields: Vec::new(),
            peer: SocketAddr::from(([0, 0, 0, 0], 0)),
            tls: false,
            stream: Stream { token: mio::Token(0), id: 0 },
            received_at: IngestionTime::default(),
            receive_duration: Duration(0),
        }
    }

    /// Refills this storage for a new call, keeping its capacity.
    fn fill(
        &mut self,
        path: &[u8],
        fields: Fields<'_>,
        peer: SocketAddr,
        tls: bool,
        stream: Stream,
        received_at: IngestionTime,
    ) {
        self.bytes.clear();
        self.fields.clear();
        self.bytes.extend_from_slice(path);
        self.path_len = path.len();
        for (name, value) in fields.iter() {
            self.bytes.extend_from_slice(name.as_bytes());
            let name_end = self.bytes.len() as u32;
            self.bytes.extend_from_slice(value);
            self.fields.push((name_end, self.bytes.len() as u32));
        }
        self.peer = peer;
        self.tls = tls;
        self.stream = stream;
        self.received_at = received_at;
        self.receive_duration = Duration(0);
    }

    fn raw(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let mut start = self.path_len;
        self.fields.iter().map(move |&(name_end, value_end)| {
            let (name_end, value_end) = (name_end as usize, value_end as usize);
            let field = (&self.bytes[start..name_end], &self.bytes[name_end..value_end]);
            start = value_end;
            field
        })
    }
}

/// One call and its single request message, passed to the route callback and
/// borrowed for its duration. Use [`Self::to_header_map`] to keep the metadata.
#[derive(Clone, Copy, Debug)]
pub struct Request<'a> {
    metadata: &'a Metadata,
    message: &'a [u8],
}

impl<'a> Request<'a> {
    /// Case-sensitive `/package.Service/Method`.
    pub fn path(&self) -> &'a [u8] {
        &self.metadata.bytes[..self.metadata.path_len]
    }

    /// The serialized request message.
    pub const fn message(&self) -> &'a [u8] {
        self.message
    }

    /// The first value of a lowercase metadata `name`.
    pub fn header(&self, name: &str) -> Option<&'a [u8]> {
        self.metadata.raw().find(|(n, _)| *n == name.as_bytes()).map(|(_, value)| value)
    }

    /// Metadata in wire order and HTTP wire form, including gRPC control
    /// headers; `-bin` values remain base64.
    pub fn headers(&self) -> impl Iterator<Item = (&'a str, &'a [u8])> + 'a {
        // Names come from validated `HeaderName`s.
        self.metadata
            .raw()
            .map(|(name, value)| (std::str::from_utf8(name).unwrap_or_default(), value))
    }

    pub fn to_header_map(&self) -> HeaderMap {
        let mut map = HeaderMap::with_capacity(self.metadata.fields.len());
        for (name, value) in self.metadata.raw() {
            if let (Ok(name), Ok(value)) =
                (HeaderName::from_bytes(name), HeaderValue::from_bytes(value))
            {
                map.append(name, value);
            }
        }
        map
    }

    pub const fn peer(&self) -> SocketAddr {
        self.metadata.peer
    }

    /// When the read holding the first byte of the call's HEADERS frame was
    /// received. Latency measured from here includes the time the rest of the
    /// call took to arrive.
    pub const fn received_at(&self) -> IngestionTime {
        self.metadata.received_at
    }

    /// From [`Self::received_at`] to the read that completed the request
    /// message: the rest of the call arriving, plus any time the poll loop took
    /// between those reads.
    pub const fn receive_duration(&self) -> Duration {
        self.metadata.receive_duration
    }

    /// Whether the call arrived on a TLS listener.
    pub const fn tls(&self) -> bool {
        self.metadata.tls
    }

    /// The handle used to send to this call if the route returns
    /// [`Response::Stream`]. For other responses it is already closed.
    pub const fn stream(&self) -> Stream {
        self.metadata.stream
    }
}

pub enum Response {
    /// The reply payload the route serialized, followed by `OK`.
    Message,
    /// Finish without a message; the reply payload is discarded.
    Status(Status),
    /// Keep the call open for [`GrpcServer::send`] and [`GrpcServer::finish`].
    Stream,
}

impl From<Status> for Response {
    fn from(status: Status) -> Self {
        Self::Status(status)
    }
}

/// A call that is refused before reaching the route callback.
struct Rejection {
    http_status: StatusCode,
    status: Status,
}

impl From<Status> for Rejection {
    fn from(status: Status) -> Self {
        Self { http_status: StatusCode::OK, status }
    }
}

/// Validates an uncompressed protobuf call. Unsupported media types produce
/// HTTP 415.
fn parse_request(head: &RequestHead<'_>) -> Result<(), Rejection> {
    let content_type = single_header(head.fields, "content-type")?;
    let media_type = content_type.and_then(|value| value.split(|&c| c == b';').next());
    if !matches!(media_type, Some(b"application/grpc" | b"application/grpc+proto")) {
        return Err(Rejection {
            http_status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            status: Status::new(Code::Unimplemented, "unsupported gRPC content type"),
        });
    }
    if *head.method != Method::POST {
        return Err(Rejection {
            http_status: StatusCode::METHOD_NOT_ALLOWED,
            status: Status::new(Code::Unimplemented, "gRPC requires POST"),
        });
    }
    if !matches!(head.scheme, Some(b"http" | b"https")) {
        return Err(Status::new(Code::InvalidArgument, "invalid gRPC scheme").into());
    }
    let path = head.path.ok_or_else(|| Status::new(Code::Unimplemented, "missing RPC path"))?;
    let valid_path = path
        .strip_prefix(b"/")
        .and_then(|path| {
            let slash = path.iter().position(|&c| c == b'/')?;
            Some(
                slash != 0 &&
                    slash + 1 < path.len() &&
                    !path[slash + 1..].contains(&b'/') &&
                    !path.contains(&b'?'),
            )
        })
        .unwrap_or(false);
    if !valid_path {
        return Err(Status::new(Code::Unimplemented, "invalid RPC path").into());
    }
    if !single_header(head.fields, "te")?
        .is_some_and(|value| value.eq_ignore_ascii_case(b"trailers"))
    {
        return Err(Status::new(Code::InvalidArgument, "gRPC requires te: trailers").into());
    }
    if single_header(head.fields, "grpc-encoding")?.is_some_and(|value| value != b"identity") {
        return Err(Status::new(Code::Unimplemented, "gRPC compression is not supported").into());
    }
    Ok(())
}

fn response_headers() -> HeaderMap {
    let mut fields = HeaderMap::new();
    add_response_headers(&mut fields);
    fields
}

fn add_response_headers(fields: &mut HeaderMap) {
    fields.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/grpc"));
    fields.insert(
        HeaderName::from_static("grpc-accept-encoding"),
        HeaderValue::from_static("identity"),
    );
}

fn single_header<'a>(fields: Fields<'a>, name: &str) -> Result<Option<&'a [u8]>, Status> {
    let mut values = fields.get_all(name);
    let value = values.next();
    if values.next().is_some() {
        return Err(Status::new(Code::InvalidArgument, "duplicate gRPC control header"));
    }
    Ok(value)
}

/// A bounded, uncompressed request message stream for one HTTP/2 stream.
///
/// Complete payloads borrow input; fragmented payloads use reusable storage.
struct MessageDecoder {
    limit: usize,
    prefix: [u8; 5],
    prefix_len: usize,
    message: Vec<u8>,
}

impl MessageDecoder {
    const fn new(max_message_size: usize) -> Self {
        Self { limit: max_message_size, prefix: [0; 5], prefix_len: 0, message: Vec::new() }
    }

    /// Ready for a new stream, keeping reassembly capacity.
    fn reset(&mut self) {
        self.prefix_len = 0;
        self.message.clear();
    }

    fn receive(
        &mut self,
        mut input: &[u8],
        end_stream: bool,
        mut on_message: impl FnMut(&[u8]),
    ) -> Result<(), Status> {
        while !input.is_empty() {
            let n = (5 - self.prefix_len).min(input.len());
            self.prefix[self.prefix_len..self.prefix_len + n].copy_from_slice(&input[..n]);
            self.prefix_len += n;
            input = &input[n..];
            if self.prefix_len != 5 {
                break;
            }
            match self.prefix[0] {
                0 => {}
                1 => return Err(Status::new(Code::Unimplemented, "compressed gRPC message")),
                _ => return Err(Status::new(Code::Internal, "invalid gRPC compression flag")),
            }
            let length = u32::from_be_bytes(self.prefix[1..].try_into().unwrap()) as usize;
            if length > self.limit {
                return Err(Status::new(Code::ResourceExhausted, "gRPC message exceeds size limit"));
            }
            if self.message.is_empty() && input.len() >= length {
                on_message(&input[..length]);
                input = &input[length..];
            } else {
                let n = (length - self.message.len()).min(input.len());
                self.message.extend_from_slice(&input[..n]);
                input = &input[n..];
                if self.message.len() != length {
                    break;
                }
                on_message(&self.message);
                self.message.clear();
            }
            self.prefix_len = 0;
        }
        if end_stream && (self.prefix_len != 0 || !self.message.is_empty()) {
            return Err(Status::new(Code::Internal, "truncated gRPC message"));
        }
        Ok(())
    }
}
