use std::{collections::VecDeque, fmt, io, net::SocketAddr};

use bytes::BytesMut;
use flux_timing::{IngestionTime, Instant};
use http::{HeaderMap, StatusCode};
use mio::Token;
use rustc_hash::FxHashMap;

use super::{
    Code, MessageDecoder, Metadata, Request, Response, Status, Stream, add_response_headers,
    parse_request, response_headers,
};
use crate::{
    Framing, Group, NetworkCore, NetworkEvent, PayloadBuf, ReplayPolicy, TcpGroupConfig,
    http2::{self, FRAME_HEADER_LEN, SendError},
};

/// Output waits in HTTP/2 while TCP holds this many unsent bytes, so slow
/// peers are bounded by the HTTP/2 send buffer and per-call buffers.
const TCP_BACKLOG: usize = 64 * 1024;
/// An incomplete frame is retained between reads; anything longer than the
/// largest frame we advertise is a protocol error.
const MAX_PARTIAL_INPUT: usize = http2::DEFAULT_MAX_FRAME_SIZE as usize + FRAME_HEADER_LEN;
/// `RST_STREAM` code for a call the server cannot complete.
const INTERNAL_ERROR: u32 = 2;

/// The route callback: the request with its message, and the reply payload
/// to serialize a [`Response::Message`] into. Framing stays out of reach.
pub trait Route: FnMut(&Request<'_>, &mut PayloadBuf<'_>) -> Response {}
impl<F: FnMut(&Request<'_>, &mut PayloadBuf<'_>) -> Response> Route for F {}

#[derive(Clone, Copy, Debug)]
pub struct GrpcConfig {
    pub http2: http2::Http2Config,
    /// Largest accepted request message.
    pub max_message_size: usize,
    /// Bytes a stream may hold beyond what HTTP/2 flow control and the send
    /// buffer accept. A stream past this limit lags: its unsent messages are
    /// dropped and it finishes with `RESOURCE_EXHAUSTED`.
    pub max_queued_bytes: usize,
    /// Calls the server preallocates and reuses across all connections. More
    /// calls in flight at once are allocated and freed as they come and go.
    pub pooled_calls: usize,
    /// Capacity of a pooled call's reply and metadata buffers. Buffers that
    /// grew for a larger message shrink back to it when the call is reused.
    pub pooled_buffer: usize,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            http2: http2::Http2Config::default(),
            max_message_size: 4 * 1024 * 1024,
            max_queued_bytes: 4 * 1024 * 1024,
            pooled_calls: 128,
            pooled_buffer: 4 * 1024,
        }
    }
}

/// Why a stream has ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Closed {
    /// Finished, reset, or disconnected.
    Ended,
    /// Held more than `max_queued_bytes` unsent: its messages were dropped
    /// and it finished with `RESOURCE_EXHAUSTED`.
    Lagged,
}

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ended => "gRPC stream is closed",
            Self::Lagged => "gRPC stream lagged",
        })
    }
}

impl std::error::Error for Closed {}

fn prefix(length: usize) -> [u8; 5] {
    let length = u32::try_from(length).expect("gRPC messages are below 4 GiB");
    let mut prefix = [0; 5];
    prefix[1..].copy_from_slice(&length.to_be_bytes());
    prefix
}

#[allow(clippy::struct_excessive_bools)]
struct Call {
    metadata: Metadata,
    /// Whether the request message reached the route callback. Rejected calls
    /// finish before routing.
    routed: bool,
    decoder: MessageDecoder,
    credit: u32,
    input_closed: bool,
    streaming: bool,
    headers_sent: bool,
    /// Framed messages HTTP/2 has not taken yet; capacity is reused.
    out: Vec<u8>,
    /// Bytes of `out` already in HTTP/2.
    offset: usize,
    /// End of each message in `out`, to drop only whole unsent messages.
    ends: VecDeque<usize>,
    trailers: Option<Trailers>,
    /// Fields of a non-`OK` final status, reused across calls.
    trailer_fields: HeaderMap,
    /// Percent-encoded status messages, reused likewise.
    scratch: BytesMut,
    pending: bool,
}

enum Trailers {
    /// `grpc-status: 0`, sent from the server's prebuilt fields.
    Ok,
    /// Any other status, in `Call::trailer_fields`, with this HTTP status if
    /// sent trailers-only.
    Custom(StatusCode),
}

/// Response fields built once per server and sent by reference.
struct ResponseFields {
    headers: HeaderMap,
    ok: HeaderMap,
    /// `headers` and `ok` together, for an `OK` with no message.
    ok_only: HeaderMap,
}

impl ResponseFields {
    fn new() -> Self {
        let headers = response_headers();
        let mut ok = HeaderMap::new();
        Status::ok().write_trailers(&mut ok, &mut BytesMut::new());
        let mut ok_only = ok.clone();
        ok_only.extend(headers.clone());
        Self { headers, ok, ok_only }
    }
}

impl Call {
    fn new(limit: usize, metadata: Metadata, buffer: usize) -> Self {
        Self {
            metadata,
            routed: false,
            decoder: MessageDecoder::new(limit),
            credit: 0,
            input_closed: false,
            streaming: false,
            headers_sent: false,
            out: Vec::with_capacity(buffer),
            offset: 0,
            ends: VecDeque::new(),
            trailers: None,
            trailer_fields: HeaderMap::new(),
            scratch: BytesMut::new(),
            pending: false,
        }
    }

    /// Clears a finished call for reuse, keeping its buffers.
    fn reset(&mut self) {
        self.routed = false;
        self.decoder.reset();
        self.credit = 0;
        self.streaming = false;
        self.headers_sent = false;
        self.out.clear();
        self.offset = 0;
        self.ends.clear();
        self.trailers = None;
        self.trailer_fields.clear();
        self.pending = false;
    }

    /// Returns buffers that grew for a large message to `capacity`.
    fn shrink(&mut self, capacity: usize) {
        self.out.shrink_to(capacity);
        // Refilled by the next call.
        self.metadata.bytes.clear();
        self.metadata.bytes.shrink_to(capacity);
        // Grown for requests split across frames.
        self.decoder.message.shrink_to(capacity);
        // Grown only by lagging streams.
        self.ends.shrink_to(0);
        if self.scratch.capacity() > capacity {
            self.scratch = BytesMut::new();
        }
    }

    const fn is_open(&self) -> bool {
        self.streaming && self.trailers.is_none()
    }

    fn finish(&mut self, status: &Status) {
        self.finish_with(StatusCode::OK, status);
    }

    fn finish_with(&mut self, http_status: StatusCode, status: &Status) {
        if self.trailers.is_some() {
            return;
        }
        if status.code == Code::Ok && status.message.is_empty() && http_status == StatusCode::OK {
            self.trailers = Some(Trailers::Ok);
        } else {
            self.trailer_fields.clear();
            status.write_trailers(&mut self.trailer_fields, &mut self.scratch);
            self.trailers = Some(Trailers::Custom(http_status));
        }
    }

    fn push(&mut self, prefix: &[u8], payload: &[u8]) {
        self.out.extend_from_slice(prefix);
        self.out.extend_from_slice(payload);
        self.ends.push_back(self.out.len());
    }

    /// Drops unsent messages, keeping the rest of one partly written.
    fn drop_unsent(&mut self) {
        let mut start = 0;
        let keep = self
            .ends
            .iter()
            .find_map(|&end| {
                let partial = start < self.offset && self.offset < end;
                start = end;
                (end > self.offset).then_some(if partial { end } else { self.offset })
            })
            .unwrap_or(self.offset);
        self.out.truncate(keep);
        self.ends.retain(|&end| end <= keep);
    }

    fn receive(&mut self, data: &[u8], end: bool, read: IngestionTime, route: &mut impl Route) {
        self.input_closed |= end;
        if self.trailers.is_some() {
            return;
        }
        let Self { metadata, routed, decoder, out, .. } = self;
        let mut response = None;
        let mut extra = false;
        let result = decoder.receive(data, end, |message| {
            if *routed {
                extra = true;
                return;
            }
            *routed = true;
            metadata.receive_duration =
                read.internal().saturating_sub(metadata.received_at.internal());
            out.clear();
            out.extend_from_slice(&[0; 5]);
            let request = Request { metadata, message };
            response = Some(route(&request, &mut PayloadBuf::new(out)));
        });
        match response {
            Some(Response::Message) => {
                let length = self.out.len() - 5;
                // No `ends`: only streams lag.
                self.out[..5].copy_from_slice(&prefix(length));
                self.finish(&Status::ok());
            }
            Some(Response::Status(status)) => {
                self.out.clear();
                self.finish(&status);
            }
            Some(Response::Stream) => {
                self.out.clear();
                self.streaming = true;
            }
            None => {}
        }
        if let Err(status) = result {
            self.finish(&status);
        } else if extra {
            self.finish(&Status::new(Code::Internal, "too many request messages"));
        } else if end && !self.routed {
            self.finish(&Status::new(Code::Internal, "missing request message"));
        }
    }

    /// Moves as much as HTTP/2 accepts. `Ok(true)` once the call is done.
    fn drive(
        &mut self,
        id: u32,
        h2: &mut http2::ServerConnection,
        fields: &ResponseFields,
    ) -> Result<bool, SendError> {
        if self.credit != 0 {
            h2.release_capacity(id, self.credit)?;
            self.credit = 0;
        }
        let mut done = false;
        if !self.headers_sent {
            if self.out.is_empty() && !self.streaming {
                match self.trailers {
                    None => return Ok(false),
                    Some(Trailers::Ok) => {
                        h2.send_headers(id, StatusCode::OK, &fields.ok_only, true)?;
                    }
                    Some(Trailers::Custom(status)) => {
                        add_response_headers(&mut self.trailer_fields);
                        h2.send_headers(id, status, &self.trailer_fields, true)?;
                    }
                }
                done = true;
            } else {
                h2.send_headers(id, StatusCode::OK, &fields.headers, false)?;
            }
            self.headers_sent = true;
        }
        while self.offset < self.out.len() {
            self.offset += h2.send_data(id, &self.out[self.offset..], false)?;
        }
        self.out.clear();
        self.ends.clear();
        self.offset = 0;
        if !done && let Some(trailers) = &self.trailers {
            h2.send_trailers(id, match trailers {
                Trailers::Ok => &fields.ok,
                Trailers::Custom(_) => &self.trailer_fields,
            })?;
            done = true;
        }
        if done && !self.input_closed {
            // The response precedes NO_ERROR, so clients keep its status.
            let _ = h2.reset(id, 0);
        }
        Ok(done)
    }
}

struct Connection {
    h2: http2::ServerConnection,
    /// A frame left incomplete by the previous read.
    input: Vec<u8>,
    /// When the read holding the first byte of `input` was received.
    input_received_at: IngestionTime,
    peer: SocketAddr,
    tls: bool,
    calls: FxHashMap<u32, Call>,
    /// Calls with work for HTTP/2, each listed once.
    pending: Vec<u32>,
    closing: bool,
    /// Listed in `GrpcServer::dirty`.
    dirty: bool,
}

/// Lists `token` for the next `drive`, once.
fn touch(token: Token, connection: &mut Connection, dirty: &mut Vec<Token>) {
    if !connection.dirty {
        connection.dirty = true;
        dirty.push(token);
    }
}

fn recycle(mut call: Call, free: &mut Vec<Call>, config: &GrpcConfig) {
    if free.len() < config.pooled_calls {
        // Clear first: `shrink_to` never goes below the current length.
        call.reset();
        call.shrink(config.pooled_buffer);
        free.push(call);
    }
}

/// Bytes still needed to complete the frame that starts `input`. Leftover
/// input always starts at a frame: HTTP/2 consumes partial prefaces itself.
fn missing(input: &[u8]) -> Result<usize, http2::Error> {
    let Some(header) = input.get(..FRAME_HEADER_LEN) else {
        return Ok(FRAME_HEADER_LEN - input.len());
    };
    let frame = FRAME_HEADER_LEN +
        (usize::from(header[0]) << 16 | usize::from(header[1]) << 8 | usize::from(header[2]));
    if frame > MAX_PARTIAL_INPUT {
        return Err(http2::Error::FrameSize);
    }
    Ok(frame - input.len())
}

/// Returns every call of a closing connection to the pool.
fn release(connection: &mut Connection, free: &mut Vec<Call>, config: &GrpcConfig) {
    for (_, call) in connection.calls.drain() {
        recycle(call, free, config);
    }
    connection.pending.clear();
}

fn mark(id: u32, call: &mut Call, pending: &mut Vec<u32>) {
    if !call.pending {
        call.pending = true;
        pending.push(id);
    }
}

impl Connection {
    fn receive(
        &mut self,
        token: Token,
        bytes: &[u8],
        read: IngestionTime,
        config: &GrpcConfig,
        free: &mut Vec<Call>,
        route: &mut impl Route,
    ) -> Result<(), http2::Error> {
        let mut bytes = bytes;
        // Complete a frame left from the previous read by copying only its
        // missing bytes; the rest of this read is parsed in place.
        while !self.input.is_empty() && !bytes.is_empty() {
            let n = missing(&self.input)?.min(bytes.len());
            self.input.extend_from_slice(&bytes[..n]);
            bytes = &bytes[n..];
            if missing(&self.input)? == 0 {
                let mut input = std::mem::take(&mut self.input);
                let started = self.input_received_at;
                let consumed =
                    self.receive_frames(token, &input, started, read, config, free, route)?;
                input.drain(..consumed);
                self.input = input;
            }
        }
        if self.input.is_empty() {
            let consumed = self.receive_frames(token, bytes, read, read, config, free, route)?;
            if consumed < bytes.len() {
                self.input.extend_from_slice(&bytes[consumed..]);
                self.input_received_at = read;
            }
        }
        Ok(())
    }

    /// `started` is when the first byte of `input` was read, `read` when its
    /// last byte was.
    #[allow(clippy::too_many_arguments)]
    fn receive_frames(
        &mut self,
        token: Token,
        input: &[u8],
        started: IngestionTime,
        read: IngestionTime,
        config: &GrpcConfig,
        free: &mut Vec<Call>,
        route: &mut impl Route,
    ) -> Result<usize, http2::Error> {
        let Self { h2, peer, tls, calls, pending, .. } = self;
        h2.receive(input, started, |event| match event {
            http2::Event::Request { stream_id, head, end_stream, received_at } => {
                let stream = Stream { token, id: stream_id };
                let mut call = free
                    .pop()
                    .unwrap_or_else(|| Call::new(config.max_message_size, Metadata::new(0), 0));
                call.input_closed = end_stream;
                match parse_request(&head) {
                    Ok(()) => {
                        let path = head.path.unwrap_or_default();
                        call.metadata.fill(path, head.fields, *peer, *tls, stream, received_at);
                        if end_stream {
                            call.finish(&Status::new(Code::Internal, "missing request message"));
                        }
                    }
                    Err(rejection) => call.finish_with(rejection.http_status, &rejection.status),
                }
                if call.trailers.is_some() {
                    mark(stream_id, &mut call, pending);
                }
                calls.insert(stream_id, call);
            }
            http2::Event::Data { stream_id, data, end_stream } => {
                if let Some(call) = calls.get_mut(&stream_id) {
                    call.credit += data.len() as u32;
                    call.receive(data, end_stream, read, route);
                    mark(stream_id, call, pending);
                }
            }
            http2::Event::Trailers { stream_id, .. } => {
                if let Some(call) = calls.get_mut(&stream_id) {
                    call.receive(&[], true, read, route);
                    mark(stream_id, call, pending);
                }
            }
            http2::Event::Reset { stream_id, .. } => {
                // A stale `pending` entry is skipped by `drive`; stream ids
                // are never reused.
                if let Some(call) = calls.remove(&stream_id) {
                    recycle(call, free, config);
                }
            }
            http2::Event::GoAway { .. } | http2::Event::Writable { .. } => {}
        })
    }

    /// Advances every pending call; blocked calls stay pending.
    fn drive(&mut self, fields: &ResponseFields, config: &GrpcConfig, free: &mut Vec<Call>) {
        let Self { h2, calls, pending, .. } = self;
        // Order is irrelevant: every blocked call is retried on each drive.
        let mut i = 0;
        while i < pending.len() {
            let id = pending[i];
            let done = match calls.get_mut(&id) {
                // Reset by the client since it was listed.
                None => false,
                Some(call) => match call.drive(id, h2, fields) {
                    Err(SendError::WouldBlock) => {
                        i += 1;
                        continue;
                    }
                    Ok(false) => {
                        call.pending = false;
                        false
                    }
                    Ok(true) | Err(SendError::Closed) => true,
                    Err(_) => {
                        // Never leave the client waiting on a call we dropped.
                        let _ = h2.reset(id, INTERNAL_ERROR);
                        true
                    }
                },
            };
            if done {
                recycle(calls.remove(&id).unwrap(), free, config);
            }
            pending.swap_remove(i);
        }
    }
}

/// gRPC server over one or two TCP groups of a caller-owned network.
///
/// Two ways to run it, once per loop iteration:
///
/// - The network serves only gRPC: call [`Self::poll`], which polls, routes,
///   and drives.
/// - The network is shared with other groups (or polled externally): pass each
///   of its events to [`Self::on_event`], which ignores other groups' events,
///   then call [`Self::drive`].
///
/// Stream messages and finishes queued between iterations are written by the
/// next `poll` or `drive`, one TCP write per connection.
///
/// Every call is routed once its request message arrives. Duplicate, missing,
/// oversized, or malformed requests finish with an error status and never
/// reach the callback. Rejected calls and transport failures need no handling:
/// their streams return [`Closed`].
pub struct GrpcServer {
    config: GrpcConfig,
    plain: Group,
    tls: Option<Group>,
    connections: FxHashMap<Token, Connection>,
    /// Connections with work for `drive`, each listed once, so idle
    /// connections cost nothing per iteration.
    dirty: Vec<Token>,
    /// Preallocated calls not in flight on any connection, at most
    /// `GrpcConfig::pooled_calls`.
    free: Vec<Call>,
    fields: ResponseFields,
}

impl GrpcServer {
    pub fn new(net: &mut NetworkCore, config: GrpcConfig) -> io::Result<Self> {
        http2::ServerConnection::new(config.http2)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let plain = net.add_group(Self::group_config(config));
        let free = (0..config.pooled_calls)
            .map(|_| {
                let metadata = Metadata::new(config.pooled_buffer);
                Call::new(config.max_message_size, metadata, config.pooled_buffer)
            })
            .collect();
        Ok(Self {
            config,
            plain,
            tls: None,
            connections: FxHashMap::default(),
            dirty: Vec::new(),
            free,
            fields: ResponseFields::new(),
        })
    }

    fn group_config(config: GrpcConfig) -> TcpGroupConfig {
        TcpGroupConfig {
            name: "grpc",
            framing: Framing::Raw,
            keepalive: true,
            replay: ReplayPolicy::Drop,
            max_frame_size: config.http2.max_send_buffer,
            backlog_warn_bytes: None,
            max_backlog_bytes: Some(4 * (TCP_BACKLOG + config.http2.max_send_buffer)),
            ..TcpGroupConfig::default()
        }
    }

    pub fn listen(&mut self, net: &mut NetworkCore, addr: SocketAddr) -> io::Result<()> {
        net.listen(self.plain, addr).map(drop)
    }

    /// The configuration must advertise only `h2`; peers that negotiate no
    /// ALPN are disconnected.
    #[cfg(feature = "tls")]
    pub fn listen_tls(
        &mut self,
        net: &mut NetworkCore,
        addr: SocketAddr,
        config: std::sync::Arc<crate::tls::ServerConfig>,
    ) -> io::Result<()> {
        if config.alpn_protocols.len() != 1 || config.alpn_protocols[0] != b"h2" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "gRPC TLS requires only h2 ALPN",
            ));
        }
        let group = *self.tls.get_or_insert_with(|| net.add_group(Self::group_config(self.config)));
        net.listen_tls(group, addr, config).map(drop)
    }

    /// For a network serving only gRPC: polls it, routes calls, and drives
    /// output. Equivalent to `on_event` for every event, then `drive`.
    pub fn poll(&mut self, net: &mut crate::Network, mut route: impl Route) {
        net.poll_with(|event| {
            self.on_event(&event, &mut route);
        });
        self.drive(net);
    }

    /// For a shared network: feed every event from its `poll_with`, then call
    /// [`Self::drive`]. Returns false for events of other groups, which the
    /// caller handles. `route` runs inside this call; the request message is
    /// borrowed for its duration.
    pub fn on_event(&mut self, event: &NetworkEvent<'_>, mut route: impl Route) -> bool {
        let group = event.group();
        if group != self.plain && Some(group) != self.tls {
            return false;
        }
        match *event {
            NetworkEvent::Accepted { token, peer_addr, .. } => {
                let h2 = http2::ServerConnection::new(self.config.http2)
                    .expect("validated HTTP/2 configuration");
                let tls = Some(group) == self.tls;
                let connection = self.connections.entry(token).insert_entry(Connection {
                    h2,
                    input: Vec::new(),
                    input_received_at: IngestionTime::default(),
                    peer: peer_addr,
                    tls,
                    calls: FxHashMap::default(),
                    pending: Vec::new(),
                    closing: false,
                    dirty: false,
                });
                // HTTP/2 queued its SETTINGS.
                touch(token, connection.into_mut(), &mut self.dirty);
            }
            NetworkEvent::Message { token, payload, send_ts, .. } => {
                let Self { connections, dirty, free, config, .. } = self;
                if let Some(connection) = connections.get_mut(&token) &&
                    !connection.closing
                {
                    touch(token, connection, dirty);
                    let read = IngestionTime::new(send_ts, Instant::now());
                    let received =
                        connection.receive(token, payload, read, config, free, &mut route);
                    if received.is_err() {
                        // HTTP/2 queued GOAWAY; flush it and close.
                        connection.closing = true;
                        release(connection, free, config);
                        connection.input.clear();
                    }
                }
            }
            NetworkEvent::Disconnected { token, .. } => {
                if let Some(mut connection) = self.connections.remove(&token) {
                    release(&mut connection, &mut self.free, &self.config);
                }
            }
            NetworkEvent::Connected { .. } => {}
        }
        true
    }

    /// Sends one serialized message on an open stream. Fails once the stream
    /// has ended; drop the handle then. Bytes HTTP/2 cannot take yet are
    /// copied into the stream's buffer; past `max_queued_bytes` it lags and
    /// closes. Panics above 4 GiB.
    pub fn send(&mut self, stream: Stream, payload: &[u8]) -> Result<(), Closed> {
        let max_queued = self.config.max_queued_bytes;
        let connection = self.connections.get_mut(&stream.token).ok_or(Closed::Ended)?;
        touch(stream.token, connection, &mut self.dirty);
        let call = connection
            .calls
            .get_mut(&stream.id)
            .filter(|call| call.is_open())
            .ok_or(Closed::Ended)?;
        let prefix = prefix(payload.len());
        if call.headers_sent && call.out.is_empty() {
            // Fast path: header, prefix and payload go straight into HTTP/2.
            match connection.h2.send_data_prefixed(stream.id, &prefix, payload, false) {
                Ok(n) if n == prefix.len() + payload.len() => return Ok(()),
                Ok(n) => call.offset = n,
                Err(SendError::WouldBlock) => {}
                Err(error) => {
                    if error != SendError::Closed {
                        let _ = connection.h2.reset(stream.id, INTERNAL_ERROR);
                    }
                    let call = connection.calls.remove(&stream.id).unwrap();
                    recycle(call, &mut self.free, &self.config);
                    return Err(Closed::Ended);
                }
            }
        } else if call.out.len() - call.offset + prefix.len() + payload.len() > max_queued {
            call.drop_unsent();
            call.finish(&Status::new(Code::ResourceExhausted, "stream lagged"));
            mark(stream.id, call, &mut connection.pending);
            return Err(Closed::Lagged);
        }
        call.push(&prefix, payload);
        mark(stream.id, call, &mut connection.pending);
        Ok(())
    }

    /// Ends an open stream after its queued messages.
    pub fn finish(&mut self, stream: Stream, status: &Status) -> Result<(), Closed> {
        let connection = self.connections.get_mut(&stream.token).ok_or(Closed::Ended)?;
        touch(stream.token, connection, &mut self.dirty);
        let call = connection
            .calls
            .get_mut(&stream.id)
            .filter(|call| call.is_open())
            .ok_or(Closed::Ended)?;
        call.finish(status);
        mark(stream.id, call, &mut connection.pending);
        Ok(())
    }

    /// Refuses new calls and finishes open streams with `UNAVAILABLE`.
    /// Connections close once their calls drain; see [`Self::is_idle`].
    pub fn shutdown(&mut self) {
        let status = Status::new(Code::Unavailable, "server shutting down");
        for (&token, connection) in &mut self.connections {
            touch(token, connection, &mut self.dirty);
            let _ = connection.h2.go_away();
            for (&id, call) in &mut connection.calls {
                call.finish(&status);
                mark(id, call, &mut connection.pending);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn pooled(&self) -> usize {
        self.free.len()
    }

    #[cfg(test)]
    pub(super) fn dirty(&self) -> usize {
        self.dirty.len()
    }

    pub fn is_idle(&self) -> bool {
        self.connections.is_empty()
    }

    /// Advances calls and writes each connection's output to TCP. Call once
    /// per iteration after `on_event`, even without events, so queued sends
    /// and finishes go out.
    pub fn drive(&mut self, net: &mut NetworkCore) {
        let Self { connections, dirty, free, fields, config, .. } = self;
        let mut i = 0;
        while i < dirty.len() {
            let token = dirty[i];
            let keep = match connections.get_mut(&token) {
                None => false,
                Some(connection) => {
                    if drive_connection(token, connection, net, fields, config, free) {
                        // Output still waits on TCP; flow-control waits end
                        // with an event, which lists the connection again.
                        connection.dirty = !connection.h2.output().is_empty();
                        connection.dirty
                    } else {
                        connections.remove(&token);
                        false
                    }
                }
            };
            if keep {
                i += 1;
            } else {
                dirty.swap_remove(i);
            }
        }
    }
}

/// Advances one connection's calls and writes its output. False once the
/// connection is closed and its calls are back in the pool.
fn drive_connection(
    token: Token,
    connection: &mut Connection,
    net: &mut NetworkCore,
    fields: &ResponseFields,
    config: &GrpcConfig,
    free: &mut Vec<Call>,
) -> bool {
    // Repeat while calls blocked by a full HTTP/2 buffer can move: each pass
    // produced output and TCP took all of it. What still blocks after a pass
    // that produced nothing waits on the peer's window, whose update arrives
    // as an event.
    loop {
        if !connection.closing {
            connection.drive(fields, config, free);
        }
        let produced = !connection.h2.output().is_empty();
        if !flush(token, connection, net) {
            release(connection, free, config);
            return false;
        }
        if connection.closing ||
            connection.pending.is_empty() ||
            !produced ||
            !connection.h2.output().is_empty()
        {
            break;
        }
    }
    let drained =
        connection.closing || (connection.h2.is_draining() && connection.calls.is_empty());
    if drained && connection.h2.output().is_empty() {
        net.disconnect_when_drained(token);
        release(connection, free, config);
        return false;
    }
    true
}

/// Writes HTTP/2 output while TCP's backlog is short. False if the socket is
/// gone.
fn flush(token: Token, connection: &mut Connection, net: &mut NetworkCore) -> bool {
    let output = connection.h2.output();
    if output.is_empty() ||
        net.pending_write_bytes(token).is_none_or(|queued| queued >= TCP_BACKLOG)
    {
        return true;
    }
    let n = output.len();
    if !net.send_with(token, |out| out.extend_from_slice(output)) {
        return false;
    }
    connection.h2.consume_output(n);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recycled_calls_shrink_and_the_pool_stays_bounded() {
        let config = GrpcConfig { pooled_calls: 1, pooled_buffer: 1024, ..GrpcConfig::default() };
        let new_call = || {
            let metadata = Metadata::new(1024);
            Call::new(config.max_message_size, metadata, 1024)
        };
        let mut grown = new_call();
        grown.out.resize(100_000, 0);
        grown.metadata.bytes.resize(100_000, 0);
        let mut free = Vec::new();
        recycle(grown, &mut free, &config);
        recycle(new_call(), &mut free, &config);
        assert_eq!(free.len(), 1, "the pool keeps at most `pooled_calls`");
        assert!(free[0].out.capacity() <= 1024);
        assert!(free[0].metadata.bytes.capacity() <= 1024);
    }
}
