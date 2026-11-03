use std::{borrow::Cow, io, net::SocketAddr};

use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use mio::Token;
use rustc_hash::FxHashMap;

use super::{Code, MessageDecoder, Status, TCP_BACKLOG, missing, prefix};
use crate::{
    Framing, Group, NetworkCore, NetworkEvent, ReplayPolicy, TcpGroupConfig,
    http2::{self, ClientConnection, Fields, RequestHeaders, SendError},
};

/// `RST_STREAM` code for an abandoned call.
const CANCEL: u32 = 8;
/// Capacity a pooled call's buffers shrink back to.
const POOLED_BUFFER: usize = 4 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct GrpcClientConfig {
    pub http2: http2::Http2Config,
    /// Largest accepted response message.
    pub max_message_size: usize,
    /// Finished calls kept for reuse with their buffers.
    pub pooled_calls: usize,
}

impl Default for GrpcClientConfig {
    fn default() -> Self {
        Self {
            http2: http2::Http2Config::default(),
            max_message_size: 4 * 1024 * 1024,
            pooled_calls: 64,
        }
    }
}

/// A connection to one server, redialled whenever it drops.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Channel(Token);

/// A method of a channel with its request metadata, encoded once and reused
/// for every call. Build a new one when the metadata changes.
#[derive(Clone, Debug)]
pub struct Rpc {
    channel: Channel,
    headers: RequestHeaders,
}

impl Rpc {
    pub const fn channel(&self) -> Channel {
        self.channel
    }
}

/// One call. Never reused for another call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Call {
    token: Token,
    id: u32,
}

impl Call {
    pub const fn channel(&self) -> Channel {
        Channel(self.token)
    }
}

#[derive(Debug)]
pub enum ClientEvent<'a> {
    /// The channel connected, or reconnected, and takes calls.
    Connected(Channel),
    /// The channel lost its connection; it redials. Its calls finished with
    /// `UNAVAILABLE` first.
    Disconnected(Channel),
    /// A response message, borrowed for the callback.
    Message { call: Call, message: &'a [u8] },
    /// The call ended. Every call from [`GrpcClient::call`] that is not
    /// cancelled finishes exactly once.
    Finished { call: Call, status: Status },
}

struct CallState {
    decoder: MessageDecoder,
    http_status: StatusCode,
    /// The unsent end of the framed request.
    out: Vec<u8>,
    offset: usize,
}

impl CallState {
    const fn new(max_message_size: usize) -> Self {
        Self {
            decoder: MessageDecoder::new(max_message_size),
            http_status: StatusCode::OK,
            out: Vec::new(),
            offset: 0,
        }
    }
}

fn recycle(mut call: CallState, free: &mut Vec<CallState>, config: &GrpcClientConfig) {
    if free.len() < config.pooled_calls {
        call.decoder.reset();
        call.decoder.message.shrink_to(POOLED_BUFFER);
        call.http_status = StatusCode::OK;
        call.out.clear();
        call.out.shrink_to(POOLED_BUFFER);
        call.offset = 0;
        free.push(call);
    }
}

struct Connection {
    authority: String,
    tls: bool,
    /// None while disconnected.
    h2: Option<ClientConnection>,
    /// A frame left incomplete by the previous read.
    input: Vec<u8>,
    calls: FxHashMap<u32, CallState>,
    /// Calls whose request is not fully written, oldest first.
    pending: Vec<u32>,
    /// Streams to reset on the next drive.
    resets: Vec<u32>,
    /// A protocol error: flush the GOAWAY, then reconnect.
    closing: bool,
}

/// gRPC client for unary and server-streaming calls over one TCP group of a
/// caller-owned network.
///
/// Run it like [`GrpcServer`](super::GrpcServer), once per loop iteration:
/// [`Self::poll`] for a network of its own, or [`Self::on_event`] for each
/// event of a shared network and then [`Self::drive`]. Calls are written by
/// the next `poll` or `drive`.
///
/// Requests are uncompressed messages over HTTP/2, plaintext or TLS (h2 ALPN).
/// Deadlines are the caller's: [`Self::cancel`] a call that took too long.
pub struct GrpcClient {
    config: GrpcClientConfig,
    group: Group,
    connections: FxHashMap<Token, Connection>,
    free: Vec<CallState>,
}

impl GrpcClient {
    pub fn new(net: &mut NetworkCore, config: GrpcClientConfig) -> io::Result<Self> {
        ClientConnection::new(config.http2)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let group = net.add_group(TcpGroupConfig {
            name: "grpc-client",
            framing: Framing::Raw,
            keepalive: true,
            replay: ReplayPolicy::Drop,
            max_frame_size: config.http2.max_send_buffer,
            backlog_warn_bytes: None,
            max_backlog_bytes: Some(4 * (TCP_BACKLOG + config.http2.max_send_buffer)),
            ..TcpGroupConfig::default()
        });
        let free = (0..config.pooled_calls).map(|_| CallState::new(config.max_message_size));
        Ok(Self { config, group, connections: FxHashMap::default(), free: free.collect() })
    }

    /// A plaintext (h2c) channel. `authority` is the `:authority` of its calls.
    pub fn connect(&mut self, net: &mut NetworkCore, addr: SocketAddr, authority: &str) -> Channel {
        let token = net.connect(self.group, addr);
        self.add(token, authority, false)
    }

    /// A TLS channel. The host of `authority` is the server name. The
    /// configuration must advertise only `h2`.
    #[cfg(feature = "tls")]
    pub fn connect_tls(
        &mut self,
        net: &mut NetworkCore,
        addr: SocketAddr,
        authority: &str,
        config: std::sync::Arc<crate::tls::ClientConfig>,
    ) -> io::Result<Channel> {
        if config.alpn_protocols.len() != 1 || config.alpn_protocols[0] != b"h2" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "gRPC TLS requires only h2 ALPN",
            ));
        }
        let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "invalid TLS authority");
        let parsed: http::uri::Authority = authority.parse().map_err(|_| invalid())?;
        let host = parsed.host().trim_start_matches('[').trim_end_matches(']');
        // `Session::new` panics on an invalid name.
        crate::tls::rustls::pki_types::ServerName::try_from(host).map_err(|_| invalid())?;
        let session = crate::tls::Session::new(host).with_config(config);
        let token = net.connect_tls(self.group, addr, session);
        Ok(self.add(token, authority, true))
    }

    fn add(&mut self, token: Token, authority: &str, tls: bool) -> Channel {
        self.connections.insert(token, Connection {
            authority: authority.to_owned(),
            tls,
            h2: None,
            input: Vec::new(),
            calls: FxHashMap::default(),
            pending: Vec::new(),
            resets: Vec::new(),
            closing: false,
        });
        Channel(token)
    }

    /// Closes `channel` for good. Its calls end without
    /// [`ClientEvent::Finished`], as if cancelled.
    pub fn close(&mut self, net: &mut NetworkCore, channel: Channel) {
        if let Some(mut connection) = self.connections.remove(&channel.0) {
            net.remove(channel.0);
            for (_, call) in connection.calls.drain() {
                recycle(call, &mut self.free, &self.config);
            }
        }
    }

    /// Whether `channel` is connected and takes new calls.
    pub fn is_connected(&self, channel: Channel) -> bool {
        self.connections.get(&channel.0).is_some_and(|connection| {
            !connection.closing && connection.h2.as_ref().is_some_and(|h2| !h2.is_draining())
        })
    }

    /// Prepares calls to `path` (`/package.Service/Method`) on `channel` with
    /// lowercase `metadata`.
    pub fn rpc(
        &self,
        channel: Channel,
        path: &str,
        metadata: &HeaderMap,
    ) -> Result<Rpc, SendError> {
        let connection = self.connections.get(&channel.0).ok_or(SendError::Closed)?;
        let mut fields = HeaderMap::with_capacity(metadata.len() + 2);
        fields.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/grpc"));
        fields.insert(HeaderName::from_static("te"), HeaderValue::from_static("trailers"));
        fields.extend(metadata.clone());
        let scheme = if connection.tls { "https" } else { "http" };
        let headers =
            RequestHeaders::new(Method::POST, scheme, &connection.authority, path, &fields)?;
        Ok(Rpc { channel, headers })
    }

    /// Starts a call with one serialized request message. `WouldBlock` while
    /// the server's stream limit or the send buffer is full; `Closed` while
    /// the channel is not connected.
    pub fn call(&mut self, rpc: &Rpc, message: &[u8]) -> Result<Call, SendError> {
        let token = rpc.channel.0;
        let connection = self.connections.get_mut(&token).ok_or(SendError::Closed)?;
        let h2 = connection.h2.as_mut().filter(|_| !connection.closing).ok_or(SendError::Closed)?;
        let id = h2.send_headers(&rpc.headers, false)?;
        let prefix = prefix(message.len());
        // Fast path: prefix and message go straight into HTTP/2.
        let sent = match h2.send_data_prefixed(id, &prefix, message, true) {
            Ok(n) => n,
            Err(SendError::WouldBlock) => 0,
            Err(error) => {
                let _ = h2.reset(id, CANCEL);
                return Err(error);
            }
        };
        let mut call =
            self.free.pop().unwrap_or_else(|| CallState::new(self.config.max_message_size));
        if sent < prefix.len() + message.len() {
            call.out.extend_from_slice(&prefix[sent.min(prefix.len())..]);
            call.out.extend_from_slice(&message[sent.saturating_sub(prefix.len())..]);
            connection.pending.push(id);
        }
        connection.calls.insert(id, call);
        Ok(Call { token, id })
    }

    /// Abandons a call without a [`ClientEvent::Finished`]. False if it had
    /// already finished.
    pub fn cancel(&mut self, call: Call) -> bool {
        let Some(connection) = self.connections.get_mut(&call.token) else { return false };
        let Some(state) = connection.calls.remove(&call.id) else { return false };
        connection.resets.push(call.id);
        recycle(state, &mut self.free, &self.config);
        true
    }

    /// For a network used only by this client: polls it, handles its events,
    /// and drives output.
    pub fn poll(&mut self, net: &mut crate::Network, mut handler: impl FnMut(ClientEvent<'_>)) {
        net.poll_with(|event| {
            self.on_event(&event, &mut handler);
        });
        self.drive(net);
    }

    /// For a shared network: feed every event from its `poll_with`, then call
    /// [`Self::drive`]. Returns false for events of other groups.
    pub fn on_event(
        &mut self,
        event: &NetworkEvent<'_>,
        mut handler: impl FnMut(ClientEvent<'_>),
    ) -> bool {
        if event.group() != self.group {
            return false;
        }
        let Self { config, connections, free, .. } = self;
        match *event {
            NetworkEvent::Connected { token, .. } => {
                if let Some(connection) = connections.get_mut(&token) {
                    connection.h2 =
                        Some(ClientConnection::new(config.http2).expect("validated configuration"));
                    handler(ClientEvent::Connected(Channel(token)));
                }
            }
            NetworkEvent::Message { token, payload, .. } => {
                if let Some(connection) = connections.get_mut(&token) &&
                    !connection.closing &&
                    connection.receive(token, payload, free, config, &mut handler).is_err()
                {
                    // HTTP/2 queued GOAWAY; `drive` flushes it and reconnects.
                    connection.closing = true;
                }
            }
            NetworkEvent::Disconnected { token, .. } => {
                if let Some(connection) = connections.get_mut(&token) {
                    connection.h2 = None;
                    connection.closing = false;
                    connection.input.clear();
                    connection.pending.clear();
                    connection.resets.clear();
                    for (id, call) in connection.calls.drain() {
                        let status = Status::new(Code::Unavailable, "connection lost");
                        handler(ClientEvent::Finished { call: Call { token, id }, status });
                        recycle(call, free, config);
                    }
                    handler(ClientEvent::Disconnected(Channel(token)));
                }
            }
            NetworkEvent::Accepted { .. } => {}
        }
        true
    }

    /// Writes pending requests, resets and HTTP/2 output to TCP, and
    /// reconnects drained channels. Call once per iteration after `on_event`.
    pub fn drive(&mut self, net: &mut NetworkCore) {
        for (&token, connection) in &mut self.connections {
            let Connection { h2: Some(h2), calls, pending, resets, closing, .. } = connection
            else {
                continue;
            };
            for id in resets.drain(..) {
                let _ = h2.reset(id, CANCEL);
            }
            // Repeat while requests blocked by a full send buffer can move:
            // what still blocks after a pass waits on the server's window,
            // whose update arrives as an event.
            loop {
                pending.retain(|id| {
                    let Some(call) = calls.get_mut(id) else { return false };
                    loop {
                        match h2.send_data(*id, &call.out[call.offset..], true) {
                            Ok(n) => call.offset += n,
                            Err(SendError::WouldBlock) => return true,
                            // Reset since; its `Finished` reports why.
                            Err(_) => return false,
                        }
                        if call.offset == call.out.len() {
                            call.out.clear();
                            call.offset = 0;
                            return false;
                        }
                    }
                });
                let produced = !h2.output().is_empty();
                if !flush(token, h2, net) {
                    break;
                }
                if pending.is_empty() || !produced || !h2.output().is_empty() {
                    break;
                }
            }
            let drained = *closing || (h2.is_draining() && calls.is_empty());
            if drained && h2.output().is_empty() {
                // Redialled; `Disconnected` finishes what is left.
                net.disconnect(token);
            }
        }
    }
}

/// Writes HTTP/2 output while TCP's backlog is short. False if the socket is
/// gone.
fn flush(token: Token, h2: &mut ClientConnection, net: &mut NetworkCore) -> bool {
    let output = h2.output();
    if output.is_empty() ||
        net.pending_write_bytes(token).is_none_or(|queued| queued >= TCP_BACKLOG)
    {
        return true;
    }
    let n = output.len();
    if !net.send_with(token, |out| out.extend_from_slice(output)) {
        return false;
    }
    h2.consume_output(n);
    true
}

impl Connection {
    fn receive(
        &mut self,
        token: Token,
        mut bytes: &[u8],
        free: &mut Vec<CallState>,
        config: &GrpcClientConfig,
        handler: &mut impl FnMut(ClientEvent<'_>),
    ) -> Result<(), http2::Error> {
        // Complete a frame left from the previous read by copying only its
        // missing bytes; the rest of this read is parsed in place.
        while !self.input.is_empty() && !bytes.is_empty() {
            let n = missing(&self.input)?.min(bytes.len());
            self.input.extend_from_slice(&bytes[..n]);
            bytes = &bytes[n..];
            if missing(&self.input)? == 0 {
                let mut input = std::mem::take(&mut self.input);
                let consumed = self.receive_frames(token, &input, free, config, handler)?;
                input.drain(..consumed);
                self.input = input;
            }
        }
        if self.input.is_empty() {
            let consumed = self.receive_frames(token, bytes, free, config, handler)?;
            self.input.extend_from_slice(&bytes[consumed..]);
        }
        Ok(())
    }

    fn receive_frames(
        &mut self,
        token: Token,
        input: &[u8],
        free: &mut Vec<CallState>,
        config: &GrpcClientConfig,
        handler: &mut impl FnMut(ClientEvent<'_>),
    ) -> Result<usize, http2::Error> {
        let Self { h2: Some(h2), calls, resets, .. } = self else { return Ok(input.len()) };
        h2.receive(input, |event| match event {
            http2::ClientEvent::Response { stream_id, status, fields, end_stream } => {
                if end_stream {
                    finish(
                        calls,
                        free,
                        config,
                        handler,
                        Call { token, id: stream_id },
                        response_status(status, Some(fields)),
                    );
                } else if let Some(call) = calls.get_mut(&stream_id) {
                    call.http_status = status;
                }
            }
            http2::ClientEvent::Data { stream_id, data, end_stream } => {
                let Some(call) = calls.get_mut(&stream_id) else { return };
                let http_status = call.http_status;
                // A non-200 body is not gRPC; its status decides.
                let decoded = if http_status == StatusCode::OK {
                    let id = Call { token, id: stream_id };
                    call.decoder.receive(data, end_stream, |message| {
                        handler(ClientEvent::Message { call: id, message });
                    })
                } else {
                    Ok(())
                };
                if let Err(status) = decoded {
                    resets.push(stream_id);
                    finish(calls, free, config, handler, Call { token, id: stream_id }, status);
                } else if end_stream {
                    finish(
                        calls,
                        free,
                        config,
                        handler,
                        Call { token, id: stream_id },
                        response_status(http_status, None),
                    );
                }
            }
            http2::ClientEvent::Trailers { stream_id, fields } => {
                let Some(call) = calls.get(&stream_id) else { return };
                finish(
                    calls,
                    free,
                    config,
                    handler,
                    Call { token, id: stream_id },
                    response_status(call.http_status, Some(fields)),
                );
            }
            http2::ClientEvent::Reset { stream_id, error_code } => {
                finish(
                    calls,
                    free,
                    config,
                    handler,
                    Call { token, id: stream_id },
                    reset_status(error_code),
                );
            }
            http2::ClientEvent::Writable { .. } |
            http2::ClientEvent::GoAway { .. } |
            http2::ClientEvent::Pong { .. } => {}
        })
    }
}

fn finish(
    calls: &mut FxHashMap<u32, CallState>,
    free: &mut Vec<CallState>,
    config: &GrpcClientConfig,
    handler: &mut impl FnMut(ClientEvent<'_>),
    call: Call,
    status: Status,
) {
    if let Some(state) = calls.remove(&call.id) {
        handler(ClientEvent::Finished { call, status });
        recycle(state, free, config);
    }
}

/// The status of a finished response: its `grpc-status`, or one derived from
/// the HTTP status (gRPC's HTTP-to-gRPC status mapping).
fn response_status(http_status: StatusCode, fields: Option<Fields<'_>>) -> Status {
    if let Some(fields) = fields &&
        let Some(code) = fields.get("grpc-status")
    {
        let code = std::str::from_utf8(code)
            .ok()
            .and_then(|c| c.parse().ok())
            .map_or(Code::Unknown, |c: u8| {
                CODES.get(usize::from(c)).copied().unwrap_or(Code::Unknown)
            });
        let message = fields.get("grpc-message").map_or(Cow::Borrowed(""), percent_decode);
        return Status::new(code, message);
    }
    let code = match http_status.as_u16() {
        400 => Code::Internal,
        401 => Code::Unauthenticated,
        403 => Code::PermissionDenied,
        404 => Code::Unimplemented,
        429 | 502..=504 => Code::Unavailable,
        _ => Code::Unknown,
    };
    if http_status == StatusCode::OK {
        Status::new(code, "missing grpc-status")
    } else {
        Status::new(code, format!("HTTP {}", http_status.as_u16()))
    }
}

const CODES: [Code; 17] = [
    Code::Ok,
    Code::Cancelled,
    Code::Unknown,
    Code::InvalidArgument,
    Code::DeadlineExceeded,
    Code::NotFound,
    Code::AlreadyExists,
    Code::PermissionDenied,
    Code::ResourceExhausted,
    Code::FailedPrecondition,
    Code::Aborted,
    Code::OutOfRange,
    Code::Unimplemented,
    Code::Internal,
    Code::Unavailable,
    Code::DataLoss,
    Code::Unauthenticated,
];

/// gRPC's mapping of HTTP/2 reset codes.
fn reset_status(error_code: u32) -> Status {
    match error_code {
        7 => Status::new(Code::Unavailable, "stream refused"),
        8 => Status::new(Code::Cancelled, "stream cancelled"),
        11 => Status::new(Code::ResourceExhausted, "stream reset: enhance your calm"),
        12 => Status::new(Code::PermissionDenied, "stream reset: inadequate security"),
        _ => Status::new(Code::Internal, "stream reset"),
    }
}

fn percent_decode(value: &[u8]) -> Cow<'static, str> {
    let mut decoded = Vec::with_capacity(value.len());
    let mut i = 0;
    while i < value.len() {
        let byte = value.get(i + 1..i + 3).filter(|_| value[i] == b'%').and_then(|hex| {
            std::str::from_utf8(hex).ok().and_then(|hex| u8::from_str_radix(hex, 16).ok())
        });
        if let Some(byte) = byte {
            decoded.push(byte);
            i += 3;
        } else {
            decoded.push(value[i]);
            i += 1;
        }
    }
    Cow::Owned(String::from_utf8_lossy(&decoded).into_owned())
}
