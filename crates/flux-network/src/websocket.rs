//! Poll-driven WebSocket (RFC 6455) over a caller-owned
//! [`crate::NetworkCore`].
//!
//! Driven like [`HttpNetwork`](crate::http::HttpNetwork):
//! [`WsNetwork::on_event`] claims its group's events and [`WsNetwork::drive`]
//! delivers them. An accepted connection delivers its upgrade request as
//! [`WsEvent::Request`], answered with [`WsNetwork::accept`] or
//! [`WsNetwork::reject`]. An outbound endpoint sends its handshake on every
//! connect and redials when dropped. Messages are delivered whole. No extension
//! is negotiated.

use std::{
    io::{self, Write as _},
    net::SocketAddr,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use flux_timing::{Duration, Instant};
use mio::Token;
use sha1::{Digest as _, Sha1};

use crate::{
    Framing, Group, NetworkCore, NetworkEvent, ReplayPolicy, TcpGroupConfig,
    http::{HttpRequest, has_token, reason_phrase, valid_token},
};

const OP_CONTINUATION: u8 = 0x0;
const OP_TEXT: u8 = 0x1;
const OP_BINARY: u8 = 0x2;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xA;

const CLOSE_PROTOCOL_ERROR: u16 = 1002;
const CLOSE_INVALID_DATA: u16 = 1007;
const CLOSE_TOO_BIG: u16 = 1009;

/// Two bytes, a 64-bit length and a mask.
const MAX_FRAME_HEADER: usize = 14;
const MAX_CONTROL: usize = 125;
const MAX_HEADERS: usize = 128;
const KEEP_BUF_BYTES: usize = 1024 * 1024;
/// Room for the read that crosses a poll's read budget.
const RX_SLACK_BYTES: usize = 64 * 1024;
/// Record overhead allowance on top of a full-size message.
const TLS_MARGIN_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WsKind {
    Text,
    Binary,
    Ping,
    Pong,
}

pub enum WsEvent<'a> {
    /// An accepted connection's upgrade request; answer it with
    /// [`WsNetwork::accept`] or [`WsNetwork::reject`]. Its body is not read.
    Request {
        token: Token,
        peer_addr: SocketAddr,
        request: HttpRequest<'a>,
    },
    /// An outbound endpoint's handshake was accepted.
    Open {
        token: Token,
    },
    /// A whole message, or a ping (already answered) or pong.
    Message {
        token: Token,
        kind: WsKind,
        payload: &'a [u8],
    },
    /// The peer's close frame, already echoed.
    Close {
        token: Token,
        payload: &'a [u8],
    },
    Disconnected {
        token: Token,
    },
}

/// Whether `request` is a WebSocket opening handshake.
pub fn is_upgrade(request: &HttpRequest<'_>) -> bool {
    request.method == "GET" &&
        has_token(request.headers, "upgrade", b"websocket") &&
        has_token(request.headers, "connection", b"upgrade") &&
        request.header("sec-websocket-version") == Some(b"13") &&
        request.header("sec-websocket-key").is_some_and(|key| key.len() == 24)
}

#[derive(Clone, Copy)]
enum State {
    /// An outbound endpoint waiting for its connect.
    Dialing,
    /// Accepted, waiting for the upgrade request.
    Request,
    /// The request was delivered; holds its accept key if it is an upgrade.
    Answer(Option<[u8; 28]>),
    /// Waiting for the `101` carrying this accept key.
    Upgrade([u8; 28]),
    Open,
}

struct Conn {
    token: Token,
    /// The peer of an accepted connection, the endpoint of an outbound one.
    addr: SocketAddr,
    state: State,
    /// When the handshake started.
    since: Instant,
    /// An outbound endpoint's handshake request up to its key.
    handshake: Vec<u8>,
    buf: Vec<u8>,
    dirty: bool,
    over_limit: bool,
    ws: Ws,
}

#[derive(Clone, Copy)]
enum Lifecycle {
    Connected(Token),
    Disconnected(Token),
}

pub struct WsNetwork {
    group: Option<Group>,
    name: &'static str,
    max_message_bytes: usize,
    max_head_bytes: usize,
    handshake_timeout: Duration,
    reconnect_interval: Option<Duration>,
    conns: Vec<Conn>,
    lifecycle: Vec<Lifecycle>,
}

impl Default for WsNetwork {
    fn default() -> Self {
        Self {
            group: None,
            name: "websocket",
            max_message_bytes: 1024 * 1024,
            max_head_bytes: 16 * 1024,
            handshake_timeout: Duration::from_secs(10),
            reconnect_interval: None,
            conns: Vec::new(),
            lifecycle: Vec::new(),
        }
    }
}

impl WsNetwork {
    /// Sets the TCP group name.
    pub fn with_name(mut self, name: &'static str) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.name = name;
        self
    }
    /// Sets the largest message taken; a larger one closes with `1009`. Also
    /// bounds the unsent bytes past which a connection is dropped.
    pub fn with_max_message_bytes(mut self, max_message_bytes: usize) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.max_message_bytes = max_message_bytes;
        self
    }
    /// Sets the largest handshake head taken.
    pub fn with_max_head_bytes(mut self, max_head_bytes: usize) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.max_head_bytes = max_head_bytes;
        self
    }
    /// Drops a connection whose handshake has not finished this long after it
    /// connected.
    pub fn with_handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.handshake_timeout = handshake_timeout;
        self
    }
    /// Retry interval for the outbound endpoints this layer opens.
    pub fn with_reconnect_interval(mut self, reconnect_interval: Duration) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.reconnect_interval = Some(reconnect_interval);
        self
    }
    /// A poll reads about one whole message and head per connection.
    fn read_budget(&self) -> usize {
        self.max_head_bytes.saturating_add(self.max_message_bytes)
    }
    fn group(&mut self, net: &mut NetworkCore) -> Group {
        let read_budget = self.read_budget();
        let Self { group, name, max_message_bytes, reconnect_interval, .. } = self;
        *group.get_or_insert_with(|| {
            let defaults = TcpGroupConfig::default();
            let group = net.add_group(TcpGroupConfig {
                name,
                framing: Framing::Raw,
                replay: ReplayPolicy::Drop,
                reconnect_interval: reconnect_interval.unwrap_or(defaults.reconnect_interval),
                max_frame_size: usize::MAX,
                max_backlog_bytes: Some(max_message_bytes.saturating_add(TLS_MARGIN_BYTES)),
                backlog_warn_bytes: None,
                ..Default::default()
            });
            net.set_max_read_bytes_per_poll(group, read_budget);
            group
        })
    }
    pub fn listen(&mut self, net: &mut NetworkCore, addr: SocketAddr) -> io::Result<()> {
        let group = self.group(net);
        net.listen(group, addr)?;
        Ok(())
    }
    /// Opens an outbound endpoint that sends the handshake for `path` with
    /// `headers` on every connect, adding `Host` if missing. Panics on an
    /// invalid path or header.
    pub fn connect(
        &mut self,
        net: &mut NetworkCore,
        addr: SocketAddr,
        path: &str,
        headers: &[(&str, &str)],
    ) -> Token {
        let handshake = handshake(addr, path, headers);
        let group = self.group(net);
        let token = net.connect(group, addr);
        self.track_outbound(token, addr, handshake)
    }
    /// Like [`Self::connect`] but negotiates TLS once the TCP connect
    /// completes, verifying against the Mozilla roots and sending SNI
    /// `server`. Panics if `server` is not a valid DNS name or IP.
    #[cfg(feature = "tls")]
    pub fn connect_tls(
        &mut self,
        net: &mut NetworkCore,
        addr: SocketAddr,
        server: &str,
        path: &str,
        headers: &[(&str, &str)],
    ) -> Token {
        let handshake = handshake(addr, path, headers);
        let session = crate::tls::Session::new(server);
        let group = self.group(net);
        let token = net.connect_tls(group, addr, session);
        self.track_outbound(token, addr, handshake)
    }
    fn track_outbound(&mut self, token: Token, addr: SocketAddr, handshake: Vec<u8>) -> Token {
        self.conns.push(Conn::new(token, addr, State::Dialing, handshake));
        token
    }
    /// Answers a pending upgrade request with `101`; `false` if it was no
    /// upgrade.
    pub fn accept(&mut self, net: &mut NetworkCore, token: Token) -> bool {
        let Some(conn) = self.conns.iter_mut().find(|conn| conn.token == token) else {
            return false
        };
        let State::Answer(Some(accept)) = conn.state else { return false };
        let ok = net.send_with(token, |out| {
            out.extend_from_slice(
                b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ",
            );
            out.extend_from_slice(&accept);
            out.extend_from_slice(b"\r\n\r\n");
        });
        if ok {
            conn.state = State::Open;
            conn.dirty = !conn.buf.is_empty();
        }
        ok
    }
    /// Answers a pending upgrade request with an error `status` and closes the
    /// connection once it is written. `headers` must not set `Content-Length`
    /// or `Connection`.
    pub fn reject(
        &mut self,
        net: &mut NetworkCore,
        token: Token,
        status: u16,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> bool {
        if !(400..=599).contains(&status) || !valid_headers(headers) {
            return false
        }
        self.conns
            .iter_mut()
            .find(|conn| conn.token == token && matches!(conn.state, State::Answer(_)))
            .is_some_and(|conn| conn.refuse(net, status, headers, body))
    }
    /// Sends one unfragmented frame; pings and pongs take at most 125 bytes.
    pub fn send(
        &mut self,
        net: &mut NetworkCore,
        token: Token,
        kind: WsKind,
        payload: &[u8],
    ) -> bool {
        self.conns
            .iter()
            .find(|conn| conn.token == token && matches!(conn.state, State::Open))
            .is_some_and(|conn| conn.ws.send(net, token, kind, payload))
    }
    /// Sends a close frame and drops the connection once it is written. An
    /// outbound endpoint then redials; [`Self::remove`] stops it. `false` for
    /// a code RFC 6455 does not allow on the wire.
    pub fn send_close(&mut self, net: &mut NetworkCore, token: Token, code: u16) -> bool {
        valid_close_code(code) &&
            self.conns
                .iter_mut()
                .find(|conn| conn.token == token && matches!(conn.state, State::Open))
                .is_some_and(|conn| conn.ws.close(net, token, &code.to_be_bytes()))
    }
    /// Immediately disconnects an accepted client.
    pub fn disconnect(&mut self, net: &mut NetworkCore, token: Token) -> bool {
        self.conns.iter().any(|conn| conn.token == token && !conn.ws.client) &&
            net.disconnect(token)
    }
    /// Permanently removes an outbound endpoint and stops it reconnecting.
    pub fn remove(&mut self, net: &mut NetworkCore, token: Token) -> bool {
        let Some(i) = self.conns.iter().position(|conn| conn.token == token && conn.ws.client)
        else {
            return false
        };
        self.conns.swap_remove(i);
        net.remove(token)
    }
    /// Takes in one network event; returns whether it belonged to this
    /// layer's group. Call from the network's `poll_with` handler.
    pub fn on_event(&mut self, event: &NetworkEvent<'_>) -> bool {
        let Some(group) = self.group else { return false };
        match *event {
            NetworkEvent::Accepted { group: event_group, token, peer_addr }
                if event_group == group =>
            {
                self.conns.push(Conn::new(token, peer_addr, State::Request, Vec::new()));
            }
            NetworkEvent::Connected { group: event_group, token, .. } if event_group == group => {
                self.lifecycle.push(Lifecycle::Connected(token));
            }
            NetworkEvent::Message { group: event_group, token, payload, .. }
                if event_group == group =>
            {
                // An unfinished frame plus one poll's reads.
                let limit = self.read_budget().saturating_mul(2).saturating_add(RX_SLACK_BYTES);
                if let Some(conn) = self.conns.iter_mut().find(|conn| conn.token == token) &&
                    !conn.ws.closing &&
                    !conn.over_limit
                {
                    if payload.len() > limit.saturating_sub(conn.buf.len()) {
                        conn.over_limit = true;
                        conn.buf = Vec::new();
                    } else {
                        conn.buf.extend_from_slice(payload);
                        conn.dirty = true;
                    }
                }
            }
            NetworkEvent::Disconnected { group: event_group, token, .. }
                if event_group == group =>
            {
                self.lifecycle.push(Lifecycle::Disconnected(token));
            }
            _ => return false,
        }
        true
    }
    /// Delivers the events taken in since the last call and drops connections
    /// over their limits or handshake timeout. Call once per poll, after the
    /// network's `poll_with`.
    pub fn drive<F>(&mut self, net: &mut NetworkCore, mut handler: F)
    where
        F: for<'a> FnMut(WsEvent<'a>),
    {
        for at in 0..self.lifecycle.len() {
            match self.lifecycle[at] {
                Lifecycle::Connected(token) => {
                    if let Some(conn) = self.conns.iter_mut().find(|conn| conn.token == token) {
                        conn.upgrade(net);
                    }
                }
                Lifecycle::Disconnected(token) => {
                    if let Some(i) = self.conns.iter().position(|conn| conn.token == token) {
                        if self.conns[i].dirty {
                            self.parse(net, i, &mut handler);
                        }
                        if self.conns[i].ws.client {
                            self.conns[i].reset();
                        } else {
                            self.conns.swap_remove(i);
                        }
                    }
                    handler(WsEvent::Disconnected { token });
                }
            }
        }
        self.lifecycle.clear();
        for i in 0..self.conns.len() {
            if self.conns[i].dirty {
                self.parse(net, i, &mut handler);
            }
        }
        for conn in &mut self.conns {
            let handshaking =
                matches!(conn.state, State::Request | State::Answer(_) | State::Upgrade(_));
            if conn.over_limit ||
                handshaking && !conn.ws.closing && conn.since.elapsed() >= self.handshake_timeout
            {
                conn.over_limit = false;
                conn.ws.closing = true;
                net.disconnect(conn.token);
            }
        }
    }
    fn parse<F>(&mut self, net: &mut NetworkCore, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(WsEvent<'a>),
    {
        let (max_head_bytes, max_message_bytes) = (self.max_head_bytes, self.max_message_bytes);
        let conn = &mut self.conns[i];
        conn.dirty = false;
        match conn.state {
            State::Request => conn.parse_request(net, max_head_bytes, handler),
            State::Upgrade(accept) => {
                if conn.parse_response(net, accept, max_head_bytes, handler) {
                    conn.parse_frames(net, max_message_bytes, handler);
                }
            }
            State::Open => conn.parse_frames(net, max_message_bytes, handler),
            State::Dialing | State::Answer(_) => {}
        }
    }
}

impl Conn {
    fn new(token: Token, addr: SocketAddr, state: State, handshake: Vec<u8>) -> Self {
        Self {
            token,
            addr,
            state,
            since: Instant::now(),
            ws: Ws::new(!handshake.is_empty()),
            handshake,
            buf: Vec::new(),
            dirty: false,
            over_limit: false,
        }
    }
    /// Sends the client handshake with a fresh key.
    fn upgrade(&mut self, net: &mut NetworkCore) {
        let mut key = [0; 24];
        let _ = STANDARD.encode_slice(rand::random::<[u8; 16]>(), &mut key);
        let sent = net.send_with(self.token, |out| {
            out.extend_from_slice(&self.handshake);
            out.extend_from_slice(&key);
            out.extend_from_slice(b"\r\n\r\n");
        });
        if sent {
            self.state = State::Upgrade(accept_key(&key));
            self.since = Instant::now();
        } else {
            net.disconnect(self.token);
        }
    }
    /// Back to dialing after an outbound endpoint dropped.
    fn reset(&mut self) {
        self.state = State::Dialing;
        self.buf.clear();
        self.dirty = false;
        self.over_limit = false;
        self.ws = Ws::new(true);
    }
    fn refuse(
        &mut self,
        net: &mut NetworkCore,
        status: u16,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> bool {
        let ok = net.send_with(self.token, |out| {
            write!(out, "HTTP/1.1 {status} {}\r\n", reason_phrase(status)).unwrap();
            for (name, value) in headers {
                write!(out, "{name}: {value}\r\n").unwrap();
            }
            write!(out, "Content-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
            out.extend_from_slice(body);
        });
        self.ws.closing = true;
        self.buf.clear();
        net.disconnect_when_drained(self.token);
        ok
    }
    fn parse_request<F>(&mut self, net: &mut NetworkCore, max_head_bytes: usize, handler: &mut F)
    where
        F: for<'a> FnMut(WsEvent<'a>),
    {
        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut req = httparse::Request::new(&mut headers);
        let head = match req.parse(&self.buf) {
            Ok(httparse::Status::Complete(head)) if head <= max_head_bytes => head,
            Ok(httparse::Status::Partial) if self.buf.len() <= max_head_bytes => return,
            _ => {
                self.refuse(net, 400, &[], &[]);
                return
            }
        };
        let request = HttpRequest {
            method: req.method.unwrap_or(""),
            path: req.path.unwrap_or(""),
            version: req.version.unwrap_or(1),
            headers: req.headers,
            body: &[],
        };
        let accept = is_upgrade(&request)
            .then(|| accept_key(request.header("sec-websocket-key").unwrap_or_default()));
        handler(WsEvent::Request { token: self.token, peer_addr: self.addr, request });
        self.buf.drain(..head);
        self.state = State::Answer(accept);
    }
    /// Opens on a valid `101`; anything else drops the connection.
    fn parse_response<F>(
        &mut self,
        net: &mut NetworkCore,
        accept: [u8; 28],
        max_head_bytes: usize,
        handler: &mut F,
    ) -> bool
    where
        F: for<'a> FnMut(WsEvent<'a>),
    {
        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut response = httparse::Response::new(&mut headers);
        let head = match response.parse(&self.buf) {
            Ok(httparse::Status::Partial) if self.buf.len() <= max_head_bytes => return false,
            Ok(httparse::Status::Complete(head)) if head <= max_head_bytes => Some(head),
            _ => None,
        };
        let header = |name: &str| {
            response.headers.iter().find(|h| h.name.eq_ignore_ascii_case(name)).map(|h| h.value)
        };
        let accepted = response.code == Some(101) &&
            has_token(response.headers, "upgrade", b"websocket") &&
            has_token(response.headers, "connection", b"upgrade") &&
            header("sec-websocket-accept") == Some(&accept[..]) &&
            header("sec-websocket-extensions").is_none();
        let Some(head) = head.filter(|_| accepted) else {
            self.ws.closing = true;
            self.buf.clear();
            net.disconnect(self.token);
            return false
        };
        self.buf.drain(..head);
        self.state = State::Open;
        handler(WsEvent::Open { token: self.token });
        true
    }
    fn parse_frames<F>(&mut self, net: &mut NetworkCore, max_message_bytes: usize, handler: &mut F)
    where
        F: for<'a> FnMut(WsEvent<'a>),
    {
        let held = self.buf.len();
        self.ws.parse(net, self.token, max_message_bytes, &mut self.buf, handler);
        release(&mut self.buf, held);
    }
}

/// The framing state of a connection.
struct Ws {
    /// Clients mask what they send; servers expect masked frames.
    client: bool,
    /// A close or a refusal went out: nothing more is sent or delivered.
    closing: bool,
    /// The data message being reassembled from fragments.
    fragmented: Option<WsKind>,
    message: Vec<u8>,
}

impl Ws {
    fn new(client: bool) -> Self {
        Self { client, closing: false, fragmented: None, message: Vec::new() }
    }

    fn send(&self, net: &mut NetworkCore, token: Token, kind: WsKind, payload: &[u8]) -> bool {
        let opcode = match kind {
            WsKind::Text => OP_TEXT,
            WsKind::Binary => OP_BINARY,
            WsKind::Ping => OP_PING,
            WsKind::Pong => OP_PONG,
        };
        if opcode >= OP_CLOSE && payload.len() > MAX_CONTROL {
            return false
        }
        !self.closing && self.send_frame(net, token, opcode, payload)
    }

    /// Sends a close frame and drops the connection once it is written;
    /// `false` if it was already closing.
    fn close(&mut self, net: &mut NetworkCore, token: Token, payload: &[u8]) -> bool {
        if self.closing {
            return false
        }
        self.send_frame(net, token, OP_CLOSE, payload);
        self.closing = true;
        net.disconnect_when_drained(token);
        true
    }

    fn close_with_code(&mut self, net: &mut NetworkCore, token: Token, code: u16) {
        self.close(net, token, &code.to_be_bytes());
    }

    fn send_frame(&self, net: &mut NetworkCore, token: Token, opcode: u8, payload: &[u8]) -> bool {
        let mut head = [0; MAX_FRAME_HEADER];
        if !self.client {
            let len = write_header(&mut head, opcode, payload.len(), None);
            return net.send_parts(token, &head[..len], payload)
        }
        // RFC 6455 §5.3: masks come from a strong source of entropy.
        let mask = rand::random();
        let len = write_header(&mut head, opcode, payload.len(), Some(mask));
        net.send_with(token, |out| {
            out.extend_from_slice(&head[..len]);
            let start = out.len();
            out.extend_from_slice(payload);
            apply_mask(&mut out.as_mut_slice()[start..], mask);
        })
    }

    /// Delivers and drains the messages in `buf`. Pings get one pong per drive,
    /// with the last payload.
    fn parse<F>(
        &mut self,
        net: &mut NetworkCore,
        token: Token,
        max_message_bytes: usize,
        buf: &mut Vec<u8>,
        handler: &mut F,
    ) where
        F: for<'a> FnMut(WsEvent<'a>),
    {
        let mut at = 0;
        let (mut pong, mut pong_len) = ([0; MAX_CONTROL], None);
        while !self.closing {
            let frame = match parse_header(&buf[at..]) {
                Ok(Some(frame)) if frame.mask.is_some() != self.client => frame,
                Ok(None) => break,
                _ => {
                    self.close_with_code(net, token, CLOSE_PROTOCOL_ERROR);
                    break
                }
            };
            let held = if frame.opcode == OP_CONTINUATION { self.message.len() } else { 0 };
            if frame.opcode < OP_CLOSE && frame.len > max_message_bytes.saturating_sub(held) {
                self.close_with_code(net, token, CLOSE_TOO_BIG);
                break
            }
            let start = at + frame.header_len;
            let Some(end) = start.checked_add(frame.len) else {
                self.close_with_code(net, token, CLOSE_TOO_BIG);
                break
            };
            if buf.len() < end {
                break
            }
            at = end;
            let payload = &mut buf[start..end];
            if let Some(mask) = frame.mask {
                apply_mask(payload, mask);
            }
            let kind = match frame.opcode {
                OP_PING => {
                    pong[..payload.len()].copy_from_slice(payload);
                    pong_len = Some(payload.len());
                    handler(WsEvent::Message { token, kind: WsKind::Ping, payload });
                    continue
                }
                OP_PONG => {
                    handler(WsEvent::Message { token, kind: WsKind::Pong, payload });
                    continue
                }
                OP_CLOSE => {
                    if let Some(code) = close_error(payload) {
                        self.close_with_code(net, token, code);
                    } else {
                        self.close(net, token, payload.get(..2).unwrap_or_default());
                        handler(WsEvent::Close { token, payload });
                    }
                    break
                }
                OP_TEXT => Some(WsKind::Text),
                OP_BINARY => Some(WsKind::Binary),
                _ => None,
            };
            let kind = match (kind, self.fragmented) {
                // Reject orphan continuations and interleaved data messages.
                (Some(_), Some(_)) | (None, None) => {
                    self.close_with_code(net, token, CLOSE_PROTOCOL_ERROR);
                    break
                }
                (Some(kind), None) if frame.fin => {
                    if kind == WsKind::Text && std::str::from_utf8(payload).is_err() {
                        self.close_with_code(net, token, CLOSE_INVALID_DATA);
                        break
                    }
                    handler(WsEvent::Message { token, kind, payload });
                    continue
                }
                (Some(kind), None) => {
                    self.fragmented = Some(kind);
                    self.message.clear();
                    self.message.extend_from_slice(payload);
                    continue
                }
                (None, Some(kind)) => {
                    self.message.extend_from_slice(payload);
                    if !frame.fin {
                        continue
                    }
                    self.fragmented = None;
                    kind
                }
            };
            if kind == WsKind::Text && std::str::from_utf8(&self.message).is_err() {
                self.close_with_code(net, token, CLOSE_INVALID_DATA);
                break
            }
            handler(WsEvent::Message { token, kind, payload: &self.message });
        }
        if self.closing {
            buf.clear();
        } else {
            if let Some(len) = pong_len {
                self.send_frame(net, token, OP_PONG, &pong[..len]);
            }
            buf.drain(..at);
        }
        if self.fragmented.is_none() {
            let held = self.message.len();
            self.message.clear();
            release(&mut self.message, held);
        }
    }
}

/// Frees an oversized empty buffer unless it just held a large message, so a
/// steady stream of them does not reallocate every drive.
fn release(buf: &mut Vec<u8>, held: usize) {
    if buf.is_empty() && held <= KEEP_BUF_BYTES / 2 && buf.capacity() > KEEP_BUF_BYTES {
        *buf = Vec::new();
    }
}

struct Frame {
    fin: bool,
    opcode: u8,
    mask: Option<[u8; 4]>,
    header_len: usize,
    len: usize,
}

/// `Ok(None)` until the header is buffered; `Err` for one no peer may send.
fn parse_header(buf: &[u8]) -> Result<Option<Frame>, ()> {
    let [b0, b1, ..] = *buf else { return Ok(None) };
    let (fin, opcode) = (b0 & 0x80 != 0, b0 & 0x0F);
    let (len, mut at) = match b1 & 0x7F {
        126 => match buf.get(2..4) {
            Some(bytes) => (u64::from(u16::from_be_bytes([bytes[0], bytes[1]])), 4),
            None => return Ok(None),
        },
        127 => match buf.get(2..10) {
            Some(bytes) => (u64::from_be_bytes(bytes.try_into().unwrap()), 10),
            None => return Ok(None),
        },
        len => (u64::from(len), 2),
    };
    let control = opcode & 0x08 != 0;
    let known =
        matches!(opcode, OP_CONTINUATION | OP_TEXT | OP_BINARY | OP_CLOSE | OP_PING | OP_PONG);
    if b0 & 0x70 != 0 || !known || len >> 63 != 0 || (control && (!fin || len > MAX_CONTROL as u64))
    {
        return Err(())
    }
    let mask = if b1 & 0x80 != 0 {
        let Some(bytes) = buf.get(at..at + 4) else { return Ok(None) };
        at += 4;
        Some(bytes.try_into().unwrap())
    } else {
        None
    };
    let len = usize::try_from(len).map_err(|_| ())?;
    Ok(Some(Frame { fin, opcode, mask, header_len: at, len }))
}

/// A final frame's header; returns its length.
fn write_header(
    out: &mut [u8; MAX_FRAME_HEADER],
    opcode: u8,
    len: usize,
    mask: Option<[u8; 4]>,
) -> usize {
    out[0] = 0x80 | opcode;
    let mask_bit = if mask.is_some() { 0x80 } else { 0 };
    let mut at = if len < 126 {
        out[1] = mask_bit | len as u8;
        2
    } else if let Ok(len) = u16::try_from(len) {
        out[1] = mask_bit | 126;
        out[2..4].copy_from_slice(&len.to_be_bytes());
        4
    } else {
        out[1] = mask_bit | 127;
        out[2..10].copy_from_slice(&(len as u64).to_be_bytes());
        10
    };
    if let Some(mask) = mask {
        out[at..at + 4].copy_from_slice(&mask);
        at += 4;
    }
    at
}

fn apply_mask(data: &mut [u8], mask: [u8; 4]) {
    let wide = u64::from_ne_bytes([
        mask[0], mask[1], mask[2], mask[3], mask[0], mask[1], mask[2], mask[3],
    ]);
    let mut chunks = data.chunks_exact_mut(8);
    for chunk in &mut chunks {
        let value = u64::from_ne_bytes(chunk.try_into().unwrap()) ^ wide;
        chunk.copy_from_slice(&value.to_ne_bytes());
    }
    for (i, byte) in chunks.into_remainder().iter_mut().enumerate() {
        *byte ^= mask[i % 4];
    }
}

/// `Sec-WebSocket-Accept` for a `Sec-WebSocket-Key`.
fn accept_key(key: &[u8]) -> [u8; 28] {
    let digest = Sha1::new()
        .chain_update(key)
        .chain_update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
        .finalize();
    let mut out = [0; 28];
    let _ = STANDARD.encode_slice(digest, &mut out);
    out
}

/// A client handshake up to its `Sec-WebSocket-Key` value.
fn handshake(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Vec<u8> {
    assert!(
        !path.is_empty() && !path.contains(['\r', '\n', ' ']) && valid_headers(headers),
        "invalid path or header"
    );
    let mut out = Vec::new();
    write!(out, "GET {path} HTTP/1.1\r\n").unwrap();
    for (name, value) in headers {
        write!(out, "{name}: {value}\r\n").unwrap();
    }
    if !headers.iter().any(|(name, _)| name.eq_ignore_ascii_case("host")) {
        write!(out, "Host: {addr}\r\n").unwrap();
    }
    out.extend_from_slice(
        b"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ",
    );
    out
}

/// The code refusing a close payload with a bad code or reason.
fn close_error(payload: &[u8]) -> Option<u16> {
    match payload {
        [] => None,
        [_] => Some(CLOSE_PROTOCOL_ERROR),
        [a, b, ..] if !valid_close_code(u16::from_be_bytes([*a, *b])) => Some(CLOSE_PROTOCOL_ERROR),
        [_, _, reason @ ..] => std::str::from_utf8(reason).is_err().then_some(CLOSE_INVALID_DATA),
    }
}

/// Codes an endpoint may send, as RFC 6455 §7.4 assigns them.
fn valid_close_code(code: u16) -> bool {
    matches!(code, 1000..=1003 | 1007..=1013 | 3000..=4999)
}

fn valid_headers(headers: &[(&str, &str)]) -> bool {
    headers.iter().all(|(name, value)| valid_token(name) && !value.contains(['\r', '\n']))
}
