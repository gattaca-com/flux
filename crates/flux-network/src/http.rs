//! Poll-driven HTTP over a caller-owned [`crate::NetworkCore`],
//! sharing one poll with the tile's other traffic.
//!
//! [`HttpNetwork`] can listen for requests and maintain outbound endpoints in
//! one event loop: [`HttpNetwork::on_event`] claims its group's events from
//! the network's handler and [`HttpNetwork::drive`] delivers them and sends
//! queued requests. Events borrow parsed data only for the callback duration.
//!
//! ```no_run
//! use std::net::SocketAddr;
//! use flux_network::{http::{HttpEvent, HttpNetwork}, Network};
//! let mut net = Network::default();
//! let mut http = HttpNetwork::default();
//! http.listen(&mut net, "127.0.0.1:8080".parse::<SocketAddr>().unwrap())?;
//! let peer = http.connect(&mut net, "127.0.0.1:8081".parse::<SocketAddr>().unwrap());
//! loop {
//!     let mut response = None;
//!     let mut request = false;
//!     net.poll_with(|event| {
//!         http.on_event(&event);
//!     });
//!     http.drive(&mut net, |event| match event {
//!         HttpEvent::Request { token, .. } => response = Some(token),
//!         HttpEvent::Connected { token } if token == peer => request = true,
//!         _ => {}
//!     });
//!     if let Some(token) = response { http.respond(&mut net, token, 200, &[], b"hello"); }
//!     if request { http.request(&mut net, peer, "GET", "/", &[], &[]); }
//! }
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! # Limitations
//! HTTP/1.1 and HTTP/1.0 responses are supported. Request bodies may use one
//! `Content-Length` or, on HTTP/1.1, `Transfer-Encoding: chunked`; any other
//! or ambiguous framing is answered `400` and closes the connection. Chunked
//! bodies are decoded strictly and incrementally: chunk sizes are checked
//! against `max_body_bytes` before their data, size lines and trailers are
//! bounded, the raw bytes beneath the decoder are capped, and trailers are
//! validated and dropped. Response bodies may use `Content-Length`, chunked
//! transfer coding, or EOF delimiting. `Expect: 100-continue` is handled
//! automatically.
//!
//! [`HttpNetwork::with_request_heads`] delivers request heads before their
//! bodies, reads a body only when asked, pauses reading connections whose
//! input is not wanted (backpressure through the kernel's receive window),
//! and answers clients that have half-closed.
//!
//! Server-side TLS, HTTP/2, compression, trailer exposure, upgrades, and
//! `WebSockets` are not supported. Valid response and request trailers are
//! parsed and discarded. Half-close is supported only in request-head mode.
//! After an error response, the connection closes without a lingering-close
//! delay. Pipelined requests are served strictly one at a time per
//! connection.

#[cfg(feature = "tls")]
use std::sync::Arc;
use std::{
    collections::VecDeque,
    io::{self, IoSlice},
    net::{IpAddr, SocketAddr},
};

use flux_timing::{Duration, Instant};
use mio::Token;
use rustc_hash::FxHashMap;

use crate::{Framing, Group, NetworkCore, NetworkEvent, ReplayPolicy, TcpGroupConfig};

/// Header slots parsed on the stack; a larger `max_headers` falls back to the
/// heap.
const STACK_HEADERS: usize = 64;
/// Request head buffers kept for reuse once their request completes.
const SPARE_HEADS: usize = 64;
const MAX_BODY_PARTS: usize = 16;
/// Longest chunk-size line, extensions included. The size itself is at most
/// 16 hex digits; real clients send no extensions.
const MAX_CHUNK_LINE: usize = 4096;
/// Per-request buffers larger than this are released when their request
/// completes, so an idle connection does not keep its largest request's
/// memory.
const RETAINED_BUFFER_BYTES: usize = 64 * 1024;
/// Interval between idle sweeps of accepted connections.
const SWEEP_MILLIS: u64 = 20;
/// Piece of an owned response body handed to the socket at a time; more is
/// handed over only while less than this is queued there.
pub const OUT_CHUNK_BYTES: usize = 256 * 1024;

/// Record overhead allowance on top of a full-size request; see `group`.
const TLS_MARGIN_BYTES: usize = 64 * 1024;

pub enum HttpEvent<'a> {
    Accepted {
        token: Token,
        peer_addr: SocketAddr,
    },
    Connected {
        token: Token,
    },
    Response {
        token: Token,
        id: Option<RequestId>,
        response: HttpResponse<'a>,
    },
    Request {
        token: Token,
        request: HttpRequest<'a>,
    },
    /// With [`HttpNetwork::with_request_heads`], the head of a request that
    /// has a body, before any of the body is read. Answer it with
    /// [`HttpNetwork::respond`], or ask for the body with
    /// [`HttpNetwork::read_body`]. `request.body` is empty.
    RequestHead {
        token: Token,
        request: HttpRequest<'a>,
        body: RequestBody,
    },
    /// The whole body asked for with [`HttpNetwork::read_body`]; the request
    /// now awaits [`HttpNetwork::respond`].
    RequestBody {
        token: Token,
        body: &'a [u8],
    },
    /// The body asked for could not be read. The request still awaits
    /// [`HttpNetwork::respond`], after which the connection closes.
    RequestBodyFailed {
        token: Token,
        error: BodyError,
    },
    Disconnected {
        token: Token,
    },
    Failed {
        id: RequestId,
        reason: Failure,
    },
    /// The head of a streaming response; see [`HttpNetwork::request_stream`].
    ResponseHead {
        token: Token,
        status: u16,
        headers: &'a [httparse::Header<'a>],
    },
    /// One piece of a streaming body, borrowed from the receive buffer.
    Body {
        token: Token,
        chunk: &'a [u8],
    },
    StreamEnd {
        token: Token,
        reason: StreamEnd,
    },
}

/// How the body of a request delivered as [`HttpEvent::RequestHead`] is
/// framed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestBody {
    /// A `Content-Length` body of this many bytes, possibly more than
    /// `max_body_bytes` (up to `max_content_length`): such a body fails with
    /// [`BodyError::TooLarge`] if read.
    Length(usize),
    /// A chunked body of unknown length.
    Chunked,
}

/// Why a streaming response stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamEnd {
    /// The body reached its terminal chunk or its `Content-Length`.
    Complete,
    /// The connection went away first; bytes may be missing.
    Disconnected,
}

impl<'a> HttpEvent<'a> {
    /// The outcome this event carries for `pool`, if it finishes one of its
    /// requests. Clients over a pool dispatch on this instead of matching the
    /// response and failure variants themselves.
    pub fn outcome(
        &self,
        pool: HttpPool,
    ) -> Option<(RequestId, Result<&HttpResponse<'a>, Failure>)> {
        match self {
            Self::Response { id: Some(id), response, .. } if id.pool() == pool => {
                Some((*id, Ok(response)))
            }
            Self::Failed { id, reason } if id.pool() == pool => Some((*id, Err(*reason))),
            _ => None,
        }
    }
}
/// Persistent connections to one address sharing a request queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HttpPool(u32);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId {
    pool: HttpPool,
    seq: u64,
}
impl RequestId {
    pub fn pool(self) -> HttpPool {
        self.pool
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Disconnected,
    TimedOut,
}
struct Queued {
    id: RequestId,
    /// Whether the request was a `HEAD`, whose response carries no body.
    head_request: bool,
    head: Vec<u8>,
    body: Vec<u8>,
    retries: u8,
}
struct InFlight {
    queued: Queued,
    sent_at: Instant,
}
struct Pool {
    addr: SocketAddr,
    tokens: Vec<Token>,
    queue: VecDeque<Queued>,
    queued_bytes: usize,
    next_seq: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    /// Waiting for a request head.
    Idle,
    /// A request head was delivered and its body not asked for yet.
    Head,
    /// Receiving the body of a request whose head has been parsed.
    Reading,
    /// The request was delivered; waiting for [`HttpNetwork::respond`].
    Pending,
    /// An owned response body is being handed to the socket.
    Writing,
    /// Closing once the last response is written.
    Draining,
}
/// An accepted connection's request in progress.
// Independent request properties, not states of one value.
#[allow(clippy::struct_excessive_bools)]
struct Accepted {
    state: State,
    close: bool,
    continued: bool,
    head_request: bool,
    /// Whether the request asked for `100 Continue` before sending its body.
    expect: bool,
    /// The head of the request whose body is being read, kept until the
    /// whole request is delivered.
    head: Vec<u8>,
    /// What remains of the body being read.
    body: Option<BodyReader>,
    /// The decoded chunked body read so far.
    decoded: Vec<u8>,
    /// The body in `body` belongs to a request already answered early: it is
    /// read and dropped, so the connection can serve its next request.
    discard: bool,
    /// When a discard that has not finished closes the connection.
    discard_deadline: Option<Instant>,
    /// Whether reading the socket is paused (request-head mode only).
    paused: bool,
    /// The peer shut down its sending side (request-head mode only).
    peer_closed: bool,
    /// Bytes queued at the socket when last checked, to notice a peer still
    /// reading its response.
    last_queued: usize,
    /// The rest of an owned response body.
    out: Option<OutBody>,
}
/// An owned response body being handed to the socket.
struct OutBody {
    body: Vec<u8>,
    /// Bytes handed over so far.
    sent: usize,
    /// Whether the connection closes after it.
    close: bool,
}
enum ResponseBody<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}
impl ResponseBody<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Borrowed(body) => body.len(),
            Self::Owned(body) => body.len(),
        }
    }
}
impl Accepted {
    fn new() -> Self {
        Self {
            state: State::Idle,
            close: false,
            continued: false,
            head_request: false,
            expect: false,
            head: Vec::new(),
            body: None,
            decoded: Vec::new(),
            discard: false,
            discard_deadline: None,
            paused: false,
            peer_closed: false,
            last_queued: 0,
            out: None,
        }
    }
}
/// The body of a request being read.
struct BodyReader {
    framing: BodyFraming,
    /// Bytes consumed from the connection for this body, framing included.
    raw: usize,
}
enum BodyFraming {
    /// Bytes still owed by `Content-Length`.
    Length(usize),
    Chunked(ChunkDecoder),
}
enum Role {
    Accepted(Box<Accepted>),
    /// `in_flight_head` is `Some` while a request is awaiting its response,
    /// carrying whether that request was a `HEAD`.
    Outbound {
        addr: SocketAddr,
        in_flight_head: Option<bool>,
        in_flight: Option<InFlight>,
    },
}
struct Conn {
    token: Token,
    buf: Vec<u8>,
    dirty: bool,
    over_limit: bool,
    last_activity: Instant,
    role: Role,
    /// Set while a streaming request owns this connection.
    stream: Option<Stream>,
}

/// A streaming request; `body` is set once its head has been delivered.
struct Stream {
    body: Option<StreamBody>,
}

/// How the rest of a streaming body is framed.
#[derive(Clone, Copy)]
enum StreamBody {
    Chunked,
    /// Bytes still owed by `Content-Length`.
    Length(usize),
    /// Everything until the connection closes.
    Eof,
}
#[derive(Clone, Copy)]
enum Lifecycle {
    Connected(Token, Option<SocketAddr>),
    Disconnected(Token),
}
pub struct HttpNetwork {
    group: Option<Group>,
    name: &'static str,
    max_head_bytes: usize,
    max_body_bytes: usize,
    max_headers: usize,
    /// Declared request lengths above this are invalid framing (`400`).
    max_content_length: usize,
    /// Status for a request head over `max_head_bytes`.
    oversized_head_status: u16,
    /// Deliver request heads before bodies; see `with_request_heads`.
    request_heads: bool,
    /// Largest unread body dropped to keep a connection after an early
    /// response, and how long that may take.
    max_drain_bytes: Option<usize>,
    drain_timeout: Duration,
    idle_timeout: Option<Duration>,
    socket_buf_size: Option<usize>,
    listeners: Vec<Token>,
    conns: Vec<Conn>,
    /// Index of each connection in `conns`.
    index: FxHashMap<Token, usize>,
    /// When idle connections were last swept.
    swept: Instant,
    lifecycle: Vec<Lifecycle>,
    pools: Vec<Pool>,
    max_queued_bytes: usize,
    request_timeout: Option<Duration>,
    stream_idle_timeout: Option<Duration>,
    reconnect_interval: Option<Duration>,
    #[cfg(feature = "tls")]
    tls_config: Option<Arc<crate::tls::ClientConfig>>,
    failed: Vec<(RequestId, Failure)>,
    spare_heads: Vec<Vec<u8>>,
    /// Scratch for the head of an owned response.
    response_head: Vec<u8>,
    /// Decoded body of the chunked response being delivered.
    chunked_body: Vec<u8>,
}
impl Default for HttpNetwork {
    fn default() -> Self {
        Self {
            group: None,
            name: "http",
            max_head_bytes: 16 * 1024,
            max_body_bytes: 1024 * 1024,
            max_headers: 64,
            max_content_length: usize::MAX,
            oversized_head_status: 431,
            request_heads: false,
            max_drain_bytes: None,
            drain_timeout: Duration::from_secs(30),
            idle_timeout: Some(Duration::from_secs(30)),
            socket_buf_size: None,
            listeners: Vec::new(),
            conns: Vec::new(),
            index: FxHashMap::default(),
            swept: Instant::now(),
            lifecycle: Vec::new(),
            pools: Vec::new(),
            max_queued_bytes: usize::MAX,
            request_timeout: None,
            stream_idle_timeout: None,
            reconnect_interval: None,
            #[cfg(feature = "tls")]
            tls_config: None,
            failed: Vec::new(),
            spare_heads: Vec::new(),
            response_head: Vec::new(),
            chunked_body: Vec::new(),
        }
    }
}
impl HttpNetwork {
    /// Sets the TCP group name.
    pub fn with_name(mut self, name: &'static str) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.name = name;
        self
    }
    /// Sets the maximum message head size before rejecting it.
    pub fn with_max_head_bytes(mut self, max_head_bytes: usize) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.max_head_bytes = max_head_bytes;
        self
    }
    /// Sets the maximum message body size before rejecting it.
    pub fn with_max_body_bytes(mut self, max_body_bytes: usize) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.max_body_bytes = max_body_bytes;
        self
    }
    /// Sets the maximum number of request headers accepted.
    pub fn with_max_headers(mut self, max_headers: usize) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.max_headers = max_headers;
        self
    }
    /// Treats a request whose declared `Content-Length` exceeds `max` as
    /// invalid framing: it is answered `400` and the connection closes,
    /// whatever `max_body_bytes` would have said. Without it, a length above
    /// `max_body_bytes` is answered `413`.
    pub fn with_max_content_length(mut self, max: usize) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.max_content_length = max;
        self
    }
    /// Sets the status for a request head larger than `max_head_bytes`;
    /// `431` by default. Some deployments answer every malformed head `400`.
    pub fn with_oversized_head_status(mut self, status: u16) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        assert!((400..=499).contains(&status), "oversized head status must be a 4xx");
        self.oversized_head_status = status;
        self
    }
    /// Delivers each request with a body as [`HttpEvent::RequestHead`] once
    /// its head is parsed, before the body is read, so the application can
    /// answer it (for example `401`) without receiving the body, or ask for
    /// the body with [`Self::read_body`]. Requests without a body still
    /// arrive as [`HttpEvent::Request`].
    ///
    /// In this mode reading a connection is paused whenever the server is
    /// not waiting for its input (a request is being handled, or its body
    /// has not been asked for), so the kernel's receive window, not a
    /// userspace buffer, holds back a peer that sends ahead. `100 Continue`
    /// is sent only when the body is asked for. A peer that shuts down its
    /// sending side can still be answered.
    ///
    /// Answering before the body is read drops the body to keep the
    /// connection when that is safe and bounded (see
    /// [`Self::with_max_drain_bytes`]) and closes it otherwise. Body reads
    /// have no deadline here: the application bounds them, for example with
    /// [`Self::body_received`].
    pub fn with_request_heads(mut self) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.request_heads = true;
        self
    }
    /// Largest unread body that is read and dropped after an early response
    /// to keep the connection; a larger one closes it. Defaults to
    /// `max_body_bytes`. Applies with [`Self::with_request_heads`].
    pub fn with_max_drain_bytes(mut self, max_drain_bytes: usize) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.max_drain_bytes = Some(max_drain_bytes);
        self
    }
    /// How long dropping an unread body may take before the connection is
    /// closed instead; 30 seconds by default.
    pub fn with_drain_timeout(mut self, drain_timeout: Duration) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.drain_timeout = drain_timeout;
        self
    }
    /// Sets the TCP socket buffer size.
    pub fn with_socket_buf_size(mut self, socket_buf_size: usize) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.socket_buf_size = Some(socket_buf_size);
        self
    }
    /// Sets the idle timeout for accepted connections: one that is waiting
    /// for a request, or closing, and has neither received bytes nor had its
    /// response read for this long is closed. Connections whose request the
    /// application holds are exempt. Outbound endpoints remain persistent.
    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.idle_timeout = Some(idle_timeout);
        self
    }
    /// Disables the idle connection sweep.
    pub fn without_idle_timeout(mut self) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.idle_timeout = None;
        self
    }
    pub fn with_max_queued_bytes(mut self, max_queued_bytes: usize) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.max_queued_bytes = max_queued_bytes;
        self
    }
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.request_timeout = Some(request_timeout);
        self
    }
    /// Drops a streaming response that has gone this long without a byte.
    /// `request_timeout` stops applying once its head arrives, since a stream
    /// is expected to outlive any single request deadline.
    pub fn with_stream_idle_timeout(mut self, stream_idle_timeout: Duration) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.stream_idle_timeout = Some(stream_idle_timeout);
        self
    }
    /// Retry interval for the outbound endpoints this layer opens.
    pub fn with_reconnect_interval(mut self, reconnect_interval: Duration) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.reconnect_interval = Some(reconnect_interval);
        self
    }
    /// Trusts `config` for TLS endpoints instead of the default Mozilla
    /// roots. Applies to connections opened after the call.
    #[cfg(feature = "tls")]
    pub fn with_tls_config(mut self, config: Arc<crate::tls::ClientConfig>) -> Self {
        self.tls_config = Some(config);
        self
    }
    pub fn max_body_bytes(&self) -> usize {
        self.max_body_bytes
    }
    fn group(&mut self, net: &mut NetworkCore) -> Group {
        let Self {
            group,
            name,
            max_head_bytes,
            max_body_bytes,
            socket_buf_size,
            reconnect_interval,
            request_heads,
            ..
        } = self;
        *group.get_or_insert_with(|| {
            let defaults = TcpGroupConfig::default();
            net.add_group(TcpGroupConfig {
                name,
                framing: Framing::Raw,
                replay: ReplayPolicy::Drop,
                socket_buf_size: *socket_buf_size,
                reconnect_interval: reconnect_interval.unwrap_or(defaults.reconnect_interval),
                max_frame_size: usize::MAX,
                // The backlog counts wire bytes, which are ciphertext on a
                // TLS endpoint: a record adds a header and a tag per 16KB.
                // The margin keeps a full-size request from tripping the cap
                // on its own overhead.
                max_backlog_bytes: Some(
                    max_head_bytes.saturating_add(*max_body_bytes).saturating_add(TLS_MARGIN_BYTES),
                ),
                backlog_warn_bytes: None,
                half_close: *request_heads,
                ..Default::default()
            })
        })
    }
    pub fn listen(&mut self, net: &mut NetworkCore, addr: SocketAddr) -> io::Result<()> {
        let group = self.group(net);
        self.listeners.push(net.listen(group, addr)?);
        Ok(())
    }
    fn position(&self, token: Token) -> Option<usize> {
        self.index.get(&token).copied()
    }
    fn push_conn(&mut self, conn: Conn) {
        self.index.insert(conn.token, self.conns.len());
        self.conns.push(conn);
    }
    fn remove_conn(&mut self, i: usize) {
        let removed = self.conns.swap_remove(i);
        self.index.remove(&removed.token);
        if let Some(moved) = self.conns.get(i) {
            self.index.insert(moved.token, i);
        }
    }
    /// The address of the first listener, for example to learn the port
    /// chosen for a listener bound to port 0.
    pub fn local_addr(&self, net: &NetworkCore) -> Option<SocketAddr> {
        self.listeners.first().and_then(|token| net.listener_addr(*token))
    }
    /// Immediately disconnects an accepted client.
    pub fn disconnect(&mut self, net: &mut NetworkCore, token: Token) -> bool {
        self.group.is_some() &&
            self.position(token).is_some_and(|i| matches!(self.conns[i].role, Role::Accepted(_))) &&
            net.disconnect(token)
    }
    /// Takes in one network event; returns whether it belonged to this
    /// layer's group. Call from the network's `poll_with` handler.
    pub fn on_event(&mut self, event: &NetworkEvent<'_>) -> bool {
        let Some(group) = self.group else { return false };
        match *event {
            NetworkEvent::Accepted { group: event_group, token, peer_addr }
                if event_group == group =>
            {
                self.push_conn(Conn {
                    token,
                    buf: Vec::new(),
                    dirty: false,
                    over_limit: false,
                    last_activity: Instant::now(),
                    role: Role::Accepted(Box::new(Accepted::new())),
                    stream: None,
                });
                self.lifecycle.push(Lifecycle::Connected(token, Some(peer_addr)));
            }
            NetworkEvent::Connected { group: event_group, token, .. } if event_group == group => {
                self.lifecycle.push(Lifecycle::Connected(token, None));
            }
            NetworkEvent::Message { group: event_group, token, payload, .. }
                if event_group == group =>
            {
                let limit = self.buffer_limit();
                if payload.is_empty() {
                    // The peer finished sending (half-close, request-head
                    // mode only).
                    if let Some(conn) = self.position(token).map(|i| &mut self.conns[i]) &&
                        let Some(accepted) = conn.role.accepted_mut()
                    {
                        accepted.peer_closed = true;
                        conn.dirty = true;
                    }
                } else if let Some(conn) = self.position(token).map(|i| &mut self.conns[i]) &&
                    !conn.role.is_draining() &&
                    !conn.over_limit
                {
                    let available = limit.saturating_sub(conn.buf.len());
                    if payload.len() > available {
                        conn.buf.extend_from_slice(&payload[..available]);
                        conn.over_limit = true;
                    } else {
                        conn.buf.extend_from_slice(payload);
                    }
                    conn.dirty = true;
                    conn.last_activity = Instant::now();
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
    /// Delivers the events taken in since the last call, sweeps idle and
    /// timed-out connections, and sends queued requests on idle pooled
    /// connections. Call once per poll, after the network's `poll_with`.
    ///
    /// A request event remains pending until [`Self::respond`] is called. The
    /// handler may defer that call until a later drive; requests behind it
    /// stay buffered until the response is sent.
    pub fn drive<F>(&mut self, net: &mut NetworkCore, mut handler: F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        for event in std::mem::take(&mut self.lifecycle) {
            self.emit_lifecycle(net, event, &mut handler);
        }
        self.parse_dirty(net, &mut handler);
        for i in 0..self.conns.len() {
            if self.conns[i].role.accepted().is_some_and(|a| a.out.is_some()) {
                self.pump(net, i);
            }
        }
        for conn in &mut self.conns {
            if conn.over_limit && !conn.role.is_draining() {
                net.disconnect(conn.token);
                conn.over_limit = false;
            }
        }
        self.sweep_accepted(net);
        if let Some(timeout) = self.stream_idle_timeout {
            for i in 0..self.conns.len() {
                if self.stream_body(i).is_some() && self.conns[i].last_activity.elapsed() >= timeout
                {
                    net.disconnect(self.conns[i].token);
                }
            }
        }
        if let Some(timeout) = self.request_timeout {
            for i in 0..self.conns.len() {
                // A stream outlives request deadlines once its head lands.
                if self.stream_body(i).is_some() {
                    continue
                }
                let Role::Outbound { in_flight: Some(in_flight), .. } = &self.conns[i].role else {
                    continue
                };
                if in_flight.sent_at.elapsed() < timeout {
                    continue
                }
                if let Some(in_flight) = self.conns[i].role.take_in_flight() {
                    self.failed.push((in_flight.queued.id, Failure::TimedOut));
                    self.recycle(in_flight.queued);
                }
                self.conns[i].role.set_outbound_head(None);
                net.disconnect(self.conns[i].token);
            }
        }
        for (id, reason) in std::mem::take(&mut self.failed) {
            handler(HttpEvent::Failed { id, reason });
        }
        self.dispatch(net);
        if self.request_heads {
            self.update_reading(net);
        }
    }
    /// Whether [`Self::drive`] has work without new network events, for
    /// example after [`Self::read_body`] found the body already buffered or a
    /// response freed a connection with a pipelined request. An event loop
    /// that blocks in its poll should drive again first.
    pub fn needs_drive(&self) -> bool {
        !self.lifecycle.is_empty() ||
            self.conns.iter().any(|conn| {
                conn.dirty &&
                    (matches!(conn.role, Role::Accepted(_)) ||
                        conn.stream.is_some() ||
                        conn.role.outbound_head().is_some())
            })
    }
    /// Asks for the body of a request delivered as
    /// [`HttpEvent::RequestHead`]. Sends `100 Continue` if the client asked
    /// for it; the body arrives as [`HttpEvent::RequestBody`] or
    /// [`HttpEvent::RequestBodyFailed`] from a later drive. Returns `false`
    /// unless `token` has a request head awaiting this call.
    pub fn read_body(&mut self, net: &mut NetworkCore, token: Token) -> bool {
        let Some(i) = self
            .position(token)
            .filter(|&i| self.conns[i].role.accepted().is_some_and(|a| a.state == State::Head))
        else {
            return false
        };
        let Some(accepted) = self.conns[i].role.accepted_mut() else { return false };
        accepted.state = State::Reading;
        if accepted.expect && !accepted.continued {
            self.send_continue(net, i);
        }
        self.conns[i].dirty = true;
        // Resumed now, not at the next drive: the body may still be in the
        // kernel, and nothing else would wake the poll.
        sync_reading(net, &mut self.conns[i]);
        true
    }
    /// Raw bytes of the body being read for `token` that have arrived so
    /// far, framing included, while [`Self::read_body`] is in progress. An
    /// application can compare successive values to bound how slowly a body
    /// may arrive.
    pub fn body_received(&self, token: Token) -> Option<usize> {
        let conn = &self.conns[self.position(token)?];
        let accepted = conn.role.accepted()?;
        if accepted.state != State::Reading {
            return None
        }
        let reader = accepted.body.as_ref()?;
        match &reader.framing {
            BodyFraming::Length(length) => Some(conn.buf.len().min(*length)),
            BodyFraming::Chunked(_) => Some(reader.raw + conn.buf.len()),
        }
    }
    /// Closes connections that sat idle too long, and unfinished discards.
    /// Runs at most every `SWEEP_MILLIS`, so a drive stays cheap with many
    /// idle connections.
    fn sweep_accepted(&mut self, net: &mut NetworkCore) {
        let now = Instant::now();
        if now.saturating_sub(self.swept) < Duration::from_millis(SWEEP_MILLIS) {
            return
        }
        self.swept = now;
        for conn in &mut self.conns {
            let token = conn.token;
            let Role::Accepted(accepted) = &mut conn.role else { continue };
            // A peer reading its response is not idle.
            let queued = net.queued_bytes(token);
            if queued < accepted.last_queued {
                conn.last_activity = now;
            }
            accepted.last_queued = queued;
            let idle = matches!(accepted.state, State::Idle | State::Writing | State::Draining) &&
                self.idle_timeout.is_some_and(|timeout| conn.last_activity.elapsed() >= timeout);
            let overdue = accepted.discard &&
                accepted.discard_deadline.is_some_and(|deadline| now >= deadline);
            if idle || overdue {
                net.disconnect(token);
            }
        }
    }
    /// Reads only connections whose input the server is waiting for.
    fn update_reading(&mut self, net: &mut NetworkCore) {
        for conn in &mut self.conns {
            sync_reading(net, conn);
        }
    }
    fn emit_lifecycle<F>(&mut self, net: &mut NetworkCore, event: Lifecycle, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        match event {
            Lifecycle::Connected(token, Some(peer_addr)) => {
                handler(HttpEvent::Accepted { token, peer_addr });
            }
            Lifecycle::Connected(token, None) => handler(HttpEvent::Connected { token }),
            Lifecycle::Disconnected(token) => {
                if let Some(i) = self.position(token) {
                    if self.conns[i].dirty {
                        self.parse_connection(net, i, handler);
                    }
                    if self.conns[i].stream.is_some() {
                        self.conns[i].buf.clear();
                        self.conns[i].dirty = false;
                        self.end_stream(i, StreamEnd::Disconnected, handler);
                    } else if matches!(self.conns[i].role, Role::Outbound { .. }) {
                        let answered = self.parse_eof_outbound(i, handler);
                        self.conns[i].buf.clear();
                        self.conns[i].dirty = false;
                        self.conns[i].role.set_outbound_head(None);
                        if let Some(in_flight) = self.conns[i].role.take_in_flight() {
                            if answered {
                                self.recycle(in_flight.queued);
                            } else {
                                self.requeue_or_fail(in_flight.queued);
                            }
                        }
                    } else {
                        self.remove_conn(i);
                    }
                }
                handler(HttpEvent::Disconnected { token });
            }
        }
    }
    fn parse_dirty<F>(&mut self, net: &mut NetworkCore, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        for i in 0..self.conns.len() {
            if self.conns[i].dirty {
                self.parse_connection(net, i, handler);
            }
        }
    }
    fn parse_connection<F>(&mut self, net: &mut NetworkCore, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        if matches!(self.conns[i].role, Role::Accepted(_)) {
            self.parse_and_emit(net, i, handler);
        } else if self.conns[i].stream.is_some() {
            self.parse_stream(net, i, handler);
        } else {
            self.parse_outbound(net, i, handler);
        }
    }
    pub fn connect(&mut self, net: &mut NetworkCore, addr: SocketAddr) -> Token {
        let group = self.group(net);
        let token = net.connect(group, addr);
        self.track_outbound(token, addr)
    }
    fn connect_from(&mut self, net: &mut NetworkCore, addr: SocketAddr, local_ip: IpAddr) -> Token {
        let group = self.group(net);
        let token = net.connect_from(group, addr, local_ip);
        self.track_outbound(token, addr)
    }
    /// Like [`Self::connect`] but negotiates TLS once the TCP connect
    /// completes, verifying against the Mozilla roots and sending SNI
    /// `server`. Panics if `server` is not a valid DNS name or IP.
    #[cfg(feature = "tls")]
    pub fn connect_tls(&mut self, net: &mut NetworkCore, addr: SocketAddr, server: &str) -> Token {
        self.connect_tls_with(net, addr, None, server)
    }
    #[cfg(feature = "tls")]
    fn connect_tls_with(
        &mut self,
        net: &mut NetworkCore,
        addr: SocketAddr,
        local_ip: Option<IpAddr>,
        server: &str,
    ) -> Token {
        let session = crate::tls::Session::new(server);
        let session = match &self.tls_config {
            Some(config) => session.with_config(config.clone()),
            None => session,
        };
        let group = self.group(net);
        let token = match local_ip {
            Some(local_ip) => net.connect_tls_from(group, addr, local_ip, session),
            None => net.connect_tls(group, addr, session),
        };
        self.track_outbound(token, addr)
    }
    fn track_outbound(&mut self, token: Token, addr: SocketAddr) -> Token {
        self.push_conn(Conn {
            token,
            buf: Vec::new(),
            dirty: false,
            over_limit: false,
            last_activity: Instant::now(),
            role: Role::Outbound { addr, in_flight_head: None, in_flight: None },
            stream: None,
        });
        token
    }
    /// Opens `connections` persistent connections to `addr` that share one
    /// request queue; see [`Self::send`].
    pub fn pool(
        &mut self,
        net: &mut NetworkCore,
        addr: SocketAddr,
        connections: usize,
    ) -> HttpPool {
        self.pool_with(net, addr, connections, |http, net| http.connect(net, addr))
    }
    /// Like [`Self::pool`] but negotiates TLS on every connection; see
    /// [`Self::connect_tls`].
    #[cfg(feature = "tls")]
    pub fn pool_tls(
        &mut self,
        net: &mut NetworkCore,
        addr: SocketAddr,
        server: &str,
        connections: usize,
    ) -> HttpPool {
        self.pool_with(net, addr, connections, |http, net| http.connect_tls(net, addr, server))
    }
    /// Like [`Self::pool`] but every connection binds `local_ip` as its
    /// source address, across reconnects too.
    pub fn pool_from(
        &mut self,
        net: &mut NetworkCore,
        addr: SocketAddr,
        local_ip: IpAddr,
        connections: usize,
    ) -> HttpPool {
        self.pool_with(net, addr, connections, |http, net| http.connect_from(net, addr, local_ip))
    }
    /// [`Self::pool_tls`] from `local_ip`; see [`Self::pool_from`].
    #[cfg(feature = "tls")]
    pub fn pool_tls_from(
        &mut self,
        net: &mut NetworkCore,
        addr: SocketAddr,
        server: &str,
        local_ip: IpAddr,
        connections: usize,
    ) -> HttpPool {
        self.pool_with(net, addr, connections, |http, net| {
            http.connect_tls_with(net, addr, Some(local_ip), server)
        })
    }
    fn pool_with(
        &mut self,
        net: &mut NetworkCore,
        addr: SocketAddr,
        connections: usize,
        mut open: impl FnMut(&mut Self, &mut NetworkCore) -> Token,
    ) -> HttpPool {
        assert!(connections > 0, "a pool needs a connection");
        let pool = HttpPool(self.pools.len() as u32);
        let tokens = (0..connections).map(|_| open(self, net)).collect();
        self.pools.push(Pool {
            addr,
            tokens,
            queue: VecDeque::new(),
            queued_bytes: 0,
            next_seq: 0,
        });
        pool
    }
    /// Removes every listener, connection and outbound endpoint; pending
    /// requests and responses are dropped without events.
    pub fn close(self, net: &mut NetworkCore) {
        for token in self.listeners.iter().chain(self.conns.iter().map(|conn| &conn.token)) {
            net.remove(*token);
        }
    }
    /// Permanently removes an outbound endpoint and stops it reconnecting.
    pub fn remove(&mut self, net: &mut NetworkCore, token: Token) -> bool {
        let Some(i) = self.position(token) else { return false };
        if self.group.is_none() ||
            !matches!(self.conns[i].role, Role::Outbound { .. }) ||
            !net.remove(token)
        {
            return false
        }
        self.remove_conn(i);
        true
    }
    /// Sends one request on an outbound endpoint.
    pub fn request(
        &mut self,
        net: &mut NetworkCore,
        token: Token,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> bool {
        if !valid_request(method, path, headers) || body.len() > self.max_body_bytes {
            return false
        }
        let Some(i) =
            self.position(token).filter(|&i| self.conns[i].role.outbound_head().is_none())
        else {
            return false
        };
        let c = &mut self.conns[i];
        let Role::Outbound { addr, .. } = &c.role else { return false };
        let addr = *addr;
        let sent = net.send_with(token, |out| {
            write_head(out, method, path, headers, &addr, body.len());
            out.extend_from_slice(body);
        });
        if sent {
            c.role.set_outbound_head(Some(method.eq_ignore_ascii_case("HEAD")));
        }
        sent
    }
    /// Sends one request whose response body is delivered incrementally as
    /// [`HttpEvent::ResponseHead`], [`HttpEvent::Body`] and
    /// [`HttpEvent::StreamEnd`], instead of being buffered into a single
    /// [`HttpEvent::Response`].
    ///
    /// The stream ends at the terminal chunk, at `Content-Length` bytes, or
    /// at disconnect, whichever the response uses. `max_body_bytes` becomes
    /// the bound on undelivered bytes rather than on the whole body, and
    /// `request_timeout` stops applying once the head arrives; use
    /// [`Self::with_stream_idle_timeout`] for streams.
    ///
    /// Refused for a pooled token: a stream owns its connection for as long
    /// as it runs, which would starve the pool.
    pub fn request_stream(
        &mut self,
        net: &mut NetworkCore,
        token: Token,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
    ) -> bool {
        if self.pools.iter().any(|pool| pool.tokens.contains(&token)) {
            return false
        }
        if !self.request(net, token, method, path, headers, &[]) {
            return false
        }
        if let Some(conn) = self.position(token).map(|i| &mut self.conns[i]) {
            conn.stream = Some(Stream { body: None });
        }
        true
    }
    /// Sends one request on a pool. Without retries and with nothing queued
    /// ahead, it is written now on an idle connection straight from `body`.
    /// Otherwise `body` is copied into the queue and sent once a connection is
    /// idle; a retried request keeps that copy to resend. `None` when the
    /// request is invalid, too large, or the queue is full.
    #[allow(clippy::too_many_arguments)]
    pub fn send(
        &mut self,
        net: &mut NetworkCore,
        pool: HttpPool,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
        retries: u8,
    ) -> Option<RequestId> {
        self.send_vectored(net, pool, method, path, headers, &[IoSlice::new(body)], retries)
    }
    /// Like [`Self::send`], with a body of up to 16 borrowed slices. An idle,
    /// plain connection writes the request head and body in one syscall;
    /// queued or retried requests retain a contiguous copy.
    #[allow(clippy::too_many_arguments)]
    pub fn send_vectored(
        &mut self,
        net: &mut NetworkCore,
        pool: HttpPool,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[IoSlice<'_>],
        retries: u8,
    ) -> Option<RequestId> {
        if body.len() > MAX_BODY_PARTS || !valid_request(method, path, headers) {
            return None
        }
        let body_len = body.iter().try_fold(0usize, |len, part| len.checked_add(part.len()))?;
        if body_len > self.max_body_bytes {
            return None
        }
        let p = pool.0 as usize;
        let head_request = method.eq_ignore_ascii_case("HEAD");
        let id = RequestId { pool, seq: self.pools[p].next_seq };
        let mut head = self.spare_heads.pop().unwrap_or_else(|| Vec::with_capacity(512));
        write_head(&mut head, method, path, headers, &self.pools[p].addr, body_len);
        if retries == 0 &&
            self.pools[p].queue.is_empty() &&
            let Some(i) = self.write_idle(net, p, &head, body)
        {
            self.pools[p].next_seq += 1;
            self.start(i, Queued { id, head_request, head, body: Vec::new(), retries });
            return Some(id)
        }
        let pending = &mut self.pools[p];
        if pending.queued_bytes + head.len() + body_len > self.max_queued_bytes {
            head.clear();
            self.spare_heads.push(head);
            return None
        }
        pending.next_seq += 1;
        pending.queued_bytes += head.len() + body_len;
        let mut owned_body = Vec::with_capacity(body_len);
        for part in body {
            owned_body.extend_from_slice(part);
        }
        pending.queue.push_back(Queued { id, head_request, head, body: owned_body, retries });
        self.dispatch_pool(net, p);
        Some(id)
    }
    fn dispatch(&mut self, net: &mut NetworkCore) {
        for p in 0..self.pools.len() {
            self.dispatch_pool(net, p);
        }
    }
    fn dispatch_pool(&mut self, net: &mut NetworkCore, p: usize) {
        while let Some(front) = self.pools[p].queue.front() {
            let Some(i) = self.write_idle(net, p, &front.head, &[IoSlice::new(&front.body)]) else {
                break
            };
            let queued = self.pools[p].queue.pop_front().unwrap();
            self.pools[p].queued_bytes -= queued.head.len() + queued.body.len();
            self.start(i, queued);
        }
    }
    /// Writes a request on the first idle connection of pool `p` that takes
    /// it, returning that connection's index.
    fn write_idle(
        &self,
        net: &mut NetworkCore,
        p: usize,
        head: &[u8],
        body: &[IoSlice<'_>],
    ) -> Option<usize> {
        let mut parts = [IoSlice::new(&[]); MAX_BODY_PARTS + 1];
        parts[0] = IoSlice::new(head);
        parts[1..=body.len()].copy_from_slice(body);
        let parts = &parts[..=body.len()];
        self.pools[p].tokens.iter().find_map(|&token| {
            let i =
                self.position(token).filter(|&i| self.conns[i].role.outbound_head().is_none())?;
            net.send_vectored(token, parts).then_some(i)
        })
    }
    fn start(&mut self, i: usize, queued: Queued) {
        self.conns[i].role.set_outbound_head(Some(queued.head_request));
        if let Role::Outbound { in_flight, .. } = &mut self.conns[i].role {
            *in_flight = Some(InFlight { queued, sent_at: Instant::now() });
        }
    }
    fn requeue_or_fail(&mut self, mut queued: Queued) {
        if queued.retries == 0 {
            self.failed.push((queued.id, Failure::Disconnected));
            self.recycle(queued);
            return
        }
        queued.retries -= 1;
        let pool = &mut self.pools[queued.id.pool.0 as usize];
        pool.queued_bytes += queued.head.len() + queued.body.len();
        pool.queue.push_front(queued);
    }
    fn complete_in_flight(&mut self, i: usize) {
        if let Some(done) = self.conns[i].role.take_in_flight() {
            self.recycle(done.queued);
        }
    }
    fn recycle(&mut self, queued: Queued) {
        if self.spare_heads.len() < SPARE_HEADS {
            let mut head = queued.head;
            head.clear();
            self.spare_heads.push(head);
        }
    }
    fn fail_outbound(&mut self, net: &mut NetworkCore, i: usize) {
        let token = self.conns[i].token;
        self.conns[i].buf.clear();
        self.conns[i].role.set_outbound_head(None);
        net.disconnect(token);
    }
    fn buffer_limit(&self) -> usize {
        self.max_head_bytes.saturating_add(self.max_body_bytes)
    }
    fn parse_and_emit<F>(&mut self, net: &mut NetworkCore, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        // Set again by new input, or by a response that frees the
        // connection for a request already buffered.
        self.conns[i].dirty = false;
        let Some(accepted) = self.conns[i].role.accepted() else { return };
        if accepted.discard && !self.discard_step(net, i) {
            return
        }
        match self.conns[i].role.accepted().map(|a| (a.state, a.peer_closed)) {
            Some((State::Idle, false)) => self.parse_head(net, i, handler),
            Some((State::Idle, true)) => {
                // Nothing more will arrive: serve a complete request that is
                // already here, and otherwise close.
                if self.complete_head_buffered(i) {
                    self.parse_head(net, i, handler);
                } else {
                    self.close_now(net, i);
                }
            }
            Some((State::Reading, _)) => self.read_body_step(net, i, handler),
            _ => {}
        }
    }
    fn complete_head_buffered(&self, i: usize) -> bool {
        let (mut stack_slots, mut heap_slots) =
            ([httparse::EMPTY_HEADER; STACK_HEADERS], Vec::new());
        let hs = header_slots(&mut stack_slots, &mut heap_slots, self.max_headers);
        matches!(
            httparse::Request::new(hs).parse(&self.conns[i].buf),
            Ok(httparse::Status::Complete(_))
        )
    }
    /// Closes once what is queued has been written.
    fn close_now(&mut self, net: &mut NetworkCore, i: usize) {
        if let Some(accepted) = self.conns[i].role.accepted_mut() {
            accepted.state = State::Draining;
            accepted.discard = false;
            accepted.body = None;
        }
        self.conns[i].dirty = false;
        net.disconnect_when_drained(self.conns[i].token);
    }
    /// Drops what has arrived of a body answered early. Returns whether it
    /// is finished, so the next request can be parsed.
    fn discard_step(&mut self, net: &mut NetworkCore, i: usize) -> bool {
        let max_drain = self.max_drain_bytes();
        let limits = ChunkLimits {
            decoded: u64::MAX,
            trailer_bytes: self.max_head_bytes,
            trailers: self.max_headers,
        };
        let conn = &mut self.conns[i];
        let Role::Accepted(accepted) = &mut conn.role else { return false };
        let Some(reader) = accepted.body.as_mut() else {
            accepted.discard = false;
            return true
        };
        let finished = match &mut reader.framing {
            BodyFraming::Length(remaining) => {
                let take = (*remaining).min(conn.buf.len());
                conn.buf.drain(..take);
                *remaining -= take;
                reader.raw += take;
                Ok(*remaining == 0)
            }
            BodyFraming::Chunked(decoder) => match decoder.decode(&conn.buf, None, &limits) {
                Ok(consumed) => {
                    conn.buf.drain(..consumed);
                    reader.raw += consumed;
                    if reader.raw > max_drain { Err(()) } else { Ok(decoder.is_done()) }
                }
                Err(_) => Err(()),
            },
        };
        match finished {
            Ok(true) => {
                accepted.discard = false;
                accepted.discard_deadline = None;
                accepted.body = None;
                true
            }
            Ok(false) if !accepted.peer_closed => false,
            // Where the next request starts is unknown, or will never be.
            _ => {
                self.close_now(net, i);
                false
            }
        }
    }
    fn max_drain_bytes(&self) -> usize {
        self.max_drain_bytes.unwrap_or(self.max_body_bytes)
    }
    // One pass from bytes to a delivered request; splitting it would only
    // move the parsed head's borrows around.
    #[allow(clippy::too_many_lines)]
    fn parse_head<F>(&mut self, net: &mut NetworkCore, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        let over_limit = self.conns[i].over_limit;
        let buf = &self.conns[i].buf;
        let (mut stack_slots, mut heap_slots) =
            ([httparse::EMPTY_HEADER; STACK_HEADERS], Vec::new());
        let hs = header_slots(&mut stack_slots, &mut heap_slots, self.max_headers);
        let mut req = httparse::Request::new(hs);
        let Ok(state) = req.parse(buf) else {
            self.error(net, i, 400);
            return
        };
        let httparse::Status::Complete(head) = state else {
            // A partial parse means every buffered byte is still head bytes.
            if !crlf_only(buf) {
                self.error(net, i, 400);
            } else if over_limit || buf.len() > self.max_head_bytes {
                self.error(net, i, self.oversized_head_status);
            }
            return
        };
        if !crlf_only(&buf[..head]) {
            self.error(net, i, 400);
            return
        }
        if head > self.max_head_bytes {
            self.error(net, i, self.oversized_head_status);
            return
        }
        let Some(framing) = request_framing(req.headers, req.version, self.max_content_length)
        else {
            self.error(net, i, 400);
            return
        };
        let peer_closed = self.conns[i].role.accepted().is_some_and(|a| a.peer_closed);
        let close = req.version == Some(0) && !has_token(req.headers, "connection", b"keep-alive") ||
            has_token(req.headers, "connection", b"close") ||
            peer_closed;
        let expect = has_token(req.headers, "expect", b"100-continue");
        let head_request = req.method == Some("HEAD");
        if self.request_heads && framing != RequestFraming::Length(0) {
            let (body, reader) = match framing {
                RequestFraming::Length(len) => (RequestBody::Length(len), BodyFraming::Length(len)),
                RequestFraming::Chunked => {
                    (RequestBody::Chunked, BodyFraming::Chunked(ChunkDecoder::new()))
                }
            };
            let token = self.conns[i].token;
            let request = HttpRequest {
                method: req.method.unwrap_or(""),
                path: req.path.unwrap_or(""),
                version: req.version.unwrap_or(1),
                headers: req.headers,
                body: &[],
            };
            handler(HttpEvent::RequestHead { token, request, body });
            self.start_body(i, head, reader, State::Head, [close, expect, head_request]);
            return
        }
        let len = match framing {
            RequestFraming::Length(len) => len,
            RequestFraming::Chunked => {
                // The head is kept aside while the body is decoded, so the
                // connection buffer only ever holds undecoded chunk bytes.
                let conn = &mut self.conns[i];
                if let Some(accepted) = conn.role.accepted_mut() {
                    accepted.head.clear();
                    accepted.head.extend_from_slice(&conn.buf[..head]);
                }
                let decoder = BodyFraming::Chunked(ChunkDecoder::new());
                self.start_body(i, head, decoder, State::Reading, [close, expect, head_request]);
                self.read_body_step(net, i, handler);
                return
            }
        };
        if len > self.max_body_bytes {
            self.error(net, i, 413);
            return
        }
        let Some(end) = head.checked_add(len) else {
            self.error(net, i, 413);
            return
        };
        if buf.len() < end {
            if over_limit {
                self.error(net, i, 413);
                return
            }
            if expect && !self.conns[i].role.accepted().is_some_and(|a| a.continued) {
                self.send_continue(net, i);
            }
            return
        }
        let token = self.conns[i].token;
        let request = HttpRequest {
            method: req.method.unwrap_or(""),
            path: req.path.unwrap_or(""),
            version: req.version.unwrap_or(1),
            headers: req.headers,
            body: &buf[head..end],
        };
        handler(HttpEvent::Request { token, request });
        self.conns[i].buf.drain(..end);
        if let Some(accepted) = self.conns[i].role.accepted_mut() {
            accepted.state = State::Pending;
            accepted.close = close;
            accepted.continued = false;
            accepted.head_request = head_request;
        }
    }
    /// Moves past a parsed head to its body, in `state`. `flags` are the
    /// request's close, expect and head-request properties.
    fn start_body(
        &mut self,
        i: usize,
        head: usize,
        framing: BodyFraming,
        state: State,
        [close, expect, head_request]: [bool; 3],
    ) {
        let conn = &mut self.conns[i];
        conn.buf.drain(..head);
        if let Some(accepted) = conn.role.accepted_mut() {
            accepted.state = state;
            accepted.body = Some(BodyReader { framing, raw: 0 });
            accepted.decoded.clear();
            accepted.close = close;
            accepted.expect = expect;
            accepted.continued = false;
            accepted.head_request = head_request;
        }
    }
    fn send_continue(&mut self, net: &mut NetworkCore, i: usize) {
        let token = self.conns[i].token;
        if net.send_with(token, |out| out.extend_from_slice(b"HTTP/1.1 100 Continue\r\n\r\n")) &&
            let Some(accepted) = self.conns[i].role.accepted_mut()
        {
            accepted.continued = true;
        }
    }
    /// Decodes what has arrived of the body being read, and delivers the
    /// request once all of it is here.
    fn read_body_step<F>(&mut self, net: &mut NetworkCore, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        let raw_limit = self.chunked_raw_limit();
        let limits = ChunkLimits {
            decoded: self.max_body_bytes as u64,
            trailer_bytes: self.max_head_bytes,
            trailers: self.max_headers,
        };
        let max_body_bytes = self.max_body_bytes;
        let conn = &mut self.conns[i];
        let Role::Accepted(accepted) = &mut conn.role else { return };
        let Some(reader) = accepted.body.as_mut() else { return };
        let outcome = match &mut reader.framing {
            BodyFraming::Length(length) if *length > max_body_bytes => Err(BodyError::TooLarge),
            BodyFraming::Length(length) => Ok(conn.buf.len() >= *length),
            BodyFraming::Chunked(decoder) => {
                match decoder.decode(&conn.buf, Some(&mut accepted.decoded), &limits) {
                    Ok(consumed) => {
                        conn.buf.drain(..consumed);
                        reader.raw += consumed;
                        if reader.raw > raw_limit {
                            Err(BodyError::TooLarge)
                        } else {
                            Ok(decoder.is_done())
                        }
                    }
                    Err(error) => Err(error),
                }
            }
        };
        let outcome = match outcome {
            Ok(false) if accepted.peer_closed => Err(BodyError::Incomplete),
            outcome => outcome,
        };
        if self.request_heads {
            return self.finish_body_read(i, outcome, handler)
        }
        match outcome {
            Ok(false) => {
                if conn.over_limit {
                    self.error(net, i, 413);
                } else if accepted.expect && !accepted.continued {
                    self.send_continue(net, i);
                }
            }
            Ok(true) => self.deliver_buffered_head(i, handler),
            Err(BodyError::TooLarge) => self.error(net, i, 413),
            Err(_) => self.error(net, i, 400),
        }
    }
    /// Reports the outcome of a body read asked for in request-head mode.
    fn finish_body_read<F>(&mut self, i: usize, outcome: Result<bool, BodyError>, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        let token = self.conns[i].token;
        let conn = &mut self.conns[i];
        let Role::Accepted(accepted) = &mut conn.role else { return };
        match outcome {
            Ok(false) => return,
            Ok(true) => {
                if let Some(BodyReader { framing: BodyFraming::Length(length), .. }) = accepted.body
                {
                    handler(HttpEvent::RequestBody { token, body: &conn.buf[..length] });
                    conn.buf.drain(..length);
                } else {
                    handler(HttpEvent::RequestBody { token, body: &accepted.decoded });
                }
            }
            Err(error) => {
                handler(HttpEvent::RequestBodyFailed { token, error });
                // The rest of the body is unread or unreadable.
                accepted.close = true;
            }
        }
        accepted.body = None;
        accepted.state = State::Pending;
        recycle_buffer(&mut accepted.decoded);
        conn.dirty = false;
    }
    /// Delivers the request whose head was kept aside, with its decoded body.
    fn deliver_buffered_head<F>(&mut self, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        let token = self.conns[i].token;
        let max_headers = self.max_headers;
        let conn = &mut self.conns[i];
        let Role::Accepted(accepted) = &mut conn.role else { return };
        {
            let (mut stack_slots, mut heap_slots) =
                ([httparse::EMPTY_HEADER; STACK_HEADERS], Vec::new());
            let hs = header_slots(&mut stack_slots, &mut heap_slots, max_headers);
            let mut req = httparse::Request::new(hs);
            // The head parsed before it was kept, so it parses again.
            if !matches!(req.parse(&accepted.head), Ok(httparse::Status::Complete(_))) {
                return
            }
            let request = HttpRequest {
                method: req.method.unwrap_or(""),
                path: req.path.unwrap_or(""),
                version: req.version.unwrap_or(1),
                headers: req.headers,
                body: &accepted.decoded,
            };
            handler(HttpEvent::Request { token, request });
        }
        accepted.body = None;
        accepted.state = State::Pending;
        accepted.continued = false;
        accepted.head.clear();
        recycle_buffer(&mut accepted.decoded);
    }
    /// Raw bytes a chunked body may take from the connection, framing
    /// included. Framing costs at least five bytes per chunk, so this allows
    /// a full-size body in chunks of a few bytes while bounding a stream of
    /// tiny or empty-data chunks.
    fn chunked_raw_limit(&self) -> usize {
        self.max_body_bytes.saturating_mul(2).saturating_add(self.max_head_bytes)
    }
    fn error(&mut self, net: &mut NetworkCore, i: usize, status: u16) {
        if let Some(accepted) = self.conns[i].role.accepted_mut() {
            accepted.state = State::Pending;
            accepted.close = true;
            accepted.body = None;
        }
        let token = self.conns[i].token;
        let _ = self.respond(net, token, status, &[], &[]);
    }
    /// Sends the response for a pending request and returns whether it was
    /// queued.
    ///
    /// Call this after the drive that delivered the request, or from a later
    /// one. Each call completes exactly one request for `token`. In
    /// request-head mode it may also answer a request whose body has not
    /// been read; see [`Self::with_request_heads`].
    ///
    /// The body is copied into the connection's send queue. For large
    /// bodies use [`Self::respond_owned`].
    pub fn respond(
        &mut self,
        net: &mut NetworkCore,
        token: Token,
        status: u16,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> bool {
        self.respond_with(net, token, status, headers, ResponseBody::Borrowed(body))
    }
    /// Like [`Self::respond`], but takes the body and hands it to the socket
    /// in pieces as the peer reads, keeping at most about
    /// `2 * OUT_CHUNK_BYTES` of it queued at a time, so a large response to
    /// a slow reader costs its own memory and no copy of it. The connection
    /// serves its next request once the whole body has been handed over;
    /// [`Self::response_pending`] reports what is left meanwhile.
    pub fn respond_owned(
        &mut self,
        net: &mut NetworkCore,
        token: Token,
        status: u16,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> bool {
        self.respond_with(net, token, status, headers, ResponseBody::Owned(body))
    }
    /// Bytes of the response being written for `token` that the peer has
    /// not taken yet, those queued at the socket included, while a body
    /// given to [`Self::respond_owned`] is still being handed over; `None`
    /// once it all has been (at most a socket queue's worth may then remain
    /// unsent, bounded by the idle timeout). An application can compare
    /// successive values to bound how slowly a response may be read.
    pub fn response_pending(&self, net: &NetworkCore, token: Token) -> Option<usize> {
        let conn = &self.conns[self.position(token)?];
        let out = conn.role.accepted()?.out.as_ref()?;
        Some(out.body.len() - out.sent + net.queued_bytes(token))
    }
    fn respond_with(
        &mut self,
        net: &mut NetworkCore,
        token: Token,
        status: u16,
        headers: &[(&str, &str)],
        body: ResponseBody<'_>,
    ) -> bool {
        let Some(i) = self.position(token).filter(|&i| {
            self.conns[i]
                .role
                .accepted()
                .is_some_and(|a| matches!(a.state, State::Pending | State::Head | State::Reading))
        }) else {
            return false
        };
        if !(200..=599).contains(&status) ||
            headers.iter().any(|(n, v)| {
                !valid_token(n) ||
                    v.contains(['\r', '\n']) ||
                    n.eq_ignore_ascii_case("content-length") ||
                    n.eq_ignore_ascii_case("transfer-encoding")
            })
        {
            return false
        }
        let caller_close = headers.iter().any(|(n, v)| {
            n.eq_ignore_ascii_case("connection") && has_value_token(v.as_bytes(), b"close")
        });
        let max_drain = self.max_drain_bytes();
        let Some(accepted) = self.conns[i].role.accepted() else { return false };
        // Answered before its body was read: drop the body to keep the
        // connection if that is bounded and the client will send it (it
        // may wait for a `100 Continue` that now never comes); else close.
        let unread = accepted.body.as_ref().filter(|_| accepted.state != State::Pending);
        let discard = unread.is_some_and(|reader| {
            !accepted.close &&
                !caller_close &&
                !accepted.peer_closed &&
                (!accepted.expect || accepted.continued) &&
                match reader.framing {
                    BodyFraming::Length(remaining) => remaining <= max_drain,
                    BodyFraming::Chunked(_) => reader.raw <= max_drain,
                }
        });
        let close = accepted.close || caller_close || (unread.is_some() && !discard);
        let suppress_body = accepted.head_request || matches!(status, 100..=199 | 204 | 304);
        let length = (!matches!(status, 100..=199 | 204)).then_some(body.len());
        let (ok, rest) = match body {
            ResponseBody::Borrowed(body) => {
                let ok = net.send_with(token, |out| {
                    write_response_head(out, status, headers, length, close);
                    if !suppress_body {
                        out.extend_from_slice(body);
                    }
                });
                (ok, None)
            }
            ResponseBody::Owned(body) => {
                let mut head = std::mem::take(&mut self.response_head);
                head.clear();
                write_response_head(&mut head, status, headers, length, close);
                let first = if suppress_body { 0 } else { body.len().min(OUT_CHUNK_BYTES) };
                let ok = net.send_parts(token, &head, &body[..first]);
                self.response_head = head;
                let rest = (!suppress_body && first < body.len()).then_some(OutBody {
                    body,
                    sent: first,
                    close,
                });
                (ok, rest)
            }
        };
        if !ok {
            return false
        }
        self.conns[i].last_activity = Instant::now();
        let drain_timeout = self.drain_timeout;
        if let Some(accepted) = self.conns[i].role.accepted_mut() {
            if discard {
                accepted.discard = true;
                accepted.discard_deadline = Some(Instant::now() + drain_timeout);
            } else {
                accepted.body = None;
            }
            if let Some(rest) = rest {
                accepted.state = State::Writing;
                accepted.out = Some(rest);
                self.pump(net, i);
                if self.request_heads {
                    sync_reading(net, &mut self.conns[i]);
                }
                return true
            }
        }
        self.response_done(net, i, close);
        true
    }
    /// Hands more of an owned response body to the socket while little is
    /// queued there, and finishes the response once all of it has been.
    fn pump(&mut self, net: &mut NetworkCore, i: usize) {
        let token = self.conns[i].token;
        let Some(accepted) = self.conns[i].role.accepted_mut() else { return };
        let Some(out) = accepted.out.as_mut() else { return };
        while out.sent < out.body.len() && net.queued_bytes(token) < OUT_CHUNK_BYTES {
            let end = out.body.len().min(out.sent + OUT_CHUNK_BYTES);
            if !net.send_parts(token, &[], &out.body[out.sent..end]) {
                // The connection is gone; its disconnect cleans up.
                return
            }
            out.sent = end;
        }
        if out.sent == out.body.len() {
            let close = out.close;
            accepted.out = None;
            self.response_done(net, i, close);
        }
    }
    /// The response has been handed to the socket: close, or wait for the
    /// next request.
    fn response_done(&mut self, net: &mut NetworkCore, i: usize, close: bool) {
        let token = self.conns[i].token;
        let conn = &mut self.conns[i];
        conn.dirty = !close && !conn.buf.is_empty();
        if let Some(accepted) = conn.role.accepted_mut() {
            accepted.state = if close { State::Draining } else { State::Idle };
        }
        if close {
            net.disconnect_when_drained(token);
        } else if self.request_heads {
            // A pipelined request may wait in the kernel.
            sync_reading(net, &mut self.conns[i]);
        }
    }
    /// Delivers a streaming response: the head once, then body pieces
    /// borrowed straight out of the receive buffer as they arrive.
    fn parse_stream<F>(&mut self, net: &mut NetworkCore, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        self.conns[i].dirty = false;
        let token = self.conns[i].token;
        if self.stream_body(i).is_none() && !self.parse_stream_head(net, i, handler) {
            return
        }
        loop {
            let Some(body) = self.stream_body(i) else { return };
            let buf = &self.conns[i].buf;
            match body {
                StreamBody::Eof => {
                    if buf.is_empty() {
                        return
                    }
                    handler(HttpEvent::Body { token, chunk: buf });
                    self.conns[i].buf.clear();
                    return
                }
                StreamBody::Length(remaining) => {
                    let take = remaining.min(buf.len());
                    if take > 0 {
                        handler(HttpEvent::Body { token, chunk: &buf[..take] });
                        self.conns[i].buf.drain(..take);
                    }
                    self.set_stream_body(i, StreamBody::Length(remaining - take));
                    if remaining == take {
                        self.end_stream(i, StreamEnd::Complete, handler);
                    }
                    return
                }
                StreamBody::Chunked => match httparse::parse_chunk_size(buf) {
                    Err(_) => {
                        self.fail_stream(net, i, handler);
                        return
                    }
                    Ok(httparse::Status::Partial) => {
                        if buf.len() > self.max_head_bytes {
                            self.fail_stream(net, i, handler);
                        }
                        return
                    }
                    Ok(httparse::Status::Complete((consumed, 0))) => {
                        self.conns[i].buf.drain(..consumed);
                        self.end_stream(i, StreamEnd::Complete, handler);
                        return
                    }
                    Ok(httparse::Status::Complete((consumed, size))) => {
                        let Ok(size) = usize::try_from(size) else {
                            self.fail_stream(net, i, handler);
                            return
                        };
                        // A chunk is delivered whole, so one larger than the
                        // undelivered bound could never be handed over.
                        if size > self.max_body_bytes {
                            self.fail_stream(net, i, handler);
                            return
                        }
                        // The chunk data is followed by its own CRLF.
                        let end = consumed + size + 2;
                        if buf.len() < end {
                            return
                        }
                        handler(HttpEvent::Body { token, chunk: &buf[consumed..consumed + size] });
                        self.conns[i].buf.drain(..end);
                    }
                },
            }
        }
    }
    /// Parses and delivers a streaming response head; returns whether the
    /// body may now be read.
    fn parse_stream_head<F>(&mut self, net: &mut NetworkCore, i: usize, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        let token = self.conns[i].token;
        let b = &self.conns[i].buf;
        let (mut stack_slots, mut heap_slots) =
            ([httparse::EMPTY_HEADER; STACK_HEADERS], Vec::new());
        let hs = header_slots(&mut stack_slots, &mut heap_slots, self.max_headers);
        let mut response = httparse::Response::new(hs);
        let Ok(state) = response.parse(b) else {
            self.fail_stream(net, i, handler);
            return false
        };
        let httparse::Status::Complete(head) = state else {
            if !crlf_only(b) || b.len() > self.max_head_bytes {
                self.fail_stream(net, i, handler);
            }
            return false
        };
        if !crlf_only(&b[..head]) || head > self.max_head_bytes {
            self.fail_stream(net, i, handler);
            return false
        }
        let status = response.code.unwrap_or(0);
        let chunked = transfer_chunked(response.headers);
        let content_length = response_content_length(response.headers);
        if status == 101 ||
            matches!(content_length, ContentLength::Invalid) ||
            chunked.is_none() ||
            (chunked == Some(true) && !matches!(content_length, ContentLength::Absent))
        {
            self.fail_stream(net, i, handler);
            return false
        }
        // An informational head is not the response; keep reading.
        if status < 200 {
            self.conns[i].buf.drain(..head);
            self.conns[i].dirty = !self.conns[i].buf.is_empty();
            return false
        }
        let empty = matches!(status, 204 | 304);
        let body = if empty {
            StreamBody::Length(0)
        } else if chunked == Some(true) {
            StreamBody::Chunked
        } else if let ContentLength::Present(length) = content_length {
            StreamBody::Length(length)
        } else {
            StreamBody::Eof
        };
        handler(HttpEvent::ResponseHead { token, status, headers: response.headers });
        self.conns[i].buf.drain(..head);
        self.set_stream_body(i, body);
        if empty {
            self.end_stream(i, StreamEnd::Complete, handler);
            return false
        }
        true
    }
    fn stream_body(&self, i: usize) -> Option<StreamBody> {
        self.conns[i].stream.as_ref().and_then(|stream| stream.body)
    }
    fn set_stream_body(&mut self, i: usize, body: StreamBody) {
        if let Some(stream) = self.conns[i].stream.as_mut() {
            stream.body = Some(body);
        }
    }
    /// Ends a stream and frees its connection for another request.
    fn end_stream<F>(&mut self, i: usize, reason: StreamEnd, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        let token = self.conns[i].token;
        self.conns[i].stream = None;
        // A stream owns its connection and nothing is pipelined behind it, so
        // whatever trails the body, chunk trailers included, is unsolicited
        // and would otherwise be parsed as the next response head.
        self.conns[i].buf.clear();
        self.conns[i].role.set_outbound_head(None);
        self.complete_in_flight(i);
        handler(HttpEvent::StreamEnd { token, reason });
    }
    /// Drops a malformed or oversized stream; the endpoint reconnects.
    fn fail_stream<F>(&mut self, net: &mut NetworkCore, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        let token = self.conns[i].token;
        self.conns[i].buf.clear();
        self.end_stream(i, StreamEnd::Disconnected, handler);
        net.disconnect(token);
    }
    fn parse_outbound<F>(&mut self, net: &mut NetworkCore, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        self.conns[i].dirty = false;
        while self.conns[i].role.outbound_head().is_some() {
            let b = &self.conns[i].buf;
            let (mut stack_slots, mut heap_slots) =
                ([httparse::EMPTY_HEADER; STACK_HEADERS], Vec::new());
            let hs = header_slots(&mut stack_slots, &mut heap_slots, self.max_headers);
            let mut response = httparse::Response::new(hs);
            let parsed = response.parse(b);
            let Ok(state) = parsed else {
                self.fail_outbound(net, i);
                return
            };
            let httparse::Status::Complete(head) = state else {
                // A partial parse means every buffered byte is still head bytes.
                if !crlf_only(b) || b.len() > self.max_head_bytes {
                    self.fail_outbound(net, i);
                }
                return
            };
            if !crlf_only(&b[..head]) || head > self.max_head_bytes {
                self.fail_outbound(net, i);
                return
            }
            let status = response.code.unwrap_or(0);
            let no_body = self.conns[i].role.outbound_head() == Some(true) ||
                matches!(status, 100..=199 | 204 | 304);
            let chunked = transfer_chunked(response.headers);
            let content_length = response_content_length(response.headers);
            if status == 101 ||
                matches!(content_length, ContentLength::Invalid) ||
                chunked.is_none() ||
                (chunked == Some(true) && !matches!(content_length, ContentLength::Absent)) ||
                (!no_body &&
                    matches!(content_length, ContentLength::Present(length) if length > self.max_body_bytes))
            {
                self.fail_outbound(net, i);
                return
            }
            let (consumed, chunked_body) = if no_body {
                (head, false)
            } else if chunked == Some(true) {
                match Self::decode_chunked(
                    b,
                    head,
                    self.max_body_bytes,
                    self.max_headers,
                    &mut self.chunked_body,
                ) {
                    Ok(Some(consumed)) => (consumed, true),
                    Ok(None) => return,
                    Err(()) => {
                        self.fail_outbound(net, i);
                        return
                    }
                }
            } else if let ContentLength::Present(length) = content_length {
                let Some(consumed) = head.checked_add(length) else {
                    self.fail_outbound(net, i);
                    return
                };
                if b.len() < consumed {
                    return
                }
                (consumed, false)
            } else {
                return
            };
            if status < 200 {
                self.conns[i].buf.drain(..consumed);
                self.conns[i].dirty = !self.conns[i].buf.is_empty();
                continue
            }
            let token = self.conns[i].token;
            let id = self.conns[i].role.in_flight_id();
            let close = response.version == Some(0) &&
                !has_token(response.headers, "connection", b"keep-alive") ||
                has_token(response.headers, "connection", b"close");
            let response_event = HttpResponse {
                version: response.version.unwrap_or(1),
                status,
                reason: response.reason.unwrap_or(""),
                headers: response.headers,
                body: if no_body {
                    &[]
                } else if chunked_body {
                    &self.chunked_body
                } else {
                    &b[head..consumed]
                },
            };
            handler(HttpEvent::Response { token, id, response: response_event });
            self.conns[i].buf.drain(..consumed);
            self.conns[i].dirty = !self.conns[i].buf.is_empty();
            self.conns[i].role.set_outbound_head(None);
            self.complete_in_flight(i);
            if close {
                net.disconnect(token);
                return
            }
        }
    }
    fn parse_eof_outbound<F>(&self, i: usize, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        if self.conns[i].role.outbound_head().is_none() {
            return false
        }
        let b = &self.conns[i].buf;
        let (mut stack_slots, mut heap_slots) =
            ([httparse::EMPTY_HEADER; STACK_HEADERS], Vec::new());
        let headers = header_slots(&mut stack_slots, &mut heap_slots, self.max_headers);
        let mut response = httparse::Response::new(headers);
        let Ok(httparse::Status::Complete(head)) = response.parse(b) else { return false };
        if !crlf_only(&b[..head]) || head > self.max_head_bytes {
            return false
        }
        let status = response.code.unwrap_or(0);
        let no_body = self.conns[i].role.outbound_head() == Some(true) ||
            matches!(status, 100..=199 | 204 | 304);
        if no_body ||
            transfer_chunked(response.headers) != Some(false) ||
            !matches!(response_content_length(response.headers), ContentLength::Absent) ||
            b.len() - head > self.max_body_bytes
        {
            return false
        }
        let token = self.conns[i].token;
        handler(HttpEvent::Response {
            token,
            id: self.conns[i].role.in_flight_id(),
            response: HttpResponse {
                version: response.version.unwrap_or(1),
                status,
                reason: response.reason.unwrap_or(""),
                headers: response.headers,
                body: &b[head..],
            },
        });
        true
    }
    /// Decodes the complete chunked body starting at `start` into `body`,
    /// returning the offset just past it.
    fn decode_chunked(
        bytes: &[u8],
        start: usize,
        max_body_bytes: usize,
        max_headers: usize,
        body: &mut Vec<u8>,
    ) -> Result<Option<usize>, ()> {
        let bytes = &bytes[start..];
        let Some((end, body_len)) = Self::chunked_end(bytes, max_body_bytes, max_headers)? else {
            return Ok(None)
        };
        body.clear();
        body.reserve(body_len);
        let mut at = 0;
        while at < end {
            let httparse::Status::Complete((consumed, size)) =
                httparse::parse_chunk_size(&bytes[at..]).map_err(|_| ())?
            else {
                return Err(())
            };
            at = at.checked_add(consumed).ok_or(())?;
            let size = usize::try_from(size).map_err(|_| ())?;
            if size == 0 {
                return start.checked_add(end).map(Some).ok_or(())
            }
            body.extend_from_slice(&bytes[at..at + size]);
            at = at.checked_add(size + 2).ok_or(())?;
        }
        Err(())
    }
    fn chunked_end(
        bytes: &[u8],
        max_body_bytes: usize,
        max_headers: usize,
    ) -> Result<Option<(usize, usize)>, ()> {
        let mut at = 0;
        let mut body_len = 0;
        loop {
            let httparse::Status::Complete((consumed, size)) =
                httparse::parse_chunk_size(&bytes[at..]).map_err(|_| ())?
            else {
                return Ok(None)
            };
            let size = usize::try_from(size).map_err(|_| ())?;
            if size > max_body_bytes.saturating_sub(body_len) {
                return Err(())
            }
            at = at.checked_add(consumed).ok_or(())?;
            if size == 0 {
                let (mut stack_slots, mut heap_slots) =
                    ([httparse::EMPTY_HEADER; STACK_HEADERS], Vec::new());
                let headers = header_slots(&mut stack_slots, &mut heap_slots, max_headers);
                let httparse::Status::Complete((consumed, _)) =
                    httparse::parse_headers(&bytes[at..], headers).map_err(|_| ())?
                else {
                    return Ok(None)
                };
                let end = at.checked_add(consumed).ok_or(())?;
                if !crlf_only(&bytes[at..end]) {
                    return Err(())
                }
                return Ok(Some((end, body_len)))
            }
            let Some(chunk_end) = at.checked_add(size).and_then(|at| at.checked_add(2)) else {
                return Err(())
            };
            if bytes.len() < chunk_end || &bytes[at + size..chunk_end] != b"\r\n" {
                return Ok(None)
            }
            body_len += size;
            at = chunk_end;
        }
    }
}

/// Writes a response's status line and headers. Caller `Connection` headers
/// only feed the close decision; exactly one canonical `Connection` header is
/// written.
fn write_response_head(
    out: &mut impl io::Write,
    status: u16,
    headers: &[(&str, &str)],
    length: Option<usize>,
    close: bool,
) {
    out.write_all(b"HTTP/1.1 ").unwrap();
    write_decimal(out, usize::from(status));
    out.write_all(b" ").unwrap();
    out.write_all(reason_phrase(status).as_bytes()).unwrap();
    out.write_all(b"\r\n").unwrap();
    for (n, v) in headers {
        if n.eq_ignore_ascii_case("connection") {
            continue
        }
        for part in [n.as_bytes(), b": ", v.as_bytes(), b"\r\n"] {
            out.write_all(part).unwrap();
        }
    }
    if let Some(length) = length {
        out.write_all(b"Content-Length: ").unwrap();
        write_decimal(out, length);
        out.write_all(b"\r\n").unwrap();
    }
    let connection: &[u8] =
        if close { b"Connection: close\r\n\r\n" } else { b"Connection: keep-alive\r\n\r\n" };
    out.write_all(connection).unwrap();
}
/// Pauses or resumes reading an accepted connection in request-head mode to
/// match whether its input is wanted now.
fn sync_reading(net: &mut NetworkCore, conn: &mut Conn) {
    let Role::Accepted(accepted) = &mut conn.role else { return };
    let wanted = accepted.discard ||
        match accepted.state {
            State::Idle | State::Reading => true,
            State::Head | State::Pending | State::Writing | State::Draining => false,
        };
    if wanted == accepted.paused {
        let changed =
            if wanted { net.resume_reading(conn.token) } else { net.pause_reading(conn.token) };
        if changed {
            accepted.paused = !wanted;
        }
    }
}
fn crlf_only(bytes: &[u8]) -> bool {
    bytes.iter().enumerate().all(|(i, b)| *b != b'\n' || i > 0 && bytes[i - 1] == b'\r')
}
/// How a request's body is delimited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestFraming {
    Length(usize),
    Chunked,
}
/// The body framing of a request, or `None` when it is ambiguous or
/// unsupported. Framing that two parsers could read differently is a
/// request-smuggling vector, so only these are accepted: no framing headers,
/// exactly one `Content-Length` of plain decimal digits no larger than
/// `max_length`, or exactly one `Transfer-Encoding: chunked` without a
/// `Content-Length` on HTTP/1.1. Repeated lengths are rejected even when
/// equal, as are list values, other or stacked codings, and chunked
/// HTTP/1.0 requests.
fn request_framing(
    headers: &[httparse::Header<'_>],
    version: Option<u8>,
    max_length: usize,
) -> Option<RequestFraming> {
    let mut length = None;
    let mut chunked = false;
    for header in headers {
        if header.name.eq_ignore_ascii_case("content-length") {
            let value = header.value;
            if length.is_some() || value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
                return None
            }
            // Parsing fails rather than wraps above `usize::MAX`.
            let parsed: usize = std::str::from_utf8(value).ok()?.parse().ok()?;
            if parsed > max_length {
                return None
            }
            length = Some(parsed);
        } else if header.name.eq_ignore_ascii_case("transfer-encoding") {
            if chunked || !header.value.trim_ascii().eq_ignore_ascii_case(b"chunked") {
                return None
            }
            chunked = true;
        }
    }
    match (length, chunked) {
        (None, true) if version == Some(1) => Some(RequestFraming::Chunked),
        (_, true) => None,
        (length, false) => Some(RequestFraming::Length(length.unwrap_or(0))),
    }
}
fn has_token(headers: &[httparse::Header<'_>], name: &str, value: &[u8]) -> bool {
    headers
        .iter()
        .filter(|h| h.name.eq_ignore_ascii_case(name))
        .any(|h| has_value_token(h.value, value))
}
fn has_value_token(value: &[u8], wanted: &[u8]) -> bool {
    value.split(|b| *b == b',').any(|part| part.trim_ascii().eq_ignore_ascii_case(wanted))
}
fn valid_token(value: &str) -> bool {
    !value.is_empty() &&
        value.bytes().all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}
fn valid_request(method: &str, path: &str, headers: &[(&str, &str)]) -> bool {
    valid_token(method) &&
        !path.is_empty() &&
        !path.contains(['\r', '\n', ' ']) &&
        headers.iter().all(|(n, v)| {
            valid_token(n) &&
                !v.contains(['\r', '\n']) &&
                !n.eq_ignore_ascii_case("content-length") &&
                !n.eq_ignore_ascii_case("transfer-encoding")
        })
}
/// Writes a request head, defaulting `Host` to the endpoint address when the
/// caller did not supply one.
fn write_head(
    out: &mut impl io::Write,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    addr: &SocketAddr,
    body_len: usize,
) {
    let mut put = |bytes: &[u8]| out.write_all(bytes).unwrap();
    put(method.as_bytes());
    put(b" ");
    put(path.as_bytes());
    put(b" HTTP/1.1\r\n");
    let mut has_host = false;
    for (n, v) in headers {
        has_host |= n.eq_ignore_ascii_case("host");
        put(n.as_bytes());
        put(b": ");
        put(v.as_bytes());
        put(b"\r\n");
    }
    if !has_host {
        write!(out, "Host: {addr}\r\n").unwrap();
    }
    out.write_all(b"Content-Length: ").unwrap();
    write_decimal(out, body_len);
    out.write_all(b"\r\n\r\n").unwrap();
}
fn write_decimal(out: &mut impl io::Write, mut n: usize) {
    let mut digits = [0u8; 20];
    let mut at = digits.len();
    loop {
        at -= 1;
        digits[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break
        }
    }
    out.write_all(&digits[at..]).unwrap();
}
impl Role {
    fn in_flight_id(&self) -> Option<RequestId> {
        match self {
            Self::Outbound { in_flight: Some(in_flight), .. } => Some(in_flight.queued.id),
            _ => None,
        }
    }
    fn take_in_flight(&mut self) -> Option<InFlight> {
        match self {
            Self::Outbound { in_flight, .. } => in_flight.take(),
            Self::Accepted(_) => None,
        }
    }
    fn accepted(&self) -> Option<&Accepted> {
        match self {
            Self::Accepted(accepted) => Some(accepted),
            Self::Outbound { .. } => None,
        }
    }
    fn accepted_mut(&mut self) -> Option<&mut Accepted> {
        match self {
            Self::Accepted(accepted) => Some(accepted),
            Self::Outbound { .. } => None,
        }
    }
    fn is_draining(&self) -> bool {
        self.accepted().is_some_and(|accepted| accepted.state == State::Draining)
    }
    fn outbound_head(&self) -> Option<bool> {
        match self {
            Self::Outbound { in_flight_head, .. } => *in_flight_head,
            Self::Accepted(_) => None,
        }
    }
    fn set_outbound_head(&mut self, head_request: Option<bool>) {
        if let Self::Outbound { in_flight_head: current, .. } = self {
            *current = head_request;
        }
    }
}

enum ContentLength {
    Absent,
    Present(usize),
    Invalid,
}
fn header_slots<'s, 'b>(
    stack: &'s mut [httparse::Header<'b>; STACK_HEADERS],
    heap: &'s mut Vec<httparse::Header<'b>>,
    max_headers: usize,
) -> &'s mut [httparse::Header<'b>] {
    if max_headers <= STACK_HEADERS {
        return &mut stack[..max_headers]
    }
    heap.resize(max_headers, httparse::EMPTY_HEADER);
    heap
}
fn response_content_length(headers: &[httparse::Header<'_>]) -> ContentLength {
    let mut length = None;
    for header in headers.iter().filter(|h| h.name.eq_ignore_ascii_case("content-length")) {
        let Ok(value) = std::str::from_utf8(header.value) else { return ContentLength::Invalid };
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return ContentLength::Invalid
        }
        let Ok(parsed) = value.parse() else { return ContentLength::Invalid };
        if length.replace(parsed).is_some_and(|previous| previous != parsed) {
            return ContentLength::Invalid
        }
    }
    length.map_or(ContentLength::Absent, ContentLength::Present)
}
fn transfer_chunked(headers: &[httparse::Header<'_>]) -> Option<bool> {
    let mut found = false;
    for value in headers
        .iter()
        .filter(|h| h.name.eq_ignore_ascii_case("transfer-encoding"))
        .flat_map(|h| h.value.split(|b| *b == b','))
    {
        if value.trim_ascii().eq_ignore_ascii_case(b"chunked") && !found {
            found = true;
        } else {
            return None
        }
    }
    Some(found)
}

/// Why a request body could not be read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyError {
    /// The body exceeds `max_body_bytes`, or its chunk framing exceeds the
    /// raw-byte bound beneath the decoder.
    TooLarge,
    /// Invalid chunk framing. Where the next request would start is unknown,
    /// so the connection cannot be reused.
    Malformed,
    /// The peer finished sending before the end of the body.
    Incomplete,
}

/// Bounds for [`ChunkDecoder::decode`].
struct ChunkLimits {
    /// Decoded bytes allowed in total.
    decoded: u64,
    /// Trailer section size, its final empty line included.
    trailer_bytes: usize,
    /// Trailer fields allowed.
    trailers: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChunkState {
    /// Expecting a chunk-size line.
    Size,
    /// Data bytes still to come in the current chunk.
    Data(u64),
    /// Expecting the CRLF that ends a chunk's data.
    DataEnd,
    /// After the last chunk: trailer fields until an empty line.
    Trailers {
        fields: usize,
        bytes: usize,
    },
    Done,
}

/// An incremental decoder for a chunked request body (RFC 9112 section 7.1).
///
/// It consumes only complete syntax elements, so callers can drop consumed
/// input and keep at most one partial line, which is bounded. A chunk's size
/// is checked against the decoded limit before any of its data is accepted,
/// and nothing is ever allocated from a declared size: data is copied only
/// as it arrives.
struct ChunkDecoder {
    state: ChunkState,
    decoded: u64,
}

impl ChunkDecoder {
    fn new() -> Self {
        Self { state: ChunkState::Size, decoded: 0 }
    }

    fn is_done(&self) -> bool {
        self.state == ChunkState::Done
    }

    /// Decodes what `input` holds, appending chunk data to `out` (or
    /// discarding it), and returns how many bytes were consumed.
    fn decode(
        &mut self,
        input: &[u8],
        mut out: Option<&mut Vec<u8>>,
        limits: &ChunkLimits,
    ) -> Result<usize, BodyError> {
        let mut at = 0;
        loop {
            match self.state {
                ChunkState::Done => return Ok(at),
                ChunkState::Size => {
                    let Some(line) = take_line(&input[at..], MAX_CHUNK_LINE)? else {
                        return Ok(at)
                    };
                    let size = parse_chunk_size(line).ok_or(BodyError::Malformed)?;
                    at += line.len() + 2;
                    if size == 0 {
                        self.state = ChunkState::Trailers { fields: 0, bytes: 0 };
                    } else if size > limits.decoded.saturating_sub(self.decoded) {
                        return Err(BodyError::TooLarge)
                    } else {
                        self.state = ChunkState::Data(size);
                    }
                }
                ChunkState::Data(remaining) => {
                    let available = (input.len() - at) as u64;
                    if available == 0 {
                        return Ok(at)
                    }
                    let take = remaining.min(available);
                    // `take` is at most what `input` holds, so it fits.
                    let end = at + take as usize;
                    if let Some(out) = out.as_deref_mut() {
                        out.extend_from_slice(&input[at..end]);
                    }
                    at = end;
                    self.decoded += take;
                    self.state = if take == remaining {
                        ChunkState::DataEnd
                    } else {
                        ChunkState::Data(remaining - take)
                    };
                }
                ChunkState::DataEnd => match &input[at..] {
                    [b'\r', b'\n', ..] => {
                        at += 2;
                        self.state = ChunkState::Size;
                    }
                    [] | [b'\r'] => return Ok(at),
                    _ => return Err(BodyError::Malformed),
                },
                ChunkState::Trailers { fields, bytes } => {
                    let room = limits.trailer_bytes.saturating_sub(bytes);
                    let Some(line) = take_line(&input[at..], room)? else { return Ok(at) };
                    at += line.len() + 2;
                    if line.is_empty() {
                        self.state = ChunkState::Done;
                    } else if fields < limits.trailers && valid_field_line(line) {
                        // Trailers are validated and dropped, never exposed.
                        self.state = ChunkState::Trailers {
                            fields: fields + 1,
                            bytes: bytes + line.len() + 2,
                        };
                    } else {
                        return Err(BodyError::Malformed)
                    }
                }
            }
        }
    }
}

/// The CRLF-terminated line at the start of `input`, without its CRLF, or
/// `None` while it is incomplete. Bare CR or LF, and lines longer than `max`,
/// are malformed; only the first `max + 2` bytes are ever scanned.
fn take_line(input: &[u8], max: usize) -> Result<Option<&[u8]>, BodyError> {
    let longest = max.saturating_add(1);
    let window = &input[..input.len().min(longest.saturating_add(1))];
    let Some(lf) = window.iter().position(|&b| b == b'\n') else {
        // Without a LF in reach, the line is already longer than `max`.
        if window.len() > longest {
            return Err(BodyError::Malformed)
        }
        return match window.iter().position(|&b| b == b'\r') {
            Some(cr) if cr + 1 != window.len() => Err(BodyError::Malformed),
            _ => Ok(None),
        }
    };
    if lf == 0 || window[lf - 1] != b'\r' {
        return Err(BodyError::Malformed)
    }
    let line = &window[..lf - 1];
    if line.contains(&b'\r') {
        return Err(BodyError::Malformed)
    }
    Ok(Some(line))
}

/// `chunk-size [ chunk-ext ]`: one to sixteen hex digits, optionally followed
/// by extensions (ignored) that start with `;` after optional blanks and
/// contain no control characters other than HTAB.
fn parse_chunk_size(line: &[u8]) -> Option<u64> {
    let digits = line.iter().take_while(|b| b.is_ascii_hexdigit()).count();
    if digits == 0 || digits > 16 {
        return None
    }
    let size = u64::from_str_radix(std::str::from_utf8(&line[..digits]).ok()?, 16).ok()?;
    let extension = &line[digits..];
    let blanks = extension.iter().take_while(|b| matches!(b, b' ' | b'\t')).count();
    match &extension[blanks..] {
        [] if blanks == 0 => Some(size),
        [b';', rest @ ..] if rest.iter().all(|&b| field_value_byte(b)) => Some(size),
        _ => None,
    }
}

/// A trailer field line: a token name, a colon, and a field value.
fn valid_field_line(line: &[u8]) -> bool {
    let Some(colon) = line.iter().position(|&b| b == b':') else { return false };
    let (name, value) = (&line[..colon], &line[colon + 1..]);
    !name.is_empty() &&
        name.iter().all(|&b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)) &&
        value.iter().all(|&b| field_value_byte(b))
}

fn field_value_byte(b: u8) -> bool {
    b == b'\t' || (b' '..=b'~').contains(&b) || b >= 0x80
}

/// Clears a per-request buffer, releasing it if it grew large.
fn recycle_buffer(buf: &mut Vec<u8>) {
    if buf.capacity() > RETAINED_BUFFER_BYTES {
        *buf = Vec::new();
    } else {
        buf.clear();
    }
}

/// HTTP reason phrase for common status codes.
pub fn reason_phrase(status: u16) -> &'static str {
    match status {
        100 => "Continue",
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        411 => "Length Required",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

pub struct HttpRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub version: u8,
    pub headers: &'a [httparse::Header<'a>],
    pub body: &'a [u8],
}
impl<'a> HttpRequest<'a> {
    pub fn header(&self, name: &str) -> Option<&'a [u8]> {
        self.headers.iter().find(|h| h.name.eq_ignore_ascii_case(name)).map(|h| h.value)
    }
}
pub struct HttpResponse<'a> {
    pub version: u8,
    pub status: u16,
    pub reason: &'a str,
    pub headers: &'a [httparse::Header<'a>],
    pub body: &'a [u8],
}
impl<'a> HttpResponse<'a> {
    pub fn header(&self, name: &str) -> Option<&'a [u8]> {
        self.headers.iter().find(|h| h.name.eq_ignore_ascii_case(name)).map(|h| h.value)
    }
}

#[cfg(test)]
mod tests {
    use super::{BodyError, ChunkDecoder, ChunkLimits, HttpRequest};

    const LIMITS: ChunkLimits = ChunkLimits { decoded: 1024, trailer_bytes: 256, trailers: 4 };

    /// Feeds `input` in pieces of `step` bytes, as a connection buffer would
    /// receive it, keeping only what the decoder has not consumed.
    fn decode_in_steps(input: &[u8], step: usize) -> Result<(Vec<u8>, usize), BodyError> {
        let mut decoder = ChunkDecoder::new();
        let (mut buffer, mut out, mut fed) = (Vec::new(), Vec::new(), 0);
        while !decoder.is_done() {
            if fed == input.len() {
                return Err(BodyError::Incomplete)
            }
            let next = (fed + step).min(input.len());
            buffer.extend_from_slice(&input[fed..next]);
            fed = next;
            let consumed = decoder.decode(&buffer, Some(&mut out), &LIMITS)?;
            buffer.drain(..consumed);
        }
        Ok((out, buffer.len() + input.len() - fed))
    }

    #[test]
    fn chunked_bodies_decode_however_they_are_split() {
        let body = b"5\r\nhello\r\n6;name=value; flag\r\n world\r\nA \t;x\r\n0123456789\r\n0\r\nX-Sum: 1\r\n\r\nNEXT";
        for step in [1, 2, 3, 7, body.len()] {
            let (out, left) = decode_in_steps(body, step).unwrap();
            assert_eq!(out, b"hello world0123456789", "step {step}");
            assert_eq!(left, 4, "the next request stays unconsumed (step {step})");
        }
    }

    #[test]
    fn chunked_framing_errors_are_malformed() {
        for body in [
            b"\r\n".as_slice(),
            b"x\r\n",
            b"-1\r\n",
            b"+1\r\nx\r\n0\r\n\r\n",
            b" 1\r\nx\r\n0\r\n\r\n",
            b"1 \r\nx\r\n0\r\n\r\n",
            b"1\nx\r\n0\r\n\r\n",
            b"1\rx\r\n",
            b"1;\x00\r\nx\r\n",
            b"10000000000000000\r\n",
            b"1\r\nxy\r\n",
            b"1\r\nx\n0\r\n\r\n",
            b"0\r\nno colon\r\n\r\n",
            b"0\r\nBad Name: x\r\n\r\n",
            b"0\r\nX: \x01\r\n\r\n",
            b"0\r\nX: 1\n\r\n",
            b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
        ] {
            for step in [1, body.len()] {
                assert_eq!(
                    decode_in_steps(body, step),
                    Err(BodyError::Malformed),
                    "{:?} step {step}",
                    String::from_utf8_lossy(body)
                );
            }
        }
    }

    #[test]
    fn chunk_lines_and_trailers_are_bounded() {
        // An endless size line fails once it passes the line limit, without
        // waiting for a CR.
        let digits = vec![b'1'; super::MAX_CHUNK_LINE + 2];
        let mut decoder = ChunkDecoder::new();
        assert_eq!(decoder.decode(&digits, None, &LIMITS), Err(BodyError::Malformed));
        let mut decoder = ChunkDecoder::new();
        assert_eq!(decoder.decode(&digits[..super::MAX_CHUNK_LINE], None, &LIMITS), Ok(0));
        // Seventeen digits are too many even when the line is short.
        assert_eq!(decode_in_steps(b"00000000000000001\r\n", 1), Err(BodyError::Malformed));
        // Trailer count and size.
        let many = b"0\r\nA: 1\r\nB: 1\r\nC: 1\r\nD: 1\r\nE: 1\r\n\r\n";
        assert_eq!(decode_in_steps(many, 3), Err(BodyError::Malformed));
        let long = format!("0\r\nA: {}\r\n\r\n", "v".repeat(300));
        assert_eq!(decode_in_steps(long.as_bytes(), 5), Err(BodyError::Malformed));
    }

    #[test]
    fn chunk_sizes_are_checked_before_their_data() {
        // The largest possible size is refused from its line alone; nothing
        // is reserved for it.
        let mut decoder = ChunkDecoder::new();
        let mut out = Vec::new();
        assert_eq!(
            decoder.decode(b"ffffffffffffffff\r\n", Some(&mut out), &LIMITS),
            Err(BodyError::TooLarge)
        );
        assert_eq!(out.capacity(), 0);
        // The limit applies to the sum of the chunks.
        let body = format!("200\r\n{0}\r\n200\r\n{0}\r\n1\r\nx\r\n0\r\n\r\n", "a".repeat(512));
        assert_eq!(decode_in_steps(body.as_bytes(), 100), Err(BodyError::TooLarge));
        let exact = format!("200\r\n{0}\r\n200\r\n{0}\r\n0\r\n\r\n", "a".repeat(512));
        assert_eq!(decode_in_steps(exact.as_bytes(), 100).unwrap().0.len(), 1024);
        // Discarding decodes without keeping data.
        let mut decoder = ChunkDecoder::new();
        assert_eq!(decoder.decode(exact.as_bytes(), None, &LIMITS), Ok(exact.len()));
        assert!(decoder.is_done());
    }

    #[test]
    fn header_lookup_is_case_insensitive() {
        let headers = [httparse::Header { name: "Content-Type", value: b"text/plain" }];
        let request =
            HttpRequest { method: "GET", path: "/", version: 1, headers: &headers, body: &[] };
        assert_eq!(request.header("content-type"), Some(&b"text/plain"[..]));
    }
}
