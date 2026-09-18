//! Poll-driven HTTP over a caller-owned [`crate::tcp::TcpNetworkCore`],
//! sharing one poll with the tile's other traffic.
//!
//! [`HttpNetwork`] can listen for requests and maintain outbound endpoints in
//! one event loop: [`HttpNetwork::on_event`] claims its group's events from
//! the network's handler and [`HttpNetwork::drive`] delivers them and sends
//! queued requests. Events borrow parsed data only for the callback duration.
//!
//! ```no_run
//! use std::net::SocketAddr;
//! use flux_network::{http::{HttpEvent, HttpNetwork}, tcp::TcpNetwork};
//! let mut net = TcpNetwork::default();
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
//! HTTP/1.1 and HTTP/1.0 responses are supported. Request bodies require
//! `Content-Length`; chunked requests are rejected with `501`. Response bodies
//! may use `Content-Length`, chunked transfer coding, or EOF delimiting.
//! `Expect: 100-continue` is handled automatically.
//!
//! Server-side TLS, HTTP/2, compression, trailer exposure, upgrades, and
//! `WebSockets` are not supported. Valid response trailers are parsed and
//! discarded. There is no half-close support. After an error response, the
//! connection closes without a lingering-close delay. Pipelined requests are
//! served strictly one at a time per connection.

#[cfg(feature = "tls")]
use std::sync::Arc;
use std::{
    collections::VecDeque,
    io::{self, Write as _},
    net::SocketAddr,
};

use flux_timing::{Duration, Instant};
use mio::Token;

use crate::tcp::{Framing, TcpEvent, TcpGroup, TcpGroupConfig, TcpNetworkCore};

/// Record overhead allowance on top of a full-size request; see `group`.
const TLS_MARGIN_BYTES: usize = 64 * 1024;

pub enum HttpEvent<'a> {
    Accepted { token: Token, peer_addr: SocketAddr },
    Connected { token: Token },
    Response { token: Token, id: Option<RequestId>, response: HttpResponse<'a> },
    Request { token: Token, request: HttpRequest<'a> },
    Disconnected { token: Token },
    Failed { id: RequestId, reason: Failure },
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
#[derive(Clone, Copy)]
enum State {
    Idle,
    Pending,
    Draining,
}
enum Role {
    Accepted {
        state: State,
        close: bool,
        continued: bool,
        head_request: bool,
    },
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
}
#[derive(Clone, Copy)]
enum Lifecycle {
    Connected(Token, Option<SocketAddr>),
    Disconnected(Token),
}
pub struct HttpNetwork {
    group: Option<TcpGroup>,
    name: &'static str,
    max_head_bytes: usize,
    max_body_bytes: usize,
    max_headers: usize,
    idle_timeout: Option<Duration>,
    socket_buf_size: Option<usize>,
    conns: Vec<Conn>,
    lifecycle: Vec<Lifecycle>,
    pools: Vec<Pool>,
    max_queued_bytes: usize,
    request_timeout: Option<Duration>,
    #[cfg(feature = "tls")]
    tls_config: Option<Arc<crate::tls::ClientConfig>>,
    failed: Vec<(RequestId, Failure)>,
}
impl Default for HttpNetwork {
    fn default() -> Self {
        Self {
            group: None,
            name: "http",
            max_head_bytes: 16 * 1024,
            max_body_bytes: 1024 * 1024,
            max_headers: 64,
            idle_timeout: Some(Duration::from_secs(30)),
            socket_buf_size: None,
            conns: Vec::new(),
            lifecycle: Vec::new(),
            pools: Vec::new(),
            max_queued_bytes: usize::MAX,
            request_timeout: None,
            #[cfg(feature = "tls")]
            tls_config: None,
            failed: Vec::new(),
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
    /// Sets the TCP socket buffer size.
    pub fn with_socket_buf_size(mut self, socket_buf_size: usize) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.socket_buf_size = Some(socket_buf_size);
        self
    }
    /// Sets the idle timeout for accepted connections; outbound endpoints
    /// remain persistent.
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
    fn group(&mut self, net: &mut TcpNetworkCore) -> TcpGroup {
        let Self { group, name, max_head_bytes, max_body_bytes, socket_buf_size, .. } = self;
        *group.get_or_insert_with(|| {
            net.add_group(TcpGroupConfig {
                name,
                framing: Framing::Raw,
                socket_buf_size: *socket_buf_size,
                max_frame_size: usize::MAX,
                // The backlog counts wire bytes, which are ciphertext on a
                // TLS endpoint: a record adds a header and a tag per 16KB.
                // The margin keeps a full-size request from tripping the cap
                // on its own overhead.
                max_backlog_bytes: Some(
                    max_head_bytes.saturating_add(*max_body_bytes).saturating_add(TLS_MARGIN_BYTES),
                ),
                backlog_warn_bytes: None,
                ..Default::default()
            })
        })
    }
    pub fn listen(&mut self, net: &mut TcpNetworkCore, addr: SocketAddr) -> io::Result<()> {
        let group = self.group(net);
        net.listen(group, addr)
    }
    /// Immediately disconnects an accepted client.
    pub fn disconnect(&mut self, net: &mut TcpNetworkCore, token: Token) -> bool {
        self.group.is_some() &&
            self.conns
                .iter()
                .any(|conn| conn.token == token && matches!(conn.role, Role::Accepted { .. })) &&
            net.disconnect(token)
    }
    /// Takes in one network event; returns whether it belonged to this
    /// layer's group. Call from the network's `poll_with` handler.
    pub fn on_event(&mut self, event: &TcpEvent<'_>) -> bool {
        let Some(group) = self.group else { return false };
        match *event {
            TcpEvent::Accepted { group: event_group, token, peer_addr } if event_group == group => {
                self.conns.push(Conn {
                    token,
                    buf: Vec::new(),
                    dirty: false,
                    over_limit: false,
                    last_activity: Instant::now(),
                    role: Role::Accepted {
                        state: State::Idle,
                        close: false,
                        continued: false,
                        head_request: false,
                    },
                });
                self.lifecycle.push(Lifecycle::Connected(token, Some(peer_addr)));
            }
            TcpEvent::Connected { group: event_group, token, .. } if event_group == group => {
                self.lifecycle.push(Lifecycle::Connected(token, None));
            }
            TcpEvent::Message { group: event_group, token, payload, .. }
                if event_group == group =>
            {
                let limit = self.buffer_limit();
                if let Some(conn) = self.conns.iter_mut().find(|conn| conn.token == token) &&
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
            TcpEvent::Disconnected { group: event_group, token, .. } if event_group == group => {
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
    pub fn drive<F>(&mut self, net: &mut TcpNetworkCore, mut handler: F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        for event in std::mem::take(&mut self.lifecycle) {
            self.emit_lifecycle(net, event, &mut handler);
        }
        self.parse_dirty(net, &mut handler);
        for conn in &mut self.conns {
            if conn.over_limit && !conn.role.is_draining() {
                net.disconnect(conn.token);
                conn.over_limit = false;
            }
        }
        if let Some(timeout) = self.idle_timeout {
            let expired: Vec<_> = self
                .conns
                .iter()
                .filter(|conn| {
                    matches!(conn.role, Role::Accepted { .. }) &&
                        conn.last_activity.elapsed() >= timeout
                })
                .map(|conn| conn.token)
                .collect();
            for token in expired {
                net.disconnect(token);
            }
        }
        if let Some(timeout) = self.request_timeout {
            for i in 0..self.conns.len() {
                let Role::Outbound { in_flight: Some(in_flight), .. } = &self.conns[i].role else {
                    continue
                };
                if in_flight.sent_at.elapsed() < timeout {
                    continue
                }
                if let Some(in_flight) = self.conns[i].role.take_in_flight() {
                    self.failed.push((in_flight.queued.id, Failure::TimedOut));
                }
                self.conns[i].role.set_outbound_head(None);
                net.disconnect(self.conns[i].token);
            }
        }
        for (id, reason) in std::mem::take(&mut self.failed) {
            handler(HttpEvent::Failed { id, reason });
        }
        self.dispatch(net);
    }
    fn emit_lifecycle<F>(&mut self, net: &mut TcpNetworkCore, event: Lifecycle, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        match event {
            Lifecycle::Connected(token, Some(peer_addr)) => {
                handler(HttpEvent::Accepted { token, peer_addr });
            }
            Lifecycle::Connected(token, None) => handler(HttpEvent::Connected { token }),
            Lifecycle::Disconnected(token) => {
                if let Some(i) = self.conns.iter().position(|conn| conn.token == token) {
                    if self.conns[i].dirty {
                        self.parse_connection(net, i, handler);
                    }
                    if matches!(self.conns[i].role, Role::Outbound { .. }) {
                        let answered = self.parse_eof_outbound(i, handler);
                        self.conns[i].buf.clear();
                        self.conns[i].dirty = false;
                        self.conns[i].role.set_outbound_head(None);
                        if let Some(in_flight) = self.conns[i].role.take_in_flight() &&
                            !answered
                        {
                            self.requeue_or_fail(in_flight.queued);
                        }
                    } else {
                        self.conns.remove(i);
                    }
                }
                handler(HttpEvent::Disconnected { token });
            }
        }
    }
    fn parse_dirty<F>(&mut self, net: &mut TcpNetworkCore, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        for i in 0..self.conns.len() {
            if self.conns[i].dirty {
                self.parse_connection(net, i, handler);
            }
        }
    }
    fn parse_connection<F>(&mut self, net: &mut TcpNetworkCore, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        if matches!(self.conns[i].role, Role::Accepted { .. }) {
            self.parse_and_emit(net, i, handler);
        } else {
            self.parse_outbound(net, i, handler);
        }
    }
    pub fn connect(&mut self, net: &mut TcpNetworkCore, addr: SocketAddr) -> Token {
        let group = self.group(net);
        let token = net.connect(group, addr);
        self.track_outbound(token, addr)
    }
    /// Like [`Self::connect`] but negotiates TLS once the TCP connect
    /// completes, verifying against the Mozilla roots and sending SNI
    /// `server`. Panics if `server` is not a valid DNS name or IP.
    #[cfg(feature = "tls")]
    pub fn connect_tls(
        &mut self,
        net: &mut TcpNetworkCore,
        addr: SocketAddr,
        server: &str,
    ) -> Token {
        let session = crate::tls::Session::new(server);
        let session = match &self.tls_config {
            Some(config) => session.with_config(config.clone()),
            None => session,
        };
        let group = self.group(net);
        let token = net.connect_tls(group, addr, session);
        self.track_outbound(token, addr)
    }
    fn track_outbound(&mut self, token: Token, addr: SocketAddr) -> Token {
        self.conns.push(Conn {
            token,
            buf: Vec::new(),
            dirty: false,
            over_limit: false,
            last_activity: Instant::now(),
            role: Role::Outbound { addr, in_flight_head: None, in_flight: None },
        });
        token
    }
    /// Opens `connections` persistent connections to `addr` that share one
    /// request queue; see [`Self::send`].
    pub fn pool(
        &mut self,
        net: &mut TcpNetworkCore,
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
        net: &mut TcpNetworkCore,
        addr: SocketAddr,
        server: &str,
        connections: usize,
    ) -> HttpPool {
        self.pool_with(net, addr, connections, |http, net| http.connect_tls(net, addr, server))
    }
    fn pool_with(
        &mut self,
        net: &mut TcpNetworkCore,
        addr: SocketAddr,
        connections: usize,
        mut open: impl FnMut(&mut Self, &mut TcpNetworkCore) -> Token,
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
    /// Removes every connection and outbound endpoint; pending requests and
    /// responses are dropped without events. Listeners stay registered, as
    /// [`TcpNetworkCore`] has no way to remove them.
    pub fn close(self, net: &mut TcpNetworkCore) {
        for conn in &self.conns {
            net.remove(conn.token);
        }
    }
    /// Permanently removes an outbound endpoint and stops it reconnecting.
    pub fn remove(&mut self, net: &mut TcpNetworkCore, token: Token) -> bool {
        if self.group.is_none() ||
            !self
                .conns
                .iter()
                .any(|conn| conn.token == token && matches!(conn.role, Role::Outbound { .. })) ||
            !net.remove(token)
        {
            return false
        }
        self.conns.retain(|conn| conn.token != token);
        true
    }
    /// Sends one request on an outbound endpoint.
    pub fn request(
        &mut self,
        net: &mut TcpNetworkCore,
        token: Token,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> bool {
        if !valid_request(method, path, headers) || body.len() > self.max_body_bytes {
            return false
        }
        let Some(c) =
            self.conns.iter_mut().find(|c| c.token == token && c.role.outbound_head().is_none())
        else {
            return false
        };
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
    /// Queues one request on a pool for the next [`Self::drive`] to send,
    /// handing the body back when it exceeds `max_body_bytes` or the queue
    /// is full.
    pub fn send(
        &mut self,
        pool: HttpPool,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
        retries: u8,
    ) -> Result<RequestId, Vec<u8>> {
        let p = &mut self.pools[pool.0 as usize];
        if !valid_request(method, path, headers) || body.len() > self.max_body_bytes {
            return Err(body)
        }
        let mut head = Vec::new();
        write_head(&mut head, method, path, headers, &p.addr, body.len());
        if p.queued_bytes + head.len() + body.len() > self.max_queued_bytes {
            return Err(body)
        }
        let id = RequestId { pool, seq: p.next_seq };
        p.next_seq += 1;
        p.queued_bytes += head.len() + body.len();
        p.queue.push_back(Queued {
            id,
            head_request: method.eq_ignore_ascii_case("HEAD"),
            head,
            body,
            retries,
        });
        Ok(id)
    }
    fn dispatch(&mut self, net: &mut TcpNetworkCore) {
        for p in 0..self.pools.len() {
            for t in 0..self.pools[p].tokens.len() {
                let token = self.pools[p].tokens[t];
                let Some(i) = self
                    .conns
                    .iter()
                    .position(|c| c.token == token && c.role.outbound_head().is_none())
                else {
                    continue
                };
                let Some(front) = self.pools[p].queue.front() else { break };
                let sent = net.send_with(token, |out| {
                    out.extend_from_slice(&front.head);
                    out.extend_from_slice(&front.body);
                });
                if !sent {
                    continue
                }
                let queued = self.pools[p].queue.pop_front().unwrap();
                self.pools[p].queued_bytes -= queued.head.len() + queued.body.len();
                self.conns[i].role.set_outbound_head(Some(queued.head_request));
                if let Role::Outbound { in_flight, .. } = &mut self.conns[i].role {
                    *in_flight = Some(InFlight { queued, sent_at: Instant::now() });
                }
            }
        }
    }
    fn requeue_or_fail(&mut self, mut queued: Queued) {
        if queued.retries == 0 {
            self.failed.push((queued.id, Failure::Disconnected));
            return
        }
        queued.retries -= 1;
        let pool = &mut self.pools[queued.id.pool.0 as usize];
        pool.queued_bytes += queued.head.len() + queued.body.len();
        pool.queue.push_front(queued);
    }
    fn fail_outbound(&mut self, net: &mut TcpNetworkCore, i: usize) {
        let token = self.conns[i].token;
        self.conns[i].buf.clear();
        self.conns[i].role.set_outbound_head(None);
        net.disconnect(token);
    }
    fn buffer_limit(&self) -> usize {
        self.max_head_bytes.saturating_add(self.max_body_bytes)
    }
    fn parse_and_emit<F>(&mut self, net: &mut TcpNetworkCore, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        if !matches!(self.conns[i].role.accepted_state_mut(), State::Idle) {
            return
        }
        let over_limit = self.conns[i].over_limit;
        let buf = &self.conns[i].buf;
        let mut hs = vec![httparse::EMPTY_HEADER; self.max_headers];
        let mut req = httparse::Request::new(&mut hs);
        let Ok(state) = req.parse(buf) else {
            self.error(net, i, 400);
            return
        };
        let httparse::Status::Complete(head) = state else {
            // A partial parse means every buffered byte is still head bytes.
            if !crlf_only(buf) {
                self.error(net, i, 400);
            } else if over_limit || buf.len() > self.max_head_bytes {
                self.error(net, i, 431);
            }
            return
        };
        if !crlf_only(&buf[..head]) {
            self.error(net, i, 400);
            return
        }
        if head > self.max_head_bytes {
            self.error(net, i, 431);
            return
        }
        let Some(len) = request_content_length(req.headers) else {
            self.error(net, i, 400);
            return
        };
        if req.headers.iter().any(|h| h.name.eq_ignore_ascii_case("transfer-encoding")) {
            self.error(net, i, 501);
            return
        }
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
            if has_token(req.headers, "expect", b"100-continue") &&
                !self.conns[i].role.accepted_continued()
            {
                let token = self.conns[i].token;
                if net.send_with(token, |out| write!(out, "HTTP/1.1 100 Continue\r\n\r\n").unwrap())
                {
                    self.conns[i].role.set_accepted_continued(true);
                }
            }
            return
        }
        let close = req.version == Some(0) && !has_token(req.headers, "connection", b"keep-alive") ||
            has_token(req.headers, "connection", b"close");
        let token = self.conns[i].token;
        let head_request = req.method == Some("HEAD");
        let request = HttpRequest {
            method: req.method.unwrap_or(""),
            path: req.path.unwrap_or(""),
            version: req.version.unwrap_or(1),
            headers: req.headers,
            body: &buf[head..end],
        };
        handler(HttpEvent::Request { token, request });
        self.conns[i].buf.drain(..end);
        self.conns[i].dirty = !self.conns[i].buf.is_empty();
        self.conns[i].role.set_accepted_state(State::Pending);
        self.conns[i].role.set_accepted_close(close);
        self.conns[i].role.set_accepted_continued(false);
        self.conns[i].role.set_accepted_head_request(head_request);
    }
    fn error(&mut self, net: &mut TcpNetworkCore, i: usize, status: u16) {
        self.conns[i].role.set_accepted_state(State::Pending);
        self.conns[i].role.set_accepted_close(true);
        let token = self.conns[i].token;
        let _ = self.respond(net, token, status, &[], &[]);
    }
    /// Sends the response for a pending request and returns whether it was
    /// queued.
    ///
    /// Call this after the drive that delivered the request, or from a later
    /// one. Each call completes exactly one request for `token`.
    pub fn respond(
        &mut self,
        net: &mut TcpNetworkCore,
        token: Token,
        status: u16,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> bool {
        let Some(i) = self
            .conns
            .iter()
            .position(|c| c.token == token && matches!(c.role.accepted_state(), State::Pending))
        else {
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
        let close = self.conns[i].role.accepted_close() || caller_close;
        let suppress_body =
            self.conns[i].role.accepted_head_request() || matches!(status, 100..=199 | 204 | 304);
        let include_length = !matches!(status, 100..=199 | 204);
        let ok = net.send_with(token, |out| {
            write!(out, "HTTP/1.1 {status} {}\r\n", reason_phrase(status)).unwrap();
            // Caller Connection headers only feed the close decision; exactly
            // one canonical Connection header is always written below.
            for (n, v) in headers {
                if n.eq_ignore_ascii_case("connection") {
                    continue
                }
                out.extend_from_slice(n.as_bytes());
                out.extend_from_slice(b": ");
                out.extend_from_slice(v.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            if include_length {
                write!(out, "Content-Length: {}\r\n", body.len()).unwrap();
            }
            out.extend_from_slice(if close {
                b"Connection: close\r\n"
            } else {
                b"Connection: keep-alive\r\n"
            });
            out.extend_from_slice(b"\r\n");
            if !suppress_body {
                out.extend_from_slice(body);
            }
        });
        if ok {
            self.conns[i].dirty = !close && !self.conns[i].buf.is_empty();
            self.conns[i].role.set_accepted_state(if close {
                State::Draining
            } else {
                State::Idle
            });
            if close {
                net.disconnect_when_drained(token);
            }
        }
        ok
    }
    fn parse_outbound<F>(&mut self, net: &mut TcpNetworkCore, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        self.conns[i].dirty = false;
        while self.conns[i].role.outbound_head().is_some() {
            let b = &self.conns[i].buf;
            let mut hs = vec![httparse::EMPTY_HEADER; self.max_headers];
            let mut response = httparse::Response::new(&mut hs);
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
            let (consumed, decoded) = if no_body {
                (head, None)
            } else if chunked == Some(true) {
                match Self::decode_chunked(&b[head..], self.max_body_bytes, self.max_headers) {
                    Ok(Some((consumed, decoded))) => {
                        let Some(consumed) = head.checked_add(consumed) else {
                            self.fail_outbound(net, i);
                            return
                        };
                        (consumed, Some(decoded))
                    }
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
                (consumed, None)
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
                } else if let Some(decoded) = decoded.as_deref() {
                    decoded
                } else {
                    &b[head..consumed]
                },
            };
            handler(HttpEvent::Response { token, id, response: response_event });
            self.conns[i].buf.drain(..consumed);
            self.conns[i].dirty = !self.conns[i].buf.is_empty();
            self.conns[i].role.set_outbound_head(None);
            self.conns[i].role.take_in_flight();
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
        let mut headers = vec![httparse::EMPTY_HEADER; self.max_headers];
        let mut response = httparse::Response::new(&mut headers);
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
    fn decode_chunked(
        bytes: &[u8],
        max_body_bytes: usize,
        max_headers: usize,
    ) -> Result<Option<(usize, Vec<u8>)>, ()> {
        let Some((end, body_len)) = Self::chunked_end(bytes, max_body_bytes, max_headers)? else {
            return Ok(None)
        };
        let mut body = Vec::with_capacity(body_len);
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
                return Ok(Some((end, body)))
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
                let mut headers = vec![httparse::EMPTY_HEADER; max_headers];
                let httparse::Status::Complete((consumed, _)) =
                    httparse::parse_headers(&bytes[at..], &mut headers).map_err(|_| ())?
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

fn crlf_only(bytes: &[u8]) -> bool {
    bytes.iter().enumerate().all(|(i, b)| *b != b'\n' || i > 0 && bytes[i - 1] == b'\r')
}
fn request_content_length(headers: &[httparse::Header<'_>]) -> Option<usize> {
    let mut length = None;
    for header in headers.iter().filter(|h| h.name.eq_ignore_ascii_case("content-length")) {
        let value = std::str::from_utf8(header.value).ok()?;
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return None
        }
        let parsed = value.parse().ok()?;
        if length.replace(parsed).is_some_and(|previous| previous != parsed) {
            return None
        }
    }
    Some(length.unwrap_or(0))
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
    write!(out, "{method} {path} HTTP/1.1\r\n").unwrap();
    let mut has_host = false;
    for (n, v) in headers {
        has_host |= n.eq_ignore_ascii_case("host");
        write!(out, "{n}: {v}\r\n").unwrap();
    }
    if !has_host {
        write!(out, "Host: {addr}\r\n").unwrap();
    }
    write!(out, "Content-Length: {body_len}\r\n\r\n").unwrap();
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
            Self::Accepted { .. } => None,
        }
    }
    fn accepted_state(&self) -> State {
        match self {
            Self::Accepted { state, .. } => *state,
            Self::Outbound { .. } => State::Draining,
        }
    }
    fn accepted_state_mut(&mut self) -> &mut State {
        match self {
            Self::Accepted { state, .. } => state,
            Self::Outbound { .. } => panic!("accepted role"),
        }
    }
    fn is_draining(&self) -> bool {
        matches!(self, Self::Accepted { state: State::Draining, .. })
    }
    fn set_accepted_state(&mut self, state: State) {
        *self.accepted_state_mut() = state;
    }
    fn accepted_close(&self) -> bool {
        matches!(self, Self::Accepted { close: true, .. })
    }
    fn set_accepted_close(&mut self, close: bool) {
        if let Self::Accepted { close: current, .. } = self {
            *current = close;
        }
    }
    fn accepted_continued(&self) -> bool {
        matches!(self, Self::Accepted { continued: true, .. })
    }
    fn set_accepted_continued(&mut self, continued: bool) {
        if let Self::Accepted { continued: current, .. } = self {
            *current = continued;
        }
    }
    fn accepted_head_request(&self) -> bool {
        matches!(self, Self::Accepted { head_request: true, .. })
    }
    fn set_accepted_head_request(&mut self, head_request: bool) {
        if let Self::Accepted { head_request: current, .. } = self {
            *current = head_request;
        }
    }
    fn outbound_head(&self) -> Option<bool> {
        match self {
            Self::Outbound { in_flight_head, .. } => *in_flight_head,
            Self::Accepted { .. } => None,
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
    use super::HttpRequest;

    #[test]
    fn header_lookup_is_case_insensitive() {
        let headers = [httparse::Header { name: "Content-Type", value: b"text/plain" }];
        let request =
            HttpRequest { method: "GET", path: "/", version: 1, headers: &headers, body: &[] };
        assert_eq!(request.header("content-type"), Some(&b"text/plain"[..]));
    }
}
