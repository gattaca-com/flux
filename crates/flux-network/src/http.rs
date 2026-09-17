//! Poll-driven HTTP over [`crate::tcp::TcpNetwork`].
//!
//! [`HttpNetwork`] can listen for requests and maintain outbound endpoints in
//! one event loop. Events borrow parsed data only for the callback duration.
//! An [`HttpPool`] shares a request queue across several connections to one
//! address: [`HttpNetwork::send`] queues a request, its response carries the
//! [`RequestId`], and [`HttpEvent::Failed`] reports one that gets no response.
//!
//! ```no_run
//! use std::net::SocketAddr;
//! use flux_network::http::{HttpEvent, HttpNetwork};
//! let mut http = HttpNetwork::default();
//! http.listen("127.0.0.1:8080".parse::<SocketAddr>().unwrap())?;
//! let peer = http.connect("127.0.0.1:8081".parse::<SocketAddr>().unwrap());
//! loop {
//!     let mut response = None;
//!     let mut request = false;
//!     http.poll_with(|event| match event {
//!         HttpEvent::Request { token, .. } => response = Some(token),
//!         HttpEvent::Connected { token } if token == peer => request = true,
//!         _ => {}
//!     });
//!     if let Some(token) = response { http.respond(token, 200, &[], b"hello"); }
//!     if request { http.request(peer, "GET", "/", &[], &[]); }
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
//! TLS, HTTP/2, compression, trailer exposure, upgrades, and `WebSockets` are
//! not supported. Valid response trailers are parsed and discarded. There is no
//! half-close support. After an error response, the connection closes without a
//! lingering-close delay. Pipelined requests are served strictly one at a time
//! per connection.

use std::{
    collections::VecDeque,
    io::{self, Write as _},
    net::SocketAddr,
};

use flux_timing::{Duration, Instant};
use mio::Token;

use crate::tcp::{Framing, TcpEvent, TcpGroup, TcpGroupConfig, TcpNetwork};

pub enum HttpEvent<'a> {
    Accepted {
        token: Token,
        peer_addr: SocketAddr,
    },
    Connected {
        token: Token,
    },
    /// `id` is set for requests sent through a pool.
    Response {
        token: Token,
        id: Option<RequestId>,
        response: HttpResponse<'a>,
    },
    Request {
        token: Token,
        request: HttpRequest<'a>,
    },
    Disconnected {
        token: Token,
    },
    /// A pooled request that will get no response.
    Failed {
        id: RequestId,
        reason: Failure,
    },
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
    /// The connection was lost after the request went out; the server may or
    /// may not have handled it.
    Disconnected,
    TimedOut,
}
struct Queued {
    id: RequestId,
    method: String,
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
    Accepted { state: State, close: bool, continued: bool, head_request: bool },
    Outbound { addr: SocketAddr, method: Option<String>, in_flight: Option<InFlight> },
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
    network: TcpNetwork,
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
    failed: Vec<(RequestId, Failure)>,
}
impl Default for HttpNetwork {
    fn default() -> Self {
        Self {
            network: TcpNetwork::default(),
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
    /// Refuses pooled sends that would queue more than this many bytes.
    pub fn with_max_queued_bytes(mut self, max_queued_bytes: usize) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.max_queued_bytes = max_queued_bytes;
        self
    }
    /// Fails a pooled request with no response after this long and cycles its
    /// connection so a late response cannot be misattributed.
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.request_timeout = Some(request_timeout);
        self
    }
    pub fn max_body_bytes(&self) -> usize {
        self.max_body_bytes
    }
    fn group(&mut self) -> TcpGroup {
        let Self { network, group, name, max_head_bytes, max_body_bytes, socket_buf_size, .. } =
            self;
        *group.get_or_insert_with(|| {
            network.add_group(TcpGroupConfig {
                name,
                framing: Framing::Raw,
                socket_buf_size: *socket_buf_size,
                max_frame_size: usize::MAX,
                max_backlog_bytes: Some(max_head_bytes.saturating_add(*max_body_bytes)),
                backlog_warn_bytes: None,
                ..Default::default()
            })
        })
    }
    pub fn listen(&mut self, addr: SocketAddr) -> io::Result<()> {
        let group = self.group();
        self.network.listen(group, addr)
    }
    /// Immediately disconnects an accepted client.
    pub fn disconnect(&mut self, token: Token) -> bool {
        self.group.is_some() &&
            self.conns
                .iter()
                .any(|conn| conn.token == token && matches!(conn.role, Role::Accepted { .. })) &&
            self.network.disconnect(token)
    }
    /// Polls the network and delivers connection, request, and response events.
    ///
    /// A request event remains pending until [`Self::respond`] is called. The
    /// handler may defer that call until a later poll; requests behind it stay
    /// buffered until the response is sent.
    pub fn poll_with<F>(&mut self, mut handler: F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        let Some(group) = self.group else { return };
        let limit = self.buffer_limit();
        let conns = &mut self.conns;
        let lifecycle = &mut self.lifecycle;
        self.network.poll_with(|event| match event {
            TcpEvent::Accepted { group: event_group, token, peer_addr } if event_group == group => {
                conns.push(Conn {
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
                lifecycle.push(Lifecycle::Connected(token, Some(peer_addr)));
            }
            TcpEvent::Connected { group: event_group, token, .. } if event_group == group => {
                lifecycle.push(Lifecycle::Connected(token, None));
            }
            TcpEvent::Message { group: event_group, token, payload, .. }
                if event_group == group =>
            {
                if let Some(conn) = conns.iter_mut().find(|conn| conn.token == token) &&
                    !is_draining(&conn.role) &&
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
                lifecycle.push(Lifecycle::Disconnected(token));
            }
            _ => {}
        });
        for event in std::mem::take(&mut self.lifecycle) {
            self.emit_lifecycle(event, &mut handler);
        }
        self.parse_dirty(&mut handler);
        for conn in &mut self.conns {
            if conn.over_limit && !is_draining(&conn.role) {
                self.network.disconnect(conn.token);
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
                self.network.disconnect(token);
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
                if let Some(in_flight) = take_in_flight(&mut self.conns[i].role) {
                    self.failed.push((in_flight.queued.id, Failure::TimedOut));
                }
                set_outbound_method(&mut self.conns[i].role, None);
                self.network.disconnect(self.conns[i].token);
            }
        }
        for (id, reason) in std::mem::take(&mut self.failed) {
            handler(HttpEvent::Failed { id, reason });
        }
        self.dispatch();
    }
    fn emit_lifecycle<F>(&mut self, event: Lifecycle, handler: &mut F)
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
                        self.parse_connection(i, handler);
                    }
                    if matches!(self.conns[i].role, Role::Outbound { .. }) {
                        let answered = self.parse_eof_outbound(i, handler);
                        self.conns[i].buf.clear();
                        self.conns[i].dirty = false;
                        set_outbound_method(&mut self.conns[i].role, None);
                        if let Some(in_flight) = take_in_flight(&mut self.conns[i].role) &&
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
    fn parse_dirty<F>(&mut self, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        for i in 0..self.conns.len() {
            if self.conns[i].dirty {
                self.parse_connection(i, handler);
            }
        }
    }
    fn parse_connection<F>(&mut self, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        if matches!(self.conns[i].role, Role::Accepted { .. }) {
            self.parse_and_emit(i, handler);
        } else {
            self.parse_outbound(i, handler);
        }
    }
    pub fn connect(&mut self, addr: SocketAddr) -> Token {
        let group = self.group();
        let token = self.network.connect(group, addr);
        self.conns.push(Conn {
            token,
            buf: Vec::new(),
            dirty: false,
            over_limit: false,
            last_activity: Instant::now(),
            role: Role::Outbound { addr, method: None, in_flight: None },
        });
        token
    }
    /// Opens `connections` persistent connections to `addr` that share one
    /// request queue; see [`Self::send`].
    pub fn pool(&mut self, addr: SocketAddr, connections: usize) -> HttpPool {
        assert!(connections > 0, "a pool needs a connection");
        let pool = HttpPool(self.pools.len() as u32);
        let tokens = (0..connections).map(|_| self.connect(addr)).collect();
        self.pools.push(Pool {
            addr,
            tokens,
            queue: VecDeque::new(),
            queued_bytes: 0,
            next_seq: 0,
        });
        pool
    }
    /// Permanently removes an outbound endpoint and stops it reconnecting.
    pub fn remove(&mut self, token: Token) -> bool {
        if self.group.is_none() ||
            !self
                .conns
                .iter()
                .any(|conn| conn.token == token && matches!(conn.role, Role::Outbound { .. })) ||
            !self.network.remove(token)
        {
            return false
        }
        self.conns.retain(|conn| conn.token != token);
        true
    }
    /// Queues one request on an outbound endpoint; bodies over
    /// `max_body_bytes` are refused.
    pub fn request(
        &mut self,
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
            self.conns.iter_mut().find(|c| c.token == token && outbound_method(&c.role).is_none())
        else {
            return false
        };
        let Role::Outbound { addr, .. } = &c.role else { return false };
        let host = addr.to_string();
        let sent = self.network.send_with(token, |out| {
            write_head(out, method, path, headers, &host, body.len());
            out.extend_from_slice(body);
        });
        if sent {
            set_outbound_method(&mut c.role, Some(method.to_owned()));
        }
        sent
    }
    /// Queues one request on a pool; it goes out on the first idle connection
    /// and its response carries the returned id. `retries` is how many times
    /// it is resent after its connection is lost. Returns the body back when
    /// the request is invalid, over `max_body_bytes`, or the queue is full.
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
        write_head(&mut head, method, path, headers, &p.addr.to_string(), body.len());
        if p.queued_bytes + head.len() + body.len() > self.max_queued_bytes {
            return Err(body)
        }
        let id = RequestId { pool, seq: p.next_seq };
        p.next_seq += 1;
        p.queued_bytes += head.len() + body.len();
        p.queue.push_back(Queued { id, method: method.to_owned(), head, body, retries });
        self.dispatch();
        Ok(id)
    }
    fn dispatch(&mut self) {
        for p in 0..self.pools.len() {
            for t in 0..self.pools[p].tokens.len() {
                let token = self.pools[p].tokens[t];
                let Some(i) = self
                    .conns
                    .iter()
                    .position(|c| c.token == token && outbound_method(&c.role).is_none())
                else {
                    continue
                };
                let Some(next) = self.pools[p].queue.front() else { break };
                let sent = self.network.send_with(token, |out| {
                    out.extend_from_slice(&next.head);
                    out.extend_from_slice(&next.body);
                });
                if !sent {
                    continue
                }
                let queued = self.pools[p].queue.pop_front().unwrap();
                self.pools[p].queued_bytes -= queued.head.len() + queued.body.len();
                set_outbound_method(&mut self.conns[i].role, Some(queued.method.clone()));
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
    fn fail_outbound(&mut self, i: usize) {
        let token = self.conns[i].token;
        self.conns[i].buf.clear();
        set_outbound_method(&mut self.conns[i].role, None);
        self.network.disconnect(token);
    }
    fn buffer_limit(&self) -> usize {
        self.max_head_bytes.saturating_add(self.max_body_bytes)
    }
    fn parse_and_emit<F>(&mut self, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        if !matches!(accepted_state_mut(&mut self.conns[i].role), State::Idle) {
            return
        }
        let over_limit = self.conns[i].over_limit;
        let buf = &self.conns[i].buf;
        let mut hs = vec![httparse::EMPTY_HEADER; self.max_headers];
        let mut req = httparse::Request::new(&mut hs);
        let Ok(state) = req.parse(buf) else {
            self.error(i, 400);
            return
        };
        let httparse::Status::Complete(head) = state else {
            // A partial parse means every buffered byte is still head bytes.
            if !crlf_only(buf) {
                self.error(i, 400);
            } else if over_limit || buf.len() > self.max_head_bytes {
                self.error(i, 431);
            }
            return
        };
        if !crlf_only(&buf[..head]) {
            self.error(i, 400);
            return
        }
        if head > self.max_head_bytes {
            self.error(i, 431);
            return
        }
        let Some(len) = request_content_length(req.headers) else {
            self.error(i, 400);
            return
        };
        if req.headers.iter().any(|h| h.name.eq_ignore_ascii_case("transfer-encoding")) {
            self.error(i, 501);
            return
        }
        if len > self.max_body_bytes {
            self.error(i, 413);
            return
        }
        let Some(end) = head.checked_add(len) else {
            self.error(i, 413);
            return
        };
        if buf.len() < end {
            if over_limit {
                self.error(i, 413);
                return
            }
            if has_token(req.headers, "expect", b"100-continue") &&
                !accepted_continued_mut(&self.conns[i].role)
            {
                let token = self.conns[i].token;
                if self
                    .network
                    .send_with(token, |out| write!(out, "HTTP/1.1 100 Continue\r\n\r\n").unwrap())
                {
                    set_accepted_continued(&mut self.conns[i].role, true);
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
        set_accepted_state(&mut self.conns[i].role, State::Pending);
        set_accepted_close(&mut self.conns[i].role, close);
        set_accepted_continued(&mut self.conns[i].role, false);
        set_accepted_head_request(&mut self.conns[i].role, head_request);
    }
    fn error(&mut self, i: usize, status: u16) {
        set_accepted_state(&mut self.conns[i].role, State::Pending);
        set_accepted_close(&mut self.conns[i].role, true);
        let token = self.conns[i].token;
        let _ = self.respond(token, status, &[], &[]);
    }
    /// Sends the response for a pending request and returns whether it was
    /// queued.
    ///
    /// Call this from a request handler or later from the poll loop. Each call
    /// completes exactly one request for `token`.
    pub fn respond(
        &mut self,
        token: Token,
        status: u16,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> bool {
        let Some(i) = self
            .conns
            .iter()
            .position(|c| c.token == token && matches!(accepted_state(&c.role), State::Pending))
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
        let close = accepted_close(&self.conns[i].role) || caller_close;
        let suppress_body =
            accepted_head_request(&self.conns[i].role) || matches!(status, 100..=199 | 204 | 304);
        let include_length = !matches!(status, 100..=199 | 204);
        let ok = self.network.send_with(token, |out| {
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
            set_accepted_state(
                &mut self.conns[i].role,
                if close { State::Draining } else { State::Idle },
            );
            if close {
                self.network.disconnect_when_drained(token);
            }
        }
        ok
    }
    fn parse_outbound<F>(&mut self, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        self.conns[i].dirty = false;
        while outbound_method(&self.conns[i].role).is_some() {
            let b = &self.conns[i].buf;
            let mut hs = vec![httparse::EMPTY_HEADER; self.max_headers];
            let mut response = httparse::Response::new(&mut hs);
            let parsed = response.parse(b);
            let Ok(state) = parsed else {
                self.fail_outbound(i);
                return
            };
            let httparse::Status::Complete(head) = state else {
                // A partial parse means every buffered byte is still head bytes.
                if !crlf_only(b) || b.len() > self.max_head_bytes {
                    self.fail_outbound(i);
                }
                return
            };
            if !crlf_only(&b[..head]) || head > self.max_head_bytes {
                self.fail_outbound(i);
                return
            }
            let status = response.code.unwrap_or(0);
            let no_body = outbound_method(&self.conns[i].role).map(String::as_str) == Some("HEAD") ||
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
                self.fail_outbound(i);
                return
            }
            let (consumed, decoded) = if no_body {
                (head, None)
            } else if chunked == Some(true) {
                match Self::decode_chunked(&b[head..], self.max_body_bytes, self.max_headers) {
                    Ok(Some((consumed, decoded))) => {
                        let Some(consumed) = head.checked_add(consumed) else {
                            self.fail_outbound(i);
                            return
                        };
                        (consumed, Some(decoded))
                    }
                    Ok(None) => return,
                    Err(()) => {
                        self.fail_outbound(i);
                        return
                    }
                }
            } else if let ContentLength::Present(length) = content_length {
                let Some(consumed) = head.checked_add(length) else {
                    self.fail_outbound(i);
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
            let id = in_flight_id(&self.conns[i].role);
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
            set_outbound_method(&mut self.conns[i].role, None);
            take_in_flight(&mut self.conns[i].role);
            if close {
                self.network.disconnect(token);
                return
            }
        }
    }
    fn parse_eof_outbound<F>(&self, i: usize, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        if outbound_method(&self.conns[i].role).is_none() {
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
        let no_body = outbound_method(&self.conns[i].role).map(String::as_str) == Some("HEAD") ||
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
            id: in_flight_id(&self.conns[i].role),
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
fn write_head(
    out: &mut impl io::Write,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    host: &str,
    body_len: usize,
) {
    write!(out, "{method} {path} HTTP/1.1\r\n").unwrap();
    let mut has_host = false;
    for (n, v) in headers {
        has_host |= n.eq_ignore_ascii_case("host");
        write!(out, "{n}: {v}\r\n").unwrap();
    }
    if !has_host {
        write!(out, "Host: {host}\r\n").unwrap();
    }
    write!(out, "Content-Length: {body_len}\r\n\r\n").unwrap();
}
fn in_flight_id(role: &Role) -> Option<RequestId> {
    match role {
        Role::Outbound { in_flight: Some(in_flight), .. } => Some(in_flight.queued.id),
        _ => None,
    }
}
fn take_in_flight(role: &mut Role) -> Option<InFlight> {
    match role {
        Role::Outbound { in_flight, .. } => in_flight.take(),
        Role::Accepted { .. } => None,
    }
}
fn accepted_state(role: &Role) -> State {
    match role {
        Role::Accepted { state, .. } => *state,
        Role::Outbound { .. } => State::Draining,
    }
}
fn accepted_state_mut(role: &mut Role) -> &mut State {
    match role {
        Role::Accepted { state, .. } => state,
        Role::Outbound { .. } => panic!("accepted role"),
    }
}
fn is_draining(role: &Role) -> bool {
    matches!(role, Role::Accepted { state: State::Draining, .. })
}
fn set_accepted_state(role: &mut Role, state: State) {
    *accepted_state_mut(role) = state;
}
fn accepted_close(role: &Role) -> bool {
    matches!(role, Role::Accepted { close: true, .. })
}
fn set_accepted_close(role: &mut Role, close: bool) {
    if let Role::Accepted { close: current, .. } = role {
        *current = close;
    }
}
fn accepted_continued_mut(role: &Role) -> bool {
    matches!(role, Role::Accepted { continued: true, .. })
}
fn set_accepted_continued(role: &mut Role, continued: bool) {
    if let Role::Accepted { continued: current, .. } = role {
        *current = continued;
    }
}
fn accepted_head_request(role: &Role) -> bool {
    matches!(role, Role::Accepted { head_request: true, .. })
}
fn set_accepted_head_request(role: &mut Role, head_request: bool) {
    if let Role::Accepted { head_request: current, .. } = role {
        *current = head_request;
    }
}
fn outbound_method(role: &Role) -> Option<&String> {
    match role {
        Role::Outbound { method, .. } => method.as_ref(),
        Role::Accepted { .. } => None,
    }
}
fn set_outbound_method(role: &mut Role, method: Option<String>) {
    if let Role::Outbound { method: current, .. } = role {
        *current = method;
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
