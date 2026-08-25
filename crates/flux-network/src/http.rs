//! Poll-driven HTTP over [`crate::stream::StreamNetwork`].
//!
//! [`HttpService`] is a service: it owns one raw-framed [`ConnectionGroup`]
//! inside a network the caller drives, serving requests on that group's
//! listeners and issuing requests on its outbound endpoints. One call per
//! iteration drives the network, and the service's protocol events are then
//! pulled from it.
//!
//! ```no_run
//! use std::net::SocketAddr;
//!
//! use flux_network::http::{HttpConfig, HttpEvent, HttpService};
//! use flux_network::stream::{Endpoint, Framing, ConnectionGroupConfig, StreamNetwork};
//! use flux_timing::Duration;
//!
//! let mut net = StreamNetwork::default();
//! let group = net.add_group(ConnectionGroupConfig {
//!     name: "api",
//!     framing: Framing::Raw,
//!     ..ConnectionGroupConfig::default()
//! });
//! let mut http = HttpService::new(&mut net, group, HttpConfig::default());
//! http.listen(&mut net, Endpoint::Tcp("127.0.0.1:8080".parse::<SocketAddr>().unwrap()))?;
//! let upstream = http.connect(&mut net, Endpoint::Unix("/run/flux/upstream.sock".into()));
//!
//! let mut slow = Vec::new();
//! let mut ask_upstream = false;
//! loop {
//!     net.drive(Some(Duration::from_millis(10)), &mut [http.as_service()], |_| {});
//!     while let Some(event) = http.next_event(&mut net) {
//!         match event {
//!             HttpEvent::Request { token, request, responder } => {
//!                 if request.path == "/health" {
//!                     responder.respond(200, &[], b"ok");
//!                 } else {
//!                     // Dropping the responder answers later, by token.
//!                     slow.push(token);
//!                 }
//!             }
//!             HttpEvent::Connected { token } if token == upstream => ask_upstream = true,
//!             HttpEvent::Response { response, .. } => println!("{}", response.status),
//!             _ => {}
//!         }
//!     }
//!     for token in slow.drain(..) {
//!         http.respond(&mut net, token, 200, &[], b"done");
//!     }
//!     if std::mem::take(&mut ask_upstream) {
//!         http.request(&mut net, upstream, "GET", "/", &[], &[]);
//!     }
//! }
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! # The pull loop
//! An event borrows the service and the network for as long as it lives, so
//! nothing in the loop body may touch either: everything a handler needs
//! arrives through the event, and the [`Responder`] on a request is the only
//! way to answer from there. Events cannot be stored or cloned. A caller may
//! stop pulling after any number of events and resume in a later iteration
//! with nothing lost.
//!
//! Dropping a [`Responder`] without responding defers the response to
//! [`HttpService::respond`], which answers the same request by token; the
//! connection serves no further request until it is answered, and a request
//! never answered is closed by the idle sweep.
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
    io::{self, Write as _},
    ops::Range,
};

use flux_timing::{Duration, Instant};
use mio::Token;

use crate::stream::{
    ConnectionGroup, Endpoint, Framing, Peer, ServiceRef, StreamEvent, StreamNetwork, private,
};

/// HTTP parsing and connection-state policy. Transport and queue policy
/// belongs to the group the service claims.
#[derive(Clone, Copy, Debug)]
pub struct HttpConfig {
    /// Largest message head accepted before rejecting it with `431`.
    pub max_head_bytes: usize,
    /// Largest message body accepted before rejecting it with `413`.
    pub max_body_bytes: usize,
    /// Largest number of headers parsed in one message.
    pub max_headers: usize,
    /// How long an accepted connection may sit without inbound bytes before
    /// it is closed. Outbound endpoints stay connected.
    pub idle_timeout: Option<Duration>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            max_head_bytes: 16 * 1024,
            max_body_bytes: 1024 * 1024,
            max_headers: 64,
            idle_timeout: Some(Duration::from_secs(30)),
        }
    }
}

impl HttpConfig {
    /// Sets the maximum message head size before rejecting it.
    pub fn with_max_head_bytes(mut self, max_head_bytes: usize) -> Self {
        self.max_head_bytes = max_head_bytes;
        self
    }

    /// Sets the maximum message body size before rejecting it.
    pub fn with_max_body_bytes(mut self, max_body_bytes: usize) -> Self {
        self.max_body_bytes = max_body_bytes;
        self
    }

    /// Sets the maximum number of headers parsed in one message.
    pub fn with_max_headers(mut self, max_headers: usize) -> Self {
        self.max_headers = max_headers;
        self
    }

    /// Sets the idle timeout for accepted connections.
    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = Some(idle_timeout);
        self
    }

    /// Disables the idle connection sweep.
    pub fn without_idle_timeout(mut self) -> Self {
        self.idle_timeout = None;
        self
    }

    /// The most a connection may hold unanswered: one head plus one body.
    fn buffer_limit(&self) -> usize {
        self.max_head_bytes.saturating_add(self.max_body_bytes)
    }
}

/// Event pulled from an [`HttpService`].
pub enum HttpEvent<'a> {
    /// A listener accepted a client.
    Accepted { token: Token, peer: Peer },
    /// An outbound endpoint established its connection.
    Connected { token: Token },
    /// A client sent a request, with the means to answer it.
    Request { token: Token, request: HttpRequest<'a>, responder: Responder<'a> },
    /// An outbound endpoint answered a request.
    Response { token: Token, response: HttpResponse<'a> },
    /// A connection closed.
    Disconnected { token: Token },
}

/// A parsed request, borrowed from the connection that sent it.
pub struct HttpRequest<'a> {
    /// The request method.
    pub method: &'a str,
    /// The request target.
    pub path: &'a str,
    /// The HTTP minor version: `1` for HTTP/1.1.
    pub version: u8,
    /// The request body, empty when the request carried none.
    pub body: &'a [u8],
    buffer: &'a [u8],
    headers: &'a [HeaderRange],
}

impl<'a> HttpRequest<'a> {
    /// The value of the first header with this name, matched case
    /// insensitively.
    pub fn header(&self, name: &str) -> Option<&'a [u8]> {
        header(self.buffer, self.headers, name)
    }

    /// Every header, in the order the sender wrote them.
    pub fn headers(&self) -> impl Iterator<Item = (&'a str, &'a [u8])> + use<'a> {
        headers(self.buffer, self.headers)
    }
}

/// A parsed response, borrowed from the connection that sent it.
pub struct HttpResponse<'a> {
    /// The HTTP minor version: `1` for HTTP/1.1.
    pub version: u8,
    /// The status code.
    pub status: u16,
    /// The reason phrase, which may be empty.
    pub reason: &'a str,
    /// The response body, decoded when it arrived chunked.
    pub body: &'a [u8],
    buffer: &'a [u8],
    headers: &'a [HeaderRange],
}

impl<'a> HttpResponse<'a> {
    /// The value of the first header with this name, matched case
    /// insensitively.
    pub fn header(&self, name: &str) -> Option<&'a [u8]> {
        header(self.buffer, self.headers, name)
    }

    /// Every header, in the order the sender wrote them.
    pub fn headers(&self) -> impl Iterator<Item = (&'a str, &'a [u8])> + use<'a> {
        headers(self.buffer, self.headers)
    }
}

/// The means to answer one request, scoped to its connection.
///
/// Responding consumes the responder, so a request is answered exactly once.
#[must_use = "respond now, or drop it to answer later with HttpService::respond; a request never \
              answered is closed by the idle sweep"]
pub struct Responder<'a> {
    net: &'a mut StreamNetwork,
    state: &'a mut ConnState,
    token: Token,
}

impl Responder<'_> {
    /// Queues the response and returns whether it was written.
    ///
    /// The status must be in `200..=599`; `Content-Length` and
    /// `Transfer-Encoding` are the service's to write, and a `Connection`
    /// header only feeds the close decision.
    pub fn respond(self, status: u16, headers: &[(&str, &str)], body: &[u8]) -> bool {
        respond_to(self.net, self.state, self.token, status, headers, body)
    }
}

/// What answering one request touches, and all a [`Responder`] may reach.
///
/// `req_end` marks a parsed request awaiting its response; `consumed` is the
/// accounting cursor, and bytes below it are answered and cost the connection
/// nothing against its limits, whether or not they have been reclaimed yet.
struct ConnState {
    phase: Phase,
    close: bool,
    head_request: bool,
    req_end: Option<usize>,
    consumed: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Awaiting the next request.
    Idle,
    /// A request was delivered and its response is outstanding.
    Pending,
    /// Answered with a close; the connection goes once its bytes are written.
    Draining,
}

enum Role {
    Accepted,
    Outbound { endpoint: Endpoint, method: Option<String> },
}

struct Conn {
    token: Token,
    buffer: Vec<u8>,
    /// The reclamation cursor and the parse origin, never past `consumed`.
    start: usize,
    /// Whether the connection is queued as ready to parse.
    ready: bool,
    /// Whether a read was cut short by the connection's limits.
    over_limit: bool,
    /// Whether the client was told to send the body it announced.
    continued: bool,
    last_activity: Instant,
    state: ConnState,
    role: Role,
}

impl Conn {
    fn new(token: Token, role: Role) -> Self {
        Self {
            token,
            buffer: Vec::new(),
            start: 0,
            ready: false,
            over_limit: false,
            continued: false,
            last_activity: Instant::now(),
            state: ConnState {
                phase: Phase::Idle,
                close: false,
                head_request: false,
                req_end: None,
                consumed: 0,
            },
            role,
        }
    }

    /// Moves the reclamation cursor onto the answered bytes, dropping the
    /// reclaimed prefix once it is worth the move.
    fn reclaim(&mut self) {
        self.start = self.state.consumed;
        if self.start >= self.buffer.len() / 2 {
            self.compact();
        }
    }

    /// Drops the reclaimed prefix, rebasing every cursor onto what is left.
    fn compact(&mut self) {
        let start = self.start;
        if start == 0 {
            return
        }
        self.buffer.drain(..start);
        self.start = 0;
        self.state.consumed -= start;
        self.state.req_end = self.state.req_end.map(|end| end - start);
    }

    /// Forgets everything buffered, which a closed connection no longer owes
    /// anyone.
    fn clear(&mut self) {
        self.buffer.clear();
        self.start = 0;
        self.over_limit = false;
        self.continued = false;
        self.state.consumed = 0;
        self.state.req_end = None;
        self.state.phase = Phase::Idle;
        self.state.close = false;
    }

    /// Bytes counted against the connection's limits.
    fn unconsumed(&self) -> usize {
        self.buffer.len() - self.state.consumed
    }
}

/// A lifecycle event waiting to be pulled.
#[derive(Clone, Copy)]
enum Record {
    Accepted(Token, Peer),
    Connected(Token),
    Disconnected(Token),
}

/// What the next pull delivers, resolved before any borrow escapes.
enum Step {
    Accepted {
        token: Token,
        peer: Peer,
    },
    Connected {
        token: Token,
    },
    Request {
        index: usize,
        method: Range<usize>,
        path: Range<usize>,
        version: u8,
        body: Range<usize>,
    },
    Response {
        index: usize,
        version: u8,
        status: u16,
        reason: Range<usize>,
        body: Body,
    },
    Disconnected {
        token: Token,
    },
}

/// Where a response body was found.
enum Body {
    Buffer(Range<usize>),
    Decoded,
}

/// The name and value of one header, as byte ranges of a connection buffer.
type HeaderRange = (Range<usize>, Range<usize>);

/// An HTTP server, client, or both, owning one raw-framed group of a
/// [`StreamNetwork`].
///
/// The network schedules the service — hand it to
/// [`StreamNetwork::drive`] with [`Self::as_service`] — and the caller pulls
/// protocol events with [`Self::next_event`].
pub struct HttpService {
    group: ConnectionGroup,
    config: HttpConfig,
    conns: Vec<Conn>,
    /// Lifecycle events, oldest first from `record_cursor`.
    records: Vec<Record>,
    record_cursor: usize,
    /// Connections with bytes to parse, oldest first from `ready_cursor`.
    ready: Vec<Token>,
    ready_cursor: usize,
    /// Header ranges of the message the last pull parsed.
    headers: Vec<HeaderRange>,
    /// The body of the last chunked response, decoded.
    decoded: Vec<u8>,
    /// The connection the last pulled event left bookkeeping for.
    last: Option<Token>,
}

impl HttpService {
    /// Claims `group` for HTTP.
    ///
    /// # Panics
    /// The group must be [`Framing::Raw`] and unclaimed.
    pub fn new(net: &mut StreamNetwork, group: ConnectionGroup, config: HttpConfig) -> Self {
        assert!(
            net.framing(group) == Framing::Raw,
            "HTTP frames its own messages and needs a raw-framed group"
        );
        net.claim_group(group);
        Self {
            group,
            config,
            conns: Vec::new(),
            records: Vec::new(),
            record_cursor: 0,
            ready: Vec::new(),
            ready_cursor: 0,
            headers: Vec::new(),
            decoded: Vec::new(),
            last: None,
        }
    }

    /// The group this service owns.
    pub fn group(&self) -> ConnectionGroup {
        self.group
    }

    /// Hands the service to [`StreamNetwork::drive`].
    pub fn as_service(&mut self) -> ServiceRef<'_> {
        ServiceRef::new(self)
    }

    /// Adds a listener.
    ///
    /// An [`Endpoint::Unix`] socket file is created with mode `0777` less the
    /// umask bits and is unlinked when the service is closed; see
    /// [`StreamNetwork::listen`].
    pub fn listen(&mut self, net: &mut StreamNetwork, endpoint: Endpoint) -> io::Result<()> {
        net.listen(self.group, endpoint)
    }

    /// Adds a persistent outbound endpoint and immediately starts connecting.
    /// The returned token remains stable across reconnects.
    #[must_use = "the token identifies the outbound endpoint"]
    pub fn connect(&mut self, net: &mut StreamNetwork, endpoint: Endpoint) -> Token {
        let token = net.connect(self.group, endpoint.clone());
        self.conns.push(Conn::new(token, Role::Outbound { endpoint, method: None }));
        token
    }

    /// Immediately disconnects an accepted client.
    pub fn disconnect(&mut self, net: &mut StreamNetwork, token: Token) -> bool {
        self.index_of(token).is_some_and(|index| matches!(self.conns[index].role, Role::Accepted)) &&
            net.disconnect(token)
    }

    /// Permanently removes an outbound endpoint and stops it reconnecting.
    pub fn remove(&mut self, net: &mut StreamNetwork, token: Token) -> bool {
        let Some(index) = self.index_of(token) else { return false };
        if !matches!(self.conns[index].role, Role::Outbound { .. }) || !net.remove(token) {
            return false
        }
        self.conns.swap_remove(index);
        true
    }

    /// Closes the service: every connection and listener of its group goes,
    /// un-pulled events are discarded, and the group returns to unclaimed
    /// status, empty and reusable.
    pub fn close(self, net: &mut StreamNetwork) {
        net.close_group(self.group);
    }

    /// Queues one request on an outbound endpoint.
    ///
    /// When the caller supplies no `Host` header, a TCP endpoint sends its
    /// socket address; a Unix-domain endpoint has no address to name and
    /// sends `localhost`.
    pub fn request(
        &mut self,
        net: &mut StreamNetwork,
        token: Token,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> bool {
        if !valid_token(method) ||
            path.is_empty() ||
            path.contains(['\r', '\n', ' ']) ||
            headers.iter().any(|(name, value)| invalid_header(name, value))
        {
            return false
        }
        let Some(conn) = self
            .conns
            .iter_mut()
            .find(|conn| conn.token == token && outbound_method(&conn.role).is_none())
        else {
            return false
        };
        let Role::Outbound { endpoint, .. } = &conn.role else { return false };
        let host = match endpoint {
            Endpoint::Tcp(addr) => addr.to_string(),
            Endpoint::Unix(_) => "localhost".to_owned(),
        };
        let sent = net.send_with(token, |out| {
            write!(out, "{method} {path} HTTP/1.1\r\n").unwrap();
            let mut has_host = false;
            for (name, value) in headers {
                has_host |= name.eq_ignore_ascii_case("host");
                out.extend_from_slice(name.as_bytes());
                out.extend_from_slice(b": ");
                out.extend_from_slice(value.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            if !has_host {
                write!(out, "Host: {host}\r\n").unwrap();
            }
            write!(out, "Content-Length: {}\r\n\r\n", body.len()).unwrap();
            out.extend_from_slice(body);
        });
        if sent {
            set_outbound_method(&mut conn.role, Some(method.to_owned()));
        }
        sent
    }

    /// Answers a request whose [`Responder`] was dropped, and returns whether
    /// the response was written.
    ///
    /// Each call completes exactly one request for `token`.
    pub fn respond(
        &mut self,
        net: &mut StreamNetwork,
        token: Token,
        status: u16,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> bool {
        let Some(index) = self.index_of(token) else { return false };
        if !matches!(self.conns[index].role, Role::Accepted) {
            return false
        }
        if !respond_to(net, &mut self.conns[index].state, token, status, headers, body) {
            return false
        }
        self.reclaim(index);
        true
    }

    /// The next protocol event, parsed on demand from what the connection
    /// buffered.
    ///
    /// The event borrows the service and the network until it is dropped, so a
    /// handler reaches the network only through the event it was handed.
    pub fn next_event<'a>(&'a mut self, net: &'a mut StreamNetwork) -> Option<HttpEvent<'a>> {
        self.apply_bookkeeping();
        match self.plan(net)? {
            Step::Accepted { token, peer } => Some(HttpEvent::Accepted { token, peer }),
            Step::Connected { token } => Some(HttpEvent::Connected { token }),
            Step::Disconnected { token } => Some(HttpEvent::Disconnected { token }),
            Step::Request { index, method, path, version, body } => {
                let token = self.conns[index].token;
                self.last = Some(token);
                let Self { conns, headers, .. } = self;
                let headers: &[HeaderRange] = headers;
                let Conn { buffer, state, .. } = &mut conns[index];
                let buffer: &[u8] = buffer;
                let request = HttpRequest {
                    method: text(buffer, method),
                    path: text(buffer, path),
                    version,
                    body: &buffer[body],
                    buffer,
                    headers,
                };
                Some(HttpEvent::Request {
                    token,
                    request,
                    responder: Responder { net, state, token },
                })
            }
            Step::Response { index, version, status, reason, body } => {
                let token = self.conns[index].token;
                self.last = Some(token);
                let Self { conns, headers, decoded, .. } = self;
                let headers: &[HeaderRange] = headers;
                let buffer: &[u8] = &conns[index].buffer;
                let body = match body {
                    Body::Buffer(range) => &buffer[range],
                    Body::Decoded => decoded.as_slice(),
                };
                let response = HttpResponse {
                    version,
                    status,
                    reason: text(buffer, reason),
                    body,
                    buffer,
                    headers,
                };
                Some(HttpEvent::Response { token, response })
            }
        }
    }

    /// Where a connection has parsed and answered up to: `start`, `consumed`
    /// and `req_end`.
    #[doc(hidden)]
    pub fn cursors(&self, token: Token) -> Option<(usize, usize, Option<usize>)> {
        self.index_of(token).map(|index| {
            let conn = &self.conns[index];
            (conn.start, conn.state.consumed, conn.state.req_end)
        })
    }

    /// Connections queued as ready to parse.
    #[doc(hidden)]
    pub fn ready_len(&self) -> usize {
        self.ready.len() - self.ready_cursor
    }

    /// Applies what the previous event left behind: the bytes it answered are
    /// reclaimed, and a connection with more to parse queues up again.
    fn apply_bookkeeping(&mut self) {
        let Some(token) = self.last.take() else { return };
        let Some(index) = self.index_of(token) else { return };
        self.reclaim(index);
    }

    /// Reclaims one connection's answered bytes and requeues it when bytes
    /// remain.
    fn reclaim(&mut self, index: usize) {
        self.conns[index].reclaim();
        if self.conns[index].start < self.conns[index].buffer.len() {
            self.mark_ready(index);
        }
    }

    /// Resolves the next event, sending error responses and closing
    /// connections along the way; no borrow of a connection escapes.
    fn plan(&mut self, net: &mut StreamNetwork) -> Option<Step> {
        loop {
            // Lifecycle first, so a connection's Accepted precedes its first
            // request, and its Disconnected follows everything it sent.
            if let Some(record) = self.records.get(self.record_cursor).copied() {
                match record {
                    Record::Accepted(token, peer) => {
                        self.take_record();
                        return Some(Step::Accepted { token, peer })
                    }
                    Record::Connected(token) => {
                        self.take_record();
                        return Some(Step::Connected { token })
                    }
                    Record::Disconnected(token) => {
                        if let Some(index) = self.index_of(token) {
                            if let Some(step) = self.plan_connection(net, index, true) {
                                return Some(step)
                            }
                            self.close_connection(index);
                        }
                        self.take_record();
                        return Some(Step::Disconnected { token })
                    }
                }
            }
            let index = self.pop_ready()?;
            if let Some(step) = self.plan_connection(net, index, false) {
                return Some(step)
            }
        }
    }

    fn plan_connection(
        &mut self,
        net: &mut StreamNetwork,
        index: usize,
        at_eof: bool,
    ) -> Option<Step> {
        match self.conns[index].role {
            // A client that has gone can be told nothing, so its last request
            // is dropped rather than delivered with an answer that fails.
            Role::Accepted => (!at_eof).then(|| self.plan_request(net, index)).flatten(),
            Role::Outbound { .. } => self.plan_response(net, index, at_eof),
        }
    }

    /// Parses the next request of an accepted connection, answering it here
    /// when it is malformed or too large.
    fn plan_request(&mut self, net: &mut StreamNetwork, index: usize) -> Option<Step> {
        // The ready queue holds no connection that owes a response, but the
        // lifecycle path reaches a connection without going through it.
        if self.conns[index].state.phase != Phase::Idle {
            return None
        }
        self.conns[index].reclaim();
        match self.parse_request(index) {
            RequestPlan::Incomplete => None,
            RequestPlan::Error(status) => {
                self.error(net, index, status);
                None
            }
            RequestPlan::Continue => {
                let token = self.conns[index].token;
                if net.send_with(token, |out| {
                    write!(out, "HTTP/1.1 100 Continue\r\n\r\n").unwrap();
                }) {
                    self.conns[index].continued = true;
                }
                None
            }
            RequestPlan::Ready { method, path, version, body, end, close, head_request } => {
                let conn = &mut self.conns[index];
                conn.continued = false;
                conn.state.phase = Phase::Pending;
                conn.state.req_end = Some(end);
                conn.state.close = close;
                conn.state.head_request = head_request;
                Some(Step::Request { index, method, path, version, body })
            }
        }
    }

    fn parse_request(&mut self, index: usize) -> RequestPlan {
        let Self { conns, headers, config, .. } = self;
        let conn = &conns[index];
        let base = conn.start;
        let buffer = &conn.buffer[base..];
        if buffer.is_empty() {
            return RequestPlan::Incomplete
        }
        let mut scratch = vec![httparse::EMPTY_HEADER; config.max_headers];
        let mut request = httparse::Request::new(&mut scratch);
        let Ok(status) = request.parse(buffer) else { return RequestPlan::Error(400) };
        let httparse::Status::Complete(head) = status else {
            // A partial parse means every buffered byte is still head bytes.
            if !crlf_only(buffer) {
                return RequestPlan::Error(400)
            }
            if conn.over_limit || buffer.len() > config.max_head_bytes {
                return RequestPlan::Error(431)
            }
            return RequestPlan::Incomplete
        };
        if !crlf_only(&buffer[..head]) {
            return RequestPlan::Error(400)
        }
        if head > config.max_head_bytes {
            return RequestPlan::Error(431)
        }
        let Some(length) = request_content_length(request.headers) else {
            return RequestPlan::Error(400)
        };
        if request.headers.iter().any(|h| h.name.eq_ignore_ascii_case("transfer-encoding")) {
            return RequestPlan::Error(501)
        }
        if length > config.max_body_bytes {
            return RequestPlan::Error(413)
        }
        let Some(end) = head.checked_add(length) else { return RequestPlan::Error(413) };
        if buffer.len() < end {
            if conn.over_limit {
                return RequestPlan::Error(413)
            }
            if has_token(request.headers, "expect", b"100-continue") && !conn.continued {
                return RequestPlan::Continue
            }
            return RequestPlan::Incomplete
        }
        headers.clear();
        for header in request.headers.iter() {
            headers.push((
                span(buffer, header.name.as_bytes(), base),
                span(buffer, header.value, base),
            ));
        }
        RequestPlan::Ready {
            method: span(buffer, request.method.unwrap_or("").as_bytes(), base),
            path: span(buffer, request.path.unwrap_or("").as_bytes(), base),
            version: request.version.unwrap_or(1),
            body: base + head..base + end,
            end: base + end,
            close: request.version == Some(0) &&
                !has_token(request.headers, "connection", b"keep-alive") ||
                has_token(request.headers, "connection", b"close"),
            head_request: request.method == Some("HEAD"),
        }
    }

    /// Parses the next response of an outbound connection, skipping
    /// informational ones.
    fn plan_response(
        &mut self,
        net: &mut StreamNetwork,
        index: usize,
        at_eof: bool,
    ) -> Option<Step> {
        loop {
            outbound_method(&self.conns[index].role)?;
            self.conns[index].reclaim();
            match self.parse_response(index) {
                ResponsePlan::Incomplete => {
                    return at_eof.then(|| self.plan_eof_response(index)).flatten()
                }
                ResponsePlan::Fail => {
                    self.fail_outbound(net, index);
                    return None
                }
                ResponsePlan::Informational { end } => self.conns[index].state.consumed = end,
                ResponsePlan::Ready { version, status, reason, body, end, close } => {
                    let token = self.conns[index].token;
                    let conn = &mut self.conns[index];
                    conn.state.consumed = end;
                    set_outbound_method(&mut conn.role, None);
                    if close {
                        net.disconnect(token);
                    }
                    return Some(Step::Response { index, version, status, reason, body })
                }
            }
        }
    }

    fn parse_response(&mut self, index: usize) -> ResponsePlan {
        let Self { conns, headers, decoded, config, .. } = self;
        let conn = &conns[index];
        let base = conn.start;
        let buffer = &conn.buffer[base..];
        if buffer.is_empty() {
            return ResponsePlan::Incomplete
        }
        let mut scratch = vec![httparse::EMPTY_HEADER; config.max_headers];
        let mut response = httparse::Response::new(&mut scratch);
        let Ok(status) = response.parse(buffer) else { return ResponsePlan::Fail };
        let httparse::Status::Complete(head) = status else {
            // A partial parse means every buffered byte is still head bytes.
            if !crlf_only(buffer) || buffer.len() > config.max_head_bytes {
                return ResponsePlan::Fail
            }
            return ResponsePlan::Incomplete
        };
        if !crlf_only(&buffer[..head]) || head > config.max_head_bytes {
            return ResponsePlan::Fail
        }
        let code = response.code.unwrap_or(0);
        let no_body = outbound_method(&conn.role).map(String::as_str) == Some("HEAD") ||
            matches!(code, 100..=199 | 204 | 304);
        let chunked = transfer_chunked(response.headers);
        let content_length = response_content_length(response.headers);
        if code == 101 ||
            matches!(content_length, ContentLength::Invalid) ||
            chunked.is_none() ||
            (chunked == Some(true) && !matches!(content_length, ContentLength::Absent)) ||
            (!no_body &&
                matches!(content_length, ContentLength::Present(length) if length > config.max_body_bytes))
        {
            return ResponsePlan::Fail
        }
        let (end, body) = if no_body {
            (head, Body::Buffer(base + head..base + head))
        } else if chunked == Some(true) {
            match decode_chunked(
                &buffer[head..],
                config.max_body_bytes,
                config.max_headers,
                decoded,
            ) {
                Ok(Some(consumed)) => {
                    let Some(end) = head.checked_add(consumed) else { return ResponsePlan::Fail };
                    (end, Body::Decoded)
                }
                Ok(None) => return ResponsePlan::Incomplete,
                Err(()) => return ResponsePlan::Fail,
            }
        } else if let ContentLength::Present(length) = content_length {
            let Some(end) = head.checked_add(length) else { return ResponsePlan::Fail };
            if buffer.len() < end {
                return ResponsePlan::Incomplete
            }
            (end, Body::Buffer(base + head..base + end))
        } else {
            // Delimited by the close of the connection.
            return ResponsePlan::Incomplete
        };
        if code < 200 {
            return ResponsePlan::Informational { end: base + end }
        }
        headers.clear();
        for header in response.headers.iter() {
            headers.push((
                span(buffer, header.name.as_bytes(), base),
                span(buffer, header.value, base),
            ));
        }
        ResponsePlan::Ready {
            version: response.version.unwrap_or(1),
            status: code,
            reason: span(buffer, response.reason.unwrap_or("").as_bytes(), base),
            body,
            end: base + end,
            close: response.version == Some(0) &&
                !has_token(response.headers, "connection", b"keep-alive") ||
                has_token(response.headers, "connection", b"close"),
        }
    }

    /// Parses a response whose body the closing connection delimits.
    fn plan_eof_response(&mut self, index: usize) -> Option<Step> {
        let plan = {
            let Self { conns, headers, config, .. } = self;
            let conn = &conns[index];
            let base = conn.start;
            let buffer = &conn.buffer[base..];
            let mut scratch = vec![httparse::EMPTY_HEADER; config.max_headers];
            let mut response = httparse::Response::new(&mut scratch);
            let Ok(httparse::Status::Complete(head)) = response.parse(buffer) else { return None };
            if !crlf_only(&buffer[..head]) || head > config.max_head_bytes {
                return None
            }
            let code = response.code.unwrap_or(0);
            let no_body = outbound_method(&conn.role).map(String::as_str) == Some("HEAD") ||
                matches!(code, 100..=199 | 204 | 304);
            if no_body ||
                transfer_chunked(response.headers) != Some(false) ||
                !matches!(response_content_length(response.headers), ContentLength::Absent) ||
                buffer.len() - head > config.max_body_bytes
            {
                return None
            }
            headers.clear();
            for header in response.headers.iter() {
                headers.push((
                    span(buffer, header.name.as_bytes(), base),
                    span(buffer, header.value, base),
                ));
            }
            Step::Response {
                index,
                version: response.version.unwrap_or(1),
                status: code,
                reason: span(buffer, response.reason.unwrap_or("").as_bytes(), base),
                body: Body::Buffer(base + head..conn.buffer.len()),
            }
        };
        let conn = &mut self.conns[index];
        conn.state.consumed = conn.buffer.len();
        set_outbound_method(&mut conn.role, None);
        Some(plan)
    }

    /// Answers a request the service itself rejected, and closes after it.
    fn error(&mut self, net: &mut StreamNetwork, index: usize, status: u16) {
        let token = self.conns[index].token;
        let state = &mut self.conns[index].state;
        state.phase = Phase::Pending;
        state.close = true;
        state.head_request = false;
        respond_to(net, state, token, status, &[], &[]);
    }

    /// Drops an outbound connection whose peer broke the protocol.
    fn fail_outbound(&mut self, net: &mut StreamNetwork, index: usize) {
        let token = self.conns[index].token;
        self.conns[index].clear();
        set_outbound_method(&mut self.conns[index].role, None);
        net.disconnect(token);
    }

    /// Forgets what a closed connection held: an accepted one goes, an
    /// outbound one stays for its next connection.
    fn close_connection(&mut self, index: usize) {
        if matches!(self.conns[index].role, Role::Accepted) {
            self.conns.swap_remove(index);
        } else {
            self.conns[index].clear();
            set_outbound_method(&mut self.conns[index].role, None);
        }
    }

    fn index_of(&self, token: Token) -> Option<usize> {
        self.conns.iter().position(|conn| conn.token == token)
    }

    /// Queues a connection as ready to parse, once however many reads
    /// arrived and only while it is free to take the next request: a
    /// connection owing a response, or closing after one, parses nothing.
    fn mark_ready(&mut self, index: usize) {
        let conn = &mut self.conns[index];
        if conn.ready || conn.state.phase != Phase::Idle {
            return
        }
        conn.ready = true;
        self.ready.push(conn.token);
    }

    fn pop_ready(&mut self) -> Option<usize> {
        while self.ready_cursor < self.ready.len() {
            let token = self.ready[self.ready_cursor];
            self.ready_cursor += 1;
            if self.ready_cursor == self.ready.len() {
                self.ready.clear();
                self.ready_cursor = 0;
            }
            if let Some(index) = self.index_of(token) {
                self.conns[index].ready = false;
                return Some(index)
            }
        }
        None
    }

    fn take_record(&mut self) {
        self.record_cursor += 1;
        if self.record_cursor == self.records.len() {
            self.records.clear();
            self.record_cursor = 0;
        }
    }

    /// Buffers one read, up to what the connection's limits allow.
    fn buffer(&mut self, token: Token, payload: &[u8]) {
        let limit = self.config.buffer_limit();
        let Some(index) = self.index_of(token) else { return };
        let conn = &mut self.conns[index];
        if conn.state.phase == Phase::Draining || conn.over_limit {
            return
        }
        if conn.state.consumed > 0 && conn.buffer.len() + payload.len() > limit {
            conn.start = conn.state.consumed;
            conn.compact();
        }
        let available = limit.saturating_sub(conn.unconsumed());
        if payload.len() > available {
            conn.buffer.extend_from_slice(&payload[..available]);
            conn.over_limit = true;
        } else {
            conn.buffer.extend_from_slice(payload);
        }
        conn.last_activity = Instant::now();
        self.mark_ready(index);
    }

    /// Whether a pull would deliver anything.
    fn pullable(&self) -> bool {
        self.record_cursor < self.records.len() || self.ready_cursor < self.ready.len()
    }
}

impl private::ServiceDriver for HttpService {
    fn group(&self) -> ConnectionGroup {
        self.group
    }

    fn on_event(&mut self, event: &StreamEvent<'_>) {
        debug_assert_eq!(event.group(), self.group, "the network routes by group");
        match *event {
            StreamEvent::Accepted { token, peer, .. } => {
                self.conns.push(Conn::new(token, Role::Accepted));
                self.records.push(Record::Accepted(token, peer));
            }
            StreamEvent::Connected { token, .. } => self.records.push(Record::Connected(token)),
            StreamEvent::Message { token, payload, .. } => self.buffer(token, payload),
            StreamEvent::Disconnected { token, .. } => {
                self.records.push(Record::Disconnected(token));
            }
        }
    }

    fn tick(&mut self, net: &mut StreamNetwork, now: Instant) -> bool {
        // No event is live inside a driver call, so what the last pull left
        // behind is applied here rather than at the next pull: a connection
        // holding a pipelined request is pullable work, and saying otherwise
        // would park a caller that answered inline and stopped pulling.
        self.apply_bookkeeping();
        for index in 0..self.conns.len() {
            let conn = &self.conns[index];
            // A connection queued as ready is answered first: the bytes over
            // the limit may be the ones that complete a request the service
            // rejects with a status of its own.
            if conn.over_limit && !conn.ready && conn.state.phase != Phase::Draining {
                self.conns[index].over_limit = false;
                net.disconnect(self.conns[index].token);
            }
        }
        if let Some(timeout) = self.config.idle_timeout {
            for conn in &self.conns {
                if matches!(conn.role, Role::Accepted) &&
                    now.saturating_sub(conn.last_activity) >= timeout
                {
                    net.disconnect(conn.token);
                }
            }
        }
        self.pullable()
    }

    fn next_deadline(&self) -> Option<Instant> {
        let timeout = self.config.idle_timeout?;
        self.conns
            .iter()
            .filter(|conn| matches!(conn.role, Role::Accepted))
            .map(|conn| conn.last_activity + timeout)
            .min()
    }
}

/// What the next request of an accepted connection needs.
enum RequestPlan {
    /// Not all of it has arrived.
    Incomplete,
    /// Answer with this status and close.
    Error(u16),
    /// Tell the client to send the body it announced.
    Continue,
    Ready {
        method: Range<usize>,
        path: Range<usize>,
        version: u8,
        body: Range<usize>,
        end: usize,
        close: bool,
        head_request: bool,
    },
}

/// What the next response of an outbound connection needs.
enum ResponsePlan {
    /// Not all of it has arrived.
    Incomplete,
    /// The peer broke the protocol.
    Fail,
    /// An informational response, consumed without an event.
    Informational {
        end: usize,
    },
    Ready {
        version: u8,
        status: u16,
        reason: Range<usize>,
        body: Body,
        end: usize,
        close: bool,
    },
}

/// Writes one response for a pending request and moves the connection on.
fn respond_to(
    net: &mut StreamNetwork,
    state: &mut ConnState,
    token: Token,
    status: u16,
    headers: &[(&str, &str)],
    body: &[u8],
) -> bool {
    if state.phase != Phase::Pending {
        return false
    }
    if !(200..=599).contains(&status) ||
        headers.iter().any(|(name, value)| invalid_header(name, value))
    {
        return false
    }
    let caller_close = headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("connection") && has_value_token(value.as_bytes(), b"close")
    });
    let close = state.close || caller_close;
    let suppress_body = state.head_request || matches!(status, 100..=199 | 204 | 304);
    let include_length = !matches!(status, 100..=199 | 204);
    let ok = net.send_with(token, |out| {
        write!(out, "HTTP/1.1 {status} {}\r\n", reason_phrase(status)).unwrap();
        // Caller Connection headers only feed the close decision; exactly
        // one canonical Connection header is always written below.
        for (name, value) in headers {
            if name.eq_ignore_ascii_case("connection") {
                continue
            }
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(b": ");
            out.extend_from_slice(value.as_bytes());
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
        if let Some(end) = state.req_end.take() {
            state.consumed = end;
        }
        state.phase = if close { Phase::Draining } else { Phase::Idle };
        if close {
            net.disconnect_when_drained(token);
        }
    }
    ok
}

/// The byte range of `part` inside `buffer`, offset by `base`.
///
/// A parser reports an absent value as an empty string of its own, which no
/// buffer range describes; the empty range at the start is the honest answer.
fn span(buffer: &[u8], part: &[u8], base: usize) -> Range<usize> {
    let origin = buffer.as_ptr() as usize;
    let at = part.as_ptr() as usize;
    if at < origin || at + part.len() > origin + buffer.len() {
        debug_assert!(part.is_empty(), "a parsed part must lie inside the buffer it came from");
        return base..base
    }
    base + at - origin..base + at - origin + part.len()
}

fn text(buffer: &[u8], range: Range<usize>) -> &str {
    std::str::from_utf8(&buffer[range]).unwrap_or("")
}

fn header<'a>(buffer: &'a [u8], headers: &'a [HeaderRange], name: &str) -> Option<&'a [u8]> {
    headers
        .iter()
        .find(|(header, _)| buffer[header.clone()].eq_ignore_ascii_case(name.as_bytes()))
        .map(|(_, value)| &buffer[value.clone()])
}

fn headers<'a>(
    buffer: &'a [u8],
    headers: &'a [HeaderRange],
) -> impl Iterator<Item = (&'a str, &'a [u8])> + use<'a> {
    headers.iter().map(move |(name, value)| (text(buffer, name.clone()), &buffer[value.clone()]))
}

fn crlf_only(bytes: &[u8]) -> bool {
    bytes.iter().enumerate().all(|(i, b)| *b != b'\n' || i > 0 && bytes[i - 1] == b'\r')
}

/// Whether a caller-supplied header is one the service refuses to send.
fn invalid_header(name: &str, value: &str) -> bool {
    !valid_token(name) ||
        value.contains(['\r', '\n']) ||
        name.eq_ignore_ascii_case("content-length") ||
        name.eq_ignore_ascii_case("transfer-encoding")
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

fn outbound_method(role: &Role) -> Option<&String> {
    match role {
        Role::Outbound { method, .. } => method.as_ref(),
        Role::Accepted => None,
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

/// Decodes a chunked body into `out`, reporting how many bytes it spanned.
fn decode_chunked(
    bytes: &[u8],
    max_body_bytes: usize,
    max_headers: usize,
    out: &mut Vec<u8>,
) -> Result<Option<usize>, ()> {
    let Some((end, body_len)) = chunked_end(bytes, max_body_bytes, max_headers)? else {
        return Ok(None)
    };
    out.clear();
    out.reserve(body_len);
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
            return Ok(Some(end))
        }
        out.extend_from_slice(&bytes[at..at + size]);
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

#[cfg(test)]
mod tests {
    use super::{HttpRequest, span};

    #[test]
    fn header_lookup_is_case_insensitive() {
        let buffer = b"Content-Type: text/plain".as_slice();
        let headers = [(0..12, 14..24)];
        let request = HttpRequest {
            method: "GET",
            path: "/",
            version: 1,
            body: &[],
            buffer,
            headers: &headers,
        };
        assert_eq!(request.header("content-type"), Some(&b"text/plain"[..]));
        assert_eq!(request.header("content-length"), None);
        assert_eq!(request.headers().collect::<Vec<_>>(), [("Content-Type", &b"text/plain"[..])]);
    }

    #[test]
    fn a_span_is_the_offset_of_a_parsed_part() {
        let buffer = b"GET /path HTTP/1.1".as_slice();
        assert_eq!(span(buffer, &buffer[4..9], 0), 4..9);
        assert_eq!(span(buffer, &buffer[4..9], 100), 104..109);
        assert_eq!(span(buffer, b"", 7), 7..7);
    }
}
