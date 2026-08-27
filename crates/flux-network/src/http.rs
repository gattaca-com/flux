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
//! # Closing a connection
//! A response that closes its connection ends it in one of two ways. When the
//! service read the request stream as far as the request it is answering, the
//! connection *drains*: it closes as soon as the response reaches the wire.
//! When it did not — every status the service raises itself (`400`, `413`,
//! `431`, `501`) rejects a request mid-stream, and a connection over its
//! buffer limit holds bytes nothing will ever read — the connection *lingers*
//! instead: its write side shuts once the response drains, so the peer reads
//! the answer and then the end of the stream, while what it is still sending
//! is read and discarded until it stops for [`Linger::idle`], until the
//! linger has run for [`Linger::total`], or until it closes. Those caps
//! govern the reading and discarding alone: until the answer has left, the
//! connection is held to the same [`HttpConfig::idle_timeout`] as any other,
//! so a peer that stops reading loses it as surely as one that stops
//! sending. A client whose upload meets a reset before it has read the reply
//! reports a server that is down rather than the status it was sent;
//! lingering is what puts a delivered `400` or `413` in front of it instead.
//! [`HttpConfig::linger`] sets the caps, and `None` drains every closing
//! response.
//!
//! # Limitations
//! HTTP/1.1 and HTTP/1.0 responses are supported. Request bodies require
//! `Content-Length`; chunked requests are rejected with `501`. Response bodies
//! may use `Content-Length`, chunked transfer coding, or EOF delimiting.
//! `Expect: 100-continue` is handled automatically.
//!
//! TLS, HTTP/2, compression, trailer exposure, upgrades, and `WebSockets` are
//! not supported. Valid response trailers are parsed and discarded. Pipelined
//! requests are served strictly one at a time per connection.

use std::{
    io::{self, Write as _},
    ops::Range,
};

use flux_timing::{Duration, Instant};
use mio::Token;

use crate::stream::{
    ConnectionGroup, Endpoint, Framing, Peer, ServiceRef, StreamEvent, StreamNetwork, private,
};

/// The most headers one message may be parsed into, and the size of the fixed
/// stack scratch used while parsing.
pub const MAX_HEADERS: usize = 128;

/// HTTP parsing and connection-state policy. Transport and queue policy
/// belongs to the group the service claims.
#[derive(Clone, Copy, Debug)]
pub struct HttpConfig {
    /// Largest message head accepted before rejecting it with `431`.
    pub max_head_bytes: usize,
    /// Largest message body accepted before rejecting it with `413`.
    pub max_body_bytes: usize,
    /// Largest number of headers parsed in one message, itself held to
    /// [`MAX_HEADERS`]: a larger value parses [`MAX_HEADERS`] of them.
    pub max_headers: usize,
    /// How long an accepted connection may sit without inbound bytes before
    /// it is closed. Outbound endpoints stay connected.
    pub idle_timeout: Option<Duration>,
    /// The caps a connection lingers under after a response that ends a
    /// request stream the service never read to its end; `None` closes such a
    /// connection as soon as the response is written, as it does any other.
    pub linger: Option<Linger>,
    /// How long an outbound request may go unanswered before it fails with
    /// [`RequestFailure::Timeout`] and its connection closes; `None` waits as
    /// long as the endpoint holds the connection open.
    pub request_timeout: Option<Duration>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            max_head_bytes: 16 * 1024,
            max_body_bytes: 1024 * 1024,
            max_headers: 64,
            idle_timeout: Some(Duration::from_secs(30)),
            linger: Some(Linger::default()),
            request_timeout: None,
        }
    }
}

/// How long a lingering connection reads and discards before it closes.
///
/// The idle cap ends a peer that stops sending, the total cap one that never
/// does.
#[derive(Clone, Copy, Debug)]
pub struct Linger {
    /// How long the connection may go without inbound bytes.
    pub idle: Duration,
    /// How long the connection may linger, however much it sends.
    pub total: Duration,
}

impl Default for Linger {
    fn default() -> Self {
        Self { idle: Duration::from_secs(5), total: Duration::from_secs(30) }
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
    ///
    /// # Panics
    /// `max_headers` must be in <code>1..=[MAX_HEADERS]</code>.
    pub fn with_max_headers(mut self, max_headers: usize) -> Self {
        assert!(
            (1..=MAX_HEADERS).contains(&max_headers),
            "max_headers must be in 1..={MAX_HEADERS}"
        );
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

    /// Sets the caps a lingering connection closes under.
    pub fn with_linger(mut self, linger: Linger) -> Self {
        self.linger = Some(linger);
        self
    }

    /// Closes every connection as soon as its response is written, with no
    /// lingering close.
    pub fn without_linger(mut self) -> Self {
        self.linger = None;
        self
    }

    /// Sets how long an outbound request may go unanswered.
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = Some(request_timeout);
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
    /// An outbound request will not be answered. The connection closes with
    /// it, and its [`Self::Disconnected`] follows.
    RequestFailed { token: Token, reason: RequestFailure },
    /// A connection closed.
    Disconnected { token: Token },
}

/// Why an outbound request will not be answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestFailure {
    /// No answer arrived within [`HttpConfig::request_timeout`].
    Timeout,
    /// The endpoint answered with something that is not a response the
    /// service can frame.
    Malformed,
    /// The answer was larger than [`HttpConfig::max_head_bytes`] or
    /// [`HttpConfig::max_body_bytes`] allows: the caps are the operator's to
    /// raise.
    TooLarge,
    /// The endpoint closed the connection with the request in flight.
    Disconnected,
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
/// A request can be answered once, inline or later by token.
#[must_use = "respond now, or drop it to answer later with HttpService::respond; a request never \
              answered is closed by the idle sweep"]
pub struct Responder<'a> {
    net: &'a mut StreamNetwork,
    state: &'a mut ConnState,
    scratch: &'a mut Vec<u8>,
    token: Token,
    linger: Option<Linger>,
}

impl Responder<'_> {
    /// Queues the response and returns whether it was written.
    ///
    /// The status must be in `100..=599`; `Content-Length` and
    /// `Transfer-Encoding` are the service's to write, and a `Connection`
    /// header only feeds the close decision. Every status completes the
    /// request, `1xx` included: see [`HttpService::respond`].
    ///
    /// The request stops counting against connection limits here. Its bytes
    /// are reclaimed on the next pull or network tick, after the event's
    /// borrow ends. [`HttpService::respond`] reclaims before it returns.
    pub fn respond(self, status: u16, headers: &[(&str, &str)], body: &[u8]) -> bool {
        self.respond_with(status, headers, |out| out.extend_from_slice(body))
    }

    /// Queues the response with its body written by `body`, and returns
    /// whether it was written.
    ///
    /// The closure renders into a buffer the service keeps for the next
    /// response, so a body composed here costs no allocation. Every framing
    /// rule of [`Self::respond`] holds, and the closure runs whatever the
    /// request was: a `HEAD` request is answered with the `Content-Length`
    /// of the body it is not sent.
    pub fn respond_with(
        self,
        status: u16,
        headers: &[(&str, &str)],
        body: impl FnOnce(&mut Vec<u8>),
    ) -> bool {
        let Self { net, state, scratch, token, linger } = self;
        if state.phase != Phase::Pending {
            return false
        }
        if !(100..=599).contains(&status) ||
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
        scratch.clear();
        body(scratch);
        let body: &[u8] = scratch;
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
            if !close {
                state.phase = Phase::Idle;
            } else if state.framing_lost && linger.is_some() {
                // The peer is still sending a request stream this response ends.
                // Shutting the write side alone puts the answer in front of it
                // before the connection goes.
                net.shutdown_write_when_drained(token);
                // The caps time what follows the answer, so they start where the
                // answer ends: here when nothing was queued behind it, and at the
                // tick that sees the write side shut otherwise.
                let clock =
                    net.write_side_shut(token).then(|| LingerClock::started(Instant::now()));
                state.phase = Phase::Lingering { clock };
            } else {
                state.phase = Phase::Draining;
                net.disconnect_when_drained(token);
            }
        }
        ok
    }
}

/// What answering one request touches, and all a [`Responder`] may reach.
///
/// `req_end` marks a request awaiting its response. `consumed` excludes
/// answered bytes from limits before they are reclaimed.
struct ConnState {
    phase: Phase,
    close: bool,
    head_request: bool,
    /// Whether the request stream was cut short of what the connection owes
    /// its next response: what a closing response makes the peer read before
    /// the end of the stream rather than through a reset.
    framing_lost: bool,
    req_end: Option<usize>,
    consumed: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Awaiting the next request.
    Idle,
    /// A request was delivered and its response is outstanding.
    Pending,
    /// Answered with a close over a request stream read to its end; the
    /// connection goes once its bytes are written.
    Draining,
    /// Answered with a close over a request stream cut short: the write side
    /// shuts once the response is written, and what the peer sends is read
    /// and discarded until a cap or the peer itself ends the connection.
    ///
    /// The clock starts where the answer ends, so it is absent while the
    /// response is still on its way to the peer.
    Lingering { clock: Option<LingerClock> },
}

/// When a lingering connection began reading and discarding, and when it
/// last read anything: what [`Linger::total`] and [`Linger::idle`] measure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LingerClock {
    since: Instant,
    last_inbound: Instant,
}

impl LingerClock {
    fn started(at: Instant) -> Self {
        Self { since: at, last_inbound: at }
    }
}

enum Role {
    Accepted,
    Outbound { endpoint: Endpoint, in_flight: Option<InFlight> },
}

/// The request an outbound connection is waiting on, and all the service
/// needs of it once it is sent: a `HEAD` response carries no body, however
/// it is framed.
#[derive(Clone, Copy)]
struct InFlight {
    head: bool,
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
    /// When the request in flight on an outbound connection gives up.
    deadline: Option<Instant>,
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
            deadline: None,
            last_activity: Instant::now(),
            state: ConnState {
                phase: Phase::Idle,
                close: false,
                head_request: false,
                framing_lost: false,
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
        self.deadline = None;
        self.state.consumed = 0;
        self.state.req_end = None;
        self.state.phase = Phase::Idle;
        self.state.close = false;
        self.state.framing_lost = false;
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
    RequestFailed(Token, RequestFailure),
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
    RequestFailed {
        token: Token,
        reason: RequestFailure,
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
    /// The body of the last response written, kept for the next one.
    scratch: Vec<u8>,
    /// The connection awaiting bookkeeping from the last pulled event.
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
            scratch: Vec::new(),
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
        self.conns.push(Conn::new(token, Role::Outbound { endpoint, in_flight: None }));
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
        let timeout = self.config.request_timeout;
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
            .find(|conn| conn.token == token && in_flight(&conn.role).is_none())
        else {
            return false
        };
        let Role::Outbound { endpoint, .. } = &conn.role else { return false };
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
                match endpoint {
                    Endpoint::Tcp(addr) => write!(out, "Host: {addr}\r\n").unwrap(),
                    Endpoint::Unix(_) => out.extend_from_slice(b"Host: localhost\r\n"),
                }
            }
            write!(out, "Content-Length: {}\r\n\r\n", body.len()).unwrap();
            out.extend_from_slice(body);
        });
        if sent {
            set_in_flight(&mut conn.role, Some(InFlight { head: method == "HEAD" }));
            // The clock runs from here, not from the last byte reaching the
            // wire: a request queued behind a backlog is already waiting.
            conn.deadline = timeout.map(|timeout| Instant::now() + timeout);
        }
        sent
    }

    /// Answers a request whose [`Responder`] was dropped, and returns whether
    /// the response was written.
    ///
    /// Each call completes exactly one request for `token`. The status must
    /// be in `100..=599`, and one this service has no phrase for is framed
    /// with the empty reason phrase RFC 9112 permits — `HTTP/1.1 250 `.
    ///
    /// A `1xx` status is *final* here: it completes the request, framed with
    /// no `Content-Length` and no body, and the connection moves on to the
    /// next request or its close. HTTP makes an informational response the
    /// prelude to a final one instead, so this is a non-conformance the
    /// caller opts into by choosing such a status — what an endpoint echoing
    /// a status chosen elsewhere needs to pass it through untouched.
    ///
    /// Unlike [`Responder::respond`], this path holds no event borrow, so it
    /// reclaims answered bytes and requeues the connection before returning.
    pub fn respond(
        &mut self,
        net: &mut StreamNetwork,
        token: Token,
        status: u16,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> bool {
        self.respond_with(net, token, status, headers, |out| out.extend_from_slice(body))
    }

    /// Answers a request whose [`Responder`] was dropped with a body `body`
    /// writes, and returns whether the response was written.
    ///
    /// The closure renders into a buffer the service keeps for the next
    /// response, so a body composed here costs no allocation. Every framing
    /// rule of [`Self::respond`] holds, and the closure runs whatever the
    /// request was: a `HEAD` request is answered with the `Content-Length`
    /// of the body it is not sent. [`Responder::respond_with`] is the same
    /// answer, given inline.
    pub fn respond_with(
        &mut self,
        net: &mut StreamNetwork,
        token: Token,
        status: u16,
        headers: &[(&str, &str)],
        body: impl FnOnce(&mut Vec<u8>),
    ) -> bool {
        let Some(index) = self.index_of(token) else { return false };
        if !matches!(self.conns[index].role, Role::Accepted) {
            return false
        }
        let linger = self.config.linger;
        let Self { conns, scratch, .. } = self;
        let responder = Responder { net, state: &mut conns[index].state, scratch, token, linger };
        if !responder.respond_with(status, headers, body) {
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
            Step::RequestFailed { token, reason } => {
                Some(HttpEvent::RequestFailed { token, reason })
            }
            Step::Disconnected { token } => Some(HttpEvent::Disconnected { token }),
            Step::Request { index, method, path, version, body } => {
                let token = self.conns[index].token;
                let linger = self.config.linger;
                self.last = Some(token);
                let Self { conns, headers, scratch, .. } = self;
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
                    responder: Responder { net, state, scratch, token, linger },
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

    /// How many bytes a connection holds unparsed and unreclaimed.
    #[doc(hidden)]
    pub fn buffered(&self, token: Token) -> Option<usize> {
        self.index_of(token).map(|index| self.conns[index].buffer.len())
    }

    /// Applies bookkeeping deferred by the last pulled event.
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
                    Record::RequestFailed(token, reason) => {
                        self.take_record();
                        return Some(Step::RequestFailed { token, reason })
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
        let mut scratch = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut request =
            httparse::Request::new(&mut scratch[..headers_parsed(config.max_headers)]);
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
            in_flight(&self.conns[index].role)?;
            self.conns[index].reclaim();
            match self.parse_response(index) {
                ResponsePlan::Incomplete if !at_eof => return None,
                // The stream ended: what is buffered is either a response the
                // close delimits or a request nothing will ever answer.
                ResponsePlan::Incomplete => {
                    return Some(match self.plan_eof_response(index) {
                        Ok(Some(step)) => step,
                        Ok(None) => {
                            let token = self.abandon_request(index);
                            Step::RequestFailed { token, reason: RequestFailure::Disconnected }
                        }
                        Err(reason) => {
                            let token = self.abandon_request(index);
                            Step::RequestFailed { token, reason }
                        }
                    })
                }
                ResponsePlan::Fail { reason } => {
                    let token = self.conns[index].token;
                    self.fail_outbound(net, index);
                    return Some(Step::RequestFailed { token, reason })
                }
                ResponsePlan::Informational { end } => self.conns[index].state.consumed = end,
                ResponsePlan::Ready { version, status, reason, body, end, close } => {
                    let token = self.conns[index].token;
                    let conn = &mut self.conns[index];
                    conn.state.consumed = end;
                    conn.deadline = None;
                    set_in_flight(&mut conn.role, None);
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
        let mut scratch = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut response =
            httparse::Response::new(&mut scratch[..headers_parsed(config.max_headers)]);
        let Ok(status) = response.parse(buffer) else { return malformed() };
        let httparse::Status::Complete(head) = status else {
            // A partial parse means every buffered byte is still head bytes.
            if !crlf_only(buffer) {
                return malformed()
            }
            if buffer.len() > config.max_head_bytes {
                return too_large()
            }
            return ResponsePlan::Incomplete
        };
        if !crlf_only(&buffer[..head]) {
            return malformed()
        }
        if head > config.max_head_bytes {
            return too_large()
        }
        let code = response.code.unwrap_or(0);
        let no_body = in_flight(&conn.role).is_some_and(|request| request.head) ||
            matches!(code, 100..=199 | 204 | 304);
        let chunked = transfer_chunked(response.headers);
        let content_length = response_content_length(response.headers);
        if code == 101 ||
            matches!(content_length, ContentLength::Invalid) ||
            chunked.is_none() ||
            (chunked == Some(true) && !matches!(content_length, ContentLength::Absent))
        {
            return malformed()
        }
        if !no_body &&
            matches!(content_length, ContentLength::Present(length) if length > config.max_body_bytes)
        {
            return too_large()
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
                    let Some(end) = head.checked_add(consumed) else { return malformed() };
                    (end, Body::Decoded)
                }
                Ok(None) => return ResponsePlan::Incomplete,
                Err(reason) => return ResponsePlan::Fail { reason },
            }
        } else if let ContentLength::Present(length) = content_length {
            let Some(end) = head.checked_add(length) else { return malformed() };
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

    /// Parses a response whose body the closing connection delimits,
    /// reporting a cap it broke rather than a head it could not complete.
    fn plan_eof_response(&mut self, index: usize) -> Result<Option<Step>, RequestFailure> {
        let plan = {
            let Self { conns, headers, config, .. } = self;
            let conn = &conns[index];
            let base = conn.start;
            let buffer = &conn.buffer[base..];
            let mut scratch = [httparse::EMPTY_HEADER; MAX_HEADERS];
            let mut response =
                httparse::Response::new(&mut scratch[..headers_parsed(config.max_headers)]);
            let Ok(httparse::Status::Complete(head)) = response.parse(buffer) else {
                return Ok(None)
            };
            if !crlf_only(&buffer[..head]) || head > config.max_head_bytes {
                return Ok(None)
            }
            let code = response.code.unwrap_or(0);
            let no_body = in_flight(&conn.role).is_some_and(|request| request.head) ||
                matches!(code, 100..=199 | 204 | 304);
            if no_body ||
                transfer_chunked(response.headers) != Some(false) ||
                !matches!(response_content_length(response.headers), ContentLength::Absent)
            {
                return Ok(None)
            }
            if buffer.len() - head > config.max_body_bytes {
                return Err(RequestFailure::TooLarge)
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
        conn.deadline = None;
        set_in_flight(&mut conn.role, None);
        Ok(Some(plan))
    }

    /// Gives up on the request in flight, so nothing answers or fails it
    /// twice.
    fn abandon_request(&mut self, index: usize) -> Token {
        self.conns[index].deadline = None;
        set_in_flight(&mut self.conns[index].role, None);
        self.conns[index].token
    }

    /// Answers a request the service itself rejected, and closes after it.
    fn error(&mut self, net: &mut StreamNetwork, index: usize, status: u16) {
        let token = self.conns[index].token;
        let linger = self.config.linger;
        let Self { conns, scratch, .. } = self;
        let state = &mut conns[index].state;
        state.phase = Phase::Pending;
        state.close = true;
        state.head_request = false;
        // The request this answers was never read to its end, whether it was
        // unparseable, too large, or framed in a way the service refuses.
        state.framing_lost = true;
        Responder { net, state, scratch, token, linger }.respond_with(status, &[], |_| {});
    }

    /// Drops an outbound connection whose peer broke the protocol.
    fn fail_outbound(&mut self, net: &mut StreamNetwork, index: usize) {
        let token = self.conns[index].token;
        self.conns[index].clear();
        set_in_flight(&mut self.conns[index].role, None);
        net.disconnect(token);
    }

    /// Forgets what a closed connection held: an accepted one goes, an
    /// outbound one stays for its next connection.
    fn close_connection(&mut self, index: usize) {
        if matches!(self.conns[index].role, Role::Accepted) {
            self.conns.swap_remove(index);
        } else {
            self.conns[index].clear();
            set_in_flight(&mut self.conns[index].role, None);
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
        // A lingering connection reads its peer out rather than resetting it,
        // and owes an answer to nothing it reads: the bytes are dropped where
        // they arrive, and only the moment they arrived is kept.
        if let Phase::Lingering { clock } = &mut conn.state.phase {
            if let Some(clock) = clock {
                clock.last_inbound = Instant::now();
            }
            return
        }
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
                // Whether the close delimited a response or lost the request
                // is the pull's to say, from what the connection buffered;
                // the deadline is over either way, and a failure queued
                // behind this record would reach the caller out of order.
                if let Some(index) = self.index_of(token) {
                    self.conns[index].deadline = None;
                }
                self.records.push(Record::Disconnected(token));
            }
        }
    }

    fn tick(&mut self, net: &mut StreamNetwork, now: Instant) -> bool {
        // Apply deferred bookkeeping before reporting pullable work.
        self.apply_bookkeeping();
        for index in 0..self.conns.len() {
            let conn = &self.conns[index];
            // A connection queued as ready is answered first: the bytes over
            // the limit may be the ones that complete a request the service
            // rejects with a status of its own.
            if !conn.over_limit || conn.ready {
                continue
            }
            let accepted = matches!(conn.role, Role::Accepted);
            match conn.state.phase {
                // A full buffer is no verdict on a request already delivered.
                // The response the caller is producing still goes out, and
                // takes the connection into its lingering close: the bytes
                // dropped at the limit are the ones that would have completed
                // whatever the peer is sending next.
                Phase::Pending => {
                    let state = &mut self.conns[index].state;
                    state.close = true;
                    state.framing_lost = true;
                }
                // The head of whatever the client is sending was dropped at
                // the limit, which is the framing loss every other head too
                // large for the service answers with.
                Phase::Idle if accepted => self.error(net, index, 431),
                // An endpoint that overruns the limit is told nothing: what
                // the service owes it is the request it already sent.
                Phase::Idle => {
                    self.conns[index].over_limit = false;
                    net.disconnect(self.conns[index].token);
                }
                Phase::Draining | Phase::Lingering { .. } => {}
            }
        }
        if let Some(timeout) = self.config.idle_timeout {
            for conn in &self.conns {
                // A lingering connection answers to the linger's caps once
                // they are running. Until the answer has left it is held to
                // the same bound as any other, a draining one included: a
                // peer that stops reading must not hold a connection open by
                // never taking what it asked for.
                if matches!(conn.role, Role::Accepted) &&
                    !matches!(conn.state.phase, Phase::Lingering { clock: Some(_) }) &&
                    now.saturating_sub(conn.last_activity) >= timeout
                {
                    net.disconnect(conn.token);
                }
            }
        }
        for index in 0..self.conns.len() {
            if self.conns[index].deadline.is_none_or(|deadline| now < deadline) {
                continue
            }
            // A late answer would arrive against a request nothing is waiting
            // for, so the connection goes with the deadline. The endpoint
            // reconnects at its group's interval.
            let token = self.abandon_request(index);
            self.records.push(Record::RequestFailed(token, RequestFailure::Timeout));
            net.disconnect(token);
        }
        if let Some(linger) = self.config.linger {
            for index in 0..self.conns.len() {
                let token = self.conns[index].token;
                let Phase::Lingering { clock } = &mut self.conns[index].state.phase else {
                    continue
                };
                // Until the transport has the whole answer there is nothing
                // to cap: what bounds a connection still writing is its
                // transport, as it is for one that is draining. Asking costs
                // a walk of the network's connections, so only a linger that
                // is still waiting on its answer asks.
                let clock = match *clock {
                    Some(clock) => clock,
                    // `now` is the instant the poll returned, so the caps run
                    // from the end of the wait rather than the start of it.
                    None if net.write_side_shut(token) => *clock.insert(LingerClock::started(now)),
                    None => continue,
                };
                if now.saturating_sub(clock.last_inbound) >= linger.idle ||
                    now.saturating_sub(clock.since) >= linger.total
                {
                    net.disconnect(token);
                }
            }
        }
        self.pullable()
    }

    fn next_deadline(&self) -> Option<Instant> {
        let mut next: Option<Instant> = None;
        for conn in &self.conns {
            if !matches!(conn.role, Role::Accepted) {
                next = fold(next, conn.deadline);
                continue
            }
            let at = match conn.state.phase {
                // A linger whose clock has yet to start keeps the sweep it
                // has yet to be excused from.
                Phase::Lingering { clock: Some(clock) } => self.config.linger.map(|linger| {
                    (clock.last_inbound + linger.idle).min(clock.since + linger.total)
                }),
                _ => self.config.idle_timeout.map(|timeout| conn.last_activity + timeout),
            };
            next = fold(next, at);
        }
        next
    }
}

/// The earlier of two deadlines, either of which may be absent.
fn fold(next: Option<Instant>, at: Option<Instant>) -> Option<Instant> {
    match (next, at) {
        (Some(next), Some(at)) => Some(next.min(at)),
        (next, at) => next.or(at),
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
    /// The request it answers fails, for this reason.
    Fail {
        reason: RequestFailure,
    },
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

fn malformed() -> ResponsePlan {
    ResponsePlan::Fail { reason: RequestFailure::Malformed }
}

fn too_large() -> ResponsePlan {
    ResponsePlan::Fail { reason: RequestFailure::TooLarge }
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

/// How many headers a message is parsed into: what the configuration asks
/// for, held to the scratch every parse borrows.
fn headers_parsed(max_headers: usize) -> usize {
    max_headers.min(MAX_HEADERS)
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

fn in_flight(role: &Role) -> Option<InFlight> {
    match role {
        Role::Outbound { in_flight, .. } => *in_flight,
        Role::Accepted => None,
    }
}

fn set_in_flight(role: &mut Role, request: Option<InFlight>) {
    if let Role::Outbound { in_flight, .. } = role {
        *in_flight = request;
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
) -> Result<Option<usize>, RequestFailure> {
    let Some((end, body_len)) = chunked_end(bytes, max_body_bytes, max_headers)? else {
        return Ok(None)
    };
    out.clear();
    out.reserve(body_len);
    let mut at = 0;
    while at < end {
        let httparse::Status::Complete((consumed, size)) =
            httparse::parse_chunk_size(&bytes[at..]).map_err(|_| RequestFailure::Malformed)?
        else {
            return Err(RequestFailure::Malformed)
        };
        at = at.checked_add(consumed).ok_or(RequestFailure::Malformed)?;
        let size = usize::try_from(size).map_err(|_| RequestFailure::Malformed)?;
        if size == 0 {
            return Ok(Some(end))
        }
        out.extend_from_slice(&bytes[at..at + size]);
        at = at.checked_add(size + 2).ok_or(RequestFailure::Malformed)?;
    }
    Err(RequestFailure::Malformed)
}

fn chunked_end(
    bytes: &[u8],
    max_body_bytes: usize,
    max_headers: usize,
) -> Result<Option<(usize, usize)>, RequestFailure> {
    let mut at = 0;
    let mut body_len = 0;
    loop {
        let httparse::Status::Complete((consumed, size)) =
            httparse::parse_chunk_size(&bytes[at..]).map_err(|_| RequestFailure::Malformed)?
        else {
            return Ok(None)
        };
        let size = usize::try_from(size).map_err(|_| RequestFailure::Malformed)?;
        if size > max_body_bytes.saturating_sub(body_len) {
            return Err(RequestFailure::TooLarge)
        }
        at = at.checked_add(consumed).ok_or(RequestFailure::Malformed)?;
        if size == 0 {
            let mut scratch = [httparse::EMPTY_HEADER; MAX_HEADERS];
            let httparse::Status::Complete((consumed, _)) =
                httparse::parse_headers(&bytes[at..], &mut scratch[..headers_parsed(max_headers)])
                    .map_err(|_| RequestFailure::Malformed)?
            else {
                return Ok(None)
            };
            let end = at.checked_add(consumed).ok_or(RequestFailure::Malformed)?;
            if !crlf_only(&bytes[at..end]) {
                return Err(RequestFailure::Malformed)
            }
            return Ok(Some((end, body_len)))
        }
        let Some(chunk_end) = at.checked_add(size).and_then(|at| at.checked_add(2)) else {
            return Err(RequestFailure::Malformed)
        };
        if bytes.len() < chunk_end || &bytes[at + size..chunk_end] != b"\r\n" {
            return Ok(None)
        }
        body_len += size;
        at = chunk_end;
    }
}

/// HTTP reason phrase for common status codes, and the empty phrase RFC
/// 9112 permits for every other status.
pub fn reason_phrase(status: u16) -> &'static str {
    match status {
        100 => "Continue",
        101 => "Switching Protocols",
        200 => "OK",
        201 => "Created",
        203 => "Non-Authoritative Information",
        204 => "No Content",
        205 => "Reset Content",
        206 => "Partial Content",
        300 => "Multiple Choices",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        402 => "Payment Required",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        407 => "Proxy Authentication Required",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Payload Too Large",
        414 => "URI Too Long",
        415 => "Unsupported Media Type",
        416 => "Range Not Satisfiable",
        417 => "Expectation Failed",
        418 => "I'm a Teapot",
        421 => "Misdirected Request",
        422 => "Unprocessable Content",
        423 => "Locked",
        424 => "Failed Dependency",
        425 => "Too Early",
        426 => "Upgrade Required",
        428 => "Precondition Required",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        451 => "Unavailable For Legal Reasons",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        505 => "HTTP Version Not Supported",
        506 => "Variant Also Negotiates",
        507 => "Insufficient Storage",
        508 => "Loop Detected",
        510 => "Not Extended",
        511 => "Network Authentication Required",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write as _,
        net::{Ipv4Addr, SocketAddr, TcpStream},
    };

    use flux_timing::Duration;
    use mio::Token;

    use super::{
        Conn, HttpConfig, HttpEvent, HttpRequest, HttpService, Phase, Role, reason_phrase, span,
    };
    use crate::stream::{ConnectionGroupConfig, Endpoint, Framing, StreamNetwork};

    const PATIENCE: std::time::Duration = std::time::Duration::from_secs(10);

    #[test]
    fn an_unmapped_status_has_no_reason_phrase() {
        assert_eq!(reason_phrase(250), "");
        assert_eq!(reason_phrase(199), "");
        assert_eq!(reason_phrase(599), "");
        assert_eq!(reason_phrase(200), "OK");
        assert_eq!(reason_phrase(404), "Not Found");
    }

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

    fn unused_addr() -> SocketAddr {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr
    }

    /// A service on a raw group of its own, with no listener and no
    /// connections.
    fn bare_service(net: &mut StreamNetwork) -> HttpService {
        let group = net.add_group(ConnectionGroupConfig {
            name: "http",
            framing: Framing::Raw,
            ..ConnectionGroupConfig::default()
        });
        HttpService::new(net, group, HttpConfig::default())
    }

    /// A request of exactly `size` bytes, padded out with a header.
    fn padded_request(path: &str, size: usize) -> Vec<u8> {
        let bare = format!("GET {path} HTTP/1.1\r\nX-Pad: \r\n\r\n");
        let pad = size.checked_sub(bare.len()).expect("the request outgrew its size");
        let request = format!("GET {path} HTTP/1.1\r\nX-Pad: {}\r\n\r\n", "p".repeat(pad));
        assert_eq!(request.len(), size);
        request.into_bytes()
    }

    /// A connection holding `len` buffered bytes, answered up to `consumed`.
    fn buffered_conn(len: usize, consumed: usize, req_end: Option<usize>) -> Conn {
        let mut conn = Conn::new(Token(1), Role::Accepted);
        conn.buffer = vec![b'x'; len];
        conn.state.consumed = consumed;
        conn.state.req_end = req_end;
        conn
    }

    #[test]
    fn an_answered_prefix_is_dropped_once_it_is_half_the_buffer() {
        // Under half, the cursor moves onto the answered bytes and they stay.
        let mut conn = buffered_conn(100, 40, Some(90));
        conn.reclaim();
        assert_eq!((conn.start, conn.state.consumed, conn.state.req_end), (40, 40, Some(90)));
        assert_eq!(conn.buffer.len(), 100);
        assert_eq!(conn.unconsumed(), 60);

        // Half or more, the prefix goes and every cursor comes down with it.
        let mut conn = buffered_conn(100, 60, Some(90));
        conn.reclaim();
        assert_eq!((conn.start, conn.state.consumed, conn.state.req_end), (0, 0, Some(30)));
        assert_eq!(conn.buffer.len(), 40);
        assert_eq!(conn.unconsumed(), 40);
    }

    #[test]
    fn a_compaction_rebases_every_cursor() {
        let mut conn = buffered_conn(200, 128, Some(160));
        conn.buffer[128..].fill(b'y');
        conn.start = 128;
        conn.compact();
        assert_eq!(conn.start, 0);
        assert_eq!(conn.state.consumed, 0);
        assert_eq!(conn.state.req_end, Some(32), "the pending request moved with the buffer");
        assert_eq!(conn.buffer, vec![b'y'; 72], "the answered prefix is what went");
    }

    #[test]
    fn a_ready_connection_is_queued_once() {
        let mut net = StreamNetwork::default();
        let mut http = bare_service(&mut net);
        http.conns.push(Conn::new(Token(1), Role::Accepted));

        http.mark_ready(0);
        http.mark_ready(0);
        assert_eq!(http.ready, [Token(1)], "one entry however many reads arrive");
        assert!(http.conns[0].ready);

        // Taking it off the queue frees it to be queued by the next read.
        assert_eq!(http.pop_ready(), Some(0));
        assert!(!http.conns[0].ready);
        http.mark_ready(0);
        assert_eq!(http.ready, [Token(1)]);
    }

    #[test]
    fn a_connection_owing_a_response_is_never_queued() {
        let mut net = StreamNetwork::default();
        let mut http = bare_service(&mut net);
        let mut conn = Conn::new(Token(1), Role::Accepted);
        conn.state.phase = Phase::Pending;
        http.conns.push(conn);

        http.mark_ready(0);
        assert!(http.ready.is_empty());
        assert!(!http.conns[0].ready);

        // The answer frees it to parse the request pipelined behind it.
        http.conns[0].state.phase = Phase::Idle;
        http.mark_ready(0);
        assert_eq!(http.ready, [Token(1)]);
    }

    /// A service listening on loopback with one client connected to it, for
    /// the tests that need a connection to answer on.
    struct Harness {
        net: StreamNetwork,
        http: HttpService,
        /// The client end, held open for as long as the harness lives.
        _client: TcpStream,
    }

    impl Harness {
        /// A service holding two pipelined requests, the second of them the
        /// larger: an answered first request is then under half the buffer,
        /// where reclaiming it moves the cursor without moving the bytes.
        fn with_two_requests() -> (Self, usize) {
            let mut net = StreamNetwork::default();
            let mut http = bare_service(&mut net);
            let addr = unused_addr();
            http.listen(&mut net, Endpoint::Tcp(addr)).unwrap();
            let mut client = TcpStream::connect(addr).unwrap();
            let first = padded_request("/one", 64);
            client.write_all(&[first.clone(), padded_request("/two", 192)].concat()).unwrap();
            (Self { net, http, _client: client }, first.len())
        }

        fn drive(&mut self) -> bool {
            self.net.drive(Some(Duration::ZERO), &mut [self.http.as_service()], |_| {})
        }

        /// Drives until a request is delivered and reports its token,
        /// answering it through the responder it came with when `inline`.
        fn pull_request(&mut self, inline: bool) -> Token {
            let deadline = std::time::Instant::now() + PATIENCE;
            while std::time::Instant::now() < deadline {
                self.drive();
                while let Some(event) = self.http.next_event(&mut self.net) {
                    if let HttpEvent::Request { token, responder, .. } = event {
                        if inline {
                            assert!(responder.respond(200, &[], b""));
                        }
                        return token
                    }
                }
            }
            panic!("no request arrived")
        }

        /// Pulls one event with no driver call before it, which is what the
        /// reclaim-timing tests are about.
        fn pull_without_driving(&mut self) -> bool {
            self.http.next_event(&mut self.net).is_some()
        }

        fn respond(&mut self, token: Token) -> bool {
            self.http.respond(&mut self.net, token, 200, &[], b"")
        }

        /// Where a connection has parsed and answered up to: `start`,
        /// `consumed` and `req_end`.
        fn cursors(&self, token: Token) -> (usize, usize, Option<usize>) {
            let index = self.http.index_of(token).expect("the connection went");
            let conn = &self.http.conns[index];
            (conn.start, conn.state.consumed, conn.state.req_end)
        }

        /// Connections queued as ready to parse.
        fn queued(&self) -> usize {
            self.http.ready.len() - self.http.ready_cursor
        }
    }

    #[test]
    fn an_inline_answer_reclaims_at_the_next_pull() {
        let (mut harness, first) = Harness::with_two_requests();
        let token = harness.pull_request(true);

        // The event borrowed the connection, so the answer moved `consumed`
        // and nothing else: `start` sits where the parse left it, and the
        // connection stays out of the ready queue.
        assert_eq!(harness.cursors(token), (0, first, None));
        assert_eq!(harness.queued(), 0);

        assert!(harness.pull_without_driving(), "the pipelined request is delivered");
        let (start, consumed, _) = harness.cursors(token);
        assert_eq!(start, consumed, "the pull reclaimed what the answer consumed");
        assert_eq!(start, first, "the pipelined request holds off the compaction");
    }

    #[test]
    fn an_inline_answer_reclaims_at_the_next_drive() {
        let (mut harness, first) = Harness::with_two_requests();
        let token = harness.pull_request(true);
        assert_eq!(harness.cursors(token), (0, first, None));

        harness.drive();
        let (start, consumed, _) = harness.cursors(token);
        assert_eq!(start, consumed, "the tick reclaimed what the answer consumed");
        assert_eq!(start, first, "the pipelined request holds off the compaction");
        assert_eq!(harness.queued(), 1, "and queued the request behind it");
    }

    #[test]
    fn a_by_token_answer_reclaims_before_it_returns() {
        let (mut harness, first) = Harness::with_two_requests();
        let token = harness.pull_request(false);
        assert_eq!(harness.cursors(token), (0, 0, Some(first)));
        assert_eq!(harness.queued(), 0);

        // Nothing borrows the connection, so the answer reclaims and queues
        // with no driver call in between.
        assert!(harness.respond(token));
        assert_eq!(harness.cursors(token), (first, first, None));
        assert_eq!(harness.queued(), 1, "the pipelined request is queued at once");

        // A request is answered once, and the refusal consumes nothing.
        assert!(!harness.respond(token));
        assert_eq!(harness.cursors(token), (first, first, None));
    }

    /// Serves two pipelined requests, answering the first inline or by token,
    /// and reports the cursors left once the second has been pulled.
    fn cursors_after_two_requests(inline: bool) -> (usize, usize, Option<usize>) {
        let (mut harness, _) = Harness::with_two_requests();
        let first = harness.pull_request(inline);
        if !inline {
            assert!(harness.respond(first));
        }
        let second = harness.pull_request(false);
        assert_eq!(second, first, "both requests came in on one connection");
        harness.cursors(second)
    }

    #[test]
    fn both_answer_paths_leave_the_same_cursors() {
        assert_eq!(cursors_after_two_requests(true), cursors_after_two_requests(false));
    }
}
