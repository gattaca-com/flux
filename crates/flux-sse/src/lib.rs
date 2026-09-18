//! Server-sent events over a streaming [`HttpNetwork`] response.
//!
//! [`Sse::connect`] opens the endpoint, [`Sse::on_event`] claims this client's
//! network events, and [`Sse::drive`] issues the `GET` and delivers parsed
//! events. The framing is WHATWG SSE without resumption: no `id`, no `retry`,
//! and therefore no `Last-Event-ID`, because the beacon `/eth/v1/events`
//! endpoint sends none of them.
//!
//! # Gaps
//! SSE has no replay, so every reconnect silently loses whatever the server
//! published while the stream was down. [`SseEvent::Gap`] reports that, rather
//! than healing over it: for a consumer of `payload_attributes` a lost event
//! is a lost block, and the tile is expected to resync over REST instead of
//! assuming continuity.
//!
//! # Limitations
//! [`Sse::drive`] delivers every event buffered since the last poll, with no
//! ceiling on the work one call can do. That is fine for the beacon topics
//! that run at one or two events per slot, but a firehose such as
//! `attestation` or `blob_sidecar` would need a bound per poll.
//!
//! A subscription the server keeps refusing is retried at the reconnect
//! interval indefinitely, reporting [`SseEvent::Status`] each time; a caller
//! that sees the same status repeatedly, such as a mistyped path or a node
//! behind auth, should [`Sse::close`] rather than let it redial forever.
//!
//! A byte-order mark split across the first two chunks is not stripped, which
//! needs a server that flushes fewer than three bytes to begin with.

use std::{fmt::Write as _, net::SocketAddr};

use flux_network::{
    Token,
    http::{HttpEvent, HttpNetwork},
    tcp::{TcpEvent, TcpNetworkCore},
};
use flux_timing::Duration;

/// Long enough that a missed slot is not mistaken for a dead stream: beacon
/// events run on slot cadence and many nodes send no keepalives at all.
const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 60;
/// A dropped stream loses events, so redial well inside one slot.
const DEFAULT_RECONNECT_INTERVAL_MS: u64 = 250;
const DEFAULT_MAX_EVENT_BYTES: usize = 1024 * 1024;

/// What the caller sees from the stream.
#[derive(Debug, PartialEq, Eq)]
pub enum SseEvent<'a> {
    /// The server accepted the subscription; events follow.
    Open,
    /// One dispatched event. `kind` is the `event:` field, `message` by
    /// default; `data` is the joined `data:` lines, still encoded.
    Message { kind: &'a str, data: &'a [u8] },
    /// The stream ended after it had opened. Whatever the server published
    /// before the next [`SseEvent::Open`] is lost.
    Gap,
    /// The server answered the subscription with a non-2xx status.
    Status(u16),
}

pub struct Sse {
    addr: SocketAddr,
    path: String,
    query: Vec<(String, String)>,
    headers: Vec<(String, String)>,
    http: HttpNetwork,
    token: Option<Token>,
    /// Whether the current stream reached its head.
    open: bool,
    parser: Parser,
}

impl Sse {
    /// Prepares a subscription to `path` on `addr`.
    pub fn new(addr: SocketAddr, path: &str) -> Self {
        Self {
            addr,
            path: path.to_owned(),
            query: Vec::new(),
            headers: Vec::new(),
            http: HttpNetwork::default()
                .with_name("sse")
                .with_max_body_bytes(DEFAULT_MAX_EVENT_BYTES)
                .with_stream_idle_timeout(Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS))
                .with_reconnect_interval(Duration::from_millis(DEFAULT_RECONNECT_INTERVAL_MS)),
            token: None,
            open: false,
            parser: Parser::default(),
        }
    }
    /// Appends query parameters, percent-encoded.
    #[must_use]
    pub fn with_query(mut self, query: &[(&str, &str)]) -> Self {
        assert!(self.token.is_none(), "configure before connect");
        for (name, value) in query {
            self.query.push(((*name).to_owned(), (*value).to_owned()));
        }
        self
    }
    /// Adds a request header sent with the subscription.
    #[must_use]
    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        assert!(self.token.is_none(), "configure before connect");
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }
    /// Drops a stream that has gone this long without a byte, so it redials.
    #[must_use]
    pub fn with_idle_timeout(self, idle_timeout: Duration) -> Self {
        self.with_http(|http| http.with_stream_idle_timeout(idle_timeout))
    }
    /// Configures the HTTP layer this client owns, through its own builders.
    #[must_use]
    pub fn with_http(mut self, configure: impl FnOnce(HttpNetwork) -> HttpNetwork) -> Self {
        assert!(self.token.is_none(), "configure before connect");
        self.http = configure(self.http);
        self
    }
    /// Opens the endpoint; the subscription is issued once it connects.
    pub fn connect(&mut self, net: &mut TcpNetworkCore) {
        assert!(self.token.is_none(), "connect once");
        self.token = Some(self.http.connect(net, self.addr));
    }
    /// Like [`Self::connect`] but over TLS, sending and verifying `host`.
    #[cfg(feature = "tls")]
    pub fn connect_tls(&mut self, net: &mut TcpNetworkCore, host: &str) {
        assert!(self.token.is_none(), "connect once");
        self.token = Some(self.http.connect_tls(net, self.addr, host));
    }
    /// Returns whether the event belonged to this client.
    pub fn on_event(&mut self, event: &TcpEvent<'_>) -> bool {
        self.http.on_event(event)
    }
    /// Issues the subscription when the endpoint connects, then delivers the
    /// events parsed since the last call.
    pub fn drive<F>(&mut self, net: &mut TcpNetworkCore, mut handler: F)
    where
        F: for<'a> FnMut(SseEvent<'a>),
    {
        let token = self.token.expect("connect before drive");
        let Self { http, path, query, headers, open, parser, .. } = self;
        let mut subscribe = false;
        let mut ended = false;
        http.drive(net, |event| match event {
            HttpEvent::Connected { .. } => subscribe = true,
            HttpEvent::ResponseHead { status, .. } => {
                if (200..=299).contains(&status) {
                    *open = true;
                    handler(SseEvent::Open);
                } else {
                    handler(SseEvent::Status(status));
                }
            }
            HttpEvent::Body { chunk, .. } => {
                if *open {
                    parser.push(chunk, &mut handler);
                }
            }
            HttpEvent::StreamEnd { .. } => {
                // A partial event cannot be completed, and the bytes after it
                // are gone, so the caller resyncs instead.
                parser.reset();
                if std::mem::take(open) {
                    handler(SseEvent::Gap);
                }
                ended = true;
            }
            _ => {}
        });
        if ended {
            // A server that keeps the connection open after ending the
            // stream, which a non-2xx or a terminal chunk both do, would
            // otherwise never connect again and never be resubscribed to.
            // Redialling paces the retry at the reconnect interval.
            net.disconnect(token);
        }
        if subscribe {
            let target = request_target(path, query);
            let headers: Vec<(&str, &str)> = std::iter::once(("Accept", "text/event-stream"))
                .chain(headers.iter().map(|(n, v)| (n.as_str(), v.as_str())))
                .collect();
            self.http.request_stream(net, token, "GET", &target, &headers);
        }
    }
    /// Removes the endpoint; a running stream ends without a [`SseEvent::Gap`].
    pub fn close(self, net: &mut TcpNetworkCore) {
        self.http.close(net);
    }
}

/// Accumulates one event across chunk boundaries.
#[derive(Default)]
struct Parser {
    /// Bytes of the current line, carried across chunks.
    line: Vec<u8>,
    kind: String,
    data: Vec<u8>,
    has_data: bool,
    /// Whether a `\r` ended the previous chunk, so a leading `\n` is its pair.
    pending_lf: bool,
    /// Whether the leading byte-order mark has been considered.
    started: bool,
}

impl Parser {
    fn reset(&mut self) {
        self.line.clear();
        self.kind.clear();
        self.data.clear();
        self.has_data = false;
        self.pending_lf = false;
        self.started = false;
    }
    /// Feeds one body chunk, dispatching every event it completes.
    fn push<F>(&mut self, chunk: &[u8], handler: &mut F)
    where
        F: for<'a> FnMut(SseEvent<'a>),
    {
        let mut chunk = chunk;
        if !self.started {
            self.started = true;
            if let Some(rest) = chunk.strip_prefix("\u{feff}".as_bytes()) {
                chunk = rest;
            }
        }
        for &byte in chunk {
            // A CRLF is one terminator, so the LF after a CR is not a line.
            if std::mem::take(&mut self.pending_lf) && byte == b'\n' {
                continue
            }
            match byte {
                b'\r' => {
                    self.pending_lf = true;
                    self.end_line(handler);
                }
                b'\n' => self.end_line(handler),
                _ => self.line.push(byte),
            }
        }
    }
    fn end_line<F>(&mut self, handler: &mut F)
    where
        F: for<'a> FnMut(SseEvent<'a>),
    {
        let line = std::mem::take(&mut self.line);
        if line.is_empty() {
            self.dispatch(handler);
            self.line = line;
            return
        }
        // A comment line keeps the connection warm and carries nothing.
        if line[0] != b':' {
            let colon = line.iter().position(|&byte| byte == b':');
            let (field, value) = colon.map_or((&line[..], &[][..]), |colon| {
                let value = &line[colon + 1..];
                // Exactly one space after the colon belongs to the syntax.
                (&line[..colon], value.strip_prefix(b" ").unwrap_or(value))
            });
            match field {
                b"data" => {
                    if self.has_data {
                        self.data.push(b'\n');
                    }
                    self.data.extend_from_slice(value);
                    self.has_data = true;
                }
                b"event" => {
                    self.kind.clear();
                    self.kind.push_str(&String::from_utf8_lossy(value));
                }
                // `id` and `retry` are resumption, which this client does not
                // do; anything else is unknown and ignored too.
                _ => {}
            }
        }
        self.line = line;
        self.line.clear();
    }
    fn dispatch<F>(&mut self, handler: &mut F)
    where
        F: for<'a> FnMut(SseEvent<'a>),
    {
        let kind = std::mem::take(&mut self.kind);
        let had_data = std::mem::take(&mut self.has_data);
        if had_data {
            // Joining the lines with a newline already matches the spec, which
            // appends one per line and drops the last; a trailing empty data
            // line therefore keeps its newline. The buffer is handed over as
            // it stands, so an event costs no copy.
            let kind = if kind.is_empty() { "message" } else { kind.as_str() };
            handler(SseEvent::Message { kind, data: &self.data });
        }
        self.data.clear();
        self.kind = kind;
        self.kind.clear();
    }
}

/// `path?name=value&...`, percent-encoding each parameter.
fn request_target(path: &str, query: &[(String, String)]) -> String {
    let mut target = path.to_owned();
    for (index, (name, value)) in query.iter().enumerate() {
        target.push(if index == 0 { '?' } else { '&' });
        percent_encode(&mut target, name);
        target.push('=');
        percent_encode(&mut target, value);
    }
    target
}

fn percent_encode(out: &mut String, value: &str) {
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
            out.push(byte as char);
        } else {
            write!(out, "%{byte:02X}").unwrap();
        }
    }
}
