//! Poll-driven `ClickHouse` client over its HTTP interface.
//!
//! [`ClickHouse`] keeps a pool of persistent connections to one server and is
//! driven from the caller's event loop like [`HttpNetwork`]:
//! [`ClickHouse::query`] and [`ClickHouse::insert`] hand one request to an idle
//! connection and return a [`QueryId`]; [`ClickHouse::poll_with`] delivers each
//! outcome exactly once. Nothing blocks or queues: when every connection is
//! busy or disconnected a send returns `None` and the caller keeps its batch
//! for a later poll.
//!
//! ```no_run
//! use flux_clickhouse::ClickHouse;
//! let mut ch = ClickHouse::new("127.0.0.1:8123".parse().unwrap())
//!     .with_credentials("default", "")
//!     .with_database("telemetry");
//! let rows: Vec<u8> = Vec::new(); // RowBinary-encoded batch
//! let mut pending = None;
//! loop {
//!     ch.poll_with(|id, result| match result {
//!         Ok(response) => println!("{id:?} ok: {} bytes", response.body.len()),
//!         Err(err) => eprintln!("{id:?} failed: {err}"),
//!     });
//!     if pending.is_none() {
//!         pending = ch.insert("INSERT INTO events FORMAT RowBinary", &rows);
//!     }
//! }
//! ```
//!
//! Every request is a `POST`: `query` sends the SQL as the body, `insert` puts
//! it in the `query` URL parameter and sends the data as the body so binary
//! formats stay intact. Settings, including the database, travel as URL
//! parameters; credentials as `X-ClickHouse-User` and `X-ClickHouse-Key`.
//! `wait_end_of_query=1` is set by default so a failing query yields a non-200
//! status instead of an error appended to a 200 body.
//!
//! # Limitations
//! No TLS, compression, streaming, or cancellation. Bodies in both directions
//! are bounded by [`ClickHouse::with_max_body_bytes`]. `ClickHouse` closes
//! idle keep-alive connections after its `keep_alive_timeout`; the client
//! reconnects on its own and sends return `None` until it has.

use std::{fmt, net::SocketAddr};

pub use flux_network::http::HttpResponse;
use flux_network::{
    Token,
    http::{HttpEvent, HttpNetwork},
};

/// Correlates a sent request with its outcome in [`ClickHouse::poll_with`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueryId(u64);

#[derive(Debug, PartialEq, Eq)]
pub enum Error<'a> {
    /// A non-200 status. `code` is the `ClickHouse` exception code when the
    /// server reported one; `message` is the response body.
    Server { status: u16, code: Option<u32>, message: &'a [u8] },
    /// The connection dropped before a response arrived. The server may or
    /// may not have executed the query.
    Disconnected,
}

impl<'a> Error<'a> {
    fn check(response: HttpResponse<'a>) -> Result<HttpResponse<'a>, Self> {
        if response.status == 200 {
            return Ok(response)
        }
        let code = response
            .header("X-ClickHouse-Exception-Code")
            .and_then(|value| std::str::from_utf8(value).ok()?.trim().parse().ok());
        Err(Self::Server { status: response.status, code, message: response.body })
    }
}

impl fmt::Display for Error<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Server { status, code, message } => {
                write!(f, "http {status}")?;
                if let Some(code) = code {
                    write!(f, " exception {code}")?;
                }
                write!(f, ": {}", String::from_utf8_lossy(message).trim_end())
            }
            Self::Disconnected => f.write_str("disconnected before the response arrived"),
        }
    }
}

struct Conn {
    token: Token,
    connected: bool,
    in_flight: Option<QueryId>,
}

impl Conn {
    fn find(conns: &mut [Self], token: Token) -> Option<&mut Self> {
        conns.iter_mut().find(|conn| conn.token == token)
    }
}

pub struct ClickHouse {
    http: HttpNetwork,
    addr: SocketAddr,
    user: String,
    key: String,
    settings: Vec<(String, String)>,
    connections: usize,
    conns: Vec<Conn>,
    next_id: u64,
}

impl ClickHouse {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            http: HttpNetwork::default().with_name("clickhouse"),
            addr,
            user: "default".to_owned(),
            key: String::new(),
            settings: vec![("wait_end_of_query".to_owned(), "1".to_owned())],
            connections: 1,
            conns: Vec::new(),
            next_id: 0,
        }
    }
    /// Sets the user and password sent with every request.
    pub fn with_credentials(mut self, user: &str, key: &str) -> Self {
        user.clone_into(&mut self.user);
        key.clone_into(&mut self.key);
        self
    }
    /// Sets the default database.
    pub fn with_database(self, database: &str) -> Self {
        self.with_setting("database", database)
    }
    /// Sets a query setting sent as a URL parameter, replacing an earlier
    /// value.
    pub fn with_setting(mut self, name: &str, value: &str) -> Self {
        match self.settings.iter_mut().find(|(n, _)| n == name) {
            Some((_, v)) => value.clone_into(v),
            None => self.settings.push((name.to_owned(), value.to_owned())),
        }
        self
    }
    /// Sets how many connections share the load; each carries one request at
    /// a time.
    pub fn with_connections(mut self, connections: usize) -> Self {
        assert!(connections > 0, "at least one connection");
        assert!(self.conns.is_empty(), "configure before polling or sending");
        self.connections = connections;
        self
    }
    /// Bounds response bodies and, through the send backlog, insert bodies:
    /// size it to the largest batch you insert. Defaults to 1 MiB.
    pub fn with_max_body_bytes(mut self, max_body_bytes: usize) -> Self {
        self.http = std::mem::take(&mut self.http).with_max_body_bytes(max_body_bytes);
        self
    }
    fn connect(&mut self) {
        if self.conns.is_empty() {
            for _ in 0..self.connections {
                let token = self.http.connect(self.addr);
                self.conns.push(Conn { token, connected: false, in_flight: None });
            }
        }
    }
    /// Sends `sql` on an idle connection. Returns `None` when none is idle and
    /// connected; nothing is queued, so the caller retries after a poll.
    pub fn query(&mut self, sql: &str) -> Option<QueryId> {
        let path = self.path(None);
        self.send(&path, sql.as_bytes())
    }
    /// Sends an `INSERT ... FORMAT <fmt>` statement with `data` as its body.
    /// Returns `None` like [`Self::query`].
    pub fn insert(&mut self, sql: &str, data: &[u8]) -> Option<QueryId> {
        let path = self.path(Some(sql));
        self.send(&path, data)
    }
    fn send(&mut self, path: &str, body: &[u8]) -> Option<QueryId> {
        self.connect();
        let conn = self.conns.iter_mut().find(|conn| conn.connected && conn.in_flight.is_none())?;
        let headers =
            [("X-ClickHouse-User", self.user.as_str()), ("X-ClickHouse-Key", self.key.as_str())];
        if !self.http.request(conn.token, "POST", path, &headers, body) {
            return None
        }
        let id = QueryId(self.next_id);
        self.next_id += 1;
        conn.in_flight = Some(id);
        Some(id)
    }
    fn path(&self, query: Option<&str>) -> String {
        let mut path = String::from("/?");
        if let Some(query) = query {
            path.push_str("query=");
            percent_encode(&mut path, query);
        }
        for (name, value) in &self.settings {
            if path.len() > 2 {
                path.push('&');
            }
            percent_encode(&mut path, name);
            path.push('=');
            percent_encode(&mut path, value);
        }
        path
    }
    /// Polls the connections and delivers the outcome of each completed
    /// request exactly once.
    pub fn poll_with<F>(&mut self, mut handler: F)
    where
        F: for<'a> FnMut(QueryId, Result<HttpResponse<'a>, Error<'a>>),
    {
        self.connect();
        let conns = &mut self.conns;
        self.http.poll_with(|event| match event {
            HttpEvent::Connected { token } => {
                if let Some(conn) = Conn::find(conns, token) {
                    conn.connected = true;
                }
            }
            HttpEvent::Disconnected { token } => {
                if let Some(conn) = Conn::find(conns, token) {
                    conn.connected = false;
                    if let Some(id) = conn.in_flight.take() {
                        handler(id, Err(Error::Disconnected));
                    }
                }
            }
            HttpEvent::Response { token, response } => {
                if let Some(id) = Conn::find(conns, token).and_then(|conn| conn.in_flight.take()) {
                    handler(id, Error::check(response));
                }
            }
            HttpEvent::Accepted { .. } | HttpEvent::Request { .. } => {}
        });
    }
}

fn percent_encode(out: &mut String, value: &str) {
    use fmt::Write as _;
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
            out.push(byte as char);
        } else {
            write!(out, "%{byte:02X}").unwrap();
        }
    }
}
