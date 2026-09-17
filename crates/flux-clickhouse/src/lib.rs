//! Poll-driven `ClickHouse` HTTP client over a caller-owned [`HttpNetwork`].
//!
//! `query` and `insert` take an idle pooled connection and return a
//! [`QueryId`], or `None` when none is idle: nothing is queued, retry after
//! the next poll. Call [`ClickHouse::on_event`] from the network's `poll_with`
//! handler; it delivers each outcome once and returns `false` for events that
//! are not its own. Bodies are bounded by the network's `max_body_bytes`.

pub mod rowbinary;

use std::{fmt::Write as _, net::SocketAddr};

pub use flux_network::http::HttpResponse;
use flux_network::{
    Token,
    http::{HttpEvent, HttpNetwork},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueryId(u64);

#[derive(Debug, PartialEq, Eq)]
pub enum Error<'a> {
    /// Non-200 status with the `X-ClickHouse-Exception-Code` and body.
    Server { status: u16, code: Option<u32>, message: &'a [u8] },
    /// Lost before a response; the server may or may not have run the query.
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

struct Conn {
    token: Token,
    connected: bool,
    in_flight: Option<QueryId>,
}

pub struct ClickHouse {
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
            addr,
            user: "default".to_owned(),
            key: String::new(),
            settings: vec![("wait_end_of_query".to_owned(), "1".to_owned())],
            connections: 1,
            conns: Vec::new(),
            next_id: 0,
        }
    }
    pub fn with_credentials(mut self, user: &str, key: &str) -> Self {
        user.clone_into(&mut self.user);
        key.clone_into(&mut self.key);
        self
    }
    pub fn with_database(self, database: &str) -> Self {
        self.with_setting("database", database)
    }
    /// Sets a query setting sent as a URL parameter.
    pub fn with_setting(mut self, name: &str, value: &str) -> Self {
        match self.settings.iter_mut().find(|(n, _)| n == name) {
            Some((_, v)) => value.clone_into(v),
            None => self.settings.push((name.to_owned(), value.to_owned())),
        }
        self
    }
    /// Pool size; each connection carries one request at a time.
    pub fn with_connections(mut self, connections: usize) -> Self {
        assert!(connections > 0 && self.conns.is_empty(), "nonzero, before the first send");
        self.connections = connections;
        self
    }
    /// Sends `sql` as the request body.
    pub fn query(&mut self, http: &mut HttpNetwork, sql: &str) -> Option<QueryId> {
        let path = self.path(None);
        self.send(http, &path, sql.as_bytes())
    }
    /// Sends an `INSERT ... FORMAT <fmt>` statement with `data` as the body.
    /// Panics if `data` exceeds the network's `max_body_bytes`.
    pub fn insert(&mut self, http: &mut HttpNetwork, sql: &str, data: &[u8]) -> Option<QueryId> {
        let path = self.path(Some(sql));
        self.send(http, &path, data)
    }
    fn send(&mut self, http: &mut HttpNetwork, path: &str, body: &[u8]) -> Option<QueryId> {
        assert!(body.len() <= http.max_body_bytes(), "body exceeds the network's max_body_bytes");
        if self.conns.is_empty() {
            for _ in 0..self.connections {
                let token = http.connect(self.addr);
                self.conns.push(Conn { token, connected: false, in_flight: None });
            }
        }
        let conn = self.conns.iter_mut().find(|conn| conn.connected && conn.in_flight.is_none())?;
        let headers =
            [("X-ClickHouse-User", self.user.as_str()), ("X-ClickHouse-Key", self.key.as_str())];
        if !http.request(conn.token, "POST", path, &headers, body) {
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
    fn conn(&mut self, token: Token) -> Option<&mut Conn> {
        self.conns.iter_mut().find(|conn| conn.token == token)
    }
    /// Returns whether the event belonged to this client; a completed
    /// request's outcome is delivered to `handler` exactly once.
    pub fn on_event<'a, F>(&mut self, event: &HttpEvent<'a>, handler: F) -> bool
    where
        F: FnOnce(QueryId, Result<HttpResponse<'a>, Error<'a>>),
    {
        match *event {
            HttpEvent::Connected { token } => {
                let Some(conn) = self.conn(token) else { return false };
                conn.connected = true;
            }
            HttpEvent::Disconnected { token } => {
                let Some(conn) = self.conn(token) else { return false };
                conn.connected = false;
                if let Some(id) = conn.in_flight.take() {
                    handler(id, Err(Error::Disconnected));
                }
            }
            HttpEvent::Response { token, response } => {
                let Some(conn) = self.conn(token) else { return false };
                if let Some(id) = conn.in_flight.take() {
                    handler(id, Error::check(response));
                }
            }
            HttpEvent::Accepted { .. } | HttpEvent::Request { .. } => return false,
        }
        true
    }
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
