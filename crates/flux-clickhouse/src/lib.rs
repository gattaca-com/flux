//! Poll-driven `ClickHouse` HTTP client over a caller-owned [`HttpNetwork`].
//!
//! Requests queue inside the client: [`ClickHouse::insert_rows`],
//! [`ClickHouse::insert`], and [`ClickHouse::query`] return a [`QueryId`] at
//! once; [`ClickHouse::on_event`], called from the network's `poll_with`
//! handler, tracks the pooled connections; [`ClickHouse::drive`] sends queued
//! requests on idle ones and delivers one outcome per id. Inserts cut off by
//! a lost connection are resent. The queue is bounded by bytes and evicts the
//! oldest requests as [`Error::Dropped`].

pub mod rowbinary;

use std::{collections::VecDeque, fmt::Write as _, net::SocketAddr};

use flux_network::{
    Token,
    http::{HttpEvent, HttpNetwork, HttpResponse},
};
use serde::{Serialize, ser::Error as _};

const RETRIES: u8 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryId(u64);

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// Non-200 status with the `X-ClickHouse-Exception-Code` and body.
    Server { status: u16, code: Option<u32>, message: Vec<u8> },
    /// Lost before a response; the server may or may not have run the query.
    Disconnected,
    /// Evicted from a full queue before being sent.
    Dropped,
}

impl Error {
    fn check(response: &HttpResponse<'_>) -> Result<Vec<u8>, Self> {
        if response.status == 200 {
            return Ok(response.body.to_vec())
        }
        let code = response
            .header("X-ClickHouse-Exception-Code")
            .and_then(|value| std::str::from_utf8(value).ok()?.trim().parse().ok());
        Err(Self::Server { status: response.status, code, message: response.body.to_vec() })
    }
}

struct Request {
    id: QueryId,
    path: String,
    body: Vec<u8>,
    retries_left: u8,
}

struct Conn {
    token: Token,
    connected: bool,
    in_flight: Option<Request>,
}

pub struct ClickHouse {
    addr: SocketAddr,
    user: String,
    key: String,
    settings: Vec<(String, String)>,
    connections: usize,
    max_queued_bytes: usize,
    conns: Vec<Conn>,
    queue: VecDeque<Request>,
    queued_bytes: usize,
    outcomes: Vec<(QueryId, Result<Vec<u8>, Error>)>,
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
            max_queued_bytes: 256 << 20,
            conns: Vec::new(),
            queue: VecDeque::new(),
            queued_bytes: 0,
            outcomes: Vec::new(),
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
        assert!(connections > 0 && self.conns.is_empty(), "nonzero, before the first drive");
        self.connections = connections;
        self
    }
    /// Bound on queued and in-flight body bytes, exceeded by at most one
    /// request; the oldest queued requests are dropped to stay under it.
    pub fn with_max_queued_bytes(mut self, max_queued_bytes: usize) -> Self {
        self.max_queued_bytes = max_queued_bytes;
        self
    }
    /// Queues `sql` as the request body; a lost connection fails it.
    pub fn query(&mut self, sql: &str) -> QueryId {
        let path = self.path(None);
        self.enqueue(path, sql.as_bytes().to_vec(), 0)
    }
    /// Queues an `INSERT ... FORMAT <fmt>` statement with `body` as its data;
    /// a lost connection resends it, so the server may see it more than once.
    pub fn insert(&mut self, sql: &str, body: Vec<u8>) -> QueryId {
        let path = self.path(Some(sql));
        self.enqueue(path, body, RETRIES)
    }
    /// Encodes `rows` as `RowBinary` and queues them for `table`, naming the
    /// columns after the row's fields.
    pub fn insert_rows<T: Serialize>(
        &mut self,
        table: &str,
        rows: &[T],
    ) -> Result<QueryId, rowbinary::Error> {
        let first = rows.first().ok_or_else(|| rowbinary::Error::custom("empty batch"))?;
        let sql = rowbinary::insert_statement(table, first)?;
        let mut body = Vec::new();
        for row in rows {
            rowbinary::encode(&mut body, row)?;
        }
        Ok(self.insert(&sql, body))
    }
    fn enqueue(&mut self, path: String, body: Vec<u8>, retries_left: u8) -> QueryId {
        while self.queued_bytes + body.len() > self.max_queued_bytes {
            let Some(oldest) = self.queue.pop_front() else { break };
            self.finish(&oldest, Err(Error::Dropped));
        }
        let id = QueryId(self.next_id);
        self.next_id += 1;
        self.queued_bytes += body.len();
        self.queue.push_back(Request { id, path, body, retries_left });
        id
    }
    fn finish(&mut self, request: &Request, outcome: Result<Vec<u8>, Error>) {
        self.queued_bytes -= request.body.len();
        self.outcomes.push((request.id, outcome));
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
    /// Returns whether the event belonged to this client.
    pub fn on_event(&mut self, event: &HttpEvent<'_>) -> bool {
        match *event {
            HttpEvent::Connected { token } => {
                let Some(conn) = self.conn(token) else { return false };
                conn.connected = true;
            }
            HttpEvent::Disconnected { token } => {
                let Some(conn) = self.conn(token) else { return false };
                conn.connected = false;
                if let Some(mut request) = conn.in_flight.take() {
                    if request.retries_left > 0 {
                        request.retries_left -= 1;
                        self.queue.push_front(request);
                    } else {
                        self.finish(&request, Err(Error::Disconnected));
                    }
                }
            }
            HttpEvent::Response { token, ref response } => {
                let Some(conn) = self.conn(token) else { return false };
                if let Some(request) = conn.in_flight.take() {
                    self.finish(&request, Error::check(response));
                }
            }
            HttpEvent::Accepted { .. } | HttpEvent::Request { .. } => return false,
        }
        true
    }
    /// Sends queued requests on idle connections, then delivers each finished
    /// request's outcome to `handler` exactly once. Panics if a body exceeds
    /// the network's `max_body_bytes`.
    pub fn drive<F>(&mut self, http: &mut HttpNetwork, mut handler: F)
    where
        F: FnMut(QueryId, Result<Vec<u8>, Error>),
    {
        if self.conns.is_empty() {
            for _ in 0..self.connections {
                let token = http.connect(self.addr);
                self.conns.push(Conn { token, connected: false, in_flight: None });
            }
        }
        let headers =
            [("X-ClickHouse-User", self.user.as_str()), ("X-ClickHouse-Key", self.key.as_str())];
        for conn in self.conns.iter_mut().filter(|conn| conn.connected && conn.in_flight.is_none())
        {
            let Some(request) = self.queue.pop_front() else { break };
            assert!(
                request.body.len() <= http.max_body_bytes(),
                "body exceeds the network's max_body_bytes"
            );
            if http.request(conn.token, "POST", &request.path, &headers, &request.body) {
                conn.in_flight = Some(request);
            } else {
                self.queue.push_front(request);
                break
            }
        }
        for (id, outcome) in self.outcomes.drain(..) {
            handler(id, outcome);
        }
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
