pub mod rowbinary;

use std::{fmt::Write as _, net::SocketAddr};

pub use flux_network::http::RequestId;
use flux_network::{
    http::{Failure, HttpNetwork, HttpPool, HttpResponse},
    tcp::{TcpEvent, TcpNetworkCore},
};
use serde::Serialize;

#[derive(Debug, PartialEq, Eq)]
pub enum Error<'a> {
    Server { status: u16, code: Option<u32>, message: &'a [u8] },
    Disconnected,
    TimedOut,
}

impl From<Failure> for Error<'_> {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Disconnected => Self::Disconnected,
            Failure::TimedOut => Self::TimedOut,
        }
    }
}

impl<'a> Error<'a> {
    fn check(response: &HttpResponse<'a>) -> Result<&'a [u8], Self> {
        if response.status == 200 {
            return Ok(response.body)
        }
        let code = response
            .header("X-ClickHouse-Exception-Code")
            .and_then(|value| std::str::from_utf8(value).ok()?.trim().parse().ok());
        Err(Self::Server { status: response.status, code, message: response.body })
    }
}

pub struct ClickHouse {
    addr: SocketAddr,
    connections: usize,
    user: String,
    key: String,
    settings: Vec<(String, String)>,
    http: HttpNetwork,
    pool: Option<HttpPool>,
}

impl ClickHouse {
    pub fn new(addr: SocketAddr, connections: usize) -> Self {
        Self {
            addr,
            connections,
            user: "default".to_owned(),
            key: String::new(),
            settings: vec![("wait_end_of_query".to_owned(), "1".to_owned())],
            http: HttpNetwork::default().with_name("clickhouse"),
            pool: None,
        }
    }
    pub fn with_credentials(mut self, user: &str, key: &str) -> Self {
        assert!(self.pool.is_none(), "configure before connect");
        user.clone_into(&mut self.user);
        key.clone_into(&mut self.key);
        self
    }
    pub fn with_database(self, database: &str) -> Self {
        self.with_setting("database", database)
    }
    pub fn with_setting(mut self, name: &str, value: &str) -> Self {
        assert!(self.pool.is_none(), "configure before connect");
        match self.settings.iter_mut().find(|(n, _)| n == name) {
            Some((_, v)) => value.clone_into(v),
            None => self.settings.push((name.to_owned(), value.to_owned())),
        }
        self
    }
    /// Configures the HTTP layer this client owns, through its own builders.
    pub fn with_http(mut self, configure: impl FnOnce(HttpNetwork) -> HttpNetwork) -> Self {
        assert!(self.pool.is_none(), "configure before connect");
        self.http = configure(self.http);
        self
    }
    /// Opens the pool; the builders must have run.
    pub fn connect(&mut self, net: &mut TcpNetworkCore) {
        assert!(self.pool.is_none(), "connect once");
        self.pool = Some(self.http.pool(net, self.addr, self.connections));
    }
    /// Returns whether the event belonged to this client.
    pub fn on_event(&mut self, event: &TcpEvent<'_>) -> bool {
        self.http.on_event(event)
    }
    /// Sends queued requests and delivers each finished request's outcome to
    /// `handler` exactly once.
    pub fn drive<F>(&mut self, net: &mut TcpNetworkCore, mut handler: F)
    where
        F: for<'a> FnMut(RequestId, Result<&'a [u8], Error<'a>>),
    {
        let pool = self.pool.expect("connect before drive");
        self.http.drive(net, |event| {
            if let Some((id, result)) = event.outcome(pool) {
                handler(id, result.map_err(Error::from).and_then(Error::check));
            }
        });
    }
    /// Removes every pooled endpoint; queued and in-flight requests are
    /// dropped without outcomes.
    pub fn close(self, net: &mut TcpNetworkCore) {
        self.http.close(net);
    }
    pub fn query(&mut self, sql: &str) -> Result<RequestId, Vec<u8>> {
        self.queue(&self.path(None), sql.as_bytes().to_vec(), 0)
    }
    pub fn insert(&mut self, sql: &str, body: Vec<u8>) -> Result<RequestId, Vec<u8>> {
        self.queue(&self.path(Some(sql)), body, 3)
    }
    pub fn insert_rows<T: Serialize>(
        &mut self,
        table: &str,
        rows: &[T],
    ) -> Result<RequestId, Vec<u8>> {
        let sql = rowbinary::insert_statement(table, &rows[0]).expect("RowBinary row");
        let mut body = Vec::new();
        for row in rows {
            rowbinary::encode(&mut body, row).expect("RowBinary row");
        }
        self.insert(&sql, body)
    }
    fn queue(&mut self, path: &str, body: Vec<u8>, retries: u8) -> Result<RequestId, Vec<u8>> {
        let pool = self.pool.expect("connect before queueing requests");
        let Self { http, user, key, .. } = self;
        let headers = [("X-ClickHouse-User", user.as_str()), ("X-ClickHouse-Key", key.as_str())];
        http.send(pool, "POST", path, &headers, body, retries)
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
