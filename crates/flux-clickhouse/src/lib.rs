//! `ClickHouse` over an [`HttpNetwork`] pool.
//!
//! [`ClickHouse::insert_rows`], [`ClickHouse::insert`], and
//! [`ClickHouse::query`] queue requests on the network and return a
//! [`RequestId`]; [`ClickHouse::outcome`] picks this client's results out of
//! the network's events. Inserts cut off by a lost connection are resent.

pub mod rowbinary;

use std::{fmt::Write as _, net::SocketAddr};

pub use flux_network::http::RequestId;
use flux_network::http::{Failure, HttpEvent, HttpNetwork, HttpPool, HttpResponse};
use serde::Serialize;

#[derive(Debug, PartialEq, Eq)]
pub enum Error<'a> {
    /// Non-200 status with the `X-ClickHouse-Exception-Code` and body.
    Server {
        status: u16,
        code: Option<u32>,
        message: &'a [u8],
    },
    /// Lost before a response; the server may or may not have run the query.
    Disconnected,
    TimedOut,
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
    pool: HttpPool,
    user: String,
    key: String,
    settings: Vec<(String, String)>,
}

impl ClickHouse {
    /// Opens `connections` to `addr` on `http`.
    pub fn new(http: &mut HttpNetwork, addr: SocketAddr, connections: usize) -> Self {
        Self {
            pool: http.pool(addr, connections),
            user: "default".to_owned(),
            key: String::new(),
            settings: vec![("wait_end_of_query".to_owned(), "1".to_owned())],
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
    /// Queues `sql` as the request body; returns it back when the network
    /// refuses it (full queue, or over `max_body_bytes`).
    pub fn query(&self, http: &mut HttpNetwork, sql: &str) -> Result<RequestId, Vec<u8>> {
        http.send(self.pool, "POST", &self.path(None), &self.headers(), sql.as_bytes().to_vec(), 0)
    }
    /// Queues an `INSERT ... FORMAT <fmt>` statement with `body` as its data.
    /// A lost connection resends it, so the server may see it more than once.
    pub fn insert(
        &self,
        http: &mut HttpNetwork,
        sql: &str,
        body: Vec<u8>,
    ) -> Result<RequestId, Vec<u8>> {
        http.send(self.pool, "POST", &self.path(Some(sql)), &self.headers(), body, 3)
    }
    /// Encodes `rows` as `RowBinary` and queues them for `table`, naming the
    /// columns after the row's fields. Panics on an empty batch or a row type
    /// without a `RowBinary` encoding.
    pub fn insert_rows<T: Serialize>(
        &self,
        http: &mut HttpNetwork,
        table: &str,
        rows: &[T],
    ) -> Result<RequestId, Vec<u8>> {
        let sql = rowbinary::insert_statement(table, &rows[0]).expect("RowBinary row");
        let mut body = Vec::new();
        for row in rows {
            rowbinary::encode(&mut body, row).expect("RowBinary row");
        }
        self.insert(http, &sql, body)
    }
    /// This client's result in a network event, if the event was one of its
    /// requests completing.
    pub fn outcome<'a>(
        &self,
        event: &HttpEvent<'a>,
    ) -> Option<(RequestId, Result<&'a [u8], Error<'a>>)> {
        match *event {
            HttpEvent::Response { id: Some(id), ref response, .. } if id.pool() == self.pool => {
                Some((id, Error::check(response)))
            }
            HttpEvent::Failed { id, reason } if id.pool() == self.pool => Some((
                id,
                Err(match reason {
                    Failure::Disconnected => Error::Disconnected,
                    Failure::TimedOut => Error::TimedOut,
                }),
            )),
            _ => None,
        }
    }
    fn headers(&self) -> [(&str, &str); 2] {
        [("X-ClickHouse-User", &self.user), ("X-ClickHouse-Key", &self.key)]
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
