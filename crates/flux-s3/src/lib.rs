//! `S3` over an [`HttpNetwork`] pool.
//!
//! [`S3::put_object`], [`S3::get_object`], [`S3::delete_object`], and
//! [`S3::list_objects`] queue path-style requests and return a [`RequestId`];
//! [`S3::connect`] opens the pool, [`S3::on_event`] claims this client's
//! network events, and [`S3::drive`] sends what is queued and delivers each
//! outcome. Every request is SigV4-signed; all operations are idempotent, so
//! one cut off by a lost connection is resent.
//!
//! [`S3::new_tls`] speaks HTTPS to real endpoints; [`S3::new`] talks to
//! plain-HTTP ones (`MinIO`, `LocalStack`, or an HTTP gateway).

pub mod sigv4;

use std::net::SocketAddr;

pub use flux_network::http::RequestId;
use flux_network::{
    http::{Failure, HttpNetwork, HttpPool, HttpResponse},
    tcp::{TcpEvent, TcpNetworkCore},
};

const RETRIES: u8 = 3;

#[derive(Debug, PartialEq, Eq)]
pub enum Error<'a> {
    /// Non-2xx status with the S3 `<Code>` and body.
    Server {
        status: u16,
        code: Option<&'a str>,
        message: &'a [u8],
    },
    /// Lost before a response; the server may or may not have handled it.
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
        if (200..=299).contains(&response.status) {
            return Ok(response.body);
        }
        Err(Self::Server {
            status: response.status,
            code: error_code(response.body),
            message: response.body,
        })
    }
}

/// The `<Code>` of an S3 error document, if it parses.
fn error_code(body: &[u8]) -> Option<&str> {
    let start = body.windows(6).position(|w| w == b"<Code>")? + 6;
    let rest = &body[start..];
    let end = rest.windows(7).position(|w| w == b"</Code>")?;
    std::str::from_utf8(&rest[..end]).ok()
}

pub struct S3 {
    addr: SocketAddr,
    /// SNI and `Host` for a TLS endpoint; `None` leaves the wire plaintext.
    server: Option<String>,
    connections: usize,
    signer: sigv4::Signer,
    http: HttpNetwork,
    pool: Option<HttpPool>,
}

impl S3 {
    /// Prepares `connections` to a plaintext `addr`.
    pub fn new(addr: SocketAddr, connections: usize) -> Self {
        Self {
            addr,
            server: None,
            connections,
            // Plain HTTP has no transport integrity, so the payload hash is
            // the only thing binding a body to its signature.
            signer: sigv4::Signer::new(&addr.to_string(), "", "", "us-east-1"),
            http: HttpNetwork::default().with_name("s3"),
            pool: None,
        }
    }
    /// Like [`Self::new`] but over TLS to `addr`, sending and signing
    /// SNI/`Host` `host` (path-style: one pool serves every bucket). Panics
    /// if `host` is not a valid DNS name or IP.
    pub fn new_tls(addr: SocketAddr, host: &str, connections: usize) -> Self {
        Self {
            server: Some(host.to_owned()),
            // TLS already protects the body, so it is not hashed twice.
            signer: sigv4::Signer::new(host, "", "", "us-east-1").unsigned_payloads(),
            ..Self::new(addr, connections)
        }
    }
    pub fn with_credentials(mut self, access: &str, secret: &str) -> Self {
        assert!(self.pool.is_none(), "configure before connect");
        self.signer.set_credentials(access, secret);
        self
    }
    pub fn with_region(mut self, region: &str) -> Self {
        assert!(self.pool.is_none(), "configure before connect");
        self.signer.set_region(region);
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
        let Self { http, server, addr, connections, .. } = self;
        self.pool = Some(match server {
            Some(host) => http.pool_tls(net, *addr, host, *connections),
            None => http.pool(net, *addr, *connections),
        });
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
    /// Queues a PUT of `body` to `bucket/key`; returns it back when the
    /// network refuses it (full queue, or over `max_body_bytes`). S3 answers
    /// `503 SlowDown` under load; back off and resend on it.
    pub fn put_object(
        &mut self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
    ) -> Result<RequestId, Vec<u8>> {
        self.queue("PUT", &object_resource(bucket, key), "", body)
    }
    /// Queues a GET of `bucket/key`.
    pub fn get_object(&mut self, bucket: &str, key: &str) -> Result<RequestId, Vec<u8>> {
        self.queue("GET", &object_resource(bucket, key), "", Vec::new())
    }
    /// Queues a DELETE of `bucket/key`.
    pub fn delete_object(&mut self, bucket: &str, key: &str) -> Result<RequestId, Vec<u8>> {
        self.queue("DELETE", &object_resource(bucket, key), "", Vec::new())
    }
    /// Queues a `ListObjectsV2` of `bucket`, returning the raw XML. A
    /// truncated listing carries a `NextContinuationToken`; pass it back as
    /// `continuation_token` for the next page.
    pub fn list_objects(
        &mut self,
        bucket: &str,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
    ) -> Result<RequestId, Vec<u8>> {
        let mut resource = String::from("/");
        resource.push_str(bucket);
        let query = list_query(prefix, continuation_token);
        self.queue("GET", &resource, &query, Vec::new())
    }
    fn queue(
        &mut self,
        method: &str,
        resource: &str,
        query: &str,
        body: Vec<u8>,
    ) -> Result<RequestId, Vec<u8>> {
        let date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let (authorization, payload) = self.signer.sign(method, resource, query, &date, &body);
        let mut path = resource.to_owned();
        if !query.is_empty() {
            path.push('?');
            path.push_str(query);
        }
        let pool = self.pool.expect("connect before queueing requests");
        let Self { http, signer, .. } = self;
        let headers = [
            ("X-Amz-Date", date.as_str()),
            ("Authorization", authorization.as_str()),
            ("X-Amz-Content-Sha256", payload.as_str()),
            ("Host", signer.host()),
        ];
        http.send(pool, method, &path, &headers, body, RETRIES)
    }
}

/// `/bucket/key` with each key segment encoded and `/` kept.
fn object_resource(bucket: &str, key: &str) -> String {
    let mut resource = String::from("/");
    resource.push_str(bucket);
    for segment in key.split('/') {
        resource.push('/');
        sigv4::encode(&mut resource, segment);
    }
    resource
}

/// `ListObjectsV2` parameters, written in the name order `SigV4` signs.
fn list_query(prefix: Option<&str>, continuation_token: Option<&str>) -> String {
    let mut query = String::new();
    if let Some(token) = continuation_token {
        query.push_str("continuation-token=");
        sigv4::encode(&mut query, token);
        query.push('&');
    }
    query.push_str("list-type=2");
    if let Some(prefix) = prefix {
        query.push_str("&prefix=");
        sigv4::encode(&mut query, prefix);
    }
    query
}
