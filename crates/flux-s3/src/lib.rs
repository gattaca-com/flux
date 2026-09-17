//! `S3` over an [`HttpNetwork`] pool.
//!
//! [`S3::put_object`], [`S3::get_object`], [`S3::delete_object`], and
//! [`S3::list_objects`] queue path-style requests on the network and return a
//! [`RequestId`]; [`S3::outcome`] picks this client's results out of the
//! network's events. Every request is SigV4-signed; all operations are
//! idempotent, so one cut off by a lost connection is resent.
//!
//! There is no TLS, so this talks to plain-HTTP endpoints (`MinIO`,
//! `LocalStack`, or an HTTP gateway), not AWS directly.

pub mod sigv4;

use std::net::SocketAddr;

pub use flux_network::http::RequestId;
use flux_network::{
    http::{Failure, HttpEvent, HttpNetwork, HttpPool, HttpResponse},
    tcp::TcpNetworkCore,
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
    pool: HttpPool,
    signer: sigv4::Signer,
}

impl S3 {
    /// Opens `connections` to `addr` on `http`.
    pub fn new(
        http: &mut HttpNetwork,
        net: &mut TcpNetworkCore,
        addr: SocketAddr,
        connections: usize,
    ) -> Self {
        Self {
            pool: http.pool(net, addr, connections),
            signer: sigv4::Signer::new(&addr.to_string(), "", "", "us-east-1"),
        }
    }
    pub fn with_credentials(mut self, access: &str, secret: &str) -> Self {
        self.signer.set_credentials(access, secret);
        self
    }
    pub fn with_region(mut self, region: &str) -> Self {
        self.signer.set_region(region);
        self
    }
    /// Queues a PUT of `body` to `bucket/key`; returns it back when the
    /// network refuses it (full queue, or over `max_body_bytes`).
    pub fn put_object(
        &self,
        http: &mut HttpNetwork,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
    ) -> Result<RequestId, Vec<u8>> {
        self.send(http, "PUT", &object_resource(bucket, key), "", body)
    }
    /// Queues a GET of `bucket/key`.
    pub fn get_object(
        &self,
        http: &mut HttpNetwork,
        bucket: &str,
        key: &str,
    ) -> Result<RequestId, Vec<u8>> {
        self.send(http, "GET", &object_resource(bucket, key), "", Vec::new())
    }
    /// Queues a DELETE of `bucket/key`.
    pub fn delete_object(
        &self,
        http: &mut HttpNetwork,
        bucket: &str,
        key: &str,
    ) -> Result<RequestId, Vec<u8>> {
        self.send(http, "DELETE", &object_resource(bucket, key), "", Vec::new())
    }
    /// Queues a `ListObjectsV2` of `bucket`, returning the raw XML.
    pub fn list_objects(
        &self,
        http: &mut HttpNetwork,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<RequestId, Vec<u8>> {
        let mut resource = String::from("/");
        resource.push_str(bucket);
        self.send(http, "GET", &resource, &list_query(prefix), Vec::new())
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
    fn send(
        &self,
        http: &mut HttpNetwork,
        method: &str,
        resource: &str,
        query: &str,
        body: Vec<u8>,
    ) -> Result<RequestId, Vec<u8>> {
        let date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let authorization = self.signer.sign(method, resource, query, &date, &body);
        let mut path = resource.to_owned();
        if !query.is_empty() {
            path.push('?');
            path.push_str(query);
        }
        let headers = [("X-Amz-Date", date.as_str()), ("Authorization", authorization.as_str())];
        http.send(self.pool, method, &path, &headers, body, RETRIES)
    }
}

/// `/bucket/key` with each key segment encoded and `/` kept.
fn object_resource(bucket: &str, key: &str) -> String {
    let mut resource = String::from("/");
    resource.push_str(bucket);
    for segment in key.split('/') {
        resource.push('/');
        sigv4::encode_segment(&mut resource, segment);
    }
    resource
}

/// `ListObjectsV2` parameters, already sorted by name.
fn list_query(prefix: Option<&str>) -> String {
    let mut query = String::from("list-type=2");
    if let Some(prefix) = prefix {
        query.push_str("&prefix=");
        sigv4::encode_query(&mut query, prefix);
    }
    query
}
