//! `S3` over an [`HttpNetwork`] pool.
//!
//! [`S3::put_object`], [`S3::get_object`], [`S3::head_object`],
//! [`S3::delete_object`], [`S3::list_objects`] and the multipart calls queue
//! path-style requests and return a [`RequestId`]; [`S3::connect`] opens the
//! pool, [`S3::on_event`] claims this client's network events, and
//! [`S3::drive`] sends what is queued and delivers each outcome. Every
//! request is SigV4-signed, and one cut off by a lost connection is resent.
//! A resend is harmless except in multipart uploads: a resent
//! [`S3::create_multipart_upload`] can leave an empty upload behind, which a
//! bucket lifecycle rule (`AbortIncompleteMultipartUpload`) clears, and a
//! resent [`S3::complete_multipart_upload`] or
//! [`S3::abort_multipart_upload`] that already took effect can fail with
//! `NoSuchUpload`.
//!
//! [`S3::new_tls`] speaks HTTPS to real endpoints; [`S3::new`] talks to
//! plain-HTTP ones (`MinIO`, `LocalStack`, or an HTTP gateway).

pub mod sigv4;

use std::net::SocketAddr;

pub use flux_network::http::{HttpResponse, RequestId};
use flux_network::{
    NetworkCore, NetworkEvent,
    http::{Failure, HttpNetwork, HttpPool},
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
    fn check<'r>(response: &'r HttpResponse<'a>) -> Result<&'r HttpResponse<'a>, Self> {
        if (200..=299).contains(&response.status) {
            return Ok(response);
        }
        Err(Self::Server {
            status: response.status,
            code: error_code(response.body),
            message: response.body,
        })
    }

    /// A complete that fails after S3 has sent `200 OK` reports the error
    /// in the body.
    fn check_complete<'r>(response: &'r HttpResponse<'a>) -> Result<&'r HttpResponse<'a>, Self> {
        if element(response.body, "Error").is_none() {
            return Ok(response);
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
    element(body, "Code")
}

/// The `<UploadId>` of a [`S3::create_multipart_upload`] response.
pub fn upload_id(body: &[u8]) -> Option<&str> {
    element(body, "UploadId")
}

/// The text of the first `<tag>` element in `body`.
fn element<'b>(body: &'b [u8], tag: &str) -> Option<&'b str> {
    let tag = tag.as_bytes();
    let opens = |at: &[u8]| {
        at.len() > tag.len() + 1 &&
            at[0] == b'<' &&
            at[1..].starts_with(tag) &&
            at[tag.len() + 1] == b'>'
    };
    let start = (0..body.len()).find(|&i| opens(&body[i..]))? + tag.len() + 2;
    let rest = &body[start..];
    let end = (0..rest.len()).find(|&i| {
        rest[i..].starts_with(b"</") &&
            rest[i + 2..].starts_with(tag) &&
            rest.get(i + 2 + tag.len()) == Some(&b'>')
    })?;
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
    /// Completes awaiting their outcome, whose `200 OK` may still be a
    /// failure.
    completing: Vec<RequestId>,
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
            completing: Vec::new(),
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
    /// Replaces the credentials at any time, as temporary ones must be
    /// before they expire; `session_token` comes with temporary credentials.
    /// Requests already queued keep the signature they were queued with,
    /// resends included.
    pub fn set_credentials(&mut self, access: &str, secret: &str, session_token: Option<&str>) {
        self.signer.set_credentials(access, secret);
        self.signer.set_session_token(session_token);
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
    pub fn connect(&mut self, net: &mut NetworkCore) {
        assert!(self.pool.is_none(), "connect once");
        let Self { http, server, addr, connections, .. } = self;
        self.pool = Some(match server {
            Some(host) => http.pool_tls(net, *addr, host, *connections),
            None => http.pool(net, *addr, *connections),
        });
    }
    /// Returns whether the event belonged to this client.
    pub fn on_event(&mut self, event: &NetworkEvent<'_>) -> bool {
        self.http.on_event(event)
    }
    /// Sends queued requests and delivers each finished request's outcome to
    /// `handler` exactly once.
    pub fn drive<F>(&mut self, net: &mut NetworkCore, mut handler: F)
    where
        F: for<'r, 'a> FnMut(RequestId, Result<&'r HttpResponse<'a>, Error<'a>>),
    {
        let pool = self.pool.expect("connect before drive");
        self.http.drive(net, |event| {
            if let Some((id, result)) = event.outcome(pool) {
                let mut result = result.map_err(Error::from).and_then(Error::check);
                if let Some(at) = self.completing.iter().position(|&c| c == id) {
                    self.completing.swap_remove(at);
                    result = result.and_then(Error::check_complete);
                }
                handler(id, result);
            }
        });
    }
    /// Removes every pooled endpoint; queued and in-flight requests are
    /// dropped without outcomes.
    pub fn close(self, net: &mut NetworkCore) {
        self.http.close(net);
    }
    /// Sends a PUT of `body` to `bucket/key`; `None` when the network
    /// refuses it (full queue, or over `max_body_bytes`). S3 answers
    /// `503 SlowDown` under load; back off and resend on it.
    pub fn put_object(
        &mut self,
        net: &mut NetworkCore,
        bucket: &str,
        key: &str,
        body: &[u8],
    ) -> Option<RequestId> {
        self.queue(net, "PUT", &object_resource(bucket, key), "", body)
    }
    /// Sends a GET of `bucket/key`.
    pub fn get_object(
        &mut self,
        net: &mut NetworkCore,
        bucket: &str,
        key: &str,
    ) -> Option<RequestId> {
        self.queue(net, "GET", &object_resource(bucket, key), "", &[])
    }
    /// Sends a HEAD of `bucket/key`; the object's `ETag` and
    /// `Content-Length` are response headers.
    pub fn head_object(
        &mut self,
        net: &mut NetworkCore,
        bucket: &str,
        key: &str,
    ) -> Option<RequestId> {
        self.queue(net, "HEAD", &object_resource(bucket, key), "", &[])
    }
    /// Sends a DELETE of `bucket/key`.
    pub fn delete_object(
        &mut self,
        net: &mut NetworkCore,
        bucket: &str,
        key: &str,
    ) -> Option<RequestId> {
        self.queue(net, "DELETE", &object_resource(bucket, key), "", &[])
    }
    /// Sends a `ListObjectsV2` of `bucket`, returning the raw XML. A
    /// truncated listing carries a `NextContinuationToken`; pass it back as
    /// `continuation_token` for the next page.
    pub fn list_objects(
        &mut self,
        net: &mut NetworkCore,
        bucket: &str,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
    ) -> Option<RequestId> {
        let mut resource = String::from("/");
        resource.push_str(bucket);
        let query = list_query(prefix, continuation_token);
        self.queue(net, "GET", &resource, &query, &[])
    }
    /// Starts a multipart upload to `bucket/key`; [`upload_id`] reads the
    /// upload's id from the response body. Nothing is visible at `key` until
    /// the upload completes.
    pub fn create_multipart_upload(
        &mut self,
        net: &mut NetworkCore,
        bucket: &str,
        key: &str,
    ) -> Option<RequestId> {
        self.queue(net, "POST", &object_resource(bucket, key), "uploads=", &[])
    }
    /// Uploads part `part_number` (1 to 10000) of an upload; its `ETag`
    /// response header goes to [`Self::complete_multipart_upload`]. Every
    /// part but the last must be at least 5 MiB, above the HTTP layer's
    /// default body limit; raise it through [`Self::with_http`]. Uploading a
    /// part number again replaces that part.
    pub fn upload_part(
        &mut self,
        net: &mut NetworkCore,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u16,
        body: &[u8],
    ) -> Option<RequestId> {
        use std::fmt::Write as _;
        let mut query = String::new();
        write!(query, "partNumber={part_number}&uploadId=").unwrap();
        sigv4::encode(&mut query, upload_id);
        self.queue(net, "PUT", &object_resource(bucket, key), &query, body)
    }
    /// Joins the uploaded parts, given as `(part_number, etag)` in ascending
    /// part order, into the object at `key`. S3 can take minutes to answer,
    /// so a request timeout set through [`Self::with_http`] must allow for
    /// it. `NoSuchUpload` means the upload was aborted or has already
    /// completed. After a lost connection, [`Self::head_object`] tells which
    /// only for a key that held no object before, so write to unique keys.
    pub fn complete_multipart_upload<'e>(
        &mut self,
        net: &mut NetworkCore,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: impl IntoIterator<Item = (u16, &'e str)>,
    ) -> Option<RequestId> {
        use std::fmt::Write as _;
        let mut body = String::from("<CompleteMultipartUpload>");
        for (part_number, etag) in parts {
            write!(body, "<Part><PartNumber>{part_number}</PartNumber><ETag>{etag}</ETag></Part>")
                .unwrap();
        }
        body.push_str("</CompleteMultipartUpload>");
        let id = self.queue(
            net,
            "POST",
            &object_resource(bucket, key),
            &upload_query(upload_id),
            body.as_bytes(),
        )?;
        self.completing.push(id);
        Some(id)
    }
    /// Abandons an upload and frees its parts. `NoSuchUpload` means it is
    /// already gone.
    pub fn abort_multipart_upload(
        &mut self,
        net: &mut NetworkCore,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Option<RequestId> {
        self.queue(net, "DELETE", &object_resource(bucket, key), &upload_query(upload_id), &[])
    }
    fn queue(
        &mut self,
        net: &mut NetworkCore,
        method: &str,
        resource: &str,
        query: &str,
        body: &[u8],
    ) -> Option<RequestId> {
        let date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let (authorization, payload) = self.signer.sign(method, resource, query, &date, &[], body);
        let mut path = resource.to_owned();
        if !query.is_empty() {
            path.push('?');
            path.push_str(query);
        }
        let pool = self.pool.expect("connect before queueing requests");
        let Self { http, signer, .. } = self;
        let mut headers = [
            ("X-Amz-Date", date.as_str()),
            ("Authorization", authorization.as_str()),
            ("X-Amz-Content-Sha256", payload.as_str()),
            ("Host", signer.host()),
            ("X-Amz-Security-Token", ""),
        ];
        let len = match signer.session_token() {
            Some(token) => {
                headers[4].1 = token;
                headers.len()
            }
            None => headers.len() - 1,
        };
        http.send(net, pool, method, &path, &headers[..len], body, RETRIES)
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

/// The query naming one multipart upload.
fn upload_query(upload_id: &str) -> String {
    let mut query = String::from("uploadId=");
    sigv4::encode(&mut query, upload_id);
    query
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
