//! `SigV4` signing for path-style S3 requests.
//!
//! The signed headers are `host`, `x-amz-content-sha256`, and `x-amz-date`;
//! S3 rejects requests without the payload-hash header. Paths encode every
//! byte outside the unreserved set and keep `/`; query pairs sort by name
//! and encode the same way but also keep `/`, which is how botocore signs
//! S3 prefixes.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// Signs requests for one endpoint and credential pair.
pub struct Signer {
    host: String,
    access: String,
    secret: String,
    region: String,
}

impl Signer {
    pub fn new(host: &str, access: &str, secret: &str, region: &str) -> Self {
        Self {
            host: host.to_owned(),
            access: access.to_owned(),
            secret: secret.to_owned(),
            region: region.to_owned(),
        }
    }
    pub fn set_credentials(&mut self, access: &str, secret: &str) {
        access.clone_into(&mut self.access);
        secret.clone_into(&mut self.secret);
    }
    pub fn set_region(&mut self, region: &str) {
        region.clone_into(&mut self.region);
    }
    /// The `Host` this signer signs for.
    pub fn host(&self) -> &str {
        &self.host
    }
    /// Signs one request; `date` is `YYYYMMDDTHHMMSSZ`, `resource` the path
    /// without its query, `query` the sorted `name=value` pairs. Returns the
    /// `Authorization` value and the payload hash for `x-amz-content-sha256`.
    pub fn sign(
        &self,
        method: &str,
        resource: &str,
        query: &str,
        date: &str,
        body: &[u8],
    ) -> (String, String) {
        let payload = hex(&Sha256::digest(body)[..]);
        let canonical = format!(
            "{method}\n{resource}\n{query}\nhost:{}\nx-amz-content-sha256:{payload}\n\
             x-amz-date:{date}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload}",
            self.host
        );
        let scope = format!("{}/{}/s3/aws4_request", &date[..8], self.region);
        let to_sign =
            format!("AWS4-HMAC-SHA256\n{date}\n{scope}\n{}", hex(&Sha256::digest(&canonical)[..]));
        let mut prefixed = Vec::with_capacity(4 + self.secret.len());
        prefixed.extend_from_slice(b"AWS4");
        prefixed.extend_from_slice(self.secret.as_bytes());
        let mut key = hmac(&prefixed, &date.as_bytes()[..8]);
        for part in [self.region.as_bytes(), b"s3".as_slice(), b"aws4_request".as_slice()] {
            key = hmac(&key, part);
        }
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={}",
            self.access,
            hex(&hmac(&key, to_sign.as_bytes()))
        );
        (authorization, payload)
    }
}

fn hmac(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0xf)] as char);
    }
    out
}

/// Encodes one path segment: every byte outside the RFC 3986 unreserved set.
pub fn encode_segment(out: &mut String, segment: &str) {
    encode(out, segment, false);
}

/// Encodes one query name or value; `/` stays raw, like botocore.
pub fn encode_query(out: &mut String, value: &str) {
    encode(out, value, true);
}

fn encode(out: &mut String, value: &str, slash: bool) {
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) || (slash && byte == b'/') {
            out.push(byte as char);
        } else {
            use std::fmt::Write as _;
            write!(out, "%{byte:02X}").unwrap();
        }
    }
}
