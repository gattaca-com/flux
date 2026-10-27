//! `SigV4` signing for path-style S3 requests, and for other AWS services
//! through [`Signer::with_service`].
//!
//! The signed headers are `host`, `x-amz-content-sha256`, `x-amz-date`,
//! `x-amz-security-token` when a session token is set, and whatever the
//! caller passes to [`Signer::sign`]; S3 rejects requests without the
//! payload-hash header. Over TLS the hash is
//! `UNSIGNED-PAYLOAD`, since the transport already protects the body and
//! hashing it costs about a millisecond per megabyte on the poll thread.
//! Paths encode every
//! byte outside the unreserved set but keep `/`, which separates key
//! segments. Query pairs sort by name and encode `/` as `%2F` too: S3
//! re-canonicalises the request from the decoded parameters, so a prefix or
//! continuation token signed with a raw `/` fails with
//! `SignatureDoesNotMatch`.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

const UNSIGNED: &str = "UNSIGNED-PAYLOAD";
const MAX_SIGNED_HEADERS: usize = 16;

/// Signs requests for one endpoint and set of credentials.
pub struct Signer {
    host: String,
    access: String,
    secret: String,
    session_token: Option<String>,
    region: String,
    service: String,
    /// Whether bodies travel as `UNSIGNED-PAYLOAD` instead of being hashed.
    unsigned_payloads: bool,
}

impl Signer {
    pub fn new(host: &str, access: &str, secret: &str, region: &str) -> Self {
        Self {
            host: host.to_owned(),
            access: access.to_owned(),
            secret: secret.to_owned(),
            session_token: None,
            region: region.to_owned(),
            service: "s3".to_owned(),
            unsigned_payloads: false,
        }
    }
    /// Signs for `service` (`glue`, ...) instead of `s3`.
    #[must_use]
    pub fn with_service(mut self, service: &str) -> Self {
        service.clone_into(&mut self.service);
        self
    }
    /// Stops binding bodies to their signature with a `SHA256` hash. Only
    /// for S3 endpoints reached over TLS, which protects the body itself;
    /// other services reject `UNSIGNED-PAYLOAD`.
    #[must_use]
    pub fn unsigned_payloads(mut self) -> Self {
        self.unsigned_payloads = true;
        self
    }
    pub fn set_credentials(&mut self, access: &str, secret: &str) {
        access.clone_into(&mut self.access);
        secret.clone_into(&mut self.secret);
    }
    /// The token that comes with temporary credentials, sent and signed as
    /// `x-amz-security-token`; `None` for long-term keys.
    pub fn set_session_token(&mut self, session_token: Option<&str>) {
        self.session_token = session_token.map(str::to_owned);
    }
    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }
    pub fn set_region(&mut self, region: &str) {
        region.clone_into(&mut self.region);
    }
    /// The `Host` this signer signs for.
    pub fn host(&self) -> &str {
        &self.host
    }
    /// Signs one request; `date` is `YYYYMMDDTHHMMSSZ`, `resource` the path
    /// without its query, `query` the sorted `name=value` pairs, `headers`
    /// further headers to sign, with lowercase names, that the caller also
    /// sends. Their names must be unique and must not be `host` or
    /// `x-amz-*` names the signer adds; join repeated values with commas.
    /// Returns the `Authorization` value and the payload hash for
    /// `x-amz-content-sha256`.
    pub fn sign(
        &self,
        method: &str,
        resource: &str,
        query: &str,
        date: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> (String, String) {
        use std::fmt::Write as _;
        let payload = if self.unsigned_payloads {
            UNSIGNED.to_owned()
        } else {
            hex(&Sha256::digest(body)[..])
        };
        let mut signed = [("", ""); MAX_SIGNED_HEADERS];
        let fixed = [
            ("host", self.host.as_str()),
            ("x-amz-content-sha256", payload.as_str()),
            ("x-amz-date", date),
        ];
        let token = self.session_token.as_deref().map(|token| ("x-amz-security-token", token));
        let mut len = 0;
        for header in fixed.into_iter().chain(token).chain(headers.iter().copied()) {
            assert!(len < MAX_SIGNED_HEADERS, "at most {MAX_SIGNED_HEADERS} signed headers");
            debug_assert!(!header.0.bytes().any(|b| b.is_ascii_uppercase()), "lowercase names");
            signed[len] = header;
            len += 1;
        }
        let signed = &mut signed[..len];
        signed.sort_unstable_by_key(|(name, _)| *name);
        debug_assert!(signed.windows(2).all(|pair| pair[0].0 != pair[1].0), "unique names");
        let mut canonical = format!("{method}\n{resource}\n{query}\n");
        let mut names = String::new();
        for (name, value) in &*signed {
            // SigV4 trims a value and turns each run of spaces in it into one.
            write!(canonical, "{name}:").unwrap();
            for (i, word) in value.split_ascii_whitespace().enumerate() {
                if i > 0 {
                    canonical.push(' ');
                }
                canonical.push_str(word);
            }
            canonical.push('\n');
            if !names.is_empty() {
                names.push(';');
            }
            names.push_str(name);
        }
        write!(canonical, "\n{names}\n{payload}").unwrap();
        let scope = format!("{}/{}/{}/aws4_request", &date[..8], self.region, self.service);
        let to_sign =
            format!("AWS4-HMAC-SHA256\n{date}\n{scope}\n{}", hex(&Sha256::digest(&canonical)[..]));
        let mut prefixed = Vec::with_capacity(4 + self.secret.len());
        prefixed.extend_from_slice(b"AWS4");
        prefixed.extend_from_slice(self.secret.as_bytes());
        let mut key = hmac(&prefixed, &date.as_bytes()[..8]);
        for part in [self.region.as_bytes(), self.service.as_bytes(), b"aws4_request".as_slice()] {
            key = hmac(&key, part);
        }
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={names}, Signature={}",
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

/// Encodes one path segment or query component: every byte outside the
/// RFC 3986 unreserved set, `/` included. Callers keep the separators they
/// mean by encoding each segment or value on its own.
pub fn encode(out: &mut String, value: &str) {
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
            out.push(byte as char);
        } else {
            use std::fmt::Write as _;
            write!(out, "%{byte:02X}").unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Signer;

    /// The GET example from the AWS `SigV4` header-signing documentation.
    #[test]
    fn signs_the_aws_example() {
        let signer = Signer::new(
            "examplebucket.s3.amazonaws.com",
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "us-east-1",
        );
        let (authorization, payload) =
            signer.sign("GET", "/test.txt", "", "20130524T000000Z", &[("range", "bytes=0-9")], b"");
        assert_eq!(payload, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assert_eq!(
            authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }
}
