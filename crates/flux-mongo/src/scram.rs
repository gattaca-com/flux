//! SCRAM-SHA-256 client exchange: the RFC 5802 construction with SHA-256.
//!
//! Passwords are used verbatim, without `SASLprep`, so only ASCII passwords
//! authenticate reliably. Unlike `flux-postgres`, the messages here are bare
//! base64 payloads carried in `saslStart` / `saslContinue` command replies.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn attribute(message: &[u8], name: u8) -> Result<&[u8], &'static str> {
    message
        .split(|byte| *byte == b',')
        .find_map(|part| (part.len() > 2 && part[0] == name && part[1] == b'=').then(|| &part[2..]))
        .ok_or("SCRAM message misses an attribute")
}

pub struct Exchange {
    cnonce: Vec<u8>,
    client_first_bare: Vec<u8>,
    auth_message: Vec<u8>,
    salted_password: [u8; 32],
}

impl Exchange {
    pub fn start(user: &str, cnonce: &str) -> (Self, Vec<u8>) {
        let mut bare = Vec::with_capacity(user.len() + cnonce.len() + 4);
        bare.extend_from_slice(b"n=");
        for byte in user.bytes() {
            match byte {
                b',' => bare.extend_from_slice(b"=2C"),
                b'=' => bare.extend_from_slice(b"=3D"),
                _ => bare.push(byte),
            }
        }
        bare.extend_from_slice(b",r=");
        bare.extend_from_slice(cnonce.as_bytes());
        let mut message = Vec::with_capacity(bare.len() + 3);
        message.extend_from_slice(b"n,,");
        message.extend_from_slice(&bare);
        let exchange = Self {
            cnonce: cnonce.as_bytes().to_vec(),
            client_first_bare: bare,
            auth_message: Vec::new(),
            salted_password: [0; 32],
        };
        (exchange, message)
    }

    pub fn client_final(
        &mut self,
        password: &str,
        server_first: &[u8],
    ) -> Result<Vec<u8>, &'static str> {
        let nonce = attribute(server_first, b'r')?;
        if nonce.len() <= self.cnonce.len() || !nonce.starts_with(&self.cnonce) {
            return Err("SCRAM server nonce misses the client nonce");
        }
        let salt = STANDARD
            .decode(attribute(server_first, b's')?)
            .map_err(|_| "SCRAM salt is not base64")?;
        let iterations: u32 = std::str::from_utf8(attribute(server_first, b'i')?)
            .map_err(|_| "SCRAM iteration count is not a number")?
            .parse()
            .map_err(|_| "SCRAM iteration count is not a number")?;
        if iterations == 0 {
            return Err("SCRAM iteration count is zero");
        }
        let mac = HmacSha256::new_from_slice(password.as_bytes())
            .expect("HMAC accepts keys of any length");
        let mut prev: [u8; 32] = mac
            .clone()
            .chain_update(&salt)
            .chain_update(1u32.to_be_bytes())
            .finalize()
            .into_bytes()
            .into();
        let mut salted = prev;
        for _ in 1..iterations {
            prev = mac.clone().chain_update(prev).finalize().into_bytes().into();
            for (acc, byte) in salted.iter_mut().zip(prev) {
                *acc ^= byte;
            }
        }
        let mut without_proof = Vec::with_capacity(nonce.len() + 7);
        without_proof.extend_from_slice(b"c=biws,r=");
        without_proof.extend_from_slice(nonce);
        let bare = &self.client_first_bare;
        let mut auth_message =
            Vec::with_capacity(bare.len() + server_first.len() + without_proof.len() + 2);
        auth_message.extend_from_slice(bare);
        auth_message.push(b',');
        auth_message.extend_from_slice(server_first);
        auth_message.push(b',');
        auth_message.extend_from_slice(&without_proof);
        let client_key = hmac(&salted, b"Client Key");
        let signature = hmac(&Sha256::digest(client_key), &auth_message);
        let proof: Vec<u8> = client_key.iter().zip(signature).map(|(key, sig)| key ^ sig).collect();
        let mut response = Vec::with_capacity(without_proof.len() + 47);
        response.extend_from_slice(&without_proof);
        response.extend_from_slice(b",p=");
        response.extend_from_slice(STANDARD.encode(proof).as_bytes());
        self.auth_message = auth_message;
        self.salted_password = salted;
        Ok(response)
    }

    pub fn verify_server_final(&self, server_final: &[u8]) -> Result<(), &'static str> {
        let signature = STANDARD
            .decode(attribute(server_final, b'v')?)
            .map_err(|_| "SCRAM server signature is not base64")?;
        let expected = hmac(&hmac(&self.salted_password, b"Server Key"), &self.auth_message);
        if signature.as_slice() == expected {
            Ok(())
        } else {
            Err("SCRAM server signature mismatch")
        }
    }
}
