//! Batched telemetry envelopes for spine-to-receiver transport.
//!
//! A [`TelemetryBridgeMessage`] carries one type's accumulated batch for a
//! slot window: the writer's `type_hash` so the reader can migrate it, a
//! `type_name` routing label, and the flattened `(data, metadata)` byte
//! pair. It deliberately carries no slot: slot boundaries travel as an
//! ordinary sentinel message and per-item timing lives in the metadata
//! payload, so the envelope stays generic transport.
//!
//! Senders compressing on the wire use [`TelemetryBridgeMessageV2`], which
//! records the [`TelemetryBridgePayloadEncoding`] alongside the payloads.

use std::{
    borrow::Cow,
    io::{self, Read},
};

use serde::{Deserialize, Serialize};

pub const DEFAULT_TELEMETRY_BRIDGE_ZSTD_LEVEL: i32 = 0;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[repr(C)]
pub struct TelemetryBridgeMessage {
    pub type_hash: u64,
    // Not 100% required, but useful to direct handling of the hashes to
    // different parts of the code + knowing which hashes belong to which
    // type name, e.g. `Builder.Bundle.Ingested`.
    pub type_name: Cow<'static, str>,
    pub flattened_msg_meta: (Vec<u8>, Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[repr(C)]
pub struct TelemetryBridgeMessageV2 {
    pub type_hash: u64,
    pub type_name: Cow<'static, str>,
    pub flattened_msg_meta: (Vec<u8>, Vec<u8>),
    pub payload_encoding: TelemetryBridgePayloadEncoding,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum TelemetryBridgePayloadEncoding {
    Uncompressed,
    Zstd { data_len: u64, metadata_len: u64 },
}

impl TelemetryBridgeMessage {
    pub fn new(type_hash: u64, type_name: String) -> Self {
        Self { type_hash, type_name: Cow::Owned(type_name), flattened_msg_meta: (vec![], vec![]) }
    }

    /// Append one item's serialized data and metadata to the batch.
    pub fn push(&mut self, mut data: Vec<u8>, mut metadata: Vec<u8>) {
        self.flattened_msg_meta.0.append(&mut data);
        self.flattened_msg_meta.1.append(&mut metadata);
    }

    /// Compress both payloads, recording their uncompressed lengths so the
    /// reader can validate the decoded size exactly.
    ///
    /// ```
    /// use flux_versioned_types::TelemetryBridgeMessage;
    ///
    /// let mut msg = TelemetryBridgeMessage::new(7, "test".to_string());
    /// msg.push(b"payload-data".repeat(64), b"metadata".repeat(64));
    /// let compressed = msg.to_zstd_v2(0).unwrap();
    /// let back = compressed.into_uncompressed_message().unwrap();
    /// assert_eq!(back.flattened_msg_meta.0, b"payload-data".repeat(64));
    /// assert_eq!(back.flattened_msg_meta.1, b"metadata".repeat(64));
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn to_zstd_v2(&self, zstd_level: i32) -> io::Result<TelemetryBridgeMessageV2> {
        let data_len = self.flattened_msg_meta.0.len() as u64;
        let metadata_len = self.flattened_msg_meta.1.len() as u64;
        Ok(TelemetryBridgeMessageV2 {
            type_hash: self.type_hash,
            type_name: self.type_name.clone(),
            flattened_msg_meta: (
                compress_vec(&self.flattened_msg_meta.0, zstd_level)?,
                compress_vec(&self.flattened_msg_meta.1, zstd_level)?,
            ),
            payload_encoding: TelemetryBridgePayloadEncoding::Zstd { data_len, metadata_len },
        })
    }
}

impl TelemetryBridgeMessageV2 {
    pub fn into_uncompressed_message(self) -> io::Result<TelemetryBridgeMessage> {
        let (data, metadata) = match self.payload_encoding {
            TelemetryBridgePayloadEncoding::Uncompressed => self.flattened_msg_meta,
            TelemetryBridgePayloadEncoding::Zstd { data_len, metadata_len } => (
                decompress_vec(&self.flattened_msg_meta.0, data_len)?,
                decompress_vec(&self.flattened_msg_meta.1, metadata_len)?,
            ),
        };
        Ok(TelemetryBridgeMessage {
            type_hash: self.type_hash,
            type_name: self.type_name,
            flattened_msg_meta: (data, metadata),
        })
    }
}

impl From<TelemetryBridgeMessage> for TelemetryBridgeMessageV2 {
    fn from(msg: TelemetryBridgeMessage) -> Self {
        Self {
            type_hash: msg.type_hash,
            type_name: msg.type_name,
            flattened_msg_meta: msg.flattened_msg_meta,
            payload_encoding: TelemetryBridgePayloadEncoding::Uncompressed,
        }
    }
}

impl std::fmt::Display for TelemetryBridgeMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TelemetryBridgeMessage(type_hash={}, type_name={}, {}Kb)",
            self.type_hash,
            self.type_name,
            (self.flattened_msg_meta.0.len() + self.flattened_msg_meta.1.len()) / 1000
        )
    }
}

fn compress_vec(bytes: &[u8], zstd_level: i32) -> io::Result<Vec<u8>> {
    zstd::stream::encode_all(bytes, zstd_level)
}

fn decompress_vec(bytes: &[u8], expected_len: u64) -> io::Result<Vec<u8>> {
    let read_limit = expected_len.checked_add(1).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "telemetry bridge payload length overflow")
    })?;
    let mut reader = zstd::stream::read::Decoder::new(bytes)?.take(read_limit);
    let mut out = Vec::new();
    reader.read_to_end(&mut out)?;
    if out.len() as u64 != expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "telemetry bridge payload decoded to {} bytes, expected {expected_len}",
                out.len()
            ),
        ));
    }
    Ok(out)
}
