//! Versioned blobs for sending and persisting vectors of evolving types.
//!
//! A [`VersionedBlob`] stores a bincode payload alongside the `TypeHash` of
//! the version that was written, mixed with `123_456` (the same obfuscation
//! the generated `versioned_deserialize_vec` expects), so any reader can
//! detect the version and migrate it. Data and per-item metadata travel as
//! two byte strings; for [`InternalMessage`] vectors the metadata is the
//! portable [`TrackingTimestampWire`] projection of each tracking timestamp.
//!
//! The on-disk format is a magic header (`GTCVBLB`) with the type hash and
//! payload lengths, followed by zstd-compressed data and metadata. There is
//! exactly one format version; readers reject anything else.

use std::path::{Path, PathBuf};

use flux_timing::{IngestionTime, InternalMessage, PublishDelta, TrackingTimestamp};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use type_hash::TypeHash;

use crate::VersionedDeserialize;

const DISK_MAGIC: [u8; 8] = *b"GTCVBLB\0";
const DISK_VERSION: u32 = 1;
const DISK_HEADER_LEN: usize = DISK_MAGIC.len() + size_of::<u32>() + 3 * size_of::<u64>();
const MAX_DISK_BLOB_LEN: u64 = 16_000_000_000;

/// A versioned type with a stable on-disk home.
///
/// `<base_dir>/<PERSIST_DIR>/` holds its `<filename>.bin` blobs.
/// `versioned_telemetry!` with `persist = "dir"` implements this;
/// other types can opt in by hand.
pub trait VersionedPersistable: VersionedDeserialize + TypeHash + Serialize {
    const PERSIST_DIR: &'static str;

    fn persist_path(base_dir: &Path, filename: &str) -> PathBuf {
        base_dir.join(Self::PERSIST_DIR).join(filename).with_added_extension("bin")
    }
}

/// A self-describing batch of versioned values plus per-item metadata.
///
/// `Serialize`/`Deserialize` so it can go over the wire as-is; see
/// [`VersionedBlob::write_to`] for the framed on-disk form.
///
/// ```
/// use flux::type_hash_derive::type_hash_lock;
/// use flux_versioned_types::{VersionedBlob, VersionedPersistable, versioned_enum, versioned_telemetry};
///
/// versioned_telemetry!(Bid, persist = "bids" =>
///     #[type_hash_lock(hash = 3778581902668456365)]
///     BidV1 { pub price: u64 }
///
///     #[type_hash_lock(hash = 8711388280237211383)]
///     BidV2 {
///         add { pub size: u64 = 0 }
///     }
/// );
///
/// // Sender (possibly an older build): pack V1 values, ship the blob as-is.
/// let old = vec![BidV1 { price: 10 }];
/// let blob = VersionedBlob::from_parts::<BidV1, _, _>(&old, &Vec::<u8>::new());
/// let wire: Vec<u8> = bincode::serialize(&blob)?;
///
/// // Receiver: the payload migrates to the latest version on decode.
/// let blob: VersionedBlob = bincode::deserialize(&wire)?;
/// let latest: Vec<Bid> = blob.data_as()?;
/// assert_eq!(latest, vec![BidV2 { price: 10, size: 0 }]);
///
/// // Persisting is the same blob under the type's persist directory:
/// // `<base>/bids/bids.bin`, framed and compressed.
/// let base = std::env::temp_dir().join("flux-doctest");
/// blob.write_as::<Bid>(&base, "bids", 1);
/// let loaded = VersionedBlob::read_as::<Bid>(&base, "bids").unwrap();
/// assert_eq!(loaded.data_as::<Bid>()?.len(), 1);
///
/// // The persist directory defaults to the type name but can be overridden.
/// versioned_telemetry!(Legacy, persist = "legacy_bids" =>
///     #[type_hash_lock(hash = 10197174478225006219)]
///     LegacyV1 { pub price: u64 }
/// );
/// assert_eq!(<Legacy as VersionedPersistable>::PERSIST_DIR, "legacy_bids");
///
/// // Enums get the same machinery.
/// versioned_enum!(Mode, persist = "modes" =>
///     #[type_hash_lock(hash = 2116725509198536217)]
///     ModeV1 { Fast, Slow }
/// );
/// assert_eq!(<Mode as VersionedPersistable>::PERSIST_DIR, "modes");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionedBlob {
    pub type_hash: u64,
    pub data: Vec<u8>,
    pub metadata: Vec<u8>,
}

impl VersionedBlob {
    /// Pack serializable data and metadata vectors, tagging them with `T`'s
    /// version so [`VersionedBlob::data_as`] can migrate them later.
    pub fn from_parts<T: TypeHash, Data: Serialize, Metadata: Serialize>(
        data: &Data,
        metadata: &Metadata,
    ) -> Self {
        Self {
            type_hash: T::TYPE_HASH ^ 123_456,
            data: bincode::serialize(data).expect("failed to serialize"),
            metadata: bincode::serialize(metadata).expect("failed to serialize metadata"),
        }
    }

    /// Pack an [`InternalMessage`] slice, projecting each tracking timestamp
    /// to portable [`TrackingTimestampWire`].
    pub fn from_tracked<T: TypeHash + Serialize>(vals: &[InternalMessage<T>]) -> Self {
        Self::from_parts::<T, _, _>(
            &vals.iter().map(InternalMessage::data).collect::<Vec<_>>(),
            &vals
                .iter()
                .map(|m| TrackingTimestampWire::from(m.tracking_timestamp()))
                .collect::<Vec<_>>(),
        )
    }

    /// Decode and migrate the data payload to the latest version.
    pub fn data_as<T: VersionedDeserialize>(&self) -> bincode::Result<Vec<T>> {
        T::versioned_deserialize_vec(self.type_hash, &self.data)
    }

    /// Decode the metadata payload.
    pub fn metadata_as<Metadata: DeserializeOwned>(&self) -> bincode::Result<Vec<Metadata>> {
        bincode::deserialize(&self.metadata)
    }

    /// Decode to [`InternalMessage`]s, reattaching portable timing metadata.
    /// Falls back to pre-`tile_id` metadata for payloads written before it.
    pub fn to_tracked<T: VersionedDeserialize>(&self) -> Option<Vec<InternalMessage<T>>> {
        let meta: Vec<TrackingTimestampWire> = bincode::deserialize(&self.metadata)
            .or_else(|_| -> Result<_, bincode::Error> {
                let old: Vec<TrackingTimestampWireV1> = bincode::deserialize(&self.metadata)?;
                Ok(old.into_iter().map(TrackingTimestampWire::from).collect())
            })
            .inspect_err(|e| {
                tracing::error!("metadata deserialize failed: {e}");
            })
            .ok()?;
        let items = self
            .data_as()
            .inspect_err(|e| {
                tracing::error!("data deserialize failed for {}: {e}", std::any::type_name::<T>());
            })
            .ok()?;
        Some(
            items
                .into_iter()
                .zip(meta)
                .map(|(v, m)| InternalMessage::new(m.to_tracking_timestamp(), v))
                .collect(),
        )
    }

    /// Write the framed, compressed on-disk form. Failures are logged; there
    /// is no partial file worth reporting because the header is written first
    /// and readers validate lengths before allocating.
    pub fn write_to(&self, path: &Path, compression_level: i32) {
        use std::io::Write;

        let (Ok(data_len), Ok(metadata_len)) =
            (u64::try_from(self.data.len()), u64::try_from(self.metadata.len()))
        else {
            tracing::error!(?path, "persistence payload length does not fit on disk");
            return;
        };
        let Some(payload_len) = data_len.checked_add(metadata_len) else {
            tracing::error!(?path, data_len, metadata_len, "persistence payload length overflow");
            return;
        };
        if payload_len > MAX_DISK_BLOB_LEN {
            tracing::error!(?path, payload_len, "persistence payload exceeds maximum length");
            return;
        }
        let Some(parent) = path.parent() else {
            tracing::error!(?path, "couldn't find persistence parent directory");
            return;
        };
        if let Err(e) = std::fs::create_dir_all(parent) {
            tracing::warn!(?path, "couldn't create persistence directory: {e}");
            return;
        }
        let mut file = match std::fs::File::create(path) {
            Ok(file) => file,
            Err(e) => {
                tracing::error!(?path, "couldn't open persistence file: {e}");
                return;
            }
        };
        let header = [
            DISK_MAGIC.as_slice(),
            &DISK_VERSION.to_le_bytes(),
            &self.type_hash.to_le_bytes(),
            &data_len.to_le_bytes(),
            &metadata_len.to_le_bytes(),
        ];
        for bytes in header {
            if let Err(e) = file.write_all(bytes) {
                tracing::error!(?path, "couldn't write persistence header: {e}");
                return;
            }
        }
        let mut encoder = match zstd::Encoder::new(file, compression_level) {
            Ok(encoder) => encoder,
            Err(e) => {
                tracing::error!(?path, "couldn't create persistence encoder: {e}");
                return;
            }
        };
        if let Err(e) = encoder.write_all(&self.data) {
            tracing::error!(?path, "couldn't write persistence data: {e}");
            return;
        }
        if let Err(e) = encoder.write_all(&self.metadata) {
            tracing::error!(?path, "couldn't write persistence metadata: {e}");
            return;
        }
        if let Err(e) = encoder.finish() {
            tracing::error!(?path, "couldn't finish persistence file: {e}");
        }
    }

    /// Write to `<base_dir>/<T::PERSIST_DIR>/<filename>.bin`. See
    /// [`VersionedBlob::write_to`] for failure semantics.
    pub fn write_as<T: VersionedPersistable>(
        &self,
        base_dir: &Path,
        filename: &str,
        compression_level: i32,
    ) {
        self.write_to(&T::persist_path(base_dir, filename), compression_level);
    }

    /// Read back [`VersionedBlob::write_as`] output.
    pub fn read_as<T: VersionedPersistable>(base_dir: &Path, filename: &str) -> Option<Self> {
        Self::read_from(&T::persist_path(base_dir, filename))
    }

    /// Read back [`VersionedBlob::write_to`] output. Returns `None` for
    /// missing files, unknown versions, and corrupt or oversized payloads.
    pub fn read_from(path: &Path) -> Option<Self> {
        use std::io::Read;

        let mut file = std::fs::File::open(path)
            .inspect_err(|e| tracing::warn!(?path, "issue opening persistence file: {e}"))
            .ok()?;
        let mut magic = [0; DISK_MAGIC.len()];
        if let Err(e) = file.read_exact(&mut magic) {
            tracing::warn!(?path, "couldn't read persistence header: {e}");
            return None;
        }
        if magic != DISK_MAGIC {
            tracing::warn!(?path, "unknown persistence magic");
            return None;
        }
        let mut header = [0; DISK_HEADER_LEN - DISK_MAGIC.len()];
        if let Err(e) = file.read_exact(&mut header) {
            tracing::warn!(?path, "couldn't read persistence header: {e}");
            return None;
        }
        let version = u32::from_le_bytes(header[..size_of::<u32>()].try_into().unwrap());
        if version != DISK_VERSION {
            tracing::warn!(?path, version, "unsupported persistence version");
            return None;
        }
        let type_hash_start = size_of::<u32>();
        let data_len_start = type_hash_start + size_of::<u64>();
        let metadata_len_start = data_len_start + size_of::<u64>();
        let type_hash =
            u64::from_le_bytes(header[type_hash_start..data_len_start].try_into().unwrap());
        let data_len =
            u64::from_le_bytes(header[data_len_start..metadata_len_start].try_into().unwrap());
        let metadata_len = u64::from_le_bytes(header[metadata_len_start..].try_into().unwrap());
        let Some(expected_len) = data_len.checked_add(metadata_len) else {
            tracing::warn!(?path, data_len, metadata_len, "invalid persistence payload length");
            return None;
        };
        if expected_len > MAX_DISK_BLOB_LEN {
            tracing::warn!(?path, expected_len, "persistence payload exceeds maximum length");
            return None;
        }
        let (Ok(expected_len), Ok(data_len)) =
            (usize::try_from(expected_len), usize::try_from(data_len))
        else {
            tracing::warn!(?path, data_len, metadata_len, "invalid persistence payload length");
            return None;
        };
        let Some(read_limit) = expected_len.checked_add(1) else {
            tracing::warn!(?path, expected_len, "persistence payload length overflows read limit");
            return None;
        };
        let mut bytes = Vec::new();
        if let Err(e) = bytes.try_reserve_exact(read_limit) {
            tracing::warn!(?path, expected_len, "couldn't allocate persistence payload: {e}");
            return None;
        }
        let mut decoder = match zstd::Decoder::new(file) {
            Ok(decoder) => decoder.take(read_limit as u64),
            Err(e) => {
                tracing::warn!(?path, "couldn't create persistence decoder: {e}");
                return None;
            }
        };
        if let Err(e) = decoder.read_to_end(&mut bytes) {
            tracing::warn!(?path, "couldn't decode persistence file: {e}");
            return None;
        }
        if bytes.len() != expected_len {
            tracing::warn!(
                ?path,
                expected_len,
                actual_len = bytes.len(),
                "invalid persistence payload length"
            );
            return None;
        }
        let metadata = bytes.split_off(data_len);
        Some(Self { type_hash, data: bytes, metadata })
    }
}

/// Portable timing metadata sent over the wire. Wall-clock `Nanos`, safe
/// across machines with different RDTSC rates.
///
/// The blob format (`crate::raw`) stores these as 24-byte records:
/// 8 + 8 + 2 bytes of fields plus 6 bytes of explicit zero padding, so the
/// record is padding-free and every bit pattern is valid.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, byte_stable_derive::ByteStable)]
#[repr(C)]
pub struct TrackingTimestampWire {
    pub ingestion_t_real: flux_timing::Nanos,
    pub publish_t_real: flux_timing::Nanos,
    pub tile_id: u16,
    /// Zero padding to the 24-byte record stride. Skipped by serde so the
    /// bincode layout of legacy blobs is unchanged; always zero on write.
    /// Private so it cannot be set to anything else by hand.
    #[serde(skip)]
    _pad: [u8; 6],
}

// Wire-format pin: the timestamp section stride. Changing it is a new
// `raw::FORMAT_VERSION`.
const _: () = assert!(size_of::<TrackingTimestampWire>() == 24);

/// Pre-tile-id metadata format. Deserialization fallback for payloads written
/// before `tile_id` was added.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[repr(C)]
pub struct TrackingTimestampWireV1 {
    pub ingestion_t_real: flux_timing::Nanos,
    pub publish_t_real: flux_timing::Nanos,
}

impl From<TrackingTimestampWireV1> for TrackingTimestampWire {
    fn from(v: TrackingTimestampWireV1) -> Self {
        Self {
            ingestion_t_real: v.ingestion_t_real,
            publish_t_real: v.publish_t_real,
            tile_id: 0,
            _pad: [0; 6],
        }
    }
}

impl TrackingTimestampWire {
    /// Reconstruct a local `TrackingTimestamp` from portable wall-clock values.
    pub fn to_tracking_timestamp(self) -> TrackingTimestamp {
        let ingestion = IngestionTime::from(self.ingestion_t_real);
        let publish = IngestionTime::from(self.publish_t_real);

        // Needed when ingestion and publish the same, but conversion to RDTSC might
        // give some noise to have negative delta
        let publish_internal = std::cmp::max(ingestion.internal(), publish.internal());
        TrackingTimestamp {
            ingestion_t: ingestion,
            publish_delta: PublishDelta::new(self.tile_id)
                .from_ingestion_and_publish_t(ingestion.internal(), publish_internal),
        }
    }
}

impl From<TrackingTimestamp> for TrackingTimestampWire {
    fn from(t: TrackingTimestamp) -> Self {
        Self {
            ingestion_t_real: t.ingestion_t().real(),
            publish_t_real: t.publish_t(),
            tile_id: t.tile_id(),
            _pad: [0; 6],
        }
    }
}
