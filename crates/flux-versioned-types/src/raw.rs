//! Zero-copy blob: one wire and disk format for batches of versioned leaves.
//!
//! A [`Blob`] is an unsized `repr(C)` struct. Its bytes in memory *are* the
//! format: sending is a memcpy of [`Blob::as_bytes`], receiving is a validated
//! cast in [`Blob::from_bytes`], and a disk file is a concatenation of blobs.
//!
//! ```text
//! [header 128 B][user metadata, padded to 8][zstd( [InternalMetadata x n][Leaf x n] )]
//! ```
//!
//! * `header` - exactly 128 bytes, `repr(C)`, no padding. All integers are
//!   little-endian; pointers are never stored, so the layout is the native
//!   `x86_64` one. `magic` is `b"FLUXBLOB"`, `version` is [`FORMAT_VERSION`].
//! * `user metadata` - one [`Versioned`] value chosen by the sender (e.g. slot
//!   and instance), uncompressed so routers can read it without decompressing.
//!   `metadata_len` is its true size (`size_of::<U>()`); the section is padded
//!   with zeros to a multiple of 8.
//! * `compressed tail` - one zstd frame holding two homogeneous sections: the
//!   portable timestamp projection of every message ([`InternalMetadata`],
//!   24-byte stride) followed by the leaves themselves at their own `size_of`
//!   stride. Both sections have `n_messages` elements. `compressed_len` is the
//!   true zstd length; the tail is padded with zeros to a multiple of 8, so
//!   every blob's total byte length is a multiple of 8 and concatenated blobs
//!   in a file stay aligned. [`Blob::as_bytes`] returns exactly `128 +
//!   round8(metadata_len) + round8(compressed_len)` bytes.
//! * `decompressed_len` is `n_messages * (24 + size_of::<Leaf>())` and is
//!   validated after decompression.
//!
//! The leaf version is identified by `type_hash` (the leaf's latest
//! `TYPE_HASH`, no XOR) and decoded through [`Versioned::decode_versions`],
//! which casts the section as the stored version and migrates to the latest.
//! `metadata_type_hash` is the user metadata's latest `TYPE_HASH`.
//! `type_name` is [`Versioned::NAME`] truncated to 64 bytes; it is only a
//! label, identity is always the hash. `version` pins everything else: header
//! layout, timestamp record, zstd. Change any of those, bump it.
//!
//! Every `Blob` needs 8-byte alignment; [`Scratch`] provides aligned storage
//! for decompression and for building blobs.

use std::collections::HashMap;

use flux_timing::InternalMessage;
use flux_utils::ArrayStr;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::{
    blob::InternalMetadata,
    leaves::{Decoded, HasVersionedLeaves, Versioned, VisitorVersionedLeaf},
};

/// Identifies a blob on disk and lets a receiver sniff blobs from legacy
/// bincode frames, whose first bytes are a small enum index.
pub const MAGIC: [u8; 8] = *b"FLUXBLOB";
/// Format version. Version 1 = the header below + `InternalMetadata` timestamps
/// + one zstd frame.
pub const FORMAT_VERSION: u32 = 1;
/// Fixed header size in bytes.
pub const HEADER_LEN: usize = 128;
/// Stride of the timestamp section.
pub const TIMESTAMP_STRIDE: usize = 24;
/// Capacity of [`Blob::type_name`].
pub const TYPE_NAME_LEN: usize = 64;
/// Alignment required of every blob and section.
pub const ALIGN: usize = 8;

/// Round up to a multiple of 8. `None` on overflow; callers map that to
/// [`DecodeError::TooShort`] with an unsatisfiable length.
fn round8(n: u64) -> Option<u64> {
    n.checked_add(7).map(|m| m & !7)
}

/// One batch of a single leaf type. See the [module docs](self) for the layout.
#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct Blob {
    pub magic: [u8; 8],
    pub version: u32,
    /// Bytes of the user metadata value that follows the header.
    pub metadata_len: u32,
    /// Elements in each of the two compressed sections.
    pub n_messages: u32,
    _reserved: u32,
    /// `TYPE_HASH` of the leaf version that wrote this blob.
    pub type_hash: u64,
    /// `TYPE_HASH` of the user metadata version.
    pub metadata_type_hash: u64,
    /// Length of the zstd tail.
    pub compressed_len: u64,
    /// Length of the decompressed tail: `n_messages x (24 + size_of leaf)`.
    pub decompressed_len: u64,
    /// Wire label of the leaf, [`Versioned::NAME`].
    pub type_name: ArrayStr<TYPE_NAME_LEN>,
    /// `[user metadata, padded to 8][zstd tail]`.
    tail: [u8],
}

/// Why a byte slice is not a valid blob, or a blob did not decode.
#[derive(Debug)]
pub enum DecodeError {
    /// Fewer bytes than the header or the header's declared lengths.
    TooShort {
        needed: usize,
        got: usize,
    },
    /// The slice is not 8-byte aligned. Copy it into a [`Scratch`] first.
    Unaligned,
    BadMagic,
    UnsupportedVersion(u32),
    /// `type_name` is not valid UTF-8 or its length exceeds the capacity.
    BadTypeName,
    /// `type_hash` is not a version of the requested leaf, or
    /// `metadata_type_hash` is not a version of the requested user metadata.
    UnknownTypeHash(u64),
    /// A section's byte length is not `n_messages x stride`, or the
    /// decompressed tail does not match `decompressed_len`.
    LengthMismatch {
        expected: usize,
        got: usize,
    },
    /// A leaf or metadata value failed bit-pattern validation.
    InvalidValue,
    Zstd(std::io::Error),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for DecodeError {}

/// 8-byte aligned growable byte buffer for decompressing and building blobs.
#[derive(Default)]
pub struct Scratch {
    words: Vec<u64>,
    len: usize,
}

impl Scratch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resize to `len` bytes; new bytes are zero. Never shrinks the allocation.
    pub fn resize(&mut self, len: usize) {
        let words = len.div_ceil(ALIGN);
        if words > self.words.len() {
            self.words.resize(words, 0);
        }
        let old = self.len;
        self.len = len;
        if len > old {
            self.as_mut_bytes()[old..len].fill(0);
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.words.as_slice().as_bytes()[..self.len]
    }

    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        &mut self.words.as_mut_slice().as_mut_bytes()[..self.len]
    }

    /// Copy `bytes` in and return them as a blob. For inputs whose alignment
    /// is not guaranteed, such as `DiskIo` read buffers.
    pub fn load(&mut self, bytes: &[u8]) -> Result<&Blob, DecodeError> {
        self.resize(bytes.len());
        self.as_mut_bytes().copy_from_slice(bytes);
        Blob::from_bytes(self.as_bytes())
    }
}

impl Blob {
    /// Validate `bytes` as one blob and return it borrowed. No copy.
    ///
    /// Checks alignment, magic, version, header lengths against `bytes.len()`,
    /// and `type_name`. Does not decompress. With trailing bytes (a disk file
    /// of concatenated blobs) returns the first blob; [`Blob::as_bytes`]
    /// gives its exact length.
    pub fn from_bytes(bytes: &[u8]) -> Result<&Self, DecodeError> {
        if !(bytes.as_ptr() as usize).is_multiple_of(ALIGN) {
            return Err(DecodeError::Unaligned);
        }
        if bytes.len() < HEADER_LEN {
            return Err(DecodeError::TooShort { needed: HEADER_LEN, got: bytes.len() });
        }
        // Before the cast, so a legacy bincode frame is `BadMagic`, not a
        // complaint about whatever bytes sit where `type_name` would be.
        if bytes[..MAGIC.len()] != MAGIC {
            return Err(DecodeError::BadMagic);
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes"));
        if version != FORMAT_VERSION {
            return Err(DecodeError::UnsupportedVersion(version));
        }
        let head = Self::try_ref_from_bytes(&bytes[..HEADER_LEN]).map_err(|e| match e {
            zerocopy::ConvertError::Alignment(_) => DecodeError::Unaligned,
            zerocopy::ConvertError::Size(_) => {
                DecodeError::TooShort { needed: HEADER_LEN, got: bytes.len() }
            }
            zerocopy::ConvertError::Validity(_) => DecodeError::BadTypeName,
        })?;
        let tail_len = round8(u64::from(head.metadata_len))
            .and_then(|meta| round8(head.compressed_len).and_then(|comp| meta.checked_add(comp)))
            .and_then(|tail| tail.checked_add(HEADER_LEN as u64))
            .and_then(|need| usize::try_from(need).ok());
        let Some(need) = tail_len else {
            return Err(DecodeError::TooShort { needed: usize::MAX, got: bytes.len() });
        };
        if bytes.len() < need {
            return Err(DecodeError::TooShort { needed: need, got: bytes.len() });
        }
        let blob = Self::try_ref_from_bytes(&bytes[..need]).map_err(|e| match e {
            zerocopy::ConvertError::Alignment(_) => DecodeError::Unaligned,
            zerocopy::ConvertError::Size(_) => {
                DecodeError::TooShort { needed: need, got: bytes.len() }
            }
            zerocopy::ConvertError::Validity(_) => DecodeError::BadTypeName,
        })?;
        Ok(blob)
    }

    /// The exact bytes of this blob: header plus tail, no trailing padding.
    pub fn as_bytes(&self) -> &[u8] {
        // No `IntoBytes` derive: it rejects the slice DST. Sound by
        // construction: blobs only come from `from_bytes` over live bytes or
        // from `flush`, which fills header plus tail completely, so every
        // byte of the value is initialized.
        unsafe {
            core::slice::from_raw_parts(
                core::ptr::from_ref(self).cast::<u8>(),
                HEADER_LEN + self.tail.len(),
            )
        }
    }

    /// True when `type_hash` is any version of `T`.
    pub fn is<T: Versioned>(&self) -> bool {
        T::VERSION_HASHES.contains(&self.type_hash)
    }

    pub fn type_name(&self) -> &str {
        self.type_name.as_str()
    }

    /// The user metadata bytes, uncompressed. No decompression.
    pub fn user_metadata_bytes(&self) -> &[u8] {
        &self.tail[..self.metadata_len as usize]
    }

    /// The zstd tail.
    pub fn compressed(&self) -> &[u8] {
        let meta_pad = (self.metadata_len as usize).next_multiple_of(ALIGN);
        let len = self.compressed_len as usize;
        &self.tail[meta_pad..meta_pad + len]
    }

    /// Decode the user metadata as `U`, migrating if it was written as an
    /// older version. No decompression.
    pub fn user_metadata<U: Versioned>(&self) -> Result<U, DecodeError> {
        if !U::VERSION_HASHES.contains(&self.metadata_type_hash) {
            return Err(DecodeError::UnknownTypeHash(self.metadata_type_hash));
        }
        let out = U::decode_versions(self.metadata_type_hash, self.user_metadata_bytes())?;
        if out.len() != 1 {
            return Err(DecodeError::LengthMismatch { expected: 1, got: out.len() });
        }
        Ok(out[0])
    }

    /// Decompress into `scratch`, decode the timestamps and leaves, and rebuild
    /// `InternalMessage`s with local tracking timestamps.
    ///
    /// Errors when `type_hash` is not a version of `T`, when
    /// `metadata_type_hash` is not a version of `U`, or when any length or
    /// value check fails.
    pub fn decode<U: Versioned, T: Versioned>(&self, scratch: &mut Scratch) -> Decoded<U, T> {
        if !self.is::<T>() {
            return Err(DecodeError::UnknownTypeHash(self.type_hash));
        }
        let meta = self.user_metadata::<U>()?;
        // `decompressed_len` uses the stride of the version that wrote the
        // blob, which may be older than `T`; `decode_versions` validates the
        // leaf section against that stored stride.
        let n = u64::from(self.n_messages);
        let Some(ts_len) = n.checked_mul(TIMESTAMP_STRIDE as u64) else {
            return Err(DecodeError::LengthMismatch {
                expected: usize::MAX,
                got: self.decompressed_len as usize,
            });
        };
        if self.decompressed_len < ts_len {
            return Err(DecodeError::LengthMismatch {
                expected: usize::try_from(ts_len).unwrap_or(usize::MAX),
                got: self.decompressed_len as usize,
            });
        }
        let Ok(expected_len) = usize::try_from(self.decompressed_len) else {
            return Err(DecodeError::LengthMismatch { expected: usize::MAX, got: usize::MAX });
        };
        scratch.resize(expected_len);
        let written = zstd::bulk::decompress_to_buffer(self.compressed(), scratch.as_mut_bytes())
            .map_err(DecodeError::Zstd)?;
        if written != expected_len {
            return Err(DecodeError::LengthMismatch { expected: expected_len, got: written });
        }
        let bytes = scratch.as_bytes();
        let (ts_bytes, leaf_bytes) = bytes.split_at(ts_len as usize);
        let stamps = ref_timestamps(ts_bytes, n as usize)?;
        let leaves = T::decode_versions(self.type_hash, leaf_bytes)?;
        if leaves.len() != n as usize {
            return Err(DecodeError::LengthMismatch { expected: n as usize, got: leaves.len() });
        }
        Ok((
            meta,
            stamps
                .iter()
                .zip(leaves)
                .map(|(stamp, leaf)| InternalMessage::new(stamp.to_tracking_timestamp(), leaf))
                .collect(),
        ))
    }
}

fn ref_timestamps(bytes: &[u8], n: usize) -> Result<&[InternalMetadata], DecodeError> {
    let stamps = <[InternalMetadata]>::ref_from_bytes(bytes).map_err(|e| match e {
        zerocopy::ConvertError::Alignment(_) => DecodeError::Unaligned,
        zerocopy::ConvertError::Size(_) => DecodeError::LengthMismatch {
            expected: n * size_of::<InternalMetadata>(),
            got: bytes.len(),
        },
        zerocopy::ConvertError::Validity(i) => match i {},
    })?;
    if stamps.len() != n {
        return Err(DecodeError::LengthMismatch {
            expected: n * size_of::<InternalMetadata>(),
            got: bytes.len(),
        });
    }
    Ok(stamps)
}

/// Per-leaf-type byte buffers filled by [`BlobCache::push`].
struct TypedBuffer {
    name: &'static str,
    n_messages: u32,
    /// `InternalMetadata` records, 24 bytes each.
    timestamps: Vec<u8>,
    /// Leaf bytes at the leaf's `size_of` stride.
    leaves: Vec<u8>,
}

/// Accumulates messages per leaf type and emits one [`Blob`] per non-empty
/// type on [`flush`](Self::flush). No serialization: `push` appends the
/// message's projected timestamp and the leaf's bytes.
#[derive(Default)]
pub struct BlobCache {
    buffers: HashMap<u64, TypedBuffer>,
    build: Scratch,
}

impl BlobCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve `msg` to its leaf and append it to that leaf's buffer.
    pub fn push<T: HasVersionedLeaves>(&mut self, msg: &InternalMessage<T>) {
        let mut push =
            Push { cache: &mut *self, timestamp: InternalMetadata::from(msg.tracking_timestamp()) };
        msg.data().visit_leaf(&mut push);
    }

    pub fn is_empty(&self) -> bool {
        self.buffers.values().all(|buf| buf.n_messages == 0)
    }

    /// Total messages buffered across all leaf types.
    pub fn n_messages(&self) -> usize {
        self.buffers.values().map(|buf| buf.n_messages as usize).sum()
    }

    /// Build one blob per non-empty leaf type, hand each to `sink`, and clear
    /// the buffers. Blobs are built in an internal [`Scratch`], so the `&Blob`
    /// is valid only for the duration of the callback.
    pub fn flush<U: Versioned>(
        &mut self,
        user_meta: &U,
        zstd_level: i32,
        mut sink: impl FnMut(&Blob),
    ) {
        let mut keys: Vec<u64> = self
            .buffers
            .iter()
            .filter(|(_, buf)| buf.n_messages > 0)
            .map(|(key, _)| *key)
            .collect();
        keys.sort_unstable();
        let Self { buffers, build } = self;
        for key in keys {
            let buf = &buffers[&key];
            let stride = buf.leaves.len() / buf.n_messages as usize;
            let meta_bytes = <U as IntoBytes>::as_bytes(user_meta);
            let meta_pad = meta_bytes.len().next_multiple_of(ALIGN);
            let mut plain = Vec::with_capacity(buf.timestamps.len() + buf.leaves.len());
            plain.extend_from_slice(&buf.timestamps);
            plain.extend_from_slice(&buf.leaves);
            let comp =
                zstd::bulk::compress(&plain, zstd_level).expect("zstd bulk compression failed");
            let comp_pad = comp.len().next_multiple_of(ALIGN);
            // `resize` zero-fills, so the metadata and tail padding are zeros.
            build.resize(HEADER_LEN + meta_pad + comp_pad);
            let out = build.as_mut_bytes();
            out[..MAGIC.len()].copy_from_slice(&MAGIC);
            out[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
            out[12..16].copy_from_slice(&(size_of::<U>() as u32).to_le_bytes());
            out[16..20].copy_from_slice(&buf.n_messages.to_le_bytes());
            out[24..32].copy_from_slice(&key.to_le_bytes());
            out[32..40].copy_from_slice(&U::TYPE_HASH.to_le_bytes());
            out[40..48].copy_from_slice(&(comp.len() as u64).to_le_bytes());
            out[48..56].copy_from_slice(
                &(u64::from(buf.n_messages) * (TIMESTAMP_STRIDE as u64 + stride as u64))
                    .to_le_bytes(),
            );
            let type_name = ArrayStr::<TYPE_NAME_LEN>::from_str_truncate(buf.name);
            out[56..HEADER_LEN]
                .copy_from_slice(<ArrayStr<TYPE_NAME_LEN> as IntoBytes>::as_bytes(&type_name));
            out[HEADER_LEN..HEADER_LEN + meta_bytes.len()].copy_from_slice(meta_bytes);
            let tail = HEADER_LEN + meta_pad;
            out[tail..tail + comp.len()].copy_from_slice(&comp);
            let blob = Blob::from_bytes(build.as_bytes()).expect("freshly built blob is valid");
            sink(blob);
        }
        for buf in buffers.values_mut() {
            buf.timestamps.clear();
            buf.leaves.clear();
            buf.n_messages = 0;
        }
    }
}

struct Push<'a> {
    cache: &'a mut BlobCache,
    timestamp: InternalMetadata,
}

impl VisitorVersionedLeaf for Push<'_> {
    fn visit_leaf<L: Versioned>(&mut self, leaf: &L) {
        let timestamp = self.timestamp;
        let buf = self.cache.buffers.entry(L::TYPE_HASH).or_insert_with(|| TypedBuffer {
            name: L::NAME,
            n_messages: 0,
            timestamps: Vec::new(),
            leaves: Vec::new(),
        });
        buf.timestamps.extend_from_slice(<InternalMetadata as IntoBytes>::as_bytes(&timestamp));
        buf.leaves.extend_from_slice(<L as IntoBytes>::as_bytes(leaf));
        buf.n_messages += 1;
    }
}
