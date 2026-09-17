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
//! * `header` - fixed fields below, all little-endian, native x86_64 layout.
//! * `user metadata` - one [`Versioned`] value chosen by the sender (e.g. slot
//!   and instance), uncompressed so routers can read it without decompressing.
//!   `metadata_len` is its true size; the section is padded to a multiple of 8.
//! * compressed tail - one zstd frame holding two homogeneous sections: the
//!   portable timestamp projection of every message (24-byte stride,
//!   [`InternalMetadata`]) followed by the leaves themselves at their own
//!   `size_of` stride. Both sections have `n_messages` elements.
//!
//! The leaf version is identified by `type_hash` (plain `TYPE_HASH`, no XOR)
//! and decoded through [`Versioned::decode_versions`], which casts the section
//! as the stored version and migrates to the latest. `version` pins everything
//! else: header layout, timestamp record, zstd. Change any of those, bump it.
//!
//! Every `Blob` needs 8-byte alignment; [`Scratch`] provides aligned storage
//! for decompression and for building blobs.

// Scaffold: bodies are `todo!()` until implemented. Remove with them.
#![allow(unused_variables, dead_code, clippy::type_complexity, clippy::doc_markdown)]

use std::collections::HashMap;

use flux_timing::InternalMessage;
use flux_utils::ArrayStr;

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

/// One batch of a single leaf type. See the [module docs](self) for the layout.
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
        todo!("W3")
    }

    pub fn as_bytes(&self) -> &[u8] {
        todo!("W3")
    }

    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        todo!("W3")
    }

    /// Copy `bytes` in and return them as a blob. For inputs whose alignment
    /// is not guaranteed, such as `DiskIo` read buffers.
    pub fn load(&mut self, bytes: &[u8]) -> Result<&Blob, DecodeError> {
        todo!("W3")
    }
}

impl Blob {
    /// Validate `bytes` as one blob and return it borrowed. No copy.
    ///
    /// Checks alignment, magic, version, header lengths against `bytes.len()`,
    /// and `type_name`. Does not decompress.
    pub fn from_bytes(bytes: &[u8]) -> Result<&Self, DecodeError> {
        todo!("W3")
    }

    /// The exact bytes of this blob: header plus tail, no trailing padding.
    pub fn as_bytes(&self) -> &[u8] {
        todo!("W3")
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
        todo!("W3")
    }

    /// The zstd tail.
    pub fn compressed(&self) -> &[u8] {
        todo!("W3")
    }

    /// Decode the user metadata as `U`, migrating if it was written as an
    /// older version. No decompression.
    pub fn user_metadata<U: Versioned>(&self) -> Result<U, DecodeError> {
        todo!("W3")
    }

    /// Decompress into `scratch`, decode the timestamps and leaves, and rebuild
    /// `InternalMessage`s with local tracking timestamps.
    ///
    /// Errors when `type_hash` is not a version of `T`, when
    /// `metadata_type_hash` is not a version of `U`, or when any length or
    /// value check fails.
    pub fn decode<U: Versioned, T: Versioned>(&self, scratch: &mut Scratch) -> Decoded<U, T> {
        todo!("W3")
    }
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
        todo!("W3")
    }

    pub fn is_empty(&self) -> bool {
        todo!("W3")
    }

    /// Total messages buffered across all leaf types.
    pub fn n_messages(&self) -> usize {
        todo!("W3")
    }

    /// Build one blob per non-empty leaf type, hand each to `sink`, and clear
    /// the buffers. Blobs are built in an internal [`Scratch`], so the `&Blob`
    /// is valid only for the duration of the callback.
    pub fn flush<U: Versioned>(&mut self, user_meta: &U, zstd_level: i32, sink: impl FnMut(&Blob)) {
        todo!("W3")
    }
}

struct Push<'a> {
    cache: &'a mut BlobCache,
    timestamp: InternalMetadata,
}

impl VisitorVersionedLeaf for Push<'_> {
    fn visit_leaf<L: Versioned>(&mut self, leaf: &L) {
        todo!("W3")
    }
}
