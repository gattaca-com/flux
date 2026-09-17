//! Zero-copy blob: one wire and disk format for batches of versioned leaves.
//!
//! A [`Blob`] is an unsized `repr(C)` struct. Its bytes in memory *are* the
//! format: sending is a memcpy of [`Blob::as_bytes`], receiving is a validated
//! cast in [`Blob::from_bytes`], and a disk file is a concatenation of blobs.
//!
//! ```text
//! [header 144 B][user metadata, padded to 8][zstd( [TrackingTimestampWire x n][Leaf x n] )]
//! ```
//!
//! * `header` - exactly 144 bytes, `repr(C)`, no padding. All integers are
//!   little-endian; pointers are never stored, so the layout is the native
//!   `x86_64` one. `magic` is `b"FLUXBLOB"`, `version` is [`FORMAT_VERSION`].
//!   `publish_t_first`/`publish_t_last` are the earliest and latest publish
//!   wall clocks in the batch, so a router or persister can order and index
//!   blobs by time span without decompressing. They are advisory: written by
//!   the sender's clock and not validated on read.
//! * `user metadata` - one [`Versioned`] value chosen by the sender (e.g. slot
//!   and instance), uncompressed so routers can read it without decompressing.
//!   `metadata_len` is its true size (`size_of::<U>()`); the section is padded
//!   with zeros to a multiple of 8.
//! * `compressed tail` - one zstd frame holding two homogeneous sections: the
//!   portable timestamp projection of every message ([`TrackingTimestampWire`],
//!   24-byte stride) followed by the leaves themselves at their own `size_of`
//!   stride. Both sections have `n_messages` elements. `compressed_len` is the
//!   true zstd length; the tail is padded with zeros to a multiple of 8, so
//!   every blob's total byte length is a multiple of 8 and concatenated blobs
//!   in a file stay aligned. [`Blob::as_bytes`] returns exactly `144 +
//!   round8(metadata_len) + round8(compressed_len)` bytes.
//! * `decompressed_len` is `n_messages * (24 + size_of::<LeafVn>())` for the
//!   version that wrote the blob. Readers check it against
//!   [`Versioned::version_size`] before allocating and again after
//!   decompression, so a hostile header cannot drive an allocation.
//!
//! The leaf version is identified by `type_hash`: the plain `TYPE_HASH` of
//! the version that wrote the blob (any entry of [`Versioned::VERSION_HASHES`],
//! no XOR). [`Versioned::decode_versions`] casts the section as that version
//! and migrates to the latest. `metadata_type_hash` identifies the user
//! metadata version the same way.
//! `type_name` is [`Versioned::NAME`] truncated to 64 bytes; it is only a
//! label, identity is always the hash. `version` pins everything else: header
//! layout, timestamp record, zstd. Change any of those, bump it.
//!
//! Every `Blob` needs 8-byte alignment; [`Scratch`] provides aligned storage
//! for decompression and for building blobs.

use std::collections::HashMap;

use byte_stable::ByteStable;
use flux_timing::{InternalMessage, Nanos};
use flux_utils::ArrayStr;

use crate::{
    blob::TrackingTimestampWire,
    leaves::{Decoded, HasVersionedLeaves, Versioned, VisitorVersionedLeaf},
};

/// Identifies a blob on disk and lets a receiver sniff blobs from legacy
/// bincode frames, whose first bytes are a small enum index.
pub const MAGIC: [u8; 8] = *b"FLUXBLOB";
/// Format version. Version 1 = the header below + `TrackingTimestampWire`
/// timestamps
/// + one zstd frame.
pub const FORMAT_VERSION: u32 = 1;
/// Capacity of [`Blob::type_name`].
pub const TYPE_NAME_LEN: usize = 64;
/// Alignment required of every blob and section. Leaves must not need more.
pub const ALIGN: usize = 8;

// The header is read by native reinterpretation, so the format is defined by
// this layout and nothing else. All producers and consumers run on x86_64.
const _: () = assert!(cfg!(target_endian = "little") && size_of::<usize>() == 8);
// `ArrayStr` is only padding-free when `N % 8 == 0`; pin the one we embed.
const _: () = assert!(size_of::<ArrayStr<TYPE_NAME_LEN>>() == size_of::<usize>() + TYPE_NAME_LEN);

/// Round up to a multiple of 8. `None` on overflow; callers map that to
/// [`DecodeError::TooShort`] with an unsatisfiable length.
fn round8(n: u64) -> Option<u64> {
    n.checked_add(7).map(|m| m & !7)
}

/// Fixed header of a [`Blob`]: its first 144 bytes.
#[derive(Clone, Copy, byte_stable_derive::ByteStable)]
#[repr(C)]
pub struct BlobHeader {
    pub magic: [u8; 8],
    pub version: u32,
    /// Bytes of the user metadata value that follows the header.
    pub metadata_len: u32,
    /// Elements in each of the two compressed sections.
    pub n_messages: u32,
    /// Writers zero it, readers ignore it.
    _reserved: u32,
    /// `TYPE_HASH` of the leaf version that wrote this blob.
    pub type_hash: u64,
    /// `TYPE_HASH` of the user metadata version.
    pub metadata_type_hash: u64,
    /// Length of the zstd tail.
    pub compressed_len: u64,
    /// Length of the decompressed tail: `n_messages x (24 + size_of leaf)`.
    pub decompressed_len: u64,
    /// Earliest publish wall clock among the batch's messages. Advisory.
    pub publish_t_first: Nanos,
    /// Latest publish wall clock among the batch's messages. Advisory.
    pub publish_t_last: Nanos,
    /// Wire label of the leaf, [`Versioned::NAME`].
    pub type_name: ArrayStr<TYPE_NAME_LEN>,
}

// Wire-format pins. Changing either is a new `FORMAT_VERSION`.
const _: () = assert!(size_of::<BlobHeader>() == 144 && align_of::<BlobHeader>() == ALIGN);

/// One batch of a single leaf type. See the [module docs](self) for the layout.
///
/// Obtain one through [`Blob::from_bytes`], [`Scratch::load`] or
/// [`BlobCache::flush`].
#[repr(C)]
pub struct Blob {
    pub header: BlobHeader,
    /// `[user metadata, padded to 8][zstd tail, padded to 8]`. Words rather
    /// than bytes so the type has no trailing padding at any length.
    tail: [u64],
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
        &byte_stable::slice_as_bytes(&self.words)[..self.len]
    }

    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        &mut byte_stable::words_as_bytes_mut(&mut self.words)[..self.len]
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
    ///
    /// The slice must be 8-byte aligned: `flux-network` payloads already are,
    /// while other read buffers (e.g. `DiskIo`) must go through
    /// [`Scratch::load`], which copies into aligned storage first.
    pub fn from_bytes(bytes: &[u8]) -> Result<&Self, DecodeError> {
        if !(bytes.as_ptr() as usize).is_multiple_of(ALIGN) {
            return Err(DecodeError::Unaligned);
        }
        let header_len = size_of::<BlobHeader>();
        if bytes.len() < header_len {
            return Err(DecodeError::TooShort { needed: header_len, got: bytes.len() });
        }
        // Before the cast, so a legacy bincode frame is `BadMagic`, not a
        // complaint about whatever bytes sit where `type_name` would be.
        let magic_at = core::mem::offset_of!(BlobHeader, magic);
        if bytes[magic_at..magic_at + MAGIC.len()] != MAGIC {
            return Err(DecodeError::BadMagic);
        }
        let version_at = core::mem::offset_of!(BlobHeader, version);
        let version =
            u32::from_le_bytes(bytes[version_at..version_at + 4].try_into().expect("4 bytes"));
        if version != FORMAT_VERSION {
            return Err(DecodeError::UnsupportedVersion(version));
        }
        // `type_name` is the only header field that can be invalid.
        if !BlobHeader::is_valid(&bytes[..header_len]) {
            return Err(DecodeError::BadTypeName);
        }
        // The header is valid, so only alignment and length can still fail;
        // both were checked above.
        let header = match byte_stable::cast_slice::<BlobHeader>(&bytes[..header_len]) {
            Ok(headers) => headers[0],
            Err(byte_stable::CastError::Unaligned) => return Err(DecodeError::Unaligned),
            Err(byte_stable::CastError::Length { .. } | byte_stable::CastError::ZeroSized) => {
                return Err(DecodeError::TooShort { needed: header_len, got: bytes.len() });
            }
            Err(byte_stable::CastError::Invalid { .. }) => {
                return Err(DecodeError::BadTypeName);
            }
        };
        let need = round8(u64::from(header.metadata_len))
            .and_then(|meta| round8(header.compressed_len).and_then(|comp| meta.checked_add(comp)))
            .and_then(|tail| tail.checked_add(header_len as u64))
            .and_then(|need| usize::try_from(need).ok());
        let Some(need) = need else {
            return Err(DecodeError::TooShort { needed: usize::MAX, got: bytes.len() });
        };
        if bytes.len() < need {
            return Err(DecodeError::TooShort { needed: need, got: bytes.len() });
        }
        let words = (need - header_len) / ALIGN;
        // `from_bytes` checked 8-byte alignment above.
        #[allow(clippy::cast_ptr_alignment)]
        let ptr =
            core::ptr::slice_from_raw_parts(bytes.as_ptr().cast::<u64>(), words) as *const Self;
        // Safety: `bytes` holds `need` initialized bytes, 8-aligned and borrowed for
        // the returned lifetime; the header is validated and the `u64` tail accepts
        // every pattern. The pointer is the blob start, its length the tail word count.
        Ok(unsafe { &*ptr })
    }

    /// The exact bytes of this blob: header plus tail.
    pub fn as_bytes(&self) -> &[u8] {
        // Safety: contract 1 (no padding: the header is `ByteStable`, the tail
        // is `u64`s) makes every byte initialized, and contract 3 (no interior
        // mutability) keeps them from changing under the borrow.
        unsafe {
            core::slice::from_raw_parts(
                core::ptr::from_ref(self).cast::<u8>(),
                size_of::<BlobHeader>() + self.tail.len() * ALIGN,
            )
        }
    }

    /// True when `type_hash` is any version of `T`.
    pub fn is<T: Versioned>(&self) -> bool {
        T::VERSION_HASHES.contains(&self.header.type_hash)
    }

    pub fn type_name(&self) -> &str {
        self.header.type_name.as_str()
    }

    /// The user metadata bytes, uncompressed. No decompression.
    pub fn user_metadata_bytes(&self) -> &[u8] {
        &byte_stable::slice_as_bytes(&self.tail)[..self.header.metadata_len as usize]
    }

    /// The zstd tail.
    pub fn compressed(&self) -> &[u8] {
        let meta_pad = (self.header.metadata_len as usize).next_multiple_of(ALIGN);
        let len = self.header.compressed_len as usize;
        &byte_stable::slice_as_bytes(&self.tail)[meta_pad..meta_pad + len]
    }

    /// Decode the user metadata as `U`, migrating if it was written as an
    /// older version. No decompression.
    pub fn user_metadata<U: Versioned>(&self) -> Result<U, DecodeError> {
        let Some(size) = U::version_size(self.header.metadata_type_hash) else {
            return Err(DecodeError::UnknownTypeHash(self.header.metadata_type_hash));
        };
        if self.header.metadata_len as usize != size {
            return Err(DecodeError::LengthMismatch {
                expected: size,
                got: self.header.metadata_len as usize,
            });
        }
        let out = U::decode_versions(self.header.metadata_type_hash, self.user_metadata_bytes())?;
        if out.len() != 1 {
            return Err(DecodeError::LengthMismatch { expected: 1, got: out.len() });
        }
        Ok(out[0])
    }

    /// Decompress into `scratch`, decode the timestamps and leaves, and rebuild
    /// `InternalMessage`s with local tracking timestamps.
    ///
    /// Each timestamp is rebuilt from the live clock, so a `publish_t` that
    /// crossed hosts carries sub-millisecond rebuild noise;
    /// `ingestion_time().real()` and `tile_id` are exact.
    ///
    /// Errors when `type_hash` is not a version of `T`, when
    /// `metadata_type_hash` is not a version of `U`, or when any length or
    /// value check fails.
    pub fn decode<U: Versioned, T: Versioned>(&self, scratch: &mut Scratch) -> Decoded<U, T> {
        if !self.is::<T>() {
            return Err(DecodeError::UnknownTypeHash(self.header.type_hash));
        }
        let meta = self.user_metadata::<U>()?;
        // The header is untrusted: pin `decompressed_len` to what `n_messages`
        // of the writing version must occupy before allocating anything.
        let n = u64::from(self.header.n_messages);
        let leaf_size = T::version_size(self.header.type_hash)
            .ok_or(DecodeError::UnknownTypeHash(self.header.type_hash))?
            as u64;
        let ts_len = n * size_of::<TrackingTimestampWire>() as u64;
        let expected = ts_len + n * leaf_size;
        if self.header.decompressed_len != expected {
            return Err(DecodeError::LengthMismatch {
                expected: usize::try_from(expected).unwrap_or(usize::MAX),
                got: usize::try_from(self.header.decompressed_len).unwrap_or(usize::MAX),
            });
        }
        let expected_len = expected as usize;
        scratch.resize(expected_len);
        let written = zstd::bulk::decompress_to_buffer(self.compressed(), scratch.as_mut_bytes())
            .map_err(DecodeError::Zstd)?;
        if written != expected_len {
            return Err(DecodeError::LengthMismatch { expected: expected_len, got: written });
        }
        let bytes = scratch.as_bytes();
        let (ts_bytes, leaf_bytes) = bytes.split_at(ts_len as usize);
        let stamps = ref_timestamps(ts_bytes, n as usize)?;
        let leaves = T::decode_versions(self.header.type_hash, leaf_bytes)?;
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

fn ref_timestamps(bytes: &[u8], n: usize) -> Result<&[TrackingTimestampWire], DecodeError> {
    let stamps = byte_stable::cast_slice::<TrackingTimestampWire>(bytes).map_err(|e| match e {
        byte_stable::CastError::Unaligned => DecodeError::Unaligned,
        byte_stable::CastError::Length { .. } => DecodeError::LengthMismatch {
            expected: n * size_of::<TrackingTimestampWire>(),
            got: bytes.len(),
        },
        // Unreachable for a 24-byte type; `Invalid` cannot happen either
        // because every timestamp pattern is valid.
        byte_stable::CastError::Invalid { .. } | byte_stable::CastError::ZeroSized => {
            DecodeError::InvalidValue
        }
    })?;
    if stamps.len() != n {
        return Err(DecodeError::LengthMismatch {
            expected: n * size_of::<TrackingTimestampWire>(),
            got: bytes.len(),
        });
    }
    Ok(stamps)
}

/// Per-leaf-type byte buffers filled by [`BlobCache::push`].
struct TypedBuffer {
    name: &'static str,
    n_messages: u32,
    publish_t_first: Nanos,
    publish_t_last: Nanos,
    /// `TrackingTimestampWire` records, 24 bytes each.
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
        let mut push = Push {
            cache: &mut *self,
            timestamp: TrackingTimestampWire::from(msg.tracking_timestamp()),
        };
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
            let meta_bytes = ByteStable::as_bytes(user_meta);
            let meta_pad = meta_bytes.len().next_multiple_of(ALIGN);
            let mut plain = Vec::with_capacity(buf.timestamps.len() + buf.leaves.len());
            plain.extend_from_slice(&buf.timestamps);
            plain.extend_from_slice(&buf.leaves);
            let comp =
                zstd::bulk::compress(&plain, zstd_level).expect("zstd bulk compression failed");
            let comp_pad = comp.len().next_multiple_of(ALIGN);
            let header = BlobHeader {
                magic: MAGIC,
                version: FORMAT_VERSION,
                metadata_len: meta_bytes.len() as u32,
                n_messages: buf.n_messages,
                _reserved: 0,
                type_hash: key,
                metadata_type_hash: U::TYPE_HASH,
                compressed_len: comp.len() as u64,
                decompressed_len: plain.len() as u64,
                publish_t_first: buf.publish_t_first,
                publish_t_last: buf.publish_t_last,
                type_name: ArrayStr::from_str_truncate(buf.name),
            };
            // `resize` zero-fills, so the metadata and tail padding are zeros.
            build.resize(size_of::<BlobHeader>() + meta_pad + comp_pad);
            let (head, tail) = build.as_mut_bytes().split_at_mut(size_of::<BlobHeader>());
            head.copy_from_slice(ByteStable::as_bytes(&header));
            tail[..meta_bytes.len()].copy_from_slice(meta_bytes);
            tail[meta_pad..meta_pad + comp.len()].copy_from_slice(&comp);
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
    timestamp: TrackingTimestampWire,
}

impl VisitorVersionedLeaf for Push<'_> {
    fn visit_leaf<L: Versioned>(&mut self, leaf: &L) {
        // Sections start on 8-byte boundaries; a leaf needing more could never
        // be cast back on the reader.
        const { assert!(align_of::<L>() <= ALIGN) };
        let timestamp = self.timestamp;
        let buf = self.cache.buffers.entry(L::TYPE_HASH).or_insert_with(|| TypedBuffer {
            name: L::NAME,
            n_messages: 0,
            publish_t_first: Nanos(0),
            publish_t_last: Nanos(0),
            timestamps: Vec::new(),
            leaves: Vec::new(),
        });
        // Min/max rather than first/last pushed: one leaf type can arrive from
        // several queues, so push order is not publish order.
        let publish = timestamp.publish_t_real;
        if buf.n_messages == 0 {
            buf.publish_t_first = publish;
            buf.publish_t_last = publish;
        } else {
            buf.publish_t_first = buf.publish_t_first.min(publish);
            buf.publish_t_last = buf.publish_t_last.max(publish);
        }
        buf.timestamps.extend_from_slice(ByteStable::as_bytes(&timestamp));
        buf.leaves.extend_from_slice(ByteStable::as_bytes(leaf));
        buf.n_messages += 1;
    }
}
