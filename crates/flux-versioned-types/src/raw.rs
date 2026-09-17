//! One wire and disk format for batches of versioned leaves. The bytes of a
//! [`Blob`] are the format: send `as_bytes`, receive `from_bytes`, a file is
//! concatenated blobs.
//!
//! ```text
//! [BlobHeader 144 B][user metadata, padded to 8][zstd( [TrackingTimestampWire x n][Leaf x n] )]
//! ```
//!
//! `type_hash` is the plain `TYPE_HASH` of the version that wrote the blob (no
//! XOR). `version` pins the header, timestamp record, and zstd; bump it when
//! any of them change.

use std::collections::HashMap;

use byte_stable::ByteStable;
use flux_timing::{InternalMessage, Nanos};
use flux_utils::ArrayStr;

use crate::{
    blob::TrackingTimestampWire,
    leaves::{Decoded, HasVersionedLeaves, Versioned, VisitorVersionedLeaf},
};

pub const MAGIC: [u8; 8] = *b"FLUXBLOB";
pub const FORMAT_VERSION: u32 = 1;
pub const TYPE_NAME_LEN: usize = 64;
/// Leaves must not need more.
pub const ALIGN: usize = 8;

// The header is read by native reinterpretation; x86_64 layout is the format.
const _: () = assert!(cfg!(target_endian = "little") && size_of::<usize>() == 8);
const _: () = assert!(size_of::<ArrayStr<TYPE_NAME_LEN>>() == size_of::<usize>() + TYPE_NAME_LEN);

fn round8(n: u64) -> Option<u64> {
    n.checked_add(7).map(|m| m & !7)
}

#[derive(Clone, Copy, byte_stable_derive::ByteStable)]
#[repr(C)]
pub struct BlobHeader {
    pub magic: [u8; 8],
    pub version: u32,
    pub metadata_len: u32,
    pub n_messages: u32,
    _reserved: u32,
    pub type_hash: u64,
    pub metadata_type_hash: u64,
    pub compressed_len: u64,
    /// `n_messages * (24 + size_of leaf)` for the version that wrote the blob.
    pub decompressed_len: u64,
    /// Min/max publish time in the batch. Advisory, not validated.
    pub publish_t_first: Nanos,
    pub publish_t_last: Nanos,
    pub type_name: ArrayStr<TYPE_NAME_LEN>,
}

const _: () = assert!(size_of::<BlobHeader>() == 144 && align_of::<BlobHeader>() == ALIGN);

#[repr(C)]
pub struct Blob {
    pub header: BlobHeader,
    /// Words, not bytes, so the type has no trailing padding at any length.
    tail: [u64],
}

#[derive(Debug)]
pub enum DecodeError {
    TooShort { needed: usize, got: usize },
    Unaligned,
    BadMagic,
    UnsupportedVersion(u32),
    BadTypeName,
    UnknownTypeHash(u64),
    LengthMismatch { expected: usize, got: usize },
    InvalidValue,
    Zstd(std::io::Error),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for DecodeError {}

/// 8-byte aligned byte buffer.
#[derive(Default)]
pub struct Scratch {
    words: Vec<u64>,
    len: usize,
}

impl Scratch {
    pub fn new() -> Self {
        Self::default()
    }

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

    /// For buffers that are not 8-aligned, such as `DiskIo` reads.
    pub fn load(&mut self, bytes: &[u8]) -> Result<&Blob, DecodeError> {
        self.resize(bytes.len());
        self.as_mut_bytes().copy_from_slice(bytes);
        Blob::from_bytes(self.as_bytes())
    }
}

impl Blob {
    /// Borrows the first blob in `bytes`; `bytes` must be 8-aligned.
    pub fn from_bytes(bytes: &[u8]) -> Result<&Self, DecodeError> {
        if !(bytes.as_ptr() as usize).is_multiple_of(ALIGN) {
            return Err(DecodeError::Unaligned);
        }
        let header_len = size_of::<BlobHeader>();
        if bytes.len() < header_len {
            return Err(DecodeError::TooShort { needed: header_len, got: bytes.len() });
        }
        // Magic before the cast so legacy frames are `BadMagic`, not `BadTypeName`.
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
        if !BlobHeader::is_valid(&bytes[..header_len]) {
            return Err(DecodeError::BadTypeName);
        }
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
        #[allow(clippy::cast_ptr_alignment)]
        let ptr =
            core::ptr::slice_from_raw_parts(bytes.as_ptr().cast::<u64>(), words) as *const Self;
        // Safety: 8-aligned, `need` bytes in bounds, header validated, `u64` tail
        // accepts any pattern; the slice length is the tail word count.
        Ok(unsafe { &*ptr })
    }

    pub fn as_bytes(&self) -> &[u8] {
        // Safety: header is `ByteStable` and the tail is `u64`s, so every byte
        // is initialized and nothing has interior mutability.
        unsafe {
            core::slice::from_raw_parts(
                core::ptr::from_ref(self).cast::<u8>(),
                size_of::<BlobHeader>() + self.tail.len() * ALIGN,
            )
        }
    }

    pub fn is<T: Versioned>(&self) -> bool {
        T::VERSION_HASHES.contains(&self.header.type_hash)
    }

    pub fn type_name(&self) -> &str {
        self.header.type_name.as_str()
    }

    pub fn user_metadata_bytes(&self) -> &[u8] {
        &byte_stable::slice_as_bytes(&self.tail)[..self.header.metadata_len as usize]
    }

    pub fn compressed(&self) -> &[u8] {
        let meta_pad = (self.header.metadata_len as usize).next_multiple_of(ALIGN);
        let len = self.header.compressed_len as usize;
        &byte_stable::slice_as_bytes(&self.tail)[meta_pad..meta_pad + len]
    }

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

    /// Rebuilt `publish_t` carries sub-millisecond clock noise across hosts;
    /// `ingestion_time().real()` and `tile_id` are exact.
    pub fn decode<U: Versioned, T: Versioned>(&self, scratch: &mut Scratch) -> Decoded<U, T> {
        if !self.is::<T>() {
            return Err(DecodeError::UnknownTypeHash(self.header.type_hash));
        }
        let meta = self.user_metadata::<U>()?;
        // Untrusted header: pin `decompressed_len` before allocating.
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

struct TypedBuffer {
    type_hash: u64,
    n_messages: u32,
    publish_t_first: Nanos,
    publish_t_last: Nanos,
    timestamps: Vec<u8>,
    leaves: Vec<u8>,
}

/// Buffers messages per leaf type; [`flush`](Self::flush) emits one [`Blob`]
/// per type.
#[derive(Default)]
pub struct BlobCache {
    buffers: HashMap<&'static str, TypedBuffer>,
    build: Scratch,
}

impl BlobCache {
    pub fn new() -> Self {
        Self::default()
    }

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

    pub fn n_messages(&self) -> usize {
        self.buffers.values().map(|buf| buf.n_messages as usize).sum()
    }

    pub fn n_blobs(&self) -> usize {
        self.buffers.values().filter(|buf| buf.n_messages > 0).count()
    }

    /// The `&Blob` is valid only inside `sink`.
    pub fn flush<U: Versioned>(
        &mut self,
        user_meta: &U,
        zstd_level: i32,
        mut sink: impl FnMut(&Blob),
    ) {
        let mut keys: Vec<&'static str> = self
            .buffers
            .iter()
            .filter(|(_, buf)| buf.n_messages > 0)
            .map(|(key, _)| *key)
            .collect();
        keys.sort_unstable();
        let Self { buffers, build } = self;
        for name in keys {
            let buf = &buffers[name];
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
                type_hash: buf.type_hash,
                metadata_type_hash: U::TYPE_HASH,
                compressed_len: comp.len() as u64,
                decompressed_len: plain.len() as u64,
                publish_t_first: buf.publish_t_first,
                publish_t_last: buf.publish_t_last,
                type_name: ArrayStr::from_str_truncate(name),
            };
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
    fn visit_leaf<L: Versioned>(&mut self, name: &'static str, leaf: &L) {
        const { assert!(align_of::<L>() <= ALIGN) };
        let timestamp = self.timestamp;
        let buf = self.cache.buffers.entry(name).or_insert_with(|| TypedBuffer {
            type_hash: L::TYPE_HASH,
            n_messages: 0,
            publish_t_first: Nanos(0),
            publish_t_last: Nanos(0),
            timestamps: Vec::new(),
            leaves: Vec::new(),
        });
        // Min/max: one leaf type can arrive from several queues.
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
