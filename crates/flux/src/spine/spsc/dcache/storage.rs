//! Fixed payload regions owned by SPSC metadata slots.
//!
//! The core queue's producer and consumer cursors govern access to these
//! regions. A producer may write a region only while its metadata slot is free;
//! a consumer may read it only while it holds that slot. Its release/acquire
//! publication orders payload bytes. Neither a region nor a borrowed slice may
//! outlive the corresponding slot ownership.
//!
//! Each region uses the metadata slot's index, derived from the core producer's
//! next sequence number. The core cursor also governs endpoint replacement,
//! messages without payloads, and sequence rollover.

use std::{
    alloc::Layout,
    fmt,
    mem::{align_of, size_of},
    path::Path,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use flux_utils::{DCache, DCacheError, DCachePtr, DCacheRef};
use shared_memory::{Shmem, ShmemConf, ShmemError};
use thiserror::Error;

const CACHELINE: usize = 64;
const MAGIC: u64 = u64::from_le_bytes(*b"FXSDCH02");

#[derive(Debug, Error)]
pub(super) enum StorageError {
    #[error("SPSC DCache capacity, MTU, or allocation size is invalid")]
    InvalidConfiguration,
    #[error("SPSC DCache is not initialized; retry after the creator finishes")]
    Uninitialized,
    #[error("incompatible SPSC DCache shared-memory layout, capacity, or MTU")]
    IncompatibleLayout,
    #[error(transparent)]
    SharedMemory(#[from] ShmemError),
}

#[repr(C, align(64))]
struct Header {
    ready: AtomicU64,
    capacity: usize,
    mtu: usize,
}

struct Shape {
    layout: Layout,
    dcache_offset: usize,
    data_capacity: usize,
    stride: usize,
}

fn shape(capacity: usize, mtu: usize) -> Result<Shape, StorageError> {
    if !capacity.is_power_of_two() || mtu == 0 {
        return Err(StorageError::InvalidConfiguration);
    }
    let stride = mtu
        .checked_add(CACHELINE - 1)
        .map(|n| n & !(CACHELINE - 1))
        .ok_or(StorageError::InvalidConfiguration)?;
    let occupied = capacity.checked_mul(stride).ok_or(StorageError::InvalidConfiguration)?;
    let data_capacity =
        occupied.checked_next_power_of_two().ok_or(StorageError::InvalidConfiguration)?;
    let dcache_layout = Layout::from_size_align(
        CACHELINE.checked_add(data_capacity).ok_or(StorageError::InvalidConfiguration)?,
        CACHELINE,
    )
    .map_err(|_| StorageError::InvalidConfiguration)?;
    let (layout, dcache_offset) = Layout::new::<Header>()
        .extend(dcache_layout)
        .map_err(|_| StorageError::InvalidConfiguration)?;
    Ok(Shape { layout: layout.pad_to_align(), dcache_offset, data_capacity, stride })
}

enum Backing {
    Heap { _cache: Arc<DCache> },
    Shared { _mapping: Shmem },
}

struct Inner {
    cache: DCachePtr,
    _backing: Backing,
    capacity: usize,
    mtu: usize,
    stride: usize,
}

// SAFETY: the cached DCache pointer addresses the allocation retained by
// `_backing`. Only an attached producer may write a region and only a consumer
// holding the matching metadata slot may read it. The core queue's cursor
// publication orders these accesses. Shmem may be unmapped on any thread
// after the last Arc handle is dropped.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for Inner {}
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Sync for Inner {}

#[derive(Clone)]
pub(super) struct Storage(Arc<Inner>);

impl Storage {
    /// Allocate one fixed payload region per metadata queue slot.
    ///
    /// `capacity` is the already rounded core queue capacity.
    pub(super) fn new(capacity: usize, mtu: usize) -> Self {
        let shape = shape(capacity, mtu).expect("invalid SPSC DCache configuration");
        let cache = DCache::new(shape.data_capacity);
        // SAFETY: the Arc retains the allocation for every clone of Storage.
        let ptr = unsafe { DCachePtr::from_raw(Arc::as_ptr(&cache)) };
        Self(Arc::new(Inner {
            cache: ptr,
            _backing: Backing::Heap { _cache: cache },
            capacity,
            mtu,
            stride: shape.stride,
        }))
    }

    /// Create or open persistent storage without changing an existing arena.
    /// The parent directory must already exist. A concurrent creator may leave
    /// an opening process observing `Uninitialized` until it publishes MAGIC.
    ///
    /// # Safety
    /// All participants must agree on the architecture and application schema
    /// and access payloads solely under the matching core SPSC queue's slot
    /// ownership protocol. Inherited handles must not be used after `fork`.
    pub(super) unsafe fn create_or_open_shared(
        path: impl AsRef<Path>,
        capacity: usize,
        mtu: usize,
    ) -> Result<Self, StorageError> {
        let shape = shape(capacity, mtu)?;
        match ShmemConf::new().size(shape.layout.size()).flink(path.as_ref()).create() {
            Ok(mut mapping) => {
                let ptr = NonNull::new(mapping.as_ptr()).ok_or(StorageError::IncompatibleLayout)?;
                if !(ptr.as_ptr() as usize).is_multiple_of(shape.layout.align()) ||
                    mapping.len() != shape.layout.size()
                {
                    return Err(StorageError::IncompatibleLayout);
                }
                // A new shared-memory object is zero-filled by the OS. Leave
                // ready at zero while initializing the immutable header fields.
                let header = ptr.cast::<Header>();
                // SAFETY: only the creator writes these fields, and openers
                // read them after the ready flag's Release/Acquire handshake.
                unsafe {
                    (&raw mut (*header.as_ptr()).capacity).write(capacity);
                    (&raw mut (*header.as_ptr()).mtu).write(mtu);
                }
                // SAFETY: the checked layout reserves the full aligned DCache
                // prefix and data region, and the mapping remains owned below.
                let cache_ptr = unsafe { ptr.as_ptr().add(shape.dcache_offset) };
                let raw_cache = DCache::from_ptr(cache_ptr, shape.data_capacity);
                // SAFETY: the pointer remains valid through mapping ownership.
                let cache = unsafe { DCachePtr::from_raw(raw_cache) };
                // SAFETY: Header is aligned and initialized above.
                unsafe { header.as_ref().ready.store(MAGIC, Ordering::Release) };
                mapping.set_owner(false);
                Ok(Self(Arc::new(Inner {
                    cache,
                    _backing: Backing::Shared { _mapping: mapping },
                    capacity,
                    mtu,
                    stride: shape.stride,
                })))
            }
            Err(ShmemError::LinkExists) => {
                // SAFETY: inherited from this function's shared-memory contract.
                unsafe { Self::open_shared(path, capacity, mtu, &shape) }
            }
            Err(error) => Err(error.into()),
        }
    }

    unsafe fn open_shared(
        path: impl AsRef<Path>,
        capacity: usize,
        mtu: usize,
        shape: &Shape,
    ) -> Result<Self, StorageError> {
        let mapping = ShmemConf::new().flink(path).open()?;
        let ptr = NonNull::new(mapping.as_ptr()).ok_or(StorageError::IncompatibleLayout)?;
        if mapping.len() < size_of::<Header>() ||
            !(ptr.as_ptr() as usize).is_multiple_of(align_of::<Header>())
        {
            return Err(StorageError::IncompatibleLayout);
        }
        let header = ptr.cast::<Header>();
        // SAFETY: bounds and alignment permit borrowing ready alone. The
        // creator may still be writing the other, non-atomic header fields.
        let ready = unsafe { &(*header.as_ptr()).ready };
        match ready.load(Ordering::Acquire) {
            0 => return Err(StorageError::Uninitialized),
            MAGIC => {}
            _ => return Err(StorageError::IncompatibleLayout),
        }
        // SAFETY: acquiring MAGIC observes the creator's header writes.
        let stored = unsafe { header.as_ref() };
        if stored.capacity != capacity ||
            stored.mtu != mtu ||
            mapping.len() != shape.layout.size() ||
            !(ptr.as_ptr() as usize).is_multiple_of(shape.layout.align())
        {
            return Err(StorageError::IncompatibleLayout);
        }
        // SAFETY: the validated mapping spans the DCache prefix and data.
        let cache_ptr = unsafe { ptr.as_ptr().add(shape.dcache_offset) };
        let raw_cache = DCache::from_ptr(cache_ptr, shape.data_capacity);
        // SAFETY: the pointer remains valid through mapping ownership.
        let cache = unsafe { DCachePtr::from_raw(raw_cache) };
        Ok(Self(Arc::new(Inner {
            cache,
            _backing: Backing::Shared { _mapping: mapping },
            capacity,
            mtu,
            stride: shape.stride,
        })))
    }

    pub(super) fn mtu(&self) -> usize {
        self.0.mtu
    }

    pub(super) fn capacity(&self) -> usize {
        self.0.capacity
    }

    pub(super) fn validate_len(&self, len: usize) -> Result<(), DCacheError> {
        if len == 0 {
            Err(DCacheError::ReserveZero)
        } else if len > self.mtu() {
            Err(DCacheError::DataLenExceedsCapacity(len, self.mtu()))
        } else {
            Ok(())
        }
    }

    /// Write a free metadata slot's fixed payload region.
    ///
    /// # Safety
    /// The caller must own the sole producer role and a free metadata slot
    /// corresponding to `slot`. It must publish that slot only after this
    /// function returns, and must not concurrently write the same region.
    pub(super) unsafe fn write<R>(
        &self,
        slot: usize,
        len: usize,
        f: impl FnOnce(&mut [u8]) -> R,
    ) -> Result<(DCacheRef, R), DCacheError> {
        self.validate_len(len)?;
        if slot >= self.capacity() {
            return Err(DCacheError::InvalidWriteIntoOffset(slot, self.capacity()));
        }
        let dref = DCacheRef { offset: slot * self.0.stride, len };
        let result = self.0.cache.write_into(dref, 0, f)?;
        Ok((dref, result))
    }

    /// Read a payload while its metadata queue slot remains held.
    ///
    /// # Safety
    /// The caller must hold the corresponding metadata slot for the entire
    /// callback, and no producer may reuse its region during that callback.
    pub(super) unsafe fn read<R>(
        &self,
        dref: DCacheRef,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, DCacheError> {
        self.validate_len(dref.len)?;
        let occupied = self.capacity() * self.0.stride;
        if dref.offset >= occupied || !dref.offset.is_multiple_of(self.0.stride) {
            return Err(DCacheError::InvalidWriteIntoOffset(dref.offset, occupied));
        }
        self.0.cache.map(dref, f)
    }
}

impl fmt::Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Storage")
            .field("capacity", &self.capacity())
            .field("mtu", &self.mtu())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_configuration() {
        for (capacity, mtu) in [(0, 1), (3, 1), (2, 0), (2, usize::MAX)] {
            assert!(shape(capacity, mtu).is_err());
        }
        assert!(shape(usize::MAX / 2 + 1, 64).is_err());
    }

    #[test]
    fn shared_reopen_preserves_payload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("arena");
        // SAFETY: this test exclusively owns the matching arena and slot.
        let created = unsafe { Storage::create_or_open_shared(&path, 4, 65) }.unwrap();
        let (dref, ()) =
            unsafe { created.write(2, 5, |bytes| bytes.copy_from_slice(b"hello")) }.unwrap();
        drop(created);

        // SAFETY: this test retains exclusive ownership of the same slot.
        let opened = unsafe { Storage::create_or_open_shared(&path, 4, 65) }.unwrap();
        assert_eq!(unsafe { opened.read(dref, <[u8]>::to_vec) }.unwrap(), b"hello");
        assert!(matches!(
            unsafe { Storage::create_or_open_shared(&path, 4, 64) },
            Err(StorageError::IncompatibleLayout)
        ));
        assert!(matches!(
            unsafe { Storage::create_or_open_shared(&path, 8, 65) },
            Err(StorageError::IncompatibleLayout)
        ));
        drop(opened);
        crate::communication::cleanup_flink(&path).unwrap();
    }

    #[test]
    fn persisted_index_arena_format_is_rejected_without_modification() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("arena");
        let capacity = 4;
        let mtu = 65;
        let layout = shape(capacity, mtu).unwrap();
        let mapping = ShmemConf::new().flink(&path).size(layout.layout.size()).create().unwrap();
        let header = NonNull::new(mapping.as_ptr()).unwrap().cast::<Header>().as_ptr();
        let old_magic = u64::from_le_bytes(*b"FXSDCH01");
        // Model the former format's header prefix in otherwise valid storage.
        // Its header was also 64 bytes, so size checks alone cannot reject it.
        // SAFETY: this test exclusively owns the aligned, zero-filled mapping;
        // no endpoints exist and only header validation will access it.
        unsafe {
            (&raw mut (*header).capacity).write(capacity);
            (&raw mut (*header).mtu).write(mtu);
            (*header).ready.store(old_magic, Ordering::Release);
            assert!(matches!(
                Storage::create_or_open_shared(&path, capacity, mtu),
                Err(StorageError::IncompatibleLayout)
            ));
            assert_eq!((*header).ready.load(Ordering::Acquire), old_magic);
        }
    }

    #[test]
    fn malformed_refs_do_not_reach_dcache_mapping() {
        let storage = Storage::new(2, 65);
        for dref in
            [DCacheRef { offset: 1, len: 1 }, DCacheRef { offset: 2 * 128, len: 1 }, DCacheRef {
                offset: 0,
                len: 66,
            }]
        {
            assert!(unsafe { storage.read(dref, |_| ()) }.is_err());
        }
    }
}
