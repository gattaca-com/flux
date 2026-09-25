//! Bounded SPSC queues for threads and shared memory, inspired by Seastar.
//!
//! ```
//! use flux_communication::queue::spsc::Queue;
//!
//! let queue = Queue::<_>::new(16);
//! let mut producer = queue.try_producer().unwrap();
//! let mut consumer = queue.try_consumer().unwrap();
//! producer.produce(&42).unwrap();
//! let mut value = 0;
//! consumer.try_consume(&mut value).unwrap();
//! assert_eq!(value, 42);
//! ```
//!
//! Shared mappings persist; call [`crate::cleanup_flink`] after all users
//! detach. Crashed or forgotten endpoints retain their claims: detach all users
//! and recreate the mapping to recover.
//!
//! Keep file links outside Flux's queue-discovery directories: this layout is
//! incompatible with [`super::Queue`].

use std::{
    alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error},
    cell::UnsafeCell,
    marker::PhantomData,
    mem::{align_of, size_of},
    path::Path,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use shared_memory::{Shmem, ShmemConf, ShmemError};
use thiserror::Error;

use crate::EmptyError;

const MAGIC: u64 = u64::from_le_bytes(*b"FXSPSC02");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("SPSC queue is full")]
pub struct FullError;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("SPSC capacity must be nonzero and its allocation must fit in isize")]
    InvalidCapacity,
    #[error("SPSC producer is already attached")]
    ProducerAttached,
    #[error("SPSC consumer is already attached")]
    ConsumerAttached,
    #[error("SPSC queue is not initialized; retry after the creator finishes")]
    Uninitialized,
    #[error("Incompatible SPSC shared-memory layout, element layout, or capacity")]
    IncompatibleLayout,
    #[error(transparent)]
    SharedMemory(#[from] ShmemError),
}

#[repr(C, align(128))]
struct Cursor(AtomicUsize);

#[repr(C, align(128))]
struct Header {
    ready: AtomicU64,
    capacity: usize,
    element_size: usize,
    element_align: usize,
    payload_size: usize,
    payload_align: usize,
    // Endpoint drop (Release) → replacement claim (Acquire).
    producer_claimed: AtomicBool,
    consumer_claimed: AtomicBool,
    // write: producer Release → consumer Acquire (payload published).
    // read: consumer Release → producer Acquire (slot reusable).
    write: Cursor,
    read: Cursor,
}

const fn slot_size<T, const SLOT_SIZE: usize>() -> usize {
    const {
        let size = if SLOT_SIZE == 0 { size_of::<T>() } else { SLOT_SIZE };
        assert!(size >= size_of::<T>(), "SPSC slot is smaller than its payload");
        size
    }
}

const fn slot_align<T, const SLOT_SIZE: usize>() -> usize {
    const {
        let align = if SLOT_SIZE == 0 {
            align_of::<T>()
        } else {
            1usize << slot_size::<T, SLOT_SIZE>().trailing_zeros()
        };
        assert!(
            align.is_multiple_of(align_of::<T>()),
            "SPSC slot alignment is not a multiple of payload alignment"
        );
        align
    }
}

fn layout<T, const SLOT_SIZE: usize>(capacity: usize) -> Result<(Layout, usize), QueueError> {
    let stride = slot_size::<T, SLOT_SIZE>();
    if !capacity.is_power_of_two() || capacity > isize::MAX as usize {
        return Err(QueueError::InvalidCapacity);
    }
    let bytes = stride.checked_mul(capacity).ok_or(QueueError::InvalidCapacity)?;
    let slots = Layout::from_size_align(bytes, slot_align::<T, SLOT_SIZE>())
        .map_err(|_| QueueError::InvalidCapacity)?;
    let (layout, offset) =
        Layout::new::<Header>().extend(slots).map_err(|_| QueueError::InvalidCapacity)?;
    Ok((layout.pad_to_align(), offset))
}

fn rounded_capacity(len: usize) -> Result<usize, QueueError> {
    if len == 0 {
        return Err(QueueError::InvalidCapacity);
    }
    len.checked_next_power_of_two().ok_or(QueueError::InvalidCapacity)
}

struct Storage<T, const SLOT_SIZE: usize> {
    ptr: NonNull<u8>,
    layout: Layout,
    slots_offset: usize,
    capacity: usize,
    shared: Option<Shmem>,
    // Invariant in T: aliases cannot shorten stored payload lifetimes.
    _value: PhantomData<UnsafeCell<T>>,
}

// SAFETY: unique role claims and release/acquire cursors transfer exclusive
// slot access; references cannot escape. Once shared, Shmem is immutable and
// may unmap on any thread.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl<T: Send, const SLOT_SIZE: usize> Send for Storage<T, SLOT_SIZE> {}
unsafe impl<T: Send, const SLOT_SIZE: usize> Sync for Storage<T, SLOT_SIZE> {}

impl<T, const SLOT_SIZE: usize> Storage<T, SLOT_SIZE> {
    fn header(&self) -> &Header {
        unsafe { self.ptr.cast::<Header>().as_ref() }
    }

    fn initialize(&mut self) {
        let header = self.ptr.cast::<Header>().as_ptr();
        // Never write the whole Header: openers may already be polling ready.
        unsafe {
            (&raw mut (*header).capacity).write(self.capacity);
            (&raw mut (*header).element_size).write(slot_size::<T, SLOT_SIZE>());
            (&raw mut (*header).element_align).write(slot_align::<T, SLOT_SIZE>());
            (&raw mut (*header).payload_size).write(size_of::<T>());
            (&raw mut (*header).payload_align).write(align_of::<T>());
        }
        self.header().ready.store(MAGIC, Ordering::Release);
    }
}

impl<T, const SLOT_SIZE: usize> Drop for Storage<T, SLOT_SIZE> {
    fn drop(&mut self) {
        if self.shared.is_none() {
            unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
        }
    }
}

struct EndpointStorage<T, const SLOT_SIZE: usize> {
    storage: Arc<Storage<T, SLOT_SIZE>>,
    slots: *mut u8,
    mask: usize,
    read: *const AtomicUsize,
    write: *const AtomicUsize,
}

// SAFETY: storage keeps pointers valid and T invariant; its slot protocol
// applies. Payload access requires &mut self, so sharing an endpoint cannot
// access slots.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl<T: Send, const SLOT_SIZE: usize> Send for EndpointStorage<T, SLOT_SIZE> {}
unsafe impl<T: Send, const SLOT_SIZE: usize> Sync for EndpointStorage<T, SLOT_SIZE> {}

impl<T, const SLOT_SIZE: usize> EndpointStorage<T, SLOT_SIZE> {
    fn new(storage: Arc<Storage<T, SLOT_SIZE>>) -> Self {
        let header = storage.header();
        // SAFETY: layout provides aligned slots (one-past for zero-sized slots).
        let slots = unsafe { storage.ptr.as_ptr().add(storage.slots_offset) };
        Self {
            slots,
            mask: storage.capacity - 1,
            read: &raw const header.read.0,
            write: &raw const header.write.0,
            storage,
        }
    }

    fn slot(&self, position: usize) -> *mut T {
        // SAFETY: masked index is in bounds; layout ensures aligned slots and
        // an allocation within isize::MAX, preventing offset overflow.
        unsafe { self.slots.add((position & self.mask) * slot_size::<T, SLOT_SIZE>()).cast::<T>() }
    }

    fn read(&self) -> &AtomicUsize {
        unsafe { &*self.read }
    }

    fn write(&self) -> &AtomicUsize {
        unsafe { &*self.write }
    }
}

/// | `SLOT_SIZE` | Stride | Slot alignment |
/// |---|---|---|
/// | `0` (default) | `size_of::<T>()` | `align_of::<T>()` |
/// | Nonzero | `SLOT_SIZE` | Largest power-of-two divisor (48 → 16, 256 → 256) |
///
/// ```
/// use flux_communication::queue::spsc::Queue;
/// let queue = Queue::<u64, 64>::new(16);
/// queue.try_producer().unwrap().produce(&42).unwrap();
/// queue.try_consumer().unwrap().consume_ref(|value| assert_eq!(*value, 42));
/// // Non-power-of-two strides are valid too.
/// let _ = Queue::<u64, 24>::new(4);
/// ```
///
/// Slot assertions require a build; `cargo check` may not evaluate them.
///
/// ```compile_fail,E0080
/// use flux_communication::queue::spsc::Queue;
/// let _ = Queue::<u64, 4>::new(4);
/// ```
///
/// ```compile_fail,E0080
/// use flux_communication::queue::spsc::Queue;
/// let _ = Queue::<u64, 12>::new(4);
/// ```
///
/// ```compile_fail,E0597
/// use flux_communication::queue::spsc::Queue;
/// fn inject<'a>(queue: &Queue<&'a str>, value: &'a str) {
///     queue.try_producer().unwrap().produce(&value).unwrap();
/// }
/// let queue = Queue::<&'static str>::new(1);
/// {
///     let local = String::from("temporary");
///     inject(&queue, &local);
/// }
/// queue.try_consumer().unwrap().consume(|value| println!("{value}"));
/// ```
pub struct Queue<T: Copy, const SLOT_SIZE: usize = 0> {
    storage: Arc<Storage<T, SLOT_SIZE>>,
}

impl<T: Copy, const SLOT_SIZE: usize> Clone for Queue<T, SLOT_SIZE> {
    fn clone(&self) -> Self {
        Self { storage: Arc::clone(&self.storage) }
    }
}

impl<T: Copy, const SLOT_SIZE: usize> Queue<T, SLOT_SIZE> {
    pub const SLOT_SIZE: usize = slot_size::<T, SLOT_SIZE>();

    pub const SLOT_ALIGN: usize = slot_align::<T, SLOT_SIZE>();

    pub fn new(len: usize) -> Self {
        let capacity = rounded_capacity(len).expect("invalid SPSC capacity");
        let (layout, slots_offset) =
            layout::<T, SLOT_SIZE>(capacity).expect("invalid SPSC allocation");
        let ptr = NonNull::new(unsafe { alloc_zeroed(layout) })
            .unwrap_or_else(|| handle_alloc_error(layout));
        let mut storage =
            Storage { ptr, layout, slots_offset, capacity, shared: None, _value: PhantomData };
        storage.initialize();
        Self { storage: Arc::new(storage) }
    }

    /// Alignment above the page size may fail with
    /// [`QueueError::IncompatibleLayout`].
    ///
    /// # Safety
    /// All participants must use this implementation with identical `T`, slot
    /// layout, architecture and schema. Values must be process-independent
    /// (no local pointers/references). Access the mapping only through this
    /// API. After `fork`, the child must neither use nor drop inherited
    /// endpoints.
    pub unsafe fn create_or_open_shared(
        path: impl AsRef<Path>,
        len: usize,
    ) -> Result<Self, QueueError> {
        let capacity = rounded_capacity(len)?;
        let (layout, slots_offset) = layout::<T, SLOT_SIZE>(capacity)?;
        match ShmemConf::new().size(layout.size()).flink(path.as_ref()).create() {
            Ok(shared) => {
                let ptr = NonNull::new(shared.as_ptr()).ok_or(QueueError::IncompatibleLayout)?;
                if !(ptr.as_ptr() as usize).is_multiple_of(layout.align()) {
                    return Err(QueueError::IncompatibleLayout);
                }
                let mut storage = Storage {
                    ptr,
                    layout,
                    slots_offset,
                    capacity,
                    shared: Some(shared),
                    _value: PhantomData,
                };
                storage.initialize();
                storage.shared.as_mut().unwrap().set_owner(false);
                Ok(Self { storage: Arc::new(storage) })
            }
            Err(ShmemError::LinkExists) => {
                let queue = unsafe { Self::open_shared(path) }?;
                if queue.capacity() != capacity {
                    return Err(QueueError::IncompatibleLayout);
                }
                Ok(queue)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// # Safety
    /// The shared-memory contract of [`Self::create_or_open_shared`] applies.
    pub unsafe fn open_shared(path: impl AsRef<Path>) -> Result<Self, QueueError> {
        let shared = ShmemConf::new().flink(path).open()?;
        let ptr = NonNull::new(shared.as_ptr()).ok_or(QueueError::IncompatibleLayout)?;
        if shared.len() < size_of::<Header>() ||
            !(ptr.as_ptr() as usize).is_multiple_of(align_of::<Header>())
        {
            return Err(QueueError::IncompatibleLayout);
        }
        let header_ptr = ptr.cast::<Header>().as_ptr();
        // SAFETY: size/alignment checked; only ready may be read before publication.
        let ready = unsafe { &(*header_ptr).ready };
        match ready.load(Ordering::Acquire) {
            0 => return Err(QueueError::Uninitialized),
            MAGIC => {}
            _ => return Err(QueueError::IncompatibleLayout),
        }
        let header = unsafe { &*header_ptr };
        if header.element_size != slot_size::<T, SLOT_SIZE>() ||
            header.element_align != slot_align::<T, SLOT_SIZE>() ||
            header.payload_size != size_of::<T>() ||
            header.payload_align != align_of::<T>()
        {
            return Err(QueueError::IncompatibleLayout);
        }
        let capacity = header.capacity;
        let (layout, slots_offset) = layout::<T, SLOT_SIZE>(capacity)?;
        if shared.len() < layout.size() || !(ptr.as_ptr() as usize).is_multiple_of(layout.align()) {
            return Err(QueueError::IncompatibleLayout);
        }
        Ok(Self {
            storage: Arc::new(Storage {
                ptr,
                layout,
                slots_offset,
                capacity,
                shared: Some(shared),
                _value: PhantomData,
            }),
        })
    }

    pub fn capacity(&self) -> usize {
        self.storage.capacity
    }

    pub fn try_producer(&self) -> Result<Producer<T, SLOT_SIZE>, QueueError> {
        let header = self.storage.header();
        header
            .producer_claimed
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .map_err(|_| QueueError::ProducerAttached)?;
        Ok(Producer {
            storage: EndpointStorage::new(Arc::clone(&self.storage)),
            write: header.write.0.load(Ordering::Relaxed),
            cached_read: header.read.0.load(Ordering::Acquire),
        })
    }

    pub fn try_consumer(&self) -> Result<Consumer<T, SLOT_SIZE>, QueueError> {
        let header = self.storage.header();
        header
            .consumer_claimed
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .map_err(|_| QueueError::ConsumerAttached)?;
        Ok(Consumer {
            storage: EndpointStorage::new(Arc::clone(&self.storage)),
            read: header.read.0.load(Ordering::Relaxed),
            cached_write: header.write.0.load(Ordering::Acquire),
        })
    }
}

pub struct Producer<T: Copy, const SLOT_SIZE: usize = 0> {
    storage: EndpointStorage<T, SLOT_SIZE>,
    write: usize,
    cached_read: usize,
}

impl<T: Copy, const SLOT_SIZE: usize> Producer<T, SLOT_SIZE> {
    #[inline]
    pub fn next_sequence(&self) -> usize {
        self.write
    }

    /// Returns the wrapping, zero-based sequence number.
    #[inline]
    pub fn produce(&mut self, msg: &T) -> Result<usize, FullError> {
        self.produce_with(|| *msg)
    }

    /// Closure `f` blocks the write cursor.
    #[inline]
    pub fn produce_with(&mut self, f: impl FnOnce() -> T) -> Result<usize, FullError> {
        if self.write.wrapping_sub(self.cached_read) == self.storage.mask + 1 {
            self.cached_read = self.storage.read().load(Ordering::Acquire);
            if self.write.wrapping_sub(self.cached_read) == self.storage.mask + 1 {
                return Err(FullError);
            }
        }
        let position = self.write;
        unsafe { self.storage.slot(position).write(f()) };
        self.write = position.wrapping_add(1);
        self.storage.write().store(self.write, Ordering::Release);
        Ok(position)
    }

    pub fn max_writable_msgs_without_speeding_past(&self) -> usize {
        let read = self.storage.read().load(Ordering::Acquire);
        self.storage.mask + 1 - self.write.wrapping_sub(read)
    }
}

impl<T: Copy, const SLOT_SIZE: usize> Drop for Producer<T, SLOT_SIZE> {
    fn drop(&mut self) {
        self.storage.storage.header().producer_claimed.store(false, Ordering::Release);
    }
}

pub struct Consumer<T: Copy, const SLOT_SIZE: usize = 0> {
    storage: EndpointStorage<T, SLOT_SIZE>,
    read: usize,
    cached_write: usize,
}

struct SlotRelease<'a> {
    position: &'a mut usize,
    read: &'a AtomicUsize,
}

impl Drop for SlotRelease<'_> {
    #[inline]
    fn drop(&mut self) {
        *self.position = self.position.wrapping_add(1);
        self.read.store(*self.position, Ordering::Release);
    }
}

impl<T: Copy, const SLOT_SIZE: usize> Consumer<T, SLOT_SIZE> {
    #[inline]
    fn pop(&mut self) -> Option<T> {
        if self.read == self.cached_write {
            self.cached_write = self.storage.write().load(Ordering::Acquire);
            if self.read == self.cached_write {
                return None;
            }
        }
        let value = unsafe { self.storage.slot(self.read).read() };
        self.read = self.read.wrapping_add(1);
        self.storage.read().store(self.read, Ordering::Release);
        Some(value)
    }

    #[inline]
    pub fn try_consume(&mut self, value: &mut T) -> Result<(), EmptyError> {
        *value = self.pop().ok_or(EmptyError::Empty)?;
        Ok(())
    }

    #[inline]
    pub fn consume(&mut self, mut f: impl FnMut(&mut T)) -> bool {
        let Some(mut value) = self.pop() else { return false };
        f(&mut value);
        true
    }

    /// Closure `f` blocks the read cursor.
    #[inline]
    pub fn consume_ref(&mut self, f: impl FnOnce(&T)) -> bool {
        if self.read == self.cached_write {
            self.cached_write = self.storage.write().load(Ordering::Acquire);
            if self.read == self.cached_write {
                return false;
            }
        }
        let slot = self.storage.slot(self.read);
        let release = SlotRelease { position: &mut self.read, read: self.storage.read() };
        f(unsafe { &*slot });
        drop(release);
        true
    }

    pub fn queue_message_count(&self) -> usize {
        self.storage.write().load(Ordering::Acquire).wrapping_sub(self.read)
    }
}

impl<T: Copy, const SLOT_SIZE: usize> Drop for Consumer<T, SLOT_SIZE> {
    fn drop(&mut self) {
        self.storage.storage.header().consumer_claimed.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod tests_layout;
