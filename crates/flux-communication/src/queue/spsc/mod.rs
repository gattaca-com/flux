//! Bounded single-producer, single-consumer queues for threads and shared
//! memory.
//!
//! Each queue permits one live producer and one live consumer. Writes return
//! [`FullError`] rather than overwriting unread messages. Each cursor has one
//! writer and its own cache line; enqueue/dequeue use only atomic loads and
//! stores. Transferring cursor and payload cache lines still costs coherence
//! traffic. Peer cursors are cached until the queue appears full or empty.
//!
//! Inspired by Seastar's cross-core message queues, which use Boost.Lockfree's
//! SPSC ring and batch transfers. This implementation publishes each message.
//!
//! ```
//! use flux_communication::queue::spsc::Queue;
//!
//! let queue = Queue::new(16);
//! let mut producer = queue.try_producer().unwrap();
//! let mut consumer = queue.try_consumer().unwrap();
//! producer.produce(&42).unwrap();
//! let mut value = 0;
//! consumer.try_consume(&mut value).unwrap();
//! assert_eq!(value, 42);
//! ```
//!
//! Shared mappings persist after all handles are dropped; use
//! [`crate::cleanup_flink`] once all users have detached. A crashed or
//! forgotten endpoint retains its claim. Recovery requires detaching all users
//! and recreating the mapping; claims are never stolen on a timeout.
//!
//! This layout is independent of [`super::Queue`]. Keep its file links outside
//! Flux's existing queue-discovery directories, whose tools expect that layout.

use std::{
    alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error},
    cell::UnsafeCell,
    marker::PhantomData,
    mem::{MaybeUninit, align_of, size_of},
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

// The final byte identifies the shared-memory layout version.
const MAGIC: u64 = u64::from_le_bytes(*b"FXSPSC01");

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
    producer_claimed: AtomicBool,
    consumer_claimed: AtomicBool,
    write: Cursor,
    read: Cursor,
}

fn layout<T>(capacity: usize) -> Result<(Layout, usize), QueueError> {
    if !capacity.is_power_of_two() || capacity > isize::MAX as usize {
        return Err(QueueError::InvalidCapacity);
    }
    let slots =
        Layout::array::<MaybeUninit<T>>(capacity).map_err(|_| QueueError::InvalidCapacity)?;
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

struct Storage<T> {
    ptr: NonNull<u8>,
    layout: Layout,
    slots_offset: usize,
    capacity: usize,
    // Dropping Shmem unmaps locally. Its owner flag is cleared on successful
    // creation so endpoint drop does not unlink a queue used by another process.
    shared: Option<Shmem>,
    // Shared queue handles can create writers. Invariance prevents shortening
    // payload lifetimes through one alias and reading them through another.
    _value: PhantomData<UnsafeCell<T>>,
}

// SAFETY: each slot is exclusively accessed by its owning endpoint. Release /
// acquire cursor publication transfers that ownership, and role claims prevent
// concurrent endpoints of the same kind. No references to slots escape.
// Shmem's pointer is used under the same rules; its configuration is immutable
// after construction and its mapping can be unmapped on any thread.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl<T: Send> Send for Storage<T> {}
unsafe impl<T: Send> Sync for Storage<T> {}

impl<T> Storage<T> {
    fn header(&self) -> &Header {
        // SAFETY: construction initializes or validates the complete header,
        // and the allocation remains alive through this borrow.
        unsafe { self.ptr.cast::<Header>().as_ref() }
    }

    fn initialize(&mut self) {
        let header = self.ptr.cast::<Header>().as_ptr();
        // SAFETY: only the mapping creator initializes these immutable fields.
        // Openers inspect ready atomically before reading them. Do not write the
        // whole Header: the link can already be visible to an opening process.
        unsafe {
            (&raw mut (*header).capacity).write(self.capacity);
            (&raw mut (*header).element_size).write(size_of::<T>());
            (&raw mut (*header).element_align).write(align_of::<T>());
        }
        self.header().ready.store(MAGIC, Ordering::Release);
    }
}

impl<T> Drop for Storage<T> {
    fn drop(&mut self) {
        if self.shared.is_none() {
            // SAFETY: this allocation used this exact layout and the last Arc
            // has dropped, so no endpoint can still access it.
            unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
        }
    }
}

/// Process-local addresses retained by an endpoint. The owning Arc keeps every
/// cached pointer valid even after queue handles are dropped or the endpoint
/// moves.
struct EndpointStorage<T> {
    storage: Arc<Storage<T>>,
    slots: *mut T,
    mask: usize,
    read: *const AtomicUsize,
    write: *const AtomicUsize,
}

// SAFETY: these pointers address the allocation owned by storage. Slot access
// follows the same exclusive-role and release/acquire rules as Storage, and
// shared endpoint access cannot read or write payloads. Moving an endpoint does
// not move its allocation. Storage also keeps the payload type invariant.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl<T: Send> Send for EndpointStorage<T> {}
unsafe impl<T: Send> Sync for EndpointStorage<T> {}

impl<T> EndpointStorage<T> {
    fn new(storage: Arc<Storage<T>>) -> Self {
        let header = storage.header();
        // SAFETY: layout reserves capacity properly aligned slots, including
        // an aligned one-past pointer for zero-sized payloads.
        let slots = unsafe { storage.ptr.as_ptr().add(storage.slots_offset).cast::<T>() };
        Self {
            slots,
            mask: storage.capacity - 1,
            read: &raw const header.read.0,
            write: &raw const header.write.0,
            storage,
        }
    }

    fn slot(&self, position: usize) -> *mut T {
        // SAFETY: masking keeps the slot in bounds, including on counter wrap.
        unsafe { self.slots.add(position & self.mask) }
    }

    fn read(&self) -> &AtomicUsize {
        // SAFETY: storage owns the initialized header for this borrow.
        unsafe { &*self.read }
    }

    fn write(&self) -> &AtomicUsize {
        // SAFETY: storage owns the initialized header for this borrow.
        unsafe { &*self.write }
    }
}

/// A queue handle that can claim one producer and one consumer.
///
/// Cloning this handle does not clone either endpoint. Dropping it does not
/// invalidate endpoints, which keep the backing allocation or mapping alive.
///
/// Borrowed payloads must outlive every queue handle that can read them:
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
#[derive(Clone)]
pub struct Queue<T: Copy> {
    storage: Arc<Storage<T>>,
}

impl<T: Copy> Queue<T> {
    /// Allocate a queue, rounding `len` up to a power of two.
    ///
    /// # Panics
    /// Panics if `len` is zero or the allocation size overflows.
    pub fn new(len: usize) -> Self {
        let capacity = rounded_capacity(len).expect("invalid SPSC capacity");
        let (layout, slots_offset) = layout::<T>(capacity).expect("invalid SPSC allocation");
        // SAFETY: the checked layout has nonzero size and the required alignment.
        let ptr = NonNull::new(unsafe { alloc_zeroed(layout) })
            .unwrap_or_else(|| handle_alloc_error(layout));
        let mut storage =
            Storage { ptr, layout, slots_offset, capacity, shared: None, _value: PhantomData };
        storage.initialize();
        Self { storage: Arc::new(storage) }
    }

    /// Create a persistent shared queue or open one with the same capacity.
    ///
    /// `len` is rounded up to a power of two. The parent directory must exist.
    /// An existing mapping is never reset or replaced. A concurrent creator may
    /// still be publishing the file link or initializing the header; callers
    /// can retry an open failure or [`QueueError::Uninitialized`].
    ///
    /// # Safety
    /// All participants must use this queue implementation with the same `T`,
    /// layout, architecture and application schema. Values must be valid in
    /// every participating process (in particular, no process-local pointers
    /// or references). No participant may modify the mapping outside this
    /// interface, or use or drop inherited endpoints in a child after `fork`.
    /// Size/alignment validation cannot establish this contract for the caller.
    pub unsafe fn create_or_open_shared(
        path: impl AsRef<Path>,
        len: usize,
    ) -> Result<Self, QueueError> {
        let capacity = rounded_capacity(len)?;
        let (layout, slots_offset) = layout::<T>(capacity)?;
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
                // SAFETY: inherited from this function's shared-memory contract.
                let queue = unsafe { Self::open_shared(path) }?;
                if queue.capacity() != capacity {
                    return Err(QueueError::IncompatibleLayout);
                }
                Ok(queue)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Open a persistent shared queue without changing its contents or claims.
    ///
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
        // SAFETY: mapping length and alignment were checked. Borrow only ready
        // while the creator may still be writing the non-atomic header fields.
        let ready = unsafe { &(*header_ptr).ready };
        match ready.load(Ordering::Acquire) {
            0 => return Err(QueueError::Uninitialized),
            MAGIC => {}
            _ => return Err(QueueError::IncompatibleLayout),
        }
        // SAFETY: acquiring MAGIC observes completed initialization. The caller
        // ensures participants obey the shared-memory contract thereafter.
        let header = unsafe { &*header_ptr };
        if header.element_size != size_of::<T>() || header.element_align != align_of::<T>() {
            return Err(QueueError::IncompatibleLayout);
        }
        let capacity = header.capacity;
        let (layout, slots_offset) = layout::<T>(capacity)?;
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

    /// Claim the producer role, resuming at the last published position.
    pub fn try_producer(&self) -> Result<Producer<T>, QueueError> {
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

    /// Claim the consumer role, continuing with the oldest unread message.
    pub fn try_consumer(&self) -> Result<Consumer<T>, QueueError> {
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

/// Exclusive producer. Operations require a mutable borrow; this handle cannot
/// be cloned. Dropping it permits a replacement producer to attach.
///
/// ```compile_fail
/// use flux_communication::queue::spsc::Queue;
/// let queue = Queue::<u64>::new(4);
/// let producer = queue.try_producer().unwrap();
/// let duplicate = producer.clone();
/// ```
///
/// A payload must be `Send` to move its endpoint to another thread:
///
/// ```compile_fail
/// use flux_communication::queue::spsc::Queue;
/// let queue = Queue::<*const u8>::new(4);
/// let mut producer = queue.try_producer().unwrap();
/// std::thread::spawn(move || producer.produce(&std::ptr::null()));
/// ```
pub struct Producer<T: Copy> {
    storage: EndpointStorage<T>,
    write: usize,
    cached_read: usize,
}

impl<T: Copy> Producer<T> {
    /// Sequence number returned by the next successful publication, wrapping
    /// at `usize::MAX`. A full queue or a panicking factory leaves it
    /// unchanged. This reads the producer's local cursor; it neither checks
    /// nor reserves capacity.
    #[inline]
    pub fn next_sequence(&self) -> usize {
        self.write
    }

    /// Publish one message, returning its wrapping, zero-based sequence number.
    /// A full queue is unchanged, and the caller retains `msg` for retry.
    #[inline]
    pub fn produce(&mut self, msg: &T) -> Result<usize, FullError> {
        self.produce_with(|| *msg)
    }

    /// Construct and publish one message if capacity is available, returning
    /// its sequence number.
    ///
    /// The factory is not called when the queue is full. If it panics, no
    /// message is published and the producer's write position is unchanged.
    #[inline]
    pub fn produce_with(&mut self, message: impl FnOnce() -> T) -> Result<usize, FullError> {
        if self.write.wrapping_sub(self.cached_read) == self.storage.mask + 1 {
            self.cached_read = self.storage.read().load(Ordering::Acquire);
            if self.write.wrapping_sub(self.cached_read) == self.storage.mask + 1 {
                return Err(FullError);
            }
        }
        let position = self.write;
        // SAFETY: the acquired read cursor grants ownership of this free slot.
        // The consumer cannot read it until the following release publication.
        unsafe { self.storage.slot(position).write(message()) };
        self.write = position.wrapping_add(1);
        self.storage.write().store(self.write, Ordering::Release);
        Ok(position)
    }

    /// Available slots at the observed consumer position. The consumer may
    /// release more slots immediately afterwards.
    pub fn max_writable_msgs_without_speeding_past(&self) -> usize {
        let read = self.storage.read().load(Ordering::Acquire);
        self.storage.mask + 1 - self.write.wrapping_sub(read)
    }
}

impl<T: Copy> Drop for Producer<T> {
    fn drop(&mut self) {
        self.storage.storage.header().producer_claimed.store(false, Ordering::Release);
    }
}

/// Exclusive consumer. Copying operations release slots before invoking
/// callbacks; [`Consumer::consume_ref`] borrows a slot until its callback
/// finishes. Dropping permits a replacement reader.
///
/// ```compile_fail
/// use flux_communication::queue::spsc::Queue;
/// let queue = Queue::<u64>::new(4);
/// let consumer = queue.try_consumer().unwrap();
/// let duplicate = consumer.clone();
/// ```
pub struct Consumer<T: Copy> {
    storage: EndpointStorage<T>,
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
        // Publishing the read cursor returns this slot to the producer.
        self.read.store(*self.position, Ordering::Release);
    }
}

impl<T: Copy> Consumer<T> {
    #[inline]
    fn pop(&mut self) -> Option<T> {
        if self.read == self.cached_write {
            self.cached_write = self.storage.write().load(Ordering::Acquire);
            if self.read == self.cached_write {
                return None;
            }
        }
        // SAFETY: the acquired write cursor proves this slot is initialized.
        // Only this consumer reads it; the producer cannot reuse it until the
        // following release publication, which occurs after the copy completes.
        let value = unsafe { self.storage.slot(self.read).read() };
        self.read = self.read.wrapping_add(1);
        self.storage.read().store(self.read, Ordering::Release);
        Some(value)
    }

    /// Copy the next message to `value`, leaving it unchanged if empty.
    #[inline]
    pub fn try_consume(&mut self, value: &mut T) -> Result<(), EmptyError> {
        *value = self.pop().ok_or(EmptyError::Empty)?;
        Ok(())
    }

    /// Call `f` with the next message and return whether one was consumed.
    /// The message is removed before calling `f`, including if it panics.
    #[inline]
    pub fn consume(&mut self, mut f: impl FnMut(&mut T)) -> bool {
        let Some(mut value) = self.pop() else { return false };
        f(&mut value);
        true
    }

    /// Borrow the next message in its queue slot without copying it.
    ///
    /// Returns false without calling `f` when empty. The slot remains occupied
    /// throughout `f` and is released when it returns or unwinds. A callback
    /// must not wait for producer progress that requires this slot to be free.
    /// Use [`Self::consume`] to release the slot before running a callback.
    ///
    /// The callback cannot retain the slot's reference:
    ///
    /// ```compile_fail,E0521
    /// use flux_communication::queue::spsc::Queue;
    /// let queue = Queue::<u64>::new(1);
    /// let mut consumer = queue.try_consumer().unwrap();
    /// let mut saved = None;
    /// consumer.consume_ref(|message| saved = Some(message));
    /// ```
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
        // SAFETY: the acquired write cursor proves this slot is initialized.
        // The producer cannot reuse it until the guard publishes read, after this
        // callback's borrow ends, including during unwinding.
        f(unsafe { &*slot });
        drop(release);
        true
    }

    /// Unread messages at the observed producer position.
    pub fn queue_message_count(&self) -> usize {
        self.storage.write().load(Ordering::Acquire).wrapping_sub(self.read)
    }
}

impl<T: Copy> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.storage.storage.header().consumer_claimed.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod tests_layout;
