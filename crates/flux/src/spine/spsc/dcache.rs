//! Managed side payloads whose lifetime follows the SPSC metadata slot.

mod storage;

use std::{
    fmt,
    path::Path,
    time::{Duration, Instant},
};

use flux_timing::InternalMessage;
use flux_utils::{DCacheError, directories::shmem_dir_with_base, short_typename};

use self::storage::{Storage, StorageError};
use super::{
    SpineSpscConsumer, SpineSpscProducer, SpineSpscQueue, SpscAttachedDCacheConsumer,
    SpscProduceError,
};
use crate::{
    Timer,
    communication::queue::spsc,
    spine::{DCacheMsg, DCacheRead, FluxSpine, SpineProducers},
    tile::Tile,
};

#[derive(Debug, thiserror::Error)]
pub enum SpscDCacheProduceError {
    #[error("SPSC queue is full")]
    Full,
    #[error(transparent)]
    Attach(#[from] spsc::QueueError),
    #[error(transparent)]
    Payload(#[from] DCacheError),
}

impl From<SpscProduceError> for SpscDCacheProduceError {
    fn from(error: SpscProduceError) -> Self {
        match error {
            SpscProduceError::Full => Self::Full,
            SpscProduceError::Attach(error) => Self::Attach(error),
        }
    }
}

/// A bounded SPSC queue with managed payloads of at most `mtu` bytes.
///
/// Each metadata slot owns one payload region. Payload reads hold the metadata
/// slot, so neither unread nor borrowed bytes can be overwritten. Only managed
/// writes are supported; no raw `DCache` pointer or independently allocated
/// reference can be used to bypass the queue's capacity check.
/// Storage reserves `capacity * round_up(mtu, 64)` bytes, rounded to a power
/// of two, plus headers. All endpoints retain both allocations.
///
/// With an inferred payload, write `SpineSpscDCacheQueue::<_>::new(...)` to
/// select the natural metadata slot size.
pub struct SpineSpscDCacheQueue<T: Copy, const SLOT_SIZE: usize = 0> {
    inner: SpineSpscQueue<DCacheMsg<T>, SLOT_SIZE>,
    storage: Storage,
}

impl<T: Copy, const SLOT_SIZE: usize> Clone for SpineSpscDCacheQueue<T, SLOT_SIZE> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), storage: self.storage.clone() }
    }
}

impl<T: Copy, const SLOT_SIZE: usize> SpineSpscDCacheQueue<T, SLOT_SIZE> {
    pub fn new(len: usize, mtu: usize) -> Self {
        let inner = SpineSpscQueue::new(len);
        let storage = Storage::new(inner.capacity(), mtu);
        Self { inner, storage }
    }

    /// Open persistent metadata and payload storage under `app/shmem/spsc`.
    /// The payload file link is named `<field_name>.dcache`.
    /// Both mappings must be removed together, only after every peer detaches.
    /// Existing mappings are never reset or replaced.
    ///
    /// # Safety
    /// All participants must use this queue implementation with matching
    /// payload types, application schema, architecture, capacity and MTU.
    /// Metadata must be valid in every process, without process-local pointers.
    /// No other interface may access either mapping. Inherited endpoints must
    /// not be used or dropped in a child after `fork`.
    pub unsafe fn create_or_open_shared_with_base_dir(
        base_dir: impl AsRef<Path>,
        app: impl AsRef<Path>,
        field_name: &str,
        len: usize,
        mtu: usize,
    ) -> Self {
        let directory = shmem_dir_with_base(&base_dir, &app).join("spsc");
        std::fs::create_dir_all(&directory).unwrap_or_else(|error| {
            panic!("cannot create SPSC queue directory {}: {error}", directory.display());
        });
        let queue_path = directory.join(field_name);
        let payload_path = directory.join(format!("{field_name}.dcache"));
        assert!(
            !queue_path.exists() || payload_path.exists(),
            "SPSC queue {} is missing its payload storage {}; stop all peers and clean up both mappings before restarting",
            queue_path.display(),
            payload_path.display(),
        );
        let capacity =
            len.checked_next_power_of_two().filter(|_| len != 0).expect("invalid SPSC capacity");
        let started = Instant::now();
        let storage = loop {
            // SAFETY: the caller supplies the shared payload/access contract.
            match unsafe { Storage::create_or_open_shared(&payload_path, capacity, mtu) } {
                Ok(storage) => break storage,
                Err(StorageError::Uninitialized) if started.elapsed() < Duration::from_secs(1) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!(
                    "cannot open SPSC payload storage {} (capacity {capacity}, mtu {mtu}): {error}; stop all peers before cleaning up storage",
                    payload_path.display(),
                ),
            }
        };
        // The side arena is ready before the metadata queue becomes visible.
        // SAFETY: the caller supplies the metadata and shared-storage contract.
        let inner = unsafe {
            SpineSpscQueue::create_or_open_shared_with_base_dir(base_dir, app, field_name, len)
        };
        assert_eq!(
            storage.capacity(),
            inner.capacity(),
            "SPSC payload and metadata capacity must match"
        );
        Self { inner, storage }
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn mtu(&self) -> usize {
        self.storage.mtu()
    }
}

impl<T: Copy, const SLOT_SIZE: usize> fmt::Debug for SpineSpscDCacheQueue<T, SLOT_SIZE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpineSpscDCacheQueue")
            .field("capacity", &self.capacity())
            .field("mtu", &self.mtu())
            .finish_non_exhaustive()
    }
}

/// Exclusive producer of metadata and managed side payloads.
///
/// Payloads cannot be written independently of the queue's capacity gate:
///
/// ```compile_fail,E0599
/// use flux::spine::{SpineSpscDCacheQueue, SpineSpscProducerWithDCache};
/// let producer = SpineSpscProducerWithDCache::new(SpineSpscDCacheQueue::<u64>::new(8, 256));
/// let raw_cache = producer.dcache_ptr();
/// ```
pub struct SpineSpscProducerWithDCache<T: Copy, const SLOT_SIZE: usize = 0> {
    inner: SpineSpscProducer<DCacheMsg<T>, SLOT_SIZE>,
    storage: Storage,
}

impl<T: Copy, const SLOT_SIZE: usize> SpineSpscProducerWithDCache<T, SLOT_SIZE> {
    pub fn new(queue: SpineSpscDCacheQueue<T, SLOT_SIZE>) -> Self {
        Self { inner: SpineSpscProducer::new(queue.inner), storage: queue.storage }
    }

    pub fn is_attached(&self) -> bool {
        self.inner.is_attached()
    }

    pub fn try_attach(&mut self) -> Result<(), spsc::QueueError> {
        self.inner.try_attach()
    }

    /// Fill a payload and construct its metadata only after a slot is free.
    /// `None` publishes metadata without a payload. Explicit lengths must be
    /// nonzero and at most the configured MTU. Invalid lengths, attachment
    /// errors and Full never invoke `make`. A panic publishes nothing.
    /// The callback must initialize every byte it intends the consumer to use;
    /// unwritten bytes retain their previous contents.
    pub fn try_produce_with(
        &mut self,
        len: Option<usize>,
        make: impl FnOnce(Option<&mut [u8]>) -> InternalMessage<T>,
    ) -> Result<(), SpscDCacheProduceError> {
        if let Some(len) = len {
            self.storage.validate_len(len)?;
        }
        self.try_attach()?;
        let storage = &self.storage;
        let slot = self.inner.inner.as_ref().unwrap().next_sequence() & (storage.capacity() - 1);
        self.inner.try_produce_with(|| {
            let (message, dref) = if let Some(len) = len {
                // SAFETY: the core factory runs only with a free metadata
                // slot. The region is indexed by that publication's sequence,
                // so it cannot belong to an unread or borrowed slot.
                let (dref, message) =
                    unsafe { storage.write(slot, len, |bytes| make(Some(bytes))) }
                        .expect("validated SPSC payload region");
                (message, Some(dref))
            } else {
                (make(None), None)
            };
            message.with_data(DCacheMsg::new(message.into_data(), dref))
        })?;
        Ok(())
    }
}

impl<T: Copy, const SLOT_SIZE: usize> fmt::Debug for SpineSpscProducerWithDCache<T, SLOT_SIZE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpineSpscProducerWithDCache")
            .field("attached", &self.is_attached())
            .finish_non_exhaustive()
    }
}

/// Exclusive consumer of metadata and its callback-scoped payload bytes.
pub struct SpineSpscDCacheConsumer<T: Copy, const SLOT_SIZE: usize = 0> {
    inner: SpineSpscConsumer<DCacheMsg<T>, SLOT_SIZE>,
    storage: Storage,
}

impl<T: 'static + Copy, const SLOT_SIZE: usize> SpineSpscDCacheConsumer<T, SLOT_SIZE> {
    pub fn attach<D, S, Tl>(
        base_dir: D,
        tile: &Tl,
        queue: SpineSpscDCacheQueue<T, SLOT_SIZE>,
    ) -> Self
    where
        D: AsRef<Path>,
        S: FluxSpine,
        Tl: Tile<S>,
    {
        Self {
            inner: SpineSpscConsumer {
                queue: queue.inner,
                inner: None,
                // Label telemetry with the application type, not DCacheMsg<T>.
                timer: Timer::new_with_base_dir(
                    base_dir,
                    S::app_name(),
                    format!("{}-{}", tile.name(), short_typename::<T>()),
                ),
            },
            storage: queue.storage,
        }
    }

    pub fn is_attached(&self) -> bool {
        self.inner.is_attached()
    }

    pub fn try_attach(&mut self) -> Result<(), spsc::QueueError> {
        self.inner.try_attach()
    }

    /// Claim the role if needed, then borrow the endpoint, timer and payload
    /// storage.
    pub fn try_attached(
        &mut self,
    ) -> Result<impl SpscAttachedDCacheConsumer<T> + '_, spsc::QueueError> {
        self.try_attach()?;
        Ok(AttachedDCacheConsumer {
            inner: self.inner.inner.as_mut().unwrap(),
            timer: &mut self.inner.timer,
            storage: &self.storage,
        })
    }
}

impl<T: Copy, const SLOT_SIZE: usize> fmt::Debug for SpineSpscDCacheConsumer<T, SLOT_SIZE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpineSpscDCacheConsumer")
            .field("attached", &self.inner.inner.is_some())
            .finish_non_exhaustive()
    }
}

struct AttachedDCacheConsumer<'a, T: Copy, const SLOT_SIZE: usize> {
    inner: &'a mut spsc::Consumer<InternalMessage<DCacheMsg<T>>, SLOT_SIZE>,
    timer: &'a mut Timer,
    storage: &'a Storage,
}

impl<T: Copy, const SLOT_SIZE: usize> super::sealed::Attached
    for AttachedDCacheConsumer<'_, T, SLOT_SIZE>
{
}

impl<T: Copy, const SLOT_SIZE: usize> AttachedDCacheConsumer<'_, T, SLOT_SIZE> {
    /// Extract while the core slot is held; return owned results after release.
    pub(crate) fn extract<P, R>(
        &mut self,
        producers: &mut P,
        mut read: impl FnMut(&InternalMessage<T>, &[u8]) -> R,
    ) -> Option<DCacheRead<InternalMessage<T>, R>>
    where
        P: SpineProducers,
    {
        let mut result = None;
        let storage = self.storage;
        self.inner.consume_ref(|message| {
            let user_message = message.with_data(message.data().data);
            *producers.timestamp_mut().ingestion_t_mut() = message.ingestion_time();
            self.timer.start();
            let dref = message.data().dref;
            result = Some(if dref.is_none() {
                DCacheRead::NoRef(user_message)
            } else {
                // SAFETY: consume_ref retains the metadata slot throughout
                // read, preventing the sole producer from reusing its region.
                let extracted = unsafe { storage.read(dref, |bytes| read(&user_message, bytes)) }
                    .expect("published SPSC payload descriptor");
                DCacheRead::Ok((user_message, extracted))
            });
        });
        result
    }
}

impl<T: Copy, const SLOT_SIZE: usize> SpscAttachedDCacheConsumer<T>
    for AttachedDCacheConsumer<'_, T, SLOT_SIZE>
{
    fn consume_maybe_track<P, R>(
        &mut self,
        producers: &mut P,
        did_work: &mut bool,
        read: &mut impl FnMut(T, &[u8]) -> R,
        handle: &mut impl FnMut(DCacheRead<T, R>, &mut P) -> bool,
    ) -> bool
    where
        P: SpineProducers,
    {
        let Some(result) =
            self.extract(producers, |message, bytes| read(message.into_data(), bytes))
        else {
            return false;
        };
        let ingestion = producers.timestamp().ingestion_t;
        let result = match result {
            DCacheRead::Ok((message, value)) => DCacheRead::Ok((message.into_data(), value)),
            DCacheRead::NoRef(message) => DCacheRead::NoRef(message.into_data()),
            DCacheRead::Lost(_) | DCacheRead::SpedPast => unreachable!("owned SPSC payload"),
        };
        *did_work = true;
        if handle(result, producers) {
            self.timer.record_processing_and_latency_from(ingestion.into());
        }
        true
    }
}
