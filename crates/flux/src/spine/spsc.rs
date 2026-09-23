//! SPSC endpoints used by generated Spine bundles.
//!
//! Endpoints are claimed on first use, so attaching a tile does not reserve
//! queues it never uses. Call `try_attach` during `Tile::on_attach` to reserve
//! a role before workers start. A second owner receives an error. Dropping the
//! bundle releases its roles; unread messages remain available to replacements.
//!
//! SPSC operations require polling, including after `Full` or an empty read.
//! The tile runner does not park a tile with a claimed SPSC endpoint: the
//! process-local parking signal cannot wake peers in other processes.

use std::{
    fmt,
    path::Path,
    time::{Duration, Instant},
};

use flux_timing::InternalMessage;
use flux_utils::{directories::shmem_dir_with_base, short_typename};

use super::{FluxSpine, SpineProducers};
use crate::{Timer, communication::queue::spsc, tile::Tile};

#[derive(Debug, thiserror::Error)]
pub enum SpscProduceError {
    #[error("SPSC queue is full")]
    Full,
    #[error(transparent)]
    Attach(#[from] spsc::QueueError),
}

/// A Spine queue with one producer and one consumer across all processes.
///
/// `#[queue(flavour("spsc"))]` rewrites a `SpineQueue<T>` field to this type.
/// Such spines use unsafe shared-memory constructors and non-cloneable endpoint
/// bundles. Broadcast, collaborative consumers, dcache and gather are
/// unavailable.
#[derive(Clone)]
pub struct SpineSpscQueue<T: Copy> {
    inner: spsc::Queue<InternalMessage<T>>,
}

impl<T: Copy> SpineSpscQueue<T> {
    pub fn new(len: usize) -> Self {
        Self { inner: spsc::Queue::new(len) }
    }

    /// Create or open an SPSC mapping under `app/shmem/spsc/field_name`.
    /// This directory keeps the distinct layout away from broadcast discovery.
    /// An initializing creator is retried for up to one second. Existing
    /// mappings and file links are never reset, including stale links left
    /// after a reboot. Once all participants have stopped, remove stale storage
    /// with [`crate::communication::cleanup_flink`] before restarting them.
    ///
    /// # Safety
    /// All participants must use the same payload type, layout, architecture
    /// and application schema. Values must be valid in every process, with no
    /// process-local pointers or references. All access must use this queue
    /// implementation. Inherited endpoints must not be used or dropped in a
    /// child after `fork`. See [`spsc::Queue::create_or_open_shared`].
    pub unsafe fn create_or_open_shared_with_base_dir(
        base_dir: impl AsRef<Path>,
        app: impl AsRef<Path>,
        field_name: &str,
        len: usize,
    ) -> Self {
        let directory = shmem_dir_with_base(base_dir, app).join("spsc");
        std::fs::create_dir_all(&directory).unwrap_or_else(|error| {
            panic!("cannot create SPSC queue directory {}: {error}", directory.display());
        });
        let path = directory.join(field_name);
        let started = Instant::now();
        loop {
            // SAFETY: the caller supplies the shared-memory payload/access contract.
            match unsafe { spsc::Queue::create_or_open_shared(&path, len) } {
                Ok(inner) => return Self { inner },
                Err(spsc::QueueError::Uninitialized)
                    if started.elapsed() < Duration::from_secs(1) =>
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => {
                    // An otherwise compatible mapping can explain a capacity
                    // mismatch without accessing the core queue's private header.
                    let existing_capacity = if matches!(error, spsc::QueueError::IncompatibleLayout)
                    {
                        // SAFETY: same payload/access contract as the constructor.
                        unsafe { spsc::Queue::<InternalMessage<T>>::open_shared(&path) }
                            .ok()
                            .map(|queue| queue.capacity())
                    } else {
                        None
                    };
                    let existing = existing_capacity.map_or_else(String::new, |capacity| {
                        format!(", existing capacity {capacity}")
                    });
                    panic!(
                        "cannot open SPSC queue {} (requested length {len}{existing}): {error}. \
                         Check that participants use matching capacity and payload configuration. \
                         To remove stale or incompatible storage, stop all participants and call \
                         flux::communication::cleanup_flink on this path before restarting.",
                        path.display(),
                    );
                }
            }
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}

impl<T: Copy> fmt::Debug for SpineSpscQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpineSpscQueue").field("capacity", &self.capacity()).finish_non_exhaustive()
    }
}

/// A tile's lazily claimed SPSC producer. It cannot be cloned or copied.
pub struct SpineSpscProducer<T: Copy> {
    queue: SpineSpscQueue<T>,
    inner: Option<spsc::Producer<InternalMessage<T>>>,
}

impl<T: Copy> SpineSpscProducer<T> {
    pub fn new(queue: SpineSpscQueue<T>) -> Self {
        Self { queue, inner: None }
    }

    pub fn is_attached(&self) -> bool {
        self.inner.is_some()
    }

    /// Reserve the producer role. Repeated calls on this handle are no-ops.
    pub fn try_attach(&mut self) -> Result<(), spsc::QueueError> {
        if self.inner.is_none() {
            self.inner = Some(self.queue.inner.try_producer()?);
        }
        Ok(())
    }

    /// Publish a message, preserving its tracking metadata. A full queue is
    /// unchanged; the caller retains the message for retry.
    #[inline]
    pub fn try_produce(&mut self, message: &InternalMessage<T>) -> Result<(), SpscProduceError> {
        self.try_attach()?;
        self.inner.as_mut().unwrap().produce(message).map_err(|_| SpscProduceError::Full)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn try_produce_with(
        &mut self,
        message: impl FnOnce() -> InternalMessage<T>,
    ) -> Result<(), SpscProduceError> {
        self.try_attach()?;
        self.inner.as_mut().unwrap().produce_with(message).map_err(|_| SpscProduceError::Full)?;
        Ok(())
    }
}

impl<T: Copy> fmt::Debug for SpineSpscProducer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpineSpscProducer")
            .field("queue", &self.queue)
            .field("attached", &self.is_attached())
            .finish_non_exhaustive()
    }
}

/// A tile's lazily claimed SPSC consumer. Empty reads still claim the role.
pub struct SpineSpscConsumer<T: Copy> {
    queue: SpineSpscQueue<T>,
    inner: Option<spsc::Consumer<InternalMessage<T>>>,
    timer: Timer,
}

/// Borrows an attached endpoint and timer for repeated consumption.
pub(super) struct AttachedSpscConsumer<'a, T: Copy> {
    inner: &'a mut spsc::Consumer<InternalMessage<T>>,
    timer: &'a mut Timer,
}

impl<T: Copy> AttachedSpscConsumer<'_, T> {
    #[inline]
    pub(super) fn consume_ref_maybe_track<P, F>(&mut self, producers: &mut P, mut f: F) -> bool
    where
        P: SpineProducers,
        F: FnMut(&T, &mut P) -> bool,
    {
        self.inner.consume_ref(|message| {
            *producers.timestamp_mut().ingestion_t_mut() = message.ingestion_time();
            self.timer.start();
            if f(message.data(), producers) {
                self.timer
                    .record_processing_and_latency_from(producers.timestamp().ingestion_t.into());
            }
        })
    }

    #[inline]
    pub(super) fn consume_internal_message_maybe_track<P, F>(
        &mut self,
        producers: &mut P,
        mut f: F,
    ) -> bool
    where
        P: SpineProducers,
        F: FnMut(&mut InternalMessage<T>, &mut P) -> bool,
    {
        self.inner.consume(|message| {
            *producers.timestamp_mut().ingestion_t_mut() = message.ingestion_time();
            self.timer.start();
            if f(message, producers) {
                self.timer
                    .record_processing_and_latency_from(producers.timestamp().ingestion_t.into());
            }
        })
    }
}

impl<T: 'static + Copy> SpineSpscConsumer<T> {
    pub fn attach<D, S, Tl>(base_dir: D, tile: &Tl, queue: SpineSpscQueue<T>) -> Self
    where
        D: AsRef<Path>,
        S: FluxSpine,
        Tl: Tile<S>,
    {
        Self {
            queue,
            inner: None,
            timer: Timer::new_with_base_dir(
                base_dir,
                S::app_name(),
                format!("{}-{}", tile.name(), short_typename::<T>()),
            ),
        }
    }

    pub fn is_attached(&self) -> bool {
        self.inner.is_some()
    }

    /// Reserve the consumer role. Repeated calls on this handle are no-ops.
    pub fn try_attach(&mut self) -> Result<(), spsc::QueueError> {
        if self.inner.is_none() {
            self.inner = Some(self.queue.inner.try_consumer()?);
        }
        Ok(())
    }

    /// Claim the role if needed, then borrow the endpoint and timer.
    #[inline]
    pub(super) fn try_attached(&mut self) -> Result<AttachedSpscConsumer<'_, T>, spsc::QueueError> {
        self.try_attach()?;
        Ok(AttachedSpscConsumer { inner: self.inner.as_mut().unwrap(), timer: &mut self.timer })
    }

    /// Consume one value with Spine ingestion/latency tracking. The message is
    /// removed before the callback, including if the callback panics.
    #[inline]
    pub fn try_consume<P, F>(
        &mut self,
        producers: &mut P,
        mut f: F,
    ) -> Result<bool, spsc::QueueError>
    where
        P: SpineProducers,
        F: FnMut(T, &mut P),
    {
        self.try_consume_internal_message(producers, |message, p| f(message.into_data(), p))
    }

    /// Consume one value, recording processing time and latency only when the
    /// callback returns true. A false callback result still consumes the value
    /// and propagates its ingestion time. The initial clock read is retained.
    /// Returns whether a value was consumed, independently of the tracking
    /// choice.
    #[inline]
    pub fn try_consume_maybe_track<P, F>(
        &mut self,
        producers: &mut P,
        mut f: F,
    ) -> Result<bool, spsc::QueueError>
    where
        P: SpineProducers,
        F: FnMut(T, &mut P) -> bool,
    {
        self.try_consume_internal_message_maybe_track(producers, |message, p| {
            f(message.into_data(), p)
        })
    }

    /// Borrow one payload in its queue slot, with consumption telemetry.
    /// The slot stays occupied through the callback and timing records, and
    /// is released on return or unwind. Returns false when empty.
    #[inline]
    pub fn consume_ref<P, F>(
        &mut self,
        producers: &mut P,
        mut f: F,
    ) -> Result<bool, spsc::QueueError>
    where
        P: SpineProducers,
        F: FnMut(&T, &mut P),
    {
        self.consume_ref_maybe_track(producers, |message, p| {
            f(message, p);
            true
        })
    }

    /// Borrow one payload, recording timings only when the callback returns
    /// true. A false result still consumes it and propagates its ingestion
    /// time. The initial clock read is retained. The slot stays occupied
    /// through the callback and selected records, then releases on return or
    /// unwind. Returns whether a payload was consumed.
    #[inline]
    pub fn consume_ref_maybe_track<P, F>(
        &mut self,
        producers: &mut P,
        f: F,
    ) -> Result<bool, spsc::QueueError>
    where
        P: SpineProducers,
        F: FnMut(&T, &mut P) -> bool,
    {
        Ok(self.try_attached()?.consume_ref_maybe_track(producers, f))
    }

    /// Consume one message, including its original tracking metadata.
    #[inline]
    pub fn try_consume_internal_message<P, F>(
        &mut self,
        producers: &mut P,
        mut f: F,
    ) -> Result<bool, spsc::QueueError>
    where
        P: SpineProducers,
        F: FnMut(&mut InternalMessage<T>, &mut P),
    {
        self.try_consume_internal_message_maybe_track(producers, |message, p| {
            f(message, p);
            true
        })
    }

    /// Consume one message with its original tracking metadata. The callback's
    /// return value controls timing records, not consumption or ingestion-time
    /// propagation. The initial clock read is retained. The message is removed
    /// before the callback, including if it panics.
    #[inline]
    pub fn try_consume_internal_message_maybe_track<P, F>(
        &mut self,
        producers: &mut P,
        f: F,
    ) -> Result<bool, spsc::QueueError>
    where
        P: SpineProducers,
        F: FnMut(&mut InternalMessage<T>, &mut P) -> bool,
    {
        Ok(self.try_attached()?.consume_internal_message_maybe_track(producers, f))
    }
}

impl<T: 'static + Copy> fmt::Debug for SpineSpscConsumer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpineSpscConsumer")
            .field("queue", &self.queue)
            .field("attached", &self.is_attached())
            .finish_non_exhaustive()
    }
}
