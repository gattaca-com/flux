mod dcache;

use std::{
    fmt,
    path::Path,
    time::{Duration, Instant},
};

pub use dcache::{
    SpineSpscDCacheConsumer, SpineSpscDCacheQueue, SpineSpscProducerWithDCache,
    SpscDCacheProduceError,
};
use flux_timing::InternalMessage;
use flux_utils::{directories::shmem_dir_with_base, short_typename};

use super::{FluxSpine, SpineProducers};
use crate::{Timer, communication::queue::spsc, tile::Tile};

mod sealed {
    pub trait Attached {}
}

pub trait SpscProducerAccess<T: Copy> {
    fn spsc_try_produce_with(
        &mut self,
        make: impl FnOnce() -> InternalMessage<T>,
    ) -> Result<(), SpscProduceError>;
}

pub trait SpscAttachedConsumer<T: Copy>: sealed::Attached {
    fn consume_ref_maybe_track<P, F>(&mut self, producers: &mut P, f: F) -> bool
    where
        P: SpineProducers,
        F: FnOnce(&T, &mut P) -> bool;

    fn consume_internal_message_maybe_track<P, F>(&mut self, producers: &mut P, f: F) -> bool
    where
        P: SpineProducers,
        F: FnMut(&mut InternalMessage<T>, &mut P) -> bool;
}

pub trait SpscConsumerAccess<T: Copy> {
    fn spsc_try_attached(&mut self) -> Result<impl SpscAttachedConsumer<T> + '_, spsc::QueueError>;
}

pub trait SpscDCacheProducerAccess<T: Copy> {
    fn spsc_dcache_try_produce_with(
        &mut self,
        len: Option<usize>,
        make: impl FnOnce(Option<&mut [u8]>) -> InternalMessage<T>,
    ) -> Result<(), SpscDCacheProduceError>;
}

pub trait SpscAttachedDCacheConsumer<T: Copy>: sealed::Attached {
    fn consume_maybe_track<P, R>(
        &mut self,
        producers: &mut P,
        did_work: &mut bool,
        read: &mut impl FnMut(T, &[u8]) -> R,
        handle: &mut impl FnMut(super::DCacheRead<T, R>, &mut P) -> bool,
    ) -> bool
    where
        P: SpineProducers;
}

pub trait SpscDCacheConsumerAccess<T: Copy> {
    fn spsc_dcache_try_attached(
        &mut self,
    ) -> Result<impl SpscAttachedDCacheConsumer<T> + '_, spsc::QueueError>;
}

#[derive(Debug, thiserror::Error)]
pub enum SpscProduceError {
    #[error("SPSC queue is full")]
    Full,
    #[error(transparent)]
    Attach(#[from] spsc::QueueError),
}

/// ```compile_fail,E0080
/// let _ = flux::spine::SpineSpscQueue::<u64, 1>::new(2);
/// ```
pub struct SpineSpscQueue<T: Copy, const SLOT_SIZE: usize = 0> {
    inner: spsc::Queue<InternalMessage<T>, SLOT_SIZE>,
}

impl<T: Copy, const SLOT_SIZE: usize> Clone for SpineSpscQueue<T, SLOT_SIZE> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<T: Copy, const SLOT_SIZE: usize> SpineSpscQueue<T, SLOT_SIZE> {
    pub fn new(len: usize) -> Self {
        Self { inner: spsc::Queue::new(len) }
    }

    /// # Safety
    /// The shared-memory contract of [`spsc::Queue::create_or_open_shared`]
    /// applies.
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
            match unsafe { spsc::Queue::create_or_open_shared(&path, len) } {
                Ok(inner) => return Self { inner },
                Err(spsc::QueueError::Uninitialized)
                    if started.elapsed() < Duration::from_secs(1) =>
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => {
                    let existing_capacity = if matches!(error, spsc::QueueError::IncompatibleLayout)
                    {
                        unsafe { spsc::Queue::<InternalMessage<T>, SLOT_SIZE>::open_shared(&path) }
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
                         Check that participants use matching capacity, payload and slot layout. \
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

impl<T: Copy, const SLOT_SIZE: usize> fmt::Debug for SpineSpscQueue<T, SLOT_SIZE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpineSpscQueue").field("capacity", &self.capacity()).finish_non_exhaustive()
    }
}

pub struct SpineSpscProducer<T: Copy, const SLOT_SIZE: usize = 0> {
    queue: SpineSpscQueue<T, SLOT_SIZE>,
    inner: Option<spsc::Producer<InternalMessage<T>, SLOT_SIZE>>,
}

impl<T: Copy, const SLOT_SIZE: usize> SpineSpscProducer<T, SLOT_SIZE> {
    pub fn new(queue: SpineSpscQueue<T, SLOT_SIZE>) -> Self {
        Self { queue, inner: None }
    }

    pub fn is_attached(&self) -> bool {
        self.inner.is_some()
    }

    pub fn try_attach(&mut self) -> Result<(), spsc::QueueError> {
        if self.inner.is_none() {
            self.inner = Some(self.queue.inner.try_producer()?);
        }
        Ok(())
    }

    #[inline]
    pub fn try_produce(&mut self, message: &InternalMessage<T>) -> Result<(), SpscProduceError> {
        self.try_attach()?;
        self.inner.as_mut().unwrap().produce(message).map_err(|_| SpscProduceError::Full)?;
        Ok(())
    }

    #[inline]
    pub fn try_produce_with(
        &mut self,
        message: impl FnOnce() -> InternalMessage<T>,
    ) -> Result<(), SpscProduceError> {
        self.try_attach()?;
        self.inner.as_mut().unwrap().produce_with(message).map_err(|_| SpscProduceError::Full)?;
        Ok(())
    }
}

impl<T: Copy, const SLOT_SIZE: usize> fmt::Debug for SpineSpscProducer<T, SLOT_SIZE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpineSpscProducer")
            .field("queue", &self.queue)
            .field("attached", &self.is_attached())
            .finish_non_exhaustive()
    }
}

pub struct SpineSpscConsumer<T: Copy, const SLOT_SIZE: usize = 0> {
    queue: SpineSpscQueue<T, SLOT_SIZE>,
    inner: Option<spsc::Consumer<InternalMessage<T>, SLOT_SIZE>>,
    timer: Timer,
}

struct AttachedSpscConsumer<'a, T: Copy, const SLOT_SIZE: usize> {
    inner: &'a mut spsc::Consumer<InternalMessage<T>, SLOT_SIZE>,
    timer: &'a mut Timer,
}

impl<T: Copy, const SLOT_SIZE: usize> sealed::Attached for AttachedSpscConsumer<'_, T, SLOT_SIZE> {}

impl<T: Copy, const SLOT_SIZE: usize> SpscAttachedConsumer<T>
    for AttachedSpscConsumer<'_, T, SLOT_SIZE>
{
    #[inline]
    fn consume_ref_maybe_track<P, F>(&mut self, producers: &mut P, f: F) -> bool
    where
        P: SpineProducers,
        F: FnOnce(&T, &mut P) -> bool,
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
    fn consume_internal_message_maybe_track<P, F>(&mut self, producers: &mut P, mut f: F) -> bool
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

impl<T: 'static + Copy, const SLOT_SIZE: usize> SpineSpscConsumer<T, SLOT_SIZE> {
    pub fn attach<D, S, Tl>(base_dir: D, tile: &Tl, queue: SpineSpscQueue<T, SLOT_SIZE>) -> Self
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

    pub fn try_attach(&mut self) -> Result<(), spsc::QueueError> {
        if self.inner.is_none() {
            self.inner = Some(self.queue.inner.try_consumer()?);
        }
        Ok(())
    }

    #[inline]
    pub fn try_attached(&mut self) -> Result<impl SpscAttachedConsumer<T> + '_, spsc::QueueError> {
        self.try_attach()?;
        Ok(AttachedSpscConsumer { inner: self.inner.as_mut().unwrap(), timer: &mut self.timer })
    }

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

    #[inline]
    pub fn consume_ref<P, F>(&mut self, producers: &mut P, f: F) -> Result<bool, spsc::QueueError>
    where
        P: SpineProducers,
        F: FnOnce(&T, &mut P),
    {
        self.consume_ref_maybe_track(producers, |message, p| {
            f(message, p);
            true
        })
    }

    #[inline]
    pub fn consume_ref_maybe_track<P, F>(
        &mut self,
        producers: &mut P,
        f: F,
    ) -> Result<bool, spsc::QueueError>
    where
        P: SpineProducers,
        F: FnOnce(&T, &mut P) -> bool,
    {
        Ok(self.try_attached()?.consume_ref_maybe_track(producers, f))
    }

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

impl<T: 'static + Copy, const SLOT_SIZE: usize> fmt::Debug for SpineSpscConsumer<T, SLOT_SIZE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpineSpscConsumer")
            .field("queue", &self.queue)
            .field("attached", &self.is_attached())
            .finish_non_exhaustive()
    }
}
