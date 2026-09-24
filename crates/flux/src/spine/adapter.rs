use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use flux_timing::{IngestionTime, InternalMessage};
use flux_utils::DCacheError;
use signal_hook::consts::SIGINT;

use crate::{
    spine::{
        DCacheRead, FluxSpine, SpineConsumer, SpineDCacheConsumer, SpineProducer,
        SpineProducerWithDCache, SpineProducers, SpscConsumerAccess, SpscDCacheConsumerAccess,
        SpscDCacheProduceError, SpscDCacheProducerAccess, SpscProduceError, SpscProducerAccess,
    },
    tile::Tile,
};

#[derive(Debug)]
pub struct SpineAdapter<S: FluxSpine> {
    pub consumers: S::Consumers,
    pub producers: S::Producers,
    pub stop_flag: Option<Arc<AtomicUsize>>,
    did_work: bool,
    #[cfg(feature = "park")]
    waker_registered: bool,
}

impl<S: FluxSpine> SpineAdapter<S> {
    #[inline]
    pub fn connect_tile<Tl: Tile<S>>(tile: &Tl, spine: &mut S) -> Self {
        Self {
            consumers: spine.attach_consumers(tile),
            producers: spine.attach_producers(tile),
            stop_flag: None,
            did_work: false,
            #[cfg(feature = "park")]
            waker_registered: false,
        }
    }

    #[inline]
    pub fn connect_tile_with_stop_flag<Tl: Tile<S>>(
        tile: &Tl,
        spine: &mut S,
        stop_flag: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            consumers: spine.attach_consumers(tile),
            producers: spine.attach_producers(tile),
            stop_flag: Some(stop_flag),
            did_work: false,
            #[cfg(feature = "park")]
            waker_registered: false,
        }
    }

    #[inline]
    pub fn request_stop_scope(&self) {
        if let Some(f) = &self.stop_flag {
            f.store(SIGINT as usize, Ordering::Relaxed);
            #[cfg(feature = "park")]
            crate::park::SIGNAL.signal();
        }
    }

    #[cfg(feature = "park")]
    #[inline]
    pub fn register_waker(&mut self, waker: mio::Waker) {
        self.waker_registered = true;
        crate::park::SIGNAL.register_waker(waker);
    }

    #[cfg(feature = "park")]
    #[inline]
    pub fn waker_registered(&self) -> bool {
        self.waker_registered
    }

    /// Called by `attach_tile` before each `loop_body`.
    #[inline]
    pub fn begin_loop(&mut self, ingestion_t: IngestionTime) {
        self.set_ingestion_time(ingestion_t);
        self.did_work = false;
    }

    #[inline]
    pub fn did_work(&self) -> bool {
        self.did_work
    }

    /// SPSC peers can be in another process, outside the parking signal's
    /// reach.
    pub fn requires_polling(&self) -> bool {
        S::requires_polling(&self.consumers, &self.producers)
    }

    /// Manually mark work as done. Use for non-consume/produce work like
    /// business logic ticks.
    #[inline]
    pub fn mark_work(&mut self) {
        self.did_work = true;
    }

    #[inline]
    pub fn ingestion_t(&mut self) -> IngestionTime {
        self.producers.timestamp().ingestion_t()
    }

    #[inline]
    pub fn ingestion_t_mut(&mut self) -> &mut IngestionTime {
        self.producers.timestamp_mut().ingestion_t_mut()
    }

    #[inline]
    pub fn set_ingestion_time(&mut self, now: IngestionTime) {
        *self.producers.timestamp_mut().ingestion_t_mut() = now;
    }

    #[inline]
    pub fn produce<T: Copy>(&mut self, d: T)
    where
        S::Producers: SpineProducers + AsRef<SpineProducer<T>>,
    {
        self.producers.produce(d);
        self.did_work = true;
    }

    /// Try to publish to an SPSC queue. Only a successful publication counts as
    /// work. Keep pending output in the tile and retry `Full` on a later loop.
    #[inline]
    pub fn try_produce<T: Copy>(&mut self, data: T) -> Result<(), SpscProduceError>
    where
        S::Producers: SpscProducerAccess<T>,
    {
        self.producers.try_produce(data)?;
        self.did_work = true;
        Ok(())
    }

    /// Construct and publish an SPSC message only if a slot is available.
    /// An error never invokes `make` or counts as work.
    #[inline]
    pub fn try_produce_with<T: Copy>(
        &mut self,
        make: impl FnOnce() -> T,
    ) -> Result<(), SpscProduceError>
    where
        S::Producers: SpscProducerAccess<T>,
    {
        self.producers.try_produce_with(make)?;
        self.did_work = true;
        Ok(())
    }

    /// Publish a managed SPSC payload after checking queue capacity. A full
    /// queue does not invoke the writer. `None` is received as
    /// `DCacheRead::NoRef`.
    #[inline]
    pub fn try_produce_with_dcache<T: Copy, F: FnOnce(&mut [u8])>(
        &mut self,
        data: T,
        payload: Option<(usize, F)>,
    ) -> Result<(), SpscDCacheProduceError>
    where
        S::Producers: SpscDCacheProducerAccess<T>,
    {
        self.producers.try_produce_with_dcache(data, payload)?;
        self.did_work = true;
        Ok(())
    }

    /// Drain managed SPSC messages. The reader borrows payload bytes while
    /// the slot is held; the handler receives its owned result after release.
    /// `None` payloads skip the reader and reach the handler as `NoRef`.
    /// Timings cover both callbacks, including metadata-only messages.
    /// Returns whether any message was consumed.
    /// The reader must not wait for a publication that needs its held slot;
    /// retain pending output and retry after the slot releases instead.
    ///
    /// The extracted result cannot retain a payload reference after release:
    ///
    /// ```compile_fail
    /// use flux::{communication::ShmemData, spine::SpineAdapter, tile::TileInfo};
    /// use spine_derive::from_spine;
    /// #[from_spine("payload-borrow-example")]
    /// struct App {
    ///     tile_info: ShmemData<TileInfo>,
    ///     #[queue(flavour("spsc"), mtu(256))]
    ///     messages: flux::spine::SpineQueue<u64>,
    /// }
    /// fn borrow(adapter: &mut SpineAdapter<App>) {
    ///     adapter.try_consume_with_dcache(|_: u64, bytes| bytes, |_, _| {}).unwrap();
    /// }
    /// ```
    #[inline]
    pub fn try_consume_with_dcache<T, R, F, G>(
        &mut self,
        read: F,
        mut handle: G,
    ) -> Result<bool, crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscDCacheConsumerAccess<T>,
        F: FnMut(T, &[u8]) -> R,
        G: FnMut(DCacheRead<T, R>, &mut S::Producers),
    {
        self.try_consume_with_dcache_maybe_track(read, |result, producers| {
            handle(result, producers);
            true
        })
    }

    /// Drain managed SPSC messages, recording reader and handler timings only
    /// when the handler returns true. Untracked messages still count as work
    /// and propagate ingestion time. The initial clock read is retained.
    /// SPSC ownership prevents `Lost` and `SpedPast` outcomes.
    #[inline]
    pub fn try_consume_with_dcache_maybe_track<T, R, F, G>(
        &mut self,
        mut read: F,
        mut handle: G,
    ) -> Result<bool, crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscDCacheConsumerAccess<T>,
        F: FnMut(T, &[u8]) -> R,
        G: FnMut(DCacheRead<T, R>, &mut S::Producers) -> bool,
    {
        let mut consumer = self.consumers.spsc_dcache_consumer().try_attached()?;
        let mut handled = false;
        while consumer.consume_maybe_track(
            &mut self.producers,
            &mut self.did_work,
            &mut read,
            &mut handle,
        ) {
            handled = true;
        }
        Ok(handled)
    }

    /// Consume at most one managed SPSC message, with reader and handler
    /// telemetry. The slot releases before the handler, or if the reader
    /// unwinds.
    #[inline]
    pub fn try_consume_with_dcache_one<T, R, F, G>(
        &mut self,
        read: F,
        mut handle: G,
    ) -> Result<bool, crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscDCacheConsumerAccess<T>,
        F: FnMut(T, &[u8]) -> R,
        G: FnMut(DCacheRead<T, R>, &mut S::Producers),
    {
        self.try_consume_with_dcache_one_maybe_track(read, |result, producers| {
            handle(result, producers);
            true
        })
    }

    /// Consume at most one managed SPSC message; the handler selects whether
    /// to record reader and handler timings, independently of consumption.
    #[inline]
    pub fn try_consume_with_dcache_one_maybe_track<T, R, F, G>(
        &mut self,
        mut read: F,
        mut handle: G,
    ) -> Result<bool, crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscDCacheConsumerAccess<T>,
        F: FnMut(T, &[u8]) -> R,
        G: FnMut(DCacheRead<T, R>, &mut S::Producers) -> bool,
    {
        Ok(self.consumers.spsc_dcache_consumer().try_attached()?.consume_maybe_track(
            &mut self.producers,
            &mut self.did_work,
            &mut read,
            &mut handle,
        ))
    }

    /// Consume all available SPSC messages. An empty queue is successful; a
    /// second consumer receives an attachment error.
    #[inline]
    pub fn try_consume<T, F>(
        &mut self,
        mut f: F,
    ) -> Result<(), crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscConsumerAccess<T>,
        F: FnMut(T, &mut S::Producers),
    {
        self.try_consume_maybe_track(|message, producers| {
            f(message, producers);
            true
        })
    }

    /// Drain SPSC values, recording timings only when the callback returns
    /// true. A false callback result still counts as work and does not stop
    /// the drain. Ingestion times are propagated and the initial clock read
    /// is retained.
    #[inline]
    pub fn try_consume_maybe_track<T, F>(
        &mut self,
        mut f: F,
    ) -> Result<(), crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscConsumerAccess<T>,
        F: FnMut(T, &mut S::Producers) -> bool,
    {
        self.try_consume_internal_message_maybe_track(|message, producers| {
            f(message.into_data(), producers)
        })
    }

    /// Consume at most one SPSC message, returning whether one was available.
    #[inline]
    pub fn try_consume_one<T, F>(
        &mut self,
        f: F,
    ) -> Result<bool, crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscConsumerAccess<T>,
        F: FnMut(T, &mut S::Producers),
    {
        let consumed = self.consumers.spsc_consumer().try_consume(&mut self.producers, f)?;
        self.did_work |= consumed;
        Ok(consumed)
    }

    /// Consume at most one SPSC value, recording timings only when the callback
    /// returns true. Returns whether a value was consumed; an untracked value
    /// still counts as work and propagates ingestion time. The initial clock
    /// read is retained.
    #[inline]
    pub fn try_consume_one_maybe_track<T, F>(
        &mut self,
        f: F,
    ) -> Result<bool, crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscConsumerAccess<T>,
        F: FnMut(T, &mut S::Producers) -> bool,
    {
        let consumed =
            self.consumers.spsc_consumer().try_consume_maybe_track(&mut self.producers, f)?;
        self.did_work |= consumed;
        Ok(consumed)
    }

    /// Drain SPSC payloads by reference, with consumption telemetry.
    /// Each slot stays occupied through its callback and timing records,
    /// and is released on return or unwind.
    ///
    /// Only SPSC accessors satisfy this method's bound. An MPMC queue is
    /// rejected at compile time:
    ///
    /// ```compile_fail,E0277
    /// use flux::{communication::ShmemData, spine::SpineAdapter, tile::TileInfo};
    /// use spine_derive::from_spine;
    /// #[from_spine("borrow-example")]
    /// struct App {
    ///     tile_info: ShmemData<TileInfo>,
    ///     #[queue(flavour("mpmc"))]
    ///     messages: flux::spine::SpineQueue<u64>,
    /// }
    /// fn borrow(adapter: &mut SpineAdapter<App>) {
    ///     adapter.consume_ref(|_: &u64, _| {}).unwrap();
    /// }
    /// ```
    #[inline]
    pub fn consume_ref<T, F>(
        &mut self,
        mut f: F,
    ) -> Result<(), crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscConsumerAccess<T>,
        F: FnMut(&T, &mut S::Producers),
    {
        self.consume_ref_maybe_track(|message, producers| {
            f(message, producers);
            true
        })
    }

    /// Drain borrowed SPSC payloads, recording timings when the callback
    /// returns true. Untracked values still count as work, propagate ingestion
    /// times, and take the initial clock read. Each slot stays occupied through
    /// its callback and selected records, then releases on return or unwind.
    ///
    /// Selective tracking also requires an SPSC queue:
    ///
    /// ```compile_fail,E0277
    /// use flux::{communication::ShmemData, spine::SpineAdapter, tile::TileInfo};
    /// use spine_derive::from_spine;
    /// #[from_spine("borrow-example")]
    /// struct App {
    ///     tile_info: ShmemData<TileInfo>,
    ///     #[queue(flavour("spmc"))]
    ///     messages: flux::spine::SpineQueue<u64>,
    /// }
    /// fn borrow(adapter: &mut SpineAdapter<App>) {
    ///     adapter.consume_ref_maybe_track(|_: &u64, _| false).unwrap();
    /// }
    /// ```
    #[inline]
    pub fn consume_ref_maybe_track<T, F>(
        &mut self,
        mut f: F,
    ) -> Result<(), crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscConsumerAccess<T>,
        F: FnMut(&T, &mut S::Producers) -> bool,
    {
        let mut consumer = self.consumers.spsc_consumer().try_attached()?;
        while consumer.consume_ref_maybe_track(&mut self.producers, &mut f) {
            self.did_work = true;
        }
        Ok(())
    }

    /// Borrow at most one SPSC payload, with consumption telemetry.
    /// The slot stays occupied through the callback and timing records,
    /// and is released on return or unwind. Returns false when empty.
    #[inline]
    pub fn consume_ref_one<T, F>(
        &mut self,
        f: F,
    ) -> Result<bool, crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscConsumerAccess<T>,
        F: FnOnce(&T, &mut S::Producers),
    {
        let consumed = self.consumers.spsc_consumer().consume_ref(&mut self.producers, f)?;
        self.did_work |= consumed;
        Ok(consumed)
    }

    /// Borrow at most one SPSC payload, recording timings when the callback
    /// returns true. An untracked payload still counts as work, propagates its
    /// ingestion time, and takes the initial clock read. The slot stays
    /// occupied through the callback and selected records, then releases on
    /// return or unwind. Returns whether a payload was consumed.
    #[inline]
    pub fn consume_ref_one_maybe_track<T, F>(
        &mut self,
        f: F,
    ) -> Result<bool, crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscConsumerAccess<T>,
        F: FnOnce(&T, &mut S::Producers) -> bool,
    {
        let consumed =
            self.consumers.spsc_consumer().consume_ref_maybe_track(&mut self.producers, f)?;
        self.did_work |= consumed;
        Ok(consumed)
    }

    #[inline]
    pub fn try_consume_internal_message_one<T, F>(
        &mut self,
        f: F,
    ) -> Result<bool, crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscConsumerAccess<T>,
        F: FnMut(&mut InternalMessage<T>, &mut S::Producers),
    {
        let consumed =
            self.consumers.spsc_consumer().try_consume_internal_message(&mut self.producers, f)?;
        self.did_work |= consumed;
        Ok(consumed)
    }

    /// Drain SPSC messages with their tracking metadata. A false callback
    /// result skips timing records while still counting as work and continuing
    /// the drain. Ingestion times and the initial clock read are retained.
    #[inline]
    pub fn try_consume_internal_message_maybe_track<T, F>(
        &mut self,
        mut f: F,
    ) -> Result<(), crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscConsumerAccess<T>,
        F: FnMut(&mut InternalMessage<T>, &mut S::Producers) -> bool,
    {
        let mut consumer = self.consumers.spsc_consumer().try_attached()?;
        while consumer.consume_internal_message_maybe_track(&mut self.producers, &mut f) {
            self.did_work = true;
        }
        Ok(())
    }

    /// Consume at most one SPSC message with its tracking metadata. The
    /// callback selects whether to record timings; the result reports
    /// consumption. An untracked message still counts as work and
    /// propagates ingestion time. The initial clock read is retained.
    #[inline]
    pub fn try_consume_internal_message_one_maybe_track<T, F>(
        &mut self,
        f: F,
    ) -> Result<bool, crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: SpscConsumerAccess<T>,
        F: FnMut(&mut InternalMessage<T>, &mut S::Producers) -> bool,
    {
        let consumed = self
            .consumers
            .spsc_consumer()
            .try_consume_internal_message_maybe_track(&mut self.producers, f)?;
        self.did_work |= consumed;
        Ok(consumed)
    }

    #[inline]
    pub fn produce_with_dcache<T, F>(
        &mut self,
        data: T,
        payload: Option<(usize, F)>,
    ) -> Result<(), DCacheError>
    where
        T: 'static + Copy,
        S::Producers: SpineProducers + AsRef<SpineProducerWithDCache<T>>,
        F: FnOnce(&mut [u8]),
    {
        self.producers.produce_with_dcache(data, payload)?;
        self.did_work = true;
        Ok(())
    }

    /// Subscribe to future broadcasts of `T` without consuming; repeated calls
    /// are no-ops.
    #[inline]
    pub fn subscribe_broadcast<T: 'static + Copy>(&mut self)
    where
        S::Consumers: AsMut<SpineConsumer<T>>,
    {
        let c = self.consumers.as_mut();
        c.inner.subscribe_broadcast();
    }

    #[inline]
    pub fn consume<T, F>(&mut self, mut f: F)
    where
        T: 'static + Copy,
        S::Consumers: AsMut<SpineConsumer<T>>,
        S::Producers: SpineProducers,
        F: FnMut(T, &mut S::Producers),
    {
        let c = self.consumers.as_mut();
        while c.consume(&mut self.producers, &mut f) {
            self.did_work = true;
        }
    }

    #[inline]
    pub fn consume_n<T, F>(&mut self, mut n: usize, mut f: F)
    where
        T: 'static + Copy,
        S::Consumers: AsMut<SpineConsumer<T>>,
        S::Producers: SpineProducers,
        F: FnMut(T, &mut S::Producers),
    {
        let c = self.consumers.as_mut();
        while n > 0 && c.consume(&mut self.producers, &mut f) {
            n -= 1;
            self.did_work = true;
        }
    }

    #[inline]
    pub fn consume_maybe_track<T, F>(&mut self, mut f: F)
    where
        T: 'static + Copy,
        S::Consumers: AsMut<SpineConsumer<T>>,
        S::Producers: SpineProducers,
        F: FnMut(T, &mut S::Producers) -> bool,
    {
        let c = self.consumers.as_mut();
        while c.consume_maybe_track(&mut self.producers, &mut f) {
            self.did_work = true;
        }
    }

    #[inline]
    pub fn consume_filtered<T, F, PRED>(&mut self, predicate: PRED, mut f: F)
    where
        T: 'static + Copy,
        S::Consumers: AsMut<SpineConsumer<T>>,
        S::Producers: SpineProducers,
        F: FnMut(T, &mut S::Producers),
        PRED: Fn(&T) -> bool,
    {
        let c = self.consumers.as_mut();
        while c.consume_filtered(&mut self.producers, &predicate, &mut f) {
            self.did_work = true;
        }
    }

    #[inline]
    pub fn consume_last<T, F>(&mut self, mut f: F)
    where
        T: 'static + Copy,
        S::Consumers: AsMut<SpineConsumer<T>>,
        S::Producers: SpineProducers,
        F: FnMut(T, &mut S::Producers),
    {
        let c = self.consumers.as_mut();
        if c.consume_last(&mut self.producers, &mut f) {
            self.did_work = true;
        }
    }

    #[inline]
    pub fn consume_one<T, F>(&mut self, mut f: F) -> bool
    where
        T: 'static + Copy,
        S::Consumers: AsMut<SpineConsumer<T>>,
        S::Producers: SpineProducers,
        F: FnMut(T, &mut S::Producers),
    {
        let c = self.consumers.as_mut();
        let consumed = c.consume(&mut self.producers, &mut f);
        if consumed {
            self.did_work = true;
        }
        consumed
    }

    /// Consume one item from the shared collaborative cursor.
    /// Multiple tiles on the same queue each get unique items
    /// (work-distribution).
    #[inline]
    pub fn consume_collaborative<T, F>(&mut self, mut f: F) -> bool
    where
        T: 'static + Copy,
        S::Consumers: AsMut<SpineConsumer<T>>,
        S::Producers: SpineProducers,
        F: FnMut(T, &mut S::Producers),
    {
        let c: &mut SpineConsumer<T> = self.consumers.as_mut();
        let consumed = c.consume_collaborative(&mut self.producers, &mut f);
        if consumed {
            self.did_work = true;
        }
        consumed
    }

    #[inline]
    /// Drains the queue, passing every outcome except an empty queue to
    /// `handle`. Returns whether `handle` ran at least once.
    pub fn consume_with_dcache<T, R, F, G>(&mut self, mut read: F, mut handle: G) -> bool
    where
        T: 'static + Copy,
        S::Consumers: AsMut<SpineDCacheConsumer<T>>,
        S::Producers: SpineProducers,
        F: FnMut(T, &[u8]) -> R,
        G: FnMut(DCacheRead<T, R>, &mut S::Producers),
    {
        let c: &mut SpineDCacheConsumer<T> = self.consumers.as_mut();
        let mut handled = false;
        while let Some(result) = c.consume(&mut self.producers, &mut read) {
            self.did_work |= !matches!(result, DCacheRead::SpedPast);
            handle(result, &mut self.producers);
            handled = true;
        }
        handled
    }

    #[inline]
    /// Consumes at most one message, passing it to `handle`. Returns whether
    /// `handle` ran. Single-shot `consume_with_dcache` for callers that
    /// interleave other work between messages instead of draining.
    pub fn consume_with_dcache_one<T, R, F, G>(&mut self, mut read: F, mut handle: G) -> bool
    where
        T: 'static + Copy,
        S::Consumers: AsMut<SpineDCacheConsumer<T>>,
        S::Producers: SpineProducers,
        F: FnMut(T, &[u8]) -> R,
        G: FnMut(DCacheRead<T, R>, &mut S::Producers),
    {
        let c: &mut SpineDCacheConsumer<T> = self.consumers.as_mut();
        let Some(result) = c.consume(&mut self.producers, &mut read) else {
            return false;
        };
        self.did_work |= !matches!(result, DCacheRead::SpedPast);
        handle(result, &mut self.producers);
        true
    }

    #[inline]
    /// Consumes at most one message, passing it to `handle`. Returns whether
    /// `handle` ran.
    pub fn consume_with_dcache_collaborative<T, R, F, G>(
        &mut self,
        mut read: F,
        mut handle: G,
    ) -> bool
    where
        T: 'static + Copy,
        S::Consumers: AsMut<SpineDCacheConsumer<T>>,
        S::Producers: SpineProducers,
        F: FnMut(T, &[u8], &mut S::Producers) -> R,
        G: FnMut(DCacheRead<T, R>, &mut S::Producers),
    {
        let c: &mut SpineDCacheConsumer<T> = self.consumers.as_mut();
        let Some(result) =
            c.consume_collaborative(&mut self.producers, |msg, payload, p| read(msg, payload, p))
        else {
            return false;
        };
        self.did_work |= !matches!(result, DCacheRead::SpedPast);
        handle(result, &mut self.producers);
        true
    }

    #[inline]
    /// Internal-message variant of [`Self::consume_with_dcache`].
    pub fn consume_with_dcache_internal_message<T, R, F, G>(
        &mut self,
        mut read: F,
        mut handle: G,
    ) -> bool
    where
        T: 'static + Copy,
        S::Consumers: AsMut<SpineDCacheConsumer<T>>,
        S::Producers: SpineProducers,
        F: FnMut(&InternalMessage<T>, &[u8]) -> R,
        G: FnMut(DCacheRead<InternalMessage<T>, R>, &mut S::Producers),
    {
        let c: &mut SpineDCacheConsumer<T> = self.consumers.as_mut();
        let mut handled = false;
        while let Some(result) = c.consume_internal_message(&mut self.producers, &mut read) {
            self.did_work |= !matches!(result, DCacheRead::SpedPast);
            handle(result, &mut self.producers);
            handled = true;
        }
        handled
    }

    #[inline]
    /// Internal-message variant of
    /// [`Self::consume_with_dcache_collaborative`].
    pub fn consume_with_dcache_collaborative_internal_message<T, R, F, G>(
        &mut self,
        mut read: F,
        mut handle: G,
    ) -> bool
    where
        T: 'static + Copy,
        S::Consumers: AsMut<SpineDCacheConsumer<T>>,
        S::Producers: SpineProducers,
        F: FnMut(&InternalMessage<T>, &[u8], &mut S::Producers) -> R,
        G: FnMut(DCacheRead<InternalMessage<T>, R>, &mut S::Producers),
    {
        let c: &mut SpineDCacheConsumer<T> = self.consumers.as_mut();
        let Some(result) = c
            .consume_collaborative_internal_message(&mut self.producers, |msg, payload, p| {
                read(msg, payload, p)
            })
        else {
            return false;
        };
        self.did_work |= !matches!(result, DCacheRead::SpedPast);
        handle(result, &mut self.producers);
        true
    }

    /// Override the collaborative group label for queue `T`. By default each
    /// tile instance gets a unique label (`TileType-N`) set automatically at
    /// attach time. Group label can be set in `Tile::init` to share a group
    /// across several tiles
    ///
    /// ```ignore
    /// fn init(&mut self, adapter: &mut SpineAdapter<MySpine>) {
    ///     adapter.set_collaborative_group::<OrderUpdate>("group_label");
    /// }
    /// ```
    pub fn set_collaborative_group<T: 'static + Copy>(&mut self, group_label: &'static str)
    where
        S::Consumers: AsMut<SpineConsumer<T>>,
    {
        let c: &mut SpineConsumer<T> = self.consumers.as_mut();
        c.inner.set_collaborative_group(group_label);
    }

    pub fn set_collaborative_group_dcache<T: 'static + Copy>(&mut self, group_label: &'static str)
    where
        S::Consumers: AsMut<SpineDCacheConsumer<T>>,
    {
        let c: &mut SpineDCacheConsumer<T> = self.consumers.as_mut();
        c.inner.set_collaborative_group(group_label);
    }

    #[inline]
    pub fn consume_internal_message<T: 'static + Copy, F>(&mut self, mut f: F)
    where
        S::Consumers: AsMut<SpineConsumer<T>>,
        F: FnMut(&mut InternalMessage<T>, &mut S::Producers),
    {
        let consumer = self.consumers.as_mut();
        while consumer.consume_internal_message(&mut self.producers, &mut f) {
            self.did_work = true;
        }
    }

    #[inline]
    pub fn consume_internal_message_maybe_track<T: 'static + Copy, F>(&mut self, mut f: F)
    where
        S::Consumers: AsMut<SpineConsumer<T>>,
        F: FnMut(&mut InternalMessage<T>, &mut S::Producers) -> bool,
    {
        let consumer = self.consumers.as_mut();
        while consumer.consume_internal_message_maybe_track(&mut self.producers, &mut f) {
            self.did_work = true;
        }
    }

    #[inline]
    pub fn consume_internal_message_one<T: 'static + Copy, F>(&mut self, mut f: F) -> bool
    where
        S::Consumers: AsMut<SpineConsumer<T>>,
        F: FnMut(&mut InternalMessage<T>, &mut S::Producers),
    {
        let consumer = self.consumers.as_mut();
        let consumed_message = consumer.consume_internal_message(&mut self.producers, &mut f);
        if consumed_message {
            self.did_work = true;
        }
        consumed_message
    }

    #[inline]
    pub fn consume_internal_message_last<T: 'static + Copy, F>(&mut self, mut f: F)
    where
        S::Consumers: AsMut<SpineConsumer<T>>,
        F: FnMut(&mut InternalMessage<T>, &mut S::Producers),
    {
        let consumer = self.consumers.as_mut();
        if consumer.consume_internal_message_last(&mut self.producers, &mut f) {
            self.did_work = true;
        }
    }

    #[inline]
    pub fn consume_internal_message_last_maybe_track<T: 'static + Copy, F>(&mut self, mut f: F)
    where
        S::Consumers: AsMut<SpineConsumer<T>>,
        F: FnMut(&mut InternalMessage<T>, &mut S::Producers) -> bool,
    {
        let consumer = self.consumers.as_mut();
        if consumer.consume_internal_message_last_maybe_track(&mut self.producers, &mut f) {
            self.did_work = true;
        }
    }

    #[inline]
    pub fn consume_internal_message_filtered<T: 'static + Copy, F, P>(
        &mut self,
        predicate: P,
        mut f: F,
    ) where
        S::Consumers: AsMut<SpineConsumer<T>>,
        F: FnMut(&mut InternalMessage<T>, &mut S::Producers),
        P: Fn(&InternalMessage<T>) -> bool,
    {
        let consumer = self.consumers.as_mut();
        while consumer.consume_internal_message_filtered(&mut self.producers, &predicate, &mut f) {
            self.did_work = true;
        }
    }
}
