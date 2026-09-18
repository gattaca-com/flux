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
        SpineProducerWithDCache, SpineProducers, SpineSpscConsumer, SpineSpscProducer,
        SpscProduceError,
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
        S::Producers: AsMut<SpineSpscProducer<T>>,
    {
        self.producers.try_produce(data)?;
        self.did_work = true;
        Ok(())
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
        S::Consumers: AsMut<SpineSpscConsumer<T>>,
        F: FnMut(T, &mut S::Producers),
    {
        while self.try_consume_one(&mut f)? {}
        Ok(())
    }

    /// Consume at most one SPSC message, returning whether one was available.
    #[inline]
    pub fn try_consume_one<T, F>(
        &mut self,
        f: F,
    ) -> Result<bool, crate::communication::queue::spsc::QueueError>
    where
        T: 'static + Copy,
        S::Consumers: AsMut<SpineSpscConsumer<T>>,
        F: FnMut(T, &mut S::Producers),
    {
        let consumed = self.consumers.as_mut().try_consume(&mut self.producers, f)?;
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
        S::Consumers: AsMut<SpineSpscConsumer<T>>,
        F: FnMut(&mut InternalMessage<T>, &mut S::Producers),
    {
        let consumed =
            self.consumers.as_mut().try_consume_internal_message(&mut self.producers, f)?;
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
