use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use flux_timing::{IngestionTime, InternalMessage};
use signal_hook::consts::SIGINT;

use crate::{
    spine::{FluxSpine, SpineConsumer, SpineProducer, SpineProducers},
    tile::Tile,
};

#[derive(Debug)]
pub struct SpineAdapter<S: FluxSpine> {
    pub consumers: S::Consumers,
    pub producers: S::Producers,
    pub stop_flag: Option<Arc<AtomicUsize>>,
    did_work: bool,
}

impl<S: FluxSpine> SpineAdapter<S> {
    #[inline]
    pub fn connect_tile<Tl: Tile<S>>(tile: &Tl, spine: &mut S) -> Self {
        Self {
            consumers: spine.attach_consumers(tile),
            producers: spine.attach_producers(tile),
            stop_flag: None,
            did_work: false,
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
        }
    }

    #[inline]
    pub fn request_stop_scope(&self) {
        if let Some(f) = &self.stop_flag {
            f.store(SIGINT as usize, Ordering::Relaxed);
        }
    }

    /// Called by attach_tile before each loop_body.
    #[inline]
    pub fn begin_loop(&mut self, ingestion_t: IngestionTime) {
        self.set_ingestion_time(ingestion_t);
        self.did_work = false;
    }

    #[inline]
    pub fn did_work(&self) -> bool {
        self.did_work
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

    pub fn produce<T: Copy>(&mut self, d: T)
    where
        S::Producers: SpineProducers + AsRef<SpineProducer<T>>,
    {
        self.producers.produce(d);
        self.did_work = true;
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
        let consumed = consumer.consume_internal_message(&mut self.producers, &mut f);
        if consumed {
            self.did_work = true;
        }
        consumed
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
