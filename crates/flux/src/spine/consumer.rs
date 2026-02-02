use std::ops::Deref;

use flux_timing::InternalMessage;
use flux_utils::short_typename;

use crate::{
    Timer,
    communication::queue,
    spine::{FluxSpine, SpineProducers, SpineQueue},
    tile::Tile,
};

#[derive(Clone, Copy, Debug)]
pub struct SpineConsumer<T: 'static + Copy> {
    timer: Timer,
    pub inner: queue::Consumer<InternalMessage<T>>,
}

impl<T: 'static + Copy> Deref for SpineConsumer<T> {
    type Target = queue::Consumer<InternalMessage<T>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T: 'static + Copy> SpineConsumer<T> {
    #[inline]
    pub fn consume<P, F>(&mut self, producers: &mut P, mut f: F) -> bool
    where
        P: SpineProducers,
        F: FnMut(T, &mut P),
    {
        self.inner.consume(|m| {
            *producers.timestamp_mut().ingestion_t_mut() = m.ingestion_time();
            self.timer.start();
            f(m.into_data(), producers);
            self.timer.record_processing_and_latency_from(producers.timestamp().ingestion_t.into())
        })
    }

    #[inline]
    pub fn consume_maybe_track<P, F>(&mut self, producers: &mut P, mut f: F) -> bool
    where
        P: SpineProducers,
        F: FnMut(T, &mut P) -> bool,
    {
        self.inner.consume(|m| {
            *producers.timestamp_mut().ingestion_t_mut() = m.ingestion_time();
            self.timer.start();
            if f(m.into_data(), producers) {
                self.timer
                    .record_processing_and_latency_from(producers.timestamp().ingestion_t.into())
            }
        })
    }

    #[inline]
    pub fn consume_filtered<P, F, Pred>(
        &mut self,
        producers: &mut P,
        predicate: Pred,
        mut f: F,
    ) -> bool
    where
        P: SpineProducers,
        F: FnMut(T, &mut P),
        Pred: Fn(&T) -> bool,
    {
        self.inner.consume(|m| {
            if !predicate(m) {
                return;
            }
            *producers.timestamp_mut().ingestion_t_mut() = m.ingestion_time();
            self.timer.start();
            f(m.into_data(), producers);
            self.timer.record_processing_and_latency_from(producers.timestamp().ingestion_t.into())
        })
    }

    #[inline]
    pub fn consume_last<P, F>(&mut self, producers: &mut P, mut f: F) -> bool
    where
        P: SpineProducers,
        F: FnMut(T, &mut P),
    {
        self.inner.consume_last(|m| {
            *producers.timestamp_mut().ingestion_t_mut() = m.ingestion_time();
            self.timer.start();
            f(m.into_data(), producers);
            self.timer.record_processing_and_latency_from(producers.timestamp().ingestion_t.into())
        })
    }

    #[inline]
    pub fn consume_internal_message<P, F>(&mut self, producers: &mut P, mut f: F) -> bool
    where
        P: SpineProducers,
        F: FnMut(&mut InternalMessage<T>, &mut P),
    {
        self.inner.consume(|m| {
            *producers.timestamp_mut().ingestion_t_mut() = m.ingestion_time();
            self.timer.start();
            f(m, producers);
            self.timer.record_processing_and_latency_from(producers.timestamp().ingestion_t.into())
        })
    }

    #[inline]
    pub fn consume_internal_message_maybe_track<P, F>(
        &mut self,
        producers: &mut P,
        mut f: F,
    ) -> bool
    where
        P: SpineProducers,
        F: FnMut(&mut InternalMessage<T>, &mut P) -> bool,
    {
        self.inner.consume(|m| {
            *producers.timestamp_mut().ingestion_t_mut() = m.ingestion_time();
            self.timer.start();
            if f(m, producers) {
                self.timer
                    .record_processing_and_latency_from(producers.timestamp().ingestion_t.into())
            }
        })
    }

    #[inline]
    pub fn consume_internal_message_filtered<P, F, Pred>(
        &mut self,
        producers: &mut P,
        predicate: Pred,
        mut f: F,
    ) -> bool
    where
        P: SpineProducers,
        F: FnMut(&mut InternalMessage<T>, &mut P),
        Pred: Fn(&InternalMessage<T>) -> bool,
    {
        self.inner.consume(|m| {
            if !predicate(m) {
                return;
            }
            *producers.timestamp_mut().ingestion_t_mut() = m.ingestion_time();
            self.timer.start();
            f(m, producers);
            self.timer.record_processing_and_latency_from(producers.timestamp().ingestion_t.into())
        })
    }

    #[inline]
    pub fn consume_internal_message_last<P, F>(&mut self, producers: &mut P, mut f: F) -> bool
    where
        P: SpineProducers,
        F: FnMut(&mut InternalMessage<T>, &mut P),
    {
        self.inner.consume_last(|m| {
            *producers.timestamp_mut().ingestion_t_mut() = m.ingestion_time();
            self.timer.start();
            f(m, producers);
            self.timer.record_processing_and_latency_from(producers.timestamp().ingestion_t.into())
        })
    }

    #[inline]
    pub fn consume_internal_message_last_maybe_track<P, F>(
        &mut self,
        producers: &mut P,
        mut f: F,
    ) -> bool
    where
        P: SpineProducers,
        F: FnMut(&mut InternalMessage<T>, &mut P) -> bool,
    {
        self.inner.consume_last(|m| {
            *producers.timestamp_mut().ingestion_t_mut() = m.ingestion_time();
            self.timer.start();
            if f(m, producers) {
                self.timer
                    .record_processing_and_latency_from(producers.timestamp().ingestion_t.into())
            }
        })
    }
}

impl<T: 'static + Copy> SpineConsumer<T> {
    #[inline]
    pub fn attach<S, Tl>(tile: &Tl, queue: SpineQueue<T>) -> Self
    where
        S: FluxSpine,
        Tl: Tile<S>,
    {
        let timer = Timer::new(S::app_name(), format!("{}-{}", tile.name(), short_typename::<T>()));
        Self { timer, inner: queue::Consumer::from(queue) }
    }
}
