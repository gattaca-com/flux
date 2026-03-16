use std::{ops::Deref, path::Path};

use flux_timing::InternalMessage;
use flux_utils::{DCacheRef, DcacheReader, short_typename};

use crate::{
    Timer,
    communication::{ReadError, queue},
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
    pub fn consume_collaborative<P, F>(&mut self, producers: &mut P, mut f: F) -> bool
    where
        P: SpineProducers,
        F: FnMut(T, &mut P),
    {
        self.inner.consume_collaborative(|m| {
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

impl<T: 'static + Copy + Into<DCacheRef>> SpineConsumer<T> {
    #[inline]
    pub fn consume_dcache<R, F>(&mut self, dcache: &DcacheReader, mut read: F) -> Option<R>
    where
        F: FnMut(&[u8]) -> R,
    {
        loop {
            match self.inner.try_consume_with_epoch() {
                Ok((msg, slot_pos, slot_ver)) => {
                    let r: DCacheRef = (*msg.data()).into();
                    let Ok(extracted) = dcache.map(r, |payload| read(payload)) else {
                        return None;
                    };
                    if self.inner.slot_version(slot_pos) != slot_ver {
                        return None;
                    }
                    return Some(extracted);
                }
                Err(ReadError::SpedPast) => {
                    self.inner.recover_after_error();
                }
                Err(ReadError::Empty) => return None,
            }
        }
    }
}

impl<T: 'static + Copy> SpineConsumer<T> {
    #[inline]
    pub fn attach<D, S, Tl>(base_dir: D, tile: &Tl, queue: SpineQueue<T>) -> Self
    where
        D: AsRef<Path>,
        S: FluxSpine,
        Tl: Tile<S>,
    {
        let label: &'static str = Box::leak(tile.name().as_str().to_owned().into_boxed_str());

        let timer = Timer::new_with_base_dir(
            base_dir,
            S::app_name(),
            format!("{}-{}", tile.name(), short_typename::<T>()),
        );

        Self { timer, inner: queue::Consumer::new(queue, label) }
    }
}
