use std::{ops::Deref, path::Path};

use flux_timing::InternalMessage;
use flux_utils::{DCachePtr, DCacheRef, safe_panic, short_typename};

use crate::{
    Timer,
    communication::{ReadError, queue},
    spine::{FluxSpine, SpineProducers, SpineQueue},
    tile::Tile,
};

#[derive(Debug)]
pub enum DCacheRead<T, R> {
    Ok((T, R)),
    /// Message consumed but no dcache ref present; payload not read.
    NoRef(T),
    /// Queue was empty.
    Empty,
    /// Consumer got sped past.
    SpedPast,
    /// A message was dequeued but the payload could not be safely read
    /// (producer lapped the consumer in either the queue seqlock or dcache).
    Lost(T),
}

/// Queue element wrapper for dcache-backed queues. Produced by
/// [`SpineProducerWithDCache`]; never constructed directly by users.
#[derive(Clone, Copy, Debug)]
pub struct DCacheMsg<T> {
    pub data: T,
    dref: DCacheRef,
}

impl<T: Copy> DCacheMsg<T> {
    pub(crate) fn new(data: T, dref: Option<DCacheRef>) -> Self {
        Self { data, dref: dref.unwrap_or(DCacheRef::NONE) }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SpineConsumer<T: 'static + Copy> {
    timer: Timer,
    pub inner: queue::Consumer<InternalMessage<T>>,
    dcache: Option<DCachePtr>,
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

impl<T: 'static + Copy> SpineConsumer<DCacheMsg<T>> {
    #[inline]
    pub fn consume_with_dcache<R, F>(&mut self, mut read: F) -> DCacheRead<T, R>
    where
        F: FnMut(&[u8]) -> R,
    {
        match self.consume_with_dcache_internal_message(|_msg, payload| read(payload)) {
            DCacheRead::Ok((msg, r)) => DCacheRead::Ok((msg.into_data(), r)),
            DCacheRead::Lost(msg) => DCacheRead::Lost(msg.into_data()),
            DCacheRead::NoRef(msg) => DCacheRead::NoRef(msg.into_data()),
            DCacheRead::Empty => DCacheRead::Empty,
            DCacheRead::SpedPast => DCacheRead::SpedPast,
        }
    }

    #[inline]
    pub fn consume_with_dcache_collaborative<R, F>(&mut self, mut read: F) -> DCacheRead<T, R>
    where
        F: FnMut(T, &[u8]) -> R,
    {
        match self
            .consume_with_dcache_collaborative_internal_message(|msg, payload| read(**msg, payload))
        {
            DCacheRead::Ok((msg, r)) => DCacheRead::Ok((msg.into_data(), r)),
            DCacheRead::Lost(msg) => DCacheRead::Lost(msg.into_data()),
            DCacheRead::NoRef(msg) => DCacheRead::NoRef(msg.into_data()),
            DCacheRead::Empty => DCacheRead::Empty,
            DCacheRead::SpedPast => DCacheRead::SpedPast,
        }
    }

    #[inline]
    pub fn consume_with_dcache_collaborative_internal_message<R, F>(
        &mut self,
        mut read: F,
    ) -> DCacheRead<InternalMessage<T>, R>
    where
        F: FnMut(&InternalMessage<T>, &[u8]) -> R,
    {
        match self.inner.try_consume_with_epoch_collaborative() {
            Ok((&msg, slot_pos, slot_ver)) => {
                let dref = msg.data().dref;
                if dref.is_none() {
                    return DCacheRead::NoRef(msg.with_data(msg.data().data));
                }
                let Some(dc) = self.dcache else {
                    safe_panic!(
                        "dcache-backed consumer has no dcache handle; use attach_with_dcache"
                    );
                    return DCacheRead::NoRef(msg.with_data(msg.data().data));
                };
                let user_msg = msg.with_data(msg.data().data);
                let Ok(extracted) = dc.map(dref, |payload| read(&user_msg, payload)) else {
                    return DCacheRead::Lost(user_msg);
                };
                if self.inner.slot_version(slot_pos) != slot_ver {
                    return DCacheRead::Lost(user_msg);
                }
                DCacheRead::Ok((user_msg, extracted))
            }
            Err(ReadError::SpedPast) => {
                self.inner.recover_collaborative_after_error();
                DCacheRead::SpedPast
            }
            Err(ReadError::Empty) => DCacheRead::Empty,
        }
    }

    #[inline]
    pub fn consume_with_dcache_internal_message<R, F>(
        &mut self,
        mut read: F,
    ) -> DCacheRead<InternalMessage<T>, R>
    where
        F: FnMut(&InternalMessage<T>, &[u8]) -> R,
    {
        loop {
            match self.inner.try_consume_with_epoch() {
                Ok((&msg, slot_pos, slot_ver)) => {
                    let dref = msg.data().dref;
                    if dref.is_none() {
                        return DCacheRead::NoRef(msg.with_data(msg.data().data));
                    }
                    let Some(dc) = self.dcache else {
                        safe_panic!(
                            "dcache-backed consumer has no dcache handle; use attach_with_dcache"
                        );
                        return DCacheRead::NoRef(msg.with_data(msg.data().data));
                    };
                    let user_msg = msg.with_data(msg.data().data);
                    let Ok(extracted) = dc.map(dref, |payload| read(&user_msg, payload)) else {
                        return DCacheRead::Lost(user_msg);
                    };
                    if self.inner.slot_version(slot_pos) != slot_ver {
                        return DCacheRead::Lost(user_msg);
                    }
                    return DCacheRead::Ok((user_msg, extracted));
                }
                Err(ReadError::SpedPast) => {
                    self.inner.recover_after_error();
                }
                Err(ReadError::Empty) => return DCacheRead::Empty,
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

        Self { timer, inner: queue::Consumer::new(queue, label), dcache: None }
    }

    #[inline]
    pub fn attach_with_dcache<D, S, Tl>(
        base_dir: D,
        tile: &Tl,
        queue: SpineQueue<T>,
        dcache: DCachePtr,
    ) -> Self
    where
        D: AsRef<Path>,
        S: FluxSpine,
        Tl: Tile<S>,
    {
        let mut s = Self::attach::<D, S, Tl>(base_dir, tile, queue);
        s.dcache = Some(dcache);
        s
    }
}
