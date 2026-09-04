use std::{ops::Deref, path::Path};

use flux_timing::InternalMessage;
use flux_utils::{DCachePtr, short_typename};

use crate::{
    Timer,
    communication::{ReadError, queue},
    spine::{DCacheMsg, FluxSpine, SpineProducers, SpineQueue},
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
            self.timer.record_processing_and_latency_from(producers.timestamp().ingestion_t.into());
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
                    .record_processing_and_latency_from(producers.timestamp().ingestion_t.into());
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
            self.timer.record_processing_and_latency_from(producers.timestamp().ingestion_t.into());
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
            self.timer.record_processing_and_latency_from(producers.timestamp().ingestion_t.into());
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
            self.timer.record_processing_and_latency_from(producers.timestamp().ingestion_t.into());
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
            self.timer.record_processing_and_latency_from(producers.timestamp().ingestion_t.into());
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
                    .record_processing_and_latency_from(producers.timestamp().ingestion_t.into());
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
            self.timer.record_processing_and_latency_from(producers.timestamp().ingestion_t.into());
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
            self.timer.record_processing_and_latency_from(producers.timestamp().ingestion_t.into());
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
                    .record_processing_and_latency_from(producers.timestamp().ingestion_t.into());
            }
        })
    }
}

enum DCacheRead<T, R> {
    Ok((T, R)),
    NoRef(T),
    Empty,
    SpedPast,
    Lost,
}

#[derive(Clone, Copy, Debug)]
pub struct SpineDCacheConsumer<T: 'static + Copy> {
    timer: Timer,
    pub inner: queue::Consumer<InternalMessage<DCacheMsg<T>>>,
    dcache: DCachePtr,
}

impl<T: 'static + Copy> Deref for SpineDCacheConsumer<T> {
    type Target = queue::Consumer<InternalMessage<DCacheMsg<T>>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T: 'static + Copy> SpineDCacheConsumer<T> {
    #[inline]
    pub fn attach<D, S, Tl>(
        base_dir: D,
        tile: &Tl,
        queue: SpineQueue<DCacheMsg<T>>,
        dcache: DCachePtr,
    ) -> Self
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
        Self { timer, inner: queue::Consumer::new(queue, label), dcache }
    }

    /// Consumes at most one message.
    ///
    /// `read` is called only when the producer supplied a payload. `handle` is
    /// called for every intact message and receives `None` when the producer
    /// supplied no payload. Queue overruns and lost payloads are logged when
    /// logging is enabled and are not passed to `handle`.
    ///
    /// Returns whether an intact message was passed to `handle`.
    #[inline]
    pub fn consume<P, R, F, G>(&mut self, producers: &mut P, mut read: F, mut handle: G) -> bool
    where
        P: SpineProducers,
        F: FnMut(T, &[u8]) -> R,
        G: FnMut(T, Option<R>, &mut P),
    {
        self.consume_internal_message(
            producers,
            |msg, payload| read(**msg, payload),
            |msg, payload, producers| handle(msg.into_data(), payload, producers),
        )
    }

    /// Collaborative variant of [`Self::consume`].
    #[inline]
    pub fn consume_collaborative<P, R, F, G>(
        &mut self,
        producers: &mut P,
        mut read: F,
        mut handle: G,
    ) -> bool
    where
        P: SpineProducers,
        F: FnMut(T, &[u8]) -> R,
        G: FnMut(T, Option<R>, &mut P),
    {
        self.consume_collaborative_internal_message(
            producers,
            |msg, payload| read(**msg, payload),
            |msg, payload, producers| handle(msg.into_data(), payload, producers),
        )
    }

    /// Collaborative variant of [`Self::consume_internal_message`].
    #[inline]
    pub fn consume_collaborative_internal_message<P, R, F, G>(
        &mut self,
        producers: &mut P,
        mut read: F,
        mut handle: G,
    ) -> bool
    where
        P: SpineProducers,
        F: FnMut(&InternalMessage<T>, &[u8]) -> R,
        G: FnMut(InternalMessage<T>, Option<R>, &mut P),
    {
        match self.try_consume_collaborative_internal_message(producers, &mut read) {
            DCacheRead::Ok((msg, payload)) => {
                handle(msg, Some(payload), producers);
                true
            }
            DCacheRead::NoRef(msg) => {
                handle(msg, None, producers);
                true
            }
            DCacheRead::Empty | DCacheRead::SpedPast => false,
            DCacheRead::Lost => {
                self.log_payload_lost();
                false
            }
        }
    }

    #[inline]
    fn try_consume_collaborative_internal_message<P, R, F>(
        &mut self,
        producers: &mut P,
        mut read: F,
    ) -> DCacheRead<InternalMessage<T>, R>
    where
        P: SpineProducers,
        F: FnMut(&InternalMessage<T>, &[u8]) -> R,
    {
        match self.inner.try_consume_with_epoch_collaborative() {
            Ok((&msg, slot_pos, slot_ver)) => {
                let ingestion_t = msg.ingestion_time();
                *producers.timestamp_mut().ingestion_t_mut() = ingestion_t;
                let dref = msg.data().dref;
                if dref.is_none() {
                    return DCacheRead::NoRef(msg.with_data(msg.data().data));
                }
                let user_msg = msg.with_data(msg.data().data);
                self.timer.start();
                let Ok(extracted) = self.dcache.map(dref, |payload| read(&user_msg, payload))
                else {
                    return DCacheRead::Lost;
                };
                if self.inner.slot_version(slot_pos) != slot_ver {
                    return DCacheRead::Lost;
                }
                self.timer.record_processing_and_latency_from(ingestion_t.into());
                DCacheRead::Ok((user_msg, extracted))
            }
            Err(ReadError::SpedPast) => {
                self.log_sped_past(true);
                self.inner.recover_collaborative_after_error();
                DCacheRead::SpedPast
            }
            Err(ReadError::Empty) => DCacheRead::Empty,
        }
    }

    /// Like [`Self::consume`], but passes the complete internal message to
    /// both callbacks.
    #[inline]
    pub fn consume_internal_message<P, R, F, G>(
        &mut self,
        producers: &mut P,
        mut read: F,
        mut handle: G,
    ) -> bool
    where
        P: SpineProducers,
        F: FnMut(&InternalMessage<T>, &[u8]) -> R,
        G: FnMut(InternalMessage<T>, Option<R>, &mut P),
    {
        loop {
            match self.try_consume_internal_message(producers, &mut read) {
                DCacheRead::Ok((msg, payload)) => {
                    handle(msg, Some(payload), producers);
                    return true;
                }
                DCacheRead::NoRef(msg) => {
                    handle(msg, None, producers);
                    return true;
                }
                DCacheRead::Empty => return false,
                DCacheRead::SpedPast => {}
                DCacheRead::Lost => {
                    self.log_payload_lost();
                }
            }
        }
    }

    #[inline]
    fn try_consume_internal_message<P, R, F>(
        &mut self,
        producers: &mut P,
        mut read: F,
    ) -> DCacheRead<InternalMessage<T>, R>
    where
        P: SpineProducers,
        F: FnMut(&InternalMessage<T>, &[u8]) -> R,
    {
        match self.inner.try_consume_with_epoch() {
            Ok((&msg, slot_pos, slot_ver)) => {
                let ingestion_t = msg.ingestion_time();
                *producers.timestamp_mut().ingestion_t_mut() = ingestion_t;
                let dref = msg.data().dref;
                if dref.is_none() {
                    return DCacheRead::NoRef(msg.with_data(msg.data().data));
                }
                let user_msg = msg.with_data(msg.data().data);
                self.timer.start();
                let Ok(extracted) = self.dcache.map(dref, |payload| read(&user_msg, payload))
                else {
                    return DCacheRead::Lost;
                };
                if self.inner.slot_version(slot_pos) != slot_ver {
                    return DCacheRead::Lost;
                }
                self.timer.record_processing_and_latency_from(ingestion_t.into());
                DCacheRead::Ok((user_msg, extracted))
            }
            Err(ReadError::SpedPast) => {
                self.log_sped_past(false);
                self.inner.recover_after_error();
                DCacheRead::SpedPast
            }
            Err(ReadError::Empty) => DCacheRead::Empty,
        }
    }

    #[inline(never)]
    fn log_sped_past(&self, collaborative: bool) {
        if self.inner.logging_enabled() {
            let mode = if collaborative { " collaborative" } else { "" };
            flux_utils::safe_panic!(
                "SpineDCacheConsumer<{}>{mode} got sped past",
                std::any::type_name::<T>()
            );
        }
    }

    #[inline(never)]
    fn log_payload_lost(&self) {
        if self.inner.logging_enabled() {
            flux_utils::safe_panic!(
                "SpineDCacheConsumer<{}> lost a dequeued message because its payload could not be safely read",
                std::any::type_name::<T>()
            );
        }
    }
}
