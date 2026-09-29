use std::{
    ops::Deref,
    path::{Path, PathBuf},
};

use flux_timing::{Instant, InternalMessage};
use flux_utils::{DCachePtr, ShortTypename, short_typename};

use crate::{
    Timer,
    communication::ShmemData,
    communication::{ReadError, queue},
    spine::{DCacheMsg, FluxSpine, SpineProducers, SpineQueue},
    tile::{Tile, TileInfo, TileName},
};

const PRODUCER_TIMER_SLOTS: usize = 256;
const UNKNOWN_PRODUCER_TIMER_SLOT: usize = PRODUCER_TIMER_SLOTS - 1;

#[derive(Clone, Copy, Debug)]
enum ConsumerTimerKey {
    Single,
    Producer(usize),
}

#[derive(Clone, Debug)]
enum ConsumerTimers {
    Single(Timer),
    PerProducer {
        base_dir: PathBuf,
        app_name: &'static str,
        consumer_name: TileName,
        message_name: ShortTypename,
        tile_info: ShmemData<TileInfo>,
        timers: Box<[Option<Timer>; PRODUCER_TIMER_SLOTS]>,
    },
}

impl ConsumerTimers {
    fn single<D, S>(base_dir: D, consumer_name: TileName, message_name: ShortTypename) -> Self
    where
        D: AsRef<Path>,
        S: FluxSpine,
    {
        Self::Single(Self::new_timer(base_dir, S::app_name(), consumer_name, None, message_name))
    }

    fn per_producer<D, S>(
        base_dir: D,
        consumer_name: TileName,
        message_name: ShortTypename,
        tile_info: ShmemData<TileInfo>,
    ) -> Self
    where
        D: AsRef<Path>,
        S: FluxSpine,
    {
        Self::PerProducer {
            base_dir: base_dir.as_ref().to_path_buf(),
            app_name: S::app_name(),
            consumer_name,
            message_name,
            tile_info,
            timers: Box::new([None; PRODUCER_TIMER_SLOTS]),
        }
    }

    fn new_timer<D>(
        base_dir: D,
        app_name: &'static str,
        consumer_name: TileName,
        producer_name: Option<String>,
        message_name: ShortTypename,
    ) -> Timer
    where
        D: AsRef<Path>,
    {
        let name = if let Some(producer_name) = producer_name {
            format!("{consumer_name}-{producer_name}-{message_name}")
        } else {
            format!("{consumer_name}-{message_name}")
        };
        Timer::new_with_base_dir(base_dir, app_name, name)
    }

    fn producer_slot(tile_id: u16) -> usize {
        let tile_id = usize::from(tile_id);
        if tile_id < UNKNOWN_PRODUCER_TIMER_SLOT { tile_id } else { UNKNOWN_PRODUCER_TIMER_SLOT }
    }

    fn producer_name(tile_info: &TileInfo, slot: usize) -> String {
        tile_info
            .tiles
            .get(slot)
            .filter(|name| !name.is_empty())
            .map_or_else(|| format!("producer-{slot}"), ToString::to_string)
    }

    fn timer_mut(&mut self, key: ConsumerTimerKey) -> &mut Timer {
        match self {
            Self::Single(timer) => timer,
            Self::PerProducer {
                base_dir,
                app_name,
                consumer_name,
                message_name,
                tile_info,
                timers,
            } => {
                let ConsumerTimerKey::Producer(slot) = key else {
                    unreachable!("single timer key used with per-producer timers")
                };
                if timers[slot].is_none() {
                    let producer_name = if slot == UNKNOWN_PRODUCER_TIMER_SLOT {
                        "unknown-producer".to_owned()
                    } else {
                        Self::producer_name(tile_info, slot)
                    };
                    timers[slot] = Some(Self::new_timer(
                        base_dir,
                        app_name,
                        *consumer_name,
                        Some(producer_name),
                        *message_name,
                    ));
                }
                timers[slot].as_mut().expect("timer was just initialised")
            }
        }
    }

    fn start_for(&mut self, tile_id: u16) -> ConsumerTimerKey {
        let key = match self {
            Self::Single(_) => ConsumerTimerKey::Single,
            Self::PerProducer { .. } => ConsumerTimerKey::Producer(Self::producer_slot(tile_id)),
        };
        self.timer_mut(key).start();
        key
    }

    fn record_processing_and_latency_from(&mut self, key: ConsumerTimerKey, ingestion_t: Instant) {
        self.timer_mut(key).record_processing_and_latency_from(ingestion_t);
    }
}

#[derive(Clone, Debug)]
pub struct SpineConsumer<T: 'static + Copy> {
    timers: ConsumerTimers,
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
        let label = tile.name();
        let timer_label: &'static str = Box::leak(label.as_str().to_owned().into_boxed_str());
        let timers = ConsumerTimers::single::<_, S>(base_dir, label, short_typename::<T>());

        Self { timers, inner: queue::Consumer::new(queue, timer_label) }
    }

    #[inline]
    pub fn attach_with_producer_timers<D, S, Tl>(
        base_dir: D,
        tile: &Tl,
        queue: SpineQueue<T>,
        tile_info: ShmemData<TileInfo>,
    ) -> Self
    where
        D: AsRef<Path>,
        S: FluxSpine,
        Tl: Tile<S>,
    {
        let label = tile.name();
        let timer_label: &'static str = Box::leak(label.as_str().to_owned().into_boxed_str());
        let timers =
            ConsumerTimers::per_producer::<_, S>(base_dir, label, short_typename::<T>(), tile_info);

        Self { timers, inner: queue::Consumer::new(queue, timer_label) }
    }

    #[inline]
    pub fn consume<P, F>(&mut self, producers: &mut P, mut f: F) -> bool
    where
        P: SpineProducers,
        F: FnMut(T, &mut P),
    {
        self.inner.consume(|m| {
            *producers.timestamp_mut().ingestion_t_mut() = m.ingestion_time();
            let timer = self.timers.start_for(m.tile_id());
            f(m.into_data(), producers);
            self.timers.record_processing_and_latency_from(
                timer,
                producers.timestamp().ingestion_t.into(),
            );
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
            let timer = self.timers.start_for(m.tile_id());
            if f(m.into_data(), producers) {
                self.timers.record_processing_and_latency_from(
                    timer,
                    producers.timestamp().ingestion_t.into(),
                );
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
            let timer = self.timers.start_for(m.tile_id());
            f(m.into_data(), producers);
            self.timers.record_processing_and_latency_from(
                timer,
                producers.timestamp().ingestion_t.into(),
            );
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
            let timer = self.timers.start_for(m.tile_id());
            f(m.into_data(), producers);
            self.timers.record_processing_and_latency_from(
                timer,
                producers.timestamp().ingestion_t.into(),
            );
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
            let timer = self.timers.start_for(m.tile_id());
            f(m.into_data(), producers);
            self.timers.record_processing_and_latency_from(
                timer,
                producers.timestamp().ingestion_t.into(),
            );
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
            let timer = self.timers.start_for(m.tile_id());
            f(m, producers);
            self.timers.record_processing_and_latency_from(
                timer,
                producers.timestamp().ingestion_t.into(),
            );
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
            let timer = self.timers.start_for(m.tile_id());
            if f(m, producers) {
                self.timers.record_processing_and_latency_from(
                    timer,
                    producers.timestamp().ingestion_t.into(),
                );
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
            let timer = self.timers.start_for(m.tile_id());
            f(m, producers);
            self.timers.record_processing_and_latency_from(
                timer,
                producers.timestamp().ingestion_t.into(),
            );
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
            let timer = self.timers.start_for(m.tile_id());
            f(m, producers);
            self.timers.record_processing_and_latency_from(
                timer,
                producers.timestamp().ingestion_t.into(),
            );
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
            let timer = self.timers.start_for(m.tile_id());
            if f(m, producers) {
                self.timers.record_processing_and_latency_from(
                    timer,
                    producers.timestamp().ingestion_t.into(),
                );
            }
        })
    }
}

#[derive(Debug)]
pub enum DCacheRead<T, R> {
    Ok((T, R)),
    /// Message consumed but no dcache ref present; payload not read.
    NoRef(T),
    /// Consumer got sped past.
    SpedPast,
    /// A message was dequeued but the payload could not be safely read
    /// (producer lapped the consumer in either the queue seqlock or dcache).
    Lost(T),
}

#[derive(Clone, Debug)]
pub struct SpineDCacheConsumer<T: 'static + Copy> {
    timers: ConsumerTimers,
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
        let label = tile.name();
        let timer_label: &'static str = Box::leak(label.as_str().to_owned().into_boxed_str());
        let timers = ConsumerTimers::single::<_, S>(base_dir, label, short_typename::<T>());
        Self { timers, inner: queue::Consumer::new(queue, timer_label), dcache }
    }

    #[inline]
    pub fn attach_with_producer_timers<D, S, Tl>(
        base_dir: D,
        tile: &Tl,
        queue: SpineQueue<DCacheMsg<T>>,
        dcache: DCachePtr,
        tile_info: ShmemData<TileInfo>,
    ) -> Self
    where
        D: AsRef<Path>,
        S: FluxSpine,
        Tl: Tile<S>,
    {
        let label = tile.name();
        let timer_label: &'static str = Box::leak(label.as_str().to_owned().into_boxed_str());
        let timers =
            ConsumerTimers::per_producer::<_, S>(base_dir, label, short_typename::<T>(), tile_info);
        Self { timers, inner: queue::Consumer::new(queue, timer_label), dcache }
    }

    #[inline]
    pub(crate) fn consume<P, R, F>(
        &mut self,
        producers: &mut P,
        mut read: F,
    ) -> Option<DCacheRead<T, R>>
    where
        P: SpineProducers,
        F: FnMut(T, &[u8]) -> R,
    {
        self.consume_internal_message(producers, |msg, payload| read(**msg, payload)).map(|res| {
            match res {
                DCacheRead::Ok((msg, r)) => DCacheRead::Ok((msg.into_data(), r)),
                DCacheRead::Lost(msg) => DCacheRead::Lost(msg.into_data()),
                DCacheRead::NoRef(msg) => DCacheRead::NoRef(msg.into_data()),
                DCacheRead::SpedPast => DCacheRead::SpedPast,
            }
        })
    }

    #[inline]
    pub(crate) fn consume_collaborative<P, R, F>(
        &mut self,
        producers: &mut P,
        mut read: F,
    ) -> Option<DCacheRead<T, R>>
    where
        P: SpineProducers,
        F: FnMut(T, &[u8], &mut P) -> R,
    {
        self.consume_collaborative_internal_message(producers, |msg, payload, p| {
            read(**msg, payload, p)
        })
        .map(|res| match res {
            DCacheRead::Ok((msg, r)) => DCacheRead::Ok((msg.into_data(), r)),
            DCacheRead::Lost(msg) => DCacheRead::Lost(msg.into_data()),
            DCacheRead::NoRef(msg) => DCacheRead::NoRef(msg.into_data()),
            DCacheRead::SpedPast => DCacheRead::SpedPast,
        })
    }

    #[inline]
    pub(crate) fn consume_collaborative_internal_message<P, R, F>(
        &mut self,
        producers: &mut P,
        mut read: F,
    ) -> Option<DCacheRead<InternalMessage<T>, R>>
    where
        P: SpineProducers,
        F: FnMut(&InternalMessage<T>, &[u8], &mut P) -> R,
    {
        Some(match self.inner.try_consume_with_epoch_collaborative() {
            Ok((&msg, slot_pos, slot_ver)) => {
                let ingestion_t = msg.ingestion_time();
                *producers.timestamp_mut().ingestion_t_mut() = ingestion_t;
                let dref = msg.data().dref;
                if dref.is_none() {
                    return Some(DCacheRead::NoRef(msg.with_data(msg.data().data)));
                }
                let user_msg = msg.with_data(msg.data().data);
                let timer = self.timers.start_for(msg.tile_id());
                let Ok(extracted) =
                    self.dcache.map(dref, |payload| read(&user_msg, payload, &mut *producers))
                else {
                    return Some(DCacheRead::Lost(user_msg));
                };
                if self.inner.slot_version(slot_pos) != slot_ver {
                    return Some(DCacheRead::Lost(user_msg));
                }
                self.timers.record_processing_and_latency_from(timer, ingestion_t.into());
                DCacheRead::Ok((user_msg, extracted))
            }
            Err(ReadError::SpedPast) => {
                self.inner.recover_collaborative_after_error();
                DCacheRead::SpedPast
            }
            Err(ReadError::Empty) => return None,
        })
    }

    #[inline]
    pub(crate) fn consume_internal_message<P, R, F>(
        &mut self,
        producers: &mut P,
        mut read: F,
    ) -> Option<DCacheRead<InternalMessage<T>, R>>
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
                    return Some(DCacheRead::NoRef(msg.with_data(msg.data().data)));
                }
                let user_msg = msg.with_data(msg.data().data);
                let timer = self.timers.start_for(msg.tile_id());
                let Ok(extracted) = self.dcache.map(dref, |payload| read(&user_msg, payload))
                else {
                    return Some(DCacheRead::Lost(user_msg));
                };
                if self.inner.slot_version(slot_pos) != slot_ver {
                    return Some(DCacheRead::Lost(user_msg));
                }
                self.timers.record_processing_and_latency_from(timer, ingestion_t.into());
                Some(DCacheRead::Ok((user_msg, extracted)))
            }
            Err(ReadError::SpedPast) => {
                self.inner.recover_after_error();
                Some(DCacheRead::SpedPast)
            }
            Err(ReadError::Empty) => None,
        }
    }
}
