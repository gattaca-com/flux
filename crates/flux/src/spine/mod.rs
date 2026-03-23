mod adapter;
mod consumer;
mod scoped;
mod standalone_producer;

use std::path::Path;

pub use adapter::SpineAdapter;
pub use consumer::{DCacheRead, SpineConsumer, SpineDCacheConsumer};
use flux_timing::{IngestionTime, InternalMessage, Nanos, TrackingTimestamp};
use flux_utils::{DCacheError, DCachePtr, DCacheRef, directories::shmem_dir};
pub use scoped::ScopedSpine;
pub use standalone_producer::{StandaloneDCacheProducer, StandaloneProducer};

use crate::{
    communication::queue::{self},
    tile::{Tile, TileName},
};

pub type SpineProducer<T> = queue::Producer<InternalMessage<T>>;
pub type SpineQueue<T> = queue::Queue<InternalMessage<T>>;

/// Wire type for dcache-backed queues. Internal to the spine; users see `T`
/// and `&[u8]` at consume sites.
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
pub struct SpineProducerWithDCache<T: 'static + Copy> {
    pub(crate) inner: SpineProducer<DCacheMsg<T>>,
    pub(crate) dcache: DCachePtr,
}

impl<T: 'static + Copy> SpineProducerWithDCache<T> {
    pub fn new(queue: SpineQueue<DCacheMsg<T>>, dcache: DCachePtr) -> Self {
        Self { inner: queue::Producer::from(queue), dcache }
    }

    pub fn dcache_ptr(&self) -> DCachePtr {
        self.dcache
    }
}

impl<T: 'static + Copy> AsRef<SpineProducer<DCacheMsg<T>>> for SpineProducerWithDCache<T> {
    fn as_ref(&self) -> &SpineProducer<DCacheMsg<T>> {
        &self.inner
    }
}

/// Implemented by spine structs for each dcache-backed queue field. Generated
/// by the `#[from_spine]` macro; used by `standalone_dcache_producer_for`.
pub trait HasDCacheQueue<T: 'static + Copy> {
    fn dcache_queue_and_ptr(&self) -> (SpineQueue<DCacheMsg<T>>, DCachePtr);
}

pub trait SpineProducers {
    fn timestamp(&self) -> &TrackingTimestamp;
    fn timestamp_mut(&mut self) -> &mut TrackingTimestamp;

    fn produce<T: Copy>(&self, d: T)
    where
        Self: AsRef<SpineProducer<T>>,
    {
        let msg = InternalMessage::new(self.timestamp().with_new_publish_delta(), d);
        self.as_ref().produce_without_first(&msg);
    }

    fn produce_with_ingestion<T: Copy>(&self, d: T, ingestion_t: IngestionTime)
    where
        Self: AsRef<SpineProducer<T>>,
    {
        let msg = InternalMessage::new(self.timestamp().with_ingestion_t(ingestion_t), d);
        self.as_ref().produce_without_first(&msg);
    }

    fn forward<T: Copy>(&self, msg: &InternalMessage<T>)
    where
        Self: AsRef<SpineProducer<T>>,
    {
        self.as_ref().produce_without_first(msg);
    }

    fn produce_with_dcache<T: 'static + Copy, F: FnOnce(&mut [u8])>(
        &self,
        data: T,
        payload: Option<(usize, F)>,
    ) -> Result<(), DCacheError>
    where
        Self: AsRef<SpineProducerWithDCache<T>>,
    {
        let ts = self.timestamp().with_new_publish_delta();
        let p: &SpineProducerWithDCache<T> = self.as_ref();
        let dref = if let Some((len, f)) = payload { Some(p.dcache.write(len, f)?) } else { None };
        let msg = InternalMessage::new(ts, DCacheMsg::new(data, dref));
        p.inner.produce_without_first(&msg);
        Ok(())
    }

    fn produce_with_dref<T: 'static + Copy>(&self, data: T, dref: DCacheRef, send_ts: Nanos)
    where
        Self: AsRef<SpineProducerWithDCache<T>>,
    {
        let ts = self.timestamp().with_ingestion_t(send_ts.into());
        let p: &SpineProducerWithDCache<T> = self.as_ref();
        let msg = InternalMessage::new(ts, DCacheMsg::new(data, Some(dref)));
        p.inner.produce_without_first(&msg);
    }

    fn produce_with_dcache_and_ingestion<T: 'static + Copy, F: FnOnce(&mut [u8])>(
        &self,
        data: T,
        payload: Option<(usize, F)>,
        ingestion_t: IngestionTime,
    ) -> Result<(), DCacheError>
    where
        Self: AsRef<SpineProducerWithDCache<T>>,
    {
        let ts = self.timestamp().with_ingestion_t(ingestion_t);
        let p: &SpineProducerWithDCache<T> = self.as_ref();
        let dref = if let Some((len, f)) = payload { Some(p.dcache.write(len, f)?) } else { None };
        let msg = InternalMessage::new(ts, DCacheMsg::new(data, dref));
        p.inner.produce_without_first(&msg);
        Ok(())
    }
}

pub trait FluxSpine: Sized + Send {
    type Consumers: Clone + Send;
    type Producers: SpineProducers + Clone + Send;

    fn attach_consumers<Tl: Tile<Self>>(&mut self, tile: &Tl) -> Self::Consumers;
    fn attach_producers<Tl: Tile<Self>>(&mut self, tile: &Tl) -> Self::Producers;
    fn new_in_base_dir(base_dir: impl AsRef<Path>) -> Self;

    fn register_tile(&mut self, name: TileName) -> u16;
    fn app_name() -> &'static str;
    fn base_dir(&self) -> &Path;

    /// Returns a [`StandaloneProducer`] for the queue of message type `T` and
    /// registers `name` as a tile entry.
    fn standalone_producer_for<T: Copy>(&mut self, name: TileName) -> StandaloneProducer<T>
    where
        Self: AsRef<SpineQueue<T>>,
    {
        let id = self.register_tile(name);
        StandaloneProducer::new(*<Self as AsRef<SpineQueue<T>>>::as_ref(self), id)
    }

    fn standalone_dcache_producer_for<T: Copy>(
        &mut self,
        name: TileName,
    ) -> StandaloneDCacheProducer<T>
    where
        Self: HasDCacheQueue<T>,
    {
        let id = self.register_tile(name);
        let (queue, dcache) = self.dcache_queue_and_ptr();
        StandaloneDCacheProducer::new(queue, dcache, id)
    }

    /// Removes all files related to a given spine. Does not clear the shared
    /// memory itself. CAUTION: this includes data files.
    fn remove_all_files() {
        let _ = std::fs::remove_dir_all(shmem_dir(Self::app_name()))
            .inspect_err(|e| tracing::error!("couldn't remove spine queues {e}"));
    }
}
