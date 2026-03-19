mod adapter;
mod consumer;
mod scoped;
mod standalone_producer;

use std::path::Path;

pub use adapter::SpineAdapter;
pub use consumer::{DCacheMsg, DCacheRead, SpineConsumer};
use flux_timing::{IngestionTime, InternalMessage, TrackingTimestamp};
use flux_utils::{DCachePtr, directories::shmem_dir};
pub use scoped::ScopedSpine;
pub use standalone_producer::StandaloneProducer;

use crate::{
    communication::queue::{self},
    tile::{Tile, TileName},
};

pub type SpineProducer<T> = queue::Producer<InternalMessage<T>>;
pub type SpineQueue<T> = queue::Queue<InternalMessage<T>>;

#[derive(Clone, Copy, Debug)]
pub struct SpineProducerWithDCache<T: 'static + Copy> {
    pub(crate) inner: SpineProducer<DCacheMsg<T>>,
    pub(crate) dcache: DCachePtr,
}

impl<T: 'static + Copy> SpineProducerWithDCache<T> {
    pub fn new(queue: SpineQueue<DCacheMsg<T>>, dcache: DCachePtr) -> Self {
        Self { inner: queue::Producer::from(queue), dcache }
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
    ) -> StandaloneProducer<DCacheMsg<T>>
    where
        Self: HasDCacheQueue<T>,
    {
        let id = self.register_tile(name);
        let (queue, dcache) = self.dcache_queue_and_ptr();
        StandaloneProducer::new_with_dcache(queue, dcache, id)
    }

    /// Removes all files related to a given spine. Does not clear the shared
    /// memory itself. CAUTION: this includes data files.
    fn remove_all_files() {
        let _ = std::fs::remove_dir_all(shmem_dir(Self::app_name()))
            .inspect_err(|e| tracing::error!("couldn't remove spine queues {e}"));
    }
}
