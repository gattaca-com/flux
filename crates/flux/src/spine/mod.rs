mod adapter;
mod consumer;
mod scoped;

use std::path::Path;

pub use adapter::SpineAdapter;
pub use consumer::SpineConsumer;
use flux_timing::{IngestionTime, InternalMessage, TrackingTimestamp};
use flux_utils::directories::shmem_dir;
pub use scoped::ScopedSpine;

use crate::{
    communication::queue::{self},
    tile::{Tile, TileName},
};

pub type SpineProducer<T> = queue::Producer<InternalMessage<T>>;
pub type SpineQueue<T> = queue::Queue<InternalMessage<T>>;

pub trait SpineProducers {
    fn timestamp(&self) -> &TrackingTimestamp;
    fn timestamp_mut(&mut self) -> &mut TrackingTimestamp;

    fn produce<T: Copy>(&mut self, d: T)
    where
        Self: AsRef<SpineProducer<T>>,
    {
        let msg = InternalMessage::new(self.timestamp().with_new_publish_delta(), d);
        self.as_ref().produce_without_first(&msg);
    }

    fn produce_with_ingestion<T: Copy>(&mut self, d: T, ingestion_t: IngestionTime)
    where
        Self: AsRef<SpineProducer<T>>,
    {
        let msg = InternalMessage::new(self.timestamp().with_ingestion_t(ingestion_t), d);
        self.as_ref().produce_without_first(&msg);
    }

    fn forward<T: Copy>(&mut self, msg: &InternalMessage<T>)
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

    /// Removes all files related to a given spine. Does not clear the shared
    /// memory itself. CAUTION: this includes data files.
    fn remove_all_files() {
        let _ = std::fs::remove_dir_all(shmem_dir(Self::app_name()))
            .inspect_err(|e| tracing::error!("couldn't remove spine queues {e}"));
    }
}
