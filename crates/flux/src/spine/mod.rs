//! Typed queues connecting tiles.
//!
//! Select `#[queue(flavour("spsc"))]` for a bounded queue with one producer and
//! one consumer across threads or processes. Use `try_produce` and
//! `try_consume_one` (or `try_consume` to drain); `Full` leaves unread messages
//! intact. Retain pending output and retry it before consuming more input.
//! `try_consume_maybe_track` and `try_consume_one_maybe_track` let callbacks
//! return false to skip processing-time and latency records for a message.
//! Consumption and ingestion-time propagation still occur, as does the initial
//! clock read. The ordinary consume methods record timings for every message.
//! For SPSC queues, `consume_ref` and `consume_ref_one` pass `&T` directly
//! from the slot, keeping it occupied through the callback and timing records.
//! Their `maybe_track` variants select telemetry in the same way. The slot
//! releases on return or unwind, and the callback cannot retain its reference.
//! A borrowed callback must not wait for output that needs its occupied slot
//! to become free; see
//! [`crate::communication::queue::spsc::Consumer::consume_ref`].
//!
//! ```no_run
//! # #![deny(unused_imports)]
//! use flux::{
//!     communication::ShmemData,
//!     spine::{SpineAdapter, SpscProduceError},
//!     tile::TileInfo,
//! };
//! use spine_derive::from_spine;
//!
//! #[derive(Clone, Copy, Debug)]
//! #[repr(C)]
//! struct Reading(u64);
//!
//! #[from_spine("readings")]
//! struct Readings {
//!     tile_info: ShmemData<TileInfo>,
//!     #[queue(size(1024), flavour("spsc"))]
//!     readings: flux::spine::SpineQueue<Reading>,
//! }
//!
//! fn publish_pending(adapter: &mut SpineAdapter<Readings>, pending: &mut Option<Reading>) {
//!     if let Some(message) = *pending {
//!         match adapter.try_produce(message) {
//!             Ok(()) => *pending = None,
//!             Err(SpscProduceError::Full) => {}, // retry on a later loop
//!             Err(SpscProduceError::Attach(error)) => panic!("producer role: {error}"),
//!         }
//!     }
//! }
//!
//! fn read_in_place(adapter: &mut SpineAdapter<Readings>, total: &mut u64) {
//!     adapter.consume_ref(|reading: &Reading, _producers| {
//!         *total += reading.0;
//!     }).unwrap();
//!     adapter.consume_ref_maybe_track(|reading: &Reading, _producers| {
//!         *total += reading.0;
//!         reading.0 != 0
//!     }).unwrap();
//! }
//!
//! // SAFETY: participants use this exact schema and process-independent values,
//! // only access the mapping through Flux, and do not inherit endpoints via fork.
//! let spine = unsafe { Readings::new(None) };
//! ```
//!
//! SPSC endpoints are claimed lazily, or explicitly with the generated field's
//! `try_attach` in `Tile::on_attach`. Dropping a bundle releases its roles; an
//! additional producer or consumer gets an attachment error. Endpoint bundles
//! in SPSC-enabled spines cannot be cloned. Claimed roles keep their tile
//! polling even with `TileConfig::with_park()`, since parking signals are
//! process-local. A crashed owner leaves its role claimed; reset storage only
//! after all peers have detached. SPSC mappings live under
//! `app/shmem/spsc/<field>` and are not included in broadcast queue discovery.
//! `mtu` and `gather` are unsupported.
//!
//! Constructing shared SPSC storage requires acknowledging the payload
//! contract:
//!
//! ```compile_fail,E0133
//! use flux::{communication::ShmemData, tile::TileInfo};
//! use spine_derive::from_spine;
//! #[from_spine("example")]
//! struct App {
//!     tile_info: ShmemData<TileInfo>,
//!     #[queue(flavour("spsc"))]
//!     messages: flux::spine::SpineQueue<u64>,
//! }
//! let app = App::new(None);
//! ```
//!
//! Owning a producer bundle does not allow duplicating its SPSC role:
//!
//! ```compile_fail,E0599
//! use flux::{communication::ShmemData, tile::TileInfo};
//! use spine_derive::from_spine;
//! #[from_spine("example")]
//! struct App {
//!     tile_info: ShmemData<TileInfo>,
//!     #[queue(flavour("spsc"))]
//!     messages: flux::spine::SpineQueue<u64>,
//! }
//! fn duplicate(producers: AppProducers) { let copy = producers.clone(); }
//! ```

mod adapter;
mod consumer;
mod scoped;
mod spsc;
mod standalone_producer;

use std::path::Path;

pub use adapter::SpineAdapter;
pub use consumer::{DCacheRead, SpineConsumer, SpineDCacheConsumer};
use flux_timing::{IngestionTime, InternalMessage, Nanos, TrackingTimestamp};
use flux_utils::{DCacheError, DCachePtr, DCacheRef, directories::shmem_dir};
pub use scoped::ScopedSpine;
pub use spsc::{SpineSpscConsumer, SpineSpscProducer, SpineSpscQueue, SpscProduceError};
pub use standalone_producer::{StandaloneDCacheProducer, StandaloneProducer};

use crate::{
    communication::queue::{self},
    tile::{Tile, TileName},
};

pub type SpineProducer<T> = queue::Producer<InternalMessage<T>>;
pub type SpineQueue<T> = queue::Queue<InternalMessage<T>>;

#[derive(Clone, Copy, Debug, serde::Deserialize)]
pub struct QueueParams {
    pub size: usize,
}

#[derive(Clone, Copy, Debug, serde::Deserialize)]
pub struct DCacheQueueParams {
    pub size: usize,
    pub mtu: usize,
}

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

    /// Publish to an SPSC queue, returning `Full` for caller-managed retry.
    /// No message is published on error. Retain pending output in the tile
    /// when forwarding from a consume callback; consumption is not rolled back.
    fn try_produce<T: Copy>(&mut self, data: T) -> Result<(), SpscProduceError>
    where
        Self: AsMut<SpineSpscProducer<T>>,
    {
        let message = InternalMessage::new(self.timestamp().with_new_publish_delta(), data);
        self.as_mut().try_produce(&message)
    }

    fn try_produce_with_ingestion<T: Copy>(
        &mut self,
        data: T,
        ingestion_t: IngestionTime,
    ) -> Result<(), SpscProduceError>
    where
        Self: AsMut<SpineSpscProducer<T>>,
    {
        let message = InternalMessage::new(self.timestamp().with_ingestion_t(ingestion_t), data);
        self.as_mut().try_produce(&message)
    }

    /// Forward a message to an SPSC queue without changing its tracking
    /// metadata.
    fn try_forward<T: Copy>(&mut self, message: &InternalMessage<T>) -> Result<(), SpscProduceError>
    where
        Self: AsMut<SpineSpscProducer<T>>,
    {
        self.as_mut().try_produce(message)
    }

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
    type Consumers: Send;
    type Producers: SpineProducers + Send;

    fn attach_consumers<Tl: Tile<Self>>(&mut self, tile: &Tl) -> Self::Consumers;
    fn attach_producers<Tl: Tile<Self>>(&mut self, tile: &Tl) -> Self::Producers;
    /// Construct a spine with shared-memory queues.
    ///
    /// # Safety
    /// For SPSC fields, all processes must agree on payload types, layout and
    /// architecture, use process-independent values and access the mapping only
    /// through the queue interface. Inherited endpoints must not be used or
    /// dropped after `fork`. See
    /// [`SpineSpscQueue::create_or_open_shared_with_base_dir`].
    /// Broadcast-only generated spines also provide safe inherent constructors.
    unsafe fn new_in_base_dir(base_dir: impl AsRef<Path>) -> Self;

    /// Whether a tile has an endpoint whose progress requires polling.
    /// Generated spines report claimed SPSC roles, including those used
    /// directly through producer/consumer fields or from a consume
    /// callback.
    fn requires_polling(_consumers: &Self::Consumers, _producers: &Self::Producers) -> bool {
        false
    }

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

    fn dcache_ptr_for<T: 'static + Copy>(&self) -> DCachePtr
    where
        Self: HasDCacheQueue<T>,
    {
        self.dcache_queue_and_ptr().1
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

    /// Fast-forward the collaborative group `label` for queue `T` to the
    /// producer's current write head, dropping any backlog already in the
    /// ring.
    ///
    /// Call once before attaching any tile in the group — see the
    /// synchronisation note on
    /// [`crate::communication::queue::Queue::fast_forward_collaborative_group`].
    fn fast_forward_collaborative_group<T: 'static + Copy>(&self, label: &str)
    where
        Self: AsRef<SpineQueue<T>>,
    {
        let q: &SpineQueue<T> = self.as_ref();
        q.fast_forward_collaborative_group(label);
    }
}
