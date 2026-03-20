use flux_timing::{IngestionTime, InternalMessage, TrackingTimestamp};
use flux_utils::DCachePtr;

use super::{DCacheMsg, SpineProducer, SpineProducerWithDCache, SpineQueue};
use crate::communication::queue;

#[derive(Debug)]
pub struct StandaloneProducer<T: 'static + Copy> {
    producer: SpineProducer<T>,
    timestamp: TrackingTimestamp,
}

impl<T: 'static + Copy> StandaloneProducer<T> {
    pub(super) fn new(queue: SpineQueue<T>, tile_id: u16) -> Self {
        Self { producer: queue::Producer::from(queue), timestamp: TrackingTimestamp::new(tile_id) }
    }

    pub fn produce_with_ingestion(&self, d: T, ingestion_t: IngestionTime) {
        let msg = InternalMessage::new(self.timestamp.with_ingestion_t(ingestion_t), d);
        self.producer.produce_without_first(&msg);
    }
}

#[derive(Debug)]
pub struct StandaloneDCacheProducer<T: 'static + Copy> {
    inner: SpineProducerWithDCache<T>,
    timestamp: TrackingTimestamp,
}

impl<T: 'static + Copy> StandaloneDCacheProducer<T> {
    pub(super) fn new(queue: SpineQueue<DCacheMsg<T>>, dcache: DCachePtr, tile_id: u16) -> Self {
        Self {
            inner: SpineProducerWithDCache::new(queue, dcache),
            timestamp: TrackingTimestamp::new(tile_id),
        }
    }

    pub fn produce_with_ingestion<F: FnOnce(&mut [u8])>(
        &self,
        data: T,
        payload: Option<(usize, F)>,
        ingestion_t: IngestionTime,
    ) {
        let ts = self.timestamp.with_ingestion_t(ingestion_t);
        let dref =
            payload.map(|(len, write)| self.inner.dcache.write(len, write).expect("dcache write"));
        let msg = InternalMessage::new(ts, DCacheMsg::new(data, dref));
        self.inner.inner.produce_without_first(&msg);
    }
}
