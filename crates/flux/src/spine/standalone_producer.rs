use flux_timing::{TrackingTimestamp, UNREGISTERED_TILE_ID};
use flux_utils::DCachePtr;

use super::{DCacheMsg, SpineProducer, SpineProducers, SpineQueue};
use crate::communication::queue;

#[derive(Debug)]
pub struct StandaloneProducer<T: 'static + Copy> {
    producer: SpineProducer<T>,
    timestamp: TrackingTimestamp,
    dcache: Option<DCachePtr>,
}

impl<T: 'static + Copy> StandaloneProducer<T> {
    pub(super) fn new(queue: SpineQueue<T>, tile_id: u16) -> Self {
        Self {
            producer: queue::Producer::from(queue),
            timestamp: TrackingTimestamp::new(tile_id),
            dcache: None,
        }
    }

    pub(super) fn new_with_dcache(queue: SpineQueue<T>, dcache: DCachePtr, tile_id: u16) -> Self {
        Self {
            producer: queue::Producer::from(queue),
            timestamp: TrackingTimestamp::new(tile_id),
            dcache: Some(dcache),
        }
    }
}

impl<T: 'static + Copy> SpineProducers for StandaloneProducer<T> {
    fn timestamp(&self) -> &TrackingTimestamp {
        &self.timestamp
    }

    fn timestamp_mut(&mut self) -> &mut TrackingTimestamp {
        &mut self.timestamp
    }
}

impl<T: 'static + Copy> AsRef<SpineProducer<T>> for StandaloneProducer<T> {
    fn as_ref(&self) -> &SpineProducer<T> {
        &self.producer
    }
}

impl<T: 'static + Copy> From<SpineQueue<T>> for StandaloneProducer<T> {
    fn from(queue: SpineQueue<T>) -> Self {
        Self::new(queue, UNREGISTERED_TILE_ID)
    }
}

impl<T: 'static + Copy> StandaloneProducer<DCacheMsg<T>> {
    pub fn produce_with_dcache<F: FnOnce(&mut [u8])>(
        &mut self,
        data: T,
        payload: Option<(usize, F)>,
    ) {
        let dref = payload.map(|(len, write)| {
            self.dcache
                .expect("producer has no dcache")
                .write(len, write)
                .expect("dcache write failed")
        });
        self.produce(DCacheMsg::new(data, dref));
    }
}
