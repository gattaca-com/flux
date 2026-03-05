use flux_timing::{TrackingTimestamp, UNREGISTERED_TILE_ID};

use super::{SpineProducer, SpineProducers, SpineQueue};
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
