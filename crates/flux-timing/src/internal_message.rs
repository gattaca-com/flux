use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};
use type_hash_derive::TypeHash;

use crate::{IngestionTime, Instant, Nanos, PublishDelta, TrackingTimestamp};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Default, TypeHash)]
#[repr(C)]
pub struct InternalMessage<T> {
    tracking_t: TrackingTimestamp,
    data: T,
}

impl<T: PartialEq> PartialEq for InternalMessage<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data.eq(&other.data)
    }
}
impl<T: PartialOrd> PartialOrd for InternalMessage<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.data.partial_cmp(&other.data)
    }
}

impl<T: Ord> Ord for InternalMessage<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.data.cmp(&other.data)
    }
}

impl<T: Eq> Eq for InternalMessage<T> {}

impl<T> From<T> for InternalMessage<T> {
    fn from(value: T) -> Self {
        Self {
            tracking_t: TrackingTimestamp {
                ingestion_t: IngestionTime::now(),
                publish_delta: PublishDelta::default(),
            },
            data: value,
        }
    }
}

impl<T> InternalMessage<T> {
    #[inline]
    pub fn new(tracking_t: TrackingTimestamp, data: T) -> Self {
        Self { tracking_t, data }
    }

    #[inline]
    pub fn with_data<D>(&self, data: D) -> InternalMessage<D> {
        InternalMessage::new(self.tracking_t, data)
    }

    #[inline]
    pub fn data(&self) -> &T {
        &self.data
    }

    #[inline]
    pub fn into_data(self) -> T {
        self.data
    }

    #[inline]
    pub fn map<R>(self, f: impl FnOnce(T) -> R) -> InternalMessage<R> {
        InternalMessage { tracking_t: self.tracking_t, data: f(self.data) }
    }

    #[inline]
    pub fn map_ref<R>(&self, f: impl FnOnce(&T) -> R) -> InternalMessage<R> {
        InternalMessage { tracking_t: self.tracking_t, data: f(&self.data) }
    }

    #[inline]
    pub fn unpack(self) -> (TrackingTimestamp, T) {
        (self.tracking_t, self.data)
    }

    #[inline]
    pub fn ingestion_time(&self) -> IngestionTime {
        self.tracking_t.ingestion_t
    }

    #[inline]
    pub fn tracking_timestamp(&self) -> TrackingTimestamp {
        self.tracking_t
    }

    #[inline]
    pub fn publish_t(&self) -> Nanos {
        self.tracking_t.publish_t()
    }

    #[inline]
    pub fn publish_t_internal(&self) -> Instant {
        self.tracking_t.publish_t_internal()
    }

    #[inline]
    pub fn set_publish_delta(&mut self, publish_delta: PublishDelta) {
        self.tracking_t.publish_delta = publish_delta
    }

    #[inline]
    pub fn tile_id(&self) -> u16 {
        self.tracking_t.publish_delta.tile_id()
    }

    /// From ingestion to publish
    #[inline]
    pub fn latency_e2e(&self) -> Nanos {
        self.tracking_t.latency_until_publish()
    }
}

impl<T> From<InternalMessage<T>> for (TrackingTimestamp, T) {
    #[inline]
    fn from(value: InternalMessage<T>) -> Self {
        value.unpack()
    }
}

impl<T> From<InternalMessage<T>> for (IngestionTime, T) {
    #[inline]
    fn from(value: InternalMessage<T>) -> Self {
        (value.tracking_t.ingestion_t, value.data)
    }
}

impl<T> Deref for InternalMessage<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> DerefMut for InternalMessage<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T> From<&InternalMessage<T>> for IngestionTime {
    #[inline]
    fn from(value: &InternalMessage<T>) -> Self {
        value.tracking_t.ingestion_t
    }
}

impl<T> AsRef<IngestionTime> for InternalMessage<T> {
    #[inline]
    fn as_ref(&self) -> &IngestionTime {
        &self.tracking_t.ingestion_t
    }
}

impl<T> From<&InternalMessage<T>> for Instant {
    #[inline]
    fn from(value: &InternalMessage<T>) -> Self {
        value.tracking_t.ingestion_t.into()
    }
}

impl<T> From<&InternalMessage<T>> for Nanos {
    #[inline]
    fn from(value: &InternalMessage<T>) -> Self {
        value.tracking_t.ingestion_t.into()
    }
}

impl<T> From<InternalMessage<T>> for Instant {
    #[inline]
    fn from(value: InternalMessage<T>) -> Self {
        value.tracking_t.ingestion_t.into()
    }
}

impl<T> From<InternalMessage<T>> for Nanos {
    #[inline]
    fn from(value: InternalMessage<T>) -> Self {
        value.tracking_t.ingestion_t.into()
    }
}
