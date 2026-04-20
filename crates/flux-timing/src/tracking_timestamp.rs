use serde::{Deserialize, Serialize};
use type_hash_derive::TypeHash;

use crate::{IngestionTime, Instant, Nanos, PublishDelta};

/// Sentinel `tile_id` for producers not registered with a spine tile.
pub const UNREGISTERED_TILE_ID: u16 = u16::MAX;

/// This is used in `InternalMessage` to track who published
/// a message when.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Default, Deserialize, TypeHash)]
#[repr(C)]
pub struct TrackingTimestamp {
    pub ingestion_t: IngestionTime,
    pub publish_delta: PublishDelta,
}

impl TrackingTimestamp {
    #[inline]
    pub fn new(id: u16) -> Self {
        Self { ingestion_t: IngestionTime::now(), publish_delta: PublishDelta::new(id) }
    }

    /// Note: this should not really be needed if Tiles are used properly
    /// throughout the system. It's purely here due to legacy reasons.
    #[inline]
    pub fn new_without_tile() -> Self {
        Self {
            ingestion_t: IngestionTime::now(),
            publish_delta: PublishDelta::new(UNREGISTERED_TILE_ID),
        }
    }

    #[inline]
    pub fn with_ingestion_t(&self, ingestion_t: IngestionTime) -> Self {
        Self {
            ingestion_t,
            publish_delta: self.publish_delta.from_ingestion(ingestion_t.internal()),
        }
    }

    #[inline]
    pub fn ingestion_t(&self) -> IngestionTime {
        self.ingestion_t
    }

    #[inline]
    pub fn ingestion_t_mut(&mut self) -> &mut IngestionTime {
        &mut self.ingestion_t
    }

    #[inline]
    pub fn publish_t(&self) -> Nanos {
        self.ingestion_t.real() + self.publish_delta.delta().as_delta_nanos()
    }

    #[inline]
    pub fn publish_t_internal(&self) -> Instant {
        self.ingestion_t.internal() + self.publish_delta.delta()
    }

    #[inline]
    pub fn with_new_publish_delta(&self) -> Self {
        Self {
            publish_delta: self.publish_delta.from_ingestion(self.ingestion_t.internal()),
            ingestion_t: self.ingestion_t,
        }
    }

    #[inline]
    pub fn tile_id(&self) -> u16 {
        self.publish_delta.tile_id()
    }

    /// From ingestion to publish
    #[inline]
    pub fn latency_until_publish(&self) -> Nanos {
        self.publish_delta.delta().as_delta_nanos()
    }
}
