use serde::{Deserialize, Serialize};
use type_hash_derive::TypeHash;

use crate::Instant;

//TODO: This and all other latency things should probably be using Nanos if
// multiple sockets are involved.. top 16 bits is the publisher id, bottom 48
// are the delta since ingestion
#[derive(
    Clone, Copy, Debug, PartialEq, PartialOrd, Serialize, Deserialize, Default, Hash, Eq, TypeHash,
)]
#[repr(C)]
pub struct PublishDelta(u64);
impl PublishDelta {
    pub fn new(id: u16) -> Self {
        Self((id as u64) << 48)
    }

    #[inline]
    pub fn from_ingestion_and_publish_t(&self, origin_t: Instant, publish_t: Instant) -> Self {
        let delta = (publish_t.0 - origin_t.0) & 0x0000ffffffffffff;
        Self(delta | self.0 & 0xffff000000000000)
    }

    #[inline]
    pub fn from_ingestion(&self, ingestion_t: Instant) -> Self {
        self.from_ingestion_and_publish_t(ingestion_t, Instant::now())
    }

    #[inline]
    pub fn id(&self) -> u16 {
        (self.0 >> 48) as u16
    }

    #[inline]
    pub fn delta(&self) -> Instant {
        Instant(self.0 & 0x0000ffffffffffff)
    }

    #[inline]
    pub fn tile_id(&self) -> u16 {
        self.id()
    }
}
