use serde::{Deserialize, Serialize};
use type_hash_derive::TypeHash;

use crate::{Duration, instant::Instant, nanos::Nanos};

/// A Timestamp that should be used for internal latency/performance tracking.
///
/// Should be instantiated at the very first time that a message arrives from
/// the network on the box. It takes a "real" timestamp, i.e. nanos since unix
/// epoch, and links it to the current value of the rdtscp counter.
/// When reporting internal latency/performance, the rdtscp counter alone should
/// be used. When creating an "exiting" message, i.e. a message leaving this box
/// over the network, the "real" time + linked rdtscp counter can be used to
/// generate an approximate outgoing "real" timestamp. This is approximate, but
/// 2x faster than Nanos::now() which has the same performance as
/// SystemTime::now.
#[derive(
    Clone, Copy, Debug, PartialEq, PartialOrd, Default, Serialize, Deserialize, Eq, Ord, TypeHash,
)]
#[repr(C)]
pub struct IngestionTime {
    real: Nanos,
    internal: Instant,
}

impl IngestionTime {
    #[inline]
    pub fn new(real: Nanos, internal: Instant) -> Self {
        Self { real, internal }
    }

    #[inline]
    pub fn now() -> Self {
        let real = Nanos::now();
        let internal = Instant::now();
        Self { real, internal }
    }

    #[inline]
    /// `real` of Nanos type might be mocked, but `internal` of Instant type
    /// always use real clock, so for correctness we should also mock
    /// `internal` time by decreasing it to mocked time delta
    pub fn consider_mocked_clock(&self) -> Self {
        let now = IngestionTime::now();
        Self { real: self.real, internal: now.internal - now.real().saturating_sub(self.real) }
    }

    #[inline]
    pub fn internal(&self) -> Instant {
        self.internal
    }

    #[inline]
    pub fn real(&self) -> Nanos {
        self.real
    }
}

impl From<&IngestionTime> for Instant {
    #[inline]
    fn from(value: &IngestionTime) -> Self {
        value.internal
    }
}

impl From<&IngestionTime> for Nanos {
    #[inline]
    fn from(value: &IngestionTime) -> Self {
        value.real
    }
}

impl From<IngestionTime> for Instant {
    #[inline]
    fn from(value: IngestionTime) -> Self {
        value.internal
    }
}

impl From<IngestionTime> for Nanos {
    #[inline]
    fn from(value: IngestionTime) -> Self {
        value.real
    }
}

impl From<Nanos> for IngestionTime {
    #[inline]
    fn from(value: Nanos) -> Self {
        let curt = Instant::now();
        Self { internal: curt - Duration::from(value.elapsed()), real: value }
    }
}
