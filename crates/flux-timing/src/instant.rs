use std::ops::{Add, AddAssign, Sub};

use serde::{Deserialize, Serialize};
use type_hash_derive::TypeHash;

use crate::{
    Duration, Nanos,
    global_clock::{MULTIPLIER, global_clock_not_mocked, nanos_for_multiplier},
};

// Socket is in the top 2 bits, rdtscp counter in lower 62
#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize, Hash, PartialEq, TypeHash)]
#[repr(C)]
pub struct Instant(pub u64);
impl Instant {
    pub const MAX: Self = Self(u64::MAX);
    pub const ZERO: Self = Self(0);

    #[inline]
    pub fn now() -> Self {
        Instant(global_clock_not_mocked().raw())
    }

    #[inline]
    fn remove_socket(self) -> Self {
        Instant(self.0 & 0x3fffffffffffffff)
    }

    #[inline]
    pub fn same_socket(&self, other: &Self) -> bool {
        (self.0 & 0xc000000000000000) == (other.0 & 0xc000000000000000)
    }

    #[inline]
    pub fn elapsed(&self) -> Duration {
        let curt = Instant::now();
        curt.saturating_sub(*self)
    }

    #[inline]
    pub fn elapsed_since(&self, since: Instant) -> Duration {
        self.saturating_sub(since)
    }

    #[inline]
    pub fn as_delta_nanos(&self) -> Nanos {
        Nanos(global_clock_not_mocked().delta_as_nanos(0, self.remove_socket().0))
    }

    #[inline]
    pub fn saturating_sub(&self, other: Instant) -> Duration {
        Duration(self.0.saturating_sub(other.0))
    }
}

impl Eq for Instant {}

impl PartialOrd for Instant {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Instant {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl Sub for Instant {
    type Output = Duration;

    fn sub(self, rhs: Instant) -> Duration {
        Duration(self.0.saturating_sub(rhs.0))
    }
}

impl Add<Nanos> for Instant {
    type Output = Instant;

    fn add(self, rhs: Nanos) -> Self::Output {
        Instant(self.0 + rhs.0 * MULTIPLIER / nanos_for_multiplier())
    }
}

impl Sub<Nanos> for Instant {
    type Output = Instant;

    fn sub(self, rhs: Nanos) -> Self::Output {
        Instant(self.0.saturating_sub(rhs.0 * MULTIPLIER / nanos_for_multiplier()))
    }
}

impl Sub<Duration> for Instant {
    type Output = Instant;

    fn sub(self, rhs: Duration) -> Instant {
        Instant(self.0.saturating_sub(rhs.0))
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, rhs: Duration) -> Self::Output {
        Instant(self.0 + rhs.0)
    }
}

impl Add<Instant> for Instant {
    type Output = Instant;

    fn add(self, rhs: Instant) -> Self::Output {
        Instant(self.0 + rhs.0)
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, rhs: Duration) {
        self.0 += rhs.0
    }
}
