use std::ops::{Add, AddAssign, Sub};

use serde::{Deserialize, Serialize};
use type_hash_derive::TypeHash;

use crate::{
    Duration, Nanos,
    global_clock::{global_clock_not_mocked, ticks_per_micro},
};

/// Bit position of the socket tag inside an [`Instant`] word.
pub const SOCKET_SHIFT: u32 = 62;
/// Low 62 bits: the invariant-TSC counter.
pub const TSC_MASK: u64 = (1 << SOCKET_SHIFT) - 1;
/// Top 2 bits: the producing socket (NUMA node) id.
pub const SOCKET_MASK: u64 = !TSC_MASK;

/// Reads the invariant TSC and the calling core's NUMA node in one `rdtscp`,
/// both raw — the caller decides how to pack/mask them.
///
/// Linux programs `IA32_TSC_AUX` as `(numa_node << 12) | cpu`, so the node
/// falls out of the same instruction. Nodes nest within a socket and share its
/// TSC, so the node is a safe (finer) proxy for the TSC coherence domain.
#[cfg(target_arch = "x86_64")]
#[inline]
pub fn read_tsc_and_node() -> (u64, u16) {
    let mut aux: u32 = 0;
    // SAFETY: rdtscp is present on every x86_64 part carrying the invariant TSC
    // this crate's clock already depends on; `aux` is a valid out pointer.
    let tsc = unsafe { core::arch::x86_64::__rdtscp(&raw mut aux) };
    (tsc, (aux >> 12) as u16)
}

/// Portable fallback: no per-core node tag, counter from the calibrated clock.
#[cfg(not(target_arch = "x86_64"))]
#[inline]
pub fn read_tsc_and_node() -> (u64, u16) {
    (global_clock_not_mocked().raw(), 0)
}

// Socket is in the top 2 bits, rdtscp counter in lower 62
#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize, Hash, PartialEq, TypeHash)]
#[repr(C)]
pub struct Instant(pub u64);
impl Instant {
    pub const MAX: Self = Self(u64::MAX);
    pub const ZERO: Self = Self(0);

    #[inline]
    pub fn now() -> Self {
        Self(global_clock_not_mocked().raw())
    }

    #[inline]
    fn remove_socket(self) -> Self {
        Self(self.0 & TSC_MASK)
    }

    #[inline]
    pub fn same_socket(&self, other: &Self) -> bool {
        (self.0 & SOCKET_MASK) == (other.0 & SOCKET_MASK)
    }

    #[inline]
    pub fn elapsed(&self) -> Duration {
        let curt = Self::now();
        curt.saturating_sub(*self)
    }

    #[inline]
    pub fn elapsed_since(&self, since: Self) -> Duration {
        self.saturating_sub(since)
    }

    #[inline]
    pub fn as_delta_nanos(&self) -> Nanos {
        Nanos(global_clock_not_mocked().delta_as_nanos(0, self.remove_socket().0))
    }

    #[inline]
    pub fn saturating_sub(&self, other: Self) -> Duration {
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

    fn sub(self, rhs: Self) -> Duration {
        Duration(self.0.saturating_sub(rhs.0))
    }
}

impl Add<Nanos> for Instant {
    type Output = Self;

    fn add(self, rhs: Nanos) -> Self::Output {
        Self(self.0 + rhs.0 * ticks_per_micro() / 1000)
    }
}

impl Sub<Nanos> for Instant {
    type Output = Self;

    fn sub(self, rhs: Nanos) -> Self::Output {
        Self(self.0.saturating_sub(rhs.0 * ticks_per_micro() / 1000))
    }
}

impl Sub<Duration> for Instant {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }
}

impl Add<Duration> for Instant {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl Add<Self> for Instant {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, rhs: Duration) {
        self.0 += rhs.0;
    }
}
