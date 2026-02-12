use std::ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Sub, SubAssign};

use serde::{Deserialize, Serialize};
use type_hash_derive::TypeHash;

use crate::{
    Nanos,
    global_clock::{global_clock_not_mocked, ticks_per_micro, ticks_per_milli, ticks_per_sec},
};

#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize, TypeHash)]
#[repr(C)]
pub struct Duration(pub u64);

impl Duration {
    pub const MAX: Self = Self(u64::MAX);
    pub const ZERO: Self = Self(0);
    pub const MIN: Self = Self(0);
    pub const MILLIS_10: Self = Self::from_millis_and_MHz(10, 3_200);
    pub const MILLIS_5: Self = Self::from_millis_and_MHz(5, 3_200);

    /// Given millis and the base clockspeed as multiplier this generates an
    /// approximate Duration in rdtscp count
    #[inline]
    #[allow(non_snake_case)]
    pub const fn from_millis_and_MHz(millis: u64, mhz: u64) -> Self {
        Self(1000 * mhz * millis)
    }

    #[inline]
    pub fn saturating_sub(self, rhs: Duration) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }

    #[inline]
    pub fn saturating_add(self, rhs: Duration) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }

    /// Overflows at ~182 years.
    #[inline]
    pub fn from_secs(s: u64) -> Self {
        Self(s * ticks_per_sec())
    }

    /// Overflows at ~182 years.
    #[inline]
    pub fn from_mins(s: u64) -> Self {
        Self::from_secs(s * 60)
    }

    #[inline]
    pub fn from_secs_f64(s: f64) -> Self {
        Self::from_nanos((s * 1_000_000_000.0).round() as u64)
    }

    /// Overflows at ~182 years.
    #[inline]
    pub fn from_millis(s: u64) -> Self {
        Self(s * ticks_per_milli())
    }

    /// Overflows at ~66 days.
    #[inline]
    pub fn from_micros(s: u64) -> Self {
        Self(s * ticks_per_milli() / 1_000)
    }

    /// Overflows at ~66 days.
    #[inline]
    pub fn from_nanos(s: u64) -> Self {
        Self(s * ticks_per_micro() / 1000)
    }

    #[inline]
    pub fn as_secs(&self) -> f64 {
        self.0 as f64 / ticks_per_sec() as f64
    }

    #[inline]
    pub fn as_secs_u64(&self) -> u64 {
        self.0 / ticks_per_sec()
    }

    #[inline]
    pub fn as_millis(&self) -> f64 {
        self.0 as f64 / ticks_per_milli() as f64
    }

    #[inline]
    pub fn as_millis_u64(&self) -> u64 {
        self.0 / ticks_per_milli()
    }

    #[inline]
    pub fn as_micros(&self) -> f64 {
        self.0 as f64 * 1_000.0 / ticks_per_milli() as f64
    }

    #[inline]
    pub fn as_micros_u64(&self) -> u64 {
        self.0 / ticks_per_micro()
    }

    #[inline]
    pub fn as_micros_u128(&self) -> u128 {
        (self.0 / ticks_per_micro()) as u128
    }

    #[inline]
    pub fn as_nanos(&self) -> f64 {
        self.0 as f64 * 1000.0 / ticks_per_micro() as f64
    }
}

impl std::fmt::Display for Duration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Nanos(global_clock_not_mocked().delta_as_nanos(0, self.0)).fmt(f)
    }
}

impl From<u64> for Duration {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Duration> for u64 {
    fn from(value: Duration) -> Self {
        value.0
    }
}

impl Add for Duration {
    type Output = Duration;

    #[inline]
    fn add(self, rhs: Duration) -> Duration {
        Duration(self.0.wrapping_add(rhs.0))
    }
}

impl AddAssign for Duration {
    #[inline]
    fn add_assign(&mut self, rhs: Duration) {
        *self = *self + rhs;
    }
}

impl Sub for Duration {
    type Output = Duration;

    #[inline]
    fn sub(self, rhs: Duration) -> Duration {
        Duration(self.0.wrapping_sub(rhs.0))
    }
}

impl SubAssign for Duration {
    #[inline]
    fn sub_assign(&mut self, rhs: Duration) {
        *self = *self - rhs;
    }
}

impl Sub<u64> for Duration {
    type Output = Duration;

    #[inline]
    fn sub(self, rhs: u64) -> Duration {
        Duration(self.0.wrapping_sub(rhs))
    }
}

impl SubAssign<u64> for Duration {
    #[inline]
    fn sub_assign(&mut self, rhs: u64) {
        *self = *self - rhs;
    }
}

impl Mul<u32> for Duration {
    type Output = Duration;

    #[inline]
    fn mul(self, rhs: u32) -> Duration {
        Duration(self.0 * rhs as u64)
    }
}

impl Mul<usize> for Duration {
    type Output = Duration;

    #[inline]
    fn mul(self, rhs: usize) -> Duration {
        Duration(self.0 * rhs as u64)
    }
}

impl Mul<Duration> for u32 {
    type Output = Duration;

    #[inline]
    fn mul(self, rhs: Duration) -> Duration {
        rhs * self
    }
}

impl MulAssign<u32> for Duration {
    #[inline]
    fn mul_assign(&mut self, rhs: u32) {
        *self = *self * rhs;
    }
}

impl Div<u32> for Duration {
    type Output = Duration;

    #[inline]
    fn div(self, rhs: u32) -> Duration {
        Duration(self.0 / rhs as u64)
    }
}

impl Div<usize> for Duration {
    type Output = Duration;

    #[inline]
    fn div(self, rhs: usize) -> Duration {
        Duration(self.0 / rhs as u64)
    }
}

impl Div<Duration> for Duration {
    type Output = u64;

    #[inline]
    fn div(self, rhs: Duration) -> u64 {
        self.0 / rhs.0
    }
}

impl DivAssign<u32> for Duration {
    #[inline]
    fn div_assign(&mut self, rhs: u32) {
        *self = *self / rhs;
    }
}

impl Mul<u64> for Duration {
    type Output = Duration;

    #[inline]
    fn mul(self, rhs: u64) -> Duration {
        Duration(self.0 * rhs)
    }
}

impl Mul<Duration> for u64 {
    type Output = Duration;

    #[inline]
    fn mul(self, rhs: Duration) -> Duration {
        rhs * self
    }
}

impl MulAssign<u64> for Duration {
    #[inline]
    fn mul_assign(&mut self, rhs: u64) {
        *self = *self * rhs;
    }
}

impl Div<u64> for Duration {
    type Output = Duration;

    #[inline]
    fn div(self, rhs: u64) -> Duration {
        Duration(self.0 / rhs)
    }
}

impl DivAssign<u64> for Duration {
    #[inline]
    fn div_assign(&mut self, rhs: u64) {
        *self = *self / rhs;
    }
}

impl PartialEq for Duration {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Duration {}

impl PartialOrd for Duration {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Duration {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl From<Duration> for f64 {
    #[inline]
    fn from(value: Duration) -> f64 {
        value.0 as f64
    }
}

impl std::iter::Sum for Duration {
    #[inline]
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Duration(iter.map(|v| v.0).sum())
    }
}

impl<'a> std::iter::Sum<&'a Self> for Duration {
    #[inline]
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = &'a Self>,
    {
        Duration(iter.map(|v| v.0).sum())
    }
}

impl From<u128> for Duration {
    #[inline]
    fn from(value: u128) -> Self {
        Duration(value as u64)
    }
}

impl From<u32> for Duration {
    #[inline]
    fn from(value: u32) -> Self {
        Duration(value as u64)
    }
}

impl From<i64> for Duration {
    #[inline]
    fn from(value: i64) -> Self {
        Duration(value as u64)
    }
}

impl From<i32> for Duration {
    #[inline]
    fn from(value: i32) -> Self {
        Duration(value as u64)
    }
}

impl From<Duration> for i64 {
    #[inline]
    fn from(val: Duration) -> Self {
        val.0 as i64
    }
}

impl From<Duration> for std::time::Duration {
    #[inline]
    fn from(value: Duration) -> Self {
        std::time::Duration::from_nanos(global_clock_not_mocked().delta_as_nanos(0, value.0))
    }
}

impl From<std::time::Duration> for Duration {
    #[inline]
    fn from(value: std::time::Duration) -> Self {
        Self::from_secs(value.as_secs()) + Self::from_nanos(value.subsec_nanos() as u64)
    }
}

impl From<Nanos> for Duration {
    /// Overflows at ~66 days.
    #[inline]
    fn from(value: Nanos) -> Self {
        Self(value.0 * ticks_per_micro() / 1000)
    }
}

impl DivAssign<usize> for Duration {
    #[inline]
    fn div_assign(&mut self, rhs: usize) {
        self.0 /= rhs as u64
    }
}

impl DivAssign<i32> for Duration {
    #[inline]
    fn div_assign(&mut self, rhs: i32) {
        self.0 /= rhs as u64
    }
}

impl Div<i32> for Duration {
    type Output = Self;
    #[inline]
    fn div(self, rhs: i32) -> Self {
        Self(self.0 / rhs as u64)
    }
}

impl Mul<i32> for Duration {
    type Output = Self;
    #[inline]
    fn mul(self, rhs: i32) -> Self {
        Self(self.0 * rhs as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_ns(d: Duration) -> u128 {
        std::time::Duration::from(d).as_nanos()
    }

    /// Assert drift < 500ppm of expected, minimum 10ns absolute.
    /// from_nanos path uses ticks_per_micro (~3200) so truncation can reach
    /// ~200ppm.
    fn check(ours: Duration, expected: std::time::Duration) {
        let actual_ns = to_ns(ours);
        let expected_ns = expected.as_nanos();
        let diff = (actual_ns as i128 - expected_ns as i128).unsigned_abs();
        let max_err = (expected_ns / 2_000).max(10); // 500ppm or 10ns
        assert!(
            diff <= max_err,
            "drift {diff}ns > {max_err}ns (500ppm): actual={actual_ns}ns expected={expected_ns}ns"
        );
    }

    #[test]
    fn from_secs_matches_std() {
        for s in [0, 1, 5, 60, 3600, 86400, 604800] {
            check(Duration::from_secs(s), std::time::Duration::from_secs(s));
        }
    }

    #[test]
    fn from_millis_matches_std() {
        for ms in [0, 1, 10, 100, 500, 1_000, 30_000, 60_000] {
            check(Duration::from_millis(ms), std::time::Duration::from_millis(ms));
        }
    }

    #[test]
    fn from_micros_matches_std() {
        for us in [0, 1, 10, 100, 500, 1_000, 10_000, 500_000, 1_000_000] {
            check(Duration::from_micros(us), std::time::Duration::from_micros(us));
        }
    }

    #[test]
    fn from_nanos_matches_std() {
        for ns in [0, 1, 100, 1_000, 10_000, 100_000, 1_000_000, 1_000_000_000] {
            check(Duration::from_nanos(ns), std::time::Duration::from_nanos(ns));
        }
    }

    #[test]
    fn from_std_roundtrip() {
        for std_dur in [
            std::time::Duration::from_millis(1),
            std::time::Duration::from_micros(500),
            std::time::Duration::from_secs(10),
            std::time::Duration::new(1, 500_000),
        ] {
            let ours: Duration = std_dur.into();
            check(ours, std_dur);
        }
    }

    #[test]
    fn from_secs_f64_matches_std() {
        for s in [0.001, 0.5, 1.0, 1.5, 60.0] {
            check(Duration::from_secs_f64(s), std::time::Duration::from_secs_f64(s));
        }
    }
}
