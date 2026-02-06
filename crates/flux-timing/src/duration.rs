use std::ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Sub, SubAssign};

use serde::{Deserialize, Serialize};
use type_hash_derive::TypeHash;

use crate::{
    Nanos,
    global_clock::{MULTIPLIER, global_clock_not_mocked, nanos_for_multiplier},
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

    #[inline]
    pub fn from_secs(s: u64) -> Self {
        Self(s * 1_000_000_000 * MULTIPLIER / nanos_for_multiplier())
    }

    #[inline]
    pub fn from_mins(s: u64) -> Self {
        Self::from_secs(s * 60)
    }

    #[inline]
    pub fn from_secs_f64(s: f64) -> Self {
        Self::from_nanos((s * 1_000_000_000.0).round() as u64)
    }

    #[inline]
    pub fn from_millis(s: u64) -> Self {
        Self(s * 1_000_000 * MULTIPLIER / nanos_for_multiplier())
    }

    #[inline]
    pub fn from_micros(s: u64) -> Self {
        Self(s * 1_000 * MULTIPLIER / nanos_for_multiplier())
    }

    #[inline]
    pub fn from_nanos(s: u64) -> Self {
        Self(s * MULTIPLIER / nanos_for_multiplier())
    }

    #[inline]
    pub fn as_secs(&self) -> f64 {
        (self.0 * nanos_for_multiplier()) as f64 / 1_000_000_000.0 / MULTIPLIER as f64
    }

    #[inline]
    pub fn as_millis(&self) -> f64 {
        (self.0 * nanos_for_multiplier()) as f64 / 1_000_000.0 / MULTIPLIER as f64
    }

    #[inline]
    pub fn as_micros(&self) -> f64 {
        (self.0 * nanos_for_multiplier()) as f64 / 1000.0 / MULTIPLIER as f64
    }

    #[inline]
    pub fn as_micros_u128(&self) -> u128 {
        (self.0 * nanos_for_multiplier()) as u128 / 1000 / MULTIPLIER as u128
    }

    #[inline]
    pub fn as_nanos(&self) -> f64 {
        (self.0 * nanos_for_multiplier()) as f64 / MULTIPLIER as f64
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
        Self((value.as_nanos() * 100 / nanos_for_multiplier() as u128) as u64)
    }
}

impl From<Nanos> for Duration {
    #[inline]
    fn from(value: Nanos) -> Self {
        Self(value.0 * 100 / nanos_for_multiplier())
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
