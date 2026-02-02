use std::{
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Sub, SubAssign},
    str::FromStr,
};

use chrono::{SecondsFormat, Utc};
use humantime::{Duration as HumanDuration, DurationError as HumanDurationError};
use quanta::IntoNanoseconds;
use serde::{Deserializer, Serialize};
use tracing::warn;
use type_hash_derive::TypeHash;

use crate::{
    Duration,
    global_clock::{global_clock, global_clock_not_mocked},
};

/// Nanos since unix epoch, good till 2554 or so
#[derive(Copy, Clone, Debug, Default, Serialize, Hash, PartialEq, TypeHash)]
#[cfg_attr(feature = "wincode", derive(wincode_derive::SchemaRead, wincode_derive::SchemaWrite,))]
#[repr(C)]
pub struct Nanos(pub u64);

impl Nanos {
    pub const MAX: Nanos = Nanos(u64::MAX);
    pub const ZERO: Nanos = Nanos(0);

    #[inline]
    pub const fn from_secs(s: u64) -> Self {
        Nanos(s * 1_000_000_000)
    }

    #[inline]
    pub const fn from_months(s: u64) -> Self {
        Self::from_secs(s * 2629800)
    }

    #[inline]
    pub fn from_secs_f64(s: f64) -> Self {
        Nanos((s * 1_000_000_000.0).round() as u64)
    }

    #[inline]
    pub fn from_millis_f64(s: f64) -> Self {
        Nanos((s * 1_000_000.0).round() as u64)
    }

    #[inline]
    pub const fn from_millis(s: u64) -> Self {
        Nanos(s * 1_000_000)
    }

    #[inline]
    pub const fn from_micros(s: u64) -> Self {
        Nanos(s * 1_000)
    }

    #[inline]
    pub fn from_micros_f64(s: f64) -> Self {
        Nanos((s * 1_000.0).round() as u64)
    }

    #[inline]
    pub const fn from_mins(s: u64) -> Self {
        Nanos(s * 60 * 1_000_000_000)
    }

    #[inline]
    pub const fn from_hours(s: u64) -> Self {
        Nanos::from_mins(s * 60)
    }

    pub fn from_rfc3339(datetime_str: &str) -> Option<Self> {
        match chrono::DateTime::parse_from_rfc3339(datetime_str)
            .map(|d| d.timestamp_nanos_opt().map(Self::from))
        {
            Ok(Some(n)) => Some(n),
            Ok(None) => {
                warn!("timestamp out of nanoseconds reach, using ingestion time");
                None
            }
            Err(e) => {
                warn!("Couldn't parse timestamp {}: {e}", datetime_str);
                None
            }
        }
    }

    #[inline]
    pub fn as_secs(&self) -> f64 {
        self.0 as f64 / 1_000_000_000.0
    }

    #[inline]
    pub fn as_millis(&self) -> f64 {
        self.0 as f64 / 1_000_000.0
    }

    #[inline]
    pub fn as_millis_u64(&self) -> u64 {
        self.0 / 1_000_000
    }

    #[inline]
    pub fn as_micros(&self) -> f64 {
        self.0 as f64 / 1_000.0
    }

    #[inline]
    pub fn now() -> Self {
        global_clock().now()
    }

    #[inline]
    pub fn saturating_sub(self, rhs: Nanos) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }

    #[inline]
    pub fn elapsed(&self) -> Self {
        let curt = Self::now();
        Nanos(curt.0 - self.0)
    }

    #[inline]
    pub fn elapsed_saturating(&self) -> Self {
        let curt = Self::now();
        Nanos(curt.0.saturating_sub(self.0))
    }

    #[inline]
    pub fn elapsed_since(&self, since: Self) -> Self {
        Nanos(self.0.saturating_sub(since.0))
    }

    pub fn with_fmt_utc(&self, fmt: &str) -> String {
        chrono::DateTime::<Utc>::from(*self).format(fmt).to_string()
    }

    pub fn to_rfc3339_utc(&self, secform: SecondsFormat, use_z: bool) -> String {
        chrono::DateTime::<Utc>::from(*self).to_rfc3339_opts(secform, use_z)
    }

    #[inline]
    pub fn round_to_secs(mut self) -> Nanos {
        self.0 /= 1_000_000_000;
        self.0 *= 1_000_000_000;
        self
    }

    #[inline]
    pub fn round_to_interval(mut self, interval: Nanos) -> Nanos {
        self.0 /= interval.0;
        self.0 *= interval.0;
        self
    }
}

impl From<Nanos> for chrono::DateTime<Utc> {
    fn from(value: Nanos) -> Self {
        chrono::DateTime::from_timestamp_nanos(value.0 as i64)
    }
}

impl std::fmt::Display for Nanos {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 == 0 {
            write!(f, "")
        } else if *self < Nanos::from_micros(1) {
            write!(f, "{}ns", self.0)
        } else if *self < Nanos::from_millis(1) {
            write!(f, "{}μs", self.0 as f64 / 1000.0)
        } else if *self < Nanos::from_secs(1) {
            write!(f, "{}ms", (self.0 / 1000) as f64 / 1000.0)
        } else if *self < Nanos::from_mins(1) {
            write!(f, "{:0>2}s", (self.0 / 1_000_000) as f64 / 1000.0)
        } else if *self < Nanos::from_hours(1) {
            let min = self.0 / Nanos::from_mins(1).0;
            let s = *self - Nanos::from_mins(min);
            write!(f, "{:0>2}m:{:0>2}", min, s)
        } else if *self <= Nanos::from_hours(24) {
            let hours = self.0 / Nanos::from_hours(1).0;
            let min = *self - Nanos::from_hours(hours);
            write!(f, "{:0>2}h:{:0>2}", hours, min)
        } else {
            write!(f, "{}", chrono::DateTime::<Utc>::from(*self))
        }
    }
}

impl From<Nanos> for u64 {
    #[inline]
    fn from(value: Nanos) -> Self {
        value.0
    }
}

impl Add for Nanos {
    type Output = Nanos;

    #[inline]
    fn add(self, rhs: Nanos) -> Nanos {
        Nanos(self.0.wrapping_add(rhs.0))
    }
}

impl AddAssign for Nanos {
    #[inline]
    fn add_assign(&mut self, rhs: Nanos) {
        *self = *self + rhs;
    }
}

impl Sub for Nanos {
    type Output = Nanos;

    #[inline]
    fn sub(self, rhs: Nanos) -> Nanos {
        Nanos(self.0 - rhs.0)
    }
}

impl SubAssign for Nanos {
    #[inline]
    fn sub_assign(&mut self, rhs: Nanos) {
        *self = *self - rhs;
    }
}

impl Sub<u64> for Nanos {
    type Output = Nanos;

    #[inline]
    fn sub(self, rhs: u64) -> Nanos {
        Nanos(self.0 - rhs)
    }
}

impl SubAssign<u64> for Nanos {
    #[inline]
    fn sub_assign(&mut self, rhs: u64) {
        *self = *self - rhs;
    }
}

impl Mul<u32> for Nanos {
    type Output = Nanos;

    #[inline]
    fn mul(self, rhs: u32) -> Nanos {
        Nanos(self.0 * rhs as u64)
    }
}

impl Mul<i32> for Nanos {
    type Output = Nanos;

    #[inline]
    fn mul(self, rhs: i32) -> Nanos {
        Nanos(self.0 * rhs as u64)
    }
}

impl Mul<Nanos> for u32 {
    type Output = Nanos;

    #[inline]
    fn mul(self, rhs: Nanos) -> Nanos {
        rhs * self
    }
}
impl Mul<Nanos> for i32 {
    type Output = Nanos;

    #[inline]
    fn mul(self, rhs: Nanos) -> Nanos {
        rhs * self
    }
}

impl MulAssign<u32> for Nanos {
    #[inline]
    fn mul_assign(&mut self, rhs: u32) {
        *self = *self * rhs;
    }
}

impl Div<u32> for Nanos {
    type Output = Nanos;

    #[inline]
    fn div(self, rhs: u32) -> Nanos {
        Nanos(self.0 / rhs as u64)
    }
}

impl Div<usize> for Nanos {
    type Output = Nanos;

    #[inline]
    fn div(self, rhs: usize) -> Nanos {
        Nanos(self.0 / rhs as u64)
    }
}

impl DivAssign<u32> for Nanos {
    #[inline]
    fn div_assign(&mut self, rhs: u32) {
        *self = *self / rhs;
    }
}

impl Mul<u64> for Nanos {
    type Output = Nanos;

    #[inline]
    fn mul(self, rhs: u64) -> Nanos {
        Nanos(self.0 * rhs)
    }
}

impl Mul<Nanos> for u64 {
    type Output = Nanos;

    #[inline]
    fn mul(self, rhs: Nanos) -> Nanos {
        rhs * self
    }
}

impl Mul<Nanos> for Nanos {
    type Output = Nanos;

    #[inline]
    fn mul(self, rhs: Nanos) -> Nanos {
        Nanos(rhs.0 * self.0)
    }
}

impl MulAssign<u64> for Nanos {
    #[inline]
    fn mul_assign(&mut self, rhs: u64) {
        *self = *self * rhs;
    }
}

impl Div<u64> for Nanos {
    type Output = Nanos;

    #[inline]
    fn div(self, rhs: u64) -> Nanos {
        Nanos(self.0 / rhs)
    }
}

impl DivAssign<u64> for Nanos {
    #[inline]
    fn div_assign(&mut self, rhs: u64) {
        *self = *self / rhs;
    }
}

impl Div<Nanos> for Nanos {
    type Output = u64;

    #[inline]
    fn div(self, rhs: Nanos) -> u64 {
        self.0 / rhs.0
    }
}

impl Eq for Nanos {}

impl PartialOrd for Nanos {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Nanos {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl std::iter::Sum for Nanos {
    #[inline]
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Nanos(iter.map(|v| v.0).sum())
    }
}

impl<'a> std::iter::Sum<&'a Self> for Nanos {
    #[inline]
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = &'a Self>,
    {
        Nanos(iter.map(|v| v.0).sum())
    }
}

impl From<Nanos> for f64 {
    #[inline]
    fn from(value: Nanos) -> Self {
        value.0 as f64
    }
}

impl From<u64> for Nanos {
    #[inline]
    fn from(value: u64) -> Self {
        Nanos(value)
    }
}

impl From<u128> for Nanos {
    #[inline]
    fn from(value: u128) -> Self {
        Nanos(value as u64)
    }
}

impl From<u32> for Nanos {
    #[inline]
    fn from(value: u32) -> Self {
        Nanos(value as u64)
    }
}

impl From<i64> for Nanos {
    #[inline]
    fn from(value: i64) -> Self {
        Nanos(value as u64)
    }
}

impl From<i32> for Nanos {
    #[inline]
    fn from(value: i32) -> Self {
        Nanos(value as u64)
    }
}

impl From<Nanos> for i64 {
    #[inline]
    fn from(val: Nanos) -> Self {
        val.0 as i64
    }
}

impl FromStr for Nanos {
    type Err = HumanDurationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().parse::<HumanDuration>() {
            Ok(duration) => {
                let std_duration: std::time::Duration = duration.into();
                Ok(Nanos(std_duration.as_nanos() as u64))
            }
            Err(err) => Err(err),
        }
    }
}

impl From<Duration> for Nanos {
    #[inline]
    fn from(value: Duration) -> Self {
        Nanos(global_clock_not_mocked().delta_as_nanos(0, value.0))
    }
}

impl From<Nanos> for std::time::Duration {
    #[inline]
    fn from(value: Nanos) -> Self {
        std::time::Duration::from_nanos(value.0)
    }
}

impl IntoNanoseconds for Nanos {
    #[inline]
    fn into_nanos(self) -> u64 {
        self.0
    }
}

impl<'de> serde::Deserialize<'de> for Nanos {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use std::fmt;

        use serde::de::{self, Visitor};

        struct NanosVisitor;

        impl<'de> Visitor<'de> for NanosVisitor {
            type Value = Nanos;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("An integer or a string with optional suffix (s, ms, us, ...)")
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(Nanos(value))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                if value < 0 {
                    return Err(E::custom(format!("Nanos cannot be negative, got {}", value)));
                }
                Ok(Nanos(value as u64))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Nanos::from_str(value).map_err(|e| {
                    E::custom(format!("Failed to parse time value '{}' as duration: {}", value, e))
                })
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_any(NanosVisitor)
        } else {
            u64::deserialize(deserializer).map(Nanos)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nanos_bincode_serialization() {
        let nanos = Nanos::from_secs(1);
        let bytes = bitcode::serialize(&nanos).unwrap();
        let result = bitcode::deserialize::<Nanos>(&bytes).unwrap();
        assert_eq!(result, nanos);
    }

    #[test]
    fn test_nanos_from_string_seconds() {
        let result: Nanos = serde_json::from_str(r#""10s""#).unwrap();
        assert_eq!(result, Nanos::from_secs(10));
    }

    #[test]
    fn test_nanos_from_string_float_seconds() {
        let result: Nanos = serde_json::from_str(r#""10.12s""#).unwrap();
        assert_eq!(result, Nanos::from_millis(10_120));
    }

    #[test]
    fn test_nanos_from_string_milliseconds() {
        let result: Nanos = serde_json::from_str(r#""11ms""#).unwrap();
        assert_eq!(result, Nanos::from_millis(11));
    }

    #[test]
    fn test_nanos_from_string_float_milliseconds() {
        let result: Nanos = serde_json::from_str(r#""11.12ms""#).unwrap();
        assert_eq!(result, Nanos::from_micros(11_120));
    }

    #[test]
    fn test_nanos_from_string_microseconds() {
        let result: Nanos = serde_json::from_str(r#""12us""#).unwrap();
        assert_eq!(result, Nanos::from_micros(12));
    }

    #[test]
    fn test_nanos_from_number() {
        let result: Nanos = serde_json::from_str(r#"1"#).unwrap();
        assert_eq!(result, Nanos(1));
    }
}
