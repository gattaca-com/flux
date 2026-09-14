//! SQL/Arrow-facing shape of a telemetry type.
//!
//! A type's serialized form is not always the shape you want in a table:
//! hashes and addresses read better as hex strings, time newtypes as integers,
//! enums with payloads as flattened variant columns. `TelemetrySchema`
//! maps each Rust type to `Proxy` (nested shape) and `FlattenedProxy` (one
//! optional column per variant, for enums) mirrors, both `Deserialize`-only.
//! The derive generates the mirrors and rejects serialization-changing serde
//! attributes unless the field opts out with an explicit proxy.
//!
//! Foreign types with no blessed mapping (UUIDs, 128-bit integers, hash and
//! address types) use that same escape hatch at the field level; only the
//! standard library and flux's own time and container types are covered here.

use serde::de::DeserializeOwned;

pub trait TelemetrySchema {
    type Proxy: DeserializeOwned;
    type FlattenedProxy: DeserializeOwned;
}

macro_rules! same_proxy {
    ($($type:ty),* $(,)?) => {$(
        impl TelemetrySchema for $type {
            type Proxy = Self;
            type FlattenedProxy = Self;
        }
    )*};
}

same_proxy!((), bool, char, String, i8, i16, i32, i64, u8, u16, u32, u64, f32, f64,);

impl TelemetrySchema for isize {
    type Proxy = i64;
    type FlattenedProxy = i64;
}

impl TelemetrySchema for usize {
    type Proxy = u64;
    type FlattenedProxy = u64;
}

impl<T: TelemetrySchema> TelemetrySchema for Option<T> {
    type Proxy = Option<T::Proxy>;
    type FlattenedProxy = Option<T::FlattenedProxy>;
}

impl<T: TelemetrySchema> TelemetrySchema for Vec<T> {
    type Proxy = Vec<T::Proxy>;
    type FlattenedProxy = Vec<T::FlattenedProxy>;
}

impl<T: TelemetrySchema, const N: usize> TelemetrySchema for [T; N]
where
    [T::Proxy; N]: DeserializeOwned,
    [T::FlattenedProxy; N]: DeserializeOwned,
{
    type Proxy = [T::Proxy; N];
    type FlattenedProxy = [T::FlattenedProxy; N];
}

macro_rules! u64_proxy {
    ($($type:ty),* $(,)?) => {$(
        impl TelemetrySchema for $type {
            type Proxy = u64;
            type FlattenedProxy = u64;
        }
    )*};
}

u64_proxy!(flux_timing::Nanos, flux_timing::Duration, flux_timing::Instant);

impl TelemetrySchema for uuid::Uuid {
    type Proxy = String;
    type FlattenedProxy = String;
}

impl<const N: usize> TelemetrySchema for alloy_primitives::FixedBytes<N> {
    type Proxy = String;
    type FlattenedProxy = String;
}

impl<const BITS: usize, const LIMBS: usize> TelemetrySchema
    for alloy_primitives::Uint<BITS, LIMBS>
{
    type Proxy = String;
    type FlattenedProxy = String;
}

impl<const BITS: usize, const LIMBS: usize> TelemetrySchema
    for alloy_primitives::Signed<BITS, LIMBS>
{
    type Proxy = String;
    type FlattenedProxy = String;
}

impl TelemetrySchema for alloy_primitives::Address {
    type Proxy = String;
    type FlattenedProxy = String;
}

impl<T: TelemetrySchema + Copy, const N: usize> TelemetrySchema for flux_utils::ArrayVec<T, N> {
    type Proxy = Vec<T::Proxy>;
    type FlattenedProxy = Vec<T::FlattenedProxy>;
}

impl<const N: usize> TelemetrySchema for flux_utils::ArrayStr<N> {
    type Proxy = Vec<u8>;
    type FlattenedProxy = Vec<u8>;
}

macro_rules! tuple_proxy {
    ($(($($type:ident),+)),* $(,)?) => {$(
        impl<$($type: TelemetrySchema),+> TelemetrySchema for ($($type,)+) {
            type Proxy = ($($type::Proxy,)+);
            type FlattenedProxy = ($($type::FlattenedProxy,)+);
        }
    )*};
}

tuple_proxy!((A), (A, B), (A, B, C), (A, B, C, D));
