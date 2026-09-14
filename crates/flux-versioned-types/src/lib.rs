//! Version-tolerant Rust data types with explicit, compile-time migrations.
//!
//! The core is [`VersionedDeserialize`]: evolving types decode a stored
//! bincode payload using the type hash of the version that was written.
//! [`VersionedBlob`] packs such payloads with their hash for sending and
//! persisting, and [`TelemetrySchema`] describes their SQL/Arrow-facing
//! shape. None of this knows about any application's registries or metadata:
//! those stay downstream.

pub mod blob;
mod schema;

pub use blob::{InternalMetadata, InternalMetadataV1, VersionedBlob, VersionedPersistable};
pub use flux_versioned_types_macros::{
    TelemetrySchema, evolve_enum, evolve_struct, roll_chain_into,
};
pub use schema::TelemetrySchema;

/// A type whose historical bincode payloads can be migrated to its latest form.
pub trait VersionedDeserialize: Sized {
    /// Decode a vector written as the version identified by `stored_type_hash`.
    fn versioned_deserialize_vec(stored_type_hash: u64, bytes: &[u8])
    -> bincode::Result<Vec<Self>>;
}

/// Implement [`VersionedDeserialize`] for a type generated with `roll_into`.
#[macro_export]
macro_rules! impl_versioned_deserialize {
    ($name:ident) => {
        impl $crate::VersionedDeserialize for $name {
            fn versioned_deserialize_vec(
                stored_type_hash: u64,
                bytes: &[u8],
            ) -> bincode::Result<Vec<Self>> {
                <$name>::versioned_deserialize_vec(stored_type_hash, bytes)
            }
        }
    };
}

/// Define an evolving struct and its hash-directed decoder.
#[macro_export]
macro_rules! versioned_struct {
    ($name:ident => $($tokens:tt)*) => {
        $crate::evolve_struct! {
            roll_into $name
            $($tokens)*
        }
        $crate::impl_versioned_deserialize!($name);
    };
}

/// Define an evolving enum, its hash-directed decoder, and its
/// `TelemetrySchema`.
///
/// With `persist = "dir"` the type also gets a [`VersionedPersistable`]
/// home under that directory.
#[macro_export]
macro_rules! versioned_enum {
    ($name:ident, persist = $dir:expr => $($tokens:tt)*) => {
        $crate::__versioned_enum_inner!($name => $($tokens)*);
        impl $crate::VersionedPersistable for $name {
            const PERSIST_DIR: &'static str = $dir;
        }
    };
    ($name:ident => $($tokens:tt)*) => {
        $crate::__versioned_enum_inner!($name => $($tokens)*);
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __versioned_enum_inner {
    ($name:ident => $($tokens:tt)*) => {
        $crate::evolve_enum! {
            roll_into $name
            final_attrs {
                #[derive($crate::TelemetrySchema)]
            }
            $($tokens)*
        }
        $crate::impl_versioned_deserialize!($name);
    };
}

/// Define an evolving telemetry struct.
///
/// Like [`versioned_struct`], but the latest version also derives
/// [`TelemetrySchema`] so it is queryable. With `persist = "dir"` the type
/// also gets a [`VersionedPersistable`] home under that directory.
#[macro_export]
macro_rules! versioned_telemetry {
    ($name:ident, persist = $dir:expr => $($tokens:tt)*) => {
        $crate::__versioned_telemetry_inner!($name => $($tokens)*);
        impl $crate::VersionedPersistable for $name {
            const PERSIST_DIR: &'static str = $dir;
        }
    };
    ($name:ident => $($tokens:tt)*) => {
        $crate::__versioned_telemetry_inner!($name => $($tokens)*);
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __versioned_telemetry_inner {
    ($name:ident => $($tokens:tt)*) => {
        $crate::evolve_struct! {
            roll_into $name
            final_attrs {
                #[derive($crate::TelemetrySchema)]
            }
            $($tokens)*
        }
        $crate::impl_versioned_deserialize!($name);
    };
}
