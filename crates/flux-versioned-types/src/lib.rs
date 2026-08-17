//! Version-tolerant Rust data types with explicit, compile-time migrations.
//!
//! This crate deliberately contains no persistence directories, telemetry
//! registries, or application-specific metadata. It defines evolving types and
//! decodes a stored bincode payload using the type hash of the version that was
//! written.

pub use flux_versioned_types_macros::{evolve_enum, evolve_struct, roll_chain_into};

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
            __require_type_hash_locks
            roll_into $name
            $($tokens)*
        }
        $crate::impl_versioned_deserialize!($name);
    };
}

/// Define an evolving enum and its hash-directed decoder.
#[macro_export]
macro_rules! versioned_enum {
    ($name:ident => $($tokens:tt)*) => {
        $crate::evolve_enum! {
            __require_type_hash_locks
            roll_into $name
            $($tokens)*
        }
        $crate::impl_versioned_deserialize!($name);
    };
}
