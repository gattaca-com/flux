//! Version-tolerant Rust data types with explicit, compile-time migrations.
//!
//! This crate deliberately contains no persistence directories, telemetry
//! registries, or application-specific metadata. It defines evolving types and
//! decodes a stored bincode payload using the type hash of the version that was
//! written.

pub use flux_versioned_types_macros::{evolve_enum, evolve_struct, roll_chain_into};
pub use type_hash::TypeHash;
pub use type_hash_derive::{TypeHash, type_hash_lock};

/// Compatibility salt used by the original wire format.
///
/// This value is part of the persisted format and must not be changed.
pub const STORED_TYPE_HASH_XOR: u64 = 123_456;

/// A type whose historical bincode payloads can be migrated to its latest form.
pub trait VersionedDeserialize: Sized {
    /// Decode a vector written as the version identified by `stored_type_hash`.
    fn versioned_deserialize_vec(stored_type_hash: u64, bytes: &[u8])
    -> bincode::Result<Vec<Self>>;
}

#[doc(hidden)]
pub mod __private {
    pub use bincode;
    pub use serde;
    pub use type_hash;
    pub use type_hash_derive;
}

/// Implement [`VersionedDeserialize`] for a type generated with `roll_into`.
#[macro_export]
macro_rules! impl_versioned_deserialize {
    ($name:ident) => {
        impl $crate::VersionedDeserialize for $name {
            fn versioned_deserialize_vec(
                stored_type_hash: u64,
                bytes: &[u8],
            ) -> $crate::__private::bincode::Result<::std::vec::Vec<Self>> {
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

/// Define an evolving enum and its hash-directed decoder.
#[macro_export]
macro_rules! versioned_enum {
    ($name:ident => $($tokens:tt)*) => {
        $crate::evolve_enum! {
            roll_into $name
            $($tokens)*
        }
        $crate::impl_versioned_deserialize!($name);
    };
}
