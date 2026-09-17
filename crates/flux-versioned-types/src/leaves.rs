//! Leaf discovery and repacking for wire-stable message trees.
//!
//! A *leaf* is a `Copy` type with a fixed `repr(C)` layout that can be shipped
//! as raw bytes: every version of it implements [`Versioned`]. A *family* is
//! an enum whose newtype variants hold leaves or other families, such as a
//! spine's `Telemetry` enum. [`HasVersionedLeaves`] walks a family down to the
//! leaf it holds (unpack) and rebuilds the family from a decoded blob (repack).
//!
//! Leaves get their [`Versioned`] and [`HasVersionedLeaves`] impls from
//! `versioned_struct!`/`versioned_enum!` when the `zerocopy` feature is on.
//! Families use `#[derive(HasVersionedLeaves)]`.

use flux_timing::InternalMessage;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::raw::{Blob, DecodeError, Scratch};

/// Result of decoding one blob: its user metadata and its messages.
pub type Decoded<U, T> = Result<(U, Vec<InternalMessage<T>>), DecodeError>;

/// A wire-stable leaf type.
///
/// Every version is a padding-free `repr(C)` `Copy` type whose bytes can be
/// written as is and validated on read. The stored [`type_hash`](Blob) of a
/// blob is the plain `TYPE_HASH` of the version that wrote it (no XOR, unlike
/// legacy bincode blobs).
pub trait Versioned:
    type_hash::TypeHash + Copy + IntoBytes + TryFromBytes + KnownLayout + Immutable + 'static
{
    /// Wire label carried in [`Blob::type_name`]. Defaults to the alias name of
    /// the roll chain; override with `#[wire_name = ".."]`.
    const NAME: &'static str;

    /// `TYPE_HASH` of every version, oldest first, latest last.
    const VERSION_HASHES: &'static [u64];

    /// `size_of` the version identified by `type_hash`, `None` when unknown.
    /// Lets a reader validate section lengths before it allocates.
    fn version_size(type_hash: u64) -> Option<usize>;

    /// Cast `bytes` as a slice of the version identified by `type_hash` and
    /// migrate each element to `Self`.
    ///
    /// `bytes` must be aligned to that version's alignment and be an exact
    /// multiple of its size; anything else is [`DecodeError`], never UB.
    fn decode_versions(type_hash: u64, bytes: &[u8]) -> Result<Vec<Self>, DecodeError>;
}

/// Receives the single leaf a message resolves to. Implemented by
/// [`BlobCache`](crate::raw::BlobCache); generic method, static dispatch.
pub trait VisitorVersionedLeaf {
    fn visit_leaf<L: Versioned>(&mut self, leaf: &L);
}

/// A message type that resolves to exactly one [`Versioned`] leaf.
///
/// Leaves implement it trivially (the leaf is itself). Families implement it
/// by delegating to the variant they hold; `#[derive(HasVersionedLeaves)]`
/// generates that, plus `From<Field> for Family` for each kept variant
/// (direct fields only; a nested family's leaves convert through it).
pub trait HasVersionedLeaves: Copy {
    /// Unpack: hand the leaf inside `self` to `visitor`.
    fn visit_leaf<V: VisitorVersionedLeaf>(&self, visitor: &mut V);

    /// Repack: decode `blob` if it holds one of this type's leaves.
    ///
    /// `None` when the blob's type hash belongs to none of them. `Some(Err)`
    /// when it does but the payload is invalid. Variants are tried in
    /// declaration order and the first match wins, so leaf types must be
    /// unique across the whole reachable family.
    fn decode_blob<U: Versioned>(blob: &Blob, scratch: &mut Scratch) -> Option<Decoded<U, Self>>;
}
