//! Leaves and the families that hold them.
//!
//! A *leaf* is a `ByteStable` type with a roll chain ([`Versioned`]). A
//! *family* is an enum of newtype variants holding leaves or other families
//! ([`HasVersionedLeaves`], derived with `#[derive(VersionedLeaves)]`).

use flux_timing::InternalMessage;

use crate::raw::{Blob, DecodeError, Scratch};

pub type Decoded<U, T> = Result<(U, Vec<InternalMessage<T>>), DecodeError>;

pub trait Versioned: type_hash::TypeHash + byte_stable::ByteStable {
    /// Alias name unless overridden with `#[wire_name = ".."]`.
    const NAME: &'static str;

    /// Oldest first.
    const VERSION_HASHES: &'static [u64];

    fn version_size(type_hash: u64) -> Option<usize>;

    /// Casts `bytes` as the version `type_hash` names and migrates to `Self`.
    fn decode_versions(type_hash: u64, bytes: &[u8]) -> Result<Vec<Self>, DecodeError>;
}

pub trait VisitorVersionedLeaf {
    fn visit_leaf<L: Versioned>(&mut self, leaf: &L);
}

pub trait HasVersionedLeaves: Copy {
    fn visit_leaf<V: VisitorVersionedLeaf>(&self, visitor: &mut V);

    /// `None` when `blob` holds none of this type's leaves. First matching
    /// variant wins, so leaf types must be unique across the family.
    fn decode_blob<U: Versioned>(blob: &Blob, scratch: &mut Scratch) -> Option<Decoded<U, Self>>;
}
