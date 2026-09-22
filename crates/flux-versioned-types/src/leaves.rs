//! Leaves and the families that hold them.
//!
//! A *leaf* is a `ByteStable` type with a roll chain ([`Versioned`]). A
//! *family* is an enum of newtype variants holding leaves or other families
//! ([`HasVersionedLeaves`], derived with `#[derive(VersionedLeaves)]`).
//!
//! A blob is identified by two things: its wire name, which says which
//! message this is, and its type hash, which says how to decode it. The name
//! is the leaf's [`Versioned::NAME`]; to reuse a payload under a second name,
//! wrap it in its own leaf type. Names must be unique across a family tree;
//! the derive checks that at compile time.

use flux_timing::InternalMessage;

use crate::raw::{Blob, DecodeError, DecompressedBlob, Scratch, TYPE_NAME_LEN};

pub type Decoded<U, T> = Result<(U, Vec<InternalMessage<T>>), DecodeError>;

/// Lazy counterpart to [`Decoded`].
///
/// The user metadata plus an iterator that decodes and migrates one message
/// per `next`, backed by the blob's owned decompressed bytes. Generated
/// dispatch boxes once for the leaf and once per matched family nesting
/// level, never per message. `nth` skips positionally without decoding.
pub type DecodedIter<'a, U, T> = Result<
    (U, Box<dyn ExactSizeIterator<Item = Result<InternalMessage<T>, DecodeError>> + 'a>),
    DecodeError,
>;

/// Owning family iterator; consumes the backing without decompressing again.
pub type OwnedDecodedIter<U, T> = DecodedIter<'static, U, T>;

/// Generated family wrapping that preserves positional `nth` skipping.
#[doc(hidden)]
pub struct FamilyIter<I, F> {
    inner: I,
    wrap: F,
}

impl<I, F> FamilyIter<I, F> {
    pub fn new(inner: I, wrap: F) -> Self {
        Self { inner, wrap }
    }
}

impl<I, F, T, U> Iterator for FamilyIter<I, F>
where
    I: Iterator<Item = Result<InternalMessage<T>, DecodeError>>,
    F: Fn(T) -> U,
{
    type Item = Result<InternalMessage<U>, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|msg| msg.map(|m| m.map(&self.wrap)))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.inner.nth(n).map(|msg| msg.map(|m| m.map(&self.wrap)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<I, F, T, U> ExactSizeIterator for FamilyIter<I, F>
where
    I: ExactSizeIterator<Item = Result<InternalMessage<T>, DecodeError>>,
    F: Fn(T) -> U,
{
}

pub trait Versioned: type_hash::TypeHash + byte_stable::ByteStable {
    /// Alias name unless overridden with `#[wire_name = ".."]`.
    const NAME: &'static str;

    /// Oldest first.
    const VERSION_HASHES: &'static [u64];

    fn version_size(type_hash: u64) -> Option<usize>;

    /// Casts `bytes` as the version `type_hash` names and migrates to `Self`.
    fn decode_versions(type_hash: u64, bytes: &[u8]) -> Result<Vec<Self>, DecodeError>;

    /// Casts exactly one `version_size(type_hash)` record and migrates it to
    /// `Self`. Same per-record semantics as
    /// [`decode_versions`](Self::decode_versions) without building a `Vec`;
    /// the lazy iterators call this once per message.
    fn decode_one(type_hash: u64, bytes: &[u8]) -> Result<Self, DecodeError>;
}

pub trait VisitorVersionedLeaf {
    fn visit_leaf<L: Versioned>(&mut self, leaf: &L);
}

pub trait HasVersionedLeaves: Copy {
    /// Wire name of every leaf position reachable from this type.
    const LEAF_NAMES: &'static [&'static str];

    fn visit_leaf<V: VisitorVersionedLeaf>(&self, visitor: &mut V);

    /// `None` when `blob`'s name and type match none of this type's positions.
    fn decode_blob<U: Versioned>(blob: &Blob, scratch: &mut Scratch) -> Option<Decoded<U, Self>>;

    /// Lazy counterpart to [`decode_blob`](Self::decode_blob): `None` under
    /// the same conditions, otherwise the user metadata plus an iterator that
    /// migrates one message per `next` without building a `Vec`.
    fn decode_iter<U: Versioned>(blob: &DecompressedBlob) -> Option<DecodedIter<'_, U, Self>>;

    /// Owning counterpart to [`decode_iter`](Self::decode_iter), suitable for
    /// a paused pending run after its source and reader have been dropped.
    fn into_decode_iter<U: Versioned>(blob: DecompressedBlob) -> Option<OwnedDecodedIter<U, Self>>
    where
        Self: 'static;
}

pub const fn concat_names<const N: usize>(parts: &[&'static [&'static str]]) -> [&'static str; N] {
    let mut out = [""; N];
    let mut k = 0;
    let mut p = 0;
    while p < parts.len() {
        let mut i = 0;
        while i < parts[p].len() {
            out[k] = parts[p][i];
            k += 1;
            i += 1;
        }
        p += 1;
    }
    assert!(k == N);
    out
}

pub const fn str_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

pub const fn names_disjoint(a: &[&str], b: &[&str]) -> bool {
    let mut i = 0;
    while i < a.len() {
        let mut j = 0;
        while j < b.len() {
            if str_eq(a[i], b[j]) {
                return false;
            }
            j += 1;
        }
        i += 1;
    }
    true
}

pub const fn name_fits(name: &str) -> bool {
    name.len() <= TYPE_NAME_LEN
}
