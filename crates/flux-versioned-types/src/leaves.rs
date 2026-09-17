//! Leaves and the families that hold them.
//!
//! A *leaf* is a `ByteStable` type with a roll chain ([`Versioned`]). A
//! *family* is an enum of newtype variants holding leaves or other families
//! ([`HasVersionedLeaves`], derived with `#[derive(VersionedLeaves)]`).
//!
//! A blob is identified by two things: its wire name, which says which
//! message this is, and its type hash, which says how to decode it. The name
//! defaults to the leaf's [`Versioned::NAME`]; a family variant overrides it
//! with `#[leaves(name = "..")]`, so one leaf type can sit at several
//! positions. Names must be unique across a family tree; the derive checks
//! that at compile time. The same tree is walkable at the type level with
//! `visit_leaf_types`, so applications can decode wrapped leaves.

use core::marker::PhantomData;

use flux_timing::InternalMessage;

use crate::raw::{Blob, DecodeError, Scratch, TYPE_NAME_LEN};

pub type Decoded<U, T> = Result<(U, Vec<InternalMessage<T>>), DecodeError>;

pub trait Versioned: type_hash::TypeHash + byte_stable::ByteStable {
    /// Alias name unless overridden with `#[wire_name = ".."]`.
    const NAME: &'static str;

    fn version_size(type_hash: u64) -> Option<usize>;

    /// Casts `bytes` as the version `type_hash` names and migrates to `Self`.
    fn decode_versions(type_hash: u64, bytes: &[u8]) -> Result<Vec<Self>, DecodeError>;
}

/// Visits the leaf *types* of a family tree, with the path back to `Root`.
pub trait VisitorLeafType<Root> {
    type Out;
    /// One call per leaf position: its wire `name`, leaf type `L`, and `wrap`
    /// rebuilding the root message from a leaf. Return `Some` to stop the
    /// walk.
    fn visit_type<L: Versioned>(
        &mut self,
        name: &'static str,
        wrap: &dyn Fn(L) -> Root,
    ) -> Option<Self::Out>;
}

pub trait VisitorVersionedLeaf {
    fn visit_leaf<L: Versioned>(&mut self, name: &'static str, leaf: &L);
}

pub trait HasVersionedLeaves: Copy {
    /// Wire name of every leaf position reachable from this type.
    const LEAF_NAMES: &'static [&'static str];

    fn visit_leaf<V: VisitorVersionedLeaf>(&self, visitor: &mut V);

    /// Walks every leaf position reachable from `Self`, in declaration order,
    /// until the visitor returns `Some`.
    fn visit_leaf_types<Root, V: VisitorLeafType<Root>>(
        visitor: &mut V,
        wrap: &dyn Fn(Self) -> Root,
    ) -> Option<V::Out>;

    /// `None` when `blob`'s name and type match none of this type's positions.
    fn decode_blob<U: Versioned>(blob: &Blob, scratch: &mut Scratch) -> Option<Decoded<U, Self>> {
        Self::visit_leaf_types(&mut Decode { blob, scratch, meta: PhantomData::<U> }, &|x| x)
    }
}

struct Decode<'a, U> {
    blob: &'a Blob,
    scratch: &'a mut Scratch,
    meta: PhantomData<U>,
}

impl<Root: Copy, U: Versioned> VisitorLeafType<Root> for Decode<'_, U> {
    type Out = Decoded<U, Root>;
    fn visit_type<L: Versioned>(
        &mut self,
        name: &'static str,
        wrap: &dyn Fn(L) -> Root,
    ) -> Option<Self::Out> {
        (self.blob.type_name() == name && self.blob.is::<L>()).then(|| {
            self.blob
                .decode::<U, L>(self.scratch)
                .map(|(meta, msgs)| (meta, msgs.into_iter().map(|m| m.map(wrap)).collect()))
        })
    }
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
