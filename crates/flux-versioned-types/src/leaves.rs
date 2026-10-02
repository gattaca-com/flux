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

use crate::raw::{Blob, DecodeError, Scratch, TYPE_NAME_LEN};

pub type Decoded<U, T> = Result<(U, Vec<InternalMessage<T>>), DecodeError>;

pub trait Versioned: type_hash::TypeHash + byte_stable::ByteStable {
    /// Alias name unless overridden with `#[wire_name = ".."]`.
    const NAME: &'static str;

    /// Oldest first.
    const VERSION_HASHES: &'static [u64];

    fn version_size(type_hash: u64) -> Option<usize>;

    /// Casts `bytes` as `n` values of the version `type_hash` names and
    /// migrates them to `Self`. Zero-sized versions have no bytes, so `n` is
    /// the only source of the count.
    fn decode_versions(type_hash: u64, bytes: &[u8], n: usize) -> Result<Vec<Self>, DecodeError>;

    /// [`decode_versions`](Self::decode_versions), handing each value to `f`
    /// in order instead of collecting them. Nothing reaches `f` on error.
    fn decode_versions_each<F: FnMut(Self)>(
        type_hash: u64,
        bytes: &[u8],
        n: usize,
        mut f: F,
    ) -> Result<(), DecodeError> {
        for value in Self::decode_versions(type_hash, bytes, n)? {
            f(value);
        }
        Ok(())
    }
}

/// Where [`HasVersionedLeaves::decode_blob_into`] puts a blob's messages.
pub trait MessageSink<T> {
    /// Called once per blob, before its messages, with their count. The blob
    /// can still fail to decode after it.
    fn reserve(&mut self, n: usize);

    fn push(&mut self, msg: InternalMessage<T>);
}

impl<T> MessageSink<T> for Vec<InternalMessage<T>> {
    #[inline]
    fn reserve(&mut self, n: usize) {
        Self::reserve(self, n);
    }

    #[inline]
    fn push(&mut self, msg: InternalMessage<T>) {
        Self::push(self, msg);
    }
}

/// Wraps a variant's messages into their family on the way to the family's
/// sink. The derive builds one per family level.
#[doc(hidden)]
pub struct WrapSink<'a, S, F> {
    sink: &'a mut S,
    wrap: F,
}

impl<'a, S, F> WrapSink<'a, S, F> {
    #[inline]
    pub fn new(sink: &'a mut S, wrap: F) -> Self {
        Self { sink, wrap }
    }
}

impl<T, P, S: MessageSink<P>, F: Fn(T) -> P> MessageSink<T> for WrapSink<'_, S, F> {
    #[inline]
    fn reserve(&mut self, n: usize) {
        self.sink.reserve(n);
    }

    #[inline]
    fn push(&mut self, msg: InternalMessage<T>) {
        self.sink.push(msg.map(&self.wrap));
    }
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

    /// [`decode_blob`](Self::decode_blob) straight into `sink`, with no
    /// intermediate batch per family level. No message is pushed when the
    /// blob is not this type's or fails to decode.
    ///
    /// The derive decodes in one pass and builds `decode_blob` on this; the
    /// default suits a hand-written `decode_blob` and moves its batch across.
    fn decode_blob_into<U: Versioned, S: MessageSink<Self>>(
        blob: &Blob,
        scratch: &mut Scratch,
        sink: &mut S,
    ) -> Option<Result<U, DecodeError>> {
        Some(Self::decode_blob::<U>(blob, scratch)?.map(|(meta, msgs)| {
            sink.reserve(msgs.len());
            for msg in msgs {
                sink.push(msg);
            }
            meta
        }))
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
