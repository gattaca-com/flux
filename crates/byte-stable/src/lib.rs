//! `Copy` types whose in-memory bytes are a stable, validated wire format.
//!
//! A [`ByteStable`] value can be written to a wire or a file as its raw
//! bytes and read back with a checked cast, on any host with the same
//! target layout. The trait is flux-owned so it can be implemented for
//! foreign types (`uuid::Uuid`, `alloy_primitives::FixedBytes`, ...) behind
//! cargo features, the way `type_hash::TypeHash` is.
//!
//! `#[derive(ByteStable)]` (from `byte-stable-derive`) implements it for
//! padding-free `repr(C)` structs and `repr(u8)` fieldless enums and fails to
//! compile for anything else. Manual impls are `unsafe` and must uphold the
//! contract on the trait.

#![no_std]

use core::{mem, slice};

/// A `Copy` type that is safe to read and write as raw bytes.
///
/// # Safety
///
/// Implementors guarantee, for every value of `Self`:
///
/// 1. **No padding.** `Self` has a defined layout (`repr(C)`,
///    `repr(transparent)` or `repr(u8)`) and every one of its
///    `size_of::<Self>()` bytes is initialized. Nothing may read uninitialized
///    memory through [`as_bytes`](Self::as_bytes).
/// 2. **Exact validation.** [`is_valid`](Self::is_valid) returns `true` only
///    for byte strings that are a valid `Self`. Callers pass exactly
///    `size_of::<Self>()` bytes at an address aligned to `align_of::<Self>()`;
///    the impl may rely on both.
/// 3. **No interior mutability.** `Self` contains no `UnsafeCell`, so the bytes
///    behind a shared reference cannot change while borrowed.
pub unsafe trait ByteStable: Copy + 'static {
    /// Whether `bytes` is a valid `Self`. See the trait's safety contract for
    /// what the caller guarantees about `bytes`.
    fn is_valid(bytes: &[u8]) -> bool;

    /// The value's bytes.
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        // Safety: contract 1 makes every byte of `*self` initialized, and
        // contract 3 keeps them from changing under the borrow.
        unsafe {
            slice::from_raw_parts(core::ptr::from_ref(self).cast::<u8>(), mem::size_of::<Self>())
        }
    }
}

/// Why a byte slice could not be cast.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CastError {
    /// The slice does not start on an `align_of::<T>()` boundary.
    Unaligned,
    /// The length is not a multiple of `size_of::<T>()`.
    Length { got: usize, size: usize },
    /// Element `index` failed [`ByteStable::is_valid`].
    Invalid { index: usize },
    /// `T` is zero-sized, so the element count cannot come from the length.
    ZeroSized,
}

impl core::fmt::Display for CastError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt(self, f)
    }
}

/// Cast `bytes` to a slice of `T`, validating every element.
///
/// Fails instead of panicking on misalignment, a length that is not a whole
/// number of elements, or an invalid element. Never reads past `bytes`.
pub fn cast_slice<T: ByteStable>(bytes: &[u8]) -> Result<&[T], CastError> {
    let size = mem::size_of::<T>();
    if size == 0 {
        return Err(CastError::ZeroSized);
    }
    if !(bytes.as_ptr() as usize).is_multiple_of(mem::align_of::<T>()) {
        return Err(CastError::Unaligned);
    }
    if !bytes.len().is_multiple_of(size) {
        return Err(CastError::Length { got: bytes.len(), size });
    }
    if let Some(index) = bytes.chunks_exact(size).position(|chunk| !T::is_valid(chunk)) {
        return Err(CastError::Invalid { index });
    }
    // Safety: aligned, an exact multiple of `size_of::<T>()`, every element
    // validated, and `bytes` stays borrowed for the returned lifetime.
    Ok(unsafe { slice::from_raw_parts(bytes.as_ptr().cast::<T>(), bytes.len() / size) })
}

/// The bytes of a slice of `T`.
#[inline]
pub fn slice_as_bytes<T: ByteStable>(values: &[T]) -> &[u8] {
    // Safety: contract 1 for every element; slices have no inter-element
    // padding because `size_of` already includes trailing padding, which
    // contract 1 rules out.
    unsafe { slice::from_raw_parts(values.as_ptr().cast::<u8>(), mem::size_of_val(values)) }
}

/// Mutable byte view of a `u64` slice, for 8-aligned byte buffers built on
/// `Vec<u64>`. Only `u64` because every byte pattern is a valid `u64`, so
/// arbitrary writes cannot break the slice.
#[inline]
pub fn words_as_bytes_mut(words: &mut [u64]) -> &mut [u8] {
    // Safety: `u64` is padding-free and accepts every bit pattern; the
    // exclusive borrow is carried over.
    unsafe { slice::from_raw_parts_mut(words.as_mut_ptr().cast::<u8>(), mem::size_of_val(words)) }
}

macro_rules! impl_any_pattern {
    ($($t:ty),* $(,)?) => {$(
        // Safety: primitive integers are padding-free and every bit pattern
        // is valid.
        unsafe impl ByteStable for $t {
            #[inline]
            fn is_valid(_bytes: &[u8]) -> bool {
                true
            }
        }
    )*};
}

impl_any_pattern!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize);

// Safety: one byte, valid iff 0 or 1.
unsafe impl ByteStable for bool {
    #[inline]
    fn is_valid(bytes: &[u8]) -> bool {
        bytes[0] < 2
    }
}

// Safety: arrays are `repr(C)` sequences of `T` with no padding beyond
// `T`'s own, which contract 1 on `T` rules out; validity is elementwise.
unsafe impl<T: ByteStable, const N: usize> ByteStable for [T; N] {
    #[inline]
    fn is_valid(bytes: &[u8]) -> bool {
        let size = mem::size_of::<T>();
        size == 0 || bytes.chunks_exact(size).all(T::is_valid)
    }
}

// Safety: points 1-3 of the trait contract. (1) `Uuid` is `repr(transparent)`
// over `[u8; 16]`, pinned below, so every byte is initialized. (2) Every
// 16-byte pattern is a valid `Uuid`. (3) It contains no `UnsafeCell`.
#[cfg(feature = "uuid")]
unsafe impl ByteStable for uuid::Uuid {
    #[inline]
    fn is_valid(_bytes: &[u8]) -> bool {
        true
    }
}

#[cfg(feature = "uuid")]
const _: () = ::core::assert!(
    ::core::mem::size_of::<uuid::Uuid>() == 16 && ::core::mem::align_of::<uuid::Uuid>() == 1
);

// Safety: points 1-3 of the trait contract. (1) `FixedBytes<N>` is
// `repr(transparent)` over `[u8; N]`, pinned per instantiation below, so every
// byte is initialized. (2) Every byte pattern is valid. (3) It contains no
// `UnsafeCell`.
#[cfg(feature = "alloy")]
unsafe impl<const N: usize> ByteStable for alloy_primitives::FixedBytes<N> {
    #[inline]
    fn is_valid(_bytes: &[u8]) -> bool {
        const {
            ::core::assert!(
                ::core::mem::size_of::<Self>() == N && ::core::mem::align_of::<Self>() == 1
            );
        }
        true
    }
}

// Safety: points 1-3 of the trait contract. (1) `Address` is
// `repr(transparent)` over `FixedBytes<20>`, pinned below, so every byte is
// initialized. (2) Every 20-byte pattern is a valid address. (3) It contains
// no `UnsafeCell`.
#[cfg(feature = "alloy")]
unsafe impl ByteStable for alloy_primitives::Address {
    #[inline]
    fn is_valid(_bytes: &[u8]) -> bool {
        true
    }
}

#[cfg(feature = "alloy")]
const _: () = ::core::assert!(
    ::core::mem::size_of::<alloy_primitives::Address>() == 20 &&
        ::core::mem::align_of::<alloy_primitives::Address>() == 1
);

// Safety: points 1-3 of the trait contract. (1) `Uint` is `repr(transparent)`
// over `[u64; LIMBS]`, pinned per instantiation below, so every byte is
// initialized. (2) Validity is exact: the value is in range iff the top limb
// fits `MASK`. (3) It contains no `UnsafeCell`.
#[cfg(feature = "alloy")]
unsafe impl<const BITS: usize, const LIMBS: usize> ByteStable
    for alloy_primitives::Uint<BITS, LIMBS>
{
    #[inline]
    fn is_valid(bytes: &[u8]) -> bool {
        const {
            ::core::assert!(::core::mem::size_of::<Self>() == 8 * LIMBS);
        }
        if LIMBS == 0 {
            return true;
        }
        let mut limb = [0u8; 8];
        limb.copy_from_slice(&bytes[bytes.len() - 8..]);
        u64::from_le_bytes(limb) & !Self::MASK == 0
    }
}
