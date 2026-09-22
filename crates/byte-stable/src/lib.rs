//! `Copy` types whose in-memory bytes are a stable, validated wire format.
//! Flux-owned so it can be implemented for foreign types behind features.

#![no_std]

use core::{mem, slice};

/// # Safety
///
/// 1. No padding: `Self` is `repr(C)`, `repr(transparent)` or `repr(u8)` and
///    every byte of every value is initialized.
/// 2. `is_valid` accepts exactly the valid values. Callers pass
///    `size_of::<Self>()` bytes aligned to `align_of::<Self>()`.
/// 3. No `UnsafeCell`.
pub unsafe trait ByteStable: Copy + 'static {
    /// Asserts point 1. Evaluated by every byte view, so a padded type does not
    /// build.
    const LAYOUT_PROOF: ();

    fn is_valid(bytes: &[u8]) -> bool;

    /// Read exactly one aligned, validated value, including a zero-sized one.
    fn read(bytes: &[u8]) -> Result<Self, CastError> {
        let () = Self::LAYOUT_PROOF;
        if bytes.len() != mem::size_of::<Self>() {
            return Err(CastError::Length { got: bytes.len(), size: mem::size_of::<Self>() });
        }
        if !(bytes.as_ptr() as usize).is_multiple_of(mem::align_of::<Self>()) {
            return Err(CastError::Unaligned);
        }
        if !Self::is_valid(bytes) {
            return Err(CastError::Invalid { index: 0 });
        }
        // Safety: exact size, aligned, initialized and validated, including ZSTs.
        Ok(unsafe { bytes.as_ptr().cast::<Self>().read() })
    }

    #[inline]
    fn as_bytes(&self) -> &[u8] {
        slice_as_bytes(slice::from_ref(self))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CastError {
    Unaligned,
    Length { got: usize, size: usize },
    Invalid { index: usize },
    ZeroSized,
}

impl core::fmt::Display for CastError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt(self, f)
    }
}

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
    // Safety: aligned, whole elements, each validated, borrow carried over.
    Ok(unsafe { slice::from_raw_parts(bytes.as_ptr().cast::<T>(), bytes.len() / size) })
}

#[inline]
pub fn slice_as_bytes<T: ByteStable>(values: &[T]) -> &[u8] {
    let () = T::LAYOUT_PROOF;
    // Safety: point 1 (no padding, so no inter-element gaps) and point 3.
    unsafe { slice::from_raw_parts(values.as_ptr().cast::<u8>(), mem::size_of_val(values)) }
}

/// Only `u64`: every byte pattern is a valid `u64`, so arbitrary writes are
/// fine.
#[inline]
pub fn words_as_bytes_mut(words: &mut [u64]) -> &mut [u8] {
    // Safety: padding-free, any pattern valid, exclusive borrow carried over.
    unsafe { slice::from_raw_parts_mut(words.as_mut_ptr().cast::<u8>(), mem::size_of_val(words)) }
}

macro_rules! impl_any_pattern {
    ($($t:ty),* $(,)?) => {$(
        // Safety: padding-free, every bit pattern valid.
        unsafe impl ByteStable for $t {
            const LAYOUT_PROOF: () = ();

            #[inline]
            fn is_valid(_bytes: &[u8]) -> bool {
                true
            }
        }
    )*};
}

impl_any_pattern!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64);

// Safety: one byte, valid iff 0 or 1.
unsafe impl ByteStable for bool {
    const LAYOUT_PROOF: () = ();

    #[inline]
    fn is_valid(bytes: &[u8]) -> bool {
        bytes.first().is_some_and(|b| *b < 2)
    }
}

// Safety: no padding beyond `T`'s own, validity is elementwise.
unsafe impl<T: ByteStable, const N: usize> ByteStable for [T; N] {
    const LAYOUT_PROOF: () = T::LAYOUT_PROOF;

    #[inline]
    fn is_valid(bytes: &[u8]) -> bool {
        let size = mem::size_of::<T>();
        size == 0 || bytes.chunks_exact(size).all(T::is_valid)
    }
}

// Safety: `repr(transparent)` over `[u8; 16]`, every pattern valid.
#[cfg(feature = "uuid")]
unsafe impl ByteStable for uuid::Uuid {
    const LAYOUT_PROOF: () = assert!(mem::size_of::<Self>() == 16 && mem::align_of::<Self>() == 1);

    #[inline]
    fn is_valid(_bytes: &[u8]) -> bool {
        true
    }
}

// Safety: `repr(transparent)` over `[u8; N]`, every pattern valid.
#[cfg(feature = "alloy")]
unsafe impl<const N: usize> ByteStable for alloy_primitives::FixedBytes<N> {
    const LAYOUT_PROOF: () = assert!(mem::size_of::<Self>() == N && mem::align_of::<Self>() == 1);

    #[inline]
    fn is_valid(_bytes: &[u8]) -> bool {
        true
    }
}

// Safety: `repr(transparent)` over `FixedBytes<20>`, every pattern valid.
#[cfg(feature = "alloy")]
unsafe impl ByteStable for alloy_primitives::Address {
    const LAYOUT_PROOF: () = assert!(mem::size_of::<Self>() == 20 && mem::align_of::<Self>() == 1);

    #[inline]
    fn is_valid(_bytes: &[u8]) -> bool {
        true
    }
}

// Safety: `repr(transparent)` over `[u64; LIMBS]`; valid iff the top limb fits
// `MASK`, which is exact only when `LIMBS == BITS.div_ceil(64)`.
#[cfg(feature = "alloy")]
unsafe impl<const BITS: usize, const LIMBS: usize> ByteStable
    for alloy_primitives::Uint<BITS, LIMBS>
{
    const LAYOUT_PROOF: () =
        assert!(mem::size_of::<Self>() == 8 * LIMBS && LIMBS == BITS.div_ceil(64));

    #[inline]
    fn is_valid(bytes: &[u8]) -> bool {
        if LIMBS == 0 {
            return true;
        }
        let Some(top) = bytes.len().checked_sub(8).and_then(|at| bytes.get(at..)) else {
            return false;
        };
        let mut limb = [0u8; 8];
        limb.copy_from_slice(top);
        u64::from_ne_bytes(limb) & !Self::MASK == 0
    }
}
