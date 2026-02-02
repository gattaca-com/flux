#![no_std]

use core::mem;

/// A trait for a "stable-ish" type fingerprint you control.
/// We provide impls for intrinsic primitive types that are Copy + Sized.
/// You can also implement this for your own types.
pub trait TypeHash: Copy + Sized {
    const TYPE_HASH: u64;
}

/// Const FNV-1a 64-bit over bytes.
pub const fn fnv1a64_bytes(mut h: u64, bytes: &[u8]) -> u64 {
    let mut i = 0;
    while i < bytes.len() {
        h ^= bytes[i] as u64;
        h = h.wrapping_mul(0x100000001b3);
        i += 1;
    }
    h
}

pub const fn fnv1a64_str(h: u64, s: &str) -> u64 {
    fnv1a64_bytes(h, s.as_bytes())
}

/// Mix helper (cheap avalanche-ish).
pub const fn mix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    x
}

pub const fn hash_u64(seed: u64, x: u64) -> u64 {
    mix64(seed ^ mix64(x))
}

pub const fn hash_layout_of<T>(seed: u64) -> u64 {
    let mut h = seed;
    h = hash_u64(h, mem::size_of::<T>() as u64);
    h = hash_u64(h, mem::align_of::<T>() as u64);
    h
}

/// --- Intrinsic primitive impls (Copy + Sized) ---
/// We intentionally assign fixed constants (not derived from type_name).
macro_rules! impl_primitive_type_hash {
    ($($t:ty => $id:expr),* $(,)?) => {
        $(
            impl TypeHash for $t {
                const TYPE_HASH: u64 = $id;
            }
        )*
    };
}

impl_primitive_type_hash! {
    ()      => 0x01,
    bool    => 0x02,
    char    => 0x03,

    u8      => 0x10,  i8  => 0x11,
    u16     => 0x12,  i16 => 0x13,
    u32     => 0x14,  i32 => 0x15,
    u64     => 0x16,  i64 => 0x17,
    u128    => 0x18,  i128=> 0x19,
    usize   => 0x1A,  isize=>0x1B,

    f32     => 0x20,
    f64     => 0x21,
}

/// Arrays: Copy + Sized only (this requires T: Copy, and arrays are Copy if T
/// is).
impl<T: TypeHash, const N: usize> TypeHash for [T; N] {
    // Mix element hash + N + layout
    const TYPE_HASH: u64 = {
        let mut h = 0xcbf29ce484222325u64;
        h = fnv1a64_str(h, "[T;N]");
        h = hash_u64(h, T::TYPE_HASH);
        h = hash_u64(h, N as u64);
        h = hash_layout_of::<[T; N]>(h);
        h
    };
}

/// Maybe you want Option<T> when T: Copy.
/// (Option<T> is Copy if T: Copy)
impl<T: TypeHash> TypeHash for Option<T> {
    const TYPE_HASH: u64 = {
        let mut h = 0xcbf29ce484222325u64;
        h = fnv1a64_str(h, "Option");
        h = hash_u64(h, T::TYPE_HASH);
        h = hash_layout_of::<Option<T>>(h);
        h
    };
}
