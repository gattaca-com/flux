//! `#[derive(ByteStable)]`. See `byte-stable` for the trait it implements.

use proc_macro::TokenStream;

/// Implements `ByteStable` for a `repr(C)`/`repr(transparent)` struct or a
/// `repr(u8)` fieldless enum.
///
/// `#[byte_stable(crate = "::some::path")]` overrides where the trait is
/// found; the default resolves the `byte-stable` dependency of the calling
/// crate and falls back to `::flux::byte_stable`.
#[proc_macro_derive(ByteStable, attributes(byte_stable))]
pub fn derive_byte_stable(_input: TokenStream) -> TokenStream {
    todo!("W-A")
}
