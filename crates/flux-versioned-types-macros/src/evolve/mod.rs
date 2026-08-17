mod generate;
mod parse;

use generate::{generate_base_struct, generate_evolution};
use parse::EvolveInput;
use proc_macro::TokenStream;
use syn::parse_macro_input;

/// Macro for defining structs that evolve through versions without field
/// duplication.
///
/// # Example
/// ```ignore
/// evolve_struct! {
///     default_attrs {
///         #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TypeHash)]
///         #[repr(C)]
///     }
///
///     #[type_hash_lock(hash = 123456)]
///     MyTypeV1 {
///         pub a: u32,
///         pub b: String,
///     }
///
///     #[type_hash_lock(hash = 789012)]
///     MyTypeV2 {
///         add {
///             pub c: bool = false,
///         }
///     }
///
///     #[type_hash_lock(hash = 345678)]
///     MyTypeV3 {
///         remove { b }
///         modify {
///             a: u64 = |v| v as u64,
///         }
///         add {
///             pub d: i64 = 0,
///         }
///     }
/// }
/// ```
///
/// This generates:
/// - `MyTypeV1 { a: u32, b }` with `default_attrs` + `type_hash_lock`
/// - `MyTypeV2 { a: u32, b, c }` with `Into<MyTypeV2> for MyTypeV1`
/// - `MyTypeV3 { a: u64, c, d }` with `Into<MyTypeV3> for MyTypeV2`
pub fn evolve_struct(input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as EvolveInput);
    input.ensure_default_attrs(crate::shared::default_struct_attrs);
    crate::shared::generate_evolving(&mut input, generate_base_struct, generate_evolution).into()
}
