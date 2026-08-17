mod generate;
mod parse;

use generate::{generate_base_enum, generate_evolution};
use parse::EvolveEnumInput;
use proc_macro::TokenStream;
use syn::parse_macro_input;

/// Macro for defining enums that evolve through versions without variant
/// duplication.
///
/// Supports the same three operations as `evolve_struct!`:
/// - **add** — introduce new variants
/// - **remove** — drop variants (old values map to `Default::default()`)
/// - **modify** — change a variant's payload type with a converter
///
/// # Example
/// ```ignore
/// evolve_enum! {
///     default_attrs {
///         #[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
///         #[repr(u8)]
///     }
///
///     #[type_hash_lock(hash = 123456)]
///     MyEnumV1 {
///         Slot(u32),
///         Pair(u32, u32),
///         #[default]
///         Uninitialized,
///     }
///
///     #[type_hash_lock(hash = 789012)]
///     MyEnumV2 {
///         add {
///             NewVariant(u64),
///         }
///         remove { Slot }
///         modify {
///             // single-field: converter takes OldT, returns NewT
///             Uninitialized(u64) = |v: u32| v as u64,
///             // multi-field: converter takes and returns a tuple
///             Pair(u64, u64) = |(a, b): (u32, u32)| (a as u64, b as u64),
///         }
///     }
/// }
/// ```
///
/// Pair this with `roll_chain_into!(MyEnum, [MyEnumV1, MyEnumV2])` to get
/// the type alias and `versioned_deserialize_vec`.
pub fn evolve_enum(input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as EvolveEnumInput);
    input.ensure_default_attrs(crate::shared::default_enum_attrs);
    let require_type_hash_locks = input.require_type_hash_locks;
    crate::shared::generate_evolving(
        &mut input,
        generate_base_enum,
        generate_evolution,
        require_type_hash_locks,
    )
    .into()
}
