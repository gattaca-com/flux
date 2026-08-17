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
    let require_type_hash_locks = input.require_type_hash_locks;
    crate::shared::generate_evolving(
        &mut input,
        generate_base_struct,
        generate_evolution,
        require_type_hash_locks,
    )
    .into()
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::{EvolveInput, generate_base_struct, generate_evolution};

    fn expand(input: proc_macro2::TokenStream) -> String {
        let mut input: EvolveInput = syn::parse2(input).unwrap();
        input.ensure_default_attrs(crate::shared::default_struct_attrs);
        let require_type_hash_locks = input.require_type_hash_locks;
        crate::shared::generate_evolving(
            &mut input,
            generate_base_struct,
            generate_evolution,
            require_type_hash_locks,
        )
        .to_string()
    }

    #[test]
    fn versioned_struct_requires_a_lock_on_every_version() {
        let output = expand(quote! {
            __require_type_hash_locks
            roll_into Message
            #[type_hash_lock(hash = 1)]
            MessageV1 { pub value: u32 }
            MessageV2 { add { pub accepted: bool = true } }
        });

        assert!(output.contains("MessageV2 is missing a type hash lock"));
        assert!(output.contains("#[type_hash_lock(hash = 0)]"));
    }

    #[test]
    fn versioned_struct_accepts_locked_versions() {
        let output = expand(quote! {
            __require_type_hash_locks
            roll_into Message
            #[type_hash_lock(hash = 1)]
            MessageV1 { pub value: u32 }
            #[type_hash_lock(hash = 2)]
            MessageV2 { add { pub accepted: bool = true } }
        });

        assert!(!output.contains("compile_error"));
    }
}
