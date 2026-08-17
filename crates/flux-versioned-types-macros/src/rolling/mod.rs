use proc_macro::TokenStream;
use syn::parse_macro_input;

pub(crate) mod generate;
mod parse;

use parse::RollChainInput;

/// Generates rolling type infrastructure: type alias, versioned
/// deserialization, and conversions.
///
/// # Example
/// ```ignore
/// roll_chain_into!(MyType, [MyTypeV1, MyTypeV2, MyTypeV3]);
/// ```
///
/// This generates:
/// - `type MyType = MyTypeV3;`
/// - `impl MyTypeV3 { fn versioned_deserialize_vec(...) }` for version-aware
///   deserialization
/// - Transitive `Into` impls for version migration (V1→V2→V3, V1→V3)
pub fn roll_chain_into(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as RollChainInput);
    generate::generate_roll_chain(&input.name, &input.versions).into()
}
