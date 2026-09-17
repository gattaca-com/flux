use proc_macro::TokenStream;

mod evolve;
mod evolve_enum;
mod family;
mod rolling;
mod schema;
mod shared;

#[proc_macro]
pub fn evolve_struct(input: TokenStream) -> TokenStream {
    evolve::evolve_struct(input)
}

#[proc_macro]
pub fn evolve_enum(input: TokenStream) -> TokenStream {
    evolve_enum::evolve_enum(input)
}

#[proc_macro]
pub fn roll_chain_into(input: TokenStream) -> TokenStream {
    rolling::roll_chain_into(input)
}

#[proc_macro_derive(VersionedLeaves, attributes(leaves))]
pub fn derive_versioned_leaves(input: TokenStream) -> TokenStream {
    family::derive_versioned_leaves(input)
}

#[proc_macro_derive(TelemetrySchema, attributes(serde, telemetry_schema))]
pub fn derive_telemetry_schema(input: TokenStream) -> TokenStream {
    schema::derive_telemetry_schema(input)
}
