use proc_macro::TokenStream;

mod evolve;
mod evolve_enum;
mod rolling;
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
