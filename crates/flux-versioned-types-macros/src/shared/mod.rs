mod generate;
mod parse;

pub(crate) use generate::{
    default_enum_attrs, default_struct_attrs, generate_evolving, is_closure,
};
pub(crate) use parse::{BaseBlock, EvolveBlock, EvolveInputGeneric, ParseEvolveOp};

pub(crate) fn runtime_crate() -> proc_macro2::TokenStream {
    use proc_macro_crate::{FoundCrate, crate_name};
    use quote::{format_ident, quote};

    match crate_name("flux-versioned-types").expect("flux-versioned-types must be a dependency") {
        FoundCrate::Itself => quote!(crate),
        FoundCrate::Name(name) => {
            let name = format_ident!("{}", name);
            quote!(::#name)
        }
    }
}

pub(crate) fn serde_crate_path() -> syn::LitStr {
    use proc_macro_crate::{FoundCrate, crate_name};

    let path = match crate_name("flux-versioned-types")
        .expect("flux-versioned-types must be a dependency")
    {
        FoundCrate::Itself => "crate::__private::serde".to_owned(),
        FoundCrate::Name(name) => format!("{name}::__private::serde"),
    };
    syn::LitStr::new(&path, proc_macro2::Span::call_site())
}
