use syn::{
    Ident, Result, Token,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

pub(crate) struct RollChainInput {
    pub name: Ident,
    pub versions: Vec<Ident>,
}

impl Parse for RollChainInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let name: Ident = input.parse()?;
        input.parse::<Token![,]>()?;

        let content;
        syn::bracketed!(content in input);
        let versions: Punctuated<Ident, Token![,]> =
            content.parse_terminated(Ident::parse, Token![,])?;

        Ok(Self { name, versions: versions.into_iter().collect() })
    }
}
