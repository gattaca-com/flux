use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::Ident;

pub(crate) struct RollChainContext<'a> {
    pub name: &'a Ident,
    pub last: &'a Ident,
    pub previous: Vec<&'a Ident>,
}

impl<'a> RollChainContext<'a> {
    pub fn new(name: &'a Ident, versions: &[&'a Ident]) -> Option<Self> {
        if versions.is_empty() {
            return None;
        }

        let last = *versions.last().unwrap();
        let previous: Vec<_> = versions.iter().take(versions.len() - 1).copied().collect();

        Some(Self { name, last, previous })
    }
}

pub(crate) fn generate_type_alias_and_codec(ctx: &RollChainContext) -> TokenStream2 {
    let runtime = crate::shared::runtime_crate();
    let name = ctx.name;
    let last = ctx.last;
    let previous = &ctx.previous;

    quote! {
        pub type #name = #last;

        impl #last {
            #[inline]
            pub fn versioned_deserialize_vec(type_hash: u64, bytes: &[u8]) -> #runtime::__private::bincode::Result<::std::vec::Vec<Self>> {
                match type_hash ^ #runtime::STORED_TYPE_HASH_XOR {
                    #(<#previous as #runtime::__private::type_hash::TypeHash>::TYPE_HASH => {
                        let v: ::std::vec::Vec<#previous> = #runtime::__private::bincode::deserialize(bytes)?;
                        Ok(v.into_iter().map(Into::into).collect())
                    },)*
                    <#last as #runtime::__private::type_hash::TypeHash>::TYPE_HASH => Ok(#runtime::__private::bincode::deserialize(bytes)?),
                    _ => Err(::std::boxed::Box::new(#runtime::__private::bincode::ErrorKind::Custom(::std::format!("Invalid type hash: {}", type_hash)))),
                }
            }
        }
    }
}

pub(crate) fn generate_roll_chain(roll_into: &Ident, version_names: &[Ident]) -> TokenStream2 {
    let version_refs: Vec<&Ident> = version_names.iter().collect();
    let Some(ctx) = RollChainContext::new(roll_into, &version_refs) else {
        return TokenStream2::new();
    };
    let mut output = generate_type_alias_and_codec(&ctx);
    output.extend(generate_transitive_into_impls(&version_refs));
    output
}

pub(crate) fn generate_transitive_into_impls(versions: &[&Ident]) -> TokenStream2 {
    let mut output = TokenStream2::new();

    for (i, &from) in versions.iter().enumerate() {
        for &to in versions.iter().skip(i + 2) {
            let via = versions[i + 1];
            output.extend(quote! {
                impl Into<#to> for #from {
                    fn into(self) -> #to {
                        let via: #via = self.into();
                        via.into()
                    }
                }
            });
        }
    }

    output
}
