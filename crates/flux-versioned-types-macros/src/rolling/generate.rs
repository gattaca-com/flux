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
    let name = ctx.name;
    let last = ctx.last;
    let previous = &ctx.previous;

    quote! {
        pub type #name = #last;

        impl #last {
            #[inline]
            pub fn versioned_deserialize_vec(type_hash: u64, bytes: &[u8]) -> bincode::Result<Vec<Self>> {
                match type_hash ^ 123456 {
                    #(<#previous as flux::type_hash::TypeHash>::TYPE_HASH => {
                        let v: Vec<#previous> = bincode::deserialize(bytes)?;
                        Ok(v.into_iter().map(Into::into).collect())
                    },)*
                    <#last as flux::type_hash::TypeHash>::TYPE_HASH => Ok(bincode::deserialize(bytes)?),
                    _ => Err(Box::new(bincode::ErrorKind::Custom(format!("Invalid type hash: {}", type_hash)))),
                }
            }
        }
    }
}

pub(crate) fn generate_roll_chain(
    roll_into: &Ident,
    version_names: &[Ident],
    wire_name: Option<&syn::LitStr>,
    wire_skip: bool,
) -> TokenStream2 {
    let version_refs: Vec<&Ident> = version_names.iter().collect();
    let Some(ctx) = RollChainContext::new(roll_into, &version_refs) else {
        return TokenStream2::new();
    };
    let mut output = generate_type_alias_and_codec(&ctx);
    output.extend(generate_transitive_into_impls(&version_refs));
    if !wire_skip {
        output.extend(generate_versioned_impls(roll_into, &version_refs, wire_name));
    }
    output
}

#[cfg(feature = "zerocopy")]
fn generate_versioned_impls(
    alias: &Ident,
    versions: &[&Ident],
    wire_name: Option<&syn::LitStr>,
) -> TokenStream2 {
    let Some(&last) = versions.last() else {
        return TokenStream2::new();
    };
    let name_tokens =
        wire_name.map_or_else(|| quote! { stringify!(#alias) }, |lit| quote! { #lit });
    let decode_arms = versions.iter().map(|version| {
        let migrate = if *version == last {
            quote! { Ok(slice.to_vec()) }
        } else {
            quote! { Ok(slice.iter().copied().map(::core::convert::Into::into).collect()) }
        };
        quote! {
            <#version as flux::type_hash::TypeHash>::TYPE_HASH => {
                match <[#version] as ::flux_versioned_types::zerocopy::TryFromBytes>::try_ref_from_bytes(bytes) {
                    Ok(slice) => #migrate,
                    Err(::flux_versioned_types::zerocopy::ConvertError::Alignment(_)) => {
                        Err(::flux_versioned_types::DecodeError::Unaligned)
                    }
                    Err(::flux_versioned_types::zerocopy::ConvertError::Size(_)) => {
                        let stride = ::core::mem::size_of::<#version>();
                        Err(::flux_versioned_types::DecodeError::LengthMismatch {
                            expected: if stride == 0 { 0 } else { stride * (bytes.len() / stride + 1) },
                            got: bytes.len(),
                        })
                    }
                    Err(::flux_versioned_types::zerocopy::ConvertError::Validity(_)) => {
                        Err(::flux_versioned_types::DecodeError::InvalidValue)
                    }
                }
            }
        }
    });
    quote! {
        impl ::flux_versioned_types::Versioned for #last {
            const NAME: &'static str = #name_tokens;
            const VERSION_HASHES: &'static [u64] = &[
                #(<#versions as flux::type_hash::TypeHash>::TYPE_HASH,)*
            ];
            fn version_size(type_hash: u64) -> Option<usize> {
                match type_hash {
                    #(<#versions as flux::type_hash::TypeHash>::TYPE_HASH => Some(::core::mem::size_of::<#versions>()),)*
                    _ => None,
                }
            }
            fn decode_versions(
                type_hash: u64,
                bytes: &[u8],
            ) -> Result<Vec<Self>, ::flux_versioned_types::DecodeError> {
                match type_hash {
                    #(#decode_arms,)*
                    _ => Err(::flux_versioned_types::DecodeError::UnknownTypeHash(type_hash)),
                }
            }
        }

        impl ::flux_versioned_types::HasVersionedLeaves for #last {
            fn visit_leaf<V: ::flux_versioned_types::VisitorVersionedLeaf>(
                &self,
                visitor: &mut V,
            ) {
                visitor.visit_leaf(self);
            }
            fn decode_blob<U: ::flux_versioned_types::Versioned>(
                blob: &::flux_versioned_types::Blob,
                scratch: &mut ::flux_versioned_types::Scratch,
            ) -> Option<::flux_versioned_types::Decoded<U, Self>> {
                if blob.is::<Self>() {
                    Some(blob.decode::<U, Self>(scratch))
                } else {
                    None
                }
            }
        }
    }
}

#[cfg(not(feature = "zerocopy"))]
fn generate_versioned_impls(
    _alias: &Ident,
    _versions: &[&Ident],
    _wire_name: Option<&syn::LitStr>,
) -> TokenStream2 {
    TokenStream2::new()
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
