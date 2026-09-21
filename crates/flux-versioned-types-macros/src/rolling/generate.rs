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
            pub fn versioned_deserialize_vec(type_hash: u64, bytes: &[u8]) -> ::flux_versioned_types::bincode::Result<Vec<Self>> {
                match type_hash ^ 123456 {
                    #(<#previous as flux::type_hash::TypeHash>::TYPE_HASH => {
                        let v: Vec<#previous> = ::flux_versioned_types::bincode::deserialize(bytes)?;
                        Ok(v.into_iter().map(Into::into).collect())
                    },)*
                    <#last as flux::type_hash::TypeHash>::TYPE_HASH => Ok(::flux_versioned_types::bincode::deserialize(bytes)?),
                    _ => Err(Box::new(::flux_versioned_types::bincode::ErrorKind::Custom(format!("Invalid type hash: {}", type_hash)))),
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
    version_skipped: &[bool],
) -> TokenStream2 {
    let version_refs: Vec<&Ident> = version_names.iter().collect();
    let Some(ctx) = RollChainContext::new(roll_into, &version_refs) else {
        return TokenStream2::new();
    };
    let mut output = generate_type_alias_and_codec(&ctx);
    output.extend(generate_transitive_into_impls(&version_refs));
    if !wire_skip {
        output.extend(generate_versioned_impls(
            roll_into,
            &version_refs,
            wire_name,
            version_skipped,
        ));
    }
    output
}

fn generate_versioned_impls(
    alias: &Ident,
    versions: &[&Ident],
    wire_name: Option<&syn::LitStr>,
    version_skipped: &[bool],
) -> TokenStream2 {
    let Some(&last) = versions.last() else {
        return TokenStream2::new();
    };
    let name_tokens =
        wire_name.map_or_else(|| quote! { stringify!(#alias) }, |lit| quote! { #lit });
    // Skipped versions keep the bincode path only: no size or decode arms,
    // so both fall through to `None` / `UnknownTypeHash`.
    let wired: Vec<&Ident> = versions
        .iter()
        .zip(version_skipped.iter().chain(std::iter::repeat(&false)))
        .filter(|(_, skipped)| !**skipped)
        .map(|(version, _)| *version)
        .collect();
    let decode_arms = wired.iter().map(|version| {
        let migrate = if *version == last {
            quote! { Ok(slice.to_vec()) }
        } else {
            quote! { Ok(slice.iter().copied().map(::core::convert::Into::into).collect()) }
        };
        quote! {
            <#version as flux::type_hash::TypeHash>::TYPE_HASH => {
                match ::flux_versioned_types::byte_stable::cast_slice::<#version>(bytes) {
                    Ok(slice) => #migrate,
                    Err(::flux_versioned_types::byte_stable::CastError::Unaligned) => {
                        Err(::flux_versioned_types::DecodeError::Unaligned)
                    }
                    Err(::flux_versioned_types::byte_stable::CastError::Length { got, size }) => {
                        Err(::flux_versioned_types::DecodeError::LengthMismatch {
                            expected: if size == 0 {
                                0
                            } else {
                                size.saturating_mul(got / size + 1)
                            },
                            got,
                        })
                    }
                    Err(::flux_versioned_types::byte_stable::CastError::Invalid { .. }) => {
                        Err(::flux_versioned_types::DecodeError::InvalidValue)
                    }
                    Err(::flux_versioned_types::byte_stable::CastError::ZeroSized) => {
                        Err(::flux_versioned_types::DecodeError::LengthMismatch {
                            expected: 0,
                            got: bytes.len(),
                        })
                    }
                }
            }
        }
    });
    quote! {
        impl ::flux_versioned_types::Versioned for #last {
            const NAME: &'static str = #name_tokens;
            const VERSION_HASHES: &'static [u64] = &[
                #(<#wired as flux::type_hash::TypeHash>::TYPE_HASH,)*
            ];
            fn version_size(type_hash: u64) -> Option<usize> {
                match type_hash {
                    #(<#wired as flux::type_hash::TypeHash>::TYPE_HASH => Some(::core::mem::size_of::<#wired>()),)*
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

        const _: () = assert!(
            ::flux_versioned_types::leaves::name_fits(#name_tokens),
            "wire name longer than TYPE_NAME_LEN"
        );

        impl ::flux_versioned_types::HasVersionedLeaves for #last {
            const LEAF_NAMES: &'static [&'static str] = &[#name_tokens];
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
                if blob.type_name() == #name_tokens && blob.is::<Self>() {
                    Some(blob.decode::<U, Self>(scratch))
                } else {
                    None
                }
            }
        }
    }
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
