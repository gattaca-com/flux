use proc_macro::TokenStream;
use proc_macro_crate::{crate_name, FoundCrate};
use quote::{quote, ToTokens};
use syn::{
    parse_macro_input, Attribute, Data, DataStruct, DeriveInput, Expr, ExprLit, Fields, Lit, Meta,
    MetaNameValue,
};

fn runtime_crate_path() -> proc_macro2::TokenStream {
    match crate_name("type-hash") {
        Ok(FoundCrate::Itself) => quote!(::type_hash),
        Ok(FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote!(::#ident)
        }
        Err(_) => {
            // fallback: allow a module path used by workspace re-export patterns
            quote!(::flux::type_hash)
        }
    }
}

fn type_hash_literal(attrs: &[Attribute]) -> Result<Option<String>, syn::Error> {
    for attr in attrs {
        if !attr.path().is_ident("type_hash") {
            continue;
        }

        let Meta::List(list) = &attr.meta else {
            return Err(syn::Error::new_spanned(attr, "expected #[type_hash(literal = \"...\")]"));
        };

        // Parse as a comma-separated list of Meta
        let metas = list.parse_args_with(
            syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
        )?;

        if let Some(meta) = metas.into_iter().next() {
            match meta {
                Meta::NameValue(MetaNameValue { path, value, .. }) if path.is_ident("literal") => {
                    match value {
                        Expr::Lit(ExprLit { lit: Lit::Str(s), .. }) => {
                            return Ok(Some(s.value()));
                        }
                        _ => {
                            return Err(syn::Error::new_spanned(
                                value,
                                "expected string literal: literal = \"...\"",
                            ));
                        }
                    }
                }

                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "unsupported key in #[type_hash(...)] (only literal is supported)",
                    ));
                }
            }
        }

        return Err(syn::Error::new_spanned(attr, "expected #[type_hash(literal = \"...\")]"));
    }

    Ok(None)
}
use syn::{DataEnum, WhereClause, WherePredicate};

fn derive_for_enum(
    input: &DeriveInput,
    en: &DataEnum,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let name = &input.ident;
    let generics = input.generics.clone();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let mut steps: Vec<proc_macro2::TokenStream> = Vec::new();
    let th = runtime_crate_path();

    for variant in &en.variants {
        let v_ident = &variant.ident;
        let v_name = v_ident.to_string();

        // Variant name
        steps.push(quote! {
            h = #th::fnv1a64_str(h, #v_name);
        });

        // Discriminant (best-effort)
        if let Some((_, expr)) = &variant.discriminant {
            // If it's an integer literal, hash the value; else hash tokens.
            match expr {
                Expr::Lit(ExprLit { lit: Lit::Int(li), .. }) => {
                    // Parse to u128 then down-mix
                    let val_u128 = li.base10_parse::<u128>()?;
                    let lo = (val_u128 & 0xFFFF_FFFF_FFFF_FFFF) as u64;
                    let hi = ((val_u128 >> 64) & 0xFFFF_FFFF_FFFF_FFFF) as u64;

                    steps.push(quote! {
                        h ^= #th::mix64(#lo);
                        h ^= #th::mix64(#hi);
                        h = #th::mix64(h);
                    });
                }
                other => {
                    let tokens = other.to_token_stream();
                    steps.push(quote! {
                        h = #th::fnv1a64_str(h, stringify!(#tokens));
                        h = #th::mix64(h);
                    });
                }
            }
        } else {
            // No explicit discriminant: still distinguish “implicit” vs “explicit”
            steps.push(quote! {
                h = #th::fnv1a64_str(h, "<implicit_discriminant>");
                h = #th::mix64(h);
            });
        }

        // Fields
        match &variant.fields {
            Fields::Unit => {
                steps.push(quote! {
                    h = #th::fnv1a64_str(h, "<unit>");
                    h = #th::mix64(h);
                });
            }
            Fields::Unnamed(unnamed) => {
                steps.push(quote! {
                    h = #th::fnv1a64_str(h, "<tuple>");
                    h = #th::mix64(h);
                });

                for (i, f) in unnamed.unnamed.iter().enumerate() {
                    let idx_str = i.to_string();
                    let ty = &f.ty;
                    let ty_tokens = ty.to_token_stream();

                    let override_lit = type_hash_literal(&f.attrs)?;

                    if let Some(lit) = override_lit {
                        // overridden foreign/canonical
                        push_where_pred(
                            &mut where_clause.cloned(),
                            syn::parse_quote!(#ty: ::core::marker::Copy + ::core::marker::Sized),
                        );
                        steps.push(quote! {
                            h = #th::fnv1a64_str(h, #idx_str);
                            h = #th::fnv1a64_str(h, #lit);

                            h ^= #th::mix64(::core::mem::size_of::<#ty>() as u64);
                            h ^= #th::mix64(::core::mem::align_of::<#ty>() as u64);

                            h = #th::mix64(h);
                        });
                    } else {
                        // default: trait-based
                        push_where_pred(
                            &mut where_clause.cloned(),
                            syn::parse_quote!(#ty: #th::TypeHash),
                        );
                        steps.push(quote! {
                            h = #th::fnv1a64_str(h, #idx_str);
                            h = #th::fnv1a64_str(h, stringify!(#ty_tokens));

                            h ^= #th::mix64(<#ty as #th::TypeHash>::TYPE_HASH);

                            h ^= #th::mix64(::core::mem::size_of::<#ty>() as u64);
                            h ^= #th::mix64(::core::mem::align_of::<#ty>() as u64);

                            h = #th::mix64(h);
                        });
                    }
                }
            }
            Fields::Named(named) => {
                steps.push(quote! {
                    h = #th::fnv1a64_str(h, "<struct>");
                    h = #th::mix64(h);
                });

                for f in &named.named {
                    let f_ident = f.ident.as_ref().unwrap();
                    let f_name = f_ident.to_string();
                    let ty = &f.ty;
                    let ty_tokens = ty.to_token_stream();

                    let override_lit = type_hash_literal(&f.attrs)?;

                    if let Some(lit) = override_lit {
                        push_where_pred(
                            &mut where_clause.cloned(),
                            syn::parse_quote!(#ty: ::core::marker::Copy + ::core::marker::Sized),
                        );
                        steps.push(quote! {
                            h = #th::fnv1a64_str(h, #f_name);
                            h = #th::fnv1a64_str(h, #lit);

                            h ^= #th::mix64(::core::mem::size_of::<#ty>() as u64);
                            h ^= #th::mix64(::core::mem::align_of::<#ty>() as u64);

                            h = #th::mix64(h);
                        });
                    } else {
                        push_where_pred(
                            &mut where_clause.cloned(),
                            syn::parse_quote!(#ty: #th::TypeHash),
                        );
                        steps.push(quote! {
                            h = #th::fnv1a64_str(h, #f_name);
                            h = #th::fnv1a64_str(h, stringify!(#ty_tokens));

                            h ^= #th::mix64(<#ty as #th::TypeHash>::TYPE_HASH);

                            h ^= #th::mix64(::core::mem::size_of::<#ty>() as u64);
                            h ^= #th::mix64(::core::mem::align_of::<#ty>() as u64);

                            h = #th::mix64(h);
                        });
                    }
                }
            }
        }

        // Separator per variant
        steps.push(quote! {
            h = #th::fnv1a64_str(h, "<end_variant>");
            h = #th::mix64(h);
        });
    }

    let where_clause_tokens = where_clause;

    Ok(quote! {
        impl #impl_generics #th::TypeHash for #name #ty_generics #where_clause_tokens {
            const TYPE_HASH: u64 = {
                    let mut h: u64 = 0xcbf29ce484222325u64;

                    // Enum name
                    h = #th::fnv1a64_str(h, stringify!(#name));

                    // Enum layout
                    h ^= #th::mix64(::core::mem::size_of::<Self>() as u64);
                    h ^= #th::mix64(::core::mem::align_of::<Self>() as u64);
                    h = #th::mix64(h);

                    #(#steps)*

                    h
                };
        }
    })
}

fn push_where_pred(where_clause: &mut Option<WhereClause>, pred: WherePredicate) {
    where_clause
        .get_or_insert_with(|| WhereClause {
            where_token: Default::default(),
            predicates: Default::default(),
        })
        .predicates
        .push(pred);
}

#[proc_macro_derive(TypeHash, attributes(type_hash))]
pub fn derive_type_struct_hash(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;

    let expanded = match &input.data {
        Data::Struct(s) => derive_for_struct(&input, s),
        Data::Enum(e) => derive_for_enum(&input, e).unwrap().to_token_stream(),
        _ => {
            return syn::Error::new_spanned(
                &input.ident,
                format!("{name}: TypeHash can only be derived for structs or enums"),
            )
            .to_compile_error()
            .into();
        }
    };

    expanded.into()
}
fn derive_for_struct(input: &DeriveInput, data: &DataStruct) -> proc_macro2::TokenStream {
    let name = &input.ident;
    let generics = input.generics.clone();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let mut where_clause = where_clause.cloned();
    // We support named structs and tuple structs.
    // Unit structs hash just the struct layout.
    let mut field_steps = Vec::new();
    let th = runtime_crate_path();

    match &data.fields {
        Fields::Named(named) => {
            for f in &named.named {
                let ident = f.ident.as_ref().unwrap();
                let field_name_str = ident.to_string();
                let ty = &f.ty;
                let ty_tokens = ty.to_token_stream();

                // Add trait bounds: field type must be Copy + Sized + TypeHash.
                // (TypeHash already includes Copy+Sized, but keeping the intent explicit is
                // fine.) We'll attach bounds on the impl where-clause.
                let override_kind = match type_hash_literal(&f.attrs) {
                    Ok(v) => v,
                    Err(e) => return e.to_compile_error(),
                };

                if let Some(lit) = override_kind {
                    // Foreign/canonical override: don't require typeTypeHash.
                    // Keep your "only Copy/Sized" policy:
                    let pred: syn::WherePredicate =
                        syn::parse_quote! { #ty: ::core::marker::Copy + ::core::marker::Sized };
                    where_clause
                        .get_or_insert_with(|| syn::WhereClause {
                            where_token: Default::default(),
                            predicates: Default::default(),
                        })
                        .predicates
                        .push(pred);

                    field_steps.push(quote! {
                        h = #th::fnv1a64_str(h, #field_name_str);

                        // Canonical type name chosen by the user:
                        h = #th::fnv1a64_str(h, #lit);

                        // Layout still reflects the *real* type:
                        h ^= #th::mix64(::core::mem::size_of::<#ty>() as u64);
                        h ^= #th::mix64(::core::mem::align_of::<#ty>() as u64);

                        // Offset in the struct:
                        h ^= #th::mix64(::core::mem::offset_of!(Self, #ident) as u64);

                        h = #th::mix64(h);
                    });
                } else {
                    // Default behavior: require typeTypeHash
                    let pred: syn::WherePredicate = syn::parse_quote! { #ty: #th::TypeHash };
                    where_clause
                        .get_or_insert_with(|| syn::WhereClause {
                            where_token: Default::default(),
                            predicates: Default::default(),
                        })
                        .predicates
                        .push(pred);

                    field_steps.push(quote! {
                        h = #th::fnv1a64_str(h, #field_name_str);

                        // (Optional) keep literal spelling too:
                        h = #th::fnv1a64_str(h, stringify!(#ty_tokens));

                        h ^= #th::mix64(<#ty as #th::TypeHash>::TYPE_HASH);

                        h ^= #th::mix64(::core::mem::size_of::<#ty>() as u64);
                        h ^= #th::mix64(::core::mem::align_of::<#ty>() as u64);

                        h ^= #th::mix64(::core::mem::offset_of!(Self, #ident) as u64);

                        h = #th::mix64(h);
                    });
                }
            }
        }
        Fields::Unnamed(unnamed) => {
            for (idx, f) in unnamed.unnamed.iter().enumerate() {
                let index = syn::Index::from(idx);
                let field_name_str = idx.to_string(); // "0", "1", ...
                let ty = &f.ty;
                let ty_tokens = ty.to_token_stream();

                let pred: syn::WherePredicate = syn::parse_quote! { #ty: #th::TypeHash };
                where_clause
                    .get_or_insert_with(|| syn::WhereClause {
                        where_token: Default::default(),
                        predicates: Default::default(),
                    })
                    .predicates
                    .push(pred);

                field_steps.push(quote! {
                    h = #th::fnv1a64_str(h, #field_name_str);
                    h = #th::fnv1a64_str(h, stringify!(#ty_tokens));

                    h ^= #th::mix64(<#ty as #th::TypeHash>::TYPE_HASH);

                    h ^= #th::mix64(::core::mem::size_of::<#ty>() as u64);
                    h ^= #th::mix64(::core::mem::align_of::<#ty>() as u64);

                    h ^= #th::mix64(::core::mem::offset_of!(Self, #index) as u64);

                    h = #th::mix64(h);
                });
            }
        }
        Fields::Unit => {}
    }

    let where_clause_tokens = where_clause;

    let expanded = quote! {
        impl #impl_generics #th::TypeHash for #name #ty_generics #where_clause_tokens {
            const TYPE_HASH: u64 = {

                    let mut h: u64 = 0xcbf29ce484222325u64;

                    // Struct name as written (identifier)
                    h = #th::fnv1a64_str(h, stringify!(#name));

                    // Struct layout
                    h ^= #th::mix64(::core::mem::size_of::<Self>() as u64);
                    h ^= #th::mix64(::core::mem::align_of::<Self>() as u64);

                    #(#field_steps)*

                    h
                };

            }

    };

    expanded
}

// Example:
// #[derive(Clone, Copy, Debug, TypeHash)]
// #[type_hash_lock(hash=16166293017989477462)]
// struct MyTypeV1 {
//     a: u8,
//     b: u8,
//     c: u8,
// }
#[proc_macro_attribute]
pub fn type_hash_lock(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = &input.ident;

    let meta = parse_macro_input!(attr as Meta);
    let Meta::NameValue(MetaNameValue { path, value, .. }) = &meta else {
        return syn::Error::new_spanned(&meta, "expected #[type_hash_lock(hash=xxx)]")
            .to_compile_error()
            .into();
    };

    if path.is_ident("hash") {
        if let Expr::Lit(ExprLit { lit: Lit::Int(li), .. }) = value {
            let hash_value = li.base10_parse::<u64>().expect("expected integer literal: hash=xxx");
            let th = runtime_crate_path();
            let expanded = quote! {
                #input

                #[allow(unknown_lints, eq_op)]
                const _: [(); 0 - {
                    const ASSERT: bool =
                        ((<#name as #th::TypeHash>::TYPE_HASH) == #hash_value);
                    !ASSERT as u64 * (<#name as #th::TypeHash>::TYPE_HASH)
                } as usize] = [];
            };
            return expanded.into();
        }
    }
    syn::Error::new_spanned(&meta, "expected #[type_hash_lock(hash=123456)]")
        .to_compile_error()
        .into()
}
