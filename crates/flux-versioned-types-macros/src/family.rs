use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

pub fn derive_versioned_leaves(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match generate(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn generate(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "VersionedLeaves can only be derived for enums",
        ));
    };
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let mut kept: Vec<(&syn::Ident, &syn::Type)> = Vec::new();
    let mut skipped: Vec<(&syn::Ident, &Fields)> = Vec::new();
    let mut seen: std::collections::HashMap<String, &syn::Ident> = std::collections::HashMap::new();
    for variant in &data.variants {
        let mut skip = false;
        for attr in &variant.attrs {
            if attr.path().is_ident("leaves") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("skip") {
                        skip = true;
                        Ok(())
                    } else {
                        Err(meta.error("expected `skip`"))
                    }
                })?;
            }
        }
        if skip {
            skipped.push((&variant.ident, &variant.fields));
            continue;
        }
        let Fields::Unnamed(fields) = &variant.fields else {
            return Err(syn::Error::new_spanned(
                &variant.ident,
                "VersionedLeaves variants must have exactly one unnamed field; use #[leaves(skip)] to exclude",
            ));
        };
        if fields.unnamed.len() != 1 {
            return Err(syn::Error::new_spanned(
                &variant.ident,
                "VersionedLeaves variants must have exactly one unnamed field; use #[leaves(skip)] to exclude",
            ));
        }
        let ty = &fields.unnamed[0].ty;
        let key = quote! { #ty }.to_string();
        if let Some(first) = seen.get(&key) {
            return Err(syn::Error::new_spanned(
                &variant.ident,
                format!("duplicate leaf type also used by variant `{first}`; ambiguous `From`",),
            ));
        }
        seen.insert(key, &variant.ident);
        kept.push((&variant.ident, ty));
    }

    let visit_kept = kept.iter().map(|(vname, _)| {
        quote! { Self::#vname(x) => x.visit_leaf(visitor) }
    });
    let visit_skipped = skipped.iter().map(|(vname, fields)| match fields {
        Fields::Named(_) => quote! { Self::#vname { .. } => {} },
        Fields::Unnamed(_) => quote! { Self::#vname(..) => {} },
        Fields::Unit => quote! { Self::#vname => {} },
    });
    let decode_steps = kept.iter().map(|(_, ty)| {
        quote! {
            if let Some(found) = <#ty as ::flux_versioned_types::HasVersionedLeaves>::decode_blob::<U>(blob, scratch) {
                return Some(found.map(|(meta, msgs)| {
                    (meta, msgs.into_iter().map(|m| m.map(Self::from)).collect())
                }));
            }
        }
    });
    let from_impls = kept.iter().map(|(vname, ty)| {
        quote! {
            impl #impl_generics From<#ty> for #name #ty_generics #where_clause {
                fn from(x: #ty) -> Self {
                    Self::#vname(x)
                }
            }
        }
    });

    Ok(quote! {
        impl #impl_generics ::flux_versioned_types::HasVersionedLeaves for #name #ty_generics #where_clause {
            fn visit_leaf<V: ::flux_versioned_types::VisitorVersionedLeaf>(
                &self,
                visitor: &mut V,
            ) {
                match self {
                    #(#visit_kept,)*
                    #(#visit_skipped,)*
                }
            }
            fn decode_blob<U: ::flux_versioned_types::Versioned>(
                blob: &::flux_versioned_types::Blob,
                scratch: &mut ::flux_versioned_types::Scratch,
            ) -> Option<::flux_versioned_types::Decoded<U, Self>> {
                #(#decode_steps)*
                None
            }
        }
        #(#from_impls)*
    })
}
