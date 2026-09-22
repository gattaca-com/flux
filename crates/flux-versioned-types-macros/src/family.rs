use proc_macro::TokenStream;
use proc_macro2::TokenStream as Tokens;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

pub fn derive_versioned_leaves(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match generate(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

struct Kept<'a> {
    variant: &'a syn::Ident,
    ty: &'a syn::Type,
}

type Skipped<'a> = (&'a syn::Ident, &'a Fields);

impl Kept<'_> {
    /// The wire names this variant contributes.
    fn names(&self) -> Tokens {
        let ty = self.ty;
        quote! { <#ty as ::flux_versioned_types::HasVersionedLeaves>::LEAF_NAMES }
    }

    fn visit_arm(&self) -> Tokens {
        let v = self.variant;
        quote! { Self::#v(x) => x.visit_leaf(visitor) }
    }

    fn decode_step(&self) -> Tokens {
        let (v, ty) = (self.variant, self.ty);
        quote! {
            if let Some(found) = <#ty as ::flux_versioned_types::HasVersionedLeaves>::decode_blob::<U>(blob, scratch) {
                return Some(found.map(|(meta, msgs): (U, Vec<_>)| {
                    (meta, msgs.into_iter().map(|m| m.map(Self::#v)).collect())
                }));
            }
        }
    }

    fn owned_decode_iter_step(&self) -> Tokens {
        let (v, ty) = (self.variant, self.ty);
        quote! {
            if <#ty as ::flux_versioned_types::HasVersionedLeaves>::LEAF_NAMES.contains(&blob.type_name()) {
                return <#ty as ::flux_versioned_types::HasVersionedLeaves>::into_decode_iter::<U>(blob)
                    .map(|found| found.map(|(meta, msgs)| {
                        let mapped = ::flux_versioned_types::leaves::FamilyIter::new(msgs, Self::#v);
                        (meta, Box::new(mapped) as Box<dyn ExactSizeIterator<Item = _>>)
                    }));
            }
        }
    }

    fn decode_iter_step(&self) -> Tokens {
        let (v, ty) = (self.variant, self.ty);
        quote! {
            if let Some(found) = <#ty as ::flux_versioned_types::HasVersionedLeaves>::decode_iter::<U>(blob) {
                return Some(found.map(|(meta, msgs)| {
                    let mapped = ::flux_versioned_types::leaves::FamilyIter::new(msgs, Self::#v);
                    (meta, Box::new(mapped) as Box<dyn ExactSizeIterator<Item = _> + '_>)
                }));
            }
        }
    }
}

fn parse_variants(data: &syn::DataEnum) -> syn::Result<(Vec<Kept<'_>>, Vec<Skipped<'_>>)> {
    let mut kept = Vec::new();
    let mut skipped = Vec::new();
    for variant in &data.variants {
        let mut skip = false;
        for attr in variant.attrs.iter().filter(|a| a.path().is_ident("leaves")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("skip") {
                    skip = true;
                    Ok(())
                } else {
                    Err(meta.error("expected `skip`"))
                }
            })?;
        }
        if skip {
            skipped.push((&variant.ident, &variant.fields));
            continue;
        }
        let ty = match &variant.fields {
            Fields::Unnamed(f) if f.unnamed.len() == 1 => &f.unnamed[0].ty,
            _ => {
                return Err(syn::Error::new_spanned(
                    &variant.ident,
                    "VersionedLeaves variants must have exactly one unnamed field; use #[leaves(skip)] to exclude",
                ));
            }
        };
        kept.push(Kept { variant: &variant.ident, ty });
    }
    Ok((kept, skipped))
}

fn checks(family: &str, kept: &[Kept<'_>]) -> Tokens {
    let pairs = kept.iter().enumerate().flat_map(|(i, a)| {
        kept.iter().skip(i + 1).map(move |b| {
            let msg = format!(
                "family `{family}`: variants `{}` and `{}` hold leaves with the same name; wrap one in its own leaf type",
                a.variant, b.variant
            );
            let (na, nb) = (a.names(), b.names());
            quote! { assert!(::flux_versioned_types::leaves::names_disjoint(#na, #nb), #msg); }
        })
    });
    quote! { #(#pairs)* }
}

fn generate(input: &DeriveInput) -> syn::Result<Tokens> {
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "VersionedLeaves can only be derived for enums",
        ));
    };
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let (kept, skipped) = parse_variants(data)?;

    let visit_kept = kept.iter().map(Kept::visit_arm);
    let visit_skipped = skipped.iter().map(|(v, fields)| match fields {
        Fields::Named(_) => quote! { Self::#v { .. } => {} },
        Fields::Unnamed(_) => quote! { Self::#v(..) => {} },
        Fields::Unit => quote! { Self::#v => {} },
    });
    let leaf_names = kept.iter().map(Kept::names);
    let leaf_names_len = kept.iter().map(|k| {
        let names = k.names();
        quote! { #names.len() }
    });
    let decode_steps = kept.iter().map(Kept::decode_step);
    let decode_iter_steps = kept.iter().map(Kept::decode_iter_step);
    let into_decode_iter_steps = kept.iter().map(Kept::owned_decode_iter_step);
    // `From` only for field types that appear once; a repeated type has no
    // single variant to map to.
    let type_key = |ty: &syn::Type| quote! { #ty }.to_string();
    let from_impls = kept
        .iter()
        .filter(|k| kept.iter().filter(|o| type_key(o.ty) == type_key(k.ty)).count() == 1)
        .map(|k| {
            let (v, ty) = (k.variant, k.ty);
            quote! {
                impl #impl_generics From<#ty> for #name #ty_generics #where_clause {
                    fn from(x: #ty) -> Self {
                        Self::#v(x)
                    }
                }
            }
        });
    let checks = checks(&name.to_string(), &kept);
    let eager = input.generics.params.is_empty().then(|| quote! { const _: () = { #checks }; });

    Ok(quote! {
        #eager
        impl #impl_generics ::flux_versioned_types::HasVersionedLeaves for #name #ty_generics #where_clause {
            const LEAF_NAMES: &'static [&'static str] = &::flux_versioned_types::leaves::concat_names::<
                { 0 #(+ #leaf_names_len)* }
            >(&[#(#leaf_names),*]);
            fn visit_leaf<V: ::flux_versioned_types::VisitorVersionedLeaf>(
                &self,
                visitor: &mut V,
            ) {
                const { #checks };
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
            fn into_decode_iter<U: ::flux_versioned_types::Versioned>(
                blob: ::flux_versioned_types::DecompressedBlob,
            ) -> Option<::flux_versioned_types::OwnedDecodedIter<U, Self>>
            where
                Self: 'static,
            {
                #(#into_decode_iter_steps)*
                None
            }
            fn decode_iter<U: ::flux_versioned_types::Versioned>(
                blob: &::flux_versioned_types::DecompressedBlob,
            ) -> Option<::flux_versioned_types::DecodedIter<'_, U, Self>> {
                #(#decode_iter_steps)*
                None
            }
        }
        #(#from_impls)*
    })
}
