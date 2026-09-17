use proc_macro::TokenStream;
use proc_macro2::TokenStream as Tokens;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr, parse_macro_input};

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
    name: Option<LitStr>,
}

type Skipped<'a> = (&'a syn::Ident, &'a Fields);

impl Kept<'_> {
    /// The wire names this variant contributes.
    fn names(&self) -> Tokens {
        let ty = self.ty;
        self.name.as_ref().map_or_else(
            || quote! { <#ty as ::flux_versioned_types::HasVersionedLeaves>::LEAF_NAMES },
            |n| quote! { &[#n] },
        )
    }

    fn names_len(&self) -> Tokens {
        let ty = self.ty;
        self.name.as_ref().map_or_else(
            || quote! { <#ty as ::flux_versioned_types::HasVersionedLeaves>::LEAF_NAMES.len() },
            |_| quote! { 1 },
        )
    }

    fn visit_arm(&self) -> Tokens {
        let (v, ty) = (self.variant, self.ty);
        // Calling the visitor directly needs `ty: Versioned`, so a name
        // override on a sub-family variant is a type error.
        self.name.as_ref().map_or_else(
            || quote! { Self::#v(x) => x.visit_leaf(visitor) },
            |n| quote! { Self::#v(x) => visitor.visit_leaf::<#ty>(#n, x) },
        )
    }

    fn visit_step(&self) -> Tokens {
        let (v, ty) = (self.variant, self.ty);
        self.name.as_ref().map_or_else(
            || {
                quote! {
                    if let Some(out) = <#ty as ::flux_versioned_types::HasVersionedLeaves>::visit_leaf_types(visitor, &|x| wrap(Self::#v(x))) {
                        return Some(out);
                    }
                }
            },
            |n| {
                quote! {
                    if let Some(out) = visitor.visit_type::<#ty>(#n, &|x| wrap(Self::#v(x))) {
                        return Some(out);
                    }
                }
            },
        )
    }
}

fn parse_variants(data: &syn::DataEnum) -> syn::Result<(Vec<Kept<'_>>, Vec<Skipped<'_>>)> {
    let mut kept = Vec::new();
    let mut skipped = Vec::new();
    for variant in &data.variants {
        let mut skip = false;
        let mut name = None;
        for attr in variant.attrs.iter().filter(|a| a.path().is_ident("leaves")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("skip") {
                    skip = true;
                } else if meta.path.is_ident("name") {
                    name = Some(meta.value()?.parse::<LitStr>()?);
                } else {
                    return Err(meta.error("expected `skip` or `name = \"..\"`"));
                }
                Ok(())
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
        kept.push(Kept { variant: &variant.ident, ty, name });
    }
    Ok((kept, skipped))
}

fn checks(family: &str, kept: &[Kept<'_>]) -> Tokens {
    let pairs = kept.iter().enumerate().flat_map(|(i, a)| {
        kept.iter().skip(i + 1).map(move |b| {
            let msg = format!(
                "family `{family}`: variants `{}` and `{}` reach the same wire name; set #[leaves(name = \"..\")] on one",
                a.variant, b.variant
            );
            let (na, nb) = (a.names(), b.names());
            quote! { assert!(::flux_versioned_types::leaves::names_disjoint(#na, #nb), #msg); }
        })
    });
    let fits = kept.iter().filter_map(|k| {
        let n = k.name.as_ref()?;
        let msg =
            format!("family `{family}`: wire name of `{}` is longer than TYPE_NAME_LEN", k.variant);
        Some(quote! { assert!(::flux_versioned_types::leaves::name_fits(#n), #msg); })
    });
    quote! { #(#pairs)* #(#fits)* }
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
    let leaf_names_len = kept.iter().map(Kept::names_len);
    let visit_steps = kept.iter().map(Kept::visit_step);
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
            fn visit_leaf_types<Root, V: ::flux_versioned_types::VisitorLeafType<Root>>(
                visitor: &mut V,
                wrap: &dyn Fn(Self) -> Root,
            ) -> Option<V::Out> {
                #(#visit_steps)*
                None
            }
        }
        #(#from_impls)*
    })
}
