//! `#[derive(ByteStable)]`. See `byte-stable` for the trait it implements.

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use quote::{ToTokens, quote};
use syn::{
    Attribute, Data, DataEnum, DataStruct, DeriveInput, Expr, ExprLit, Fields, Lit, Meta,
    MetaNameValue, parse_macro_input, punctuated::Punctuated, token::Comma,
};

/// Implements `ByteStable` for a `repr(C)`/`repr(transparent)` struct or a
/// `repr(u8)` fieldless enum.
///
/// # Requirements
///
/// - Structs must carry `#[repr(C)]` or `#[repr(transparent)]`; enums must
///   carry `#[repr(u8)]` and have only fieldless variants.
/// - Unions and lifetime parameters are rejected with a compile error.
/// - Every field type must implement `ByteStable`. Generic type parameters get
///   a `where T: ByteStable` bound automatically.
///
/// # What it proves
///
/// For structs the derive emits a `size_of` equality,
/// `size_of::<S>() == size_of::<F1>() + ...`, which fails `cargo check` for
/// any padded type (for `repr(C)` the equality holds iff there is no padding
/// anywhere, including trailing padding). Non-generic types get the assertion
/// at item level; generic types get it in an inline `const` inside `is_valid`,
/// checked at monomorphisation.
///
/// # Crate path
///
/// Every emitted path goes through one root: `#[byte_stable(crate =
/// "::some::path")]` on the item overrides it, otherwise the `byte-stable`
/// dependency of the calling crate is used (`proc_macro_crate`), falling back
/// to `::flux::byte_stable`. No import is assumed in the caller.
///
/// # Soundness argument (covers the generated `unsafe impl`)
///
/// 1. No padding: the required `repr` gives a defined layout and the `size_of`
///    proof rules out padding, so every byte is initialized.
/// 2. Exact validation: `is_valid` checks each field over its disjoint
///    `offset_of` range, tiling the whole value (enums check discriminant
///    membership), so only valid values pass.
/// 3. No interior mutability: every field type is `ByteStable`, which already
///    rules out `UnsafeCell`, so the property holds transitively.
///
/// Padding is rejected:
///
/// ```compile_fail
/// use byte_stable_derive::ByteStable;
///
/// #[derive(ByteStable)]
/// #[byte_stable(crate = "::byte_stable")]
/// #[repr(C)]
/// struct Padded {
///     a: u8,
///     b: u64,
/// }
/// ```
///
/// A missing `repr` is rejected:
///
/// ```compile_fail
/// use byte_stable_derive::ByteStable;
///
/// #[derive(ByteStable)]
/// #[byte_stable(crate = "::byte_stable")]
/// struct NoRepr {
///     a: u64,
/// }
/// ```
#[proc_macro_derive(ByteStable, attributes(byte_stable))]
pub fn derive_byte_stable(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match derive(&input) {
        Ok(expanded) => expanded.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn derive(input: &DeriveInput) -> Result<proc_macro2::TokenStream, syn::Error> {
    if input.generics.lifetimes().next().is_some() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "ByteStable cannot be derived for types with lifetimes",
        ));
    }
    let root = runtime_crate_path(input)?;
    match &input.data {
        Data::Struct(data) => derive_struct(input, data, &root),
        Data::Enum(data) => derive_enum(input, data, &root),
        Data::Union(_) => {
            Err(syn::Error::new_spanned(&input.ident, "ByteStable cannot be derived for unions"))
        }
    }
}

fn runtime_crate_path(input: &DeriveInput) -> Result<proc_macro2::TokenStream, syn::Error> {
    if let Some(path) = crate_override(&input.attrs)? {
        return Ok(path.to_token_stream());
    }
    Ok(match crate_name("byte-stable") {
        Ok(FoundCrate::Itself) => quote!(::byte_stable),
        Ok(FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote!(::#ident)
        }
        Err(_) => quote!(::flux::byte_stable),
    })
}

fn crate_override(attrs: &[Attribute]) -> Result<Option<syn::Path>, syn::Error> {
    for attr in attrs {
        if !attr.path().is_ident("byte_stable") {
            continue;
        }
        let Meta::List(list) = &attr.meta else {
            return Err(syn::Error::new_spanned(
                attr,
                "expected #[byte_stable(crate = \"::some::path\")]",
            ));
        };
        let metas = list.parse_args_with(Punctuated::<Meta, Comma>::parse_terminated)?;
        let Some(meta) = metas.into_iter().next() else {
            return Ok(None);
        };
        match meta {
            Meta::NameValue(MetaNameValue { path: key, value, .. }) if key.is_ident("crate") => {
                let Expr::Lit(ExprLit { lit: Lit::Str(path), .. }) = value else {
                    return Err(syn::Error::new_spanned(
                        value,
                        "expected a string literal: crate = \"::some::path\"",
                    ));
                };
                return Ok(Some(path.parse()?));
            }
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "unsupported key in #[byte_stable(...)]: only crate is supported",
                ));
            }
        }
    }
    Ok(None)
}

fn has_repr(attrs: &[Attribute], what: &str) -> bool {
    attrs.iter().filter(|attr| attr.path().is_ident("repr")).any(|attr| {
        let Meta::List(list) = &attr.meta else {
            return false;
        };
        let Ok(metas) = list.parse_args_with(Punctuated::<Meta, Comma>::parse_terminated) else {
            return false;
        };
        metas.iter().any(|meta| matches!(meta, Meta::Path(path) if path.is_ident(what)))
    })
}

fn derive_struct(
    input: &DeriveInput,
    data: &DataStruct,
    root: &proc_macro2::TokenStream,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let name = &input.ident;
    if !has_repr(&input.attrs, "C") && !has_repr(&input.attrs, "transparent") {
        return Err(syn::Error::new_spanned(
            name,
            "ByteStable derive requires #[repr(C)] or #[repr(transparent)] on structs",
        ));
    }
    // `packed` removes the padding the proof looks for but leaves fields
    // misaligned, which every `&field` read would then violate.
    if has_repr(&input.attrs, "packed") {
        return Err(syn::Error::new_spanned(name, "ByteStable derive rejects #[repr(packed)]"));
    }
    let mut members: Vec<(proc_macro2::TokenStream, syn::Type)> = Vec::new();
    match &data.fields {
        Fields::Named(named) => {
            for field in &named.named {
                let ident = field.ident.clone().expect("named field has an ident");
                members.push((quote!(#ident), field.ty.clone()));
            }
        }
        Fields::Unnamed(unnamed) => {
            for (index, field) in unnamed.unnamed.iter().enumerate() {
                let index = syn::Index::from(index);
                members.push((quote!(#index), field.ty.clone()));
            }
        }
        Fields::Unit => {}
    }
    let tys: Vec<&syn::Type> = members.iter().map(|(_, ty)| ty).collect();
    let (impl_generics, ty_generics, _) = input.generics.split_for_impl();
    let mut where_clause = input.generics.where_clause.clone();
    for param in input.generics.type_params() {
        let ident = &param.ident;
        let predicate: syn::WherePredicate = syn::parse_quote!(#ident: #root::ByteStable);
        where_clause
            .get_or_insert_with(|| syn::WhereClause {
                where_token: syn::token::Where::default(),
                predicates: Punctuated::new(),
            })
            .predicates
            .push(predicate);
    }
    let size = quote!(::core::mem::size_of::<#name #ty_generics>());
    let sum = quote!(0 #(+ ::core::mem::size_of::<#tys>())*);
    let body = if members.is_empty() {
        quote!(true)
    } else {
        let checks = members.iter().map(|(member, ty)| {
            quote!(
                <#ty as #root::ByteStable>::is_valid(
                    &bytes[::core::mem::offset_of!(Self, #member)
                        ..::core::mem::offset_of!(Self, #member) + ::core::mem::size_of::<#ty>()]
                )
            )
        });
        quote!(#(#checks)&&*)
    };
    // Field proofs first so a padded field reports at its own type; then the
    // size sum, which for `repr(C)` is zero padding. Non-generic types also
    // get an item-level copy so the failure surfaces at `cargo check`.
    let proof = quote!({
        #(let () = <#tys as #root::ByteStable>::LAYOUT_PROOF;)*
        ::core::assert!(#size == #sum);
    });
    let eager = input.generics.params.is_empty().then(|| quote!(const _: () = #proof;));
    Ok(quote!(
        #eager
        unsafe impl #impl_generics #root::ByteStable for #name #ty_generics #where_clause {
            const LAYOUT_PROOF: () = #proof;

            #[inline]
            fn is_valid(bytes: &[u8]) -> bool {
                #body
            }
        }
    ))
}

fn derive_enum(
    input: &DeriveInput,
    data: &DataEnum,
    root: &proc_macro2::TokenStream,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let name = &input.ident;
    if !has_repr(&input.attrs, "u8") {
        return Err(syn::Error::new_spanned(
            name,
            "ByteStable derive requires #[repr(u8)] on enums; only repr(u8) fieldless enums are supported",
        ));
    }
    let mut variants = Vec::new();
    for variant in &data.variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                &variant.ident,
                "ByteStable derive only supports fieldless enum variants",
            ));
        }
        variants.push(&variant.ident);
    }
    let body = if variants.is_empty() {
        quote!(let _ = bytes; false)
    } else {
        quote!(
            ::core::matches!(bytes.first(), ::core::option::Option::Some(&discriminant) if #(discriminant == Self::#variants as u8)||*)
        )
    };
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote!(
        const _: () = ::core::assert!(::core::mem::size_of::<#name>() == 1);
        unsafe impl #impl_generics #root::ByteStable for #name #ty_generics #where_clause {
            const LAYOUT_PROOF: () = ::core::assert!(::core::mem::size_of::<Self>() == 1);

            #[inline]
            fn is_valid(bytes: &[u8]) -> bool {
                #body
            }
        }
    ))
}
