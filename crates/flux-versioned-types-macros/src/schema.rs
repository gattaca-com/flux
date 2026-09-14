use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Error, Expr, Fields, Result, Token, Type, parse_macro_input,
};

pub(crate) fn derive_telemetry_schema(input: TokenStream) -> TokenStream {
    match derive(parse_macro_input!(input as DeriveInput)) {
        Ok(output) => output.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

fn derive(input: DeriveInput) -> Result<TokenStream2> {
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            input.generics,
            "TelemetrySchema does not support generic types",
        ));
    }
    let name = input.ident;
    let explicit = explicit_proxy(&input.attrs)?;
    reject_serde_changes(&input.attrs, explicit.is_some(), true)?;
    if let Some(proxy) = explicit {
        return Ok(quote! {
            impl ::flux_versioned_types::TelemetrySchema for #name {
                type Proxy = #proxy;
                type FlattenedProxy = #proxy;
            }
        });
    }
    let proxy = format_ident!("__TelemetrySchemaProxyFor{}", name);
    let flattened = format_ident!("__TelemetrySchemaFlattenedProxyFor{}", name);
    let transparent = serde_transparent(&input.attrs)?;

    match input.data {
        Data::Struct(data) if transparent => {
            let field = match data.fields {
                Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                    fields.unnamed.first().unwrap().clone()
                }
                Fields::Named(fields) if fields.named.len() == 1 => {
                    fields.named.first().unwrap().clone()
                }
                fields => {
                    return Err(Error::new_spanned(
                        fields,
                        "serde(transparent) requires exactly one field",
                    ));
                }
            };
            let ty = field_proxy_ty(&field)?;
            let flattened_ty = field_flattened_proxy_ty(&field)?;
            Ok(quote! {
                impl ::flux_versioned_types::TelemetrySchema for #name {
                    type Proxy = #ty;
                    type FlattenedProxy = #flattened_ty;
                }
            })
        }
        Data::Struct(data) => derive_struct(&name, &proxy, data.fields),
        Data::Enum(data) => {
            derive_enum(&name, &proxy, &flattened, data.variants.into_iter().collect())
        }
        Data::Union(data) => {
            Err(Error::new_spanned(data.union_token, "TelemetrySchema does not support unions"))
        }
    }
}

fn derive_struct(name: &syn::Ident, proxy: &syn::Ident, fields: Fields) -> Result<TokenStream2> {
    match fields {
        Fields::Named(fields) => {
            let fields = fields
                .named
                .iter()
                .map(|field| {
                    let name = &field.ident;
                    let ty = field_proxy_ty(field)?;
                    Ok(quote!(pub #name: #ty))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(quote! {
                #[doc(hidden)]
                #[allow(non_camel_case_types, clippy::pub_underscore_fields)]
                #[derive(::serde::Deserialize)]
                pub struct #proxy { #(#fields,)* }
                impl ::flux_versioned_types::TelemetrySchema for #name { type Proxy = #proxy; type FlattenedProxy = #proxy; }
            })
        }
        Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
            let ty = field_proxy_ty(fields.unnamed.first().unwrap())?;
            Ok(quote! {
                #[doc(hidden)] #[allow(non_camel_case_types)] #[derive(::serde::Deserialize)]
                pub struct #proxy(pub #ty);
                impl ::flux_versioned_types::TelemetrySchema for #name { type Proxy = #proxy; type FlattenedProxy = #proxy; }
            })
        }
        fields => Err(Error::new_spanned(
            fields,
            "TelemetrySchema only supports named structs and newtype structs",
        )),
    }
}

fn derive_enum(
    name: &syn::Ident,
    proxy: &syn::Ident,
    flattened: &syn::Ident,
    variants: Vec<syn::Variant>,
) -> Result<TokenStream2> {
    let mut enum_variants = Vec::new();
    let mut flat_fields = Vec::new();
    let mut nested_defs = Vec::new();
    for variant in variants {
        if explicit_proxy(&variant.attrs)?.is_some() {
            return Err(Error::new_spanned(
                variant,
                "telemetry_schema proxy overrides are not supported on enum variants",
            ));
        }
        reject_serde_changes(&variant.attrs, false, false)?;
        let variant_name = variant.ident;
        match variant.fields {
            Fields::Unit => {
                enum_variants.push(quote!(#variant_name));
                flat_fields.push(quote!(pub #variant_name: Option<()>));
            }
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                let ty = field_proxy_ty(fields.unnamed.first().unwrap())?;
                enum_variants.push(quote!(#variant_name(#ty)));
                flat_fields.push(quote!(pub #variant_name: Option<#ty>));
            }
            Fields::Unnamed(fields) => {
                let types =
                    fields.unnamed.iter().map(field_proxy_ty).collect::<Result<Vec<_>>>()?;
                enum_variants.push(quote!(#variant_name(#(#types),*)));
                flat_fields.push(quote!(pub #variant_name: Option<(#(#types),*)>));
            }
            Fields::Named(fields) => {
                let nested = format_ident!("__TelemetrySchemaProxyFor{}_{}", name, variant_name);
                let fields = fields
                    .named
                    .iter()
                    .map(|field| {
                        let name = &field.ident;
                        let ty = field_proxy_ty(field)?;
                        Ok(quote!(pub #name: #ty))
                    })
                    .collect::<Result<Vec<_>>>()?;
                enum_variants.push(quote!(#variant_name(#nested)));
                flat_fields.push(quote!(pub #variant_name: Option<#nested>));
                nested_defs.push(quote!(
                    #[doc(hidden)]
                    #[allow(non_camel_case_types, clippy::pub_underscore_fields)]
                    #[derive(::serde::Deserialize)]
                    pub struct #nested { #(#fields,)* }
                ));
            }
        }
    }
    Ok(quote! {
        #(#nested_defs)*
        #[doc(hidden)] #[allow(non_camel_case_types)] #[derive(::serde::Deserialize)] pub enum #proxy { #(#enum_variants,)* }
        #[doc(hidden)]
        #[allow(non_camel_case_types, non_snake_case, clippy::pub_underscore_fields)]
        #[derive(::serde::Deserialize)]
        pub struct #flattened { #(#flat_fields,)* }
        impl ::flux_versioned_types::TelemetrySchema for #name { type Proxy = #proxy; type FlattenedProxy = #flattened; }
    })
}

fn field_proxy_ty(field: &syn::Field) -> Result<TokenStream2> {
    if let Some(proxy) = explicit_proxy(&field.attrs)? {
        return Ok(quote!(#proxy));
    }
    reject_serde_changes(&field.attrs, false, false)?;
    let ty = &field.ty;
    Ok(quote!(<#ty as ::flux_versioned_types::TelemetrySchema>::Proxy))
}

fn field_flattened_proxy_ty(field: &syn::Field) -> Result<TokenStream2> {
    if let Some(proxy) = explicit_proxy(&field.attrs)? {
        return Ok(quote!(#proxy));
    }
    reject_serde_changes(&field.attrs, false, false)?;
    let ty = &field.ty;
    Ok(quote!(<#ty as ::flux_versioned_types::TelemetrySchema>::FlattenedProxy))
}

fn explicit_proxy(attrs: &[Attribute]) -> Result<Option<Type>> {
    let mut proxy = None;
    for attr in attrs {
        if !attr.path().is_ident("telemetry_schema") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if !meta.path.is_ident("proxy") {
                return Err(meta.error("expected telemetry_schema(proxy = Type)"));
            }
            if proxy.is_some() {
                return Err(meta.error("duplicate telemetry_schema proxy"));
            }
            proxy = Some(meta.value()?.parse()?);
            Ok(())
        })?;
    }
    Ok(proxy)
}

fn serde_transparent(attrs: &[Attribute]) -> Result<bool> {
    let mut transparent = false;
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            consume_meta_value(&meta)?;
            if meta.path.is_ident("transparent") {
                transparent = true;
            }
            Ok(())
        })?;
    }
    Ok(transparent)
}

fn reject_serde_changes(
    attrs: &[Attribute],
    has_proxy: bool,
    allow_transparent: bool,
) -> Result<()> {
    if has_proxy {
        return Ok(());
    }
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            let neutral = meta.path.is_ident("default") ||
                meta.path.is_ident("alias") ||
                meta.path.is_ident("other") ||
                (allow_transparent && meta.path.is_ident("transparent"));
            consume_meta_value(&meta)?;
            if neutral {
                Ok(())
            } else {
                Err(meta.error(
                    "serialization-changing serde attributes require #[telemetry_schema(proxy = Type)]",
                ))
            }
        })?;
    }
    Ok(())
}

fn consume_meta_value(meta: &syn::meta::ParseNestedMeta<'_>) -> Result<()> {
    if meta.input.peek(Token![=]) {
        let _: Expr = meta.value()?.parse()?;
    }
    Ok(())
}
