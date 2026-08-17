use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use rustc_hash::FxHashMap;
use syn::{Attribute, Expr, Ident, Index};

use super::parse::{
    EnumVariant, EvolveEnum, EvolveEnumInput, EvolveEnumOp, ModifyVariant, RemoveVariant,
};
use crate::shared::is_closure;

fn generate_enum_def(
    name: &Ident,
    default_attrs: &[Attribute],
    enum_attrs: &[Attribute],
    variants: &[EnumVariant],
) -> TokenStream2 {
    let variant_tokens: Vec<_> = variants
        .iter()
        .map(|v| {
            let vattrs = &v.attrs;
            let vname = &v.name;
            let disc = v.discriminant.as_ref().map(|d| quote! { = #d });
            if v.is_unit() {
                quote! { #(#vattrs)* #vname #disc }
            } else {
                let types: Vec<_> = v
                    .fields
                    .iter()
                    .map(|f| {
                        let fattrs = &f.attrs;
                        let ty = &f.ty;
                        quote! { #(#fattrs)* #ty }
                    })
                    .collect();
                quote! { #(#vattrs)* #vname(#(#types),*) #disc }
            }
        })
        .collect();

    quote! {
        #(#default_attrs)*
        #(#enum_attrs)*
        pub enum #name {
            #(#variant_tokens,)*
        }
    }
}

/// Generates a single match arm that converts a variant from `prev_name` into
/// `new_name`. Used by modify, from-rename, and passthrough paths.
fn generate_conversion_arm(
    prev_name: &Ident,
    new_name: &Ident,
    src: &Ident,
    dst: &Ident,
    src_fields: usize,
    dst_fields: usize,
    converter: Option<&Expr>,
) -> TokenStream2 {
    match (src_fields, converter) {
        (0, None) => {
            quote! { #prev_name::#src => #new_name::#dst }
        }
        (0, Some(conv)) if dst_fields == 0 => syn::Error::new_spanned(
            conv,
            "converter between unit variants has no effect — neither variant has fields",
        )
        .to_compile_error(),
        // Unit source → tuple destination: converter produces initial values.
        (0, Some(conv)) if dst_fields == 1 => {
            quote! {
                #prev_name::#src => #new_name::#dst((#conv)())
            }
        }
        (0, Some(conv)) => {
            let indices: Vec<Index> = (0..dst_fields).map(Index::from).collect();
            if is_closure(conv) {
                quote! {
                    #prev_name::#src => {
                        let __r = (#conv)();
                        #new_name::#dst(#(__r.#indices),*)
                    }
                }
            } else {
                quote! {
                    #prev_name::#src => {
                        let __r = #conv();
                        #new_name::#dst(#(__r.#indices),*)
                    }
                }
            }
        }
        (1, Some(conv)) if dst_fields == 1 => {
            quote! {
                #prev_name::#src(__f0) => #new_name::#dst((#conv)(__f0))
            }
        }
        (_, Some(conv)) => {
            let src_bindings: Vec<_> = (0..src_fields).map(|i| format_ident!("__f{}", i)).collect();
            let indices: Vec<Index> = (0..dst_fields).map(Index::from).collect();
            let call_args = if src_fields == 1 {
                quote! { __f0 }
            } else {
                quote! { (#(#src_bindings),*) }
            };

            if is_closure(conv) {
                quote! {
                    #prev_name::#src(#(#src_bindings),*) => {
                        let __r = (#conv)(#call_args);
                        #new_name::#dst(#(__r.#indices),*)
                    }
                }
            } else {
                quote! {
                    #prev_name::#src(#(#src_bindings),*) => {
                        let __r = #conv(#call_args);
                        #new_name::#dst(#(__r.#indices),*)
                    }
                }
            }
        }
        (_, None) => {
            let bindings: Vec<_> = (0..src_fields).map(|i| format_ident!("__f{}", i)).collect();
            quote! {
                #prev_name::#src(#(#bindings),*) => #new_name::#dst(#(#bindings),*)
            }
        }
    }
}

fn generate_into_impl(
    prev_name: &Ident,
    new_name: &Ident,
    prev_variants: &[EnumVariant],
    remove_map: &FxHashMap<String, &RemoveVariant>,
    modify_map: &FxHashMap<String, &ModifyVariant>,
    from_map: &FxHashMap<String, &EnumVariant>,
) -> TokenStream2 {
    let arms: Vec<_> = prev_variants
        .iter()
        .map(|v| {
            let vname = &v.name;
            let key = vname.to_string();
            let n = v.fields.len();

            from_map.get(&key).map_or_else(
                || {
                    remove_map.get(&key).map_or_else(
                        || {
                            modify_map.get(&key).map_or_else(
                                || {
                                    generate_conversion_arm(
                                        prev_name, new_name, vname, vname, n, n, None,
                                    )
                                },
                                |modify| {
                                    generate_conversion_arm(
                                        prev_name,
                                        new_name,
                                        vname,
                                        vname,
                                        n,
                                        modify.new_fields.len(),
                                        modify.converter.as_ref(),
                                    )
                                },
                            )
                        },
                        |rv| {
                            let fallback: Expr = rv.fallback.clone().unwrap_or_else(|| {
                                syn::parse_quote!(::core::default::Default::default())
                            });
                            let bindings = &rv.bindings;
                            if !bindings.is_empty() {
                                quote! { #prev_name::#vname(#(#bindings),*) => #fallback }
                            } else if v.is_unit() {
                                quote! { #prev_name::#vname => #fallback }
                            } else {
                                quote! { #prev_name::#vname(..) => #fallback }
                            }
                        },
                    )
                },
                |target| {
                    generate_conversion_arm(
                        prev_name,
                        new_name,
                        vname,
                        &target.name,
                        n,
                        target.fields.len(),
                        target.from_converter.as_ref(),
                    )
                },
            )
        })
        .collect();

    quote! {
        impl Into<#new_name> for #prev_name {
            fn into(self) -> #new_name {
                match self {
                    #(#arms,)*
                }
            }
        }
    }
}

pub(crate) fn generate_base_enum(input: &EvolveEnumInput) -> (TokenStream2, Vec<EnumVariant>) {
    let output = generate_enum_def(
        &input.base.name,
        &input.default_attrs,
        &input.base.attrs,
        &input.base.items,
    );

    (output, input.base.items.clone())
}

pub(crate) fn generate_evolution(
    evolution: &EvolveEnum,
    default_attrs: &[Attribute],
    current_variants: &[EnumVariant],
    prev_name: &Ident,
) -> (TokenStream2, Vec<EnumVariant>) {
    let mut add_variants = Vec::new();
    let mut remove_map = FxHashMap::default();
    let mut modify_map = FxHashMap::default();

    for op in &evolution.ops {
        match op {
            EvolveEnumOp::Add(variants) => add_variants.extend(variants),
            EvolveEnumOp::Remove(variants) => {
                for rv in variants {
                    remove_map.insert(rv.name.to_string(), rv);
                }
            }
            EvolveEnumOp::Modify(variants) => {
                for v in variants {
                    modify_map.insert(v.name.to_string(), v);
                }
            }
        }
    }

    // Build from_map: old variant name → new add variant that replaces it.
    let from_map: FxHashMap<String, &EnumVariant> = add_variants
        .iter()
        .filter_map(|v| v.from_variant.as_ref().map(|old| (old.to_string(), *v)))
        .collect();

    // Build the new variant list: kept (possibly modified) + added.
    let mut new_variants: Vec<EnumVariant> = current_variants
        .iter()
        .filter(|v| !remove_map.contains_key(&v.name.to_string()))
        .map(|v| {
            modify_map.get(&v.name.to_string()).map_or_else(
                || v.clone(),
                |m| EnumVariant {
                    attrs: if m.attrs.is_empty() { v.attrs.clone() } else { m.attrs.clone() },
                    name: v.name.clone(),
                    fields: m.new_fields.clone(),
                    discriminant: v.discriminant.clone(),
                    from_variant: None,
                    from_converter: None,
                },
            )
        })
        .collect();

    for av in add_variants {
        new_variants.push(av.clone());
    }

    let enum_def =
        generate_enum_def(&evolution.name, default_attrs, &evolution.attrs, &new_variants);
    let into_impl = generate_into_impl(
        prev_name,
        &evolution.name,
        current_variants,
        &remove_map,
        &modify_map,
        &from_map,
    );

    let mut output = enum_def;
    output.extend(into_impl);

    (output, new_variants)
}
