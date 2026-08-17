use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use rustc_hash::FxHashMap;
use syn::{Attribute, Ident, Type, Visibility};

use super::parse::{AddField, EvolutionOp, EvolveInput, EvolveStruct, ModifyField, StructField};
use crate::shared::is_closure;

pub(crate) struct FieldInfo {
    pub attrs: Vec<Attribute>,
    pub vis: Visibility,
    pub name: Ident,
    pub ty: Type,
}

impl FieldInfo {
    pub fn from_struct_field(f: &StructField) -> Self {
        Self { attrs: f.attrs.clone(), vis: f.vis.clone(), name: f.name.clone(), ty: f.ty.clone() }
    }
}

pub(crate) struct EvolutionContext<'a> {
    pub add_fields: Vec<&'a AddField>,
    pub remove_names: Vec<String>,
    pub modify_map: FxHashMap<String, &'a ModifyField>,
}

impl<'a> EvolutionContext<'a> {
    pub fn from_evolution(evolution: &'a EvolveStruct) -> Self {
        let mut ctx = Self {
            add_fields: Vec::new(),
            remove_names: Vec::new(),
            modify_map: FxHashMap::default(),
        };

        for op in &evolution.ops {
            match op {
                EvolutionOp::Add(fields) => {
                    for f in fields {
                        ctx.add_fields.push(f);
                    }
                }
                EvolutionOp::Remove(names) => {
                    for name in names {
                        ctx.remove_names.push(name.to_string());
                    }
                }
                EvolutionOp::Modify(fields) => {
                    for f in fields {
                        ctx.modify_map.insert(f.name.to_string(), f);
                    }
                }
            }
        }

        ctx
    }
}

pub(crate) fn generate_struct_def(
    name: &Ident,
    default_attrs: &[Attribute],
    struct_attrs: &[Attribute],
    fields: &Vec<TokenStream2>,
) -> TokenStream2 {
    quote! {
        #(#default_attrs)*
        #(#struct_attrs)*
        pub struct #name {
            #(#fields),*
        }
    }
}

pub(crate) fn generate_base_struct(input: &EvolveInput) -> (TokenStream2, Vec<FieldInfo>) {
    let fields: Vec<_> = input
        .base
        .items
        .iter()
        .map(|f| {
            let attrs = &f.attrs;
            let vis = &f.vis;
            let name = &f.name;
            let ty = &f.ty;
            quote! { #(#attrs)* #vis #name: #ty }
        })
        .collect();

    let output =
        generate_struct_def(&input.base.name, &input.default_attrs, &input.base.attrs, &fields);

    let field_infos = input.base.items.iter().map(FieldInfo::from_struct_field).collect();

    (output, field_infos)
}

fn generate_evolved_struct_fields(
    kept_fields: &[&FieldInfo],
    ctx: &EvolutionContext,
) -> Vec<TokenStream2> {
    kept_fields
        .iter()
        .map(|f| {
            let name = &f.name;
            let name_str = name.to_string();

            ctx.modify_map.get(&name_str).map_or_else(
                || {
                    let attrs = &f.attrs;
                    let vis = &f.vis;
                    let ty = &f.ty;
                    quote! { #(#attrs)* #vis #name: #ty }
                },
                |modify| {
                    let attrs = &modify.attrs;
                    let vis = &f.vis;
                    let ty = &modify.new_ty;
                    quote! { #(#attrs)* #vis #name: #ty }
                },
            )
        })
        .chain(ctx.add_fields.iter().map(|f| {
            let attrs = &f.attrs;
            let vis = &f.vis;
            let name = &f.name;
            let ty = &f.ty;
            quote! { #(#attrs)* #vis #name: #ty }
        }))
        .collect()
}

fn generate_into_impl(
    prev_name: &Ident,
    ev_name: &Ident,
    kept_fields: &[&FieldInfo],
    ctx: &EvolutionContext,
) -> TokenStream2 {
    let kept_assignments: Vec<_> = kept_fields
        .iter()
        .map(|f| {
            let name = &f.name;
            let name_str = name.to_string();

            ctx.modify_map.get(&name_str).map_or_else(
                || quote! { #name: self.#name },
                |modify| {
                    let converter = &modify.converter;
                    quote! { #name: (#converter)(self.#name) }
                },
            )
        })
        .collect();

    let mut closure_bindings = Vec::new();
    let mut add_assignments = Vec::new();

    for f in &ctx.add_fields {
        let name = &f.name;
        let default = &f.default;
        if is_closure(default) {
            let binding_name = format_ident!("__evolve_add_{}", name);
            closure_bindings.push(quote! {
                let #binding_name = (#default)(&self);
            });
            add_assignments.push(quote! { #name: #binding_name });
        } else {
            add_assignments.push(quote! { #name: #default });
        }
    }

    quote! {
        impl Into<#ev_name> for #prev_name {
            fn into(self) -> #ev_name {
                #(#closure_bindings)*
                #ev_name {
                    #(#kept_assignments,)*
                    #(#add_assignments,)*
                }
            }
        }
    }
}

fn update_fields_after_evolution(
    kept_fields: &[&FieldInfo],
    ctx: &EvolutionContext,
) -> Vec<FieldInfo> {
    kept_fields
        .iter()
        .map(|f| {
            let name_str = f.name.to_string();
            ctx.modify_map.get(&name_str).map_or_else(
                || FieldInfo {
                    attrs: f.attrs.clone(),
                    vis: f.vis.clone(),
                    name: f.name.clone(),
                    ty: f.ty.clone(),
                },
                |modify| FieldInfo {
                    attrs: if modify.attrs.is_empty() {
                        f.attrs.clone()
                    } else {
                        modify.attrs.clone()
                    },
                    vis: f.vis.clone(),
                    name: f.name.clone(),
                    ty: modify.new_ty.clone(),
                },
            )
        })
        .chain(ctx.add_fields.iter().map(|f| FieldInfo {
            attrs: f.attrs.clone(),
            vis: f.vis.clone(),
            name: f.name.clone(),
            ty: f.ty.clone(),
        }))
        .collect()
}

pub(crate) fn generate_evolution(
    evolution: &EvolveStruct,
    default_attrs: &[Attribute],
    current_fields: &[FieldInfo],
    prev_name: &Ident,
) -> (TokenStream2, Vec<FieldInfo>) {
    let ctx = EvolutionContext::from_evolution(evolution);

    let kept_fields: Vec<_> =
        current_fields.iter().filter(|f| !ctx.remove_names.contains(&f.name.to_string())).collect();

    let struct_fields = generate_evolved_struct_fields(&kept_fields, &ctx);
    let struct_def =
        generate_struct_def(&evolution.name, default_attrs, &evolution.attrs, &struct_fields);
    let into_impl = generate_into_impl(prev_name, &evolution.name, &kept_fields, &ctx);

    let mut output = struct_def;
    output.extend(into_impl);

    let new_fields = update_fields_after_evolution(&kept_fields, &ctx);

    (output, new_fields)
}
