use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Attribute, Expr, Ident, Result,
    parse::{Parse, ParseStream},
};

use super::parse::{BaseBlock, EvolveBlock, EvolveInputGeneric};

pub(crate) fn is_closure(expr: &Expr) -> bool {
    matches!(expr, Expr::Closure(_))
}

pub(crate) trait Named {
    fn name(&self) -> &Ident;
    fn attrs(&self) -> &[Attribute];
    fn attrs_mut(&mut self) -> &mut Vec<Attribute>;
}

impl<T> Named for BaseBlock<T> {
    fn name(&self) -> &Ident {
        &self.name
    }

    fn attrs(&self) -> &[Attribute] {
        &self.attrs
    }

    fn attrs_mut(&mut self) -> &mut Vec<Attribute> {
        &mut self.attrs
    }
}

impl<Op> Named for EvolveBlock<Op> {
    fn name(&self) -> &Ident {
        &self.name
    }

    fn attrs(&self) -> &[Attribute] {
        &self.attrs
    }

    fn attrs_mut(&mut self) -> &mut Vec<Attribute> {
        &mut self.attrs
    }
}

/// Schema-only attributes on fields and variants describe the final,
/// queryable shape, so they are stripped from every older version's
/// expansion: only the version carrying the schema derive may name them.
/// Version-level attributes are left alone, so an explicit whole-type proxy
/// keeps working on any version.
pub(crate) fn without_schema_attrs(attrs: &[Attribute]) -> Vec<Attribute> {
    attrs.iter().filter(|attr| !attr.path().is_ident("telemetry_schema")).cloned().collect()
}

// `#[wire_skip]` on a version block opts that version out of the byte-stable
// wire; stripped before the version's item is emitted.
pub(crate) fn version_wire_skip(attrs: &[Attribute]) -> Option<&Attribute> {
    attrs.iter().find(|attr| attr.path().is_ident("wire_skip"))
}

pub(crate) fn strip_wire_skip(attrs: &[Attribute]) -> Vec<Attribute> {
    attrs.iter().filter(|attr| !attr.path().is_ident("wire_skip")).cloned().collect()
}

// Reads per-version `#[wire_skip]` markers, reporting redundant markers and
// a skipped newest version. Returns base + evolution markers in order.
fn version_skips<B: Named, E: Named>(
    input: &EvolveInputGeneric<B, E>,
    errors: &mut TokenStream2,
) -> (bool, Vec<bool>) {
    let base = version_wire_skip(input.base.attrs()).is_some();
    let evos: Vec<bool> =
        input.evolutions.iter().map(|ev| version_wire_skip(ev.attrs()).is_some()).collect();
    if input.wire_skip {
        if let Some(attr) = version_wire_skip(input.base.attrs()) {
            errors.extend(
                syn::Error::new_spanned(attr, "chain is already wire_skip").to_compile_error(),
            );
        }
        for ev in &input.evolutions {
            if let Some(attr) = version_wire_skip(ev.attrs()) {
                errors.extend(
                    syn::Error::new_spanned(attr, "chain is already wire_skip").to_compile_error(),
                );
            }
        }
    } else {
        let newest = input.evolutions.last().map_or_else(|| input.base.attrs(), |ev| ev.attrs());
        if let Some(attr) = newest.iter().find(|attr| attr.path().is_ident("wire_skip")) {
            errors.extend(
                syn::Error::new_spanned(
                    attr,
                    "the newest version cannot be wire_skip; use a chain-level #[wire_skip]",
                )
                .to_compile_error(),
            );
        }
    }
    (base, evos)
}

pub(crate) fn generate_evolving<B: Named, E: Named, Item>(
    input: &mut EvolveInputGeneric<B, E>,
    generate_base: impl FnOnce(&EvolveInputGeneric<B, E>, bool) -> (TokenStream2, Vec<Item>),
    generate_step: impl Fn(&E, &[Attribute], &[Item], &Ident, bool, bool) -> (TokenStream2, Vec<Item>),
) -> TokenStream2 {
    if input.wire_skip && input.wire_name.is_some() {
        let name = input.wire_name.as_ref().expect("checked above");
        return syn::Error::new(name.span(), "wire_skip and wire_name are mutually exclusive")
            .to_compile_error();
    }
    if input.evolutions.is_empty() {
        input.base.attrs_mut().extend(input.final_attrs.clone());
    } else if let Some(last) = input.evolutions.last_mut() {
        last.attrs_mut().extend(input.final_attrs.clone());
    }

    let versions = std::iter::once(&input.base as &dyn Named)
        .chain(input.evolutions.iter().map(|version| version as &dyn Named));
    let mut errors = TokenStream2::new();
    for version in versions {
        let has_lock = version.attrs().iter().any(|attr| {
            attr.path().segments.last().is_some_and(|segment| segment.ident == "type_hash_lock")
        });
        if !has_lock {
            let name = version.name();
            errors.extend(
                syn::Error::new(
                    name.span(),
                    format!(
                        "{name} is missing a type hash lock; add #[type_hash_lock(hash = 0)], compile once, then replace 0 with the computed hash shown in the TypeHashLock diagnostic"
                    ),
                )
                .to_compile_error(),
            );
        }
    }
    let (base_skipped, evo_skipped) = version_skips(input, &mut errors);
    if !errors.is_empty() {
        return errors;
    }
    // Not a real Rust attribute: strip before emitting version items.
    let kept = strip_wire_skip(input.base.attrs());
    *input.base.attrs_mut() = kept;
    for ev in &mut input.evolutions {
        let kept = strip_wire_skip(ev.attrs());
        *ev.attrs_mut() = kept;
    }

    let (base_output, mut current) = generate_base(input, input.wire_skip || base_skipped);
    let mut output = base_output;
    let mut prev_name = input.base.name().clone();

    for (index, evolution) in input.evolutions.iter().enumerate() {
        let is_final = index + 1 == input.evolutions.len();
        let (ev_output, new_items) = generate_step(
            evolution,
            &input.default_attrs,
            &current,
            &prev_name,
            is_final,
            input.wire_skip || evo_skipped[index],
        );
        output.extend(ev_output);
        current = new_items;
        prev_name = evolution.name().clone();
    }

    if let Some(ref roll_name) = input.roll_into {
        let mut version_names = vec![input.base.name().clone()];
        for ev in &input.evolutions {
            version_names.push(ev.name().clone());
        }
        let mut skipped = vec![input.wire_skip || base_skipped];
        skipped.extend(evo_skipped.iter().map(|skipped| input.wire_skip || *skipped));
        output.extend(crate::rolling::generate::generate_roll_chain(
            roll_name,
            &version_names,
            input.wire_name.as_ref(),
            input.wire_skip,
            &skipped,
        ));
    }

    output
}

// -- Default attrs helpers --------------------------------------------------

struct AttrsWrapper(Vec<Attribute>);

impl Parse for AttrsWrapper {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(Self(input.call(Attribute::parse_outer)?))
    }
}

fn default_attrs_with_repr(repr: &TokenStream2) -> Vec<Attribute> {
    let tokens = quote! {
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize, flux::type_hash_derive::TypeHash)]
        #[type_hash(skip_typename_on_derive)]
        #[repr(#repr)]
    };
    syn::parse2::<AttrsWrapper>(tokens).unwrap().0
}

pub(crate) fn default_struct_attrs() -> Vec<Attribute> {
    default_attrs_with_repr(&quote!(C))
}

pub(crate) fn default_enum_attrs() -> Vec<Attribute> {
    default_attrs_with_repr(&quote!(u8))
}

pub(crate) fn byte_stable_derive_attrs() -> Vec<Attribute> {
    let tokens = quote! {
        #[derive(::flux_versioned_types::ByteStable)]
        #[byte_stable(crate = "::flux_versioned_types::byte_stable")]
    };
    syn::parse2::<AttrsWrapper>(tokens).unwrap().0
}
