use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
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

/// A field or variant of a version, as listed by `#[export_versions]`.
pub(crate) trait Member {
    /// Key of the member list in the exported macro.
    const LIST: &'static str;
    fn member_name(&self) -> &Ident;
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

fn is_export_versions(attr: &Attribute) -> bool {
    attr.path().is_ident("export_versions")
}

// `#[export_versions]` on the newest version, usually through `final_attrs`,
// asks for the exported version list; removed before the item is emitted.
fn take_export_versions(attrs: &mut Vec<Attribute>) -> Option<Attribute> {
    let first = attrs.iter().find(|attr| is_export_versions(attr)).cloned();
    attrs.retain(|attr| !is_export_versions(attr));
    first
}

/// `__versions_<wire name>!(path::to::callback, args..)` expands to
/// `callback! { args.. ; name = "..", latest = T, versions = [V1, ..], fields =
/// [..] }`, `variants` in place of `fields` for an enum. Versions run oldest
/// first and are bare names, so they resolve where the callback expands;
/// members are the newest version's, in declaration order.
fn generate_version_list<Item: Member>(
    wire_name: &str,
    versions: &[Ident],
    members: &[Item],
) -> TokenStream2 {
    let suffix: String =
        wire_name.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '_' }).collect();
    let macro_name = format_ident!("__versions_{suffix}");
    let latest = versions.last().expect("a chain has a version");
    let list = format_ident!("{}", Item::LIST);
    let names = members.iter().map(Member::member_name);
    quote! {
        #[doc(hidden)]
        #[macro_export]
        macro_rules! #macro_name {
            ($($callback:ident)::+ $(, $($args:tt)*)?) => {
                $($callback)::+! {
                    $($($args)*)? ;
                    name = #wire_name,
                    latest = #latest,
                    versions = [#(#versions),*],
                    #list = [#(#names),*]
                }
            };
        }
    }
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

pub(crate) fn generate_evolving<B: Named, E: Named, Item: Member>(
    input: &mut EvolveInputGeneric<B, E>,
    generate_base: impl FnOnce(&EvolveInputGeneric<B, E>, bool) -> (TokenStream2, Vec<Item>),
    generate_step: impl Fn(&E, &[Attribute], &[Item], &Ident, bool, bool) -> (TokenStream2, Vec<Item>),
) -> TokenStream2 {
    if input.wire_skip && input.wire_name.is_some() {
        let name = input.wire_name.as_ref().expect("checked above");
        return syn::Error::new(name.span(), "wire_skip and wire_name are mutually exclusive")
            .to_compile_error();
    }
    let newest: &mut dyn Named = match input.evolutions.last_mut() {
        Some(last) => last,
        None => &mut input.base,
    };
    newest.attrs_mut().extend(input.final_attrs.clone());
    let export_versions = take_export_versions(newest.attrs_mut());
    if let Some(attr) = &export_versions {
        let missing = if input.roll_into.is_none() {
            Some("export_versions needs roll_into")
        } else if input.wire_skip {
            Some("export_versions needs a wire name, which wire_skip removes")
        } else {
            None
        };
        if let Some(message) = missing {
            return syn::Error::new_spanned(attr, message).to_compile_error();
        }
    }
    let older = std::iter::once(&input.base as &dyn Named)
        .chain(input.evolutions.iter().map(|version| version as &dyn Named))
        .take(input.evolutions.len());
    for version in older {
        if let Some(attr) = version.attrs().iter().find(|attr| is_export_versions(attr)) {
            return syn::Error::new_spanned(attr, "export_versions goes on the newest version")
                .to_compile_error();
        }
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
        if export_versions.is_some() {
            let wire_name =
                input.wire_name.as_ref().map_or_else(|| roll_name.to_string(), syn::LitStr::value);
            output.extend(generate_version_list(&wire_name, &version_names, &current));
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
