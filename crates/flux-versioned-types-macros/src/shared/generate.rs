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

pub(crate) fn generate_evolving<B: Named, E: Named, Item>(
    input: &mut EvolveInputGeneric<B, E>,
    generate_base: impl FnOnce(&EvolveInputGeneric<B, E>) -> (TokenStream2, Vec<Item>),
    generate_step: impl Fn(&E, &[Attribute], &[Item], &Ident) -> (TokenStream2, Vec<Item>),
    require_type_hash_locks: bool,
) -> TokenStream2 {
    if input.evolutions.is_empty() {
        input.base.attrs_mut().extend(input.final_attrs.clone());
    } else if let Some(last) = input.evolutions.last_mut() {
        last.attrs_mut().extend(input.final_attrs.clone());
    }

    if require_type_hash_locks {
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
        if !errors.is_empty() {
            return errors;
        }
    }

    let (base_output, mut current) = generate_base(input);
    let mut output = base_output;
    let mut prev_name = input.base.name().clone();

    for evolution in &input.evolutions {
        let (ev_output, new_items) =
            generate_step(evolution, &input.default_attrs, &current, &prev_name);
        output.extend(ev_output);
        current = new_items;
        prev_name = evolution.name().clone();
    }

    if let Some(ref roll_name) = input.roll_into {
        let mut version_names = vec![input.base.name().clone()];
        for ev in &input.evolutions {
            version_names.push(ev.name().clone());
        }
        output.extend(crate::rolling::generate::generate_roll_chain(roll_name, &version_names));
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
