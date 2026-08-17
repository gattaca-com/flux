use syn::{
    Attribute, Expr, Ident, Result, Token, Type, parenthesized,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

use crate::shared::{EvolveBlock, ParseEvolveOp};

/// A `remove` entry with optional bindings and fallback expression.
///
/// Simple: `OldVariant` → maps to `Default::default()`
/// With fallback: `OldVariant(v) = NewEnum::NewVariant(v)`
pub(crate) struct RemoveVariant {
    pub name: Ident,
    /// Variable names bound from the old variant's tuple fields.
    pub bindings: Vec<Ident>,
    /// `None` → `Default::default()`. `Some(expr)` may reference the bindings.
    pub fallback: Option<Expr>,
}

impl Parse for RemoveVariant {
    fn parse(input: ParseStream) -> Result<Self> {
        let name: Ident = input.parse()?;

        let bindings = if input.peek(syn::token::Paren) {
            let content;
            parenthesized!(content in input);
            let idents: Punctuated<Ident, Token![,]> =
                content.parse_terminated(Ident::parse, Token![,])?;
            idents.into_iter().collect()
        } else {
            Vec::new()
        };

        let fallback = if input.peek(Token![=]) {
            input.parse::<Token![=]>()?;
            Some(input.parse::<Expr>()?)
        } else {
            None
        };

        Ok(Self { name, bindings, fallback })
    }
}

#[derive(Clone)]
pub(crate) struct TupleField {
    pub attrs: Vec<Attribute>,
    pub ty: Type,
}

impl Parse for TupleField {
    fn parse(input: ParseStream) -> Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let ty: Type = input.parse()?;
        Ok(Self { attrs, ty })
    }
}

/// Complete enum variant definition — used in `base` and `add` blocks.
#[derive(Clone)]
pub(crate) struct EnumVariant {
    pub attrs: Vec<Attribute>,
    pub name: Ident,
    /// Empty means unit variant.
    pub fields: Vec<TupleField>,
    /// Explicit discriminant value, e.g. `= 5` in `Titan = 5`.
    pub discriminant: Option<Expr>,
    /// Maps this added variant to a removed variant from the previous version.
    /// Syntax: `NewName(u32) from OldName`
    pub from_variant: Option<Ident>,
    /// Converter when `from_variant` is set and types differ.
    /// Syntax: `NewName(u64) from OldName = |v: u32| v as u64`
    pub from_converter: Option<Expr>,
}

impl Parse for EnumVariant {
    fn parse(input: ParseStream) -> Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let name: Ident = input.parse()?;

        let fields = if input.peek(syn::token::Paren) {
            let content;
            parenthesized!(content in input);
            let types: Punctuated<TupleField, Token![,]> =
                content.parse_terminated(TupleField::parse, Token![,])?;
            types.into_iter().collect()
        } else {
            Vec::new()
        };

        // Discriminant first: `= 4` — syn stops at non-expression tokens like `from`.
        let discriminant = if input.peek(Token![=]) {
            input.parse::<Token![=]>()?;
            Some(input.parse::<Expr>()?)
        } else {
            None
        };

        // `from OldVariant` or `from OldVariant = converter`
        let (from_variant, from_converter) = if input.peek(Ident) {
            let fork = input.fork();
            let kw: Ident = fork.parse()?;
            if kw == "from" {
                input.parse::<Ident>()?; // consume "from"
                let old: Ident = input.parse()?;
                let conv = if input.peek(Token![=]) {
                    input.parse::<Token![=]>()?;
                    Some(input.parse::<Expr>()?)
                } else {
                    None
                };
                (Some(old), conv)
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        Ok(Self { attrs, name, fields, discriminant, from_variant, from_converter })
    }
}

impl EnumVariant {
    pub fn is_unit(&self) -> bool {
        self.fields.is_empty()
    }
}

/// `modify` entry: `VariantName(NewType1, NewType2) = converter_expr`
///
/// For a single-field variant the converter is `|v: OldType| new_value`.
/// For N fields the converter takes and returns a tuple:
/// `|(a, b): (OldT1, OldT2)| (new_a, new_b)`.
/// Unit variants use no converter (omit `= ...`).
pub(crate) struct ModifyVariant {
    pub attrs: Vec<Attribute>,
    pub name: Ident,
    pub new_fields: Vec<TupleField>,
    /// None only for unit variants (no payload to convert).
    pub converter: Option<Expr>,
}

impl Parse for ModifyVariant {
    fn parse(input: ParseStream) -> Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let name: Ident = input.parse()?;

        let new_fields = if input.peek(syn::token::Paren) {
            let content;
            parenthesized!(content in input);
            let types: Punctuated<TupleField, Token![,]> =
                content.parse_terminated(TupleField::parse, Token![,])?;
            types.into_iter().collect()
        } else {
            Vec::new()
        };

        let converter = if input.peek(Token![=]) {
            input.parse::<Token![=]>()?;
            Some(input.parse::<Expr>()?)
        } else {
            None
        };

        Ok(Self { attrs, name, new_fields, converter })
    }
}

pub(crate) enum EvolveEnumOp {
    Add(Vec<EnumVariant>),
    Remove(Vec<RemoveVariant>),
    Modify(Vec<ModifyVariant>),
}

impl ParseEvolveOp for EvolveEnumOp {
    fn parse_add(content: ParseStream) -> Result<Self> {
        let variants: Punctuated<EnumVariant, Token![,]> =
            content.parse_terminated(EnumVariant::parse, Token![,])?;
        Ok(Self::Add(variants.into_iter().collect()))
    }

    fn parse_remove(content: ParseStream) -> Result<Self> {
        let variants: Punctuated<RemoveVariant, Token![,]> =
            content.parse_terminated(RemoveVariant::parse, Token![,])?;
        Ok(Self::Remove(variants.into_iter().collect()))
    }

    fn parse_modify(content: ParseStream) -> Result<Self> {
        let variants: Punctuated<ModifyVariant, Token![,]> =
            content.parse_terminated(ModifyVariant::parse, Token![,])?;
        Ok(Self::Modify(variants.into_iter().collect()))
    }
}

pub(crate) type BaseEnum = crate::shared::BaseBlock<EnumVariant>;
pub(crate) type EvolveEnum = EvolveBlock<EvolveEnumOp>;
pub(crate) type EvolveEnumInput = crate::shared::EvolveInputGeneric<BaseEnum, EvolveEnum>;
