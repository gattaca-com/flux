use syn::{
    Attribute, Expr, Ident, Result, Token, Type, Visibility,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

use crate::shared::{EvolveBlock, ParseEvolveOp};

pub(crate) struct StructField {
    pub attrs: Vec<Attribute>,
    pub vis: Visibility,
    pub name: Ident,
    pub ty: Type,
}

impl Parse for StructField {
    fn parse(input: ParseStream) -> Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let vis: Visibility = input.parse()?;
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let ty: Type = input.parse()?;
        Ok(Self { attrs, vis, name, ty })
    }
}

pub(crate) struct AddField {
    pub attrs: Vec<Attribute>,
    pub vis: Visibility,
    pub name: Ident,
    pub ty: Type,
    pub default: Expr,
}

impl Parse for AddField {
    fn parse(input: ParseStream) -> Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let vis: Visibility = input.parse()?;
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let ty: Type = input.parse()?;
        input.parse::<Token![=]>()?;
        let default: Expr = input.parse()?;
        Ok(Self { attrs, vis, name, ty, default })
    }
}

pub(crate) struct ModifyField {
    pub attrs: Vec<Attribute>,
    pub name: Ident,
    pub new_ty: Type,
    pub converter: Expr,
}

impl Parse for ModifyField {
    fn parse(input: ParseStream) -> Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let new_ty: Type = input.parse()?;
        input.parse::<Token![=]>()?;
        let converter: Expr = input.parse()?;
        Ok(Self { attrs, name, new_ty, converter })
    }
}

pub(crate) enum EvolutionOp {
    Add(Vec<AddField>),
    Remove(Vec<Ident>),
    Modify(Vec<ModifyField>),
}

impl ParseEvolveOp for EvolutionOp {
    fn parse_add(content: ParseStream) -> Result<Self> {
        let fields: Punctuated<AddField, Token![,]> =
            content.parse_terminated(AddField::parse, Token![,])?;
        Ok(Self::Add(fields.into_iter().collect()))
    }

    fn from_remove(names: Vec<Ident>) -> Self {
        Self::Remove(names)
    }

    fn parse_modify(content: ParseStream) -> Result<Self> {
        let fields: Punctuated<ModifyField, Token![,]> =
            content.parse_terminated(ModifyField::parse, Token![,])?;
        Ok(Self::Modify(fields.into_iter().collect()))
    }
}

pub(crate) type BaseStruct = crate::shared::BaseBlock<StructField>;
pub(crate) type EvolveStruct = EvolveBlock<EvolutionOp>;
pub(crate) type EvolveInput = crate::shared::EvolveInputGeneric<BaseStruct, EvolveStruct>;
