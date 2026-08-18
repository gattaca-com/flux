use syn::{
    Attribute, Ident, Result, Token, braced,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

/// Generic macro input: optional `roll_into Name`, optional
/// `default_attrs { ... }` block + base definition + evolution steps.
pub(crate) struct EvolveInputGeneric<B, E> {
    pub roll_into: Option<Ident>,
    pub default_attrs: Vec<Attribute>,
    pub final_attrs: Vec<Attribute>,
    pub base: B,
    pub evolutions: Vec<E>,
}

impl<B: Parse, E: Parse> Parse for EvolveInputGeneric<B, E> {
    fn parse(input: ParseStream) -> Result<Self> {
        let roll_into = parse_optional_keyword(input, "roll_into")?;

        let mut default_attrs = Vec::new();
        let mut final_attrs = Vec::new();
        while input.peek(Ident) {
            let fork = input.fork();
            let ident: Ident = fork.parse()?;
            let attrs = match ident.to_string().as_str() {
                "default_attrs" => &mut default_attrs,
                "final_attrs" => &mut final_attrs,
                _ => break,
            };
            input.parse::<Ident>()?;
            let content;
            braced!(content in input);
            attrs.extend(content.call(Attribute::parse_outer)?);
        }

        let base: B = input.parse()?;
        let mut evolutions = Vec::new();
        while !input.is_empty() {
            evolutions.push(input.parse()?);
        }

        Ok(Self {
            roll_into,
            default_attrs,
            final_attrs,
            base,
            evolutions,
        })
    }
}

impl<B, E> EvolveInputGeneric<B, E> {
    pub fn ensure_default_attrs(&mut self, defaults: fn() -> Vec<Attribute>) {
        if self.default_attrs.is_empty() && self.roll_into.is_some() {
            self.default_attrs = defaults();
        }
    }
}

fn parse_optional_keyword(input: ParseStream, keyword: &str) -> Result<Option<Ident>> {
    if input.peek(Ident) {
        let fork = input.fork();
        let ident: Ident = fork.parse()?;
        if ident == keyword {
            input.parse::<Ident>()?;
            Ok(Some(input.parse()?))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

/// Generic base block: outer attrs + optional `pub` + name + braced items.
pub(crate) struct BaseBlock<Item> {
    pub attrs: Vec<Attribute>,
    pub name: Ident,
    pub items: Vec<Item>,
}

impl<Item: Parse> Parse for BaseBlock<Item> {
    fn parse(input: ParseStream) -> Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        input.parse::<Token![pub]>().ok();
        let name: Ident = input.parse()?;
        let content;
        braced!(content in input);
        let items: Punctuated<Item, Token![,]> =
            content.parse_terminated(Item::parse, Token![,])?;
        Ok(Self { attrs, name, items: items.into_iter().collect() })
    }
}

/// Implemented by operation enums to plug into the generic `EvolveBlock`
/// dispatcher.
pub(crate) trait ParseEvolveOp: Sized {
    fn parse_add(content: ParseStream) -> Result<Self>;

    /// Override to customise `remove` parsing (e.g. richer enum syntax).
    /// Default: parses a comma-separated list of idents and delegates to
    /// `from_remove`.
    fn parse_remove(content: ParseStream) -> Result<Self> {
        let names: Punctuated<Ident, Token![,]> =
            content.parse_terminated(Ident::parse, Token![,])?;
        Ok(Self::from_remove(names.into_iter().collect()))
    }

    /// Called by the default `parse_remove`. Types that override `parse_remove`
    /// directly do not need to implement this.
    fn from_remove(_names: Vec<Ident>) -> Self {
        unreachable!("from_remove called but parse_remove was not overridden")
    }

    fn parse_modify(content: ParseStream) -> Result<Self>;
}

/// Generic evolution block: outer attrs + optional `pub` + name + braced ops.
pub(crate) struct EvolveBlock<Op> {
    pub attrs: Vec<Attribute>,
    pub name: Ident,
    pub ops: Vec<Op>,
}

impl<Op: ParseEvolveOp> Parse for EvolveBlock<Op> {
    fn parse(input: ParseStream) -> Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        input.parse::<Token![pub]>().ok();
        let name: Ident = input.parse()?;

        let content;
        braced!(content in input);
        let mut ops = Vec::new();

        while !content.is_empty() {
            let keyword: Ident = content.parse()?;
            let inner;
            braced!(inner in content);
            let op = match keyword.to_string().as_str() {
                "add" => Op::parse_add(&inner)?,
                "remove" => Op::parse_remove(&inner)?,
                "modify" => Op::parse_modify(&inner)?,
                other => {
                    return Err(syn::Error::new(
                        keyword.span(),
                        format!("expected 'add', 'remove', or 'modify', found '{other}'"),
                    ));
                }
            };
            ops.push(op);
        }

        Ok(Self { attrs, name, ops })
    }
}
