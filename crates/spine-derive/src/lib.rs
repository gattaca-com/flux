// Make the following work:
// #[from_spine]
// #[derive(Clone, Debug)]
// pub struct Spine {
//     persistence_dir: PathBuf,
//     #[queue(persist, size=2usize.pow(15))]
//     pub update_pool: Queue<messages::UpdatePool>,
//     #[queue(persist)]
//     pub arb_path:    Queue<messages::ArbPath>,
// }

// spine_derive/src/lib.rs   (only the changed parts are shown)
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Attribute, Expr, GenericArgument, Ident, ItemStruct, LitStr, Path, PathArguments, Result,
    Token, Type, parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    token::Comma,
};

enum FromSpineArg {
    Str(LitStr),
    Path(Path),
    Named(LitStr),
}

impl Parse for FromSpineArg {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        if input.is_empty() {
            return Err(syn::Error::new(input.span(), r#"expected `from_spine("...")`"#));
        }

        // #[from_spine("app")]
        if input.peek(LitStr) {
            let s: LitStr = input.parse()?;
            return Ok(FromSpineArg::Str(s));
        }

        // #[from_spine(name = "app")]
        if input.peek(Ident) {
            let ident: Ident = input.parse()?;
            if input.peek(Token![=]) {
                input.parse::<Token![=]>()?;
                let s: LitStr = input.parse()?;
                if ident == "name" {
                    return Ok(FromSpineArg::Named(s));
                } else {
                    return Err(syn::Error::new_spanned(ident, r#"expected `name = "..."`"#));
                }
            } else {
                // #[from_spine(path_like)]
                // We already consumed one ident; parse the full path starting from it.
                // Reconstruct a Path by parsing the rest (optional).
                // Easiest: put ident back by building a Path from scratch.
                let mut segments = syn::punctuated::Punctuated::new();
                segments.push(syn::PathSegment::from(ident));
                // Allow `::more::segments`
                while input.peek(Token![::]) {
                    input.parse::<Token![::]>()?;
                    let seg: Ident = input.parse()?;
                    segments.push(syn::PathSegment::from(seg));
                }
                let p = Path { leading_colon: None, segments };
                return Ok(FromSpineArg::Path(p));
            }
        }

        // Fallback: a full Path (e.g., starting with ::)
        let p: Path = input.parse()?;
        Ok(FromSpineArg::Path(p))
    }
}

impl FromSpineArg {
    fn as_tokens(&self) -> proc_macro2::TokenStream {
        match self {
            FromSpineArg::Str(s) => quote! { #s },
            FromSpineArg::Named(s) => quote! { #s },
            FromSpineArg::Path(p) => quote! { stringify!(#p) },
        }
    }
}

// Helper types and parser for #[queue(...)] attributes
mod kw {
    syn::custom_keyword!(persist);
    syn::custom_keyword!(size);
}

fn get_queue_config(attrs: &[Attribute]) -> (bool, Option<Expr>) {
    let mut is_persistent = false;
    let mut size_expr: Option<Expr> = None;

    for attr in attrs {
        if attr.path().is_ident("queue") {
            attr.parse_nested_meta(|meta| {
                // #[repr(C)]
                if meta.path.is_ident("persist") {
                    is_persistent = true;
                    return Ok(());
                }

                // #[repr(align(N))]
                if meta.path.is_ident("size") {
                    let content;
                    parenthesized!(content in meta.input);
                    let lit: Expr = content.parse()?;
                    // let n: usize = lit.base10_parse()?;
                    size_expr = Some(lit);
                    return Ok(());
                }
                Err(meta.error("unrecognized repr"))
            })
            .expect("couldn't parse attr");
        }
    }

    (is_persistent, size_expr)
}

#[proc_macro_attribute]
pub fn from_spine(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as FromSpineArg);
    let app_name_tokens = args.as_tokens();
    // ─── 1. parse the annotated struct ────────────────────────────────────
    let input: ItemStruct = parse_macro_input!(item);
    let struct_ident = &input.ident;
    let consumers_ident = format_ident!("{}Consumers", struct_ident);
    let producers_ident = format_ident!("{}Producers", struct_ident);

    // ─── 2. collect per-field info ────────────────────────────────────────
    let mut consumer_fields = Punctuated::<_, Comma>::new();
    let mut producer_fields = Punctuated::<_, Comma>::new();
    let mut consumer_init = Punctuated::<_, Comma>::new();
    let mut producer_init = Punctuated::<_, Comma>::new();

    let mut as_ref_impls = Vec::<proc_macro2::TokenStream>::new();
    let mut as_mut_impls = Vec::<proc_macro2::TokenStream>::new();
    let mut persisting = Vec::<proc_macro2::TokenStream>::new();
    let mut message_types = Vec::<proc_macro2::TokenStream>::new();

    let mut new_method_initializers = Punctuated::<proc_macro2::TokenStream, Comma>::new();
    for field in &input.fields {
        let field_ident = field.ident.as_ref().expect("named field required");

        // recognise Queue<T>
        if let Type::Path(tp) = &field.ty &&
            tp.path.segments.last().is_some_and(|s| s.ident == "SpineQueue") &&
            let PathArguments::AngleBracketed(args) = &tp.path.segments[0].arguments &&
            let Some(GenericArgument::Type(inner_ty)) = args.args.first()
        {
            message_types.push(quote! {
                ::flux::utils::short_typename::<#inner_ty>().to_string()
            });
            // --------------------------------- standard impls
            as_ref_impls.push(quote! {
                impl AsRef<::flux::spine::SpineProducer<#inner_ty>> for #producers_ident {
                    fn as_ref(&self)->&::flux::spine::SpineProducer<#inner_ty>{ &self.#field_ident }
                }
            });

            as_mut_impls.push(quote! {
                impl AsMut<::flux::spine::SpineConsumer<#inner_ty>> for #consumers_ident {
                    fn as_mut(&mut self)->&mut ::flux::spine::SpineConsumer<#inner_ty>{
                        &mut self.#field_ident
                    }
                }
            });

            as_mut_impls.push(quote! {
                impl AsRef<::flux::spine::SpineConsumer<#inner_ty>> for #consumers_ident {
                    fn as_ref(&self)->&::flux::spine::SpineConsumer<#inner_ty>{
                        &self.#field_ident
                    }
                }
            });

            // struct field lists
            consumer_fields
                .push(quote! { pub #field_ident : ::flux::spine::SpineConsumer<#inner_ty> });
            producer_fields
                .push(quote! { pub #field_ident : ::flux::spine::SpineProducer<#inner_ty> });
            consumer_init.push(quote! {
                            #field_ident : ::flux::spine::SpineConsumer::attach::<#struct_ident, _>(tile, spine.#field_ident)
                        });
            producer_init.push(quote! { #field_ident : ::flux::communication::queue::Producer::from(spine.#field_ident) });

            // ------------------------ NEW: only if #[persist] or needs size for
            // PersistingQueueTile
            let (is_persistent, _size_expr_opt) = get_queue_config(&field.attrs);
            if is_persistent {
                persisting.push(quote! {
                                let last_core = ::flux::core_affinity::get_core_ids().unwrap().last().unwrap().id;
                                let cfg = ::flux::tile::TileConfig::background(
                                    Some(last_core),
                                    Some(::flux::timing::Duration::from_millis(10)),
                                );
                                ::flux::tile::attach_tile(
                                    ::flux::persistence::PersistingQueueTile::<#inner_ty>::new(),
                                    &mut scoped,
                                    cfg,
                                );
                            });
            }
        }
        new_method_initializers
            .push(quote! { #field_ident : #field.ty::attach(#app_name_tokens, tile, spine) });
    }

    // ---- Prepare initializers for the `new` method ----
    let mut new_method_initializers_for_new_fn =
        Punctuated::<proc_macro2::TokenStream, Comma>::new();
    for field_for_new in &input.fields {
        let field_ident_for_new =
            field_for_new.ident.as_ref().expect("named field required for new method");
        let field_ty_for_new = &field_for_new.ty;

        if field_ident_for_new == "tile_info" {
            new_method_initializers_for_new_fn.push(quote! {
                #field_ident_for_new: ::flux::communication::ShmemData::open_or_init(
                    &format!("{}{}", #app_name_tokens, path_suffix),
                    || Default::default(),
                ).expect("couldn't open or init tile info shmem")
            });
        } else if let Type::Path(tp_for_new) = field_ty_for_new {
            if tp_for_new.path.segments.last().is_some_and(|s| s.ident == "SpineQueue") {
                let (_is_persistent, size_expr_opt_for_new) =
                    get_queue_config(&field_for_new.attrs);
                let size_arg_for_new = match size_expr_opt_for_new {
                    Some(expr_for_new) => quote! { #expr_for_new },
                    None => quote! { 2usize.pow(15) },
                };
                new_method_initializers_for_new_fn.push(quote! {
                    #field_ident_for_new: ::flux::communication::shmem_queue(
                        &format!("{}{}", #app_name_tokens, path_suffix),
                        #size_arg_for_new,
                        ::flux::communication::queue::QueueType::MPMC
                    )
                });
            } else {
                new_method_initializers_for_new_fn
                    .push(quote! { #field_ident_for_new: Default::default() });
            }
        } else {
            new_method_initializers_for_new_fn
                .push(quote! { #field_ident_for_new: Default::default() });
        }
    }

    let generated_new_method_token_stream = quote! {
        pub fn new(path_suffix: Option<&str>) -> Self {
            let path_suffix = path_suffix.unwrap_or(&"");
            Self {
                #new_method_initializers_for_new_fn
            }
        }
    };

    // Reconstruct the input struct without #[queue] attributes on its fields
    let input_attrs = &input.attrs;
    let vis = &input.vis;
    let struct_ident = &input.ident;
    let generics_decl = &input.generics;

    let reconstructed_fields = match &input.fields {
        syn::Fields::Named(fields_named) => {
            let iter = fields_named.named.iter().map(|f| {
                let attrs = f.attrs.iter().filter(|attr| !attr.path().is_ident("queue"));
                let vis = &f.vis;
                let ident = &f.ident;
                let colon_token = &f.colon_token;
                let ty = &f.ty;
                quote! { #(#attrs)* #vis #ident #colon_token #ty }
            });
            quote! { { #(#iter),* } }
        }
        syn::Fields::Unnamed(fields_unnamed) => {
            let iter = fields_unnamed.unnamed.iter().map(|f| {
                let attrs = f.attrs.iter().filter(|attr| !attr.path().is_ident("queue"));
                let vis = &f.vis;
                let ty = &f.ty;
                quote! { #(#attrs)* #vis #ty }
            });
            quote! { ( #(#iter),* ); }
        }
        syn::Fields::Unit => quote! { ; },
    };

    let reconstructed_input_struct = quote! {
        #(#input_attrs)*
        #vis struct #struct_ident #generics_decl
        #reconstructed_fields
    };

    // ─── 3. compose generated code ────────────────────────────────────────
    let expanded = quote! {
        #reconstructed_input_struct // Use the reconstructed struct instead of #input

        // generated Consumers / Producers structs
        #[derive(Clone, Copy, Debug)]
        #vis struct #consumers_ident { #consumer_fields }
        impl #consumers_ident {
            pub fn attach<Tl: ::flux::tile::Tile<#struct_ident>>(tile: &Tl, spine: &mut #struct_ident) -> Self {
                Self { #consumer_init }
            }
        }

        #[derive(Clone, Copy, Debug)]
        #vis struct #producers_ident { #producer_fields, pub timestamp: ::flux::timing::TrackingTimestamp }
        impl #producers_ident {
            pub fn attach<Tl: ::flux::tile::Tile<#struct_ident>>(tile: &Tl, spine:&mut #struct_ident)->Self {
                let id = spine.tile_info.register_tile(tile.name());
                Self { #producer_init, timestamp: ::flux::timing::TrackingTimestamp::new(id) }
            }
        }

        impl ::flux::spine::SpineProducers for #producers_ident {
            #[inline]
            fn timestamp(&self) -> &::flux::timing::TrackingTimestamp { &self.timestamp }
            #[inline]
            fn timestamp_mut(&mut self) -> &mut ::flux::timing::TrackingTimestamp { &mut self.timestamp }
        }

        // AsRef / AsMut passthroughs + legacy impls
        #(#as_ref_impls)*
        #(#as_mut_impls)*

        impl ::flux::spine::FluxSpine for #struct_ident {
            type Consumers = #consumers_ident;
            type Producers = #producers_ident;

            fn attach_consumers<Tl: ::flux::tile::Tile<Self>>(&mut self, tile: &Tl) -> Self::Consumers {
                #consumers_ident::attach(tile, self)
            }

            fn attach_producers<Tl: ::flux::tile::Tile<Self>>(&mut self, tile: &Tl) -> Self::Producers {
                #producers_ident::attach(tile, self)
            }

            fn register_tile(&mut self, name: ::flux::tile::TileName) -> u16 {
                self.tile_info.register_tile(name)
            }

            fn app_name() -> &'static str {
                #app_name_tokens
            }
        }

        // start() method with only the necessary PersistingQueues
        impl #struct_ident {
            #generated_new_method_token_stream // Use the correctly generated new method

            #[::flux::tracing::instrument(skip_all, fields(system = "Spine"))]
            pub fn start<F>(mut self, on_panic: Option<Box<dyn Fn(&::std::panic::PanicHookInfo<'_>) + Sync + Send>>, f: F)
            where F: FnOnce(&mut ::flux::spine::ScopedSpine<'_, '_, #struct_ident>),
            {
                std::thread::scope(|s| {
                    let mut scoped = ::flux::spine::ScopedSpine::new(&mut self, s, on_panic);
                    f(&mut scoped);

                    ::flux::core_affinity::set_for_current(*::flux::core_affinity::get_core_ids().unwrap().last().unwrap());

                    #(#persisting)*     // ← injected only for #[persist] fields
                });
                ::flux::tracing::info!("Finished…");
            }

            #[::flux::tracing::instrument(skip_all, fields(system = "Spine"))]
            pub fn start_no_persist<F>(mut self, on_panic: Option<Box<dyn Fn(&::std::panic::PanicHookInfo<'_>) + Sync + Send>>, f: F)
            where
                F: FnOnce(&mut ::flux::spine::ScopedSpine<'_, '_, #struct_ident>),
            {
                std::thread::scope(|s| {
                    let mut scoped = ::flux::spine::ScopedSpine::new(&mut self, s, on_panic);
                    f(&mut scoped);
                })
            }

            pub fn message_names(&self) -> Vec<String>
            {
                vec![ #(#message_types),* ]

            }
       }
    };

    TokenStream::from(expanded)
}
