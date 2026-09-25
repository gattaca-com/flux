use proc_macro::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::{
    Attribute, Expr, GenericArgument, Ident, ItemStruct, LitStr, Path, PathArguments, Result,
    Token, Type, parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    spanned::Spanned,
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
            return Ok(Self::Str(s));
        }

        // #[from_spine(name = "app")]
        if input.peek(Ident) {
            let ident: Ident = input.parse()?;
            if input.peek(Token![=]) {
                input.parse::<Token![=]>()?;
                let s: LitStr = input.parse()?;
                if ident == "name" {
                    return Ok(Self::Named(s));
                }
                return Err(syn::Error::new_spanned(ident, r#"expected `name = "..."`"#));
            }
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
            return Ok(Self::Path(p));
        }

        // Fallback: a full Path (e.g., starting with ::)
        let p: Path = input.parse()?;
        Ok(Self::Path(p))
    }
}

impl FromSpineArg {
    fn as_tokens(&self) -> proc_macro2::TokenStream {
        match self {
            Self::Str(s) | Self::Named(s) => quote! { #s },
            Self::Path(p) => quote! { stringify!(#p) },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueueFlavour {
    Mpmc,
    Spmc,
    Spsc,
}

struct QueueConfig {
    is_gather: bool,
    size_expr: Option<Expr>,
    flavour: QueueFlavour,
    mtu_expr: Option<Expr>,
    slot_expr: Option<Expr>,
    gather_with_args: bool,
    spsc_span: Option<proc_macro2::Span>,
}

fn get_queue_config(attrs: &[Attribute]) -> Result<QueueConfig> {
    let mut config = QueueConfig {
        is_gather: false,
        size_expr: None,
        flavour: QueueFlavour::Mpmc,
        mtu_expr: None,
        slot_expr: None,
        gather_with_args: false,
        spsc_span: None,
    };

    for attr in attrs {
        if attr.path().is_ident("queue") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("gather") {
                    config.is_gather = true;
                    if meta.input.peek(syn::token::Paren) {
                        let content;
                        parenthesized!(content in meta.input);
                        let _: proc_macro2::TokenStream = content.parse()?;
                        config.gather_with_args = true;
                    }
                    return Ok(());
                }
                if meta.path.is_ident("size") {
                    let content;
                    parenthesized!(content in meta.input);
                    let lit: Expr = content.parse()?;
                    config.size_expr = Some(lit);
                    return Ok(());
                }
                if meta.path.is_ident("flavour") {
                    let content;
                    parenthesized!(content in meta.input);
                    let s: LitStr = content.parse()?;
                    config.flavour = match s.value().as_str() {
                        "mpmc" => QueueFlavour::Mpmc,
                        "spmc" => QueueFlavour::Spmc,
                        "spsc" => {
                            config.spsc_span = Some(s.span());
                            QueueFlavour::Spsc
                        }
                        flavour => {
                            return Err(meta.error(format!(
                                "unsupported queue flavour `{flavour}`; expected `mpmc`, `spmc`, or `spsc`"
                            )));
                        }
                    };
                    return Ok(());
                }
                if meta.path.is_ident("mtu") {
                    let content;
                    parenthesized!(content in meta.input);
                    let lit: Expr = content.parse()?;
                    config.mtu_expr = Some(lit);
                    return Ok(());
                }
                if meta.path.is_ident("slot") {
                    let content;
                    parenthesized!(content in meta.input);
                    let expr: Expr = content.parse()?;
                    if !content.is_empty() {
                        return Err(content.error("expected one slot size expression"));
                    }
                    if config.slot_expr.replace(expr).is_some() {
                        return Err(meta.error("duplicate `slot` argument"));
                    }
                    return Ok(());
                }
                Err(meta.error("unrecognized queue argument"))
            })?;
        }
    }

    if config.flavour == QueueFlavour::Spsc {
        let span = config.spsc_span.unwrap_or_else(proc_macro2::Span::call_site);
        if config.is_gather {
            return Err(syn::Error::new(span, "SPSC queues cannot use `gather`"));
        }
    }
    if config.slot_expr.is_some() && config.flavour != QueueFlavour::Spsc {
        return Err(syn::Error::new_spanned(
            config.slot_expr.as_ref().unwrap(),
            "`slot` requires `flavour(\"spsc\")`",
        ));
    }

    Ok(config)
}

fn last_path_type_arg(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(inner_ty) => Some(inner_ty),
        _ => None,
    })
}

fn spine_queue_inner_ty(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    if type_path.path.segments.last()?.ident != "SpineQueue" {
        return None;
    }
    last_path_type_arg(ty)
}
/// Generate a spine struct plus consumers/producers, config, and the
/// `FluxSpine` impl.
///
/// Queue attributes (`#[queue(..)]` on `SpineQueue<T>` fields):
/// - `size(..)`: queue capacity (default `2usize.pow(15)`).
/// - `flavour("mpmc")`, `flavour("spmc")`, or `flavour("spsc")`: queue flavour.
/// - `mtu(..)`: dcache-backed queue with the given max frame size.
/// - `slot(bytes)`: SPSC byte stride including tracking metadata. Nonzero sizes
///   select alignment equal to their largest power-of-two divisor. Zero selects
///   the natural stored message size and alignment.
/// - `gather`: drain this queue into a `BlobCache` via the generated
///   `GatherQueues` impl; every gathered type must implement
///   `HasVersionedLeaves` and the crate needs a direct `flux-gather`
///   dependency. A boundary orders only its own producer thread's messages and
///   rings are independent, so drain once more after a boundary before
///   flushing.
///
/// SPSC queues return `Full` through `SpineAdapter::try_produce`; the caller
/// keeps pending output and retries. Their endpoints are claimed on first use
/// and cannot be cloned. SPSC queues with `mtu` use a Spine-managed `DCache`.
/// SPSC queues do not support `gather`. Spines containing SPSC queues have
/// unsafe shared-memory constructors; see `SpineSpscQueue` and
/// `SpineSpscDCacheQueue`.
#[allow(clippy::too_many_lines)]
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
    let mut spine_as_ref_impls = Vec::<proc_macro2::TokenStream>::new();
    let mut gather_fields = Vec::<(Type, bool)>::new();
    let mut message_types = Vec::<proc_macro2::TokenStream>::new();
    let mut ffi_check_items = Vec::<proc_macro2::TokenStream>::new();
    let mut has_spsc = false;
    let mut spsc_fields = Vec::<Ident>::new();

    for field in &input.fields {
        let field_ident = field.ident.as_ref().expect("named field required");

        // recognise Queue<T>
        if let Some(inner_ty) = spine_queue_inner_ty(&field.ty) {
            message_types.push(quote! {
                ::flux::utils::short_typename::<#inner_ty>().to_string()
            });

            let check_fn = format_ident!("_ffi_check_{}_{}", struct_ident, field_ident);
            let inner_ty_span = inner_ty.span();
            ffi_check_items
                .push(quote_spanned! { inner_ty_span => fn #check_fn(var: *const #inner_ty); });

            let queue_config = match get_queue_config(&field.attrs) {
                Ok(config) => config,
                Err(error) => return error.into_compile_error().into(),
            };
            let is_gather = queue_config.is_gather;
            let mtu_expr = queue_config.mtu_expr.as_ref();
            let gather_with_args = queue_config.gather_with_args;

            if gather_with_args {
                return syn::Error::new_spanned(field_ident, "expected `gather`")
                    .to_compile_error()
                    .into();
            }
            if is_gather {
                gather_fields.push((inner_ty.clone(), mtu_expr.is_some()));
            }

            if queue_config.flavour == QueueFlavour::Spsc {
                has_spsc = true;
                spsc_fields.push(field_ident.clone());
                let slot_arg = queue_config.slot_expr.as_ref().map(|expr| quote! { , { #expr } });

                let (consumer_ty, producer_ty, queue_ty) = if mtu_expr.is_some() {
                    (
                        quote! { ::flux::spine::SpineSpscDCacheConsumer<#inner_ty #slot_arg> },
                        quote! { ::flux::spine::SpineSpscProducerWithDCache<#inner_ty #slot_arg> },
                        quote! { ::flux::spine::SpineSpscDCacheQueue<#inner_ty #slot_arg> },
                    )
                } else {
                    (
                        quote! { ::flux::spine::SpineSpscConsumer<#inner_ty #slot_arg> },
                        quote! { ::flux::spine::SpineSpscProducer<#inner_ty #slot_arg> },
                        quote! { ::flux::spine::SpineSpscQueue<#inner_ty #slot_arg> },
                    )
                };

                consumer_fields.push(quote! {
                    pub #field_ident : #consumer_ty
                });
                producer_fields.push(quote! {
                    pub #field_ident : #producer_ty
                });

                consumer_init.push(quote! {
                    #field_ident : <#consumer_ty>::attach::<_, #struct_ident, _>(
                        &spine.base_dir, tile, spine.#field_ident.clone())
                });
                producer_init.push(quote! {
                    #field_ident : <#producer_ty>::new(spine.#field_ident.clone())
                });

                as_mut_impls.push(quote! {
                    impl AsMut<#consumer_ty> for #consumers_ident {
                        fn as_mut(&mut self) -> &mut #consumer_ty {
                            &mut self.#field_ident
                        }
                    }
                    impl AsMut<#producer_ty> for #producers_ident {
                        fn as_mut(&mut self) -> &mut #producer_ty {
                            &mut self.#field_ident
                        }
                    }
                });

                as_mut_impls.push(if mtu_expr.is_some() {
                    quote! {
                        impl ::flux::spine::SpscDCacheConsumerAccess<#inner_ty> for #consumers_ident {
                            #[inline]
                            fn spsc_dcache_try_attached(&mut self)
                                -> ::core::result::Result<impl ::flux::spine::SpscAttachedDCacheConsumer<#inner_ty> + '_,
                                          ::flux::communication::queue::spsc::QueueError>
                            {
                                self.#field_ident.try_attached()
                            }
                        }
                        impl ::flux::spine::SpscDCacheProducerAccess<#inner_ty> for #producers_ident {
                            #[inline]
                            fn spsc_dcache_try_produce_with(
                                &mut self,
                                len: ::core::option::Option<usize>,
                                make: impl ::core::ops::FnOnce(::core::option::Option<&mut [u8]>) -> ::flux::timing::InternalMessage<#inner_ty>,
                            ) -> ::core::result::Result<(), ::flux::spine::SpscDCacheProduceError> {
                                self.#field_ident.try_produce_with(len, make)
                            }
                        }
                    }
                } else {
                    quote! {
                        impl ::flux::spine::SpscConsumerAccess<#inner_ty> for #consumers_ident {
                            #[inline]
                            fn spsc_try_attached(&mut self)
                                -> ::core::result::Result<impl ::flux::spine::SpscAttachedConsumer<#inner_ty> + '_,
                                          ::flux::communication::queue::spsc::QueueError>
                            {
                                self.#field_ident.try_attached()
                            }
                        }
                        impl ::flux::spine::SpscProducerAccess<#inner_ty> for #producers_ident {
                            #[inline]
                            fn spsc_try_produce_with(
                                &mut self,
                                make: impl ::core::ops::FnOnce() -> ::flux::timing::InternalMessage<#inner_ty>,
                            ) -> ::core::result::Result<(), ::flux::spine::SpscProduceError> {
                                self.#field_ident.try_produce_with(make)
                            }
                        }
                    }
                });

                spine_as_ref_impls.push(quote! {
                    impl AsRef<#queue_ty> for #struct_ident {
                        fn as_ref(&self) -> &#queue_ty {
                            &self.#field_ident
                        }
                    }
                });
            } else if mtu_expr.is_some() {
                // ── dcache-backed queue ───────────────────────────────────
                let dcache_ident = format_ident!("{}_dcache", field_ident);

                consumer_fields.push(quote! {
                    pub #field_ident : ::flux::spine::SpineDCacheConsumer<#inner_ty>
                });
                producer_fields.push(quote! {
                    pub #field_ident : ::flux::spine::SpineProducerWithDCache<#inner_ty>
                });

                consumer_init.push(quote! {
                    #field_ident : ::flux::spine::SpineDCacheConsumer::attach::<_, #struct_ident, _>(
                        &spine.base_dir, tile, spine.#field_ident, spine.#dcache_ident)
                });
                producer_init.push(quote! {
                    #field_ident : ::flux::spine::SpineProducerWithDCache::new(
                        spine.#field_ident, spine.#dcache_ident)
                });

                as_ref_impls.push(quote! {
                    impl AsRef<::flux::spine::SpineProducer<::flux::spine::DCacheMsg<#inner_ty>>>
                        for #producers_ident
                    {
                        fn as_ref(&self) -> &::flux::spine::SpineProducer<::flux::spine::DCacheMsg<#inner_ty>> {
                            self.#field_ident.as_ref()
                        }
                    }
                    impl AsRef<::flux::spine::SpineProducerWithDCache<#inner_ty>>
                        for #producers_ident
                    {
                        fn as_ref(&self) -> &::flux::spine::SpineProducerWithDCache<#inner_ty> {
                            &self.#field_ident
                        }
                    }
                });

                as_mut_impls.push(quote! {
                    impl AsMut<::flux::spine::SpineDCacheConsumer<#inner_ty>>
                        for #consumers_ident
                    {
                        fn as_mut(&mut self) -> &mut ::flux::spine::SpineDCacheConsumer<#inner_ty> {
                            &mut self.#field_ident
                        }
                    }
                    impl AsRef<::flux::spine::SpineDCacheConsumer<#inner_ty>>
                        for #consumers_ident
                    {
                        fn as_ref(&self) -> &::flux::spine::SpineDCacheConsumer<#inner_ty> {
                            &self.#field_ident
                        }
                    }
                });

                spine_as_ref_impls.push(quote! {
                    impl AsRef<::flux::spine::SpineQueue<::flux::spine::DCacheMsg<#inner_ty>>>
                        for #struct_ident
                    {
                        fn as_ref(&self) -> &::flux::spine::SpineQueue<::flux::spine::DCacheMsg<#inner_ty>> {
                            &self.#field_ident
                        }
                    }
                    impl ::flux::spine::HasDCacheQueue<#inner_ty> for #struct_ident {
                        fn dcache_queue_and_ptr(
                            &self,
                        ) -> (
                            ::flux::spine::SpineQueue<::flux::spine::DCacheMsg<#inner_ty>>,
                            ::flux::utils::DCachePtr,
                        ) {
                            (self.#field_ident, self.#dcache_ident)
                        }
                    }
                });
            } else {
                // ── standard queue ────────────────────────────────────────
                consumer_fields
                    .push(quote! { pub #field_ident : ::flux::spine::SpineConsumer<#inner_ty> });
                producer_fields
                    .push(quote! { pub #field_ident : ::flux::spine::SpineProducer<#inner_ty> });

                consumer_init.push(quote! {
                    #field_ident : ::flux::spine::SpineConsumer::attach::<_, #struct_ident, _>(
                        &spine.base_dir, tile, spine.#field_ident)
                });
                producer_init.push(quote! {
                    #field_ident : ::flux::communication::queue::Producer::from(spine.#field_ident)
                });

                as_ref_impls.push(quote! {
                    impl AsRef<::flux::spine::SpineProducer<#inner_ty>> for #producers_ident {
                        fn as_ref(&self)->&::flux::spine::SpineProducer<#inner_ty>{ &self.#field_ident }
                    }
                });

                spine_as_ref_impls.push(quote! {
                    impl AsRef<::flux::spine::SpineQueue<#inner_ty>> for #struct_ident {
                        fn as_ref(&self) -> &::flux::spine::SpineQueue<#inner_ty> { &self.#field_ident }
                    }
                });

                as_mut_impls.push(quote! {
                    impl AsMut<::flux::spine::SpineConsumer<#inner_ty>> for #consumers_ident {
                        fn as_mut(&mut self)->&mut ::flux::spine::SpineConsumer<#inner_ty>{
                            &mut self.#field_ident
                        }
                    }
                    impl AsRef<::flux::spine::SpineConsumer<#inner_ty>> for #consumers_ident {
                        fn as_ref(&self)->&::flux::spine::SpineConsumer<#inner_ty>{
                            &self.#field_ident
                        }
                    }
                });
            }
        } else if let Some(inner_ty) = last_path_type_arg(&field.ty) {
            let check_fn = format_ident!("_ffi_check_{}_{}", struct_ident, field_ident);
            let inner_ty_span = inner_ty.span();
            ffi_check_items
                .push(quote_spanned! { inner_ty_span => fn #check_fn(var: *const #inner_ty); });
        }
    }

    let mut gather_passes = Vec::<proc_macro2::TokenStream>::new();
    for (inner_ty, is_dcache) in &gather_fields {
        let inner_ty_span = inner_ty.span();
        if *is_dcache {
            gather_passes.push(quote_spanned! { inner_ty_span =>
                adapter.consume_with_dcache_internal_message(
                    |_: &::flux::timing::InternalMessage<#inner_ty>, _payload: &[u8]| {},
                    |r, _| match r {
                        ::flux::spine::DCacheRead::Ok((m, ())) | ::flux::spine::DCacheRead::NoRef(m) | ::flux::spine::DCacheRead::Lost(m) => cache.push(&m),
                        ::flux::spine::DCacheRead::SpedPast => {}
                    },
                );
            });
        } else {
            gather_passes.push(quote_spanned! { inner_ty_span =>
                adapter.consume_internal_message(
                    |m: &mut ::flux::timing::InternalMessage<#inner_ty>, _| cache.push(&*m),
                );
            });
        }
    }
    let gather_impl = if gather_fields.is_empty() {
        None
    } else {
        Some(quote! {
            impl ::flux_gather::GatherQueues for #struct_ident {
                fn gather_into(
                    adapter: &mut ::flux::spine::SpineAdapter<Self>,
                    cache: &mut ::flux_gather::BlobCache,
                ) {
                    #(#gather_passes)*
                }
            }
        })
    };

    // ---- Build `new_with_base_dir` body as explicit let-bindings ----
    // This allows dcache queue fields to destructure a tuple (queue, dcache_ptr)
    // while non-dcache fields remain single-binding.
    let mut new_let_stmts: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut new_struct_field_names: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut config_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut config_defaults: Vec<proc_macro2::TokenStream> = Vec::new();
    new_struct_field_names.push(quote! { base_dir });

    for field in &input.fields {
        let field_ident = field.ident.as_ref().expect("named field required for new method");

        //TODO: @gd this is so cursed
        if field_ident == "tile_info" {
            new_let_stmts.push(quote! {
                let tile_info = ::flux::communication::ShmemData::open_or_init_with_base_dir(
                    &base_dir,
                    &format!("{}{}", #app_name_tokens, path_suffix),
                    || Default::default(),
                ).expect("couldn't open or init tile info shmem");
            });
            new_struct_field_names.push(quote! { tile_info });
        } else if let Some(inner_ty) = spine_queue_inner_ty(&field.ty) {
            let queue_config = match get_queue_config(&field.attrs) {
                Ok(config) => config,
                Err(error) => return error.into_compile_error().into(),
            };
            let size_arg = queue_config
                .size_expr
                .as_ref()
                .map_or_else(|| quote! { 2usize.pow(15) }, |expr| quote! { #expr });
            let queue_type = match queue_config.flavour {
                QueueFlavour::Mpmc => quote! { ::flux::communication::queue::QueueType::MPMC },
                QueueFlavour::Spmc => quote! { ::flux::communication::queue::QueueType::SPMC },
                QueueFlavour::Spsc => quote! {},
            };
            if queue_config.flavour == QueueFlavour::Spsc {
                let slot_arg = queue_config.slot_expr.as_ref().map(|expr| quote! { , { #expr } });
                if let Some(mtu_expr) = queue_config.mtu_expr.as_ref() {
                    config_fields.push(quote! {
                        pub #field_ident: ::flux::spine::DCacheQueueParams
                    });
                    config_defaults.push(quote! {
                        #field_ident: ::flux::spine::DCacheQueueParams { size: #size_arg, mtu: #mtu_expr }
                    });
                    new_let_stmts.push(quote! {
                        let #field_ident = unsafe {
                            ::flux::spine::SpineSpscDCacheQueue::<#inner_ty #slot_arg>::create_or_open_shared_with_base_dir(
                                &base_dir,
                                &format!("{}{}", #app_name_tokens, path_suffix),
                                stringify!(#field_ident),
                                config.#field_ident.size,
                                config.#field_ident.mtu,
                            )
                        };
                    });
                } else {
                    config_fields.push(quote! {
                        pub #field_ident: ::flux::spine::QueueParams
                    });
                    config_defaults.push(quote! {
                        #field_ident: ::flux::spine::QueueParams { size: #size_arg }
                    });
                    new_let_stmts.push(quote! {
                        let #field_ident = unsafe {
                            ::flux::spine::SpineSpscQueue::<#inner_ty #slot_arg>::create_or_open_shared_with_base_dir(
                                &base_dir,
                                &format!("{}{}", #app_name_tokens, path_suffix),
                                stringify!(#field_ident),
                                config.#field_ident.size,
                            )
                        };
                    });
                }
                new_struct_field_names.push(quote! { #field_ident });
            } else if let Some(mtu_expr) = queue_config.mtu_expr.as_ref() {
                let dcache_ident = format_ident!("{}_dcache", field_ident);
                config_fields.push(quote! {
                    pub #field_ident: ::flux::spine::DCacheQueueParams
                });
                config_defaults.push(quote! {
                        #field_ident: ::flux::spine::DCacheQueueParams { size: #size_arg, mtu: #mtu_expr }
                    });
                new_let_stmts.push(quote! {
                    let (#field_ident, #dcache_ident) =
                        ::flux::communication::shmem_queue_dcache_with_base_dir(
                            &base_dir,
                            &format!("{}{}", #app_name_tokens, path_suffix),
                            config.#field_ident.size,
                            config.#field_ident.mtu,
                            #queue_type,
                        );
                });
                new_struct_field_names.push(quote! { #field_ident });
                new_struct_field_names.push(quote! { #dcache_ident });
            } else {
                config_fields.push(quote! {
                    pub #field_ident: ::flux::spine::QueueParams
                });
                config_defaults.push(quote! {
                    #field_ident: ::flux::spine::QueueParams { size: #size_arg }
                });
                new_let_stmts.push(quote! {
                    let #field_ident = ::flux::communication::shmem_queue_with_base_dir(
                        &base_dir,
                        &format!("{}{}", #app_name_tokens, path_suffix),
                        config.#field_ident.size,
                        #queue_type,
                    );
                });
                new_struct_field_names.push(quote! { #field_ident });
            }
        } else {
            new_let_stmts.push(quote! { let #field_ident = Default::default(); });
            new_struct_field_names.push(quote! { #field_ident });
        }
    }

    let config_ident = format_ident!("{}Config", struct_ident);

    let constructor_unsafety = has_spsc.then(|| quote! { unsafe });
    let constructor_docs = has_spsc.then(|| quote! {
        #[doc = "# Safety"]
        #[doc = ""]
        #[doc = "All participants must use the same payload types, layout, architecture, and application schema."]
        #[doc = "Values must be valid in every process, without process-local pointers or references."]
        #[doc = "All mapping access must use the SPSC queue implementation. Endpoints inherited across `fork` must not be used or dropped in the child."]
        #[doc = "See the SPSC queue type's `create_or_open_shared_with_base_dir` safety contract."]
    });
    let call_constructor = |call| {
        if has_spsc {
            quote! { unsafe { #call } }
        } else {
            call
        }
    };
    let new_default = call_constructor(quote! {
        Self::new_with_base_dir(::flux::utils::directories::local_share_dir(), path_suffix)
    });
    let new_config = call_constructor(quote! {
        Self::new_with_base_dir_and_config(
            ::flux::utils::directories::local_share_dir(), path_suffix, config,
        )
    });
    let new_base_dir = call_constructor(quote! {
        Self::new_with_base_dir_and_config(base_dir, path_suffix, #config_ident::default())
    });
    let generated_new_method_token_stream = quote! {
        #constructor_docs
        pub #constructor_unsafety fn new(path_suffix: Option<&str>) -> Self {
            #new_default
        }
        #constructor_docs
        pub #constructor_unsafety fn new_with_config(path_suffix: Option<&str>, config: #config_ident) -> Self {
            #new_config
        }
        #constructor_docs
        pub #constructor_unsafety fn new_with_base_dir<D: AsRef<std::path::Path>>(
            base_dir: D,
            path_suffix: Option<&str>,
        ) -> Self {
            #new_base_dir
        }
        #constructor_docs
        pub #constructor_unsafety fn new_with_base_dir_and_config<D: AsRef<std::path::Path>>(
            base_dir: D,
            path_suffix: Option<&str>,
            config: #config_ident,
        ) -> Self {
            let path_suffix = path_suffix.unwrap_or("");
            let base_dir = base_dir.as_ref().to_path_buf();
            #(#new_let_stmts)*
            Self { #(#new_struct_field_names),* }
        }
    };

    let bundle_derives = if has_spsc {
        quote! { #[derive(Debug)] }
    } else {
        quote! { #[derive(Clone, Copy, Debug)] }
    };
    let requires_polling_impl = if has_spsc {
        quote! {
            fn requires_polling(consumers: &Self::Consumers, producers: &Self::Producers) -> bool {
                false #(|| consumers.#spsc_fields.is_attached())* #(|| producers.#spsc_fields.is_attached())*
            }
        }
    } else {
        quote! {}
    };
    let new_in_base_dir_body = if has_spsc {
        quote! { unsafe { Self::new_with_base_dir(base_dir, None) } }
    } else {
        quote! { Self::new_with_base_dir(base_dir, None) }
    };

    // Reconstruct the input struct without #[queue] attributes on its fields.
    // For non-SPSC SpineQueue fields with `mtu`, also inject a
    // `{field}_dcache: DCachePtr` field.
    let input_attrs = &input.attrs;
    let vis = &input.vis;
    let struct_ident = &input.ident;
    let generics_decl = &input.generics;

    let reconstructed_fields = match &input.fields {
        syn::Fields::Named(fields_named) => {
            let mut all_fields: Vec<proc_macro2::TokenStream> = Vec::new();
            for f in &fields_named.named {
                let attrs = f.attrs.iter().filter(|a| !a.path().is_ident("queue"));
                let fvis = &f.vis;
                let ident = &f.ident;
                let colon_token = &f.colon_token;
                let ty = &f.ty;

                // Dcache and SPSC queues use storage types different from the input marker.
                if let Some(inner_ty) = spine_queue_inner_ty(ty) {
                    let queue_config = match get_queue_config(&f.attrs) {
                        Ok(config) => config,
                        Err(error) => return error.into_compile_error().into(),
                    };
                    if queue_config.flavour == QueueFlavour::Spsc {
                        let slot_arg =
                            queue_config.slot_expr.as_ref().map(|expr| quote! { , { #expr } });
                        let new_ty = if queue_config.mtu_expr.is_some() {
                            quote! { ::flux::spine::SpineSpscDCacheQueue<#inner_ty #slot_arg> }
                        } else {
                            quote! { ::flux::spine::SpineSpscQueue<#inner_ty #slot_arg> }
                        };
                        all_fields.push(quote! { #(#attrs)* #fvis #ident #colon_token #new_ty });
                    } else if queue_config.mtu_expr.is_some() {
                        let dcache_ident = format_ident!("{}_dcache", ident.as_ref().unwrap());
                        let new_ty = quote! {
                            ::flux::spine::SpineQueue<::flux::spine::DCacheMsg<#inner_ty>>
                        };
                        all_fields.push(quote! { #(#attrs)* #fvis #ident #colon_token #new_ty });
                        all_fields.push(quote! { #dcache_ident: ::flux::utils::DCachePtr });
                    } else {
                        all_fields.push(quote! { #(#attrs)* #fvis #ident #colon_token #ty });
                    }
                } else {
                    all_fields.push(quote! { #(#attrs)* #fvis #ident #colon_token #ty });
                }
            }
            all_fields.push(quote! { base_dir: std::path::PathBuf });
            quote! { { #(#all_fields),* } }
        }
        syn::Fields::Unnamed(fields_unnamed) => {
            let iter = fields_unnamed.unnamed.iter().map(|f| {
                let attrs = f.attrs.iter().filter(|attr| !attr.path().is_ident("queue"));
                let fvis = &f.vis;
                let ty = &f.ty;
                quote! { #(#attrs)* #fvis #ty }
            });
            quote! { ( #(#iter),*, std::path::PathBuf ); }
        }
        syn::Fields::Unit => quote! { {base_dir: std::path::PathBuf}},
    };

    let reconstructed_input_struct = quote! {
        #(#input_attrs)*
        #vis struct #struct_ident #generics_decl
        #reconstructed_fields
    };

    // ─── 3. compose generated code ────────────────────────────────────────
    let expanded = quote! {
        #reconstructed_input_struct // Use the reconstructed struct instead of #input

        #[derive(Clone, Debug, ::serde::Deserialize)]
        #[serde(default)]
        #vis struct #config_ident {
            #(#config_fields),*
        }
        impl Default for #config_ident {
            fn default() -> Self {
                Self { #(#config_defaults),* }
            }
        }

        // generated Consumers / Producers structs
        #bundle_derives
        #vis struct #consumers_ident { #consumer_fields }
        impl #consumers_ident {
            pub fn attach<Tl: ::flux::tile::Tile<#struct_ident>>(tile: &Tl, spine: &mut #struct_ident) -> Self {
                Self { #consumer_init }
            }
        }

        #bundle_derives
        #vis struct #producers_ident { #producer_fields, timestamp: ::flux::timing::TrackingTimestamp }
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
        #(#spine_as_ref_impls)*

        #gather_impl

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

            unsafe fn new_in_base_dir(base_dir: impl AsRef<std::path::Path>) -> Self {
                #new_in_base_dir_body
            }

            #requires_polling_impl

            fn app_name() -> &'static str {
                #app_name_tokens
            }

            fn base_dir(&self) -> &std::path::Path {
                &self.base_dir
            }
        }

        impl #struct_ident {
            #generated_new_method_token_stream // Use the correctly generated new method

            #[::flux::tracing::instrument(skip_all, fields(system = "Spine"))]
            pub fn start<F>(mut self, on_panic: Option<Box<dyn Fn(&::std::panic::PanicHookInfo<'_>) + Sync + Send>>, custom_signal_handler: Option<::std::time::Duration>, f: F)
            where F: FnOnce(&mut ::flux::spine::ScopedSpine<'_, '_, #struct_ident>),
            {
                std::thread::scope(|s| {
                    let mut scoped = ::flux::spine::ScopedSpine::new(&mut self, s, on_panic, custom_signal_handler);
                    f(&mut scoped);
                    ::flux::core_affinity::set_for_current(*::flux::core_affinity::get_core_ids().unwrap().last().unwrap());
                });
                ::flux::tracing::info!("Finished…");
            }

            #[::flux::tracing::instrument(skip_all, fields(system = "Spine"))]
            pub fn start_no_persist<F>(self, on_panic: Option<Box<dyn Fn(&::std::panic::PanicHookInfo<'_>) + Sync + Send>>, custom_signal_handler: Option<::std::time::Duration>, f: F)
            where
                F: FnOnce(&mut ::flux::spine::ScopedSpine<'_, '_, #struct_ident>),
            {
                self.start(on_panic, custom_signal_handler, f);
            }

            pub fn message_names(&self) -> Vec<String>
            {
                vec![ #(#message_types),* ]

            }
       }

        unsafe extern "C" {
            #(#ffi_check_items)*
        }
    };

    TokenStream::from(expanded)
}

#[cfg(test)]
mod tests {
    use super::{QueueFlavour, get_queue_config, spine_queue_inner_ty};

    #[test]
    fn parses_supported_queue_flavours() {
        for (attribute, flavour) in [
            (syn::parse_quote!(#[queue(flavour("mpmc"))]), QueueFlavour::Mpmc),
            (syn::parse_quote!(#[queue(flavour("spmc"))]), QueueFlavour::Spmc),
            (syn::parse_quote!(#[queue(flavour("spsc"))]), QueueFlavour::Spsc),
        ] {
            assert_eq!(get_queue_config(&[attribute]).unwrap().flavour, flavour);
        }
    }

    #[test]
    fn parses_spsc_slot_expression_and_rejects_other_flavours() {
        let attribute = syn::parse_quote!(#[queue(flavour("spsc"), slot(8 * 3))]);
        let config = get_queue_config(&[attribute]).unwrap();
        let expr = config.slot_expr.unwrap();
        assert_eq!(quote::quote!(#expr).to_string(), "8 * 3");

        for attribute in [
            syn::parse_quote!(#[queue(slot(256))]),
            syn::parse_quote!(#[queue(flavour("mpmc"), slot(256))]),
            syn::parse_quote!(#[queue(flavour("spmc"), slot(256))]),
        ] {
            let error = get_queue_config(&[attribute]).err().unwrap();
            assert!(error.to_string().contains("requires `flavour(\"spsc\")`"));
        }
    }

    #[test]
    fn rejects_unknown_queue_flavour() {
        let attribute = syn::parse_quote!(#[queue(flavour("broadcast"))]);
        assert!(get_queue_config(&[attribute]).is_err());
    }

    #[test]
    fn rejects_spsc_with_gather() {
        let gather = syn::parse_quote!(#[queue(flavour("spsc"), gather)]);
        let error = get_queue_config(&[gather]).err().unwrap();
        assert!(error.to_string().contains("SPSC queues cannot use `gather`"));
    }

    #[test]
    fn accepts_spsc_with_mtu() {
        let mtu = syn::parse_quote!(#[queue(flavour("spsc"), mtu(1500))]);
        let config = get_queue_config(&[mtu]).unwrap();
        assert_eq!(config.flavour, QueueFlavour::Spsc);
        assert!(config.mtu_expr.is_some());
    }

    #[test]
    fn recognizes_qualified_spine_queue_with_spsc_mtu() {
        let input: syn::ItemStruct = syn::parse_quote! {
            struct Example {
                #[queue(flavour("spsc"), mtu(1500))]
                frames: ::flux::spine::SpineQueue<u64>
            }
        };
        let field = input.fields.iter().next().unwrap();
        let inner_ty = spine_queue_inner_ty(&field.ty).unwrap();
        assert!(matches!(inner_ty, syn::Type::Path(path) if path.path.is_ident("u64")));
        let config = get_queue_config(&field.attrs).unwrap();
        assert_eq!(config.flavour, QueueFlavour::Spsc);
        assert!(config.mtu_expr.is_some());
    }
}
