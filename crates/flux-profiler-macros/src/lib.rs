//! Proc-macro support for the flux profiler. Exposes `#[timed]`, which wraps a
//! function body in a drop guard that records a frame open/close into the
//! cross-process flamegraph rings. Built with the `perf` feature, the same
//! marks carry hardware counters (instructions, cycles, branch/cache misses)
//! via rdpmc. Storage and Drop-side recording live in `flux-profiler`; this
//! crate is only the attribute-macro glue.

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, LitStr, parse_macro_input};

/// Wrap a function body in a `#[timed]` frame guard.
///
/// Default frame name is `concat!(module_path!(), "::", fn_name)` resolved at
/// the call site, so a frame reads `{crate}::{module}::{fn_name}` and stays
/// unambiguous across crates without manual labelling.
///
/// On a method, the default name auto-qualifies by the monomorphized `Self`:
/// `type_name` bakes the concrete type in per instantiation, so generic code
/// whose frames would otherwise collapse onto one compile-time string (e.g.
/// `ColumnGroup<Balances>` vs `ColumnGroup<Inactivity>`) stays split — the type
/// info a flamegraph can't recover from a bare address in-process. Generic
/// helpers called underneath still split by their qualified parent path, so
/// only the receiver type is folded in (not fn-level type params, which would
/// fork a frame per closure type / call site). The name is a `&'static str`
/// (no hot-path cost); the report unwraps the marker into a `fn<Type>` label.
/// Free functions keep the plain `module::path::fn` name.
///
/// Pass a string literal to override: `#[timed("custom_name")]` uses that name
/// verbatim (no module prefix, no type qualification).
///
/// Records the frame's close on every exit path — normal return, `?`, early
/// `return`, panic-unwind — via a Drop guard.
#[proc_macro_attribute]
pub fn timed(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    // Profiling disabled: macro expands to just the function body, no guard generated
    if cfg!(feature = "disable-profiling") {
        drop(attr);
        return quote! { #input }.into();
    }

    let name = if attr.is_empty() { None } else { Some(parse_macro_input!(attr as LitStr)) };
    let func_name_str = input.sig.ident.to_string();

    let is_method = matches!(input.sig.inputs.first(), Some(syn::FnArg::Receiver(_)));
    let timer_name_expr = match &name {
        Some(lit) => quote! { #lit },
        None if is_method => quote! {{
            struct __TimedTy<T: ?Sized>(::core::marker::PhantomData<T>);
            ::core::any::type_name::<__TimedTy<Self>>()
        }},
        None => quote! { concat!(module_path!(), "::", #func_name_str) },
    };

    let ItemFn { attrs, vis, sig, block } = input;

    let expanded = quote! {
        #(#attrs)* #vis #sig {
            let __timed_guard = ::flux_profiler::TimerGuard::new(#timer_name_expr);
            #block
        }
    };

    expanded.into()
}
