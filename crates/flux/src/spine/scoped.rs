use std::{
    panic::PanicHookInfo,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread::{self, ScopedJoinHandle},
    time::Duration,
};

use signal_hook::{
    consts::{SIGINT, SIGQUIT, SIGTERM},
    flag as signal_flag,
};

pub struct ScopedSpine<'a, 'b: 'a, S> {
    pub spine: &'b mut S,
    pub scope: &'a thread::Scope<'a, 'b>,
    pub stop_flag: Arc<AtomicUsize>,
}

// Note: this is unecassary if every thread in a process runs as a tile.
// We just have this here in case the process runs some threads as
// non-tiles
fn spawn_signal_handler_fallback(stop_flag: Arc<AtomicUsize>, grace: Duration) {
    thread::spawn(move || {
        loop {
            let sig = stop_flag.load(Ordering::Relaxed);
            if sig != 0 {
                thread::sleep(grace);
                let _ = signal_hook::low_level::emulate_default_handler(sig as libc::c_int);
            }
            thread::sleep(Duration::from_secs(1));
        }
    });
}

#[allow(clippy::type_complexity)]
fn setup_panic_hook(
    stop_flag: Arc<AtomicUsize>,
    on_panic: Option<Box<dyn Fn(&PanicHookInfo<'_>) + Sync + Send>>,
) {
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        stop_flag.store(SIGINT as usize, Ordering::Relaxed);
        if let Some(on_panic) = &on_panic {
            on_panic(panic_info)
        }
        original_hook(panic_info);
    }));
}

impl<'a, 'b: 'a, S> ScopedSpine<'a, 'b, S> {
    /// Pass through `Some` `custom_signal_handler` if you want a custom signal handler.
    /// Field is equal to the grace (sleep) time it will give to Tile teardown before shutting down the process. 
    #[allow(clippy::type_complexity)]
    pub fn new(
        spine: &'b mut S,
        scope: &'a thread::Scope<'a, 'b>,
        on_panic: Option<Box<dyn Fn(&PanicHookInfo<'_>) + Sync + Send>>,
        custom_signal_handler: Option<Duration>,
    ) -> Self {
        let stop_flag = Arc::new(AtomicUsize::new(0));
        const SIGTERM_U: usize = SIGTERM as usize;
        const SIGINT_U: usize = SIGINT as usize;
        const SIGQUIT_U: usize = SIGQUIT as usize;
        signal_flag::register_usize(SIGTERM, Arc::clone(&stop_flag), SIGTERM_U)
            .expect("register SIGTERM");
        signal_flag::register_usize(SIGINT, Arc::clone(&stop_flag), SIGINT_U)
            .expect("register SIGINT");
        signal_flag::register_usize(SIGQUIT, Arc::clone(&stop_flag), SIGQUIT_U)
            .expect("register SIGQUIT");

        setup_panic_hook(Arc::clone(&stop_flag), on_panic);
        if let Some(grace) = custom_signal_handler {
            spawn_signal_handler_fallback(Arc::clone(&stop_flag), grace);
        }

        Self { spine, scope, stop_flag }
    }

    #[inline]
    pub fn spawn<F, T>(&self, f: F) -> ScopedJoinHandle<'a, T>
    where
        F: FnOnce() -> T + Send + 'a,
        T: Send + 'a,
    {
        self.scope.spawn(f)
    }
}
