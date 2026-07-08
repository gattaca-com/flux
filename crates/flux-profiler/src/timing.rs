//! The drop guard behind `#[timed]`. Until [`QUEUE_DIR`] is locked (the
//! default) it allocates nothing — one atomic load per call, so tests running
//! `#[timed]` prod code touch no shmem. Locked, the guard records a frame
//! open/close into the cross-process per-thread profiler rings; hardware
//! counters (with the `perf` feature) ride alongside each mark there.

use super::{producer, queue_dir::QUEUE_DIR};

/// Drop-based timer scope used by the `#[timed]` macro expansion. Records a
/// frame open on construction and a close on every exit path — normal return,
/// `?`, early `return`, panic-unwind.
#[doc(hidden)]
pub struct TimerGuard {
    close: Option<&'static str>,
}

impl TimerGuard {
    #[inline]
    pub fn new(name: &'static str) -> Self {
        let close = QUEUE_DIR.get().is_some().then_some(name);
        if let Some(name) = close {
            producer::record_open(name);
        }
        Self { close }
    }
}

impl Drop for TimerGuard {
    fn drop(&mut self) {
        if let Some(name) = self.close {
            producer::record_close(name);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{test_shmem::ShmemGuard, timed};

    #[timed]
    fn timed_default_name(x: u64) -> u64 {
        x * 2
    }

    #[timed("custom_label")]
    fn timed_custom_name(x: u64) -> Result<u64, &'static str> {
        if x == 0 { Err("zero") } else { Ok(x + 1) }
    }

    #[test]
    fn timed_macro_expands_and_runs() {
        let _guard = ShmemGuard::new();

        assert_eq!(timed_default_name(7), 14);
        assert_eq!(timed_custom_name(0), Err("zero"));
        assert_eq!(timed_custom_name(41), Ok(42));
    }
}
