//! Per-thread allocated/freed byte accounting for the profiler rings.

#[cfg(feature = "alloc-profile")]
mod counting;
#[cfg(feature = "alloc-profile")]
pub use counting::CountingAllocator;
#[cfg(feature = "alloc-profile")]
pub(super) use counting::read;

/// The allocation analogue of `PerfSample`: two monotonic per-thread byte
/// counts.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct AllocSample {
    pub allocated: u64,
    pub freed: u64,
}

impl AllocSample {
    #[inline]
    pub fn delta(&self, earlier: &Self) -> Self {
        Self {
            allocated: self.allocated.saturating_sub(earlier.allocated),
            freed: self.freed.saturating_sub(earlier.freed),
        }
    }

    #[inline]
    pub fn add(&self, other: &Self) -> Self {
        Self {
            allocated: self.allocated.saturating_add(other.allocated),
            freed: self.freed.saturating_add(other.freed),
        }
    }

    /// Bytes still resident: a cross-thread free can push one thread's `freed`
    /// past its `allocated`, so this clamps at zero rather than wrapping.
    #[inline]
    pub fn live(&self) -> u64 {
        self.allocated.saturating_sub(self.freed)
    }
}

#[cfg(test)]
mod tests {
    use super::AllocSample;

    #[test]
    fn delta_add_and_live() {
        let open = AllocSample { allocated: 100, freed: 40 };
        let close = AllocSample { allocated: 250, freed: 90 };

        let d = close.delta(&open);
        assert_eq!((d.allocated, d.freed), (150, 50), "entry→exit charges the frame");

        let total = d.add(&AllocSample { allocated: 10, freed: 5 });
        assert_eq!((total.allocated, total.freed), (160, 55));

        assert_eq!(close.live(), 160, "live = allocated − freed");
    }

    #[test]
    fn saturates_on_cross_thread_skew() {
        // A thread that freed blocks another thread allocated: freed > allocated.
        let skewed = AllocSample { allocated: 10, freed: 25 };
        assert_eq!(skewed.live(), 0, "clamped, not wrapped");
        let d = AllocSample::default().delta(&skewed);
        assert_eq!((d.allocated, d.freed), (0, 0), "delta clamps too");
    }
}
