//! The raw counter record. Plain numbers, positional by [`Schema`] slot, so
//! the hot path does no interpretation — labels and ratios (IPC, misses/call)
//! are derived in postprocessing.
//!
//! [`Schema`]: super::Schema

/// Max counters read per call. Bounds the hot-path array and the streamed
/// queue element; the PMU's general-purpose counter budget is usually the
/// tighter limit.
pub const MAX_EVENTS: usize = 8;

/// One call's raw counter values, positional by [`Schema`](super::Schema) slot.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct PerfSample {
    pub vals: [u64; MAX_EVENTS],
}

impl PerfSample {
    /// Per-slot saturating delta (`self - earlier`), for entry→exit diffs.
    #[inline]
    pub fn delta(&self, earlier: &Self) -> Self {
        let mut out = Self::default();
        for i in 0..MAX_EVENTS {
            out.vals[i] = self.vals[i].saturating_sub(earlier.vals[i]);
        }
        out
    }

    /// Per-slot saturating sum, for accumulating deltas across calls.
    #[inline]
    pub fn add(&self, other: &Self) -> Self {
        let mut out = Self::default();
        for i in 0..MAX_EVENTS {
            out.vals[i] = self.vals[i].saturating_add(other.vals[i]);
        }
        out
    }
}
