//! Counter source: open the [`Schema::local`](super::Schema::local) events
//! per thread via rdpmc, then [`read`] a snapshot to ride alongside each
//! mark.

use super::{PerfSample, Schema, raw::HwCounter};

/// Per-thread counters that opened, each paired with its [`Schema`] slot
/// (index into [`PerfSample::vals`]). Events that couldn't open — over budget
/// or unsupported — are dropped here rather than left as holes.
struct Counters {
    opened: Vec<(usize, HwCounter)>,
}

impl Counters {
    fn open() -> Option<Self> {
        let opened: Vec<_> = Schema::local()
            .iter()
            .enumerate()
            .filter_map(|(i, e)| Some((i, HwCounter::event(e.type_, e.config)?)))
            .collect();
        (!opened.is_empty()).then_some(Self { opened })
    }

    #[inline]
    fn read(&self) -> PerfSample {
        let mut s = PerfSample::default();
        // Schema::local limited to MAX_EVENTS, so write is safe
        for (i, c) in &self.opened {
            s.vals[*i] = c.read();
        }
        s
    }
}

thread_local! {
    /// Perf pid=0 events bind to the opening thread. None when
    /// perf_event_open is unavailable.
    static COUNTERS: Option<Counters> = Counters::open();
}

#[inline]
pub(crate) fn read() -> Option<PerfSample> {
    COUNTERS.with(|c| c.as_ref().map(Counters::read))
}
