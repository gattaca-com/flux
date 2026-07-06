//! Counter source: open the [`Schema::local`](super::Schema::local) events
//! per thread via rdpmc, then [`read`] a snapshot to ride alongside each
//! mark.

use super::{PerfSample, Schema, raw::HwCounter, sample::MAX_EVENTS};

/// Per-thread counters, one slot per [`Schema`] entry (`None` where the
/// event couldn't be opened — over budget or unsupported).
struct Counters {
    opened: Vec<Option<HwCounter>>,
}

impl Counters {
    fn open() -> Option<Self> {
        let opened: Vec<_> =
            Schema::local().iter().map(|e| HwCounter::event(e.type_, e.config)).collect();
        opened.iter().any(Option::is_some).then_some(Self { opened })
    }

    #[inline]
    fn read(&self) -> PerfSample {
        let mut s = PerfSample::default();
        for (i, c) in self.opened.iter().enumerate().take(MAX_EVENTS) {
            if let Some(c) = c {
                s.vals[i] = c.read();
            }
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
