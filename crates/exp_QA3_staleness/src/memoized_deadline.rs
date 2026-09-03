//! The defensive replay: the author knows caching a deadline is risky, so the
//! cache invalidates on readiness — and still loses.
//!
//! The invalidation covers every path the author can see from inside the
//! `Service` impl: readiness arrives through `handle_event`, timers through
//! `tick`, and both refresh the cache. The path it cannot see is the
//! application's own handle on the group — an endpoint added, a close queued
//! between iterations — which raises no readiness and touches no method of
//! this composer. The hole is not sloppiness; it is that correct invalidation
//! requires knowing every writer of the group's schedule, and the group hands
//! writers out (`group_mut`) without telling the composer.
//!
//! Same class as [`stashed_deadline`](crate::stashed_deadline), one rung less
//! innocent, one rung more instructive: a review that catches the naive stash
//! plausibly waves this one through because it visibly "handles" staleness.

use std::cell::Cell;

use flux_network::stream::{ConnectionGroupId, Deadline, ReadinessOutcome, Service, TickOutcome};
use flux_timing::Instant;
use mio::event::Event;

use crate::Leaf;

/// A composer that memoizes its fold behind a readiness-driven dirty flag.
pub struct MemoizingComposer {
    lower: Leaf,
    stash: Cell<Option<Deadline>>,
    /// Set by readiness, cleared by the fold: the invalidation that only
    /// covers the paths the composer can observe.
    dirty: Cell<bool>,
}

impl MemoizingComposer {
    pub fn new(lower: Leaf) -> Self {
        Self { lower, stash: Cell::new(None), dirty: Cell::new(false) }
    }

    pub fn lower_mut(&mut self) -> &mut Leaf {
        &mut self.lower
    }

    /// The fresh answer the memo stands in for: the test's source of truth.
    pub fn fresh(&self, now: Instant) -> Deadline {
        self.lower.next_deadline(now)
    }
}

impl Service for MemoizingComposer {
    fn group_id(&self) -> &ConnectionGroupId {
        self.lower.group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        self.dirty.set(true);
        self.lower.handle_event(event)
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        let outcome = self.lower.tick(now);
        self.stash.set(Some(self.lower.next_deadline(now)));
        outcome
    }

    fn next_deadline(&self, now: Instant) -> Deadline {
        if self.dirty.replace(false) {
            self.stash.set(None);
        }
        self.stash.take().unwrap_or_else(|| self.lower.next_deadline(now))
    }
}

#[cfg(test)]
mod tests {
    use flux_network::stream::{Endpoint, Service, StreamNetwork};
    use flux_timing::Instant;

    use super::MemoizingComposer;
    use crate::{Leaf, raw_group};

    /// Under debug assertions the stale read panics, which is the defence
    /// working; in release the final assertion pins the hazard: the
    /// invalidation is sound for every path the composer observes, and the
    /// application's group handle is not one of them.
    #[test]
    #[cfg_attr(debug_assertions, should_panic(expected = "replayed an earlier answer"))]
    fn an_event_driven_invalidation_misses_an_application_side_connect() {
        let mut net = StreamNetwork::default();
        let mut composer = MemoizingComposer::new(Leaf::new(raw_group(&mut net, "memoized")));

        // One quiet iteration fills the memo with "nothing due"; no readiness
        // has arrived, so nothing marks it dirty.
        let _ = composer.tick(Instant::now());

        let dir = tempfile::tempdir().unwrap();
        let _token =
            composer.lower_mut().group_mut().connect(Endpoint::Unix(dir.path().join("nobody")));

        let fold = Instant::now();
        let truth = composer.fresh(fold).instant(fold);
        assert!(truth.is_some(), "the group schedules the retry the moment it exists");

        // Under debug assertions this read is where the replay dies: the stash
        // carries the tick's instant, and this fold asks for another.
        let folded = composer.next_deadline(fold).instant(fold);
        assert!(
            folded.is_none(),
            "the memo survives its own invalidation: the connect raised no readiness event"
        );
    }
}
