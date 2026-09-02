//! The innocent replay: compute the deadline during `tick`, where `&mut self`
//! makes it convenient, and hand the stash out of `next_deadline`.
//!
//! This looks like an optimisation — the fold becomes a field read — and it
//! type-checks because a [`Deadline`] is an ordinary value. What it actually
//! does is answer the scheduler's question "what is due *now*?" with the truth
//! from the last tick. Anything the application did to the group since —
//! adding an endpoint, queueing a close — arrived through no readiness event
//! and moved no timer the composer knows about, so the stash hides it and an
//! Owned `drive(None)` sleeps on a deadline that no longer exists.
//!
//! The audit on the refactor branch panics on this composer's first stale
//! answer, because the reported instant diverges from what the group records
//! having said. Here it compiles, runs and oversleeps.

use std::cell::Cell;

use flux_network::stream::{ConnectionGroupId, Deadline, ReadinessOutcome, Service, TickOutcome};
use flux_timing::Instant;
use mio::event::Event;

use crate::Leaf;

/// A composer that folds at tick time and serves the stash at fold time.
pub struct StashingComposer {
    lower: Leaf,
    /// Yesterday's fold, replayed as today's answer. `Cell` because
    /// `next_deadline` takes `&self` — the interior mutability is the first
    /// smell of the shape, and rustc accepts it without complaint.
    stash: Cell<Option<Deadline>>,
}

impl StashingComposer {
    pub fn new(lower: Leaf) -> Self {
        Self { lower, stash: Cell::new(None) }
    }

    pub fn lower_mut(&mut self) -> &mut Leaf {
        &mut self.lower
    }

    /// The fresh answer the stash stands in for: the test's source of truth.
    pub fn fresh(&self) -> Deadline {
        self.lower.next_deadline()
    }
}

impl Service for StashingComposer {
    fn group_id(&self) -> &ConnectionGroupId {
        self.lower.group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        self.lower.handle_event(event)
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        let outcome = self.lower.tick(now);
        // "The deadline only changes when the transport does something, and
        // the transport just did it" — false: the application mutates the
        // group between iterations too.
        self.stash.set(Some(self.lower.next_deadline()));
        outcome
    }

    fn next_deadline(&self) -> Deadline {
        // Fresh on the first fold, replayed ever after: the fallback makes
        // the shape look robust while the stash makes it wrong.
        self.stash.take().unwrap_or_else(|| self.lower.next_deadline())
    }
}

#[cfg(test)]
mod tests {
    use flux_network::stream::{Endpoint, Service, StreamNetwork};
    use flux_timing::Instant;

    use super::StashingComposer;
    use crate::{Leaf, raw_group};

    /// Pins the hazard, not desired behaviour: a defence that closes the
    /// staleness hole should flip the final assertion or reject the composer
    /// at compile time.
    #[test]
    fn a_stashed_deadline_sleeps_through_a_new_endpoint() {
        let mut net = StreamNetwork::default();
        let mut composer = StashingComposer::new(Leaf::new(raw_group(&mut net, "stashed")));

        // One quiet iteration: the tick stashes "nothing due".
        let _ = composer.tick(Instant::now());

        // Between iterations the application adds an endpoint nobody listens
        // on; the group is now holding the retry deadline.
        let dir = tempfile::tempdir().unwrap();
        let _token =
            composer.lower_mut().group_mut().connect(Endpoint::Unix(dir.path().join("nobody")));

        let truth = composer.fresh().instant();
        assert!(truth.is_some(), "the group schedules the retry the moment it exists");

        let folded = composer.next_deadline().instant();
        assert!(
            folded.is_none(),
            "the stale stash hides the retry: an Owned drive(None) sleeps forever on this answer"
        );
    }
}
