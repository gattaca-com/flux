//! The deliberate replay, included as the boundary case: a leaf obtains two
//! [`TickOutcome`]s in one tick, banks one, and replays it on the next tick
//! without touching the group at all.
//!
//! It takes three conscious steps — a second `maintain` call, a field to hold
//! the spare, a `take()` in place of the call — so unlike its siblings it is
//! hard to write by accident. It marks the outer edge of the class: however a
//! defence is built, this is the shape it must catch last, and if it cannot,
//! this is the shape the documentation must name as out of scope — the same
//! ruling readiness banking already has.
//!
//! The starved obligation is `maintain`: while the bank is replayed, queued
//! disconnects are not delivered and due reconnects are not attempted, for
//! every connection of the group. The freshness stamp fails the replayed
//! outcome's first read under debug assertions, because it carries the
//! instant of the tick that banked it; in a release build the transport
//! silently freezes for exactly the iterations the bank covers.

use flux_network::stream::{ConnectionGroupId, Deadline, ReadinessOutcome, Service, TickOutcome};
use flux_timing::Instant;
use mio::event::Event;

use crate::Leaf;

/// A leaf that ticks its group every *other* scheduler tick and replays the
/// banked outcome in between.
pub struct BankingLeaf {
    lower: Leaf,
    banked: Option<TickOutcome>,
}

impl BankingLeaf {
    pub fn new(lower: Leaf) -> Self {
        Self { lower, banked: None }
    }

    pub fn lower_mut(&mut self) -> &mut Leaf {
        &mut self.lower
    }
}

impl Service for BankingLeaf {
    fn group_id(&self) -> &ConnectionGroupId {
        self.lower.group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        self.lower.handle_event(event)
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        if let Some(banked) = self.banked.take() {
            // The group is not maintained this iteration: no disconnect
            // delivery, no reconnect attempt, and a did-work report describing
            // the previous tick.
            return banked;
        }
        let outcome = self.lower.tick(now);
        self.banked = Some(self.lower.tick(now));
        outcome
    }

    fn next_deadline(&self, now: Instant) -> Deadline {
        self.lower.next_deadline(now)
    }
}

#[cfg(test)]
mod tests {
    use flux_network::stream::{Endpoint, Service, StreamNetwork};
    use flux_timing::Instant;

    use super::BankingLeaf;
    use crate::{Leaf, raw_group};

    /// A minute past `at`: far enough that any scheduled retry is overdue.
    fn a_minute_past(at: Instant) -> Instant {
        Instant(at.0 + 60_000_000_000)
    }

    /// Under debug assertions the replayed outcome's read panics, which is the
    /// defence working; in release the final assertion pins the hazard: the
    /// replayed tick leaves the group's schedule exactly where it was, proving
    /// `maintain` never ran.
    #[test]
    #[cfg_attr(debug_assertions, should_panic(expected = "replayed an earlier answer"))]
    fn a_banked_tick_outcome_freezes_the_transport() {
        let mut net = StreamNetwork::default();
        let dir = tempfile::tempdir().unwrap();

        // The control: an honest leaf with a dead endpoint attempts the
        // overdue retry and reschedules it, so its deadline moves.
        let mut honest = Leaf::new(raw_group(&mut net, "honest"));
        let _token = honest.group_mut().connect(Endpoint::Unix(dir.path().join("nobody")));
        let fold = Instant::now();
        let scheduled = honest.next_deadline(fold).instant(fold).expect("the retry is scheduled");
        let overdue = a_minute_past(scheduled);
        assert!(honest.tick(overdue).worked(overdue), "an honest tick attempts the retry");
        let fold = Instant::now();
        let rescheduled =
            honest.next_deadline(fold).instant(fold).expect("the retry stays scheduled");
        assert!(rescheduled > scheduled, "an honest tick attempts the retry and reschedules it");

        // The banker: same endpoint shape, but its second tick replays the
        // bank instead of maintaining, so the overdue retry is never
        // attempted and the schedule is frozen at the old instant.
        let mut banker = BankingLeaf::new(Leaf::new(raw_group(&mut net, "banker")));
        let _token =
            banker.lower_mut().group_mut().connect(Endpoint::Unix(dir.path().join("nobody")));
        let first = Instant::now();
        let _ = banker.tick(first).worked(first);
        let fold = Instant::now();
        let frozen = banker.next_deadline(fold).instant(fold).expect("the retry is scheduled");

        // Under debug assertions this read is where the replay dies: the
        // banked outcome carries `first`, and this tick asks for `overdue`.
        let overdue = a_minute_past(frozen);
        let _ = banker.tick(overdue).worked(overdue);
        let fold = Instant::now();
        assert_eq!(
            banker.next_deadline(fold).instant(fold),
            Some(frozen),
            "the replayed tick never reached maintain: the overdue retry was not attempted"
        );
    }
}
