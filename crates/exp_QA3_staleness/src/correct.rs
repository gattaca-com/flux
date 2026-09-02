//! The contract as intended: the group is consulted on every call.
//!
//! A deadline that arises between scheduler iterations is therefore seen by
//! the very next fold. The buggy modules diverge from exactly this behaviour.

use flux_network::stream::{ConnectionGroupId, Deadline, ReadinessOutcome, Service, TickOutcome};
use flux_timing::Instant;
use mio::event::Event;

use crate::Leaf;

/// A composer with one protocol timer of its own.
///
/// It delegates every obligation to its leaf fresh, widens the tick's work by
/// its own, and folds its timer in with `earliest` — the shape the trait
/// documentation asks for.
pub struct Composer {
    lower: Leaf,
    timer: Option<Instant>,
}

impl Composer {
    pub fn new(lower: Leaf, timer: Option<Instant>) -> Self {
        Self { lower, timer }
    }

    pub fn lower_mut(&mut self) -> &mut Leaf {
        &mut self.lower
    }
}

impl Service for Composer {
    fn group_id(&self) -> &ConnectionGroupId {
        self.lower.group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        self.lower.handle_event(event)
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        let outcome = self.lower.tick(now);
        let fired = self.timer.is_some_and(|timer| timer <= now);
        if fired {
            self.timer = None;
        }
        outcome.or_worked(fired)
    }

    fn next_deadline(&self) -> Deadline {
        self.lower.next_deadline().earliest(self.timer)
    }
}

#[cfg(test)]
mod tests {
    use flux_network::stream::{Endpoint, Service, StreamNetwork};
    use flux_timing::Instant;

    use super::Composer;
    use crate::{Leaf, raw_group};

    #[test]
    fn an_endpoint_added_between_iterations_is_scheduled() {
        let mut net = StreamNetwork::default();
        let mut leaf = Leaf::new(raw_group(&mut net, "correct-leaf"));
        assert!(leaf.next_deadline().instant().is_none(), "an idle group has nothing due");

        // Between iterations the application adds an outbound endpoint nobody
        // listens on: the inline connect fails and the retry is the group's
        // deadline from this moment on.
        let dir = tempfile::tempdir().unwrap();
        let _token = leaf.group_mut().connect(Endpoint::Unix(dir.path().join("nobody")));

        assert!(leaf.next_deadline().instant().is_some(), "the very next fold schedules the retry");
    }

    #[test]
    fn a_composer_folds_fresh_and_keeps_the_earlier_instant() {
        let mut net = StreamNetwork::default();
        let leaf = Leaf::new(raw_group(&mut net, "correct-composer"));
        // A protocol timer far enough out that the transport retry, once it
        // exists, is the earlier of the two.
        let far_timer = Instant(Instant::now().0 + 3_600_000_000_000);
        let mut composer = Composer::new(leaf, Some(far_timer));
        assert_eq!(
            composer.next_deadline().instant(),
            Some(far_timer),
            "with an idle group the composer's own timer is the deadline"
        );

        let dir = tempfile::tempdir().unwrap();
        let _token =
            composer.lower_mut().group_mut().connect(Endpoint::Unix(dir.path().join("nobody")));

        let folded = composer.next_deadline().instant().expect("something is due");
        assert!(folded < far_timer, "the fresh fold brings the retry forward past the timer");
    }
}
