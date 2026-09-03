//! Demonstrations for the QA #3 staleness discussion: the bug class that
//! carrying the scheduling obligations in return types leaves open.
//!
//! Under the structural contract a `TickOutcome` or `Deadline` comes from a
//! [`ConnectionGroup`] or not at all, so *forgetting* an obligation does not
//! compile. Both are ordinary values, though: code that stashes one and
//! replays it on a later iteration compiles cleanly, and reports yesterday's
//! truth for today's transport. Each buggy module here is such a replay,
//! ordered from innocent-looking to deliberate. The freshness stamps close
//! the hole at test time: each witness carries the instant it was produced
//! for, and reading it for another instant panics under debug assertions, so
//! every replay here fails at its first stale read. The tests are
//! `should_panic` under debug assertions; in a release build, where the
//! stamps are not checked, they still pin the hazard's shape: the replay
//! compiles, runs and oversleeps.
//!
//! - [`correct`]: the contract as intended — a leaf and a composer that consult
//!   the group on every call. Their tests show what the buggy variants lose.
//! - [`stashed_deadline`]: a composer computes its deadline during `tick`,
//!   where it holds `&mut self`, and hands the stash out of `next_deadline`.
//!   The most innocent shape: it looks like moving work to the cheaper moment.
//! - [`memoized_deadline`]: the same stash behind an invalidation flag set by
//!   readiness — the author who knows caching is risky and still loses, because
//!   application-side group calls arrive through no readiness event.
//! - [`banked_tick`]: a leaf banks a second [`TickOutcome`] and replays it on
//!   the next tick, skipping `maintain` outright — the deliberate boundary
//!   case.
//!
//! Everything runs without sockets, polls or sleeps: connecting an endpoint
//! to a unix path nobody listens on fails inline and leaves the group holding
//! a real retry deadline, and `tick(now)` takes the clock as a parameter, so
//! a test fires timers by passing a later instant. Tests call the `Service`
//! methods directly, standing in for the scheduler's fold and tick.

pub mod banked_tick;
pub mod correct;
pub mod memoized_deadline;
pub mod stashed_deadline;

use flux_network::stream::{
    ConnectionGroup, ConnectionGroupConfig, ConnectionGroupId, Deadline, Framing, ReadinessOutcome,
    Service, StreamNetwork, TickOutcome,
};
use flux_timing::Instant;
use mio::event::Event;

/// An honest leaf: every obligation reaches the group, every call.
///
/// The buggy variants wrap or imitate this leaf; their tests use its fresh
/// answers as the truth to diverge from.
pub struct Leaf {
    group: ConnectionGroup,
}

impl Leaf {
    pub fn new(group: ConnectionGroup) -> Self {
        Self { group }
    }

    /// The application's handle for sends, closes and endpoint changes — the
    /// calls that can make the group due between scheduler iterations.
    pub fn group_mut(&mut self) -> &mut ConnectionGroup {
        &mut self.group
    }
}

impl Service for Leaf {
    fn group_id(&self) -> &ConnectionGroupId {
        self.group.group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        self.group.handle_event(event, &mut |_| {})
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        self.group.maintain(now, &mut |_| {})
    }

    fn next_deadline(&self, now: Instant) -> Deadline {
        self.group.next_deadline(now)
    }
}

/// A raw-framed group on `net`, named for the test that owns it.
pub fn raw_group(net: &mut StreamNetwork, name: &'static str) -> ConnectionGroup {
    net.add_group(ConnectionGroupConfig {
        name,
        framing: Framing::Raw,
        ..ConnectionGroupConfig::default()
    })
}
