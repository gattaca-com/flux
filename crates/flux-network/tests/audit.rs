//! The obligation audits, driven through the real scheduler by services built
//! on nothing but the public interface.
//!
//! Each test gives the network a leaf whose obligations are switches: honest
//! by default, and omitted one at a time to prove the audit catches exactly
//! that omission, with the message naming the group and the obligation. The
//! freshness tests prove an honest iteration leaves nothing behind that could
//! excuse a later omission, in both poll modes.

use flux_network::{
    Token,
    stream::{
        ConnectionGroup, ConnectionGroupConfig, ConnectionGroupId, Endpoint, Framing,
        ReadinessOutcome, Service, StreamNetwork,
    },
};
use flux_timing::{Duration, Instant};
use mio::Poll;

/// How a leaf treats the deadline obligation.
#[derive(Clone, Copy)]
enum Deadline {
    /// Fold the group's deadline, as the contract requires.
    Fold,
    /// Report without consulting the group.
    Omit,
    /// Consult the group, then report an instant after what it said.
    Later,
}

/// A leaf whose obligations are switches.
struct Leaf {
    group: ConnectionGroup,
    maintain: bool,
    deadline: Deadline,
}

impl Leaf {
    fn new(group: ConnectionGroup) -> Self {
        Self { group, maintain: true, deadline: Deadline::Fold }
    }
}

impl Service for Leaf {
    fn group_id(&self) -> &ConnectionGroupId {
        self.group.group_id()
    }

    fn handle_event(&mut self, event: &mio::event::Event) -> ReadinessOutcome {
        self.group.handle_event(event, &mut |_| {})
    }

    fn tick(&mut self, now: Instant) -> bool {
        if self.maintain { self.group.maintain(now, &mut |_| {}) } else { false }
    }

    fn next_deadline(&self) -> Option<Instant> {
        match self.deadline {
            Deadline::Fold => self.group.next_deadline(),
            Deadline::Omit => None,
            Deadline::Later => {
                let group = self.group.next_deadline().expect("the group has a deadline to miss");
                Some(Instant(group.0 + 1))
            }
        }
    }
}

fn raw_group(net: &mut StreamNetwork, name: &'static str) -> ConnectionGroup {
    net.add_group(ConnectionGroupConfig {
        name,
        framing: Framing::Raw,
        ..ConnectionGroupConfig::default()
    })
}

/// An External-mode network on a poll the test owns, and an honest leaf on it.
fn external(name: &'static str) -> (Poll, StreamNetwork, Leaf) {
    let poll = Poll::new().unwrap();
    let mut net = StreamNetwork::with_registry(poll.registry().try_clone().unwrap(), Token(1024));
    let leaf = Leaf::new(raw_group(&mut net, name));
    (poll, net, leaf)
}

// ---------------------------------------------------------------------------
// The three panics, through both modes' entry points

#[test]
#[should_panic(expected = "connection group 0 ticked without reaching ConnectionGroup::maintain")]
fn a_drive_whose_tick_skips_maintain_panics() {
    let mut net = StreamNetwork::default();
    let mut leaf = Leaf::new(raw_group(&mut net, "skips-maintain"));
    leaf.maintain = false;

    let _ = net.drive(Some(Duration::ZERO), &mut [&mut leaf]);
}

#[test]
#[should_panic(expected = "connection group 0's service reported its deadline without reaching \
                ConnectionGroup::next_deadline")]
fn a_drive_whose_fold_skips_the_group_panics() {
    let mut net = StreamNetwork::default();
    let mut leaf = Leaf::new(raw_group(&mut net, "skips-fold"));
    leaf.deadline = Deadline::Omit;

    let _ = net.drive(Some(Duration::ZERO), &mut [&mut leaf]);
}

#[test]
#[should_panic(expected = "connection group 0 needs a tick by")]
fn a_root_deadline_later_than_its_groups_panics() {
    let dir = tempfile::tempdir().unwrap();
    let mut net = StreamNetwork::default();
    let mut group = raw_group(&mut net, "reports-later");
    // Nothing listens at the endpoint, so the group is left holding a real
    // deadline — the retry — for the leaf to miss.
    let _endpoint = group.connect(Endpoint::Unix(dir.path().join("nobody")));
    let mut leaf = Leaf::new(group);
    leaf.deadline = Deadline::Later;

    let _ = net.drive(Some(Duration::ZERO), &mut [&mut leaf]);
}

#[test]
#[should_panic(expected = "connection group 0 ticked without reaching ConnectionGroup::maintain")]
fn an_external_tick_that_skips_maintain_panics() {
    let (_poll, mut net, mut leaf) = external("skips-maintain");
    leaf.maintain = false;

    let _ = net.tick(&mut [&mut leaf]);
}

#[test]
#[should_panic(expected = "connection group 0's service reported its deadline without reaching \
                ConnectionGroup::next_deadline")]
fn an_external_fold_that_skips_the_group_panics() {
    let (_poll, net, mut leaf) = external("skips-fold");
    leaf.deadline = Deadline::Omit;

    let _ = net.next_deadline(&[&mut leaf]);
}

// ---------------------------------------------------------------------------
// Freshness: an honest iteration excuses nothing later

#[test]
#[should_panic(expected = "connection group 0 ticked without reaching ConnectionGroup::maintain")]
fn an_honest_drive_does_not_excuse_a_skipped_maintain() {
    let mut net = StreamNetwork::default();
    let mut leaf = Leaf::new(raw_group(&mut net, "honest-then-not"));
    assert!(!net.drive(Some(Duration::ZERO), &mut [&mut leaf]), "the honest iteration is idle");

    leaf.maintain = false;
    let _ = net.drive(Some(Duration::ZERO), &mut [&mut leaf]);
}

#[test]
#[should_panic(expected = "connection group 0's service reported its deadline without reaching \
                ConnectionGroup::next_deadline")]
fn an_honest_drive_does_not_excuse_a_skipped_fold() {
    let mut net = StreamNetwork::default();
    let mut leaf = Leaf::new(raw_group(&mut net, "honest-then-not"));
    assert!(!net.drive(Some(Duration::ZERO), &mut [&mut leaf]), "the honest iteration is idle");

    leaf.deadline = Deadline::Omit;
    let _ = net.drive(Some(Duration::ZERO), &mut [&mut leaf]);
}

#[test]
#[should_panic(expected = "connection group 0 ticked without reaching ConnectionGroup::maintain")]
fn an_honest_external_iteration_does_not_excuse_a_skipped_maintain() {
    let (_poll, mut net, mut leaf) = external("honest-then-not");
    let _ = net.next_deadline(&[&mut leaf]);
    assert!(!net.tick(&mut [&mut leaf]), "the honest iteration is idle");

    leaf.maintain = false;
    let _ = net.tick(&mut [&mut leaf]);
}

#[test]
#[should_panic(expected = "connection group 0's service reported its deadline without reaching \
                ConnectionGroup::next_deadline")]
fn an_honest_external_iteration_does_not_excuse_a_skipped_fold() {
    let (_poll, mut net, mut leaf) = external("honest-then-not");
    let _ = net.next_deadline(&[&mut leaf]);
    assert!(!net.tick(&mut [&mut leaf]), "the honest iteration is idle");

    leaf.deadline = Deadline::Omit;
    let _ = net.next_deadline(&[&mut leaf]);
}

// ---------------------------------------------------------------------------
// The ruled exemption

/// A permanently nonblocking External caller never folds a deadline, and no
/// audit fires: the deadline audit runs only when something asks for one, and
/// a caller that cannot sleep cannot sleep through a deadline.
#[test]
fn an_external_caller_that_never_folds_is_not_audited_for_deadlines() {
    let (_poll, mut net, mut leaf) = external("tick-only");
    for _ in 0..3 {
        assert!(!net.tick(&mut [&mut leaf]), "an idle tick reports no work");
    }
}
