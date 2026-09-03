//! flux tests its own services against the freshness half of the scheduling
//! contract, and pins that the stamps catch a replay.
//!
//! Freshness needs no sockets: two quiet iterations with distinct instants,
//! each reading the fold and the tick for the instant it asked with. Any
//! answer replayed from the first iteration carries the first instant and
//! fails the second read under debug assertions. The replaying services here
//! are the shapes a composer can write by accident; their tests are
//! `should_panic` under debug assertions and, where the stamps are not
//! checked, pin the hazard's shape instead.

mod common;

use std::cell::Cell;

use common::{RawService, RelayService};
use flux_network::{
    http::{HttpConfig, HttpService},
    stream::{
        ConnectionGroupConfig, ConnectionGroupId, Deadline, Framing, ReadinessOutcome, Service,
        StreamEvent, StreamNetwork, StreamService, StreamSink, TickOutcome,
    },
};
use flux_timing::{Duration, Instant};
use mio::event::Event;

fn raw(name: &'static str) -> ConnectionGroupConfig {
    ConnectionGroupConfig { name, framing: Framing::Raw, ..ConnectionGroupConfig::default() }
}

/// The minimal freshness probe from the `Service` documentation: two quiet
/// iterations, strictly increasing instants, every witness read for the
/// instant it was asked with.
fn two_quiet_iterations<S: Service>(service: &mut S) {
    let base = Instant::now();
    for step in 1..=2 {
        let now = base + Duration::from_millis(step);
        let _ = service.next_deadline(now).instant(now);
        let _ = service.tick(now).worked(now);
    }
}

/// A sink that consumes in place.
struct Counting(usize);

impl StreamSink for Counting {
    fn on_event(&mut self, event: StreamEvent<'_>) {
        if let StreamEvent::Message { payload, .. } = event {
            self.0 += payload.len();
        }
    }

    fn has_pending(&self) -> bool {
        false
    }
}

#[test]
fn the_shipped_services_answer_fresh_on_every_iteration() {
    let mut net = StreamNetwork::default();
    let mut http = HttpService::new(net.add_group(raw("http")), HttpConfig::default());
    two_quiet_iterations(&mut http);

    let mut retained = StreamService::new(net.add_group(raw("retained")));
    two_quiet_iterations(&mut retained);

    let mut sink = StreamService::with_sink(net.add_group(raw("sink")), Counting(0));
    two_quiet_iterations(&mut sink);

    let mut leaf = RawService::new(net.add_group(raw("leaf")));
    two_quiet_iterations(&mut leaf);

    let lower = RawService::new(net.add_group(raw("relay")));
    let mut relay = RelayService::new(lower, 1);
    two_quiet_iterations(&mut relay);
}

#[test]
fn a_composer_that_folds_fresh_passes_with_its_own_timer_armed_or_not() {
    /// Delegates every call fresh and folds one timer of its own.
    struct Composer {
        lower: RawService,
        timer: Option<Instant>,
    }
    impl Service for Composer {
        fn group_id(&self) -> &ConnectionGroupId {
            self.lower.group_id()
        }
        fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
            self.lower.handle_event(event)
        }
        fn tick(&mut self, now: Instant) -> TickOutcome {
            let fired = self.timer.is_some_and(|timer| timer <= now);
            self.lower.tick(now).or_worked(fired)
        }
        fn next_deadline(&self, now: Instant) -> Deadline {
            self.lower.next_deadline(now).earliest(self.timer)
        }
    }

    let mut net = StreamNetwork::default();
    let far = Some(Instant::now() + Duration::from_secs(3600));
    for timer in [None, far] {
        let lower = RawService::new(net.add_group(raw("composer")));
        let mut composer = Composer { lower, timer };
        two_quiet_iterations(&mut composer);
    }
}

/// The innocent replay: a deadline folded during the tick, served at the
/// next fold.
struct Stashing {
    lower: RawService,
    stash: Cell<Option<Deadline>>,
}

impl Service for Stashing {
    fn group_id(&self) -> &ConnectionGroupId {
        self.lower.group_id()
    }
    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        self.lower.handle_event(event)
    }
    fn tick(&mut self, now: Instant) -> TickOutcome {
        let outcome = self.lower.tick(now);
        self.stash.set(Some(self.lower.next_deadline(now)));
        outcome
    }
    fn next_deadline(&self, now: Instant) -> Deadline {
        self.stash.take().unwrap_or_else(|| self.lower.next_deadline(now))
    }
}

#[test]
#[cfg_attr(debug_assertions, should_panic(expected = "the fold replayed an earlier answer"))]
fn a_stashed_deadline_fails_its_first_stale_read() {
    let mut net = StreamNetwork::default();
    let lower = RawService::new(net.add_group(raw("stashing")));
    let mut stashing = Stashing { lower, stash: Cell::new(None) };
    two_quiet_iterations(&mut stashing);
}

/// The deliberate replay: a tick outcome banked and returned next tick.
struct Banking {
    lower: RawService,
    banked: Option<TickOutcome>,
}

impl Service for Banking {
    fn group_id(&self) -> &ConnectionGroupId {
        self.lower.group_id()
    }
    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        self.lower.handle_event(event)
    }
    fn tick(&mut self, now: Instant) -> TickOutcome {
        if let Some(banked) = self.banked.take() {
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

#[test]
#[cfg_attr(debug_assertions, should_panic(expected = "the tick replayed an earlier answer"))]
fn a_banked_tick_outcome_fails_its_first_stale_read() {
    let mut net = StreamNetwork::default();
    let lower = RawService::new(net.add_group(raw("banking")));
    let mut banking = Banking { lower, banked: None };
    two_quiet_iterations(&mut banking);
}

#[test]
fn the_stamp_is_a_token_not_a_filter() {
    // A deadline already in the past is still reported for the fold that
    // asked: `now` only has to match the instant the group was asked with.
    let mut net = StreamNetwork::default();
    let mut leaf = RawService::new(net.add_group(raw("token")));
    let past = Instant(1);
    leaf.set_deadline(Some(past));
    let fold = Instant::now();
    assert_eq!(leaf.next_deadline(fold).instant(fold), Some(past));
}
