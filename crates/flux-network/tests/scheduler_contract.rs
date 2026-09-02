//! The scheduling contract's ordering and reporting guarantees, pinned
//! through the real driver.

mod common;

use std::{
    cell::RefCell,
    io::Write,
    net::{Ipv4Addr, SocketAddr, TcpStream},
    rc::Rc,
    time::{Duration as StdDuration, Instant as StdInstant},
};

use common::{RawService, RelayService};
use flux_network::{
    Token,
    http::{HttpConfig, HttpEvent, HttpService},
    stream::{
        ConnectionGroup, ConnectionGroupConfig, ConnectionGroupId, Deadline, Endpoint, Framing,
        ReadinessOutcome, Service, StreamEvent, StreamNetwork, TickOutcome,
    },
};
use flux_timing::{Duration, Instant};
use mio::{Events, Poll, event::Event};

const TIMEOUT: StdDuration = StdDuration::from_secs(10);
const TOKEN_BASE: Token = Token(1000);

fn raw_config(name: &'static str) -> ConnectionGroupConfig {
    ConnectionGroupConfig {
        name,
        framing: Framing::Raw,
        max_frame_size: usize::MAX,
        backlog_warn_bytes: None,
        ..ConnectionGroupConfig::default()
    }
}

fn ephemeral() -> Endpoint {
    Endpoint::Tcp((Ipv4Addr::LOCALHOST, 0).into())
}

fn bound_addr(bound: Endpoint) -> SocketAddr {
    match bound {
        Endpoint::Tcp(addr) => addr,
        Endpoint::Unix(path) => panic!("a TCP listener bound {}", path.display()),
    }
}

fn client_at(addr: SocketAddr) -> TcpStream {
    let client = TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client
}

// ---------------------------------------------------------------------------
// The single post-readiness tick routes transport work before protocol timers

/// A leaf that logs its tick's two phases: the transport events its
/// maintenance routes, then its own timers.
struct PhaseLog {
    group: ConnectionGroup,
    accepted: Option<Token>,
    log: Vec<&'static str>,
}

impl PhaseLog {
    fn new(group: ConnectionGroup) -> Self {
        Self { group, accepted: None, log: Vec::new() }
    }
}

impl Service for PhaseLog {
    fn group_id(&self) -> &ConnectionGroupId {
        self.group.group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        let Self { group, accepted, .. } = self;
        group.handle_event(event, &mut |event| {
            if let StreamEvent::Accepted { token, .. } = event {
                *accepted = Some(token);
            }
        })
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        let Self { group, log, .. } = self;
        let worked = group.maintain(now, &mut |event| {
            if let StreamEvent::Disconnected { .. } = event {
                log.push("transport");
            }
        });
        log.push("timers");
        worked
    }

    fn next_deadline(&self) -> Deadline {
        self.group.next_deadline()
    }
}

/// Accepts one client on `leaf`'s listener, driving the Owned network.
fn accept_one(net: &mut StreamNetwork, leaf: &mut PhaseLog, addr: SocketAddr) -> TcpStream {
    let client = client_at(addr);
    let deadline = StdInstant::now() + TIMEOUT;
    while leaf.accepted.is_none() {
        assert!(StdInstant::now() < deadline, "the client was never accepted");
        let _ = net.drive(Some(Duration::from_millis(1)), &mut [&mut *leaf]);
    }
    client
}

#[test]
fn a_maintenance_disconnect_reaches_the_service_before_its_timers() {
    let mut net = StreamNetwork::default();
    let mut leaf = PhaseLog::new(net.add_group(raw_config("cadence")));
    let addr = bound_addr(leaf.group.listen(ephemeral()).unwrap());
    let _client = accept_one(&mut net, &mut leaf, addr);
    let token = leaf.accepted.unwrap();

    // The disconnect is queued between iterations; the next drive's one tick
    // must deliver it through maintenance before the leaf's timers run.
    assert!(leaf.group.disconnect(token));
    leaf.log.clear();
    assert!(net.drive(Some(Duration::ZERO), &mut [&mut leaf]), "the disconnect is work");
    assert_eq!(
        leaf.log,
        ["transport", "timers"],
        "one tick, transport routing first, timers second"
    );
}

#[test]
fn an_external_tick_delivers_a_maintenance_disconnect_before_its_timers() {
    let poll = Poll::new().unwrap();
    let mut net = StreamNetwork::with_registry(poll.registry().try_clone().unwrap(), TOKEN_BASE);
    let mut leaf = PhaseLog::new(net.add_group(raw_config("cadence")));
    let addr = bound_addr(leaf.group.listen(ephemeral()).unwrap());

    let mut poll = poll;
    let mut events = Events::with_capacity(64);
    let _client = client_at(addr);
    let deadline = StdInstant::now() + TIMEOUT;
    while leaf.accepted.is_none() {
        assert!(StdInstant::now() < deadline, "the client was never accepted");
        poll.poll(&mut events, Some(StdDuration::from_millis(1))).unwrap();
        for event in &events {
            let _ = net.handle_event(event, &mut [&mut leaf]);
        }
        let _ = net.tick(&mut [&mut leaf]);
    }
    let token = leaf.accepted.unwrap();

    assert!(leaf.group.disconnect(token));
    leaf.log.clear();
    assert!(net.tick(&mut [&mut leaf]), "the disconnect is work");
    assert_eq!(
        leaf.log,
        ["transport", "timers"],
        "one tick, transport routing first, timers second"
    );
}

// ---------------------------------------------------------------------------
// Readiness precedes the tick, and there is exactly one tick per iteration

/// A leaf that logs every scheduling call made on it.
struct CallLog {
    group: ConnectionGroup,
    log: Vec<&'static str>,
}

impl Service for CallLog {
    fn group_id(&self) -> &ConnectionGroupId {
        self.group.group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        self.log.push("event");
        self.group.handle_event(event, &mut |_| {})
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        let worked = self.group.maintain(now, &mut |_| {});
        self.log.push("tick");
        worked
    }

    fn next_deadline(&self) -> Deadline {
        self.group.next_deadline()
    }
}

#[test]
fn readiness_is_delivered_before_the_iterations_one_tick() {
    let mut net = StreamNetwork::default();
    let mut leaf = CallLog { group: net.add_group(raw_config("readiness-first")), log: Vec::new() };
    let addr = bound_addr(leaf.group.listen(ephemeral()).unwrap());
    let _client = client_at(addr);

    // The connect is in flight, so some drive's poll returns its readiness;
    // within that drive, every event precedes the single tick.
    let deadline = StdInstant::now() + TIMEOUT;
    loop {
        assert!(StdInstant::now() < deadline, "the listener never became readable");
        leaf.log.clear();
        let _ = net.drive(Some(Duration::from_millis(1)), &mut [&mut leaf]);
        if leaf.log.contains(&"event") {
            break;
        }
    }
    assert_eq!(leaf.log.last(), Some(&"tick"), "the tick follows readiness");
    assert_eq!(
        leaf.log.iter().filter(|call| **call == "tick").count(),
        1,
        "one iteration runs one tick"
    );
    assert!(
        leaf.log[..leaf.log.len() - 1].iter().all(|call| *call == "event"),
        "every event precedes the tick: {:?}",
        leaf.log
    );
}

// ---------------------------------------------------------------------------
// A composer's tick consumes lower events before its own timers

/// A composer with an observable timer phase: its tick delegates, drains the
/// leaf, and only then runs timers of its own, logging both phases.
struct TimeredComposer {
    lower: RawService,
    log: Vec<&'static str>,
}

impl Service for TimeredComposer {
    fn group_id(&self) -> &ConnectionGroupId {
        self.lower.group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        self.lower.handle_event(event)
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        let Self { lower, log } = self;
        let worked = lower.tick(now);
        let leftovers = lower.spin(usize::MAX, |event| match event {
            common::RawEvent::Message { .. } => log.push("lower-event"),
        });
        assert!(!leftovers, "an unbounded drain leaves nothing");
        log.push("timers");
        worked
    }

    fn next_deadline(&self) -> Deadline {
        self.lower.next_deadline()
    }
}

#[test]
fn a_composers_tick_consumes_lower_events_before_its_timers() {
    let mut net = StreamNetwork::default();
    let mut raw = RawService::new(net.add_group(raw_config("timered")));
    let addr = bound_addr(raw.listen(ephemeral()).unwrap());
    let mut composer = TimeredComposer { lower: raw, log: Vec::new() };

    let mut client = client_at(addr);
    let deadline = StdInstant::now() + TIMEOUT;
    while composer.lower.accepted().is_none() {
        assert!(StdInstant::now() < deadline, "the client was never accepted");
        let _ = net.drive(Some(Duration::from_millis(1)), &mut [&mut composer]);
    }

    client.write_all(b"payload").unwrap();
    loop {
        assert!(StdInstant::now() < deadline, "the payload never arrived");
        composer.log.clear();
        let _ = net.drive(Some(Duration::from_millis(1)), &mut [&mut composer]);
        if composer.log.contains(&"lower-event") {
            break;
        }
    }
    assert_eq!(composer.log.last(), Some(&"timers"), "the timers close the tick");
    assert!(
        composer.log[..composer.log.len() - 1].iter().all(|call| *call == "lower-event"),
        "every lower event is consumed before the timers run: {:?}",
        composer.log
    );
}

// ---------------------------------------------------------------------------
// Validation: a duplicate identity is presentable in safe code, and rejected

/// A degenerate service that borrows an identity it does not own: the
/// cheapest safe way to present one group twice. Its hooks are unreachable
/// wherever full validation runs first.
struct Alias<'a>(&'a ConnectionGroupId);

impl Service for Alias<'_> {
    fn group_id(&self) -> &ConnectionGroupId {
        self.0
    }

    fn handle_event(&mut self, _: &Event) -> ReadinessOutcome {
        unreachable!("a routable duplicate diverges: it cannot construct an outcome")
    }

    fn tick(&mut self, _: Instant) -> TickOutcome {
        unreachable!("validation rejects the duplicate before any tick")
    }

    fn next_deadline(&self) -> Deadline {
        unreachable!("validation rejects the duplicate before any fold")
    }
}

#[test]
#[should_panic(expected = "duplicate service for group 0")]
fn a_duplicate_identity_is_rejected_before_any_work() {
    let poll = Poll::new().unwrap();
    let mut net = StreamNetwork::with_registry(poll.registry().try_clone().unwrap(), TOKEN_BASE);
    let group = net.add_group(raw_config("aliased"));

    let _ = net.tick(&mut [Alias(group.group_id()), Alias(group.group_id())]);
}

/// The window the per-event linear check leaves open, pinned so a change is
/// visible: `handle_event` checks identity and liveness per event and leaves
/// the pairwise uniqueness scan to the once-per-iteration calls, so a
/// duplicate can be offered an event first — where it diverges, because safe
/// code cannot construct the outcome. This pins the documented rejection
/// window, not a right to route duplicates; restoring full validation per
/// event changes the panic back to "duplicate service" and fails this test.
#[test]
#[should_panic(expected = "a routable duplicate diverges")]
fn an_events_linear_check_leaves_the_duplicate_for_the_iterations_validation() {
    let mut poll = Poll::new().unwrap();
    let mut net = StreamNetwork::with_registry(poll.registry().try_clone().unwrap(), TOKEN_BASE);
    let mut group = net.add_group(raw_config("aliased"));
    let addr = bound_addr(group.listen(ephemeral()).unwrap());
    let _client = client_at(addr);

    let mut events = Events::with_capacity(4);
    let deadline = StdInstant::now() + TIMEOUT;
    loop {
        assert!(StdInstant::now() < deadline, "the listener never became readable");
        poll.poll(&mut events, Some(StdDuration::from_millis(1))).unwrap();
        for event in &events {
            let _ =
                net.handle_event(event, &mut [Alias(group.group_id()), Alias(group.group_id())]);
        }
    }
}

// ---------------------------------------------------------------------------
// Slice order controls tick order, never routing

/// A leaf that logs its ticks into a log the test shares between services.
struct Ordered {
    group: ConnectionGroup,
    name: &'static str,
    accepted: Option<Token>,
    log: Rc<RefCell<Vec<&'static str>>>,
}

impl Service for Ordered {
    fn group_id(&self) -> &ConnectionGroupId {
        self.group.group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        let Self { group, accepted, .. } = self;
        group.handle_event(event, &mut |event| {
            if let StreamEvent::Accepted { token, .. } = event {
                *accepted = Some(token);
            }
        })
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        let worked = self.group.maintain(now, &mut |_| {});
        self.log.borrow_mut().push(self.name);
        worked
    }

    fn next_deadline(&self) -> Deadline {
        self.group.next_deadline()
    }
}

#[test]
fn slice_order_controls_tick_order_but_not_routing() {
    let log = Rc::new(RefCell::new(Vec::new()));
    let mut net = StreamNetwork::default();
    let mut first = Ordered {
        group: net.add_group(raw_config("first")),
        name: "first",
        accepted: None,
        log: Rc::clone(&log),
    };
    let mut second = Ordered {
        group: net.add_group(raw_config("second")),
        name: "second",
        accepted: None,
        log: Rc::clone(&log),
    };
    let addr = bound_addr(second.group.listen(ephemeral()).unwrap());

    let _ = net.drive(Some(Duration::ZERO), &mut [&mut first, &mut second]);
    assert_eq!(*log.borrow(), ["first", "second"], "ticks run in slice order");
    log.borrow_mut().clear();
    let _ = net.drive(Some(Duration::ZERO), &mut [&mut second, &mut first]);
    assert_eq!(*log.borrow(), ["second", "first"], "the reversed slice reverses the ticks");

    // The event belongs to the second service's listener; putting the first
    // service ahead of it in the slice must not hand it the event.
    let _client = client_at(addr);
    let deadline = StdInstant::now() + TIMEOUT;
    while second.accepted.is_none() {
        assert!(StdInstant::now() < deadline, "the client was never accepted");
        let _ = net.drive(Some(Duration::from_millis(1)), &mut [&mut first, &mut second]);
    }
    assert_eq!(first.accepted, None, "an earlier slice position claims nothing it does not own");
}

// ---------------------------------------------------------------------------
// Ticks see the instant the poll wait ended

/// A leaf that records the instant its tick was passed.
struct ClockLeaf {
    group: ConnectionGroup,
    last_tick: Option<Instant>,
}

impl Service for ClockLeaf {
    fn group_id(&self) -> &ConnectionGroupId {
        self.group.group_id()
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        self.group.handle_event(event, &mut |_| {})
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        self.last_tick = Some(now);
        self.group.maintain(now, &mut |_| {})
    }

    fn next_deadline(&self) -> Deadline {
        self.group.next_deadline()
    }
}

#[test]
fn a_tick_sees_the_instant_the_poll_wait_ended() {
    let mut net = StreamNetwork::default();
    let mut leaf = ClockLeaf { group: net.add_group(raw_config("clock")), last_tick: None };

    // Nothing is registered and nothing is due, so the poll sleeps the whole
    // cap; a timer a tick starts must run from the end of that wait.
    let wait = Duration::from_millis(20);
    let before = Instant::now();
    let _ = net.drive(Some(wait), &mut [&mut leaf]);
    let ticked_at = leaf.last_tick.expect("the drive ticked the leaf");
    assert!(
        ticked_at.saturating_sub(before) >= wait,
        "the tick saw an instant from before the poll wait ended"
    );
}

// ---------------------------------------------------------------------------
// Did-work is monotone through a composer that adds no work of its own

#[test]
fn lower_work_stays_true_through_a_composer_that_adds_none() {
    let mut poll = Poll::new().unwrap();
    let mut net = StreamNetwork::with_registry(poll.registry().try_clone().unwrap(), TOKEN_BASE);
    let mut raw = RawService::new(net.add_group(raw_config("relay")));
    let addr = bound_addr(raw.listen(ephemeral()).unwrap());
    let mut relay = RelayService::new(raw, usize::MAX);
    let _client = client_at(addr);

    // The accept is the lower service's work; the relay finds nothing to
    // drain, and the outcome must still report it.
    let mut events = Events::with_capacity(64);
    let deadline = StdInstant::now() + TIMEOUT;
    let mut owned_work = false;
    while !owned_work {
        assert!(StdInstant::now() < deadline, "the client was never accepted");
        poll.poll(&mut events, Some(StdDuration::from_millis(1))).unwrap();
        for event in &events {
            let outcome = relay.handle_event(event);
            if outcome.is_owned() {
                assert!(outcome.worked(), "the lower service's accept was withdrawn");
                owned_work = true;
            }
        }
    }
    let accepted = relay.lower().accepted().expect("the accept was recorded");

    // The disconnect is the lower tick's work; the relay again adds none.
    assert!(relay.lower_mut().disconnect(accepted));
    assert!(relay.tick(Instant::now()).worked(), "the lower service's tick work was withdrawn");
}

// ---------------------------------------------------------------------------
// Deadline folds

#[test]
fn an_external_fold_takes_the_earliest_deadline_across_services() {
    let poll = Poll::new().unwrap();
    let mut net = StreamNetwork::with_registry(poll.registry().try_clone().unwrap(), TOKEN_BASE);
    let mut near = RawService::new(net.add_group(raw_config("near")));
    let mut far = RawService::new(net.add_group(raw_config("far")));

    let now = Instant::now();
    let nearer = now + Duration::from_millis(5);
    let farther = now + Duration::from_millis(50);
    near.set_deadline(Some(nearer));
    far.set_deadline(Some(farther));

    assert_eq!(net.next_deadline(&[&mut near, &mut far]), Some(nearer));
    assert_eq!(net.next_deadline(&[&mut far, &mut near]), Some(nearer), "order changes nothing");

    near.set_deadline(None);
    assert_eq!(net.next_deadline(&[&mut near, &mut far]), Some(farther));
}

#[test]
fn pullable_events_ride_did_work_and_leave_the_deadline_alone() {
    let poll = Poll::new().unwrap();
    let mut net = StreamNetwork::with_registry(poll.registry().try_clone().unwrap(), TOKEN_BASE);
    let mut raw = RawService::new(net.add_group(raw_config("pullable")));
    assert_eq!(net.next_deadline(&[&mut raw]), None, "nothing is due while nothing waits");

    raw.push_inbound(Token(TOKEN_BASE.0), b"waiting");
    assert_eq!(
        net.next_deadline(&[&mut raw]),
        None,
        "work exposed upward is the caller's to pull, not a deadline"
    );
    assert!(net.tick(&mut [&mut raw]), "work exposed upward keeps did-work true");
    assert_eq!(net.next_deadline(&[&mut raw]), None, "the tick changes neither");
}

// ---------------------------------------------------------------------------
// A caller-owned enum carries different service types through one slice

/// The closed set of services one tile hosts, in a single homogeneous slice.
enum TileService {
    Http(HttpService),
    Raw(RawService),
}

impl Service for TileService {
    fn group_id(&self) -> &ConnectionGroupId {
        match self {
            Self::Http(http) => http.group_id(),
            Self::Raw(raw) => raw.group_id(),
        }
    }

    fn handle_event(&mut self, event: &Event) -> ReadinessOutcome {
        match self {
            Self::Http(http) => http.handle_event(event),
            Self::Raw(raw) => raw.handle_event(event),
        }
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        match self {
            Self::Http(http) => http.tick(now),
            Self::Raw(raw) => raw.tick(now),
        }
    }

    fn next_deadline(&self) -> Deadline {
        match self {
            Self::Http(http) => http.next_deadline(),
            Self::Raw(raw) => raw.next_deadline(),
        }
    }
}

#[test]
fn a_caller_owned_enum_schedules_different_service_types_together() {
    let mut net = StreamNetwork::default();
    let mut http = HttpService::new(net.add_group(raw_config("api")), HttpConfig::default());
    let http_addr = bound_addr(http.listen(ephemeral()).unwrap());
    let mut raw = RawService::new(net.add_group(raw_config("feed")));
    let raw_addr = bound_addr(raw.listen(ephemeral()).unwrap());
    let mut services = [TileService::Http(http), TileService::Raw(raw)];

    let _to_http = client_at(http_addr);
    let _to_raw = client_at(raw_addr);

    let deadline = StdInstant::now() + TIMEOUT;
    let mut http_accepted = false;
    let mut raw_accepted = false;
    while !(http_accepted && raw_accepted) {
        assert!(StdInstant::now() < deadline, "a client was never accepted");
        let _ = net.drive(Some(Duration::from_millis(1)), &mut services);
        if let TileService::Http(http) = &mut services[0] {
            while let Some(event) = http.next_event() {
                http_accepted |= matches!(event, HttpEvent::Accepted { .. });
            }
        }
        if let TileService::Raw(raw) = &services[1] {
            raw_accepted |= raw.accepted().is_some();
        }
    }
}
