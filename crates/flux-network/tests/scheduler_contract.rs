//! The scheduling contract's ordering and reporting guarantees, pinned
//! through the real driver.

mod common;

use std::{
    cell::RefCell,
    net::{Ipv4Addr, SocketAddr, TcpStream},
    rc::Rc,
    time::{Duration as StdDuration, Instant as StdInstant},
};

use common::{RawService, RelayService};
use flux_network::{
    Token,
    http::{HttpConfig, HttpEvent, HttpService},
    stream::{
        ConnectionGroup, ConnectionGroupConfig, ConnectionGroupId, Endpoint, Framing,
        ReadinessOutcome, Service, StreamEvent, StreamNetwork,
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

    fn tick(&mut self, now: Instant) -> bool {
        let Self { group, log, .. } = self;
        let worked = group.maintain(now, &mut |event| {
            if let StreamEvent::Disconnected { .. } = event {
                log.push("transport");
            }
        });
        log.push("timers");
        worked
    }

    fn next_deadline(&self) -> Option<Instant> {
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

    fn tick(&mut self, now: Instant) -> bool {
        let worked = self.group.maintain(now, &mut |_| {});
        self.log.borrow_mut().push(self.name);
        worked
    }

    fn next_deadline(&self) -> Option<Instant> {
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

    fn tick(&mut self, now: Instant) -> bool {
        self.last_tick = Some(now);
        self.group.maintain(now, &mut |_| {})
    }

    fn next_deadline(&self) -> Option<Instant> {
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
    assert!(relay.tick(Instant::now()), "the lower service's tick work was withdrawn");
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

    fn tick(&mut self, now: Instant) -> bool {
        match self {
            Self::Http(http) => http.tick(now),
            Self::Raw(raw) => raw.tick(now),
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
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
