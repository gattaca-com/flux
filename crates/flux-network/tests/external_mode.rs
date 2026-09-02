//! The network on a poll its caller owns: the three calls, the classification
//! of foreign tokens, the waker of each mode, and the misuse each mode
//! refuses.

mod common;

use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpStream},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use common::{RawService, Record, RelayService};
use flux_network::{
    Token,
    http::{HttpConfig, HttpEvent, HttpService},
    stream::{ConnectionGroupConfig, Endpoint, Framing, Service, StreamNetwork},
};
use mio::{Events, Interest, Poll, Waker};

const TIMEOUT: Duration = Duration::from_secs(10);
/// How long one iteration of a test loop is allowed to wait in the poll.
const POLL_SLICE: Duration = Duration::from_millis(1);
/// Where the network's own tokens start. Everything below is the caller's.
const TOKEN_BASE: Token = Token(1000);
/// A token of the caller's own, for a source the network never sees.
const FOREIGN: Token = Token(7);

/// A loopback endpoint whose port the kernel picks when the listener binds,
/// so no address is handed out before something holds it.
fn ephemeral() -> Endpoint {
    Endpoint::Tcp((Ipv4Addr::LOCALHOST, 0).into())
}

/// The TCP address a listener bound, port included.
fn bound_addr(bound: Endpoint) -> SocketAddr {
    match bound {
        Endpoint::Tcp(addr) => addr,
        Endpoint::Unix(path) => panic!("a TCP listener bound {}", path.display()),
    }
}

fn raw_group(name: &'static str) -> ConnectionGroupConfig {
    ConnectionGroupConfig {
        name,
        framing: Framing::Raw,
        max_frame_size: usize::MAX,
        backlog_warn_bytes: None,
        ..ConnectionGroupConfig::default()
    }
}

/// A poll the test owns, and the network registered on it.
struct External {
    poll: Poll,
    events: Events,
    net: StreamNetwork,
}

impl External {
    fn new() -> Self {
        let poll = Poll::new().unwrap();
        let net = StreamNetwork::with_registry(poll.registry().try_clone().unwrap(), TOKEN_BASE);
        Self { poll, events: Events::with_capacity(64), net }
    }

    /// One iteration of the loop a caller-held poll requires: fold the
    /// deadlines into the poll timeout, hand over every event, tick once.
    /// The timeout is capped so that a test never waits on a regression.
    fn iterate<S: Service>(&mut self, services: &mut [S]) -> bool {
        let Self { poll, events, net } = self;
        let timeout = net
            .next_deadline(services)
            .map(|deadline| Duration::from(deadline.saturating_sub(flux_timing::Instant::now())))
            .map_or(POLL_SLICE, |wait| wait.min(POLL_SLICE));
        poll.poll(events, Some(timeout)).unwrap();
        let mut worked = false;
        for event in &*events {
            worked |= net.handle_event(event, services);
        }
        worked | net.tick(services)
    }
}

fn client_at(addr: SocketAddr) -> TcpStream {
    let client = TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client
}

/// Reads what is there, reporting whether more may be waiting.
fn read_available(stream: &mut impl Read, out: &mut Vec<u8>) -> bool {
    let mut buf = [0; 8192];
    match stream.read(&mut buf) {
        Ok(0) => false,
        Ok(read) => {
            out.extend_from_slice(&buf[..read]);
            read == buf.len()
        }
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => false,
        Err(err) => panic!("read failed: {err}"),
    }
}

fn response_len(bytes: &[u8]) -> Option<usize> {
    let head = bytes.windows(4).position(|window| window == b"\r\n\r\n")? + 4;
    let text = std::str::from_utf8(&bytes[..head]).unwrap();
    let length = text
        .lines()
        .find_map(|line| line.strip_prefix("Content-Length: "))
        .unwrap()
        .parse::<usize>()
        .unwrap();
    (bytes.len() >= head + length).then_some(head + length)
}

/// A service serving on a port the kernel picked, and the address to dial it.
fn served(ext: &mut External, config: HttpConfig) -> (HttpService, SocketAddr) {
    let group = ext.net.add_group(raw_group("http"));
    let mut http = HttpService::new(group, config);
    let addr = bound_addr(http.listen(ephemeral()).unwrap());
    (http, addr)
}

// ---------------------------------------------------------------------------
// The three calls

#[test]
fn an_external_network_serves_over_the_three_calls() {
    let mut ext = External::new();
    let (mut http, addr) = served(&mut ext, HttpConfig::default());
    let mut client = client_at(addr);
    client.write_all(b"GET /over-there HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();

    let mut paths = Vec::new();
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && response_len(&received).is_none() {
        ext.iterate(&mut [&mut http]);
        while let Some(event) = http.next_event() {
            if let HttpEvent::Request { request, responder, .. } = event {
                paths.push(request.path.to_owned());
                assert!(responder.respond(200, &[], b"served"));
            }
        }
        while read_available(&mut client, &mut received) {}
    }

    assert_eq!(paths, ["/over-there"]);
    assert!(received.ends_with(b"served"), "{}", String::from_utf8_lossy(&received));
    http.close(&mut ext.net);
}

#[test]
fn a_foreign_source_keeps_its_events_and_its_connections() {
    let mut ext = External::new();
    let (mut http, addr) = served(&mut ext, HttpConfig::default());

    // A listener of the caller's own, on a token below the network's base.
    let mut foreign = mio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let foreign_addr = foreign.local_addr().unwrap();
    ext.poll.registry().register(&mut foreign, FOREIGN, Interest::READABLE).unwrap();

    let _to_foreign = client_at(foreign_addr);
    let _to_network = client_at(addr);

    let mut ours = 0;
    let mut theirs = 0;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && (ours == 0 || theirs == 0) {
        let mut services = [&mut http];
        let _ = ext.net.next_deadline(&services);
        ext.poll.poll(&mut ext.events, Some(POLL_SLICE)).unwrap();
        for event in &ext.events {
            if ext.net.handle_event(event, &mut services) {
                assert_ne!(event.token(), FOREIGN, "the network claimed the caller's token");
                ours += 1;
            } else {
                assert_eq!(event.token(), FOREIGN, "the network disowned a token of its own");
                theirs += 1;
            }
        }
        ext.net.tick(&mut services);
    }

    assert!(ours > 0, "the network never saw an event of its own");
    assert!(theirs > 0, "the caller's listener never became readable");
    // Untouched means untouched: the connection is still there to accept.
    assert!(foreign.accept().is_ok(), "the network accepted the caller's connection");
    http.close(&mut ext.net);
}

#[test]
fn a_foreign_event_is_handed_back_before_the_services_are_checked() {
    let mut ext = External::new();
    let _http = HttpService::new(ext.net.add_group(raw_group("http")), HttpConfig::default());

    // A wake of the caller's own, on a token below the network's base.
    let waker = Waker::new(ext.poll.registry(), FOREIGN).unwrap();
    waker.wake().unwrap();
    ext.poll.poll(&mut ext.events, Some(TIMEOUT)).unwrap();
    let event = ext.events.iter().next().expect("the wake was not delivered");

    // The empty slice omits the service owning the group, which any call of
    // the network's own reports; an event of the caller's is handed back
    // without the services being looked at.
    assert!(!ext.net.handle_event(event, &mut [] as &mut [RawService]));
}

#[test]
#[should_panic(expected = "connection group 0 has no service")]
fn an_omitted_service_is_reported_within_its_iteration() {
    let mut ext = External::new();
    let (_http, addr) = served(&mut ext, HttpConfig::default());
    let _client = client_at(addr);

    // The listener becomes readable on a token of the network's own. Per
    // event, only identity and liveness are checked, so the empty slice gets
    // the event handed back as this network's with nothing to route it to;
    // the iteration's tick is where the omission is a configuration error.
    let deadline = Instant::now() + TIMEOUT;
    let mut handled = false;
    while !handled {
        assert!(Instant::now() < deadline, "the listener never became readable");
        ext.poll.poll(&mut ext.events, Some(POLL_SLICE)).unwrap();
        for event in &ext.events {
            handled |= ext.net.handle_event(event, &mut [] as &mut [RawService]);
        }
    }
    ext.net.tick(&mut [] as &mut [RawService]);
}

#[test]
fn a_tick_reports_a_request_no_one_pulled() {
    let mut ext = External::new();
    let (mut http, addr) = served(&mut ext, HttpConfig::default());
    let mut client = client_at(addr);

    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = false;
    while Instant::now() < deadline && !accepted {
        ext.iterate(&mut [&mut http]);
        while let Some(event) = http.next_event() {
            accepted |= matches!(event, HttpEvent::Accepted { .. });
        }
    }
    assert!(accepted, "the client was never accepted");
    assert!(!ext.net.tick(&mut [&mut http]), "everything was pulled, so nothing is work");

    // Nothing pulls from here on: the request must be work for as long as it
    // is left there.
    client.write_all(b"GET /unpulled HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let mut pullable = false;
    while Instant::now() < deadline && !pullable {
        ext.iterate(&mut [&mut http]);
        // No event arrives inside a tick of its own, and the service has no
        // outbound endpoint to reconnect, so `true` is the request's alone.
        pullable = ext.net.tick(&mut [&mut http]);
    }
    assert!(pullable, "the request never became work");

    let mut paths = Vec::new();
    while let Some(event) = http.next_event() {
        if let HttpEvent::Request { request, .. } = event {
            paths.push(request.path.to_owned());
        }
    }
    assert_eq!(paths, ["/unpulled"]);
    http.close(&mut ext.net);
}

#[test]
fn a_tick_attempts_the_reconnect_that_is_due() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("late");
    let mut ext = External::new();
    let mut raw = RawService::new(ext.net.add_group(ConnectionGroupConfig {
        reconnect_interval: flux_timing::Duration::from_millis(20),
        ..raw_group("outbound")
    }));

    // Nothing is listening yet, so the first attempt fails and the group is
    // left with a retry due every interval.
    let token = raw.connect(Endpoint::Unix(path.clone()));
    for _ in 0..3 {
        ext.iterate(&mut [&mut raw]);
    }
    assert!(raw.records().is_empty(), "nothing is connected yet");

    let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
    let mut connected = false;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && !connected {
        ext.iterate(&mut [&mut raw]);
        connected |= raw
            .records()
            .iter()
            .any(|record| matches!(record, Record::Connected { token: at, .. } if *at == token));
    }

    assert!(connected, "no tick ever retried the endpoint");
    drop(listener);
}

#[test]
fn a_late_event_for_a_gone_connection_is_still_ours() {
    let mut ext = External::new();
    let mut raw = RawService::new(ext.net.add_group(raw_group("stale")));
    let addr = bound_addr(raw.listen(ephemeral()).unwrap());
    let mut client = client_at(addr);

    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = None;
    while Instant::now() < deadline && accepted.is_none() {
        ext.iterate(&mut [&mut raw]);
        accepted = raw.accepted();
    }
    let accepted = accepted.expect("the client was never accepted");
    client.write_all(b"late").unwrap();

    // The caller owns the loop, so a connection can go between the poll and
    // the event it returned: what arrives is ours, and gone.
    let mut handled = false;
    while Instant::now() < deadline && !handled {
        ext.poll.poll(&mut ext.events, Some(POLL_SLICE)).unwrap();
        for event in &ext.events {
            if event.token() != accepted {
                continue;
            }
            assert!(raw.remove(accepted));
            let records = raw.records().len();
            let pending = raw.pending();
            assert!(
                ext.net.handle_event(event, &mut [&mut raw]),
                "a stale token of the network's own was disowned"
            );
            assert_eq!(raw.records().len(), records, "a gone connection delivered an event");
            assert_eq!(raw.pending(), pending, "a gone connection delivered a payload");
            handled = true;
        }
    }
    assert!(handled, "the connection never became readable");
}

// ---------------------------------------------------------------------------
// Deadlines

#[test]
fn next_deadline_folds_the_services_idle_sweep() {
    let mut ext = External::new();
    let idle = flux_timing::Duration::from_millis(500);
    let (mut http, addr) = served(&mut ext, HttpConfig::default().with_idle_timeout(idle));
    assert!(
        ext.net.next_deadline(&[&mut http]).is_none(),
        "nothing is due before a connection exists"
    );

    let _client = client_at(addr);
    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = false;
    while Instant::now() < deadline && !accepted {
        ext.iterate(&mut [&mut http]);
        while let Some(event) = http.next_event() {
            accepted |= matches!(event, HttpEvent::Accepted { .. });
        }
    }
    assert!(accepted, "the client was never accepted");

    let folded = ext.net.next_deadline(&[&mut http]).expect("the sweep of that connection");
    let left = Duration::from(folded.saturating_sub(flux_timing::Instant::now()));
    assert!(left <= Duration::from(idle), "later than the sweep: {left:?}");
    assert!(left > Duration::ZERO, "the sweep is already past: {left:?}");
    http.close(&mut ext.net);
}

#[test]
#[should_panic(expected = "connection group 0 has no service")]
fn next_deadline_validates_before_it_folds() {
    let mut ext = External::new();
    let _http = HttpService::new(ext.net.add_group(raw_group("http")), HttpConfig::default());

    // No socket has moved and no event has arrived: an omitted service is a
    // configuration error the first fold reports all the same.
    let _ = ext.net.next_deadline(&[] as &[RawService]);
}

#[test]
fn a_composer_folds_its_leftovers_at_the_tick_instant_it_holds() {
    let mut ext = External::new();
    let mut raw = RawService::new(ext.net.add_group(raw_group("relay")));
    let addr = bound_addr(raw.listen(ephemeral()).unwrap());
    let mut relay = RelayService::new(raw, 1);
    let _client = client_at(addr);

    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = None;
    while Instant::now() < deadline && accepted.is_none() {
        ext.iterate(&mut [&mut relay]);
        accepted = relay.lower().accepted();
    }
    let accepted = accepted.expect("the client was never accepted");

    // Three payloads await the drain, and the relay's bound is one: each
    // tick drains one, and the leftovers fold into the deadline at exactly
    // the instant that tick held — no clock is read anywhere in the fold.
    relay.lower_mut().push_inbound(accepted, b"one");
    relay.lower_mut().push_inbound(accepted, b"two");
    relay.lower_mut().push_inbound(accepted, b"three");
    let first_tick = flux_timing::Instant::now();
    assert!(relay.tick(first_tick).worked(), "three payloads awaited the drain");
    assert_eq!(relay.echoed(), 1, "the bound of one drained exactly one payload");
    assert_eq!(
        relay.next_deadline().instant(),
        Some(first_tick),
        "the folded instant is the tick instant the composer held"
    );

    // A later tick whose drain still leaves events records its own instant:
    // the deadline is that tick's `now`, not the first tick's.
    let second_tick = flux_timing::Instant::now();
    assert!(relay.tick(second_tick).worked(), "the relay had work to report");
    assert_eq!(relay.echoed(), 2, "the second tick drained the next payload");
    assert_eq!(
        relay.next_deadline().instant(),
        Some(second_tick),
        "each tick with leftovers re-records the instant it held"
    );

    // The tick that drains the last leftover clears the deadline with it.
    assert!(relay.tick(flux_timing::Instant::now()).worked(), "the relay had work to report");
    assert_eq!(relay.next_deadline().instant(), None);
    assert_eq!(relay.echoed(), 3);
}

#[test]
fn a_composed_service_closes_by_delegating_the_consuming_close() {
    let mut ext = External::new();
    let mut kept = RawService::new(ext.net.add_group(raw_group("kept")));
    let _addr = bound_addr(kept.listen(ephemeral()).unwrap());
    let relay = RelayService::new(RawService::new(ext.net.add_group(raw_group("closed"))), 1);

    // The composed close is delegation the whole way down: the relay hands
    // back its leaf, the leaf hands back its group, and removing the group
    // is what ends the chain's life.
    ext.net.remove_group(relay.into_lower().into_group());

    // The closed slot asks for no service; the surviving one alone passes
    // validation and carries on being scheduled.
    assert!(!ext.iterate(&mut [&mut kept]), "an idle survivor reports no work");
}

// ---------------------------------------------------------------------------
// Wakers

#[test]
fn a_wake_returns_from_a_blocking_drive_without_work() {
    let mut net = StreamNetwork::default();
    let mut raw = RawService::new(net.add_group(raw_group("raw")));
    let addr = bound_addr(raw.listen(ephemeral()).unwrap());
    // A waker delivers only while it is alive, so the test holds one end of
    // it for as long as the drive it wakes.
    let waker = Arc::new(net.waker().unwrap());
    let woken = Arc::clone(&waker);

    // The late connection is the test's own deadline: a drive that missed
    // the wake returns on it, well past the bound below, rather than hang.
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        woken.wake().unwrap();
        thread::sleep(Duration::from_secs(3));
        drop(std::net::TcpStream::connect(addr));
    });

    let started = Instant::now();
    let worked = net.drive(None, &mut [&mut raw]);
    let waited = started.elapsed();

    assert!(!worked, "a wake is not work of its own");
    assert!(raw.records().is_empty(), "the wake reached the service");
    assert_eq!(raw.pending(), 0, "the wake carried a payload");
    assert!(waited >= Duration::from_millis(100), "the drive never blocked: {waited:?}");
    assert!(waited < Duration::from_secs(2), "the wake did not return from the poll: {waited:?}");
    drop(waker);
}

#[test]
fn an_external_waker_is_the_callers_own() {
    let mut ext = External::new();
    let waker = Arc::new(Waker::new(ext.poll.registry(), FOREIGN).unwrap());
    let woken = Arc::clone(&waker);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        woken.wake().unwrap();
    });

    // The timeout is the test's own deadline: a wake that never arrives ends
    // the poll with nothing in it rather than hanging.
    let started = Instant::now();
    ext.poll.poll(&mut ext.events, Some(TIMEOUT)).unwrap();
    assert!(started.elapsed() < Duration::from_secs(5), "the poll was not woken");

    let mut seen = 0;
    for event in &ext.events {
        assert_eq!(event.token(), FOREIGN);
        assert!(
            !ext.net.handle_event(event, &mut [] as &mut [RawService]),
            "the network took the caller's wake for its own"
        );
        seen += 1;
    }
    assert_eq!(seen, 1, "the wake never arrived");
    drop(waker);
}

// ---------------------------------------------------------------------------
// Mode misuse

#[test]
#[should_panic(expected = "poll this external network yourself")]
fn driving_a_network_on_a_caller_owned_poll_is_refused() {
    let mut ext = External::new();
    let _ = ext.net.drive(Some(flux_timing::Duration::ZERO), &mut [] as &mut [RawService]);
}

#[test]
#[should_panic(expected = "this network polls itself")]
fn handing_an_owned_network_an_event_is_refused() {
    // Any event will do: the mode is settled before the token is looked at.
    let mut poll = Poll::new().unwrap();
    let waker = Waker::new(poll.registry(), FOREIGN).unwrap();
    waker.wake().unwrap();
    let mut events = Events::with_capacity(4);
    poll.poll(&mut events, Some(TIMEOUT)).unwrap();
    let event = events.iter().next().expect("the wake was not delivered");

    let mut net = StreamNetwork::default();
    net.handle_event(event, &mut [] as &mut [RawService]);
}

#[test]
#[should_panic(expected = "this network polls itself")]
fn asking_an_owned_network_for_a_deadline_is_refused() {
    let net = StreamNetwork::default();
    let _ = net.next_deadline(&[] as &[RawService]);
}

#[test]
#[should_panic(expected = "this network polls itself")]
fn ticking_an_owned_network_is_refused() {
    let mut net = StreamNetwork::default();
    net.tick(&mut [] as &mut [RawService]);
}

#[test]
#[should_panic(expected = "build the waker on that poll")]
fn a_waker_for_a_caller_owned_poll_is_refused() {
    let mut ext = External::new();
    let _ = ext.net.waker();
}

#[test]
#[should_panic(expected = "already handed out its waker")]
fn a_network_hands_out_one_waker() {
    let mut net = StreamNetwork::default();
    let _first = net.waker().unwrap();
    let _second = net.waker().unwrap();
}

#[test]
fn a_caller_waker_at_the_top_of_the_token_space_is_not_ours() {
    let mut ext = External::new();
    // The token the network reserves for a waker of its own is one no
    // allocation reaches, so the caller may put its waker there too.
    let waker = Arc::new(Waker::new(ext.poll.registry(), Token(usize::MAX)).unwrap());
    let woken = Arc::clone(&waker);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        woken.wake().unwrap();
    });

    let started = Instant::now();
    ext.poll.poll(&mut ext.events, Some(TIMEOUT)).unwrap();
    assert!(started.elapsed() < Duration::from_secs(5), "the poll was not woken");

    let mut seen = 0;
    for event in &ext.events {
        assert_eq!(event.token(), Token(usize::MAX));
        assert!(
            !ext.net.handle_event(event, &mut [] as &mut [RawService]),
            "the network took the caller's wake for a stale token of its own"
        );
        seen += 1;
    }
    assert_eq!(seen, 1, "the wake never arrived");
    drop(waker);
}

#[test]
#[should_panic(expected = "stream token space exhausted")]
fn allocation_stops_below_the_reserved_waker_token() {
    let poll = Poll::new().unwrap();
    let mut net =
        StreamNetwork::with_registry(poll.registry().try_clone().unwrap(), Token(usize::MAX - 1));
    let mut group = net.add_group(raw_group("edge"));
    group.listen(ephemeral()).unwrap();
    // The next token would be the one the waker reserves.
    group.listen(ephemeral()).unwrap();
}

#[test]
fn handle_event_delivers_the_disconnect_it_produced() {
    let mut ext = External::new();
    let mut raw =
        RawService::new(ext.net.add_group(ConnectionGroupConfig {
            socket_buf_size: Some(1024),
            ..raw_group("drained")
        }));
    let addr = bound_addr(raw.listen(ephemeral()).unwrap());
    let mut client = client_at(addr);

    // No tick runs anywhere in this test: everything the network reports has
    // to come out of the events handed to it.
    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = None;
    while Instant::now() < deadline && accepted.is_none() {
        ext.poll.poll(&mut ext.events, Some(POLL_SLICE)).unwrap();
        for event in &ext.events {
            ext.net.handle_event(event, &mut [&mut raw]);
        }
        accepted = raw.accepted();
    }
    let accepted = accepted.expect("the client was never accepted");

    // A backlog the socket cannot swallow, so the queue is still there when
    // the close is asked for and the drain happens under a writable event.
    assert!(raw.send(accepted, &vec![7; 4 * 1024 * 1024]));
    assert!(raw.disconnect_when_drained(accepted));

    let mut drained = Vec::new();
    let mut disconnected = None;
    while Instant::now() < deadline && disconnected.is_none() {
        ext.poll.poll(&mut ext.events, Some(POLL_SLICE)).unwrap();
        for event in &ext.events {
            ext.net.handle_event(event, &mut [&mut raw]);
        }
        disconnected = raw.records().iter().find_map(|record| match record {
            Record::Disconnected { token, .. } => Some(*token),
            _ => None,
        });
        while read_available(&mut client, &mut drained) {}
    }

    assert_eq!(disconnected, Some(accepted), "the disconnect waited for a tick");
    assert_eq!(drained.len(), 4 * 1024 * 1024, "the backlog was not written before the close");
}
