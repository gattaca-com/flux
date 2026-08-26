//! The network on a poll its caller owns: the three calls, the classification
//! of foreign tokens, the waker of each mode, and the misuse each mode
//! refuses.

use std::{
    collections::BTreeSet,
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Token,
    http::{HttpConfig, HttpEvent, HttpService},
    stream::{ConnectionGroupConfig, Endpoint, Framing, ServiceRef, StreamEvent, StreamNetwork},
};
use mio::{Events, Interest, Poll, Waker};

const TIMEOUT: Duration = Duration::from_secs(10);
/// How long one iteration of a test loop is allowed to wait in the poll.
const POLL_SLICE: Duration = Duration::from_millis(1);
/// Where the network's own tokens start. Everything below is the caller's.
const TOKEN_BASE: Token = Token(1000);
/// A token of the caller's own, for a source the network never sees.
const FOREIGN: Token = Token(7);

/// A loopback address no listener holds. The probe binding is released before
/// the address is handed out, and a port this run has already given away is
/// never given away again.
fn unused_addr() -> SocketAddr {
    static TAKEN: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
    let mut probes = Vec::new();
    loop {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        if TAKEN.lock().unwrap().insert(addr.port()) {
            return addr;
        }
        probes.push(listener);
        assert!(probes.len() < 64, "no free loopback port");
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
    fn iterate<F>(&mut self, services: &mut [ServiceRef<'_>], mut unclaimed_handler: F) -> bool
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        let Self { poll, events, net } = self;
        let timeout = net
            .next_deadline(services)
            .map(|deadline| Duration::from(deadline.saturating_sub(flux_timing::Instant::now())))
            .map_or(POLL_SLICE, |wait| wait.min(POLL_SLICE));
        poll.poll(events, Some(timeout)).unwrap();
        let mut worked = false;
        for event in &*events {
            worked |= net.handle_event(event, services, &mut unclaimed_handler);
        }
        worked | net.tick(services, &mut unclaimed_handler)
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

/// An HTTP service listening on `addr`, inside a network on the test's poll.
fn served(ext: &mut External, addr: SocketAddr, config: HttpConfig) -> HttpService {
    let group = ext.net.add_group(raw_group("http"));
    let mut http = HttpService::new(&mut ext.net, group, config);
    http.listen(&mut ext.net, Endpoint::Tcp(addr)).unwrap();
    http
}

// ---------------------------------------------------------------------------
// The three calls

#[test]
fn an_external_network_serves_over_the_three_calls() {
    let mut ext = External::new();
    let addr = unused_addr();
    let mut http = served(&mut ext, addr, HttpConfig::default());
    let mut client = client_at(addr);
    client.write_all(b"GET /over-there HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();

    let mut paths = Vec::new();
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && response_len(&received).is_none() {
        ext.iterate(&mut [http.as_service()], |_| panic!("no unclaimed group exists"));
        while let Some(event) = http.next_event(&mut ext.net) {
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
    let addr = unused_addr();
    let mut http = served(&mut ext, addr, HttpConfig::default());

    // A listener of the caller's own, on a token below the network's base.
    let foreign_addr = unused_addr();
    let mut foreign = mio::net::TcpListener::bind(foreign_addr).unwrap();
    ext.poll.registry().register(&mut foreign, FOREIGN, Interest::READABLE).unwrap();

    let _to_foreign = client_at(foreign_addr);
    let _to_network = client_at(addr);

    let mut ours = 0;
    let mut theirs = 0;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && (ours == 0 || theirs == 0) {
        let mut services = [http.as_service()];
        let _ = ext.net.next_deadline(&services);
        ext.poll.poll(&mut ext.events, Some(POLL_SLICE)).unwrap();
        for event in &ext.events {
            if ext.net.handle_event(event, &mut services, |_| {}) {
                assert_ne!(event.token(), FOREIGN, "the network claimed the caller's token");
                ours += 1;
            } else {
                assert_eq!(event.token(), FOREIGN, "the network disowned a token of its own");
                theirs += 1;
            }
        }
        ext.net.tick(&mut services, |_| {});
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
    let group = ext.net.add_group(raw_group("http"));
    let _http = HttpService::new(&mut ext.net, group, HttpConfig::default());

    // A wake of the caller's own, on a token below the network's base.
    let waker = Waker::new(ext.poll.registry(), FOREIGN).unwrap();
    waker.wake().unwrap();
    ext.poll.poll(&mut ext.events, Some(TIMEOUT)).unwrap();
    let event = ext.events.iter().next().expect("the wake was not delivered");

    // The empty slice omits the service owning the group, which any call of
    // the network's own reports; an event of the caller's is handed back
    // without the services being looked at.
    assert!(!ext.net.handle_event(event, &mut [], |_| {}));
}

#[test]
#[should_panic(expected = "service-owned group 0 has no service")]
fn an_own_event_is_checked_against_the_services_before_it_is_routed() {
    let mut ext = External::new();
    let addr = unused_addr();
    let _http = served(&mut ext, addr, HttpConfig::default());
    let _client = client_at(addr);

    // The listener becomes readable on a token of the network's own, and the
    // same empty slice is a configuration error that event reports.
    let deadline = Instant::now() + TIMEOUT;
    loop {
        assert!(Instant::now() < deadline, "the listener never became readable");
        ext.poll.poll(&mut ext.events, Some(POLL_SLICE)).unwrap();
        if let Some(event) = ext.events.iter().next() {
            ext.net.handle_event(event, &mut [], |_| {});
        }
    }
}

#[test]
fn a_tick_reports_a_request_no_one_pulled() {
    let mut ext = External::new();
    let addr = unused_addr();
    let mut http = served(&mut ext, addr, HttpConfig::default());
    let mut client = client_at(addr);

    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = false;
    while Instant::now() < deadline && !accepted {
        ext.iterate(&mut [http.as_service()], |_| {});
        while let Some(event) = http.next_event(&mut ext.net) {
            accepted |= matches!(event, HttpEvent::Accepted { .. });
        }
    }
    assert!(accepted, "the client was never accepted");
    assert!(
        !ext.net.tick(&mut [http.as_service()], |_| {}),
        "everything was pulled, so nothing is work"
    );

    // Nothing pulls from here on: the request must be work for as long as it
    // is left there.
    client.write_all(b"GET /unpulled HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let mut pullable = false;
    while Instant::now() < deadline && !pullable {
        ext.iterate(&mut [http.as_service()], |_| {});
        // No event arrives inside a tick of its own, and the network has no
        // outbound endpoint to reconnect, so `true` is the service's alone.
        pullable = ext.net.tick(&mut [http.as_service()], |_| {});
    }
    assert!(pullable, "the request never became work");

    let mut paths = Vec::new();
    while let Some(event) = http.next_event(&mut ext.net) {
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
    let group = ext.net.add_group(ConnectionGroupConfig {
        reconnect_interval: flux_timing::Duration::from_millis(20),
        ..raw_group("outbound")
    });

    // Nothing is listening yet, so the first attempt fails and the group is
    // left with a retry due every interval.
    let token = ext.net.connect(group, Endpoint::Unix(path.clone()));
    for _ in 0..3 {
        ext.iterate(&mut [], |_| panic!("nothing is connected yet"));
    }

    let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
    let mut connected = false;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && !connected {
        ext.iterate(&mut [], |event| {
            connected |= matches!(event, StreamEvent::Connected { token: at, .. } if at == token);
        });
    }

    assert!(connected, "no tick ever retried the endpoint");
    drop(listener);
}

#[test]
fn a_late_event_for_a_gone_connection_is_still_ours() {
    let mut ext = External::new();
    let group = ext.net.add_group(raw_group("stale"));
    let addr = unused_addr();
    ext.net.listen(group, Endpoint::Tcp(addr)).unwrap();
    let mut client = client_at(addr);

    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = None;
    while Instant::now() < deadline && accepted.is_none() {
        ext.iterate(&mut [], |event| {
            if let StreamEvent::Accepted { token, .. } = event {
                accepted = Some(token);
            }
        });
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
            assert!(ext.net.remove(accepted));
            assert!(
                ext.net.handle_event(event, &mut [], |_| panic!(
                    "a gone connection delivered an event"
                )),
                "a stale token of the network's own was disowned"
            );
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
    let addr = unused_addr();
    let idle = flux_timing::Duration::from_millis(500);
    let mut http = served(&mut ext, addr, HttpConfig::default().with_idle_timeout(idle));
    assert!(
        ext.net.next_deadline(&[http.as_service()]).is_none(),
        "nothing is due before a connection exists"
    );

    let _client = client_at(addr);
    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = false;
    while Instant::now() < deadline && !accepted {
        ext.iterate(&mut [http.as_service()], |_| {});
        while let Some(event) = http.next_event(&mut ext.net) {
            accepted |= matches!(event, HttpEvent::Accepted { .. });
        }
    }
    assert!(accepted, "the client was never accepted");

    let folded = ext.net.next_deadline(&[http.as_service()]).expect("the sweep of that connection");
    let left = Duration::from(folded.saturating_sub(flux_timing::Instant::now()));
    assert!(left <= Duration::from(idle), "later than the sweep: {left:?}");
    assert!(left > Duration::ZERO, "the sweep is already past: {left:?}");
    http.close(&mut ext.net);
}

#[test]
#[should_panic(expected = "service-owned group 0 has no service")]
fn next_deadline_validates_before_it_folds() {
    let mut ext = External::new();
    let group = ext.net.add_group(raw_group("http"));
    let _http = HttpService::new(&mut ext.net, group, HttpConfig::default());

    // No socket has moved and no event has arrived: an omitted service is a
    // configuration error the first fold reports all the same.
    let _ = ext.net.next_deadline(&[]);
}

// ---------------------------------------------------------------------------
// Wakers

#[test]
fn a_wake_returns_from_a_blocking_drive_without_work() {
    let mut net = StreamNetwork::default();
    let group = net.add_group(raw_group("raw"));
    let addr = unused_addr();
    net.listen(group, Endpoint::Tcp(addr)).unwrap();
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
    let mut delivered = 0;
    let worked = net.drive(None, &mut [], |_| delivered += 1);
    let waited = started.elapsed();

    assert!(!worked, "a wake is not work of its own");
    assert_eq!(delivered, 0, "the wake reached the handler");
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
            !ext.net.handle_event(event, &mut [], |_| {}),
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
    ext.net.drive(Some(flux_timing::Duration::ZERO), &mut [], |_| {});
}

#[test]
#[should_panic(expected = "poll this external network yourself")]
fn polling_a_network_on_a_caller_owned_poll_is_refused() {
    let mut ext = External::new();
    ext.net.poll_with(|_| {});
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
    net.handle_event(event, &mut [], |_| {});
}

#[test]
#[should_panic(expected = "this network polls itself")]
fn asking_an_owned_network_for_a_deadline_is_refused() {
    let net = StreamNetwork::default();
    let _ = net.next_deadline(&[]);
}

#[test]
#[should_panic(expected = "this network polls itself")]
fn ticking_an_owned_network_is_refused() {
    let mut net = StreamNetwork::default();
    net.tick(&mut [], |_| {});
}

#[test]
#[should_panic(expected = "build the waker on that poll")]
fn a_waker_for_a_caller_owned_poll_is_refused() {
    let mut ext = External::new();
    let _ = ext.net.waker();
}

#[test]
#[should_panic(expected = "hands out one waker")]
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
            !ext.net.handle_event(event, &mut [], |_| {}),
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
    let group = net.add_group(raw_group("edge"));
    net.listen(group, Endpoint::Tcp(unused_addr())).unwrap();
    // The next token would be the one the waker reserves.
    net.listen(group, Endpoint::Tcp(unused_addr())).unwrap();
}

#[test]
fn handle_event_delivers_the_disconnect_it_produced() {
    let mut ext = External::new();
    let group = ext
        .net
        .add_group(ConnectionGroupConfig { socket_buf_size: Some(1024), ..raw_group("drained") });
    let addr = unused_addr();
    ext.net.listen(group, Endpoint::Tcp(addr)).unwrap();
    let mut client = client_at(addr);

    // No tick runs anywhere in this test: everything the network reports has
    // to come out of the events handed to it.
    let mut accepted = None;
    let mut disconnected = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && accepted.is_none() {
        ext.poll.poll(&mut ext.events, Some(POLL_SLICE)).unwrap();
        for event in &ext.events {
            ext.net.handle_event(event, &mut [], |event| {
                if let StreamEvent::Accepted { token, .. } = event {
                    accepted = Some(token);
                }
            });
        }
    }
    let accepted = accepted.expect("the client was never accepted");

    // A backlog the socket cannot swallow, so the queue is still there when
    // the close is asked for and the drain happens under a writable event.
    assert!(ext.net.send_with(accepted, |out| out.resize(4 * 1024 * 1024, 7)));
    assert!(ext.net.disconnect_when_drained(accepted));

    let mut drained = Vec::new();
    while Instant::now() < deadline && disconnected.is_none() {
        ext.poll.poll(&mut ext.events, Some(POLL_SLICE)).unwrap();
        for event in &ext.events {
            ext.net.handle_event(event, &mut [], |event| {
                if let StreamEvent::Disconnected { token, .. } = event {
                    disconnected = Some(token);
                }
            });
        }
        while read_available(&mut client, &mut drained) {}
    }

    assert_eq!(disconnected, Some(accepted), "the disconnect waited for a tick");
    assert_eq!(drained.len(), 4 * 1024 * 1024, "the backlog was not written before the close");
}
