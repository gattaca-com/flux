//! [`StreamService`] against real sockets: what its group emits reaches the
//! sink in both modes, and the service keeps the scheduler contract without a
//! line of scheduling code from its user.

use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpStream},
    time::{Duration, Instant},
};

use flux_network::{
    Token,
    stream::{
        ConnectionGroupConfig, Endpoint, Framing, Peer, Service, StreamEvent, StreamNetwork,
        StreamService, StreamSink,
    },
};

const TIMEOUT: Duration = Duration::from_secs(5);

/// The frame header the length-prefixed framing puts on the wire.
const HEADER: usize = 12;

const ZERO: Option<flux_timing::Duration> = Some(flux_timing::Duration(0));

/// A loopback endpoint whose port the kernel picks when the listener binds.
fn ephemeral() -> Endpoint {
    Endpoint::Tcp((Ipv4Addr::LOCALHOST, 0).into())
}

fn bound_addr(bound: io::Result<Endpoint>) -> SocketAddr {
    match bound.unwrap() {
        Endpoint::Tcp(addr) => addr,
        Endpoint::Unix(path) => panic!("a TCP listener bound {}", path.display()),
    }
}

fn config(name: &'static str, framing: Framing) -> ConnectionGroupConfig {
    ConnectionGroupConfig {
        name,
        framing,
        reconnect_interval: Duration::from_millis(20).into(),
        ..ConnectionGroupConfig::default()
    }
}

fn client(addr: SocketAddr) -> TcpStream {
    let client = TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client
}

/// One length-prefixed frame as a peer writes it: the payload length, the
/// sender's timestamp, the payload.
fn frame(payload: &[u8], send_ts: u64) -> Vec<u8> {
    let mut frame = Vec::with_capacity(HEADER + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&send_ts.to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Every frame `expected` bytes hold, as `(send_ts, payload)`.
fn frames(mut bytes: &[u8]) -> Vec<(u64, Vec<u8>)> {
    let mut frames = Vec::new();
    while !bytes.is_empty() {
        let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let send_ts = u64::from_le_bytes(bytes[4..HEADER].try_into().unwrap());
        frames.push((send_ts, bytes[HEADER..HEADER + len].to_vec()));
        bytes = &bytes[HEADER + len..];
    }
    frames
}

/// Reads whatever has arrived on `stream` into `out`.
fn read_available(stream: &mut TcpStream, out: &mut Vec<u8>) {
    let mut buf = [0; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return,
            Ok(read) => out.extend_from_slice(&buf[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => return,
            Err(err) => panic!("the peer could not read: {err}"),
        }
    }
}

/// What a test kept of one event, owned.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Seen {
    Accepted(Token, Peer),
    Connected(Token, Peer),
    Message(Token, Vec<u8>, u64),
    Disconnected(Token, Peer),
}

impl Seen {
    fn of(event: &StreamEvent<'_>) -> Self {
        match *event {
            StreamEvent::Accepted { token, peer } => Self::Accepted(token, peer),
            StreamEvent::Connected { token, peer } => Self::Connected(token, peer),
            StreamEvent::Message { token, payload, send_ts } => {
                Self::Message(token, payload.to_vec(), send_ts.0)
            }
            StreamEvent::Disconnected { token, peer } => Self::Disconnected(token, peer),
        }
    }

    fn token(&self) -> Token {
        match self {
            Self::Accepted(token, _) |
            Self::Connected(token, _) |
            Self::Message(token, ..) |
            Self::Disconnected(token, _) => *token,
        }
    }
}

/// Pulls everything a retained service holds, in order.
fn pull_all(service: &mut StreamService, into: &mut Vec<Seen>) {
    while let Some(event) = service.next_event() {
        into.push(Seen::of(&event));
    }
}

/// Drives `services` until `done` holds, pulling nothing.
fn drive_until<S: Service>(
    net: &mut StreamNetwork,
    services: &mut [S],
    deadline: Instant,
    mut done: impl FnMut(&mut [S]) -> bool,
) {
    while !done(services) {
        assert!(Instant::now() < deadline, "timed out waiting for the network");
        let _ = net.drive(ZERO, services);
    }
}

/// A sink that keeps a copy of everything it is handed, and reports pending
/// work only when told to.
#[derive(Default)]
struct Tally {
    seen: Vec<Seen>,
    bytes: usize,
    nag: bool,
}

impl StreamSink for Tally {
    fn on_event(&mut self, event: StreamEvent<'_>) {
        if let StreamEvent::Message { payload, .. } = &event {
            self.bytes += payload.len();
        }
        self.seen.push(Seen::of(&event));
    }

    fn has_pending(&self) -> bool {
        self.nag
    }
}

/// One peer's whole life on a retained service: accepted, its messages, gone.
fn retained_pulls_a_peers_life_in_order(framing: Framing) {
    let deadline = Instant::now() + TIMEOUT;
    let mut net = StreamNetwork::default();
    let mut service = StreamService::new(net.add_group(config("retained", framing)));
    let addr = bound_addr(service.listen(ephemeral()));

    let mut peer = client(addr);
    let wire = match framing {
        Framing::LengthPrefixed => [frame(b"alpha", 7), frame(b"beta", 8)].concat(),
        Framing::Raw => b"alphabeta".to_vec(),
    };
    peer.write_all(&wire).unwrap();

    let mut seen = Vec::new();
    let mut received = 0;
    while received < 9 {
        assert!(Instant::now() < deadline, "the messages never arrived");
        let _ = net.drive(ZERO, &mut [&mut service]);
        pull_all(&mut service, &mut seen);
        received = seen
            .iter()
            .map(|event| match event {
                Seen::Message(_, payload, _) => payload.len(),
                _ => 0,
            })
            .sum();
    }
    assert_eq!(service.pending(), 0, "a full pull leaves nothing behind");

    let Seen::Accepted(token, Peer::Tcp(peer_addr)) = seen[0] else {
        panic!("the first event is the accept, got {:?}", seen[0]);
    };
    assert_eq!(peer_addr, peer.local_addr().unwrap());
    assert!(seen.iter().all(|event| event.token() == token), "one peer, one token");
    let messages: Vec<_> = seen[1..]
        .iter()
        .map(|event| match event {
            Seen::Message(_, payload, send_ts) => (payload.clone(), *send_ts),
            other => panic!("only messages follow the accept, got {other:?}"),
        })
        .collect();
    match framing {
        // Frames arrive whole, with the timestamp the peer put on the wire.
        Framing::LengthPrefixed => {
            assert_eq!(messages, [(b"alpha".to_vec(), 7), (b"beta".to_vec(), 8)]);
        }
        // Chunks keep the bytes and their order, stamped with the local
        // receive time.
        Framing::Raw => {
            let bytes: Vec<u8> = messages.iter().flat_map(|(payload, _)| payload.clone()).collect();
            assert_eq!(bytes, b"alphabeta");
            assert!(messages.iter().all(|(_, send_ts)| *send_ts > 0));
        }
    }

    drop(peer);
    seen.clear();
    drive_until(&mut net, &mut [&mut service], deadline, |services| services[0].pending() > 0);
    pull_all(&mut service, &mut seen);
    assert_eq!(seen, [Seen::Disconnected(token, Peer::Tcp(peer_addr))]);
}

#[test]
fn a_retained_service_pulls_a_peers_life_in_order_length_prefixed() {
    retained_pulls_a_peers_life_in_order(Framing::LengthPrefixed);
}

#[test]
fn a_retained_service_pulls_a_peers_life_in_order_raw() {
    retained_pulls_a_peers_life_in_order(Framing::Raw);
}

#[test]
fn unpulled_events_ride_did_work_and_never_the_deadline() {
    let deadline = Instant::now() + TIMEOUT;
    let mut net = StreamNetwork::default();
    let mut service =
        StreamService::new(net.add_group(config("unpulled", Framing::LengthPrefixed)));
    let addr = bound_addr(service.listen(ephemeral()));
    let mut peer = client(addr);
    peer.write_all(&frame(b"waiting", 1)).unwrap();
    drive_until(&mut net, &mut [&mut service], deadline, |services| services[0].pending() >= 2);

    // The sockets are idle now; only the unpulled events can report work.
    assert!(net.drive(ZERO, &mut [&mut service]), "unpulled events are work");
    assert!(service.next_deadline().instant().is_none(), "and they never arm a deadline");

    let mut seen = Vec::new();
    pull_all(&mut service, &mut seen);
    assert_eq!(seen.len(), 2, "the accept and the message");
    assert!(!net.drive(ZERO, &mut [&mut service]), "nothing left to report once pulled");
}

#[test]
fn a_sink_sees_every_event_during_the_iteration() {
    let deadline = Instant::now() + TIMEOUT;
    let mut net = StreamNetwork::default();
    let group = net.add_group(config("sink", Framing::LengthPrefixed));
    let mut service = StreamService::with_sink(group, Tally::default());
    let addr = bound_addr(service.listen(ephemeral()));

    let mut peer = client(addr);
    peer.write_all(&frame(b"gamma", 9)).unwrap();
    drive_until(&mut net, &mut [&mut service], deadline, |services| services[0].sink().bytes == 5);
    let Seen::Accepted(token, peer_addr) = service.sink().seen[0].clone() else {
        panic!("the first event is the accept, got {:?}", service.sink().seen[0]);
    };
    assert_eq!(service.sink().seen, [
        Seen::Accepted(token, peer_addr),
        Seen::Message(token, b"gamma".to_vec(), 9)
    ]);
    // A sink that reports nothing pending leaves an idle iteration idle,
    // whatever it kept.
    assert!(!net.drive(ZERO, &mut [&mut service]));

    // The disconnect comes through the tick's maintenance, to the same sink.
    service.sink_mut().seen.clear();
    drop(peer);
    drive_until(&mut net, &mut [&mut service], deadline, |services| {
        !services[0].sink().seen.is_empty()
    });
    assert_eq!(service.sink().seen, [Seen::Disconnected(token, peer_addr)]);
}

#[test]
fn a_sinks_pending_report_is_the_services_did_work() {
    let mut net = StreamNetwork::default();
    let group = net.add_group(config("nagging", Framing::Raw));
    let mut service = StreamService::with_sink(group, Tally { nag: true, ..Tally::default() });
    bound_addr(service.listen(ephemeral()));
    assert!(net.drive(ZERO, &mut [&mut service]), "the sink's word is the did-work report");
    assert!(service.next_deadline().instant().is_none(), "and never a deadline");
    service.sink_mut().nag = false;
    assert!(!net.drive(ZERO, &mut [&mut service]));
}

#[test]
fn the_write_side_reaches_the_group_and_one_type_needs_no_enum() {
    let deadline = Instant::now() + TIMEOUT;
    let mut net = StreamNetwork::default();
    let mut left = StreamService::new(net.add_group(config("left", Framing::LengthPrefixed)));
    let left_addr = bound_addr(left.listen(ephemeral()));
    let mut right = StreamService::new(net.add_group(config("right", Framing::LengthPrefixed)));
    let right_addr = bound_addr(right.listen(ephemeral()));
    // Two services of one type: a plain array, no enum, no borrow.
    let mut services = [left, right];

    let mut first = client(left_addr);
    let mut second = client(left_addr);
    let mut other = client(right_addr);
    drive_until(&mut net, &mut services, deadline, |services| {
        services[0].pending() == 2 && services[1].pending() == 1
    });
    let mut seen = Vec::new();
    pull_all(&mut services[0], &mut seen);
    let tokens: Vec<Token> = seen.iter().map(Seen::token).collect();
    let first_token = seen
        .iter()
        .find_map(|event| match event {
            Seen::Accepted(token, Peer::Tcp(addr)) if *addr == first.local_addr().unwrap() => {
                Some(*token)
            }
            _ => None,
        })
        .expect("the first client was accepted");
    let second_token = *tokens.iter().find(|token| **token != first_token).unwrap();
    seen.clear();
    pull_all(&mut services[1], &mut seen);
    assert!(matches!(seen[..], [Seen::Accepted(..)]), "the other group accepted its own client");

    assert_eq!(services[0].broadcast_with(|out| out.extend_from_slice(b"everyone")), 2);
    assert!(services[0].send_with(first_token, |out| out.extend_from_slice(b"you")));
    assert!(!services[1].send_with(first_token, |out| out.extend_from_slice(b"lost")));

    let (mut first_read, mut second_read, mut other_read) = (Vec::new(), Vec::new(), Vec::new());
    drive_until(&mut net, &mut services, deadline, |_| {
        read_available(&mut first, &mut first_read);
        read_available(&mut second, &mut second_read);
        read_available(&mut other, &mut other_read);
        frames(&first_read).len() == 2 && frames(&second_read).len() == 1
    });
    let payloads = |bytes: &[u8]| frames(bytes).into_iter().map(|(_, p)| p).collect::<Vec<_>>();
    assert_eq!(payloads(&first_read), [b"everyone".to_vec(), b"you".to_vec()]);
    assert_eq!(payloads(&second_read), [b"everyone".to_vec()]);
    assert!(other_read.is_empty(), "a send names a token of its own group");

    // A disconnect is delivered by the next tick's maintenance.
    assert!(services[0].disconnect(second_token));
    drive_until(&mut net, &mut services, deadline, |services| services[0].pending() > 0);
    seen.clear();
    pull_all(&mut services[0], &mut seen);
    assert!(matches!(seen[..], [Seen::Disconnected(token, _)] if token == second_token));
    assert!(!services[0].disconnect(second_token), "gone already");

    assert!(services[0].remove(first_token));
    assert!(!services[0].remove(first_token), "removed already");
    assert_eq!(services[0].refused_connections(), 0);

    let [left, right] = services;
    left.close(&mut net);
    right.close(&mut net);
    // No open group is left for the scheduler to expect a service for.
    assert!(!net.drive(ZERO, &mut Vec::<StreamService>::new()));
}

#[test]
fn an_outbound_endpoint_connects_speaks_and_reconnects() {
    let deadline = Instant::now() + TIMEOUT;
    let mut net = StreamNetwork::default();
    let mut server = StreamService::new(net.add_group(config("server", Framing::LengthPrefixed)));
    let addr = bound_addr(server.listen(ephemeral()));
    let mut dialer = StreamService::new(net.add_group(config("dialer", Framing::LengthPrefixed)));
    let token = dialer.connect(Endpoint::Tcp(addr));
    let mut services = [server, dialer];

    drive_until(&mut net, &mut services, deadline, |services| {
        services[0].pending() == 1 && services[1].pending() == 1
    });
    let (mut on_server, mut on_dialer) = (Vec::new(), Vec::new());
    pull_all(&mut services[0], &mut on_server);
    pull_all(&mut services[1], &mut on_dialer);
    let Seen::Accepted(accepted, _) = on_server[0] else { panic!("got {on_server:?}") };
    assert_eq!(on_dialer, [Seen::Connected(token, Peer::Tcp(addr))]);

    assert!(services[1].send_with(token, |out| out.extend_from_slice(b"hello")));
    drive_until(&mut net, &mut services, deadline, |services| services[0].pending() > 0);
    on_server.clear();
    pull_all(&mut services[0], &mut on_server);
    assert!(matches!(&on_server[..], [Seen::Message(token, payload, _)]
        if *token == accepted && payload == b"hello"));

    // The server hangs up; the dialer reports the loss and, under the same
    // token, the reconnect.
    assert!(services[0].disconnect(accepted));
    on_dialer.clear();
    drive_until(&mut net, &mut services, deadline, |services| {
        pull_all(&mut services[1], &mut on_dialer);
        on_dialer.len() >= 2
    });
    assert_eq!(on_dialer[..2], [
        Seen::Disconnected(token, Peer::Tcp(addr)),
        Seen::Connected(token, Peer::Tcp(addr))
    ]);
}

/// A batch reaches its peer as one frame per payload, in order, every frame
/// stamped with the one instant the batch was flushed at; a later batch is
/// stamped no earlier. A broadcast batch is stamped once for every peer.
#[test]
fn a_batch_sends_every_payload_under_one_timestamp() {
    let deadline = Instant::now() + TIMEOUT;
    let mut net = StreamNetwork::default();
    let mut server = StreamService::new(net.add_group(config("server", Framing::LengthPrefixed)));
    let addr = bound_addr(server.listen(ephemeral()));
    let mut services = [server];

    let mut first = client(addr);
    let mut second = client(addr);
    drive_until(&mut net, &mut services, deadline, |services| services[0].pending() == 2);
    let mut seen = Vec::new();
    pull_all(&mut services[0], &mut seen);
    let first_token = seen
        .iter()
        .find_map(|event| match event {
            Seen::Accepted(token, Peer::Tcp(addr)) if *addr == first.local_addr().unwrap() => {
                Some(*token)
            }
            _ => None,
        })
        .expect("the first client was accepted");

    let batch: [&[u8]; 3] = [b"one", b"two", b"three"];
    assert!(services[0].send_many_with(first_token, batch, |out, item| {
        out.extend_from_slice(item);
    }));
    assert!(services[0].send_many_with(first_token, [b"four".as_slice()], |out, item| {
        out.extend_from_slice(item);
    }));
    assert_eq!(services[0].broadcast_many_with(batch, |out, item| out.extend_from_slice(item)), 2);

    let (mut first_read, mut second_read) = (Vec::new(), Vec::new());
    drive_until(&mut net, &mut services, deadline, |_| {
        read_available(&mut first, &mut first_read);
        read_available(&mut second, &mut second_read);
        frames(&first_read).len() == 7 && frames(&second_read).len() == 3
    });

    let first_frames = frames(&first_read);
    let payloads: Vec<&[u8]> = first_frames.iter().map(|(_, payload)| payload.as_slice()).collect();
    assert_eq!(payloads, [b"one".as_slice(), b"two", b"three", b"four", b"one", b"two", b"three"]);
    let stamps: Vec<u64> = first_frames.iter().map(|(send_ts, _)| *send_ts).collect();
    assert!(stamps[0] == stamps[1] && stamps[1] == stamps[2], "one batch, one stamp: {stamps:?}");
    assert!(stamps[3] >= stamps[2], "a later batch is stamped no earlier: {stamps:?}");
    assert!(stamps[4] >= stamps[3], "a later batch is stamped no earlier: {stamps:?}");
    assert!(stamps[4] == stamps[5] && stamps[5] == stamps[6], "one batch, one stamp: {stamps:?}");

    let second_frames = frames(&second_read);
    let payloads: Vec<&[u8]> =
        second_frames.iter().map(|(_, payload)| payload.as_slice()).collect();
    assert_eq!(payloads, batch);
    assert!(
        second_frames.iter().all(|(send_ts, _)| *send_ts == stamps[4]),
        "a broadcast batch carries one stamp to every peer: {second_frames:?}"
    );
}
