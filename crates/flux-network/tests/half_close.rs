//! The write-side half-close: what the peer of a half-closed connection
//! reads, and what still reaches the network afterwards.

mod common;

use std::{
    cell::Cell,
    io::{self, Read, Write},
    net::Ipv4Addr,
    thread,
    time::{Duration, Instant},
};

use common::{RawEvent, RawService, Record};
use flux_network::stream::{ConnectionGroupConfig, Endpoint, Framing, StreamNetwork};
use mio::Token;

const TIMEOUT: Duration = Duration::from_secs(10);
const INBOUND: &[u8] = b"sent after the end of the stream";
const BROADCAST: &[u8] = b"broadcast to the group";
const RECONNECTED: &[u8] = b"sent after the reconnect";

/// A loopback endpoint whose port the kernel picks when the listener binds,
/// so no address is handed out before something holds it.
fn ephemeral() -> Endpoint {
    Endpoint::Tcp((Ipv4Addr::LOCALHOST, 0).into())
}

/// Runs one test body over both transports: a loopback address whose port the
/// listener's own bind decides, and a Unix-domain socket path under a
/// temporary directory that lives for the duration of the run.
macro_rules! over_both_transports {
    ($body:ident, $tcp:ident, $unix:ident) => {
        #[test]
        fn $tcp() {
            $body(&ephemeral());
        }

        #[test]
        fn $unix() {
            let dir = tempfile::tempdir().unwrap();
            $body(&Endpoint::Unix(dir.path().join("s")));
        }
    };
}

/// A nonblocking client socket speaking raw bytes to the network under test.
trait ClientStream: Read + Write {}
impl<T: Read + Write> ClientStream for T {}

fn connect_client(endpoint: &Endpoint) -> Box<dyn ClientStream> {
    match endpoint {
        Endpoint::Tcp(addr) => {
            let stream = std::net::TcpStream::connect(addr).unwrap();
            stream.set_nonblocking(true).unwrap();
            Box::new(stream)
        }
        Endpoint::Unix(path) => {
            let stream = std::os::unix::net::UnixStream::connect(path).unwrap();
            stream.set_nonblocking(true).unwrap();
            Box::new(stream)
        }
    }
}

fn raw_group(name: &'static str) -> ConnectionGroupConfig {
    ConnectionGroupConfig { name, framing: Framing::Raw, ..ConnectionGroupConfig::default() }
}

/// A listening network, the service owning the group its connections belong
/// to, and the endpoint it bound: for a TCP request on port `0` that is where
/// a client must dial.
fn server(
    endpoint: &Endpoint,
    config: ConnectionGroupConfig,
) -> (StreamNetwork, RawService, Endpoint) {
    let mut network = StreamNetwork::default();
    let mut service = RawService::new(network.add_group(config));
    let bound = service.listen(endpoint.clone()).unwrap();
    (network, service, bound)
}

/// What the service delivered while a test drove it.
#[derive(Default)]
struct Events {
    messages: Vec<(Token, Vec<u8>)>,
    accepted: Vec<Token>,
    connected: Vec<Token>,
    disconnected: Vec<Token>,
}

impl Events {
    /// Runs one iteration of the network, recording what its service
    /// delivers.
    fn pump(&mut self, network: &mut StreamNetwork, service: &mut RawService) {
        network.drive(Some(Duration::ZERO.into()), &mut [&mut *service]);
        self.collect(service);
    }

    /// Takes everything one service is holding: the lifecycle records it
    /// pulled out of the transport, and the payloads awaiting a drain.
    fn collect(&mut self, service: &mut RawService) {
        for record in service.take_records() {
            match record {
                Record::Accepted { token, .. } => self.accepted.push(token),
                Record::Connected { token, .. } => self.connected.push(token),
                Record::Disconnected { token, .. } => self.disconnected.push(token),
            }
        }
        let messages = &mut self.messages;
        let left = service.spin(usize::MAX, |event| match event {
            RawEvent::Message { token, payload, .. } => messages.push((token, payload.to_vec())),
        });
        assert!(!left, "an unbounded drain left payloads behind");
    }

    /// Pumps until `done` holds, failing with `what` at the deadline.
    fn wait_until(
        &mut self,
        network: &mut StreamNetwork,
        service: &mut RawService,
        done: impl Fn(&Self) -> bool,
        what: &str,
    ) {
        let deadline = Instant::now() + TIMEOUT;
        while !done(self) {
            assert!(Instant::now() < deadline, "{what}");
            self.pump(network, service);
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Waits for one more connection to be accepted, and reports its token.
    fn wait_for_accept(&mut self, network: &mut StreamNetwork, service: &mut RawService) -> Token {
        let before = self.accepted.len();
        self.wait_until(
            network,
            service,
            |events| events.accepted.len() > before,
            "connection was not accepted",
        );
        self.accepted[before]
    }

    /// The bytes one connection delivered, as its peer wrote them: a raw-framed
    /// group delivers a read chunk at a time, and chunks do not preserve
    /// message boundaries.
    fn inbound_from(&self, token: Token) -> Vec<u8> {
        self.messages
            .iter()
            .filter(|(message_token, _)| *message_token == token)
            .flat_map(|(_, payload)| payload.iter().copied())
            .collect()
    }
}

/// Reads what the client can without blocking, reporting the end of the
/// stream.
///
/// A hard close can reset bytes still in flight from the client; everything
/// the network wrote before it has already arrived, so a reset is as much an
/// end of stream as a clean one.
fn read_available(client: &mut dyn Read, out: &mut Vec<u8>) -> bool {
    let mut buffer = [0; 16 * 1024];
    match client.read(&mut buffer) {
        Ok(0) => true,
        Ok(read) => {
            out.extend_from_slice(&buffer[..read]);
            false
        }
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => false,
        Err(err) if err.kind() == io::ErrorKind::ConnectionReset => true,
        Err(err) => panic!("client read failed: {err}"),
    }
}

/// Reads the client to the end of the stream, driving the network and
/// recording what its service delivers meanwhile.
fn read_to_end_of_stream(
    network: &mut StreamNetwork,
    service: &mut RawService,
    client: &mut dyn Read,
    events: &mut Events,
) -> Vec<u8> {
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if read_available(client, &mut received) {
            return received;
        }
        assert!(Instant::now() < deadline, "the peer did not see the end of the stream");
        events.pump(network, service);
    }
}

over_both_transports!(
    half_close_ends_the_stream_and_keeps_reading,
    half_close_ends_the_stream_and_keeps_reading_tcp,
    half_close_ends_the_stream_and_keeps_reading_unix
);
fn half_close_ends_the_stream_and_keeps_reading(endpoint: &Endpoint) {
    let (mut network, mut service, bound) = server(endpoint, raw_group("half-close"));
    let endpoint = &bound;
    let mut client = connect_client(endpoint);
    let mut events = Events::default();
    let token = events.wait_for_accept(&mut network, &mut service);

    assert!(service.shutdown_write_when_drained(token));

    // An empty queue shuts the write side there and then.
    let received = read_to_end_of_stream(&mut network, &mut service, &mut client, &mut events);
    assert!(received.is_empty(), "{received:?}");

    // The connection is still registered and readable, so what the peer
    // sends after the end of the stream still arrives.
    client.write_all(INBOUND).unwrap();
    events.wait_until(
        &mut network,
        &mut service,
        |events| events.inbound_from(token) == INBOUND,
        "the inbound bytes did not arrive",
    );
    assert!(events.disconnected.is_empty(), "the half-close ended the connection");

    // And the peer's own close is still a disconnect.
    drop(client);
    events.wait_until(
        &mut network,
        &mut service,
        |events| !events.disconnected.is_empty(),
        "the peer's close was not reported",
    );
    assert_eq!(events.disconnected, [token]);
}

over_both_transports!(
    queued_bytes_reach_the_peer_before_the_end_of_the_stream,
    queued_bytes_reach_the_peer_before_the_end_of_the_stream_tcp,
    queued_bytes_reach_the_peer_before_the_end_of_the_stream_unix
);
fn queued_bytes_reach_the_peer_before_the_end_of_the_stream(endpoint: &Endpoint) {
    let payload = vec![0xA5; 4 * 1024 * 1024];
    let (mut network, mut service, bound) = server(endpoint, ConnectionGroupConfig {
        socket_buf_size: Some(4096),
        max_frame_size: payload.len(),
        backlog_warn_bytes: None,
        ..raw_group("half-close-drain")
    });
    let endpoint = &bound;
    let mut client = connect_client(endpoint);
    let mut events = Events::default();
    let token = events.wait_for_accept(&mut network, &mut service);

    assert!(service.send(token, &payload));
    assert!(service.shutdown_write_when_drained(token));

    // The peer starts reading late, so the write side is asked to shut while
    // the bulk of the payload is still queued, and again while the queue
    // drains.
    for _ in 0..10 {
        events.pump(&mut network, &mut service);
        thread::sleep(Duration::from_millis(1));
    }

    let received = read_to_end_of_stream(&mut network, &mut service, &mut client, &mut events);
    assert_eq!(received.len(), payload.len(), "the end of the stream arrived early");
    assert!(received == payload, "the peer received bytes the network never sent");
    assert!(events.disconnected.is_empty(), "the drain ended the connection");
}

over_both_transports!(
    sends_after_the_half_close_are_refused,
    sends_after_the_half_close_are_refused_tcp,
    sends_after_the_half_close_are_refused_unix
);
fn sends_after_the_half_close_are_refused(endpoint: &Endpoint) {
    let (mut network, mut service, bound) = server(endpoint, raw_group("half-close-send"));
    let endpoint = &bound;
    let mut client = connect_client(endpoint);
    let mut events = Events::default();
    let token = events.wait_for_accept(&mut network, &mut service);

    assert!(service.shutdown_write_when_drained(token));

    // The closure is the payload's only path onto the wire, so both calls go
    // to the group the service owns: a refused send never serialises.
    let serialised = Cell::new(false);
    let mut group = service.into_group();
    assert!(!group.send_with(token, |out| {
        serialised.set(true);
        out.extend_from_slice(b"after the end of the stream");
    }));
    assert_eq!(
        group.broadcast_with(|out| {
            serialised.set(true);
            out.extend_from_slice(b"after the end of the stream");
        }),
        0
    );
    assert!(!serialised.get(), "a refused send serialised its payload");
    let mut service = RawService::new(group);

    let received = read_to_end_of_stream(&mut network, &mut service, &mut client, &mut events);
    assert!(received.is_empty(), "{received:?}");
    assert!(events.disconnected.is_empty(), "the refused send ended the connection");
}

over_both_transports!(
    a_hard_close_after_the_half_close_ends_the_connection,
    a_hard_close_after_the_half_close_ends_the_connection_tcp,
    a_hard_close_after_the_half_close_ends_the_connection_unix
);
fn a_hard_close_after_the_half_close_ends_the_connection(endpoint: &Endpoint) {
    let (mut network, mut service, bound) = server(endpoint, raw_group("half-close-hard"));
    let endpoint = &bound;
    let mut client = connect_client(endpoint);
    let mut events = Events::default();
    let token = events.wait_for_accept(&mut network, &mut service);

    assert!(service.shutdown_write_when_drained(token));
    assert!(read_to_end_of_stream(&mut network, &mut service, &mut client, &mut events).is_empty());

    assert!(service.disconnect(token), "the half-closed connection was gone already");
    events.wait_until(
        &mut network,
        &mut service,
        |events| !events.disconnected.is_empty(),
        "the hard close was not reported",
    );
    assert_eq!(events.disconnected, [token]);

    // The connection is gone: the token is unknown, and what the peer sends
    // reaches nobody.
    assert!(!service.disconnect(token));
    assert!(!service.shutdown_write_when_drained(token));
    let _ = client.write_all(INBOUND);
    for _ in 0..50 {
        events.pump(&mut network, &mut service);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(events.disconnected, [token]);
    assert!(events.messages.is_empty(), "{:?}", events.messages);
    assert!(read_available(&mut *client, &mut Vec::new()), "the peer read past the close");
}

/// Which of the two closes a test asks for first.
#[derive(Clone, Copy)]
enum Order {
    /// The hard close, then the half-close.
    HardCloseFirst,
    /// The half-close, then the hard close.
    HalfCloseFirst,
}

over_both_transports!(
    a_hard_close_asked_for_first_wins,
    a_hard_close_asked_for_first_wins_tcp,
    a_hard_close_asked_for_first_wins_unix
);
fn a_hard_close_asked_for_first_wins(endpoint: &Endpoint) {
    a_hard_close_wins(endpoint, Order::HardCloseFirst);
}

over_both_transports!(
    a_hard_close_asked_for_second_wins,
    a_hard_close_asked_for_second_wins_tcp,
    a_hard_close_asked_for_second_wins_unix
);
fn a_hard_close_asked_for_second_wins(endpoint: &Endpoint) {
    a_hard_close_wins(endpoint, Order::HalfCloseFirst);
}

/// A connection asked for both closes ends outright when its queue drains,
/// whichever close was asked for first: the peer reads every queued byte and
/// then the end of the stream, and the connection is gone.
fn a_hard_close_wins(endpoint: &Endpoint, order: Order) {
    let payload = vec![0xC3; 4 * 1024 * 1024];
    let (mut network, mut service, bound) = server(endpoint, ConnectionGroupConfig {
        socket_buf_size: Some(4096),
        max_frame_size: payload.len(),
        backlog_warn_bytes: None,
        ..raw_group("both-closes")
    });
    let endpoint = &bound;
    let mut client = connect_client(endpoint);
    let mut events = Events::default();
    let token = events.wait_for_accept(&mut network, &mut service);

    // Both closes are asked for while the queue holds the bulk of the
    // payload, so the drain is what decides between them.
    assert!(service.send(token, &payload));
    match order {
        Order::HardCloseFirst => {
            assert!(service.disconnect_when_drained(token));
            assert!(service.shutdown_write_when_drained(token));
        }
        Order::HalfCloseFirst => {
            assert!(service.shutdown_write_when_drained(token));
            assert!(service.disconnect_when_drained(token));
        }
    }

    let received = read_to_end_of_stream(&mut network, &mut service, &mut client, &mut events);
    assert_eq!(received.len(), payload.len(), "the end of the stream arrived early");

    events.wait_until(
        &mut network,
        &mut service,
        |events| !events.disconnected.is_empty(),
        "the connection was half-closed instead of closed",
    );
    assert_eq!(events.disconnected, [token]);
    assert!(!service.disconnect(token), "the connection outlived its hard close");

    // The peer holds its own end of the connection open, and nothing more
    // reaches the network through it.
    let _ = client.write_all(INBOUND);
    for _ in 0..50 {
        events.pump(&mut network, &mut service);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(events.disconnected, [token]);
    assert!(events.messages.is_empty(), "{:?}", events.messages);
}

over_both_transports!(
    a_broadcast_passes_over_a_half_closed_member,
    a_broadcast_passes_over_a_half_closed_member_tcp,
    a_broadcast_passes_over_a_half_closed_member_unix
);
fn a_broadcast_passes_over_a_half_closed_member(endpoint: &Endpoint) {
    let (mut network, mut service, bound) = server(endpoint, raw_group("broadcast"));
    let endpoint = &bound;
    let mut events = Events::default();
    let mut open = connect_client(endpoint);
    let open_token = events.wait_for_accept(&mut network, &mut service);
    let mut shut = connect_client(endpoint);
    let shut_token = events.wait_for_accept(&mut network, &mut service);
    assert_ne!(open_token, shut_token);

    assert!(service.shutdown_write_when_drained(shut_token));
    assert!(read_to_end_of_stream(&mut network, &mut service, &mut shut, &mut events).is_empty());

    // The half-closed member is not a recipient, and the group's other
    // connection is untouched by its being there.
    assert_eq!(service.broadcast(BROADCAST), 1);
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && received != BROADCAST {
        events.pump(&mut network, &mut service);
        read_available(&mut open, &mut received);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(received, BROADCAST);
    assert!(events.disconnected.is_empty(), "the broadcast closed a connection");

    // The half-closed member is still at the end of its stream: the
    // broadcast queued nothing for it.
    let mut late = Vec::new();
    assert!(read_available(&mut shut, &mut late), "the half-closed member was written to");
    assert!(late.is_empty(), "{late:?}");
}

/// One iteration of a network whose two groups have a service each, and
/// everything the pair delivered.
fn pump_pair(
    network: &mut StreamNetwork,
    listening: &mut RawService,
    calling: &mut RawService,
    events: &mut Events,
) {
    network.drive(Some(Duration::ZERO.into()), &mut [&mut *listening, &mut *calling]);
    events.collect(listening);
    events.collect(calling);
}

/// Pumps both services until `done` holds, failing with `what` at the
/// deadline.
fn wait_until_pair(
    network: &mut StreamNetwork,
    listening: &mut RawService,
    calling: &mut RawService,
    events: &mut Events,
    done: impl Fn(&Events) -> bool,
    what: &str,
) {
    let deadline = Instant::now() + TIMEOUT;
    while !done(events) {
        assert!(Instant::now() < deadline, "{what}");
        pump_pair(network, listening, calling, events);
        thread::sleep(Duration::from_millis(1));
    }
}

over_both_transports!(
    a_reconnect_opens_the_write_side_again,
    a_reconnect_opens_the_write_side_again_tcp,
    a_reconnect_opens_the_write_side_again_unix
);
fn a_reconnect_opens_the_write_side_again(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let mut listening = RawService::new(network.add_group(raw_group("server")));
    let mut calling = RawService::new(network.add_group(ConnectionGroupConfig {
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..raw_group("client")
    }));
    let bound = listening.listen(endpoint.clone()).unwrap();
    let outbound = calling.connect(bound);

    // Both groups are open, so both services go to every iteration.
    let mut events = Events::default();
    wait_until_pair(
        &mut network,
        &mut listening,
        &mut calling,
        &mut events,
        |events| events.connected.contains(&outbound) && !events.accepted.is_empty(),
        "the outbound endpoint did not connect",
    );

    // The half-close ends this connection: the accepting side reads the end
    // of the stream and closes what is left of it, which the endpoint
    // answers by reconnecting.
    assert!(calling.shutdown_write_when_drained(outbound));
    wait_until_pair(
        &mut network,
        &mut listening,
        &mut calling,
        &mut events,
        |events| {
            events.disconnected.len() == 2 &&
                events.connected.iter().filter(|token| **token == outbound).count() == 2 &&
                events.accepted.len() == 2
        },
        "the outbound endpoint did not reconnect",
    );

    // The half-close belonged to the socket that is gone, so the endpoint
    // sends over its new one.
    let accepted = events.accepted[1];
    assert!(calling.send(outbound, RECONNECTED));
    wait_until_pair(
        &mut network,
        &mut listening,
        &mut calling,
        &mut events,
        |events| events.inbound_from(accepted) == RECONNECTED,
        "the payload did not reach the reconnected peer",
    );
}
