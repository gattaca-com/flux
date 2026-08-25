//! The write-side half-close: what the peer of a half-closed connection
//! reads, and what still reaches the network afterwards.

use std::{
    cell::Cell,
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr},
    thread,
    time::{Duration, Instant},
};

use flux_network::stream::{
    ConnectionGroup, ConnectionGroupConfig, Endpoint, Framing, StreamEvent, StreamNetwork,
};
use mio::Token;

const TIMEOUT: Duration = Duration::from_secs(10);
const INBOUND: &[u8] = b"sent after the end of the stream";
const BROADCAST: &[u8] = b"broadcast to the group";
const RECONNECTED: &[u8] = b"sent after the reconnect";

fn unused_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

/// Runs one test body over both transports: a loopback TCP address on an
/// ephemeral port, and a Unix-domain socket path under a temporary directory
/// that lives for the duration of the run.
macro_rules! over_both_transports {
    ($body:ident, $tcp:ident, $unix:ident) => {
        #[test]
        fn $tcp() {
            $body(&Endpoint::Tcp(unused_addr()));
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

/// A listening network, and the group its connections belong to.
fn server(endpoint: &Endpoint, config: ConnectionGroupConfig) -> (StreamNetwork, ConnectionGroup) {
    let mut network = StreamNetwork::default();
    let group = network.add_group(config);
    network.listen(group, endpoint.clone()).unwrap();
    (network, group)
}

fn wait_for_accept(network: &mut StreamNetwork, group: ConnectionGroup) -> Token {
    let mut accepted = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && accepted.is_none() {
        network.poll_with(|event| {
            if let StreamEvent::Accepted { group: event_group, token, .. } = event {
                assert_eq!(event_group, group);
                accepted = Some(token);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    accepted.expect("connection was not accepted")
}

/// What the network delivered while a test drove it.
#[derive(Default)]
struct Events {
    messages: Vec<(Token, Vec<u8>)>,
    accepted: Vec<Token>,
    connected: Vec<Token>,
    disconnected: Vec<Token>,
}

impl Events {
    /// Runs one iteration of the network, recording what it delivers.
    fn pump(&mut self, network: &mut StreamNetwork) {
        let Self { messages, accepted, connected, disconnected } = self;
        network.poll_with(|event| match event {
            StreamEvent::Message { token, payload, .. } => {
                messages.push((token, payload.to_vec()));
            }
            StreamEvent::Accepted { token, .. } => accepted.push(token),
            StreamEvent::Connected { token, .. } => connected.push(token),
            StreamEvent::Disconnected { token, .. } => disconnected.push(token),
        });
    }

    /// Pumps until `done` holds, failing with `what` at the deadline.
    fn wait_until(
        &mut self,
        network: &mut StreamNetwork,
        done: impl Fn(&Self) -> bool,
        what: &str,
    ) {
        let deadline = Instant::now() + TIMEOUT;
        while !done(self) {
            assert!(Instant::now() < deadline, "{what}");
            self.pump(network);
            thread::sleep(Duration::from_millis(1));
        }
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
/// recording what it delivers meanwhile.
fn read_to_end_of_stream(
    network: &mut StreamNetwork,
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
        events.pump(network);
    }
}

over_both_transports!(
    half_close_ends_the_stream_and_keeps_reading,
    half_close_ends_the_stream_and_keeps_reading_tcp,
    half_close_ends_the_stream_and_keeps_reading_unix
);
fn half_close_ends_the_stream_and_keeps_reading(endpoint: &Endpoint) {
    let (mut network, group) = server(endpoint, raw_group("half-close"));
    let mut client = connect_client(endpoint);
    let token = wait_for_accept(&mut network, group);

    assert!(network.shutdown_write_when_drained(token));

    // An empty queue shuts the write side there and then.
    let mut events = Events::default();
    let received = read_to_end_of_stream(&mut network, &mut client, &mut events);
    assert!(received.is_empty(), "{received:?}");

    // The connection is still registered and readable, so what the peer
    // sends after the end of the stream still arrives.
    client.write_all(INBOUND).unwrap();
    events.wait_until(
        &mut network,
        |events| events.inbound_from(token) == INBOUND,
        "the inbound bytes did not arrive",
    );
    assert!(events.disconnected.is_empty(), "the half-close ended the connection");

    // And the peer's own close is still a disconnect.
    drop(client);
    events.wait_until(
        &mut network,
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
    let (mut network, group) = server(endpoint, ConnectionGroupConfig {
        socket_buf_size: Some(4096),
        max_frame_size: payload.len(),
        backlog_warn_bytes: None,
        ..raw_group("half-close-drain")
    });
    let mut client = connect_client(endpoint);
    let token = wait_for_accept(&mut network, group);

    assert!(network.send_with(token, |out| out.extend_from_slice(&payload)));
    assert!(network.shutdown_write_when_drained(token));

    // The peer starts reading late, so the write side is asked to shut while
    // the bulk of the payload is still queued, and again while the queue
    // drains.
    let mut events = Events::default();
    for _ in 0..10 {
        events.pump(&mut network);
        thread::sleep(Duration::from_millis(1));
    }

    let received = read_to_end_of_stream(&mut network, &mut client, &mut events);
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
    let (mut network, group) = server(endpoint, raw_group("half-close-send"));
    let mut client = connect_client(endpoint);
    let token = wait_for_accept(&mut network, group);

    assert!(network.shutdown_write_when_drained(token));

    let serialised = Cell::new(false);
    assert!(!network.send_with(token, |out| {
        serialised.set(true);
        out.extend_from_slice(b"after the end of the stream");
    }));
    assert_eq!(
        network.broadcast_with(group, |out| {
            serialised.set(true);
            out.extend_from_slice(b"after the end of the stream");
        }),
        0
    );
    assert!(!serialised.get(), "a refused send serialised its payload");

    let mut events = Events::default();
    let received = read_to_end_of_stream(&mut network, &mut client, &mut events);
    assert!(received.is_empty(), "{received:?}");
    assert!(events.disconnected.is_empty(), "the refused send ended the connection");
}

over_both_transports!(
    a_hard_close_after_the_half_close_ends_the_connection,
    a_hard_close_after_the_half_close_ends_the_connection_tcp,
    a_hard_close_after_the_half_close_ends_the_connection_unix
);
fn a_hard_close_after_the_half_close_ends_the_connection(endpoint: &Endpoint) {
    let (mut network, group) = server(endpoint, raw_group("half-close-hard"));
    let mut client = connect_client(endpoint);
    let token = wait_for_accept(&mut network, group);

    assert!(network.shutdown_write_when_drained(token));
    let mut events = Events::default();
    assert!(read_to_end_of_stream(&mut network, &mut client, &mut events).is_empty());

    assert!(network.disconnect(token), "the half-closed connection was gone already");
    events.wait_until(
        &mut network,
        |events| !events.disconnected.is_empty(),
        "the hard close was not reported",
    );
    assert_eq!(events.disconnected, [token]);

    // The connection is gone: the token is unknown, and what the peer sends
    // reaches nobody.
    assert!(!network.disconnect(token));
    assert!(!network.shutdown_write_when_drained(token));
    let _ = client.write_all(INBOUND);
    for _ in 0..50 {
        events.pump(&mut network);
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
    let (mut network, group) = server(endpoint, ConnectionGroupConfig {
        socket_buf_size: Some(4096),
        max_frame_size: payload.len(),
        backlog_warn_bytes: None,
        ..raw_group("both-closes")
    });
    let mut client = connect_client(endpoint);
    let token = wait_for_accept(&mut network, group);

    // Both closes are asked for while the queue holds the bulk of the
    // payload, so the drain is what decides between them.
    assert!(network.send_with(token, |out| out.extend_from_slice(&payload)));
    match order {
        Order::HardCloseFirst => {
            assert!(network.disconnect_when_drained(token));
            assert!(network.shutdown_write_when_drained(token));
        }
        Order::HalfCloseFirst => {
            assert!(network.shutdown_write_when_drained(token));
            assert!(network.disconnect_when_drained(token));
        }
    }

    let mut events = Events::default();
    let received = read_to_end_of_stream(&mut network, &mut client, &mut events);
    assert_eq!(received.len(), payload.len(), "the end of the stream arrived early");

    events.wait_until(
        &mut network,
        |events| !events.disconnected.is_empty(),
        "the connection was half-closed instead of closed",
    );
    assert_eq!(events.disconnected, [token]);
    assert!(!network.disconnect(token), "the connection outlived its hard close");

    // The peer holds its own end of the connection open, and nothing more
    // reaches the network through it.
    let _ = client.write_all(INBOUND);
    for _ in 0..50 {
        events.pump(&mut network);
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
    let (mut network, group) = server(endpoint, raw_group("broadcast"));
    let mut open = connect_client(endpoint);
    let open_token = wait_for_accept(&mut network, group);
    let mut shut = connect_client(endpoint);
    let shut_token = wait_for_accept(&mut network, group);
    assert_ne!(open_token, shut_token);

    assert!(network.shutdown_write_when_drained(shut_token));
    let mut events = Events::default();
    assert!(read_to_end_of_stream(&mut network, &mut shut, &mut events).is_empty());

    // The half-closed member is not a recipient, and the group's other
    // connection is untouched by its being there.
    assert_eq!(network.broadcast_with(group, |out| out.extend_from_slice(BROADCAST)), 1);
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && received != BROADCAST {
        events.pump(&mut network);
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

over_both_transports!(
    a_reconnect_opens_the_write_side_again,
    a_reconnect_opens_the_write_side_again_tcp,
    a_reconnect_opens_the_write_side_again_unix
);
fn a_reconnect_opens_the_write_side_again(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let server_group = network.add_group(raw_group("server"));
    let client_group = network.add_group(ConnectionGroupConfig {
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..raw_group("client")
    });
    network.listen(server_group, endpoint.clone()).unwrap();
    let outbound = network.connect(client_group, endpoint.clone());

    let mut events = Events::default();
    events.wait_until(
        &mut network,
        |events| events.connected.contains(&outbound) && !events.accepted.is_empty(),
        "the outbound endpoint did not connect",
    );

    // The half-close ends this connection: the accepting side reads the end
    // of the stream and closes what is left of it, which the endpoint
    // answers by reconnecting.
    assert!(network.shutdown_write_when_drained(outbound));
    events.wait_until(
        &mut network,
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
    assert!(network.send_with(outbound, |out| out.extend_from_slice(RECONNECTED)));
    events.wait_until(
        &mut network,
        |events| events.inbound_from(accepted) == RECONNECTED,
        "the payload did not reach the reconnected peer",
    );
}
