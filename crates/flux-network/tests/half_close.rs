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

/// What the network delivered after the connection was accepted.
#[derive(Default)]
struct Events {
    messages: Vec<Vec<u8>>,
    disconnected: usize,
}

impl Events {
    /// Runs one iteration of the network, recording what it delivers.
    fn pump(&mut self, network: &mut StreamNetwork) {
        network.poll_with(|event| match event {
            StreamEvent::Message { payload, .. } => self.messages.push(payload.to_vec()),
            StreamEvent::Disconnected { .. } => self.disconnected += 1,
            StreamEvent::Accepted { .. } | StreamEvent::Connected { .. } => {
                panic!("unexpected lifecycle event")
            }
        });
    }

    /// The inbound bytes as the peer wrote them: a raw-framed group delivers a
    /// read chunk at a time, and chunks do not preserve message boundaries.
    fn inbound(&self) -> Vec<u8> {
        self.messages.concat()
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

/// Reads the client to the end of the stream, driving the network meanwhile.
fn read_to_end_of_stream(network: &mut StreamNetwork, client: &mut dyn Read) -> Vec<u8> {
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if read_available(client, &mut received) {
            return received;
        }
        assert!(Instant::now() < deadline, "the peer did not see the end of the stream");
        network.poll_with(|_| {});
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
    let received = read_to_end_of_stream(&mut network, &mut client);
    assert!(received.is_empty(), "{received:?}");

    // The connection is still registered and readable, so what the peer
    // sends after the end of the stream still arrives.
    client.write_all(INBOUND).unwrap();
    let mut events = Events::default();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && events.inbound() != INBOUND {
        events.pump(&mut network);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(events.inbound(), INBOUND);
    assert_eq!(events.disconnected, 0, "the half-close ended the connection");

    // And the peer's own close is still a disconnect.
    drop(client);
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && events.disconnected == 0 {
        events.pump(&mut network);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(events.disconnected, 1);
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

    let received = read_to_end_of_stream(&mut network, &mut client);
    assert_eq!(received.len(), payload.len(), "the end of the stream arrived early");
    assert!(received == payload, "the peer received bytes the network never sent");
    assert_eq!(events.disconnected, 0, "the drain ended the connection");
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

    let received = read_to_end_of_stream(&mut network, &mut client);
    assert!(received.is_empty(), "{received:?}");
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
    assert!(read_to_end_of_stream(&mut network, &mut client).is_empty());

    assert!(network.disconnect(token), "the half-closed connection was gone already");
    let mut events = Events::default();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && events.disconnected == 0 {
        events.pump(&mut network);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(events.disconnected, 1);

    // The connection is gone: the token is unknown, and what the peer sends
    // reaches nobody.
    assert!(!network.disconnect(token));
    assert!(!network.shutdown_write_when_drained(token));
    let _ = client.write_all(INBOUND);
    for _ in 0..50 {
        events.pump(&mut network);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(events.disconnected, 1);
    assert!(events.messages.is_empty(), "{:?}", events.messages);
    assert!(read_available(&mut *client, &mut Vec::new()), "the peer read past the close");
}
