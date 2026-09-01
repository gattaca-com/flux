//! The per-group connection cap: which connections hold a place in it, and
//! what a client that arrives at a full group sees.

use std::{
    io::{self, Read, Write},
    net::Ipv4Addr,
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Token,
    http::{HttpConfig, HttpEvent, HttpService},
    stream::{
        ConnectionGroup, ConnectionGroupConfig, Endpoint, Framing, StreamEvent, StreamNetwork,
    },
};

const TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST: &[u8] = b"GET /hello HTTP/1.1\r\nHost: x\r\n\r\n";
const BODY: &[u8] = b"hello";

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

fn capped_group(name: &'static str, max: usize) -> ConnectionGroupConfig {
    ConnectionGroupConfig { max_connections: Some(max), ..raw_group(name) }
}

/// Reads what the client can without blocking, reporting the end of the
/// stream.
///
/// A refused connection is closed under bytes the client has already sent,
/// which answers a read with a reset rather than a clean end; both mean the
/// same thing here.
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

/// A network under test, and the lifecycle events it delivered.
struct Server {
    network: StreamNetwork,
    accepted: Vec<(ConnectionGroup, Token)>,
    connected: Vec<Token>,
    disconnected: Vec<Token>,
}

impl Server {
    fn new() -> Self {
        Self {
            network: StreamNetwork::default(),
            accepted: Vec::new(),
            connected: Vec::new(),
            disconnected: Vec::new(),
        }
    }

    /// Adds a group and a listener for it, reporting the group and the
    /// endpoint the listener bound.
    fn listen(
        &mut self,
        config: ConnectionGroupConfig,
        endpoint: &Endpoint,
    ) -> (ConnectionGroup, Endpoint) {
        let group = self.network.add_group(config);
        let bound = self.network.listen(group, endpoint.clone()).unwrap();
        (group, bound)
    }

    /// Runs one iteration of the network, recording what it delivers.
    fn pump(&mut self) {
        let Self { network, accepted, connected, disconnected } = self;
        network.poll_with(|event| match event {
            StreamEvent::Accepted { group, token, .. } => accepted.push((group, token)),
            StreamEvent::Connected { token, .. } => connected.push(token),
            StreamEvent::Disconnected { token, .. } => disconnected.push(token),
            StreamEvent::Message { .. } => {}
        });
    }

    fn accepted_in(&self, group: ConnectionGroup) -> Vec<Token> {
        self.accepted
            .iter()
            .filter(|(event_group, _)| *event_group == group)
            .map(|(_, token)| *token)
            .collect()
    }

    fn refused(&self, group: ConnectionGroup) -> u64 {
        self.network.refused_connections(group)
    }

    /// Pumps until `group` has accepted `count` connections.
    fn wait_for_accepts(&mut self, group: ConnectionGroup, count: usize) -> Vec<Token> {
        let deadline = Instant::now() + TIMEOUT;
        while Instant::now() < deadline && self.accepted_in(group).len() < count {
            self.pump();
            thread::sleep(Duration::from_millis(1));
        }
        let accepted = self.accepted_in(group);
        assert_eq!(accepted.len(), count, "the group accepted the wrong number of connections");
        accepted
    }

    /// Pumps until `count` connections have been reported closed.
    fn wait_for_disconnects(&mut self, count: usize) {
        let deadline = Instant::now() + TIMEOUT;
        while Instant::now() < deadline && self.disconnected.len() < count {
            self.pump();
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(self.disconnected.len(), count);
    }

    /// Asserts that the network closes `client` without sending it a byte.
    fn expect_refusal(&mut self, client: &mut dyn Read) {
        let deadline = Instant::now() + TIMEOUT;
        let mut received = Vec::new();
        while !read_available(client, &mut received) {
            assert!(Instant::now() < deadline, "the connection was not refused");
            self.pump();
        }
        assert!(received.is_empty(), "a refused connection was sent bytes: {received:?}");
    }

    /// Reads `client` to the end of its stream, driving the network meanwhile.
    fn read_to_end_of_stream(&mut self, client: &mut dyn Read) -> Vec<u8> {
        let deadline = Instant::now() + TIMEOUT;
        let mut received = Vec::new();
        while !read_available(client, &mut received) {
            assert!(Instant::now() < deadline, "the peer did not see the end of the stream");
            self.pump();
        }
        received
    }
}

over_both_transports!(
    a_client_arriving_at_the_cap_is_refused,
    a_client_arriving_at_the_cap_is_refused_tcp,
    a_client_arriving_at_the_cap_is_refused_unix
);
fn a_client_arriving_at_the_cap_is_refused(endpoint: &Endpoint) {
    let mut server = Server::new();
    let (group, bound) = server.listen(capped_group("capped", 2), endpoint);
    let endpoint = &bound;

    let mut first = connect_client(endpoint);
    let mut second = connect_client(endpoint);
    server.wait_for_accepts(group, 2);

    let mut third = connect_client(endpoint);
    server.expect_refusal(&mut *third);
    assert_eq!(server.refused(group), 1);
    assert_eq!(server.accepted_in(group).len(), 2, "the group accepted past its cap");

    // The connections that hold the cap are untouched by the refusal.
    assert!(!read_available(&mut *first, &mut Vec::new()));
    assert!(!read_available(&mut *second, &mut Vec::new()));
}

over_both_transports!(
    a_draining_connection_holds_its_place,
    a_draining_connection_holds_its_place_tcp,
    a_draining_connection_holds_its_place_unix
);
fn a_draining_connection_holds_its_place(endpoint: &Endpoint) {
    let payload = vec![0x5A; 4 * 1024 * 1024];
    let mut server = Server::new();
    let (group, bound) = server.listen(
        ConnectionGroupConfig {
            socket_buf_size: Some(4096),
            max_frame_size: payload.len(),
            backlog_warn_bytes: None,
            ..capped_group("draining", 1)
        },
        endpoint,
    );
    let endpoint = &bound;

    let mut first = connect_client(endpoint);
    let token = server.wait_for_accepts(group, 1)[0];
    assert!(server.network.send_with(token, |out| out.extend_from_slice(&payload)));
    assert!(server.network.disconnect_when_drained(token));

    // The peer reads none of the backlog, so the connection is still
    // draining when the next client arrives.
    let mut second = connect_client(endpoint);
    server.expect_refusal(&mut *second);
    assert_eq!(server.refused(group), 1);

    // Its place comes free when the drain finishes.
    let received = server.read_to_end_of_stream(&mut *first);
    assert_eq!(received.len(), payload.len());
    server.wait_for_disconnects(1);

    let _third = connect_client(endpoint);
    server.wait_for_accepts(group, 2);
    assert_eq!(server.refused(group), 1);
}

over_both_transports!(
    a_half_closed_connection_holds_its_place,
    a_half_closed_connection_holds_its_place_tcp,
    a_half_closed_connection_holds_its_place_unix
);
fn a_half_closed_connection_holds_its_place(endpoint: &Endpoint) {
    let mut server = Server::new();
    let (group, bound) = server.listen(capped_group("half-closed", 1), endpoint);
    let endpoint = &bound;

    let mut first = connect_client(endpoint);
    let token = server.wait_for_accepts(group, 1)[0];
    assert!(server.network.shutdown_write_when_drained(token));
    assert!(server.read_to_end_of_stream(&mut *first).is_empty());

    // The peer has read the end of the stream, and the connection is still
    // there to read what the peer sends.
    let mut second = connect_client(endpoint);
    server.expect_refusal(&mut *second);
    assert_eq!(server.refused(group), 1);

    drop(first);
    server.wait_for_disconnects(1);
    let _third = connect_client(endpoint);
    server.wait_for_accepts(group, 2);
    assert_eq!(server.refused(group), 1);
}

/// The accept loop drains the backlog whatever it makes of each connection:
/// an edge-triggered listener is told about a pending connection once.
#[test]
fn a_refusal_leaves_none_of_the_backlog_unaccepted() {
    let mut server = Server::new();
    let (group, endpoint) = server.listen(capped_group("backlog", 1), &ephemeral());

    let _first = connect_client(&endpoint);
    server.wait_for_accepts(group, 1);

    // Both arrive before the network looks at the listener again, so one
    // readiness event has to answer both.
    let mut second = connect_client(&endpoint);
    let mut third = connect_client(&endpoint);
    server.expect_refusal(&mut *second);
    server.expect_refusal(&mut *third);
    assert_eq!(server.refused(group), 2);
}

#[test]
fn a_closed_connection_frees_its_place() {
    let mut server = Server::new();
    let (group, endpoint) = server.listen(capped_group("freed", 1), &ephemeral());

    let first = connect_client(&endpoint);
    server.wait_for_accepts(group, 1);
    drop(first);
    server.wait_for_disconnects(1);

    let _second = connect_client(&endpoint);
    server.wait_for_accepts(group, 2);
    assert_eq!(server.refused(group), 0, "the group refused a client it had room for");
}

/// The client is left where it is, so the place comes free because the network
/// let the connection go rather than because the peer went.
#[test]
fn a_disconnected_connection_frees_its_place() {
    let mut server = Server::new();
    let (group, endpoint) = server.listen(capped_group("disconnected", 1), &ephemeral());

    let _first = connect_client(&endpoint);
    let token = server.wait_for_accepts(group, 1)[0];
    assert!(server.network.disconnect(token));
    server.wait_for_disconnects(1);

    let _second = connect_client(&endpoint);
    server.wait_for_accepts(group, 2);
    assert_eq!(server.refused(group), 0, "the group refused a client it had room for");
}

/// A removed connection leaves no [`StreamEvent::Disconnected`] behind, so the
/// place it held is freed by the removal itself.
#[test]
fn a_removed_connection_frees_its_place() {
    let mut server = Server::new();
    let (group, endpoint) = server.listen(capped_group("removed", 1), &ephemeral());

    let _first = connect_client(&endpoint);
    let token = server.wait_for_accepts(group, 1)[0];
    assert!(server.network.remove(token));

    let _second = connect_client(&endpoint);
    server.wait_for_accepts(group, 2);
    assert_eq!(server.refused(group), 0, "the group refused a client it had room for");
    assert!(server.disconnected.is_empty(), "a removed connection was reported closed");
}

#[test]
fn the_cap_belongs_to_one_group() {
    let mut server = Server::new();
    let (capped, capped_endpoint) = server.listen(capped_group("capped", 1), &ephemeral());
    let (open, open_endpoint) = server.listen(raw_group("open"), &ephemeral());

    let _first = connect_client(&capped_endpoint);
    server.wait_for_accepts(capped, 1);
    let mut refused = connect_client(&capped_endpoint);
    server.expect_refusal(&mut *refused);

    let _open_clients = [connect_client(&open_endpoint), connect_client(&open_endpoint)];
    server.wait_for_accepts(open, 2);
    assert_eq!(server.refused(capped), 1);
    assert_eq!(server.refused(open), 0);
}

/// The cap is the group's rather than a listener's: two listeners of one group
/// draw on the same allowance, whichever transport each of them speaks.
#[test]
fn two_listeners_of_one_group_share_its_cap() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::new();
    let (group, tcp) = server.listen(capped_group("shared", 1), &ephemeral());
    let unix = server.network.listen(group, Endpoint::Unix(dir.path().join("s"))).unwrap();

    let first = connect_client(&tcp);
    server.wait_for_accepts(group, 1);

    // The one place is filled, and the second listener has none of its own.
    let mut refused = connect_client(&unix);
    server.expect_refusal(&mut *refused);
    assert_eq!(server.refused(group), 1);

    drop(first);
    server.wait_for_disconnects(1);
    let _second = connect_client(&unix);
    server.wait_for_accepts(group, 2);
    assert_eq!(server.refused(group), 1, "the group refused a client it had room for");
}

#[test]
fn an_outbound_endpoint_holds_no_place() {
    let mut server = Server::new();
    let (remote, remote_endpoint) = server.listen(raw_group("remote"), &ephemeral());
    let (capped, capped_endpoint) = server.listen(
        ConnectionGroupConfig {
            reconnect_interval: flux_timing::Duration::from_millis(1),
            ..capped_group("capped", 1)
        },
        &ephemeral(),
    );

    // The capped group holds an outbound endpoint of its own: a connection
    // it made rather than accepted, which the cap counts for nothing.
    let outbound = server.network.connect(capped, remote_endpoint);
    server.wait_for_accepts(remote, 1);
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && !server.connected.contains(&outbound) {
        server.pump();
        thread::sleep(Duration::from_millis(1));
    }
    assert!(server.connected.contains(&outbound), "the outbound endpoint never connected");

    let _client = connect_client(&capped_endpoint);
    server.wait_for_accepts(capped, 1);
    assert_eq!(server.refused(capped), 0);
}

#[test]
#[should_panic(expected = "max_connections must be nonzero")]
fn a_cap_of_zero_is_rejected() {
    let _ = StreamNetwork::default().add_group(capped_group("zero", 0));
}

/// One iteration of an HTTP server: drive the network, then answer every
/// request pulled from the service.
fn serve(network: &mut StreamNetwork, service: &mut HttpService) {
    network.drive(Some(flux_timing::Duration::ZERO), &mut [&mut service]);
    let mut requests = Vec::new();
    while let Some(event) = service.next_event() {
        if let HttpEvent::Request { token, .. } = event {
            requests.push(token);
        }
    }
    for token in requests {
        assert!(service.respond(token, 200, &[], BODY));
    }
}

/// The cap is the transport's, so a protocol layer above it needs nothing:
/// an `HttpService` never hears of the connection its group refused.
#[test]
fn an_http_service_serves_its_clients_while_the_cap_refuses_another() {
    let mut network = StreamNetwork::default();
    let group = network.add_group(ConnectionGroupConfig {
        name: "http",
        framing: Framing::Raw,
        max_frame_size: usize::MAX,
        max_connections: Some(2),
        ..ConnectionGroupConfig::default()
    });
    let mut service = HttpService::new(group, HttpConfig::default());
    let endpoint = service.listen(ephemeral()).unwrap();

    let mut served = [connect_client(&endpoint), connect_client(&endpoint)];
    let mut answers = [Vec::new(), Vec::new()];
    for client in &mut served {
        client.write_all(REQUEST).unwrap();
    }
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && !answers.iter().all(|answer| answer.ends_with(BODY)) {
        serve(&mut network, &mut service);
        for (client, answer) in served.iter_mut().zip(&mut answers) {
            read_available(&mut **client, answer);
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(answers.iter().all(|answer| answer.ends_with(BODY)), "{answers:?}");

    // The third client is closed by the transport, so its request never
    // becomes a request the service sees.
    let mut refused = connect_client(&endpoint);
    refused.write_all(REQUEST).unwrap();
    let mut answer = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while !read_available(&mut *refused, &mut answer) {
        assert!(Instant::now() < deadline, "the third client was served");
        serve(&mut network, &mut service);
    }
    assert!(answer.is_empty(), "the refused client was answered: {answer:?}");
    assert_eq!(network.refused_connections(group), 1);

    // The clients holding the cap are unaffected: they are still served.
    for (client, answer) in served.iter_mut().zip(&mut answers) {
        answer.clear();
        client.write_all(REQUEST).unwrap();
    }
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && !answers.iter().all(|answer| answer.ends_with(BODY)) {
        serve(&mut network, &mut service);
        for (client, answer) in served.iter_mut().zip(&mut answers) {
            read_available(&mut **client, answer);
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(answers.iter().all(|answer| answer.ends_with(BODY)), "{answers:?}");

    service.close(&mut network);
}

/// One iteration of an HTTP server with nothing to answer, reporting whether
/// the service accepted a client.
fn drive_service(network: &mut StreamNetwork, service: &mut HttpService) -> bool {
    network.drive(Some(flux_timing::Duration::ZERO), &mut [&mut service]);
    let mut accepted = false;
    while let Some(event) = service.next_event() {
        accepted |= matches!(event, HttpEvent::Accepted { .. });
    }
    accepted
}

/// Closing a group empties it and hands it back reusable, so it listens again
/// and admits a client: the places its connections held went with them.
/// `HttpService::close` is the public way to close a group.
#[test]
fn a_closed_group_frees_the_places_it_held() {
    let mut server = Server::new();
    let group = server.network.add_group(capped_group("closed", 1));
    let mut service = HttpService::new(&mut server.network, group, HttpConfig::default());
    let endpoint = service.listen(&mut server.network, ephemeral()).unwrap();

    let _first = connect_client(&endpoint);
    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = false;
    while Instant::now() < deadline && !accepted {
        accepted = drive_service(&mut server.network, &mut service);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(accepted, "the service never accepted a client");

    // The one place is filled, which is what the next client meets.
    let mut refused = connect_client(&endpoint);
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while !read_available(&mut *refused, &mut received) {
        assert!(Instant::now() < deadline, "the connection was not refused");
        drive_service(&mut server.network, &mut service);
    }
    assert!(received.is_empty(), "a refused connection was sent bytes: {received:?}");
    assert_eq!(server.refused(group), 1);

    service.close(&mut server.network);

    let reopened_endpoint = server.network.listen(group, ephemeral()).unwrap();
    let _client = connect_client(&reopened_endpoint);
    server.wait_for_accepts(group, 1);
    assert_eq!(server.refused(group), 1, "the group refused a client it had room for");
}
