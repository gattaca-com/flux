//! The lingering close: what a peer reads when the service answers a request
//! stream it never read to the end, and how such a connection ends.

use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr},
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Token,
    http::{HttpConfig, HttpEvent, HttpService, Linger},
    stream::{ConnectionGroup, ConnectionGroupConfig, Endpoint, Framing, StreamNetwork},
};

const TIMEOUT: Duration = Duration::from_secs(10);

/// The bad requests the service answers itself, with the status each earns
/// under a 64-byte head cap and an 8-byte body cap.
const REJECTED: [(&[u8], u16); 4] = [
    (b"nope\r\n\r\n", 400),
    (
        b"GET / HTTP/1.1\r\nX: 1234567890123456789012345678901234567890123456789012345678901234\r\n\r\n",
        431,
    ),
    (b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n", 501),
    (b"POST / HTTP/1.1\r\nContent-Length: 9\r\n\r\n", 413),
];

/// A loopback address no listener holds.
fn unused_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

/// Runs one test body over both transports.
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

/// A nonblocking client socket speaking raw bytes to the server under test.
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

/// Reads what the client can without blocking, reporting the end of the
/// stream.
///
/// A hard close resets bytes still in flight from the client, which is as
/// much an end of stream as the clean one a half-close sends.
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

fn linger(idle: Duration, total: Duration) -> Linger {
    Linger { idle: idle.into(), total: total.into() }
}

fn small_heads() -> HttpConfig {
    HttpConfig::default().with_max_head_bytes(64).with_max_body_bytes(8)
}

/// A listening HTTP service, and what it has delivered so far.
struct Server {
    net: StreamNetwork,
    service: HttpService,
    group: ConnectionGroup,
    accepted: Vec<Token>,
    disconnected: Vec<Token>,
    requests: Vec<Token>,
}

impl Server {
    fn build(endpoint: &Endpoint, group: ConnectionGroupConfig, config: HttpConfig) -> Self {
        let mut net = StreamNetwork::default();
        let group = net.add_group(group);
        let mut service = HttpService::new(&mut net, group, config);
        service.listen(&mut net, endpoint.clone()).unwrap();
        Self {
            net,
            service,
            group,
            accepted: Vec::new(),
            disconnected: Vec::new(),
            requests: Vec::new(),
        }
    }

    fn new(endpoint: &Endpoint, config: HttpConfig) -> Self {
        Self::build(endpoint, raw_group(), config)
    }

    /// One iteration: drive the network, then pull every protocol event.
    /// Requests are left for the test to answer by token.
    fn pump(&mut self) {
        let Self { net, service, accepted, disconnected, requests, .. } = self;
        net.drive(Some(Duration::ZERO.into()), &mut [service.as_service()], |_| {});
        while let Some(event) = service.next_event(net) {
            match event {
                HttpEvent::Accepted { token, .. } => accepted.push(token),
                HttpEvent::Disconnected { token } => disconnected.push(token),
                HttpEvent::Request { token, .. } => requests.push(token),
                _ => {}
            }
        }
    }

    /// Pumps until `done` holds, failing with `what` at the deadline.
    fn wait_until(&mut self, done: impl Fn(&Self) -> bool, what: &str) {
        let deadline = Instant::now() + TIMEOUT;
        while !done(self) {
            assert!(Instant::now() < deadline, "{what}");
            self.pump();
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Pumps for `how_long`, so that anything the service would do meanwhile
    /// has happened by the time the call returns.
    fn pump_for(&mut self, how_long: Duration) {
        let until = Instant::now() + how_long;
        while Instant::now() < until {
            self.pump();
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn accepted_token(&mut self) -> Token {
        self.wait_until(|server| !server.accepted.is_empty(), "the client was not accepted");
        self.accepted[0]
    }

    fn respond(&mut self, token: Token, status: u16, body: &[u8]) -> bool {
        self.service.respond(&mut self.net, token, status, &[], body)
    }

    /// Reads the client to the end of the stream, driving the server
    /// meanwhile.
    fn read_to_end_of_stream(&mut self, client: &mut dyn Read) -> Vec<u8> {
        let mut received = Vec::new();
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if read_available(client, &mut received) {
                return received;
            }
            assert!(Instant::now() < deadline, "the peer did not see the end of the stream");
            self.pump();
            thread::sleep(Duration::from_millis(1));
        }
    }
}

fn raw_group() -> ConnectionGroupConfig {
    ConnectionGroupConfig {
        name: "linger",
        framing: Framing::Raw,
        max_frame_size: usize::MAX,
        backlog_warn_bytes: None,
        ..ConnectionGroupConfig::default()
    }
}

fn head_of(received: &[u8]) -> &str {
    let head =
        received.windows(4).position(|bytes| bytes == b"\r\n\r\n").expect("no response head");
    std::str::from_utf8(&received[..head]).unwrap()
}

over_both_transports!(
    an_error_response_reaches_a_peer_that_is_still_sending,
    an_error_response_reaches_a_peer_that_is_still_sending_tcp,
    an_error_response_reaches_a_peer_that_is_still_sending_unix
);
fn an_error_response_reaches_a_peer_that_is_still_sending(endpoint: &Endpoint) {
    for (request, status) in REJECTED {
        let mut server = Server::new(endpoint, small_heads());
        let mut client = connect_client(endpoint);
        let token = server.accepted_token();
        client.write_all(request).unwrap();

        // The whole answer arrives, and then the end of the stream: the write
        // side alone was shut.
        let received = server.read_to_end_of_stream(&mut *client);
        let head = head_of(&received);
        assert!(head.starts_with(&format!("HTTP/1.1 {status} ")), "{head}");
        assert!(head.contains("Connection: close"), "{head}");

        // What the peer sends after it is read and dropped, so the connection
        // neither grows nor closes under a client still uploading.
        let buffered = server.service.buffered(token).expect("the connection is still held");
        for _ in 0..8 {
            client.write_all(&[b'x'; 4096]).unwrap();
            server.pump_for(Duration::from_millis(5));
        }
        assert_eq!(server.service.buffered(token), Some(buffered), "the discarded bytes were kept");
        assert!(server.disconnected.is_empty(), "the peer was cut off mid-upload");
    }
}

over_both_transports!(
    a_lingering_connection_ends_at_the_peers_close,
    a_lingering_connection_ends_at_the_peers_close_tcp,
    a_lingering_connection_ends_at_the_peers_close_unix
);
fn a_lingering_connection_ends_at_the_peers_close(endpoint: &Endpoint) {
    // Caps far longer than the test: only the peer's close can end this.
    let config = small_heads().with_linger(linger(TIMEOUT * 3, TIMEOUT * 6));
    let mut server = Server::new(endpoint, config);
    let mut client = connect_client(endpoint);
    let token = server.accepted_token();
    client.write_all(b"nope\r\n\r\n").unwrap();

    let received = server.read_to_end_of_stream(&mut *client);
    assert!(head_of(&received).starts_with("HTTP/1.1 400 "));
    server.pump_for(Duration::from_millis(20));
    assert!(server.disconnected.is_empty(), "the linger ended without the peer");

    drop(client);
    server.wait_until(
        |server| !server.disconnected.is_empty(),
        "the peer's close did not end the linger",
    );
    assert_eq!(server.disconnected, [token]);
}

#[test]
fn a_lingering_connection_ends_at_the_idle_cap() {
    let idle = Duration::from_millis(150);
    let config = small_heads().with_linger(linger(idle, TIMEOUT * 3));
    let endpoint = Endpoint::Tcp(unused_addr());
    let mut server = Server::new(&endpoint, config);
    let mut client = connect_client(&endpoint);
    server.accepted_token();

    let sent = Instant::now();
    client.write_all(b"nope\r\n\r\n").unwrap();
    let received = server.read_to_end_of_stream(&mut *client);
    assert!(head_of(&received).starts_with("HTTP/1.1 400 "));

    // Nothing else arrives, so the idle cap is what ends it — and not before.
    server.wait_until(|server| !server.disconnected.is_empty(), "the idle cap did not fire");
    assert!(sent.elapsed() >= idle, "the linger ended after {:?}", sent.elapsed());
}

#[test]
fn a_lingering_connection_ends_at_the_total_cap() {
    let idle = Duration::from_millis(150);
    let total = Duration::from_millis(600);
    let config = small_heads().with_linger(linger(idle, total));
    let endpoint = Endpoint::Tcp(unused_addr());
    let mut server = Server::new(&endpoint, config);
    let mut client = connect_client(&endpoint);
    server.accepted_token();

    let sent = Instant::now();
    client.write_all(b"nope\r\n\r\n").unwrap();
    let received = server.read_to_end_of_stream(&mut *client);
    assert!(head_of(&received).starts_with("HTTP/1.1 400 "));

    // A peer that keeps sending keeps clearing the idle cap, so the total cap
    // is the one that ends it.
    let deadline = Instant::now() + TIMEOUT;
    while server.disconnected.is_empty() {
        assert!(Instant::now() < deadline, "the total cap did not fire");
        let _ = client.write_all(b"x");
        server.pump();
        thread::sleep(Duration::from_millis(10));
    }
    assert!(sent.elapsed() >= total, "the linger ended after {:?}", sent.elapsed());
}

#[test]
fn a_lingering_connection_outlives_a_shorter_idle_timeout() {
    let total = Duration::from_millis(400);
    let config = small_heads()
        .with_idle_timeout(Duration::from_millis(50).into())
        .with_linger(linger(TIMEOUT * 3, total));
    let endpoint = Endpoint::Tcp(unused_addr());
    let mut server = Server::new(&endpoint, config);
    let mut client = connect_client(&endpoint);
    server.accepted_token();

    let sent = Instant::now();
    client.write_all(b"nope\r\n\r\n").unwrap();
    let received = server.read_to_end_of_stream(&mut *client);
    assert!(head_of(&received).starts_with("HTTP/1.1 400 "));

    // The sweep would have taken it four times over; the linger's own caps
    // are what a closing connection answers to.
    server.pump_for(Duration::from_millis(200));
    assert!(server.disconnected.is_empty(), "the idle sweep took a lingering connection");
    server.wait_until(|server| !server.disconnected.is_empty(), "the total cap did not fire");
    assert!(sent.elapsed() >= total, "the linger ended after {:?}", sent.elapsed());
}

over_both_transports!(
    a_lingering_connection_holds_its_place,
    a_lingering_connection_holds_its_place_tcp,
    a_lingering_connection_holds_its_place_unix
);
fn a_lingering_connection_holds_its_place(endpoint: &Endpoint) {
    let config = small_heads().with_linger(linger(TIMEOUT * 3, TIMEOUT * 6));
    let group = ConnectionGroupConfig { max_connections: Some(1), ..raw_group() };
    let mut server = Server::build(endpoint, group, config);
    let mut client = connect_client(endpoint);
    let token = server.accepted_token();
    client.write_all(b"nope\r\n\r\n").unwrap();
    let received = server.read_to_end_of_stream(&mut *client);
    assert!(head_of(&received).starts_with("HTTP/1.1 400 "));

    // A connection reading its peer out is a connection the group holds.
    let mut refused = connect_client(endpoint);
    assert!(server.read_to_end_of_stream(&mut *refused).is_empty());
    assert_eq!(server.net.refused_connections(server.group), 1);

    drop(client);
    server.wait_until(|server| server.disconnected == [token], "the linger did not end");
    let mut next = connect_client(endpoint);
    server.wait_until(|server| server.accepted.len() == 2, "the freed place was not taken");
    next.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    server.wait_until(|server| !server.requests.is_empty(), "the new client was not served");
    assert_eq!(server.net.refused_connections(server.group), 1);
}

over_both_transports!(
    an_over_limit_pending_request_is_answered_before_the_linger,
    an_over_limit_pending_request_is_answered_before_the_linger_tcp,
    an_over_limit_pending_request_is_answered_before_the_linger_unix
);
fn an_over_limit_pending_request_is_answered_before_the_linger(endpoint: &Endpoint) {
    let config = HttpConfig::default()
        .with_max_head_bytes(64)
        .with_max_body_bytes(64)
        .with_linger(linger(TIMEOUT * 3, TIMEOUT * 6));
    let mut server = Server::new(endpoint, config);
    let mut client = connect_client(endpoint);
    let token = server.accepted_token();
    client.write_all(b"GET /slow HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    server.wait_until(|server| !server.requests.is_empty(), "the request was not delivered");

    // The pipelined bytes behind it run the connection over its limit while
    // the answer is still being produced.
    client.write_all(&[b'x'; 256]).unwrap();
    server.wait_until(|server| server.service.buffered(token) == Some(128), "the cap was not hit");
    server.pump_for(Duration::from_millis(20));
    assert!(server.disconnected.is_empty(), "the pending request was abandoned");

    assert!(server.respond(token, 200, b"answered"));
    let received = server.read_to_end_of_stream(&mut *client);
    let head = head_of(&received);
    assert!(head.starts_with("HTTP/1.1 200 "), "{head}");
    assert!(head.contains("Connection: close"), "{head}");
    assert!(received.ends_with(b"answered"), "the answer was cut short");

    // And the answer is delivered over a linger, not a reset.
    client.write_all(&[b'x'; 4096]).unwrap();
    server.pump_for(Duration::from_millis(20));
    assert!(server.disconnected.is_empty(), "the answered connection did not linger");
}

over_both_transports!(
    a_completed_request_answered_with_close_drains,
    a_completed_request_answered_with_close_drains_tcp,
    a_completed_request_answered_with_close_drains_unix
);
fn a_completed_request_answered_with_close_drains(endpoint: &Endpoint) {
    // The caps outlast the test, so a connection that lingered here would
    // still be open when the assertion runs.
    let config = HttpConfig::default().with_linger(linger(TIMEOUT * 3, TIMEOUT * 6));
    let mut server = Server::new(endpoint, config);
    let mut client = connect_client(endpoint);
    let token = server.accepted_token();
    client.write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").unwrap();
    server.wait_until(|server| !server.requests.is_empty(), "the request was not delivered");
    assert!(server.respond(token, 200, b"ok"));

    let received = server.read_to_end_of_stream(&mut *client);
    assert!(received.ends_with(b"ok"), "the answer was cut short");
    server
        .wait_until(|server| server.disconnected == [token], "the answered request did not close");
}

over_both_transports!(
    an_http_1_0_request_is_answered_and_closed,
    an_http_1_0_request_is_answered_and_closed_tcp,
    an_http_1_0_request_is_answered_and_closed_unix
);
fn an_http_1_0_request_is_answered_and_closed(endpoint: &Endpoint) {
    let config = HttpConfig::default().with_linger(linger(TIMEOUT * 3, TIMEOUT * 6));
    let mut server = Server::new(endpoint, config);
    let mut client = connect_client(endpoint);
    let token = server.accepted_token();
    client.write_all(b"GET / HTTP/1.0\r\nHost: x\r\n\r\n").unwrap();
    server.wait_until(|server| !server.requests.is_empty(), "the request was not delivered");
    assert!(server.respond(token, 200, b"ok"));

    let received = server.read_to_end_of_stream(&mut *client);
    assert!(received.ends_with(b"ok"), "the answer was cut short");
    server
        .wait_until(|server| server.disconnected == [token], "the answered request did not close");
}

over_both_transports!(
    without_linger_an_error_response_closes,
    without_linger_an_error_response_closes_tcp,
    without_linger_an_error_response_closes_unix
);
fn without_linger_an_error_response_closes(endpoint: &Endpoint) {
    let mut server = Server::new(endpoint, small_heads().without_linger());
    let mut client = connect_client(endpoint);
    let token = server.accepted_token();
    client.write_all(b"nope\r\n\r\n").unwrap();

    server.read_to_end_of_stream(&mut *client);
    server.wait_until(|server| server.disconnected == [token], "the error response did not close");
}
