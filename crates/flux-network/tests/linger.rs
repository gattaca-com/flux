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
    /// The body every request is answered with where it is delivered,
    /// leaving nothing for the test to answer by token.
    inline_answer: Option<Vec<u8>>,
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
            inline_answer: None,
        }
    }

    fn new(endpoint: &Endpoint, config: HttpConfig) -> Self {
        Self::build(endpoint, raw_group(), config)
    }

    /// One iteration: drive the network, then pull every protocol event.
    /// Requests are left for the test to answer by token.
    ///
    /// Reports what the drive found: a readiness event routed to the service,
    /// or events the service was already holding.
    fn pump(&mut self) -> bool {
        let Self { net, service, accepted, disconnected, requests, inline_answer, .. } = self;
        let worked = net.drive(Some(Duration::ZERO.into()), &mut [service.as_service()], |_| {});
        while let Some(event) = service.next_event(net) {
            match event {
                HttpEvent::Accepted { token, .. } => accepted.push(token),
                HttpEvent::Disconnected { token } => disconnected.push(token),
                HttpEvent::Request { token, responder, .. } => {
                    requests.push(token);
                    if let Some(body) = inline_answer {
                        assert!(responder.respond(200, &[], body), "the inline answer was refused");
                    }
                }
                _ => {}
            }
        }
        worked
    }

    /// Pumps until an iteration takes what the peer sent, failing with `what`
    /// at the deadline.
    ///
    /// An accepted connection is polled for reads alone until the service has
    /// something to write, and a raw read empties the socket, so between a
    /// request and its answer the one iteration that reports work is the one
    /// that read the whole of what the peer sent behind it.
    fn wait_for_a_read(&mut self, what: &str) {
        let deadline = Instant::now() + TIMEOUT;
        while !self.pump() {
            assert!(Instant::now() < deadline, "{what}");
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// One iteration the poll may block in, and how long it waited.
    fn drive_blocking(&mut self) -> Duration {
        let Self { net, service, .. } = self;
        let started = Instant::now();
        net.drive(None, &mut [service.as_service()], |_| {});
        started.elapsed()
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
        server.accepted_token();
        client.write_all(request).unwrap();

        // The whole answer arrives, and then the end of the stream: the write
        // side alone was shut.
        let received = server.read_to_end_of_stream(&mut *client);
        let head = head_of(&received);
        assert!(head.starts_with(&format!("HTTP/1.1 {status} ")), "{head}");
        assert!(head.contains("Connection: close"), "{head}");

        // What the peer sends after it is read out rather than reset, so a
        // client still uploading is neither cut off nor left to block.
        for _ in 0..8 {
            client.write_all(&[b'x'; 4096]).unwrap();
            server.pump_for(Duration::from_millis(5));
        }
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
    server.wait_for_a_read("the overrun did not reach the service");
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

#[test]
fn the_caps_wait_for_the_answer_to_reach_the_peer() {
    let idle = Duration::from_millis(200);
    let body = vec![7; 16 * 1024 * 1024];
    let group = ConnectionGroupConfig { socket_buf_size: Some(16 * 1024), ..raw_group() };
    let config = HttpConfig::default()
        .with_max_head_bytes(64)
        .with_max_body_bytes(64)
        .with_linger(linger(idle, TIMEOUT * 3));
    let endpoint = Endpoint::Tcp(unused_addr());
    let mut server = Server::build(&endpoint, group, config);
    let mut client = connect_client(&endpoint);
    let token = server.accepted_token();
    client.write_all(b"GET /slow HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    server.wait_until(|server| !server.requests.is_empty(), "the request was not delivered");
    client.write_all(&[b'x'; 256]).unwrap();
    server.wait_for_a_read("the overrun did not reach the service");
    server.pump_for(Duration::from_millis(20));
    assert!(server.respond(token, 200, &body));

    // An answer larger than the socket takes many writes to deliver, and the
    // peer reads none of it for twice the idle cap.
    server.pump_for(idle * 2);
    assert!(server.disconnected.is_empty(), "the caps ran while the answer was still queued");

    // All of it reaches the peer, and the end of the stream after it.
    let received = server.read_to_end_of_stream(&mut *client);
    let head = head_of(&received);
    assert!(head.contains(&format!("Content-Length: {}", body.len())), "{head}");
    assert!(head.contains("Connection: close"), "{head}");
    assert_eq!(received.len(), head.len() + 4 + body.len(), "the answer was cut short");
    assert!(received.ends_with(&body[body.len() - 1024..]), "the answer arrived corrupted");

    // Only now do the caps run: the peer holds the connection open and sends
    // nothing, and the idle cap ends it.
    let delivered = Instant::now();
    server.wait_until(|server| !server.disconnected.is_empty(), "the idle cap did not fire");
    assert!(delivered.elapsed() >= idle / 2, "the cap fired after {:?}", delivered.elapsed());
}

/// A request of exactly the buffer limit: a 64-byte head and a 64-byte body,
/// so the byte after it is the one the limit drops.
fn request_of_the_whole_limit() -> Vec<u8> {
    let mut request =
        b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 64\r\nXpad: xxxxxxxx\r\n\r\n".to_vec();
    assert_eq!(request.len(), 64, "the head must fill its own cap");
    request.extend_from_slice(&[b'b'; 64]);
    request
}

#[test]
fn an_inline_answer_at_the_limit_matches_one_answered_before_a_tick() {
    assert_eq!(
        over_the_limit_at_a_request_boundary(true, false),
        over_the_limit_at_a_request_boundary(false, false)
    );
}

/// Answers a request that ends exactly at the buffer limit, inline or by
/// token, and reports what the peer read before the end of the stream.
///
/// Two closes are correct here, and which one the peer reads is settled by
/// whether a tick falls between the request and its answer. Answered first,
/// the connection is idle when the tick finds it over its limit, and the byte
/// the limit dropped earns a `431` of its own. Answered after that tick, the
/// answer already carries the close and there is nothing left to reject.
fn over_the_limit_at_a_request_boundary(inline: bool, after_a_tick: bool) -> String {
    let config = HttpConfig::default()
        .with_max_head_bytes(64)
        .with_max_body_bytes(64)
        .with_linger(linger(TIMEOUT * 3, TIMEOUT * 6));
    let endpoint = Endpoint::Tcp(unused_addr());
    let mut server = Server::new(&endpoint, config);
    if inline {
        server.inline_answer = Some(b"ok".to_vec());
    }
    let mut client = connect_client(&endpoint);
    let token = server.accepted_token();
    let mut request = request_of_the_whole_limit();
    // The byte past the limit is dropped, so the client is mid-stream from
    // here on however the request before it was answered.
    request.push(b'x');
    client.write_all(&request).unwrap();
    server.wait_until(|server| !server.requests.is_empty(), "the request was not delivered");
    if after_a_tick {
        server.pump_for(Duration::from_millis(20));
    }
    if !inline {
        assert!(server.respond(token, 200, b"ok"));
    }

    let received = server.read_to_end_of_stream(&mut *client);
    // The peer is told what happened rather than reset, and the connection
    // reads it out afterwards.
    client.write_all(&[b'x'; 4096]).unwrap();
    server.pump_for(Duration::from_millis(20));
    assert!(server.disconnected.is_empty(), "the connection closed without lingering");
    String::from_utf8(received).unwrap()
}

#[test]
fn an_over_limit_request_boundary_is_answered() {
    let received = over_the_limit_at_a_request_boundary(true, false);
    assert!(received.starts_with("HTTP/1.1 200 OK\r\n"), "{received}");
    assert!(received.contains("Connection: keep-alive\r\n"), "{received}");
    assert!(received.contains("HTTP/1.1 431 Request Header Fields Too Large\r\n"), "{received}");
    assert!(received.ends_with("Connection: close\r\n\r\n"), "{received}");
}

#[test]
fn a_blocking_drive_wakes_for_the_idle_cap() {
    let idle = Duration::from_millis(500);
    let config = small_heads().without_idle_timeout().with_linger(linger(idle, TIMEOUT * 3));
    let addr = unused_addr();
    let endpoint = Endpoint::Tcp(addr);
    let mut server = Server::new(&endpoint, config);
    let mut client = connect_client(&endpoint);
    server.accepted_token();
    client.write_all(b"nope\r\n\r\n").unwrap();
    let received = server.read_to_end_of_stream(&mut *client);
    assert!(head_of(&received).starts_with("HTTP/1.1 400 "));

    // The answer shut the write side as it was written, so the linger's clock
    // is running and its idle cap is the only deadline the network has: an
    // uncapped drive waits for it and no longer. The late connection is the
    // test's own deadline: a drive that ignored the cap would wake on it
    // instead, well past the bound below, rather than hang.
    thread::spawn(move || {
        thread::sleep(Duration::from_secs(3));
        drop(std::net::TcpStream::connect(addr));
    });
    let waited = server.drive_blocking();
    assert!(waited >= Duration::from_millis(100), "returned at once: {waited:?}");
    assert!(waited < Duration::from_secs(2), "the linger cap was not folded: {waited:?}");
    server.wait_until(|server| !server.disconnected.is_empty(), "the idle cap did not fire");
}

over_both_transports!(
    an_undelivered_answer_is_swept,
    an_undelivered_answer_is_swept_tcp,
    an_undelivered_answer_is_swept_unix
);
fn an_undelivered_answer_is_swept(endpoint: &Endpoint) {
    let body = vec![7; 4 * 1024 * 1024];
    let timeout = Duration::from_millis(300);
    let group = ConnectionGroupConfig { socket_buf_size: Some(16 * 1024), ..raw_group() };
    let config = HttpConfig::default()
        .with_max_head_bytes(64)
        .with_max_body_bytes(64)
        .with_idle_timeout(timeout.into())
        .with_linger(linger(Duration::from_secs(5), Duration::from_secs(30)));
    let mut server = Server::build(endpoint, group, config);
    let mut client = connect_client(endpoint);
    let token = server.accepted_token();
    client.write_all(b"GET /slow HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    server.wait_until(|server| !server.requests.is_empty(), "the request was not delivered");
    client.write_all(&[b'x'; 256]).unwrap();
    server.wait_for_a_read("the overrun did not reach the service");
    server.pump_for(Duration::from_millis(20));
    let answered = Instant::now();
    assert!(server.respond(token, 200, &body));

    // The peer takes none of the answer and sends nothing more, so the caps
    // that time the reading and discarding never start. The sweep that holds
    // every other connection holds this one.
    server.wait_until(|server| !server.disconnected.is_empty(), "the sweep left it open");
    let waited = answered.elapsed();
    assert!(waited >= Duration::from_millis(200), "the connection went after {waited:?}");
    assert!(waited < Duration::from_secs(2), "the sweep did not run: {waited:?}");

    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while !read_available(&mut *client, &mut received) {
        assert!(Instant::now() < deadline, "the connection was not closed");
    }
    assert!(received.len() < body.len(), "a peer that read nothing was sent the whole answer");
}

#[test]
fn the_caps_start_when_the_answer_has_left() {
    let idle = Duration::from_millis(200);
    let answer = vec![7; 256 * 1024];
    let addr = unused_addr();
    let endpoint = Endpoint::Tcp(addr);
    let group = ConnectionGroupConfig { socket_buf_size: Some(16 * 1024), ..raw_group() };
    let config = HttpConfig::default()
        .with_max_head_bytes(64)
        .with_max_body_bytes(64)
        .with_linger(linger(idle, TIMEOUT * 3));
    let mut server = Server::build(&endpoint, group, config);

    // The peer sends more than the connection may hold and then reads nothing
    // for half a second. The answer is larger than the sockets between them
    // can hold, so the last of it is written where the pause ends, and the
    // peer reads the whole answer and the end of the stream behind it: that
    // is where the answer has left, and where the caps must start.
    let (delivered, read_back) = std::sync::mpsc::channel();
    let (ask_for_the_overrun, overrun_asked) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let mut client = std::net::TcpStream::connect(addr).unwrap();
        client.write_all(b"GET /slow HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        overrun_asked.recv().unwrap();
        client.write_all(&[b'x'; 256]).unwrap();
        thread::sleep(Duration::from_millis(500));
        let mut received = 0;
        let mut buffer = vec![0; 1024 * 1024];
        loop {
            match client.read(&mut buffer).unwrap() {
                0 => break,
                read => received += read,
            }
        }
        delivered.send((Instant::now(), received)).unwrap();
        // Held open, sending nothing, so the idle cap is what ends it.
        thread::sleep(Duration::from_secs(3));
    });

    let token = server.accepted_token();
    server.wait_until(|server| !server.requests.is_empty(), "the request was not delivered");
    // The overrun follows the request rather than riding with it, so the read
    // the gate below waits for is the one that takes it.
    ask_for_the_overrun.send(()).unwrap();
    server.wait_for_a_read("the overrun did not reach the service");
    server.pump_for(Duration::from_millis(20));
    let answered = Instant::now();
    assert!(server.respond(token, 200, &answer));

    // From here the server only blocks, so the poll wait is the whole of each
    // iteration: the caps must run from where that wait ended.
    let ended = loop {
        {
            let Server { net, service, disconnected, .. } = &mut server;
            net.drive(None, &mut [service.as_service()], |_| {});
            while let Some(event) = service.next_event(net) {
                if let HttpEvent::Disconnected { token } = event {
                    disconnected.push(token);
                }
            }
        }
        if !server.disconnected.is_empty() {
            break Instant::now();
        }
    };

    let (left, received) = read_back.recv_timeout(TIMEOUT).unwrap();
    assert!(received >= answer.len(), "the peer was sent {received} of {}", answer.len());
    // The peer took none of the answer until its pause was over, so an answer
    // capped where it was queued would have taken the connection with it.
    let queued = left.duration_since(answered);
    assert!(queued >= Duration::from_millis(400), "the answer left after {queued:?}");
    let ran = ended.duration_since(left);
    assert!(ran >= Duration::from_millis(150), "the caps ran for {ran:?}");
    assert!(ran < Duration::from_secs(2), "the caps did not end the linger: {ran:?}");
}

#[test]
fn a_deferred_answer_after_a_tick_at_the_limit_closes() {
    // The tick that falls before the answer finds the connection over its
    // limit with the request still pending, so the answer carries the close
    // itself and no `431` follows it.
    let received = over_the_limit_at_a_request_boundary(false, true);
    assert!(received.starts_with("HTTP/1.1 200 OK\r\n"), "{received}");
    assert!(!received.contains("keep-alive"), "{received}");
    assert!(!received.contains("431"), "{received}");
    assert!(received.ends_with("Connection: close\r\n\r\nok"), "{received}");
}
