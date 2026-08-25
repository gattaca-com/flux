//! Outbound requests that will not be answered: what fails them, and what
//! the caller learns in which order.

use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr},
    os::unix::net::UnixListener,
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Token,
    http::{HttpConfig, HttpEvent, HttpService, RequestFailure},
    stream::{ConnectionGroupConfig, Endpoint, Framing, StreamNetwork},
};

const TIMEOUT: Duration = Duration::from_secs(10);

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

/// What the service delivered, in the order it was pulled.
#[derive(Debug, PartialEq, Eq)]
enum Pulled {
    Connected,
    Response(u16),
    Failed(RequestFailure),
    Disconnected,
}

trait PeerStream: Read + Write {}
impl<T: Read + Write> PeerStream for T {}

/// The endpoint the service calls, under the test's control: it answers only
/// what the test writes, and reads only when the test says so.
struct Peer {
    listener: Listener,
    stream: Option<Box<dyn PeerStream>>,
}

enum Listener {
    Tcp(std::net::TcpListener),
    Unix(UnixListener),
}

impl Peer {
    fn bind(endpoint: &Endpoint) -> Self {
        let listener = match endpoint {
            Endpoint::Tcp(addr) => {
                let listener = std::net::TcpListener::bind(addr).unwrap();
                listener.set_nonblocking(true).unwrap();
                Listener::Tcp(listener)
            }
            Endpoint::Unix(path) => {
                let listener = UnixListener::bind(path).unwrap();
                listener.set_nonblocking(true).unwrap();
                Listener::Unix(listener)
            }
        };
        Self { listener, stream: None }
    }

    /// Takes the connection waiting to be accepted, if there is one.
    fn accept(&mut self) {
        let accepted: Option<Box<dyn PeerStream>> = match &self.listener {
            Listener::Tcp(listener) => match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true).unwrap();
                    Some(Box::new(stream))
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => None,
                Err(err) => panic!("accept failed: {err}"),
            },
            Listener::Unix(listener) => match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true).unwrap();
                    Some(Box::new(stream) as Box<dyn PeerStream>)
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => None,
                Err(err) => panic!("accept failed: {err}"),
            },
        };
        if accepted.is_some() {
            self.stream = accepted;
        }
    }

    /// Reads and discards whatever arrived, reporting how much.
    fn drain(&mut self) -> usize {
        let Some(stream) = &mut self.stream else { return 0 };
        let mut buffer = [0; 16 * 1024];
        let mut read = 0;
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => return read,
                Ok(bytes) => read += bytes,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => return read,
                Err(_) => return read,
            }
        }
    }

    fn answer(&mut self, bytes: &[u8]) {
        let stream = self.stream.as_mut().expect("the service has not connected");
        stream.write_all(bytes).unwrap();
    }

    fn close(&mut self) {
        self.stream = None;
    }
}

/// An HTTP service calling one outbound endpoint.
struct Client {
    net: StreamNetwork,
    service: HttpService,
    token: Token,
    pulled: Vec<Pulled>,
}

impl Client {
    fn build(endpoint: &Endpoint, group: ConnectionGroupConfig, config: HttpConfig) -> Self {
        let mut net = StreamNetwork::default();
        let group = net.add_group(group);
        let mut service = HttpService::new(&mut net, group, config);
        let token = service.connect(&mut net, endpoint.clone());
        Self { net, service, token, pulled: Vec::new() }
    }

    fn new(endpoint: &Endpoint, config: HttpConfig) -> Self {
        Self::build(endpoint, raw_group(), config)
    }

    fn pump(&mut self) {
        let Self { net, service, pulled, .. } = self;
        net.drive(Some(Duration::ZERO.into()), &mut [service.as_service()], |_| {});
        while let Some(event) = service.next_event(net) {
            match event {
                HttpEvent::Connected { .. } => pulled.push(Pulled::Connected),
                HttpEvent::Response { response, .. } => {
                    pulled.push(Pulled::Response(response.status));
                }
                HttpEvent::RequestFailed { reason, .. } => pulled.push(Pulled::Failed(reason)),
                HttpEvent::Disconnected { .. } => pulled.push(Pulled::Disconnected),
                HttpEvent::Accepted { .. } | HttpEvent::Request { .. } => {}
            }
        }
    }

    fn request(&mut self, body: &[u8]) -> bool {
        self.service.request(&mut self.net, self.token, "GET", "/", &[], body)
    }
}

fn raw_group() -> ConnectionGroupConfig {
    ConnectionGroupConfig {
        name: "outbound",
        framing: Framing::Raw,
        max_frame_size: usize::MAX,
        backlog_warn_bytes: None,
        ..ConnectionGroupConfig::default()
    }
}

/// One iteration of both ends, letting the peer take a pending connection.
fn step(client: &mut Client, peer: &mut Peer) {
    peer.accept();
    client.pump();
    thread::sleep(Duration::from_millis(1));
}

/// Runs both ends until `done` holds, failing with `what` at the deadline.
fn run_until(client: &mut Client, peer: &mut Peer, done: impl Fn(&Client) -> bool, what: &str) {
    let deadline = Instant::now() + TIMEOUT;
    while !done(client) {
        assert!(Instant::now() < deadline, "{what}");
        step(client, peer);
    }
}

/// Connects the service to the peer and waits for the request to arrive.
fn connect_and_request(client: &mut Client, peer: &mut Peer) {
    run_until(client, peer, |client| !client.pulled.is_empty(), "the endpoint did not connect");
    assert_eq!(client.pulled, [Pulled::Connected]);
    assert!(client.request(b""));
    let deadline = Instant::now() + TIMEOUT;
    let mut asked = 0;
    while asked == 0 {
        assert!(Instant::now() < deadline, "the request did not reach the peer");
        step(client, peer);
        asked = peer.drain();
    }
}

over_both_transports!(
    an_unanswered_request_times_out,
    an_unanswered_request_times_out_tcp,
    an_unanswered_request_times_out_unix
);
fn an_unanswered_request_times_out(endpoint: &Endpoint) {
    let group = ConnectionGroupConfig {
        reconnect_interval: Duration::from_millis(50).into(),
        ..raw_group()
    };
    let config = HttpConfig::default().with_request_timeout(Duration::from_millis(100).into());
    let mut peer = Peer::bind(endpoint);
    let mut client = Client::build(endpoint, group, config);
    connect_and_request(&mut client, &mut peer);

    // The failure comes first, the close it causes second, and the endpoint
    // is back at its group's interval.
    run_until(
        &mut client,
        &mut peer,
        |client| client.pulled.len() == 4,
        "the request did not fail",
    );
    assert_eq!(client.pulled, [
        Pulled::Connected,
        Pulled::Failed(RequestFailure::Timeout),
        Pulled::Disconnected,
        Pulled::Connected,
    ]);
}

over_both_transports!(
    an_answer_within_the_deadline_clears_it,
    an_answer_within_the_deadline_clears_it_tcp,
    an_answer_within_the_deadline_clears_it_unix
);
fn an_answer_within_the_deadline_clears_it(endpoint: &Endpoint) {
    let timeout = Duration::from_millis(100);
    let config = HttpConfig::default().with_request_timeout(timeout.into());
    let mut peer = Peer::bind(endpoint);
    let mut client = Client::new(endpoint, config);
    connect_and_request(&mut client, &mut peer);
    peer.answer(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");

    run_until(&mut client, &mut peer, |client| client.pulled.len() == 2, "the answer did not come");
    assert_eq!(client.pulled, [Pulled::Connected, Pulled::Response(200)]);

    // An answered request leaves no deadline behind for the idle connection
    // to trip over.
    let until = Instant::now() + timeout * 3;
    while Instant::now() < until {
        step(&mut client, &mut peer);
    }
    assert_eq!(client.pulled, [Pulled::Connected, Pulled::Response(200)]);
}

#[test]
fn the_deadline_runs_from_the_queued_request() {
    // A peer that never reads leaves the request queued behind a full socket,
    // so a clock started when the last byte left would never run at all.
    let endpoint = Endpoint::Tcp(unused_addr());
    let group = ConnectionGroupConfig { socket_buf_size: Some(4096), ..raw_group() };
    let timeout = Duration::from_millis(150);
    let config = HttpConfig::default().with_request_timeout(timeout.into());
    let mut peer = Peer::bind(&endpoint);
    let mut client = Client::build(&endpoint, group, config);
    run_until(
        &mut client,
        &mut peer,
        |client| !client.pulled.is_empty(),
        "the endpoint did not connect",
    );

    let queued = Instant::now();
    assert!(client.request(&vec![7; 4 * 1024 * 1024]));
    run_until(
        &mut client,
        &mut peer,
        |client| client.pulled.contains(&Pulled::Failed(RequestFailure::Timeout)),
        "the queued request did not time out",
    );
    assert!(queued.elapsed() >= timeout, "the request failed after {:?}", queued.elapsed());
}

/// Runs one failing exchange: the peer answers `answer`, closing afterwards
/// when `close` says so, and the caller must learn `reason` before the close.
fn assert_failure(
    endpoint: &Endpoint,
    config: HttpConfig,
    answer: &[u8],
    close: bool,
    reason: RequestFailure,
) {
    let mut peer = Peer::bind(endpoint);
    let mut client = Client::new(endpoint, config);
    connect_and_request(&mut client, &mut peer);
    peer.answer(answer);
    if close {
        peer.close();
    }

    run_until(
        &mut client,
        &mut peer,
        |client| client.pulled.len() == 3,
        "the request did not fail",
    );
    assert_eq!(client.pulled, [Pulled::Connected, Pulled::Failed(reason), Pulled::Disconnected]);
}

over_both_transports!(
    a_malformed_answer_fails_the_request,
    a_malformed_answer_fails_the_request_tcp,
    a_malformed_answer_fails_the_request_unix
);
fn a_malformed_answer_fails_the_request(endpoint: &Endpoint) {
    assert_failure(
        endpoint,
        HttpConfig::default(),
        b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx",
        false,
        RequestFailure::Malformed,
    );
}

over_both_transports!(
    an_answer_over_the_cap_fails_the_request,
    an_answer_over_the_cap_fails_the_request_tcp,
    an_answer_over_the_cap_fails_the_request_unix
);
fn an_answer_over_the_cap_fails_the_request(endpoint: &Endpoint) {
    let mut answer = b"HTTP/1.1 200 OK\r\nContent-Length: 64\r\n\r\n".to_vec();
    answer.extend_from_slice(&[b'x'; 64]);
    assert_failure(
        endpoint,
        HttpConfig::default().with_max_body_bytes(8),
        &answer,
        false,
        RequestFailure::TooLarge,
    );
}

over_both_transports!(
    an_endpoint_that_drops_mid_answer_fails_the_request,
    an_endpoint_that_drops_mid_answer_fails_the_request_tcp,
    an_endpoint_that_drops_mid_answer_fails_the_request_unix
);
fn an_endpoint_that_drops_mid_answer_fails_the_request(endpoint: &Endpoint) {
    assert_failure(
        endpoint,
        HttpConfig::default(),
        b"HTTP/1.1 200 OK\r\nContent-Length: 64\r\n\r\npartial",
        true,
        RequestFailure::Disconnected,
    );
}

over_both_transports!(
    an_endpoint_that_drops_with_nothing_in_flight_fails_nothing,
    an_endpoint_that_drops_with_nothing_in_flight_fails_nothing_tcp,
    an_endpoint_that_drops_with_nothing_in_flight_fails_nothing_unix
);
fn an_endpoint_that_drops_with_nothing_in_flight_fails_nothing(endpoint: &Endpoint) {
    let config = HttpConfig::default().with_request_timeout(Duration::from_millis(100).into());
    let mut peer = Peer::bind(endpoint);
    let mut client = Client::new(endpoint, config);
    run_until(
        &mut client,
        &mut peer,
        |client| !client.pulled.is_empty(),
        "the endpoint did not connect",
    );
    assert_eq!(client.pulled, [Pulled::Connected]);
    peer.close();

    run_until(
        &mut client,
        &mut peer,
        |client| client.pulled.len() >= 2,
        "the close was not reported",
    );
    assert_eq!(client.pulled[..2], [Pulled::Connected, Pulled::Disconnected]);
    assert!(
        !client.pulled.iter().any(|pulled| matches!(pulled, Pulled::Failed(_))),
        "a close with nothing in flight failed a request: {:?}",
        client.pulled
    );
}

#[test]
fn an_answer_whose_head_is_over_the_cap_is_too_large() {
    let mut answer = b"HTTP/1.1 200 OK\r\nX: ".to_vec();
    answer.extend_from_slice(&[b'v'; 100]);
    answer.extend_from_slice(b"\r\nContent-Length: 0\r\n\r\n");
    assert_failure(
        &Endpoint::Tcp(unused_addr()),
        HttpConfig::default().with_max_head_bytes(64),
        &answer,
        false,
        RequestFailure::TooLarge,
    );
}

#[test]
fn a_chunked_answer_over_the_cap_is_too_large() {
    assert_failure(
        &Endpoint::Tcp(unused_addr()),
        HttpConfig::default().with_max_body_bytes(8),
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n20\r\n\
          xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n0\r\n\r\n",
        false,
        RequestFailure::TooLarge,
    );
}

#[test]
fn an_answer_the_close_delimits_over_the_cap_is_too_large() {
    let mut answer = b"HTTP/1.1 200 OK\r\n\r\n".to_vec();
    answer.extend_from_slice(&[b'x'; 64]);
    assert_failure(
        &Endpoint::Tcp(unused_addr()),
        HttpConfig::default().with_max_body_bytes(8),
        &answer,
        true,
        RequestFailure::TooLarge,
    );
}

#[test]
fn a_chunk_the_service_cannot_size_is_malformed() {
    assert_failure(
        &Endpoint::Tcp(unused_addr()),
        HttpConfig::default(),
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nxx\r\n0\r\n\r\n",
        false,
        RequestFailure::Malformed,
    );
}

#[test]
fn an_answer_with_more_headers_than_the_parse_holds_is_malformed() {
    let mut answer = b"HTTP/1.1 200 OK\r\n".to_vec();
    for index in 0..8 {
        answer.extend_from_slice(format!("h{index}: v\r\n").as_bytes());
    }
    answer.extend_from_slice(b"Content-Length: 0\r\n\r\n");
    assert_failure(
        &Endpoint::Tcp(unused_addr()),
        HttpConfig::default().with_max_headers(8),
        &answer,
        false,
        RequestFailure::Malformed,
    );
}

#[test]
fn an_answer_framed_twice_over_is_malformed() {
    assert_failure(
        &Endpoint::Tcp(unused_addr()),
        HttpConfig::default(),
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        false,
        RequestFailure::Malformed,
    );
}

#[test]
fn a_blocking_drive_wakes_for_a_request_deadline() {
    let addr = unused_addr();
    let listener = std::net::TcpListener::bind(addr).unwrap();
    // The peer accepts, answers nothing, and sends one byte three seconds
    // later. That byte is the test's own deadline: a drive that ignored the
    // request deadline wakes on it, well past the bound below, rather than
    // hanging.
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        thread::sleep(Duration::from_secs(3));
        let _ = stream.write_all(b"x");
    });
    let config = HttpConfig::default().with_request_timeout(Duration::from_millis(500).into());
    let mut client = Client::new(&Endpoint::Tcp(addr), config);
    let deadline = Instant::now() + TIMEOUT;
    while client.pulled.is_empty() {
        assert!(Instant::now() < deadline, "the endpoint did not connect");
        client.pump();
        thread::sleep(Duration::from_millis(1));
    }
    assert!(client.request(b""));

    // That deadline is the only one the network has, so an uncapped drive
    // waits for it and no longer.
    let started = Instant::now();
    client.net.drive(None, &mut [client.service.as_service()], |_| {});
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(100), "returned at once: {waited:?}");
    assert!(waited < Duration::from_secs(2), "the request deadline was not folded: {waited:?}");

    while !client.pulled.contains(&Pulled::Failed(RequestFailure::Timeout)) {
        assert!(Instant::now() < deadline, "the request did not fail: {:?}", client.pulled);
        client.pump();
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn an_answer_whose_open_head_is_over_the_cap_is_too_large() {
    // The head never ends, so the cap is reached with the parse still
    // incomplete: what is buffered is head bytes and there are too many.
    let mut answer = b"HTTP/1.1 200 OK\r\nX: ".to_vec();
    answer.extend_from_slice(&[b'v'; 100]);
    answer.extend_from_slice(b"\r\n");
    assert_failure(
        &Endpoint::Tcp(unused_addr()),
        HttpConfig::default().with_max_head_bytes(64),
        &answer,
        false,
        RequestFailure::TooLarge,
    );
}
