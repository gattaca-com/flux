//! `HttpNetwork::with_request_heads`: heads before bodies, bodies on request,
//! early responses, half-close, and backpressure.

use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, Shutdown, SocketAddr, TcpStream},
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Network, Token,
    http::{BodyError, HttpEvent, HttpNetwork, RequestBody},
};

const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    Head(Token, String, RequestBody),
    Request(Token, String, Vec<u8>),
    Body(Token, Vec<u8>),
    Failed(Token, BodyError),
}

struct Server {
    net: Network,
    http: HttpNetwork,
    addr: SocketAddr,
}

impl Server {
    fn new(configure: impl FnOnce(HttpNetwork) -> HttpNetwork) -> Self {
        let mut net = Network::default();
        let mut http = configure(HttpNetwork::default().with_request_heads());
        http.listen(&mut net, (Ipv4Addr::LOCALHOST, 0).into()).unwrap();
        let addr = http.local_addr(&net).unwrap();
        Self { net, http, addr }
    }

    fn connect(&self) -> TcpStream {
        let client = TcpStream::connect(self.addr).unwrap();
        client.set_nonblocking(true).unwrap();
        client
    }

    fn step(&mut self) -> Vec<Seen> {
        let mut seen = Vec::new();
        let Self { net, http, .. } = self;
        net.poll_with(|event| {
            http.on_event(&event);
        });
        loop {
            http.drive(net, |event| match event {
                HttpEvent::RequestHead { token, request, body } => {
                    assert!(request.body.is_empty());
                    seen.push(Seen::Head(token, request.path.to_owned(), body));
                }
                HttpEvent::Request { token, request } => {
                    seen.push(Seen::Request(token, request.path.to_owned(), request.body.to_vec()));
                }
                HttpEvent::RequestBody { token, body } => {
                    seen.push(Seen::Body(token, body.to_vec()));
                }
                HttpEvent::RequestBodyFailed { token, error } => {
                    seen.push(Seen::Failed(token, error));
                }
                _ => {}
            });
            if !http.needs_drive() {
                break
            }
        }
        seen
    }

    /// Steps until an event arrives, reading whatever `client` receives.
    fn next(&mut self, client: &mut TcpStream, received: &mut Vec<u8>) -> Seen {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            assert!(Instant::now() < deadline, "no event; received {:?}", text(received));
            let mut seen = self.step();
            read_available(client, received);
            if !seen.is_empty() {
                assert_eq!(seen.len(), 1, "{seen:?}");
                return seen.remove(0)
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Steps for `duration`, asserting that no event arrives.
    fn quiet(&mut self, client: &mut TcpStream, received: &mut Vec<u8>, duration: Duration) {
        let until = Instant::now() + duration;
        while Instant::now() < until {
            assert_eq!(self.step(), []);
            read_available(client, received);
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Steps until the server closes `client`; returns everything received.
    fn until_closed(&mut self, client: &mut TcpStream, received: &mut Vec<u8>) {
        let deadline = Instant::now() + TIMEOUT;
        while !read_available(client, received) {
            assert!(Instant::now() < deadline, "not closed; received {:?}", text(received));
            assert_eq!(self.step(), []);
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Steps until `received` holds `count` complete responses.
    fn until_responses(&mut self, client: &mut TcpStream, received: &mut Vec<u8>, count: usize) {
        let deadline = Instant::now() + TIMEOUT;
        while responses(received).len() < count {
            assert!(Instant::now() < deadline, "received {:?}", text(received));
            assert_eq!(self.step(), []);
            read_available(client, received);
            thread::sleep(Duration::from_millis(1));
        }
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Returns true once the peer has closed.
fn read_available(stream: &mut TcpStream, out: &mut Vec<u8>) -> bool {
    let mut buf = vec![0; 65536];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return true,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return false,
            Err(e) if e.kind() == io::ErrorKind::ConnectionReset => return true,
            Err(e) => panic!("read failed: {e}"),
        }
    }
}

/// The status and body of each complete response in `bytes`.
fn responses(mut bytes: &[u8]) -> Vec<(u16, Vec<u8>)> {
    let mut out = Vec::new();
    while let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
        let head = std::str::from_utf8(&bytes[..end]).unwrap();
        let status: u16 = head[9..12].parse().unwrap();
        let length = head
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))
            .map_or(0, |n| n.parse().unwrap());
        if bytes.len() < end + 4 + length {
            break
        }
        out.push((status, bytes[end + 4..end + 4 + length].to_vec()));
        bytes = &bytes[end + 4 + length..];
    }
    out
}

fn post(path: &str, headers: &str) -> Vec<u8> {
    format!("POST {path} HTTP/1.1\r\nHost: x\r\n{headers}\r\n").into_bytes()
}

#[test]
fn heads_arrive_before_bodies_and_bodies_on_request() {
    let mut server = Server::new(|http| http);
    let mut client = server.connect();
    let mut received = Vec::new();
    client.write_all(&post("/length", "Content-Length: 10\r\n")).unwrap();
    client.write_all(b"01234").unwrap();
    let Seen::Head(token, path, body) = server.next(&mut client, &mut received) else { panic!() };
    assert_eq!((path.as_str(), body), ("/length", RequestBody::Length(10)));
    // Nothing more until the body is asked for, however much has arrived.
    client.write_all(b"56789").unwrap();
    server.quiet(&mut client, &mut received, Duration::from_millis(50));
    assert_eq!(server.http.body_received(token), None);
    assert!(server.http.read_body(&mut server.net, token));
    assert!(!server.http.read_body(&mut server.net, token), "asked once");
    assert_eq!(server.next(&mut client, &mut received), Seen::Body(token, b"0123456789".to_vec()));
    assert!(server.http.respond(&mut server.net, token, 200, &[], b"one"));

    // Chunked, then a bodiless request, on the same connection.
    client.write_all(&post("/chunked", "Transfer-Encoding: chunked\r\n")).unwrap();
    let Seen::Head(token, _, body) = server.next(&mut client, &mut received) else { panic!() };
    assert_eq!(body, RequestBody::Chunked);
    assert!(server.http.read_body(&mut server.net, token));
    client.write_all(b"3\r\nabc\r\n").unwrap();
    let deadline = Instant::now() + TIMEOUT;
    while server.http.body_received(token) != Some(8) {
        assert!(Instant::now() < deadline);
        assert_eq!(server.step(), []);
    }
    client.write_all(b"2\r\nde\r\n0\r\n\r\nGET /plain HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    assert_eq!(server.next(&mut client, &mut received), Seen::Body(token, b"abcde".to_vec()));
    assert!(server.http.respond(&mut server.net, token, 200, &[], b"two"));
    let Seen::Request(token, path, body) = server.next(&mut client, &mut received) else {
        panic!()
    };
    assert_eq!((path.as_str(), body.as_slice()), ("/plain", b"".as_slice()));
    assert!(server.http.respond(&mut server.net, token, 200, &[], b"three"));
    server.until_responses(&mut client, &mut received, 3);
    let bodies: Vec<_> = responses(&received).into_iter().map(|(_, body)| body).collect();
    assert_eq!(bodies, [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]);
}

#[test]
fn continue_is_sent_only_when_the_body_is_asked_for() {
    let mut server = Server::new(|http| http);
    let mut client = server.connect();
    let mut received = Vec::new();
    client.write_all(&post("/", "Content-Length: 3\r\nExpect: 100-continue\r\n")).unwrap();
    let Seen::Head(token, ..) = server.next(&mut client, &mut received) else { panic!() };
    server.quiet(&mut client, &mut received, Duration::from_millis(50));
    assert_eq!(received, b"");
    assert!(server.http.read_body(&mut server.net, token));
    server.quiet(&mut client, &mut received, Duration::from_millis(50));
    assert_eq!(received, b"HTTP/1.1 100 Continue\r\n\r\n");
    client.write_all(b"abc").unwrap();
    assert_eq!(server.next(&mut client, &mut received), Seen::Body(token, b"abc".to_vec()));
}

#[test]
fn early_response_drops_the_body_and_keeps_the_connection() {
    let mut server = Server::new(|http| http);
    for framing in ["Content-Length: 11\r\n", "Transfer-Encoding: chunked\r\n"] {
        let mut client = server.connect();
        let mut received = Vec::new();
        client.write_all(&post("/", framing)).unwrap();
        let Seen::Head(token, ..) = server.next(&mut client, &mut received) else { panic!() };
        assert!(server.http.respond(&mut server.net, token, 401, &[], b"no"));
        server.until_responses(&mut client, &mut received, 1);
        let body: &[u8] = if framing.starts_with("Content") {
            b"hello world"
        } else {
            b"5\r\nhello\r\n6;x\r\n world\r\n0\r\nT: 1\r\n\r\n"
        };
        // The body arrives after the response and is dropped; the request
        // after it is served on the same connection.
        client.write_all(body).unwrap();
        client.write_all(b"GET /next HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        let Seen::Request(token, path, _) = server.next(&mut client, &mut received) else {
            panic!()
        };
        assert_eq!(path, "/next");
        assert!(server.http.respond(&mut server.net, token, 200, &[], b"ok"));
        server.until_responses(&mut client, &mut received, 2);
        let statuses: Vec<_> = responses(&received).into_iter().map(|(s, _)| s).collect();
        assert_eq!(statuses, [401, 200]);
        assert!(text(&received).contains("Connection: keep-alive"));
    }
}

#[test]
fn early_response_closes_when_the_body_cannot_safely_be_dropped() {
    let mut server =
        Server::new(|http| http.with_max_body_bytes(64).with_max_content_length(1 << 20));
    for (headers, after) in [
        // Larger than the drain bound.
        ("Content-Length: 65\r\n", b"x".repeat(65)),
        // The client waits for a 100 Continue that never comes.
        ("Content-Length: 5\r\nExpect: 100-continue\r\n", b"hello".to_vec()),
        // The client asked to close.
        ("Content-Length: 5\r\nConnection: close\r\n", b"hello".to_vec()),
        // Chunk framing that is not chunk framing: a smuggled request.
        ("Transfer-Encoding: chunked\r\n", b"GET /smuggled HTTP/1.1\r\nHost: x\r\n\r\n".to_vec()),
        // A chunked body past the drain bound.
        (
            "Transfer-Encoding: chunked\r\n",
            format!("41\r\n{}\r\n0\r\n\r\n", "y".repeat(65)).into_bytes(),
        ),
    ] {
        let mut client = server.connect();
        let mut received = Vec::new();
        client.write_all(&post("/", headers)).unwrap();
        let Seen::Head(token, ..) = server.next(&mut client, &mut received) else { panic!() };
        assert!(server.http.respond(&mut server.net, token, 401, &[], b"no"));
        let _ = client.write_all(&after);
        let _ = client.write_all(b"GET /next HTTP/1.1\r\nHost: x\r\n\r\n");
        server.until_closed(&mut client, &mut received);
        assert_eq!(responses(&received), [(401, b"no".to_vec())], "{headers}");
        // A chunked body's fate is known only once it arrives, after the
        // response promised to keep the connection.
        if !headers.contains("chunked") {
            assert!(text(&received).contains("Connection: close"), "{headers}");
        }
    }
}

#[test]
fn unfinished_discard_closes_at_its_deadline() {
    let mut server = Server::new(|http| http.with_drain_timeout(Duration::from_millis(100).into()));
    let mut client = server.connect();
    let mut received = Vec::new();
    client.write_all(&post("/", "Content-Length: 100\r\n")).unwrap();
    let Seen::Head(token, ..) = server.next(&mut client, &mut received) else { panic!() };
    assert!(server.http.respond(&mut server.net, token, 401, &[], b""));
    client.write_all(b"partial").unwrap();
    server.until_closed(&mut client, &mut received);
    assert_eq!(responses(&received).len(), 1);
}

#[test]
fn body_failures_are_reported_and_close_after_the_response() {
    let mut server = Server::new(|http| http.with_max_body_bytes(8).with_max_content_length(100));
    for (headers, body, error) in [
        ("Content-Length: 9\r\n", b"".as_slice(), BodyError::TooLarge),
        ("Transfer-Encoding: chunked\r\n", b"9\r\n".as_slice(), BodyError::TooLarge),
        ("Transfer-Encoding: chunked\r\n", b"zz\r\n".as_slice(), BodyError::Malformed),
        ("Content-Length: 5\r\n", b"abc".as_slice(), BodyError::Incomplete),
        ("Transfer-Encoding: chunked\r\n", b"3\r\nab".as_slice(), BodyError::Incomplete),
    ] {
        let mut client = server.connect();
        let mut received = Vec::new();
        client.write_all(&post("/", headers)).unwrap();
        client.write_all(body).unwrap();
        if error == BodyError::Incomplete {
            client.shutdown(Shutdown::Write).unwrap();
        }
        let Seen::Head(token, ..) = server.next(&mut client, &mut received) else { panic!() };
        assert!(server.http.read_body(&mut server.net, token));
        assert_eq!(server.next(&mut client, &mut received), Seen::Failed(token, error));
        assert!(server.http.respond(&mut server.net, token, 400, &[], b"bad"));
        server.until_closed(&mut client, &mut received);
        assert_eq!(responses(&received), [(400, b"bad".to_vec())], "{headers}");
    }
}

#[test]
fn a_half_closed_client_still_gets_its_response() {
    // The 413 case: answered without the body, after the client has shut
    // down its sending side.
    let mut server =
        Server::new(|http| http.with_max_body_bytes(8).with_max_content_length(1 << 30));
    let mut client = server.connect();
    let mut received = Vec::new();
    client.write_all(&post("/", "Content-Length: 50000000\r\n")).unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let Seen::Head(token, _, RequestBody::Length(50_000_000)) =
        server.next(&mut client, &mut received)
    else {
        panic!()
    };
    // Let the end of input arrive before answering.
    thread::sleep(Duration::from_millis(20));
    server.step();
    assert!(server.http.respond(&mut server.net, token, 413, &[], b"too large"));
    server.until_closed(&mut client, &mut received);
    assert_eq!(responses(&received), [(413, b"too large".to_vec())]);

    // A bodiless request followed by the end of input is served, then closed.
    let mut client = server.connect();
    let mut received = Vec::new();
    client.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let Seen::Request(token, ..) = server.next(&mut client, &mut received) else { panic!() };
    thread::sleep(Duration::from_millis(20));
    server.step();
    assert!(server.http.respond(&mut server.net, token, 200, &[], b"ok"));
    server.until_closed(&mut client, &mut received);
    assert_eq!(responses(&received), [(200, b"ok".to_vec())]);
    assert!(text(&received).contains("Connection: close"));
}

#[test]
fn a_held_request_is_never_swept_as_idle() {
    let mut server = Server::new(|http| http.with_idle_timeout(Duration::from_millis(50).into()));
    let mut client = server.connect();
    let mut received = Vec::new();
    client.write_all(&post("/", "Content-Length: 2\r\n")).unwrap();
    let Seen::Head(token, ..) = server.next(&mut client, &mut received) else { panic!() };
    server.quiet(&mut client, &mut received, Duration::from_millis(150));
    assert!(server.http.read_body(&mut server.net, token));
    client.write_all(b"ok").unwrap();
    assert_eq!(server.next(&mut client, &mut received), Seen::Body(token, b"ok".to_vec()));
    server.quiet(&mut client, &mut received, Duration::from_millis(150));
    assert!(server.http.respond(&mut server.net, token, 200, &[], b"late"));
    server.until_responses(&mut client, &mut received, 1);
    // Idle again: now the sweep applies.
    server.until_closed(&mut client, &mut received);
}

#[test]
fn reading_pauses_while_a_request_is_held() {
    // With reading paused, a peer that sends far ahead is held back by the
    // kernel's windows, not buffered by the server, so the server's limit on
    // buffered bytes is never reached and the connection survives.
    let mut server = Server::new(|http| {
        http.with_max_head_bytes(1024).with_max_body_bytes(4096).with_socket_buf_size(16 * 1024)
    });
    let mut client = server.connect();
    let mut received = Vec::new();
    client.write_all(b"GET /first HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let Seen::Request(token, ..) = server.next(&mut client, &mut received) else { panic!() };
    let mut sent = 0;
    let flood = format!("GET /{} HTTP/1.1\r\nHost: x\r\n\r\n", "a".repeat(500));
    let until = Instant::now() + Duration::from_millis(300);
    while Instant::now() < until {
        match client.write(flood.as_bytes()) {
            Ok(n) => sent += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => panic!("{e}"),
        }
        assert_eq!(server.step(), []);
    }
    assert!(sent > 4 * (1024 + 4096), "only {sent} bytes were accepted by the kernel");
    assert!(!read_available(&mut client, &mut received), "the connection was closed");
    assert!(server.http.respond(&mut server.net, token, 200, &[], b"first"));
    // Reading resumes for the next request, and what was sent ahead now
    // exceeds the buffer limit, which closes the connection; a paused
    // connection never reached that limit while its request was held.
    let deadline = Instant::now() + TIMEOUT;
    while !read_available(&mut client, &mut received) {
        assert!(Instant::now() < deadline);
        for seen in server.step() {
            let Seen::Request(token, ..) = seen else { panic!("{seen:?}") };
            server.http.respond(&mut server.net, token, 200, &[], b"next");
        }
    }
    assert_eq!(responses(&received)[0], (200, b"first".to_vec()));
}
