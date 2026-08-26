//! The status line a caller chooses: which statuses the service frames, and
//! how it frames one it has no reason phrase for.

use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpStream},
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Token,
    http::{HttpConfig, HttpEvent, HttpService},
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

/// A listening HTTP service, and the requests it has yet to answer.
struct Server {
    net: StreamNetwork,
    service: HttpService,
    requests: Vec<Token>,
}

impl Server {
    /// A server on a fresh address, and a client connected to it.
    fn start() -> (Self, TcpStream) {
        let addr = unused_addr();
        let mut net = StreamNetwork::default();
        let group = net.add_group(ConnectionGroupConfig {
            name: "status",
            framing: Framing::Raw,
            ..ConnectionGroupConfig::default()
        });
        let mut service = HttpService::new(&mut net, group, HttpConfig::default());
        service.listen(&mut net, Endpoint::Tcp(addr)).unwrap();
        let client = TcpStream::connect(addr).unwrap();
        client.set_nonblocking(true).unwrap();
        (Self { net, service, requests: Vec::new() }, client)
    }

    fn pump(&mut self) {
        let Self { net, service, requests } = self;
        net.drive(Some(Duration::ZERO.into()), &mut [service.as_service()], |_| {});
        while let Some(event) = service.next_event(net) {
            if let HttpEvent::Request { token, .. } = event {
                requests.push(token);
            }
        }
    }

    /// Sends `request` and waits for the service to deliver it.
    fn ask(&mut self, client: &mut TcpStream, request: &[u8]) -> Token {
        client.write_all(request).unwrap();
        let deadline = Instant::now() + TIMEOUT;
        while self.requests.is_empty() {
            assert!(Instant::now() < deadline, "the request was not delivered");
            self.pump();
            thread::sleep(Duration::from_millis(1));
        }
        self.requests.remove(0)
    }

    fn respond(&mut self, token: Token, status: u16, body: &[u8]) -> bool {
        self.service.respond(&mut self.net, token, status, &[], body)
    }

    /// Reads until the client has as many bytes as `expected`, and asserts
    /// they are exactly those bytes.
    fn expect(&mut self, client: &mut TcpStream, expected: &str) {
        let mut received = Vec::new();
        let deadline = Instant::now() + TIMEOUT;
        while received.len() < expected.len() {
            assert!(Instant::now() < deadline, "the answer did not arrive: {received:?}");
            let mut buffer = [0; 4096];
            match client.read(&mut buffer) {
                Ok(0) => panic!("the connection closed with {received:?}"),
                Ok(read) => received.extend_from_slice(&buffer[..read]),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                Err(err) => panic!("client read failed: {err}"),
            }
            self.pump();
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(String::from_utf8(received).unwrap(), expected);
    }
}

#[test]
fn an_informational_status_completes_the_request() {
    let (mut server, mut client) = Server::start();
    let token = server.ask(&mut client, b"GET /one HTTP/1.1\r\nHost: x\r\n\r\n");
    // No Content-Length, no body, and the empty reason phrase of a status
    // the service does not name.
    assert!(server.respond(token, 199, b"ignored"));
    server.expect(&mut client, "HTTP/1.1 199 \r\nConnection: keep-alive\r\n\r\n");

    // The connection is answered and idle, so it serves the next request.
    let token = server.ask(&mut client, b"GET /two HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(server.respond(token, 200, b"ok"));
    server.expect(
        &mut client,
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
    );
}

#[test]
fn a_hundred_continue_chosen_by_the_caller_is_final() {
    let (mut server, mut client) = Server::start();
    let token = server.ask(&mut client, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(server.respond(token, 100, b""));
    server.expect(&mut client, "HTTP/1.1 100 Continue\r\nConnection: keep-alive\r\n\r\n");

    let token = server.ask(&mut client, b"GET /next HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(server.respond(token, 200, b"ok"));
    server.expect(
        &mut client,
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
    );
}

#[test]
fn an_unmapped_status_is_framed_with_an_empty_reason() {
    let (mut server, mut client) = Server::start();
    let token = server.ask(&mut client, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(server.respond(token, 250, b"ok"));
    server.expect(
        &mut client,
        "HTTP/1.1 250 \r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
    );
}

#[test]
fn a_mapped_status_keeps_its_reason() {
    let (mut server, mut client) = Server::start();
    let token = server.ask(&mut client, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(server.respond(token, 404, b""));
    server.expect(
        &mut client,
        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
    );
}

#[test]
fn a_status_outside_the_range_is_refused() {
    let (mut server, mut client) = Server::start();
    let token = server.ask(&mut client, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(!server.respond(token, 99, b"ok"), "99 is not a status");
    assert!(!server.respond(token, 600, b"ok"), "600 is not a status");

    // A refused response leaves the request pending, so the caller can still
    // answer it.
    assert!(server.respond(token, 599, b"ok"));
    server.expect(
        &mut client,
        "HTTP/1.1 599 \r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
    );
}
