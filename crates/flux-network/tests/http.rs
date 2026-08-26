use std::{
    collections::BTreeSet,
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr},
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Token,
    http::{HttpConfig, HttpEvent, HttpService},
    stream::{
        ConnectionGroupConfig, Endpoint, Framing, Peer, ServiceRef, StreamEvent, StreamNetwork,
    },
};

const TIMEOUT: Duration = Duration::from_secs(10);

/// A loopback address no listener holds.
///
/// The probe binding is released before the address is handed out, so a port
/// this run has already given away must never be given away again: the kernel
/// reuses a released ephemeral port readily, and two tests binding one port is
/// a failure that has nothing to do with what they test. Ports that collide
/// stay bound until a fresh one is found, which is what makes the kernel offer
/// another.
fn unused_addr() -> SocketAddr {
    static TAKEN: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
    let mut probes = Vec::new();
    loop {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        if TAKEN.lock().unwrap().insert(addr.port()) {
            return addr;
        }
        probes.push(listener);
        assert!(probes.len() < 64, "no free loopback port");
    }
}

fn http_group() -> ConnectionGroupConfig {
    ConnectionGroupConfig {
        name: "http",
        framing: Framing::Raw,
        max_frame_size: usize::MAX,
        backlog_warn_bytes: None,
        ..ConnectionGroupConfig::default()
    }
}

/// Who owns the poll a network is driven from.
#[derive(Clone, Copy)]
enum Mode {
    /// The network's own, driven by `drive`.
    Owned,
    /// The test's own, driven by the three calls it requires.
    External,
}

/// The poll a network is driven from, and what one iteration of it is.
struct Driver {
    /// The test's own poll, in External mode; `None` when the network polls.
    poll: Option<(mio::Poll, mio::Events)>,
}

impl Driver {
    /// A network of `mode`, and the driver that runs its iterations.
    fn build(mode: Mode) -> (Self, StreamNetwork) {
        match mode {
            Mode::Owned => (Self { poll: None }, StreamNetwork::default()),
            Mode::External => {
                let poll = mio::Poll::new().unwrap();
                let net =
                    StreamNetwork::with_registry(poll.registry().try_clone().unwrap(), Token(1000));
                (Self { poll: Some((poll, mio::Events::with_capacity(64))) }, net)
            }
        }
    }

    /// One nonblocking iteration: one `drive`, or the three calls a
    /// caller-held poll requires.
    fn iterate<F>(
        &mut self,
        net: &mut StreamNetwork,
        services: &mut [ServiceRef<'_>],
        mut unclaimed_handler: F,
    ) -> bool
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        let Some((poll, events)) = &mut self.poll else {
            return net.drive(Some(Duration::ZERO.into()), services, unclaimed_handler);
        };
        // The fold is what an External caller turns into its poll timeout;
        // this one polls without waiting, and calls it for the same
        // validation every iteration owes the network.
        let _ = net.next_deadline(services);
        poll.poll(events, Some(Duration::ZERO)).unwrap();
        let mut worked = false;
        for event in &*events {
            worked |= net.handle_event(event, services, &mut unclaimed_handler);
        }
        worked | net.tick(services, &mut unclaimed_handler)
    }
}

/// A network carrying one HTTP service, driven and pulled as one.
struct Http {
    net: StreamNetwork,
    service: HttpService,
    driver: Driver,
}

impl Http {
    fn build(mode: Mode, group: ConnectionGroupConfig, config: HttpConfig) -> Self {
        let (driver, mut net) = Driver::build(mode);
        let group = net.add_group(group);
        let service = HttpService::new(&mut net, group, config);
        Self { net, service, driver }
    }

    fn new() -> Self {
        Self::new_in(Mode::Owned)
    }

    fn new_in(mode: Mode) -> Self {
        Self::build(mode, http_group(), HttpConfig::default())
    }

    fn with_config(config: HttpConfig) -> Self {
        Self::build(Mode::Owned, http_group(), config)
    }

    /// One iteration: drive the network, then pull every protocol event.
    fn pump(&mut self, mut handler: impl for<'a> FnMut(HttpEvent<'a>)) {
        self.drive();
        let mut pulled = 0;
        while let Some(event) = self.service.next_event(&mut self.net) {
            handler(event);
            pulled += 1;
            assert!(pulled < 10_000, "the pull loop delivered the same work forever");
        }
    }

    /// One iteration of the network alone, leaving the events to be pulled.
    fn drive(&mut self) -> bool {
        let Self { net, service, driver } = self;
        driver.iterate(net, &mut [service.as_service()], |_| {})
    }

    fn listen(&mut self, endpoint: &Endpoint) {
        self.service.listen(&mut self.net, endpoint.clone()).unwrap();
    }

    fn connect(&mut self, endpoint: Endpoint) -> Token {
        self.service.connect(&mut self.net, endpoint)
    }

    fn respond(
        &mut self,
        token: Token,
        status: u16,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> bool {
        self.service.respond(&mut self.net, token, status, headers, body)
    }

    fn request(
        &mut self,
        token: Token,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> bool {
        self.service.request(&mut self.net, token, method, path, headers, body)
    }

    fn disconnect(&mut self, token: Token) -> bool {
        self.service.disconnect(&mut self.net, token)
    }

    fn remove(&mut self, token: Token) -> bool {
        self.service.remove(&mut self.net, token)
    }
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

/// Runs one test body over both transports and both poll-ownership modes: the
/// four tests one behaviour has to pass however the network is driven.
macro_rules! over_transports_and_modes {
    ($body:ident, $tcp:ident, $tcp_external:ident, $unix:ident, $unix_external:ident) => {
        #[test]
        fn $tcp() {
            $body(Mode::Owned, &Endpoint::Tcp(unused_addr()));
        }

        #[test]
        fn $tcp_external() {
            $body(Mode::External, &Endpoint::Tcp(unused_addr()));
        }

        #[test]
        fn $unix() {
            let dir = tempfile::tempdir().unwrap();
            $body(Mode::Owned, &Endpoint::Unix(dir.path().join("s")));
        }

        #[test]
        fn $unix_external() {
            let dir = tempfile::tempdir().unwrap();
            $body(Mode::External, &Endpoint::Unix(dir.path().join("s")));
        }
    };
}

/// Runs one test body under both poll-ownership modes.
macro_rules! over_both_modes {
    ($body:ident, $owned:ident, $external:ident) => {
        #[test]
        fn $owned() {
            $body(Mode::Owned);
        }

        #[test]
        fn $external() {
            $body(Mode::External);
        }
    };
}

/// A nonblocking client socket speaking raw bytes to a server under test.
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

fn read_available(stream: &mut impl Read, out: &mut Vec<u8>) -> bool {
    let mut buf = [0; 8192];
    match stream.read(&mut buf) {
        Ok(0) => true,
        Ok(n) => {
            out.extend_from_slice(&buf[..n]);
            false
        }
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => false,
        // A server that hard-closes after responding can RST bytes still in
        // flight from the client; everything sent before the close has
        // already been received, so treat it as EOF.
        Err(e) if e.kind() == io::ErrorKind::ConnectionReset => true,
        Err(e) => panic!("read failed: {e}"),
    }
}

fn response_len(bytes: &[u8]) -> Option<usize> {
    let head = bytes.windows(4).position(|b| b == b"\r\n\r\n")? + 4;
    let text = std::str::from_utf8(&bytes[..head]).unwrap();
    let length = text
        .lines()
        .find_map(|line| line.strip_prefix("Content-Length: "))
        .unwrap()
        .parse::<usize>()
        .unwrap();
    (bytes.len() >= head + length).then_some(head + length)
}

fn server_at(endpoint: &Endpoint) -> Http {
    server_at_in(Mode::Owned, endpoint)
}

fn server_at_in(mode: Mode, endpoint: &Endpoint) -> Http {
    let mut server = Http::new_in(mode);
    server.listen(endpoint);
    server
}

fn server() -> (Http, SocketAddr) {
    let addr = unused_addr();
    (server_at(&Endpoint::Tcp(addr)), addr)
}

over_transports_and_modes!(
    get_keepalive_two_requests,
    get_keepalive_two_requests_tcp,
    get_keepalive_two_requests_tcp_external,
    get_keepalive_two_requests_unix,
    get_keepalive_two_requests_unix_external
);
fn get_keepalive_two_requests(mode: Mode, endpoint: &Endpoint) {
    let mut server = server_at_in(mode, endpoint);
    let mut client = connect_client(endpoint);
    let deadline = Instant::now() + TIMEOUT;
    let mut first = Vec::new();
    client.write_all(b"GET /one HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    while Instant::now() < deadline && response_len(&first).is_none() {
        let mut replies = Vec::new();
        server.pump(|e| {
            if let HttpEvent::Request { token, request, .. } = e {
                replies.push((
                    token,
                    if request.path == "/one" { b"one".to_vec() } else { b"two".to_vec() },
                ));
            }
        });
        for (token, body) in replies {
            assert!(server.respond(token, 200, &[], &body));
        }
        read_available(&mut client, &mut first);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(response_len(&first).is_some());
    client.write_all(b"GET /two HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let mut second = Vec::new();
    while Instant::now() < deadline && response_len(&second).is_none() {
        let mut replies = Vec::new();
        server.pump(|e| {
            if let HttpEvent::Request { token, request, .. } = e {
                replies.push((
                    token,
                    if request.path == "/one" { b"one".to_vec() } else { b"two".to_vec() },
                ));
            }
        });
        for (token, body) in replies {
            assert!(server.respond(token, 200, &[], &body));
        }
        read_available(&mut client, &mut second);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(first.ends_with(b"one"));
    assert!(second.ends_with(b"two"));
}

over_transports_and_modes!(
    post_echo_body,
    post_echo_body_tcp,
    post_echo_body_tcp_external,
    post_echo_body_unix,
    post_echo_body_unix_external
);
fn post_echo_body(mode: Mode, endpoint: &Endpoint) {
    let mut server = server_at_in(mode, endpoint);
    let body = vec![42; 4096];
    let mut client = connect_client(endpoint);
    client
        .write_all(
            format!("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n", body.len())
                .as_bytes(),
        )
        .unwrap();
    client.write_all(&body).unwrap();
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && response_len(&received).is_none() {
        let mut replies = Vec::new();
        server.pump(|e| {
            if let HttpEvent::Request { token, request, .. } = e {
                replies.push((token, request.body.to_vec()));
            }
        });
        for (token, body) in replies {
            server.respond(token, 200, &[], &body);
        }
        read_available(&mut client, &mut received);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(received.ends_with(&body));
}

#[test]
fn post_binary_body_lone_lf() {
    let (mut server, addr) = server();
    let body: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
    assert!(body.windows(2).any(|w| w[0] != b'\r' && w[1] == b'\n'));
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client
        .write_all(
            format!("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n", body.len())
                .as_bytes(),
        )
        .unwrap();
    client.write_all(&body).unwrap();
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && response_len(&received).is_none() {
        let mut replies = Vec::new();
        server.pump(|e| {
            if let HttpEvent::Request { token, request, .. } = e {
                replies.push((token, request.body.to_vec()));
            }
        });
        for (token, body) in replies {
            assert!(server.respond(token, 200, &[], &body));
        }
        read_available(&mut client, &mut received);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(received.ends_with(&body));

    // The connection must stay usable for a text request after a binary body.
    client.write_all(b"GET /after HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let mut second = Vec::new();
    while Instant::now() < deadline && response_len(&second).is_none() {
        let mut replies = Vec::new();
        server.pump(|e| {
            if let HttpEvent::Request { token, request, .. } = e {
                assert_eq!(request.path, "/after");
                replies.push(token);
            }
        });
        for token in replies {
            assert!(server.respond(token, 200, &[], b"after"));
        }
        read_available(&mut client, &mut second);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(second.ends_with(b"after"));
}

#[test]
fn connection_close_large_body() {
    let addr = unused_addr();
    let mut server = Http::build(
        Mode::Owned,
        ConnectionGroupConfig { socket_buf_size: Some(1024), ..http_group() },
        HttpConfig::default(),
    );
    server.listen(&Endpoint::Tcp(addr));
    let body = vec![7; 256 * 1024];
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").unwrap();
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && !read_available(&mut client, &mut received) {
        let mut tokens = Vec::new();
        server.pump(|e| {
            if let HttpEvent::Request { token, .. } = e {
                tokens.push(token);
            }
        });
        for token in tokens {
            server.respond(token, 200, &[], &body);
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(received.ends_with(&body));
}

#[test]
fn limits_and_errors() {
    for (request, status) in [
        (b"GET / HTTP/1.1\r\nX: 1234567890123456789012345678901234567890123456789012345678901234\r\n\r\n".as_slice(), 431),
        (b"POST / HTTP/1.1\r\nContent-Length: 9\r\n\r\n".as_slice(), 413),
        (b"nope\r\n\r\n".as_slice(), 400),
        (b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n".as_slice(), 501),
    ] {
        let addr = unused_addr();
        let mut server = Http::with_config(
            HttpConfig::default().with_max_head_bytes(64).with_max_body_bytes(8),
        );
        server.listen(&Endpoint::Tcp(addr));
        let mut client = std::net::TcpStream::connect(addr).unwrap();
        client.set_nonblocking(true).unwrap();
        client.write_all(request).unwrap();
        let mut received = Vec::new();
        let deadline = Instant::now() + TIMEOUT;
        while Instant::now() < deadline && !read_available(&mut client, &mut received) {
            server.pump(|_| {});
            thread::sleep(Duration::from_millis(1));
        }
        assert!(std::str::from_utf8(&received).unwrap().starts_with(&format!("HTTP/1.1 {status}")));
    }
}

#[test]
fn caller_connection_close_header_sent() {
    let (mut server, addr) = server();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && !read_available(&mut client, &mut received) {
        let mut tokens = Vec::new();
        server.pump(|e| {
            if let HttpEvent::Request { token, .. } = e {
                tokens.push(token);
            }
        });
        for token in tokens {
            // The caller-supplied Connection header must still result in
            // exactly one canonical Connection: close on the wire.
            assert!(server.respond(token, 200, &[("Connection", "close")], b"ok"));
        }
        thread::sleep(Duration::from_millis(1));
    }
    let text = String::from_utf8(received).unwrap();
    assert_eq!(text.matches("Connection:").count(), 1, "{text}");
    assert!(text.contains("Connection: close\r\n"), "{text}");
    assert!(text.ends_with("ok"));
}

over_transports_and_modes!(
    pipelined_requests,
    pipelined_requests_tcp,
    pipelined_requests_tcp_external,
    pipelined_requests_unix,
    pipelined_requests_unix_external
);
fn pipelined_requests(mode: Mode, endpoint: &Endpoint) {
    let mut server = server_at_in(mode, endpoint);
    let mut client = connect_client(endpoint);
    client
        .write_all(b"GET /one HTTP/1.1\r\nHost: x\r\n\r\nGET /two HTTP/1.1\r\nHost: x\r\n\r\n")
        .unwrap();
    let mut paths = Vec::new();
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && paths.len() < 2 {
        let mut replies = Vec::new();
        server.pump(|e| {
            if let HttpEvent::Request { token, request, .. } = e {
                paths.push(request.path.to_owned());
                replies.push(token);
            }
        });
        for token in replies {
            server.respond(token, 200, &[], paths.last().unwrap().as_bytes());
        }
        read_available(&mut client, &mut received);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(paths, ["/one", "/two"]);
    assert!(received.ends_with(b"/two"));
}

over_transports_and_modes!(
    client_server_roundtrip,
    client_server_roundtrip_tcp,
    client_server_roundtrip_tcp_external,
    client_server_roundtrip_unix,
    client_server_roundtrip_unix_external
);
fn client_server_roundtrip(mode: Mode, endpoint: &Endpoint) {
    let mut server = server_at_in(mode, endpoint);
    let mut client = Http::new_in(mode);
    let token = client.connect(endpoint.clone());
    let mut sent = false;
    let mut bodies = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && bodies.len() < 2 {
        let mut replies = Vec::new();
        server.pump(|e| {
            if let HttpEvent::Request { token, request, .. } = e {
                replies.push((token, request.body.to_vec()));
            }
        });
        for (token, body) in replies {
            server.respond(token, 200, &[("X-Reply", "yes")], &body);
        }
        let mut send_second = false;
        let mut connected = false;
        client.pump(|e| match e {
            HttpEvent::Connected { .. } => connected = true,
            HttpEvent::Response { response, .. } => {
                bodies.push(response.body.to_vec());
                send_second = true;
            }
            _ => {}
        });
        if connected && !sent {
            assert!(client.request(token, "POST", "/", &[("X-Test", "yes")], b"hello"));
            assert!(!client.request(token, "GET", "/", &[], b""));
            sent = true;
        }
        if send_second && bodies.len() == 1 {
            assert!(client.request(token, "GET", "/", &[], b""));
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(bodies, [b"hello".to_vec(), Vec::new()]);
}

#[test]
fn client_chunked_response() {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let mut b = [0; 1024];
        let _ = s.read(&mut b);
        s.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3 \r\nhey\r\n2;ext=x\r\n!!\r\n0\r\nX: y\r\n\r\n").unwrap();
    });
    let mut client = Http::new();
    let token = client.connect(Endpoint::Tcp(addr));
    let mut sent = false;
    let mut body = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && body.is_none() {
        let mut connected = false;
        client.pump(|e| match e {
            HttpEvent::Connected { .. } => connected = true,
            HttpEvent::Response { response, .. } => body = Some(response.body.to_vec()),
            _ => {}
        });
        if connected && !sent {
            assert!(client.request(token, "GET", "/", &[], b""));
            sent = true;
        }
        thread::sleep(Duration::from_millis(1));
    }
    peer.join().unwrap();
    assert_eq!(body.unwrap(), b"hey!!");
}

#[test]
fn client_head_response_ignores_advisory_content_length() {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0; 1024];
        let _ = stream.read(&mut request);
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 999999\r\n\r\n").unwrap();
    });
    let mut client = Http::with_config(HttpConfig::default().with_max_body_bytes(8));
    let token = client.connect(Endpoint::Tcp(addr));
    let deadline = Instant::now() + TIMEOUT;
    let mut connected = false;
    let mut response = None;
    while Instant::now() < deadline && response.is_none() {
        client.pump(|event| match event {
            HttpEvent::Connected { token: event_token } if event_token == token => connected = true,
            HttpEvent::Response { token: event_token, response: event_response }
                if event_token == token =>
            {
                response = Some((event_response.status, event_response.body.len()));
            }
            _ => {}
        });
        if connected {
            assert!(client.request(token, "HEAD", "/", &[], &[]));
            connected = false;
        }
        thread::sleep(Duration::from_millis(1));
    }
    peer.join().unwrap();
    assert_eq!(response, Some((200, 0)));
}

#[test]
fn client_binary_bodies() {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let chunked_body = b"\n\x00\n\xff";
    let plain_body: Vec<u8> = (0..=255u8).collect();
    let plain = plain_body.clone();
    let peer = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let mut b = [0; 1024];
        let _ = s.read(&mut b);
        s.write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\n\n\x00\n\xff\r\n0\r\n\r\n",
        )
        .unwrap();
        let _ = s.read(&mut b);
        s.write_all(
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", plain.len()).as_bytes(),
        )
        .unwrap();
        s.write_all(&plain).unwrap();
    });
    let mut client = Http::new();
    let token = client.connect(Endpoint::Tcp(addr));
    let mut sent = false;
    let mut bodies = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && bodies.len() < 2 {
        let mut connected = false;
        let mut respond_again = false;
        client.pump(|e| match e {
            HttpEvent::Connected { .. } => connected = true,
            HttpEvent::Response { response, .. } => {
                bodies.push(response.body.to_vec());
                respond_again = true;
            }
            _ => {}
        });
        if connected && !sent {
            assert!(client.request(token, "GET", "/", &[], b""));
            sent = true;
        }
        if respond_again && bodies.len() == 1 {
            assert!(client.request(token, "GET", "/", &[], b""));
        }
        thread::sleep(Duration::from_millis(1));
    }
    peer.join().unwrap();
    assert_eq!(bodies, [chunked_body.to_vec(), plain_body]);
}

#[test]
fn client_reconnect_after_close() {
    let (mut server, addr) = server();
    let mut client = Http::new();
    let token = client.connect(Endpoint::Tcp(addr));
    let mut connected = 0;
    let mut disconnected = 0;
    let mut responses = 0;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && responses < 2 {
        let mut replies = Vec::new();
        server.pump(|e| {
            if let HttpEvent::Request { token, .. } = e {
                replies.push(token);
            }
        });
        for token in replies {
            server.respond(token, 200, &[("Connection", "close")], b"ok");
        }
        let mut request_again = false;
        client.pump(|e| match e {
            HttpEvent::Connected { .. } => {
                connected += 1;
                request_again = true;
            }
            HttpEvent::Disconnected { .. } => disconnected += 1,
            HttpEvent::Response { .. } => responses += 1,
            _ => {}
        });
        if request_again {
            assert!(client.request(token, "GET", "/", &[("Connection", "close")], b""));
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(connected >= 2 && disconnected >= 1 && responses == 2);
}

#[test]
fn smuggling_rejected() {
    for (request, status, accepted) in [
        (
            b"POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx".as_slice(),
            400,
            false,
        ),
        (
            b"POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx".as_slice(),
            200,
            true,
        ),
        (
            b"POST / HTTP/1.1\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
                .as_slice(),
            501,
            false,
        ),
    ] {
        let (mut server, addr) = server();
        let mut client = std::net::TcpStream::connect(addr).unwrap();
        client.set_nonblocking(true).unwrap();
        client.write_all(request).unwrap();
        let deadline = Instant::now() + TIMEOUT;
        let mut received = Vec::new();
        let mut requests = 0;
        let mut closed = false;
        while Instant::now() < deadline &&
            if accepted { response_len(&received).is_none() } else { !closed }
        {
            let mut replies = Vec::new();
            server.pump(|event| {
                if let HttpEvent::Request { token, .. } = event {
                    requests += 1;
                    replies.push(token);
                }
            });
            for token in replies {
                assert!(server.respond(token, 200, &[], b"ok"));
            }
            closed = read_available(&mut client, &mut received);
            thread::sleep(Duration::from_millis(1));
        }
        if accepted {
            assert!(response_len(&received).is_some());
        } else {
            assert!(closed);
        }
        assert!(std::str::from_utf8(&received).unwrap().starts_with(&format!("HTTP/1.1 {status}")));
        assert_eq!(requests > 0, accepted);
    }
}

#[test]
fn bare_lf_head_rejected() {
    let (mut server, addr) = server();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(b"GET / HTTP/1.1\nHost: x\n\n").unwrap();
    let deadline = Instant::now() + TIMEOUT;
    let mut received = Vec::new();
    let mut closed = false;
    while Instant::now() < deadline && !closed {
        server.pump(|_| {});
        closed = read_available(&mut client, &mut received);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(closed);
    assert!(received.starts_with(b"HTTP/1.1 400"));
}

fn assert_chunked_response_disconnect(response: &'static [u8], max_headers: usize) {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0; 1024];
        let _ = stream.read(&mut request);
        stream.write_all(response).unwrap();
    });
    let mut client = Http::with_config(HttpConfig::default().with_max_headers(max_headers));
    let token = client.connect(Endpoint::Tcp(addr));
    let deadline = Instant::now() + TIMEOUT;
    let mut sent = false;
    let mut disconnected = 0;
    while Instant::now() < deadline && disconnected == 0 {
        let mut connected = false;
        client.pump(|event| match event {
            HttpEvent::Connected { .. } => connected = true,
            HttpEvent::Disconnected { token: event_token } if event_token == token => {
                disconnected += 1;
            }
            _ => {}
        });
        if connected && !sent {
            assert!(client.request(token, "GET", "/", &[], b""));
            sent = true;
        }
        thread::sleep(Duration::from_millis(1));
    }
    peer.join().unwrap();
    assert_eq!(disconnected, 1);
}

#[test]
fn client_chunked_malformed_trailers_rejected() {
    assert_chunked_response_disconnect(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\ngarbage\r\n\r\n",
        64,
    );
    assert_chunked_response_disconnect(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nOne: 1\r\nTwo: 2\r\n\r\n",
        1,
    );
}

#[test]
fn client_chunked_overflow() {
    for response in [
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n10000000000000000\r\n".as_slice(),
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n11\r\n".as_slice(),
    ] {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request);
            stream.write_all(response).unwrap();
        });
        let mut client = Http::with_config(HttpConfig::default().with_max_body_bytes(16));
        let token = client.connect(Endpoint::Tcp(addr));
        let deadline = Instant::now() + TIMEOUT;
        let mut sent = false;
        let mut disconnected = 0;
        while Instant::now() < deadline && disconnected == 0 {
            let mut connected = false;
            client.pump(|event| match event {
                HttpEvent::Connected { .. } => connected = true,
                HttpEvent::Disconnected { token: event_token } if event_token == token => {
                    disconnected += 1;
                }
                _ => {}
            });
            if connected && !sent {
                assert!(client.request(token, "GET", "/", &[], b""));
                sent = true;
            }
            thread::sleep(Duration::from_millis(1));
        }
        peer.join().unwrap();
        assert_eq!(disconnected, 1);
    }
}

#[test]
fn idle_timeout_disconnects() {
    let addr = unused_addr();
    let mut server = Http::with_config(
        HttpConfig::default().with_idle_timeout(Duration::from_millis(200).into()),
    );
    server.listen(&Endpoint::Tcp(addr));
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut bytes = Vec::new();
    let mut closed = false;
    while Instant::now() < deadline && !closed {
        server.pump(|_| {});
        closed = read_available(&mut client, &mut bytes);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(closed);

    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let deadline = Instant::now() + TIMEOUT;
    let mut response = Vec::new();
    while Instant::now() < deadline && response_len(&response).is_none() {
        let mut replies = Vec::new();
        server.pump(|event| {
            if let HttpEvent::Request { token, .. } = event {
                replies.push(token);
            }
        });
        for token in replies {
            assert!(server.respond(token, 200, &[], b"ok"));
        }
        read_available(&mut client, &mut response);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(response_len(&response).is_some());
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut closed = false;
    while Instant::now() < deadline && !closed {
        server.pump(|_| {});
        closed = read_available(&mut client, &mut response);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(closed);
}

#[test]
fn pending_buffer_cap_disconnects() {
    let addr = unused_addr();
    let mut server =
        Http::with_config(HttpConfig::default().with_max_head_bytes(64).with_max_body_bytes(64));
    server.listen(&Endpoint::Tcp(addr));
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = false;
    while Instant::now() < deadline && !accepted {
        server.pump(|event| accepted |= matches!(event, HttpEvent::Request { .. }));
        thread::sleep(Duration::from_millis(1));
    }
    assert!(accepted);
    client.write_all(&[b'x'; 129]).unwrap();
    let mut received = Vec::new();
    let mut closed = false;
    while Instant::now() < deadline && !closed {
        server.pump(|_| {});
        closed = read_available(&mut client, &mut received);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(closed);
}

#[test]
fn pipelined_binary_bodies() {
    let (mut server, addr) = server();
    let first = b"\x00\n\xffone";
    let second = b"two\n\x00\xfe";
    let mut request = Vec::new();
    for (path, body) in [("/one", first.as_slice()), ("/two", second.as_slice())] {
        write!(
            request,
            "POST {path} HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .unwrap();
        request.extend_from_slice(body);
    }
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(&request).unwrap();
    let deadline = Instant::now() + TIMEOUT;
    let mut replies = Vec::new();
    let mut received = Vec::new();
    while Instant::now() < deadline && replies.len() < 2 {
        let mut pending = Vec::new();
        server.pump(|event| {
            if let HttpEvent::Request { token, request, .. } = event {
                pending.push((token, request.body.to_vec()));
            }
        });
        for (token, body) in pending {
            replies.push(body.clone());
            assert!(server.respond(token, 200, &[], &body));
        }
        read_available(&mut client, &mut received);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(replies, [first.to_vec(), second.to_vec()]);
    let first_len = response_len(&received).unwrap();
    let second_len = response_len(&received[first_len..]).unwrap();
    assert_eq!(&received[first_len - first.len()..first_len], first);
    assert_eq!(&received[first_len + second_len - second.len()..first_len + second_len], second);
}

#[test]
fn client_remove_stops_reconnect() {
    let (mut server, addr) = server();
    let mut client = Http::new();
    let token = client.connect(Endpoint::Tcp(addr));
    let deadline = Instant::now() + TIMEOUT;
    let mut connected = false;
    while Instant::now() < deadline && !connected {
        server.pump(|_| {});
        client.pump(|event| connected |= matches!(event, HttpEvent::Connected { token: event_token } if event_token == token));
        thread::sleep(Duration::from_millis(1));
    }
    assert!(connected);
    assert!(client.remove(token));
    assert!(!client.request(token, "GET", "/", &[], b""));
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut reconnects = 0;
    while Instant::now() < deadline {
        server.pump(|_| {});
        client.pump(|event| {
            if matches!(event, HttpEvent::Connected { token: event_token } if event_token == token)
            {
                reconnects += 1;
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(reconnects, 0);
}

#[test]
fn server_disconnect_kicks() {
    let (mut server, addr) = server();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + TIMEOUT;
    let mut token = None;
    while Instant::now() < deadline && token.is_none() {
        server.pump(|event| {
            if let HttpEvent::Accepted { token: connected, .. } = event {
                token = Some(connected);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    let token = token.expect("server must accept client");
    assert!(server.disconnect(token));
    let mut bytes = Vec::new();
    let mut closed = false;
    let mut disconnected = 0;
    while Instant::now() < deadline && (!closed || disconnected == 0) {
        server.pump(|event| {
            if matches!(event, HttpEvent::Disconnected { token: event_token } if event_token == token) {
                disconnected += 1;
            }
        });
        closed |= read_available(&mut client, &mut bytes);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(closed);
    assert_eq!(disconnected, 1);
}

#[test]
fn wrong_role_calls_return_false() {
    let addr = unused_addr();
    let mut http = Http::new();
    http.listen(&Endpoint::Tcp(addr));
    let outbound = http.connect(Endpoint::Tcp(addr));
    let stream = std::net::TcpStream::connect(addr).unwrap();
    stream.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = None;
    while Instant::now() < deadline && accepted.is_none() {
        http.pump(|event| {
            if let HttpEvent::Accepted { token, .. } = event {
                accepted = Some(token);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(!http.respond(outbound, 200, &[], b""));
    assert!(!http.request(accepted.unwrap(), "GET", "/", &[], b""));
}

#[test]
fn single_instance_serves_itself() {
    let addr = unused_addr();
    let mut http = Http::new();
    http.listen(&Endpoint::Tcp(addr));
    let outbound = http.connect(Endpoint::Tcp(addr));
    let deadline = Instant::now() + TIMEOUT;
    let mut sent = false;
    let mut body = None;
    while Instant::now() < deadline && body.is_none() {
        let mut respond = None;
        let mut request = false;
        http.pump(|event| match event {
            HttpEvent::Connected { token } if token == outbound => request = true,
            HttpEvent::Request { token, .. } => respond = Some(token),
            HttpEvent::Response { token, response } if token == outbound => {
                body = Some(response.body.to_vec());
            }
            _ => {}
        });
        if request && !sent {
            assert!(http.request(outbound, "GET", "/", &[], b""));
            sent = true;
        }
        if let Some(token) = respond {
            assert!(http.respond(token, 200, &[], b"self"));
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(body.as_deref(), Some(b"self".as_slice()));
}

over_both_transports!(
    outbound_host_header_names_the_endpoint,
    outbound_host_header_names_the_endpoint_tcp,
    outbound_host_header_names_the_endpoint_unix
);
fn outbound_host_header_names_the_endpoint(endpoint: &Endpoint) {
    let mut server = server_at(endpoint);
    let mut client = Http::new();
    let token = client.connect(endpoint.clone());
    let mut sent = false;
    let mut host = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && host.is_none() {
        let mut replies = Vec::new();
        server.pump(|event| {
            if let HttpEvent::Request { token, request, .. } = event {
                host = Some(request.header("Host").map(<[u8]>::to_vec));
                replies.push(token);
            }
        });
        for token in replies {
            assert!(server.respond(token, 200, &[], b""));
        }
        let mut connected = false;
        client.pump(|event| connected |= matches!(event, HttpEvent::Connected { .. }));
        if connected && !sent {
            assert!(client.request(token, "GET", "/", &[], b""));
            sent = true;
        }
        thread::sleep(Duration::from_millis(1));
    }

    let expected = match endpoint {
        // A Unix-domain endpoint has no address to name.
        Endpoint::Unix(_) => "localhost".to_owned(),
        Endpoint::Tcp(addr) => addr.to_string(),
    };
    assert_eq!(host.flatten().as_deref(), Some(expected.as_bytes()));
}

over_both_transports!(accepted_reports_peer, accepted_reports_peer_tcp, accepted_reports_peer_unix);
fn accepted_reports_peer(endpoint: &Endpoint) {
    let mut server = server_at(endpoint);
    let _client = connect_client(endpoint);
    let mut accepted = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && accepted.is_none() {
        server.pump(|event| {
            if let HttpEvent::Accepted { peer, .. } = event {
                accepted = Some(peer);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    match endpoint {
        Endpoint::Tcp(_) => assert!(matches!(accepted, Some(Peer::Tcp(_))), "{accepted:?}"),
        Endpoint::Unix(_) => assert_eq!(accepted, Some(Peer::Unix)),
    }
}

// ---------------------------------------------------------------------------
// Responder, consumption and readiness

/// Pulls events until one request has been delivered, returning its token and
/// what it said. The responder is dropped, which defers the response.
fn pull_deferred_request(server: &mut Http) -> (Token, String) {
    let deadline = Instant::now() + TIMEOUT;
    let mut request = None;
    while Instant::now() < deadline && request.is_none() {
        server.pump(|event| {
            if let HttpEvent::Request { token, request: pulled, responder } = event {
                request = Some((token, pulled.path.to_owned()));
                drop(responder);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    request.expect("no request arrived")
}

fn read_until_response(server: &mut Http, client: &mut impl Read, out: &mut Vec<u8>) {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && response_len(out).is_none() {
        server.pump(|_| {});
        read_available(client, out);
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn a_dropped_responder_answers_later_by_token() {
    let (mut server, addr) = server();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(b"GET /defer HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();

    let (token, path) = pull_deferred_request(&mut server);
    assert_eq!(path, "/defer");
    let mut received = Vec::new();
    read_available(&mut client, &mut received);
    assert!(received.is_empty(), "a dropped responder answers nothing by itself");

    assert!(server.respond(token, 200, &[], b"late"));
    read_until_response(&mut server, &mut client, &mut received);
    assert!(received.ends_with(b"late"), "{received:?}");
}

#[test]
fn a_pipelined_request_waits_for_the_pending_response() {
    let (mut server, addr) = server();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client
        .write_all(b"GET /one HTTP/1.1\r\nHost: x\r\n\r\nGET /two HTTP/1.1\r\nHost: x\r\n\r\n")
        .unwrap();

    let (token, path) = pull_deferred_request(&mut server);
    assert_eq!(path, "/one");

    let mut paths = Vec::new();
    for _ in 0..20 {
        server.pump(|event| {
            if let HttpEvent::Request { request, .. } = event {
                paths.push(request.path.to_owned());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(paths.is_empty(), "one request in flight per connection: {paths:?}");

    assert!(server.respond(token, 200, &[], b"one"));
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && paths.is_empty() {
        let mut replies = Vec::new();
        server.pump(|event| {
            if let HttpEvent::Request { token, request, .. } = event {
                paths.push(request.path.to_owned());
                replies.push(token);
            }
        });
        for token in replies {
            assert!(server.respond(token, 200, &[], b"two"));
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(paths, ["/two"]);
}

/// Serves two pipelined requests, echoing each path back as its body and
/// answering the first inline or by token, and reports the paths pulled and
/// every byte the client read.
fn serve_two_pipelined_requests(inline_first: bool) -> (Vec<String>, Vec<u8>) {
    let (mut server, addr) = server();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client
        .write_all(b"GET /one HTTP/1.1\r\nHost: x\r\n\r\nGET /two HTTP/1.1\r\nHost: x\r\n\r\n")
        .unwrap();

    let deadline = Instant::now() + TIMEOUT;
    let mut pulled: Vec<String> = Vec::new();
    let mut received = Vec::new();
    while Instant::now() < deadline && !received.ends_with(b"/two") {
        let mut deferred = None;
        server.drive();
        while let Some(event) = server.service.next_event(&mut server.net) {
            if let HttpEvent::Request { token, request, responder } = event {
                let path = request.path.to_owned();
                if pulled.is_empty() && !inline_first {
                    drop(responder);
                    deferred = Some(token);
                } else {
                    assert!(responder.respond(200, &[], path.as_bytes()));
                }
                pulled.push(path);
            }
        }
        if let Some(token) = deferred {
            assert!(server.respond(token, 200, &[], pulled[0].as_bytes()));
        }
        read_available(&mut client, &mut received);
        thread::sleep(Duration::from_millis(1));
    }
    (pulled, received)
}

#[test]
fn both_answer_paths_write_the_same_responses() {
    let (inline_paths, inline_bytes) = serve_two_pipelined_requests(true);
    let (deferred_paths, deferred_bytes) = serve_two_pipelined_requests(false);
    assert_eq!(inline_paths, ["/one", "/two"], "the requests are served in order");
    assert_eq!(deferred_paths, inline_paths, "both answer paths pull the same order");
    assert_eq!(
        String::from_utf8_lossy(&deferred_bytes),
        String::from_utf8_lossy(&inline_bytes),
        "both answer paths put the same bytes on the wire"
    );
}

/// Answers the first of two pipelined requests, inline or by token, and
/// serves the second from what the connection buffered behind it.
fn serve_the_request_behind_an_answer(inline_first: bool) {
    let path = if inline_first { "answered inline" } else { "answered by token" };
    let (mut server, addr) = server();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    let mut wire = b"GET /one HTTP/1.1\r\nHost: x\r\n\r\n".to_vec();
    wire.extend_from_slice(b"POST /two HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\nbody");
    client.write_all(&wire).unwrap();

    let deadline = Instant::now() + TIMEOUT;
    let mut answered = false;
    while Instant::now() < deadline && !answered {
        let mut deferred = None;
        server.drive();
        while let Some(event) = server.service.next_event(&mut server.net) {
            if let HttpEvent::Request { token, request, responder } = event {
                assert_eq!(request.path, "/one", "{path}");
                if inline_first {
                    assert!(responder.respond(200, &[], b"one"));
                } else {
                    drop(responder);
                    deferred = Some(token);
                }
                answered = true;
                break
            }
        }
        if let Some(token) = deferred {
            assert!(server.respond(token, 200, &[], b"one"));
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(answered, "no request arrived");

    // A connection holding the request behind an answered one is work,
    // whichever way the answer was given.
    assert!(server.drive(), "the request behind the answered one is work: {path}");

    let mut pulled = false;
    while Instant::now() < deadline && !pulled {
        while let Some(event) = server.service.next_event(&mut server.net) {
            if let HttpEvent::Request { request, responder, .. } = event {
                assert_eq!(request.method, "POST", "{path}");
                assert_eq!(request.path, "/two", "{path}");
                assert_eq!(request.body, b"body".as_slice(), "{path}");
                assert!(responder.respond(200, &[], b"two"));
                pulled = true;
                break
            }
        }
        if !pulled {
            server.drive();
            thread::sleep(Duration::from_millis(1));
        }
    }
    assert!(pulled, "the pipelined request was never served: {path}");

    // Both answers reach the client, which the connection outlives.
    let mut received = Vec::new();
    while Instant::now() < deadline && !received.ends_with(b"two") {
        server.pump(|_| {});
        assert!(!read_available(&mut client, &mut received), "the connection closed: {path}");
        thread::sleep(Duration::from_millis(1));
    }
    assert!(received.ends_with(b"two"), "{path}: {received:?}");
    assert_eq!(received.windows(3).filter(|w| *w == b"one").count(), 1, "{path}");
}

#[test]
fn a_pipelined_request_is_served_after_either_answer_path() {
    serve_the_request_behind_an_answer(true);
    serve_the_request_behind_an_answer(false);
}

#[test]
fn a_second_response_to_one_request_is_refused() {
    let (mut server, addr) = server();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(b"GET /once HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();

    let (token, _) = pull_deferred_request(&mut server);
    assert!(server.respond(token, 200, &[], b"first"));
    assert!(!server.respond(token, 200, &[], b"second"), "a request is answered once");

    let mut received = Vec::new();
    read_until_response(&mut server, &mut client, &mut received);
    assert_eq!(received.windows(5).filter(|w| *w == b"first").count(), 1);
    assert_eq!(received.windows(6).filter(|w| *w == b"second").count(), 0);
}

#[test]
fn answered_bytes_cost_the_connection_nothing() {
    let addr = unused_addr();
    // Two of these requests together outgrow the limit; one at a time does
    // not, and an answered one costs nothing even before it is reclaimed.
    let mut server =
        Http::with_config(HttpConfig::default().with_max_head_bytes(64).with_max_body_bytes(0));
    server.listen(&Endpoint::Tcp(addr));
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    let request = padded_request("/first", 60);
    client.write_all(&request).unwrap();

    // Answer inline and stop pulling, which leaves the answered bytes in the
    // buffer with nothing reclaimed.
    let deadline = Instant::now() + TIMEOUT;
    let mut token = None;
    while Instant::now() < deadline && token.is_none() {
        server.drive();
        while let Some(event) = server.service.next_event(&mut server.net) {
            if let HttpEvent::Request { token: pulled, responder, .. } = event {
                assert!(responder.respond(200, &[], b""));
                token = Some(pulled);
                break
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(token.is_some(), "no request arrived");

    // The next request arrives before the pull that would reclaim them.
    client.write_all(&padded_request("/second", 60)).unwrap();

    let mut paths = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && paths.is_empty() {
        let mut replies = Vec::new();
        server.pump(|event| {
            if let HttpEvent::Request { token, request, .. } = event {
                paths.push(request.path.to_owned());
                replies.push(token);
            }
        });
        for token in replies {
            assert!(server.respond(token, 200, &[], b""));
        }
        let mut received = Vec::new();
        assert!(!read_available(&mut client, &mut received), "the connection stayed open");
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(paths, ["/second"]);
}

/// A request of exactly `size` bytes, padded out with a header.
fn padded_request(path: &str, size: usize) -> Vec<u8> {
    let mut request = Vec::new();
    write!(request, "GET {path} HTTP/1.1\r\nX-Pad: \r\n\r\n").unwrap();
    let pad = size.checked_sub(request.len()).expect("the request outgrew its size");
    let mut request = Vec::new();
    write!(request, "GET {path} HTTP/1.1\r\nX-Pad: {}\r\n\r\n", "p".repeat(pad)).unwrap();
    assert_eq!(request.len(), size);
    request
}

#[test]
fn a_pending_request_survives_a_compaction() {
    let addr = unused_addr();
    let mut server =
        Http::with_config(HttpConfig::default().with_max_head_bytes(256).with_max_body_bytes(0));
    server.listen(&Endpoint::Tcp(addr));
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    let mut pipelined = Vec::new();
    for path in ["/one", "/two", "/three"] {
        pipelined.extend_from_slice(&padded_request(path, 64));
    }
    client.write_all(&pipelined).unwrap();

    // Answer the first request and leave the second one pending, so that the
    // buffer holds an answered prefix, an unanswered request, and a third
    // request behind it.
    let deadline = Instant::now() + TIMEOUT;
    let mut pulled = Vec::new();
    let mut pending = None;
    while Instant::now() < deadline && pulled.len() < 2 {
        server.drive();
        while pulled.len() < 2 {
            let Some(event) = server.service.next_event(&mut server.net) else { break };
            if let HttpEvent::Request { token, request, responder } = event {
                pulled.push(request.path.to_owned());
                if pulled.len() == 1 {
                    assert!(responder.respond(200, &[], b""));
                } else {
                    pending = Some(token);
                }
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(pulled, ["/one", "/two"]);
    let pending = pending.unwrap();

    // A fourth request outgrows what the limit leaves, so it is read only
    // once the answered prefix goes — which happens under the pending
    // request, whose parsed bytes move down with the buffer. Answering it
    // from rebased cursors is what lets the last two requests through.
    client.write_all(&padded_request("/four", 96)).unwrap();
    for _ in 0..20 {
        server.drive();
        thread::sleep(Duration::from_millis(1));
    }

    assert!(server.respond(pending, 200, &[], b""));
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && pulled.len() < 4 {
        let mut replies = Vec::new();
        server.pump(|event| {
            if let HttpEvent::Request { token, request, .. } = event {
                pulled.push(request.path.to_owned());
                replies.push(token);
            }
        });
        for token in replies {
            assert!(server.respond(token, 200, &[], b""));
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(pulled, ["/one", "/two", "/three", "/four"]);
}

#[test]
fn pipelined_requests_stay_ranged_across_a_compaction() {
    let (mut server, addr) = server();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    let mut wire = Vec::new();
    for index in 0..3 {
        write!(
            wire,
            "POST /path-{index} HTTP/1.1\r\nHost: x\r\nX-Index: {index}\r\nContent-Length: 5\r\n\r\nbody{index}"
        )
        .unwrap();
    }
    client.write_all(&wire).unwrap();

    // Three requests of one size: answering the first two puts the answered
    // prefix past half the buffer, so the third is parsed from a buffer whose
    // prefix has been dropped, and every range it carries is rebased.
    let mut seen = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && seen.len() < 3 {
        let mut replies = Vec::new();
        server.pump(|event| {
            if let HttpEvent::Request { token, request, .. } = event {
                seen.push((
                    request.method.to_owned(),
                    request.path.to_owned(),
                    request.header("x-index").map(<[u8]>::to_vec),
                    request.body.to_vec(),
                ));
                replies.push(token);
            }
        });
        for token in replies {
            assert!(server.respond(token, 200, &[], b"ok"));
        }
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(seen.len(), 3, "not every request was served");
    for (index, (method, path, header, body)) in seen.iter().enumerate() {
        assert_eq!(method, "POST");
        assert_eq!(path, &format!("/path-{index}"));
        assert_eq!(header.as_deref(), Some(index.to_string().as_bytes()));
        assert_eq!(body, format!("body{index}").as_bytes());
    }
}

#[test]
fn a_caller_may_stop_pulling_and_resume_later() {
    let (mut server, addr) = server();
    let mut first = std::net::TcpStream::connect(addr).unwrap();
    let mut second = std::net::TcpStream::connect(addr).unwrap();
    first.set_nonblocking(true).unwrap();
    second.set_nonblocking(true).unwrap();
    first.write_all(b"GET /first HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    second.write_all(b"GET /second HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();

    // Stop after the first request of the iteration, leaving the rest queued.
    let deadline = Instant::now() + TIMEOUT;
    let mut deferred = None;
    while Instant::now() < deadline && deferred.is_none() {
        server.drive();
        if let Some(HttpEvent::Request { token, .. }) = server.service.next_event(&mut server.net) {
            deferred = Some(token);
        }
        thread::sleep(Duration::from_millis(1));
    }
    let deferred = deferred.expect("no request arrived");

    // The next iteration delivers what was left, and the deferred response is
    // still available.
    let mut paths = Vec::new();
    while Instant::now() < deadline && paths.is_empty() {
        let mut replies = Vec::new();
        server.pump(|event| {
            if let HttpEvent::Request { token, request, .. } = event {
                paths.push(request.path.to_owned());
                replies.push(token);
            }
        });
        for token in replies {
            assert!(server.respond(token, 200, &[], b"ok"));
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(paths.len(), 1, "the un-pulled request survived the iteration: {paths:?}");
    assert!(server.respond(deferred, 200, &[], b"resumed"));

    let mut received = Vec::new();
    let mut peer = if paths[0] == "/first" { second } else { first };
    read_until_response(&mut server, &mut peer, &mut received);
    assert!(received.ends_with(b"resumed"), "{received:?}");
}

#[test]
fn a_request_split_across_reads_is_delivered_once_complete() {
    let (mut server, addr) = server();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(b"GET /split HTTP/1.1\r\n").unwrap();

    // Half a head is no request, however many iterations read it.
    for _ in 0..20 {
        server.pump(|event| {
            assert!(!matches!(event, HttpEvent::Request { .. }), "half a head was delivered");
        });
        thread::sleep(Duration::from_millis(1));
    }

    client.write_all(b"Host: x\r\n\r\n").unwrap();
    let deadline = Instant::now() + TIMEOUT;
    let mut path = None;
    while Instant::now() < deadline && path.is_none() {
        server.pump(|event| {
            if let HttpEvent::Request { request, responder, .. } = event {
                path = Some(request.path.to_owned());
                assert!(responder.respond(200, &[], b"ok"));
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(path.as_deref(), Some("/split"));

    let mut received = Vec::new();
    read_until_response(&mut server, &mut client, &mut received);
    assert!(received.ends_with(b"ok"), "{received:?}");
}

#[test]
fn a_pending_request_reports_work_and_holds_off_the_sweep() {
    let addr = unused_addr();
    let mut server = Http::with_config(
        HttpConfig::default().with_idle_timeout(Duration::from_millis(200).into()),
    );
    server.listen(&Endpoint::Tcp(addr));
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(b"GET /slow HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();

    let deadline = Instant::now() + TIMEOUT;
    let mut arrived = false;
    while Instant::now() < deadline && !arrived {
        arrived = server.drive();
        thread::sleep(Duration::from_millis(1));
    }
    assert!(arrived, "the client never reached the server");
    // An event nobody pulled is work, iteration after iteration.
    assert!(server.drive(), "un-pulled events must keep the caller awake");

    let mut token = None;
    while Instant::now() < deadline && token.is_none() {
        server.drive();
        while let Some(event) = server.service.next_event(&mut server.net) {
            if let HttpEvent::Request { token: pulled, .. } = event {
                token = Some(pulled);
                break
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
    let token = token.expect("no request arrived");

    // Inbound bytes keep resetting the idle sweep while the response is owed.
    let until = Instant::now() + Duration::from_millis(600);
    let mut bytes = Vec::new();
    while Instant::now() < until {
        client.write_all(b"X").unwrap();
        server.pump(|_| {});
        assert!(!read_available(&mut client, &mut bytes), "the sweep took a busy connection");
        thread::sleep(Duration::from_millis(20));
    }
    assert!(server.respond(token, 200, &[], b"ok"));
}

#[test]
fn work_is_reported_after_an_inline_answer_stops_the_pull() {
    let (mut server, addr) = server();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client
        .write_all(b"GET /one HTTP/1.1\r\nHost: x\r\n\r\nGET /two HTTP/1.1\r\nHost: x\r\n\r\n")
        .unwrap();

    // Answer the first request inline and stop pulling, which leaves the
    // pipelined one behind it to be picked up by the tick.
    let deadline = Instant::now() + TIMEOUT;
    let mut answered = false;
    while Instant::now() < deadline && !answered {
        server.drive();
        while let Some(event) = server.service.next_event(&mut server.net) {
            if let HttpEvent::Request { request, responder, .. } = event {
                assert_eq!(request.path, "/one");
                assert!(responder.respond(200, &[], b"one"));
                answered = true;
                break
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(answered, "no request arrived");

    assert!(server.drive(), "the request behind the answered one is work");
    match server.service.next_event(&mut server.net) {
        Some(HttpEvent::Request { request, responder, .. }) => {
            assert_eq!(request.path, "/two");
            assert!(responder.respond(200, &[], b"two"));
        }
        _ => panic!("the pipelined request was not delivered"),
    }
}

#[test]
fn a_blocking_drive_wakes_for_the_idle_sweep() {
    let addr = unused_addr();
    let mut server = Http::with_config(
        HttpConfig::default().with_idle_timeout(Duration::from_millis(500).into()),
    );
    server.listen(&Endpoint::Tcp(addr));
    let _client = std::net::TcpStream::connect(addr).unwrap();
    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = false;
    while Instant::now() < deadline && !accepted {
        server.pump(|event| accepted |= matches!(event, HttpEvent::Accepted { .. }));
        thread::sleep(Duration::from_millis(1));
    }
    assert!(accepted, "the client was never accepted");

    // The sweep of that connection is the only deadline the network has, so
    // an uncapped drive waits for it and no longer. The late connection is
    // the test's own deadline: a drive that ignored the sweep would wake on
    // it instead, well past the bound below, rather than hang.
    thread::spawn(move || {
        thread::sleep(Duration::from_secs(3));
        drop(std::net::TcpStream::connect(addr));
    });
    let started = Instant::now();
    server.net.drive(None, &mut [server.service.as_service()], |_| {});
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(100), "returned at once: {waited:?}");
    assert!(waited < Duration::from_secs(2), "the sweep deadline was not folded: {waited:?}");
}

#[test]
fn a_blocking_drive_wakes_for_a_connection() {
    let addr = unused_addr();
    let mut server = Http::with_config(HttpConfig::default().without_idle_timeout());
    server.listen(&Endpoint::Tcp(addr));

    // Nothing is due, so an uncapped drive blocks until a client arrives. The
    // second connection is the test's own deadline: it wakes the poll even if
    // the first one never lands, so a regression fails instead of hanging.
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        let early = std::net::TcpStream::connect(addr);
        thread::sleep(Duration::from_secs(3));
        let late = std::net::TcpStream::connect(addr);
        drop((early, late));
    });
    let started = Instant::now();
    let worked = server.net.drive(None, &mut [server.service.as_service()], |_| {});
    let waited = started.elapsed();
    assert!(worked, "an accepted connection is work");
    assert!(waited >= Duration::from_millis(150), "the drive did not block: {waited:?}");
    assert!(waited < Duration::from_secs(2), "the drive missed the connection: {waited:?}");

    let mut accepted = false;
    while let Some(event) = server.service.next_event(&mut server.net) {
        accepted |= matches!(event, HttpEvent::Accepted { .. });
    }
    assert!(accepted, "the connection that woke the poll was not delivered");
}

#[test]
fn a_request_from_a_client_that_left_is_dropped() {
    let (mut server, addr) = server();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.write_all(b"GET /gone HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    client.shutdown(std::net::Shutdown::Both).unwrap();
    drop(client);
    // The request and the end of stream are both queued before the server
    // reads, so one iteration sees the whole story.
    thread::sleep(Duration::from_millis(50));

    let mut order = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && !order.contains(&"disconnected") {
        server.pump(|event| {
            order.push(match event {
                HttpEvent::Accepted { .. } => "accepted",
                HttpEvent::Request { .. } => "request",
                HttpEvent::Disconnected { .. } => "disconnected",
                _ => "other",
            });
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(order, ["accepted", "disconnected"], "a request nobody can answer was delivered");
}

#[test]
#[should_panic(expected = "drive the network with StreamNetwork::drive")]
fn polling_a_network_that_has_services_is_refused() {
    let mut net = StreamNetwork::default();
    let group = net.add_group(http_group());
    let _http = HttpService::new(&mut net, group, HttpConfig::default());
    net.poll_with(|_| {});
}

#[test]
fn an_accepted_connection_is_announced_before_its_first_request() {
    let (mut server, addr) = server();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(b"GET /first HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();

    let mut order = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && order.len() < 2 {
        let mut replies = Vec::new();
        server.pump(|event| match event {
            HttpEvent::Accepted { .. } => order.push("accepted"),
            HttpEvent::Request { token, .. } => {
                order.push("request");
                replies.push(token);
            }
            _ => {}
        });
        for token in replies {
            assert!(server.respond(token, 200, &[], b"ok"));
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(order, ["accepted", "request"]);
}

over_both_modes!(
    two_services_on_one_network_keep_their_own_events,
    two_services_on_one_network_keep_their_own_events_owned,
    two_services_on_one_network_keep_their_own_events_external
);
fn two_services_on_one_network_keep_their_own_events(mode: Mode) {
    let (mut driver, mut net) = Driver::build(mode);
    let first_group = net.add_group(ConnectionGroupConfig { name: "first", ..http_group() });
    let second_group = net.add_group(ConnectionGroupConfig { name: "second", ..http_group() });
    let mut first = HttpService::new(&mut net, first_group, HttpConfig::default());
    let mut second = HttpService::new(&mut net, second_group, HttpConfig::default());
    let first_addr = unused_addr();
    let second_addr = unused_addr();
    first.listen(&mut net, Endpoint::Tcp(first_addr)).unwrap();
    second.listen(&mut net, Endpoint::Tcp(second_addr)).unwrap();

    let mut to_first = std::net::TcpStream::connect(first_addr).unwrap();
    let mut to_second = std::net::TcpStream::connect(second_addr).unwrap();
    to_first.write_all(b"GET /first HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    to_second.write_all(b"GET /second HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();

    let mut first_paths = Vec::new();
    let mut second_paths = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && (first_paths.is_empty() || second_paths.is_empty()) {
        driver.iterate(&mut net, &mut [first.as_service(), second.as_service()], |event| {
            panic!("no unclaimed group exists: {:?}", event_group(&event))
        });
        while let Some(event) = first.next_event(&mut net) {
            if let HttpEvent::Request { request, responder, .. } = event {
                first_paths.push(request.path.to_owned());
                assert!(responder.respond(200, &[], b"first"));
            }
        }
        while let Some(event) = second.next_event(&mut net) {
            if let HttpEvent::Request { request, responder, .. } = event {
                second_paths.push(request.path.to_owned());
                assert!(responder.respond(200, &[], b"second"));
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(first_paths, ["/first"]);
    assert_eq!(second_paths, ["/second"]);

    first.close(&mut net);
    second.close(&mut net);
}

fn event_group(event: &StreamEvent<'_>) -> &'static str {
    match event {
        StreamEvent::Accepted { .. } => "accepted",
        StreamEvent::Connected { .. } => "connected",
        StreamEvent::Message { .. } => "message",
        StreamEvent::Disconnected { .. } => "disconnected",
    }
}

// ---------------------------------------------------------------------------
// Service lifecycle

#[test]
fn close_returns_the_group_to_raw_use() {
    let mut net = StreamNetwork::default();
    let group = net.add_group(http_group());
    let other_group = net.add_group(http_group());
    let mut http = HttpService::new(&mut net, group, HttpConfig::default());
    let mut other = HttpService::new(&mut net, other_group, HttpConfig::default());
    http.listen(&mut net, Endpoint::Tcp(unused_addr())).unwrap();
    http.close(&mut net);

    // The remaining service alone passes validation.
    net.drive(Some(Duration::ZERO.into()), &mut [other.as_service()], |_| {});

    let addr = unused_addr();
    net.listen(group, Endpoint::Tcp(addr)).unwrap();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.write_all(b"raw bytes").unwrap();
    let mut raw = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && raw.is_empty() {
        net.drive(Some(Duration::ZERO.into()), &mut [other.as_service()], |event| {
            if let StreamEvent::Message { group: event_group, payload, .. } = event {
                assert_eq!(event_group, group);
                raw.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(raw, [b"raw bytes".to_vec()]);
    other.close(&mut net);
}

#[test]
fn close_hangs_up_on_connections_and_listeners() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("closed.sock");
    let mut net = StreamNetwork::default();
    let group = net.add_group(http_group());
    let mut http = HttpService::new(&mut net, group, HttpConfig::default());
    http.listen(&mut net, Endpoint::Unix(path.clone())).unwrap();
    assert!(path.exists());

    let mut client = std::os::unix::net::UnixStream::connect(&path).unwrap();
    client.write_all(b"GET /x HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = false;
    while Instant::now() < deadline && !accepted {
        net.drive(Some(Duration::ZERO.into()), &mut [http.as_service()], |_| {});
        while let Some(event) = http.next_event(&mut net) {
            accepted |= matches!(event, HttpEvent::Accepted { .. });
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(accepted);

    http.close(&mut net);
    assert!(!path.exists(), "the socket file outlived its listener");
    assert!(std::os::unix::net::UnixStream::connect(&path).is_err());
    client.set_nonblocking(true).unwrap();
    let mut bytes = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    let mut closed = false;
    while Instant::now() < deadline && !closed {
        closed = read_available(&mut client, &mut bytes);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(closed, "the peer never saw the end of the stream");
}

#[test]
#[should_panic(expected = "call HttpService::close before dropping it")]
fn a_service_dropped_without_closing_is_reported() {
    let mut net = StreamNetwork::default();
    let group = net.add_group(http_group());
    let http = HttpService::new(&mut net, group, HttpConfig::default());
    drop(http);
    net.drive(Some(Duration::ZERO.into()), &mut [], |_| {});
}

#[test]
fn dropping_a_service_and_its_network_together_is_harmless() {
    let mut net = StreamNetwork::default();
    let group = net.add_group(http_group());
    let mut http = HttpService::new(&mut net, group, HttpConfig::default());
    http.listen(&mut net, Endpoint::Tcp(unused_addr())).unwrap();
    net.drive(Some(Duration::ZERO.into()), &mut [http.as_service()], |_| {});
    drop(http);
    drop(net);
}

#[test]
#[should_panic(expected = "HTTP frames its own messages and needs a raw-framed group")]
fn a_service_refuses_a_length_prefixed_group() {
    let mut net = StreamNetwork::default();
    let group = net.add_group(ConnectionGroupConfig::default());
    let _http = HttpService::new(&mut net, group, HttpConfig::default());
}
