use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr},
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    http::{HttpEvent, HttpNetwork},
    stream::{Endpoint, Peer},
};

const TIMEOUT: Duration = Duration::from_secs(10);

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

fn server_at(endpoint: &Endpoint) -> HttpNetwork {
    let mut server = HttpNetwork::default();
    server.listen(endpoint.clone()).unwrap();
    server
}

fn server() -> (HttpNetwork, SocketAddr) {
    let addr = unused_addr();
    (server_at(&Endpoint::Tcp(addr)), addr)
}

over_both_transports!(
    get_keepalive_two_requests,
    get_keepalive_two_requests_tcp,
    get_keepalive_two_requests_unix
);
fn get_keepalive_two_requests(endpoint: &Endpoint) {
    let mut server = server_at(endpoint);
    let mut client = connect_client(endpoint);
    let deadline = Instant::now() + TIMEOUT;
    let mut first = Vec::new();
    client.write_all(b"GET /one HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    while Instant::now() < deadline && response_len(&first).is_none() {
        let mut replies = Vec::new();
        server.poll_with(|e| {
            if let HttpEvent::Request { token, request } = e {
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
        server.poll_with(|e| {
            if let HttpEvent::Request { token, request } = e {
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

over_both_transports!(post_echo_body, post_echo_body_tcp, post_echo_body_unix);
fn post_echo_body(endpoint: &Endpoint) {
    let mut server = server_at(endpoint);
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
        server.poll_with(|e| {
            if let HttpEvent::Request { token, request } = e {
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
        server.poll_with(|e| {
            if let HttpEvent::Request { token, request } = e {
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
        server.poll_with(|e| {
            if let HttpEvent::Request { token, request } = e {
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
    let mut server = HttpNetwork::default().with_socket_buf_size(1024);
    server.listen(Endpoint::Tcp(addr)).unwrap();
    let body = vec![7; 256 * 1024];
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").unwrap();
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && !read_available(&mut client, &mut received) {
        let mut tokens = Vec::new();
        server.poll_with(|e| {
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
        let mut server = HttpNetwork::default().with_max_head_bytes(64).with_max_body_bytes(8);
        server.listen(Endpoint::Tcp(addr)).unwrap();
        let mut client = std::net::TcpStream::connect(addr).unwrap();
        client.set_nonblocking(true).unwrap();
        client.write_all(request).unwrap();
        let mut received = Vec::new();
        let deadline = Instant::now() + TIMEOUT;
        while Instant::now() < deadline && !read_available(&mut client, &mut received) {
            server.poll_with(|_| {});
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
        server.poll_with(|e| {
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

over_both_transports!(pipelined_requests, pipelined_requests_tcp, pipelined_requests_unix);
fn pipelined_requests(endpoint: &Endpoint) {
    let mut server = server_at(endpoint);
    let mut client = connect_client(endpoint);
    client
        .write_all(b"GET /one HTTP/1.1\r\nHost: x\r\n\r\nGET /two HTTP/1.1\r\nHost: x\r\n\r\n")
        .unwrap();
    let mut paths = Vec::new();
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && paths.len() < 2 {
        let mut replies = Vec::new();
        server.poll_with(|e| {
            if let HttpEvent::Request { token, request } = e {
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

over_both_transports!(
    client_server_roundtrip,
    client_server_roundtrip_tcp,
    client_server_roundtrip_unix
);
fn client_server_roundtrip(endpoint: &Endpoint) {
    let mut server = server_at(endpoint);
    let mut client = HttpNetwork::default();
    let token = client.connect(endpoint.clone());
    let mut sent = false;
    let mut bodies = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && bodies.len() < 2 {
        let mut replies = Vec::new();
        server.poll_with(|e| {
            if let HttpEvent::Request { token, request } = e {
                replies.push((token, request.body.to_vec()));
            }
        });
        for (token, body) in replies {
            server.respond(token, 200, &[("X-Reply", "yes")], &body);
        }
        let mut send_second = false;
        let mut connected = false;
        client.poll_with(|e| match e {
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
    let mut client = HttpNetwork::default();
    let token = client.connect(Endpoint::Tcp(addr));
    let mut sent = false;
    let mut body = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && body.is_none() {
        let mut connected = false;
        client.poll_with(|e| match e {
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
    let mut client = HttpNetwork::default().with_max_body_bytes(8);
    let token = client.connect(Endpoint::Tcp(addr));
    let deadline = Instant::now() + TIMEOUT;
    let mut connected = false;
    let mut response = None;
    while Instant::now() < deadline && response.is_none() {
        client.poll_with(|event| match event {
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
    let mut client = HttpNetwork::default();
    let token = client.connect(Endpoint::Tcp(addr));
    let mut sent = false;
    let mut bodies = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && bodies.len() < 2 {
        let mut connected = false;
        let mut respond_again = false;
        client.poll_with(|e| match e {
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
    let mut client = HttpNetwork::default();
    let token = client.connect(Endpoint::Tcp(addr));
    let mut connected = 0;
    let mut disconnected = 0;
    let mut responses = 0;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && responses < 2 {
        let mut replies = Vec::new();
        server.poll_with(|e| {
            if let HttpEvent::Request { token, .. } = e {
                replies.push(token);
            }
        });
        for token in replies {
            server.respond(token, 200, &[("Connection", "close")], b"ok");
        }
        let mut request_again = false;
        client.poll_with(|e| match e {
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
            server.poll_with(|event| {
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
        server.poll_with(|_| {});
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
    let mut client = HttpNetwork::default().with_max_headers(max_headers);
    let token = client.connect(Endpoint::Tcp(addr));
    let deadline = Instant::now() + TIMEOUT;
    let mut sent = false;
    let mut disconnected = 0;
    while Instant::now() < deadline && disconnected == 0 {
        let mut connected = false;
        client.poll_with(|event| match event {
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
        let mut client = HttpNetwork::default().with_max_body_bytes(16);
        let token = client.connect(Endpoint::Tcp(addr));
        let deadline = Instant::now() + TIMEOUT;
        let mut sent = false;
        let mut disconnected = 0;
        while Instant::now() < deadline && disconnected == 0 {
            let mut connected = false;
            client.poll_with(|event| match event {
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
    let mut server = HttpNetwork::default().with_idle_timeout(Duration::from_millis(200).into());
    server.listen(Endpoint::Tcp(addr)).unwrap();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut bytes = Vec::new();
    let mut closed = false;
    while Instant::now() < deadline && !closed {
        server.poll_with(|_| {});
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
        server.poll_with(|event| {
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
        server.poll_with(|_| {});
        closed = read_available(&mut client, &mut response);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(closed);
}

#[test]
fn pending_buffer_cap_disconnects() {
    let addr = unused_addr();
    let mut server = HttpNetwork::default().with_max_head_bytes(64).with_max_body_bytes(64);
    server.listen(Endpoint::Tcp(addr)).unwrap();
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = false;
    while Instant::now() < deadline && !accepted {
        server.poll_with(|event| accepted |= matches!(event, HttpEvent::Request { .. }));
        thread::sleep(Duration::from_millis(1));
    }
    assert!(accepted);
    client.write_all(&[b'x'; 129]).unwrap();
    let mut received = Vec::new();
    let mut closed = false;
    while Instant::now() < deadline && !closed {
        server.poll_with(|_| {});
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
        server.poll_with(|event| {
            if let HttpEvent::Request { token, request } = event {
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
    let mut client = HttpNetwork::default();
    let token = client.connect(Endpoint::Tcp(addr));
    let deadline = Instant::now() + TIMEOUT;
    let mut connected = false;
    while Instant::now() < deadline && !connected {
        server.poll_with(|_| {});
        client.poll_with(|event| connected |= matches!(event, HttpEvent::Connected { token: event_token } if event_token == token));
        thread::sleep(Duration::from_millis(1));
    }
    assert!(connected);
    assert!(client.remove(token));
    assert!(!client.request(token, "GET", "/", &[], b""));
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut reconnects = 0;
    while Instant::now() < deadline {
        server.poll_with(|_| {});
        client.poll_with(|event| {
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
        server.poll_with(|event| {
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
        server.poll_with(|event| {
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
    let mut http = HttpNetwork::default();
    http.listen(Endpoint::Tcp(addr)).unwrap();
    let outbound = http.connect(Endpoint::Tcp(addr));
    let stream = std::net::TcpStream::connect(addr).unwrap();
    stream.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + TIMEOUT;
    let mut accepted = None;
    while Instant::now() < deadline && accepted.is_none() {
        http.poll_with(|event| {
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
    let mut http = HttpNetwork::default();
    http.listen(Endpoint::Tcp(addr)).unwrap();
    let outbound = http.connect(Endpoint::Tcp(addr));
    let deadline = Instant::now() + TIMEOUT;
    let mut sent = false;
    let mut body = None;
    while Instant::now() < deadline && body.is_none() {
        let mut respond = None;
        let mut request = false;
        http.poll_with(|event| match event {
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
    let mut client = HttpNetwork::default();
    let token = client.connect(endpoint.clone());
    let mut sent = false;
    let mut host = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && host.is_none() {
        let mut replies = Vec::new();
        server.poll_with(|event| {
            if let HttpEvent::Request { token, request } = event {
                host = Some(request.header("Host").map(<[u8]>::to_vec));
                replies.push(token);
            }
        });
        for token in replies {
            assert!(server.respond(token, 200, &[], b""));
        }
        let mut connected = false;
        client.poll_with(|event| connected |= matches!(event, HttpEvent::Connected { .. }));
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
        server.poll_with(|event| {
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
