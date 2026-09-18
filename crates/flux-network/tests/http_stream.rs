//! Streaming response bodies: pieces must reach the caller while the
//! response is still open, not only once it finishes.

use std::{
    io::Write,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    http::{HttpEvent, HttpNetwork, StreamEnd},
    tcp::TcpNetwork,
};

const TIMEOUT: Duration = Duration::from_secs(5);

/// What the caller observed, in order.
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    Head(u16),
    Body(Vec<u8>),
    End(StreamEnd),
}

fn listener() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

#[test]
fn chunked_body_arrives_while_the_response_is_still_open() {
    let (listener, addr) = listener();
    // The server hands out one chunk at a time, on request from the test.
    let (write_tx, write_rx) = mpsc::channel::<Option<Vec<u8>>>();
    thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut request = [0u8; 1024];
        std::io::Read::read(&mut socket, &mut request).unwrap();
        socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n").unwrap();
        socket.flush().unwrap();
        while let Ok(Some(bytes)) = write_rx.recv() {
            socket.write_all(&bytes).unwrap();
            socket.flush().unwrap();
        }
        socket.write_all(b"0\r\n\r\n").unwrap();
        socket.flush().unwrap();
        thread::sleep(Duration::from_millis(200));
    });

    let mut net = TcpNetwork::default();
    let mut http = HttpNetwork::default();
    let token = http.connect(&mut net, addr);
    let mut seen = Vec::new();
    let mut requested = false;
    let deadline = Instant::now() + TIMEOUT;

    let poll = |net: &mut TcpNetwork, http: &mut HttpNetwork, seen: &mut Vec<Seen>| {
        net.poll_with(|event| {
            http.on_event(&event);
        });
        http.drive(net, |event| match event {
            HttpEvent::ResponseHead { status, .. } => seen.push(Seen::Head(status)),
            HttpEvent::Body { chunk, .. } => seen.push(Seen::Body(chunk.to_vec())),
            HttpEvent::StreamEnd { reason, .. } => seen.push(Seen::End(reason)),
            HttpEvent::Response { .. } => panic!("a streaming request was buffered"),
            _ => {}
        });
        thread::sleep(Duration::from_millis(1));
    };

    // The head, then one chunk delivered in two TCP writes.
    while Instant::now() < deadline && seen.is_empty() {
        if !requested {
            requested = http.request_stream(&mut net, token, "GET", "/stream", &[]);
            if requested {
                write_tx.send(Some(b"5\r\nhel".to_vec())).unwrap();
                thread::sleep(Duration::from_millis(20));
                write_tx.send(Some(b"lo\r\n".to_vec())).unwrap();
            }
        }
        poll(&mut net, &mut http, &mut seen);
    }
    while Instant::now() < deadline && seen.len() < 2 {
        poll(&mut net, &mut http, &mut seen);
    }
    assert_eq!(seen, [Seen::Head(200), Seen::Body(b"hello".to_vec())]);

    // A second chunk lands while the same response is still open, which a
    // buffering implementation could not do.
    write_tx.send(Some(b"6\r\n world\r\n".to_vec())).unwrap();
    while Instant::now() < deadline && seen.len() < 3 {
        poll(&mut net, &mut http, &mut seen);
    }
    assert_eq!(seen[2], Seen::Body(b" world".to_vec()));

    // Only the terminal chunk ends it.
    write_tx.send(None).unwrap();
    while Instant::now() < deadline && seen.len() < 4 {
        poll(&mut net, &mut http, &mut seen);
    }
    assert_eq!(seen[3], Seen::End(StreamEnd::Complete));

    let body: Vec<u8> = seen
        .iter()
        .filter_map(|event| match event {
            Seen::Body(chunk) => Some(chunk.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(body, b"hello world");
}

#[test]
fn a_lost_connection_ends_the_stream() {
    let (listener, addr) = listener();
    thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut request = [0u8; 1024];
        std::io::Read::read(&mut socket, &mut request).unwrap();
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nhalf\r\n")
            .unwrap();
        socket.flush().unwrap();
        thread::sleep(Duration::from_millis(50));
        // Drops without the terminal chunk.
    });

    let mut net = TcpNetwork::default();
    let mut http = HttpNetwork::default();
    let token = http.connect(&mut net, addr);
    let mut seen = Vec::new();
    let mut requested = false;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && !seen.iter().any(|event| matches!(event, Seen::End(_))) {
        net.poll_with(|event| {
            http.on_event(&event);
        });
        http.drive(&mut net, |event| match event {
            HttpEvent::ResponseHead { status, .. } => seen.push(Seen::Head(status)),
            HttpEvent::Body { chunk, .. } => seen.push(Seen::Body(chunk.to_vec())),
            HttpEvent::StreamEnd { reason, .. } => seen.push(Seen::End(reason)),
            _ => {}
        });
        if !requested {
            requested = http.request_stream(&mut net, token, "GET", "/stream", &[]);
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(seen, [
        Seen::Head(200),
        Seen::Body(b"half".to_vec()),
        Seen::End(StreamEnd::Disconnected),
    ]);
}

/// A stream owns its connection for as long as it runs, so the pool refuses
/// one rather than losing a connection out of its rotation.
#[test]
fn a_pooled_connection_refuses_to_stream() {
    let (listener, addr) = listener();
    let (keep_tx, keep_rx) = mpsc::channel();
    thread::spawn(move || {
        for socket in listener.incoming() {
            // Held open so the pool stays connected for the check below.
            keep_tx.send(socket.unwrap()).unwrap();
        }
    });

    let mut net = TcpNetwork::default();
    let mut http = HttpNetwork::default();
    let _pool = http.pool(&mut net, addr, 1);
    let mut pooled = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && pooled.is_none() {
        net.poll_with(|event| {
            http.on_event(&event);
        });
        http.drive(&mut net, |event| {
            if let HttpEvent::Connected { token } = event {
                pooled = Some(token);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    let pooled = pooled.expect("the pool never connected");
    assert!(!http.request_stream(&mut net, pooled, "GET", "/stream", &[]));
    drop(keep_rx);
}
