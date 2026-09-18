//! End-to-end SSE against a fake event stream.

use std::{
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use flux_network::tcp::TcpNetwork;
use flux_sse::{Sse, SseEvent};

const TIMEOUT: Duration = Duration::from_secs(5);

/// What the caller observed, in order.
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    Open,
    Message(String, Vec<u8>),
    Gap,
    Status(u16),
}

/// One chunked-encoding chunk.
fn chunk(bytes: &[u8]) -> Vec<u8> {
    let mut out = format!("{:x}\r\n", bytes.len()).into_bytes();
    out.extend_from_slice(bytes);
    out.extend_from_slice(b"\r\n");
    out
}

fn drain_request(socket: &mut std::net::TcpStream) {
    let mut request = [0u8; 2048];
    let read = socket.read(&mut request).unwrap();
    let text = String::from_utf8_lossy(&request[..read]).into_owned();
    assert!(text.starts_with("GET "), "{text}");
    assert!(text.to_lowercase().contains("accept: text/event-stream"), "{text}");
}

fn head() -> &'static [u8] {
    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n"
}

fn collect(
    net: &mut TcpNetwork,
    sse: &mut Sse,
    seen: &mut Vec<Seen>,
    wanted: usize,
    deadline: Instant,
) {
    collect_with_status(net, sse, seen, wanted, deadline, false);
}

/// Drives until `wanted` events are seen, optionally recording a refusal
/// instead of failing on it.
fn collect_with_status(
    net: &mut TcpNetwork,
    sse: &mut Sse,
    seen: &mut Vec<Seen>,
    wanted: usize,
    deadline: Instant,
    keep_status: bool,
) {
    while Instant::now() < deadline && seen.len() < wanted {
        net.poll_with(|event| {
            sse.on_event(&event);
        });
        sse.drive(net, |event| match event {
            SseEvent::Open => seen.push(Seen::Open),
            SseEvent::Message { kind, data } => {
                seen.push(Seen::Message(kind.to_owned(), data.to_vec()));
            }
            SseEvent::Gap => seen.push(Seen::Gap),
            SseEvent::Status(status) => {
                assert!(keep_status, "status {status}");
                seen.push(Seen::Status(status));
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn events_survive_chunk_boundaries_comments_and_multiline_data() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        drain_request(&mut socket);
        socket.write_all(head()).unwrap();
        // A keepalive comment, then an event split mid-field across chunks.
        socket.write_all(&chunk(b":ping\n\nevent: head\ndata: {\"slo")).unwrap();
        socket.flush().unwrap();
        thread::sleep(Duration::from_millis(20));
        socket.write_all(&chunk(b"t\":1}\n\n")).unwrap();
        // Multi-line data joins with a newline; CRLF terminators too.
        socket.write_all(&chunk(b"data: one\r\ndata: two\r\n\r\n")).unwrap();
        // No `event:` field means the default kind.
        socket.write_all(&chunk(b"data: bare\n\n")).unwrap();
        // A trailing empty data line keeps its newline.
        socket.write_all(&chunk(b"data: kept\ndata:\n\n")).unwrap();
        socket.flush().unwrap();
        let _ = done_rx.recv();
    });

    let mut net = TcpNetwork::default();
    let mut sse =
        Sse::new(addr, "/eth/v1/events").with_query(&[("topics", "head,payload_attributes")]);
    sse.connect(&mut net);
    let mut seen = Vec::new();
    collect(&mut net, &mut sse, &mut seen, 5, Instant::now() + TIMEOUT);
    assert_eq!(seen, [
        Seen::Open,
        Seen::Message("head".to_owned(), br#"{"slot":1}"#.to_vec()),
        Seen::Message("message".to_owned(), b"one\ntwo".to_vec()),
        Seen::Message("message".to_owned(), b"bare".to_vec()),
        Seen::Message("message".to_owned(), b"kept\n".to_vec()),
    ]);
    drop(done_tx);
}

#[test]
fn a_dropped_stream_reports_a_gap_and_resubscribes() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    thread::spawn(move || {
        // First subscription: one event, then the server hangs up.
        let (mut socket, _) = listener.accept().unwrap();
        drain_request(&mut socket);
        socket.write_all(head()).unwrap();
        socket.write_all(&chunk(b"data: first\n\n")).unwrap();
        socket.flush().unwrap();
        thread::sleep(Duration::from_millis(30));
        drop(socket);
        // The client must come back and ask again.
        let (mut socket, _) = listener.accept().unwrap();
        drain_request(&mut socket);
        socket.write_all(head()).unwrap();
        socket.write_all(&chunk(b"data: second\n\n")).unwrap();
        socket.flush().unwrap();
        let _ = done_rx.recv();
    });

    let mut net = TcpNetwork::default();
    let mut sse = Sse::new(addr, "/eth/v1/events");
    sse.connect(&mut net);
    let mut seen = Vec::new();
    collect(&mut net, &mut sse, &mut seen, 5, Instant::now() + TIMEOUT);
    assert_eq!(seen, [
        Seen::Open,
        Seen::Message("message".to_owned(), b"first".to_vec()),
        // The gap is the point: events published while the stream was down
        // are gone, and the caller is told rather than left guessing.
        Seen::Gap,
        Seen::Open,
        Seen::Message("message".to_owned(), b"second".to_vec()),
    ]);
    drop(done_tx);
}

/// A refused subscription must be retried. The server keeps the connection
/// open after the 404, so nothing reconnects on its own: the client has to
/// drop the endpoint itself for the redial to happen.
#[test]
fn a_refused_subscription_is_retried() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        drain_request(&mut socket);
        socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n").unwrap();
        socket.flush().unwrap();
        // Held open, so only the client can break the deadlock.
        let (mut retry, _) = listener.accept().unwrap();
        drain_request(&mut retry);
        retry.write_all(head()).unwrap();
        retry.write_all(&chunk(b"data: after\n\n")).unwrap();
        retry.flush().unwrap();
        let _ = done_rx.recv();
        drop(socket);
    });

    let mut net = TcpNetwork::default();
    let mut sse = Sse::new(addr, "/eth/v1/events")
        .with_http(|http| http.with_reconnect_interval(flux_timing::Duration::from_millis(50)));
    sse.connect(&mut net);
    let mut seen = Vec::new();
    collect_with_status(&mut net, &mut sse, &mut seen, 3, Instant::now() + TIMEOUT, true);
    assert_eq!(seen, [
        Seen::Status(404),
        Seen::Open,
        Seen::Message("message".to_owned(), b"after".to_vec()),
    ]);
    drop(done_tx);
}
