//! Minimal SSE example: one event through a fake stream.

use std::{
    io::{Read, Write},
    net::{Ipv4Addr, TcpListener},
    thread,
    time::{Duration, Instant},
};

use flux_network::Network;
use flux_sse::{Sse, SseEvent};

#[test]
fn delivers_one_event() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut request = [0u8; 1024];
        assert!(socket.read(&mut request).unwrap() > 0);
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n").unwrap();
        socket.write_all(b"Transfer-Encoding: chunked\r\n\r\n").unwrap();
        let event = b"event: head\ndata: {\"slot\":1}\n\n";
        socket.write_all(format!("{:x}\r\n", event.len()).as_bytes()).unwrap();
        socket.write_all(event).unwrap();
        socket.write_all(b"\r\n").unwrap();
        socket.flush().unwrap();
        thread::sleep(Duration::from_secs(10));
    });

    let sse = Sse::new(addr, "/eth/v1/events").with_query(&[("topics", "head")]);
    let seen = collect(sse, |seen| seen.len() == 2);
    let want = [("open".to_owned(), Vec::new()), ("head".to_owned(), b"{\"slot\":1}".to_vec())];
    assert_eq!(seen, want);
}

#[test]
fn oversized_event_ends_stream() {
    let limit = 64;
    let mut at_limit = b"data: ".to_vec();
    at_limit.extend(std::iter::repeat_n(b'a', limit));
    at_limit.extend_from_slice(b"\n\n");
    let event = ("message".to_owned(), vec![b'a'; limit]);
    let unterminated_line = [b"data: ".as_slice(), &[b'b'; 58], &[b'b'; 64]].concat();
    let endless_data = b"data: 0123456789\n".repeat(9);
    for overflow in [unterminated_line, endless_data] {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let body = [at_limit.clone(), overflow].concat();
        let peer = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            let mut request = [0u8; 1024];
            assert!(socket.read(&mut request).unwrap() > 0);
            // One write, so the client cannot close the connection before the
            // peer has sent everything.
            let mut response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
            for chunk in body.chunks(limit) {
                response.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
                response.extend_from_slice(chunk);
                response.extend_from_slice(b"\r\n");
            }
            socket.write_all(&response).unwrap();
            while matches!(socket.read(&mut request), Ok(n) if n > 0) {}
        });

        let sse = Sse::new(addr, "/").with_http(|http| http.with_max_body_bytes(limit));
        let seen = collect(sse, |seen| seen.last().is_some_and(|(kind, _)| kind == "gap"));
        let want = [("open".to_owned(), Vec::new()), event.clone(), ("gap".to_owned(), Vec::new())];
        assert_eq!(seen, want);
        peer.join().unwrap();
    }
}

/// Drives `sse` until `done` holds for the events seen so far, or a deadline.
fn collect(mut sse: Sse, done: impl Fn(&[(String, Vec<u8>)]) -> bool) -> Vec<(String, Vec<u8>)> {
    let mut net = Network::default();
    sse.connect(&mut net);
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && !done(&seen) {
        net.poll_with(|event| {
            sse.on_event(&event);
        });
        sse.drive(&mut net, |event| match event {
            SseEvent::Open => seen.push(("open".to_owned(), Vec::new())),
            SseEvent::Message { kind, data } => seen.push((kind.to_owned(), data.to_vec())),
            SseEvent::Gap => seen.push(("gap".to_owned(), Vec::new())),
            SseEvent::Status(_) => {}
        });
        thread::sleep(Duration::from_millis(1));
    }
    seen
}
