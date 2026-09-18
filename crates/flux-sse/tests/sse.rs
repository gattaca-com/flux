//! Minimal SSE example: one event through a fake stream.

use std::{
    io::{Read, Write},
    net::{Ipv4Addr, TcpListener},
    thread,
    time::{Duration, Instant},
};

use flux_network::tcp::TcpNetwork;
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

    let mut net = TcpNetwork::default();
    let mut sse = Sse::new(addr, "/eth/v1/events").with_query(&[("topics", "head")]);
    sse.connect(&mut net);
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && seen.len() < 2 {
        net.poll_with(|event| {
            sse.on_event(&event);
        });
        sse.drive(&mut net, |event| match event {
            SseEvent::Open => seen.push(("open".to_owned(), Vec::new())),
            SseEvent::Message { kind, data } => seen.push((kind.to_owned(), data.to_vec())),
            _ => {}
        });
        thread::sleep(Duration::from_millis(1));
    }
    let want = [("open".to_owned(), Vec::new()), ("head".to_owned(), b"{\"slot\":1}".to_vec())];
    assert_eq!(seen, want);
}
