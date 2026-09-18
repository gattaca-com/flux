//! Minimal streaming example: one chunked response, delivered incrementally.

use std::{
    io::{Read, Write},
    net::{Ipv4Addr, TcpListener},
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    http::{HttpEvent, HttpNetwork, StreamEnd},
    tcp::TcpNetwork,
};

#[test]
fn streams_one_chunked_response() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut request = [0u8; 1024];
        assert!(socket.read(&mut request).unwrap() > 0);
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
            )
            .unwrap();
        socket.flush().unwrap();
        thread::sleep(Duration::from_secs(10));
    });

    let mut net = TcpNetwork::default();
    let mut http = HttpNetwork::default();
    let token = http.connect(&mut net, addr);
    let mut requested = false;
    let mut body = Vec::new();
    let mut ended = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !ended {
        net.poll_with(|event| {
            http.on_event(&event);
        });
        http.drive(&mut net, |event| match event {
            HttpEvent::Body { chunk, .. } => body.extend_from_slice(chunk),
            HttpEvent::StreamEnd { reason: StreamEnd::Complete, .. } => ended = true,
            _ => {}
        });
        if !requested {
            requested = http.request_stream(&mut net, token, "GET", "/stream", &[]);
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(body, b"hello");
}
