//! Minimal streaming example: one chunked response, delivered incrementally.

use std::{
    io::{Read, Write},
    net::{Ipv4Addr, TcpListener},
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Network,
    http::{HttpEvent, HttpNetwork, StreamEnd},
};

fn stream(response: &'static [u8]) -> (Vec<u8>, Option<StreamEnd>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut request = [0u8; 1024];
        assert!(socket.read(&mut request).unwrap() > 0);
        socket.write_all(response).unwrap();
        socket.flush().unwrap();
        thread::sleep(Duration::from_secs(10));
    });

    let mut net = Network::default();
    let mut http = HttpNetwork::default().with_max_body_bytes(usize::MAX);
    let token = http.connect(&mut net, addr);
    let mut requested = false;
    let mut body = Vec::new();
    let mut ended = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && ended.is_none() {
        net.poll_with(|event| {
            http.on_event(&event);
        });
        http.drive(&mut net, |event| match event {
            HttpEvent::Body { chunk, .. } => body.extend_from_slice(chunk),
            HttpEvent::StreamEnd { reason, .. } => ended = Some(reason),
            _ => {}
        });
        if !requested {
            requested = http.request_stream(&mut net, token, "GET", "/stream", &[]);
        }
        thread::sleep(Duration::from_millis(1));
    }
    (body, ended)
}

#[test]
fn streams_one_chunked_response() {
    let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
    assert_eq!(stream(response), (b"hello".to_vec(), Some(StreamEnd::Complete)));
}

#[test]
fn malformed_chunk_ends_stream() {
    for response in [
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhelloXX0\r\n\r\n".as_slice(),
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nffffffffffffffff\r\nhello\r\n"
            .as_slice(),
    ] {
        assert_eq!(stream(response), (Vec::new(), Some(StreamEnd::Disconnected)));
    }
}
