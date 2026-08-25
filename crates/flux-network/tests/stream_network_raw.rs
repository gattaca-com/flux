use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr},
    thread,
    time::{Duration, Instant},
};

use flux_network::stream::{ConnectionGroupConfig, Endpoint, Framing, StreamEvent, StreamNetwork};

const TIMEOUT: Duration = Duration::from_secs(5);

fn unused_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn raw_group(name: &'static str) -> ConnectionGroupConfig {
    ConnectionGroupConfig { name, framing: Framing::Raw, ..ConnectionGroupConfig::default() }
}

#[test]
fn raw_roundtrip() {
    let request = b"raw request bytes";
    let addr = unused_addr();
    let mut network = StreamNetwork::default();
    let group = network.add_group(raw_group("raw-server"));
    network.listen(group, Endpoint::Tcp(addr)).unwrap();

    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    let mut received = Vec::new();
    let mut echoed = false;
    let deadline = Instant::now() + TIMEOUT;
    client.write_all(request).unwrap();

    while Instant::now() < deadline && received.len() < request.len() {
        let mut echo = None;
        network.poll_with(|event| {
            if let StreamEvent::Message { group: event_group, token, payload, .. } = event {
                assert_eq!(event_group, group);
                echo = Some((token, payload.to_vec()));
            }
        });
        if let Some((token, payload)) = echo {
            assert!(network.send_with(token, |buf| buf.extend_from_slice(&payload)));
            echoed = true;
        }
        let mut buffer = [0; 128];
        match client.read(&mut buffer) {
            Ok(read) => received.extend_from_slice(&buffer[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => panic!("client read failed: {err}"),
        }
        thread::sleep(Duration::from_millis(1));
    }

    assert!(echoed);
    assert_eq!(received, request);
}

#[test]
fn http_get_smoke() {
    let request = b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
    let addr = unused_addr();
    let mut network = StreamNetwork::default();
    let group = network.add_group(raw_group("http"));
    network.listen(group, Endpoint::Tcp(addr)).unwrap();

    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(request).unwrap();
    let mut request_bytes = Vec::new();
    let mut response_token = None;
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;

    while Instant::now() < deadline && response_token.is_none() {
        let mut reply_to = None;
        network.poll_with(|event| {
            if let StreamEvent::Message { group: event_group, token, payload, .. } = event {
                assert_eq!(event_group, group);
                request_bytes.extend_from_slice(payload);
                if request_bytes.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    reply_to = Some(token);
                }
            }
        });
        if let Some(token) = reply_to {
            assert!(network.send_with(token, |buf| buf.extend_from_slice(response)));
            response_token = Some(token);
        }
        thread::sleep(Duration::from_millis(1));
    }
    let token = response_token.expect("HTTP request was not received");
    assert!(network.disconnect(token));

    let deadline = Instant::now() + TIMEOUT;
    loop {
        let mut buffer = [0; 256];
        match client.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => received.extend_from_slice(&buffer[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "HTTP response did not reach EOF");
                network.poll_with(|_| {});
                thread::sleep(Duration::from_millis(1));
            }
            Err(err) => panic!("client read failed: {err}"),
        }
    }

    assert!(received.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(received.ends_with(b"ok"));
}

#[test]
fn raw_outbound_connect() {
    let hello = b"raw hello";
    let message = b"raw message";
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let mut network = StreamNetwork::default();
    let group = network.add_group(ConnectionGroupConfig {
        name: "raw-client",
        framing: Framing::Raw,
        on_connect_msg: Some(hello.to_vec()),
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..ConnectionGroupConfig::default()
    });
    let token = network.connect(group, Endpoint::Tcp(addr));
    let mut peer = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && peer.is_none() {
        network.poll_with(|_| {});
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(true).unwrap();
                peer = Some(stream);
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => panic!("accept failed: {err}"),
        }
        thread::sleep(Duration::from_millis(1));
    }
    let mut peer = peer.expect("outbound connection was not accepted");

    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && received.len() < hello.len() {
        network.poll_with(|_| {});
        let mut buffer = [0; 128];
        match peer.read(&mut buffer) {
            Ok(read) => received.extend_from_slice(&buffer[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => panic!("peer read failed: {err}"),
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(received, hello);

    assert!(network.send_with(token, |buf| buf.extend_from_slice(message)));
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && received.len() < hello.len() + message.len() {
        network.poll_with(|_| {});
        let mut buffer = [0; 128];
        match peer.read(&mut buffer) {
            Ok(read) => received.extend_from_slice(&buffer[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => panic!("peer read failed: {err}"),
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(received, [hello.as_slice(), message.as_slice()].concat());
}

#[test]
fn framed_and_raw_coexist() {
    let framed_addr = unused_addr();
    let raw_addr = unused_addr();
    let mut server = StreamNetwork::default();
    let framed_server =
        server.add_group(ConnectionGroupConfig { name: "framed-server", ..Default::default() });
    let raw_server = server.add_group(raw_group("raw-server"));
    server.listen(framed_server, Endpoint::Tcp(framed_addr)).unwrap();
    server.listen(raw_server, Endpoint::Tcp(raw_addr)).unwrap();

    let mut framed_client = StreamNetwork::default();
    let framed_client_group = framed_client.add_group(ConnectionGroupConfig {
        name: "framed-client",
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    });
    let framed_client_token =
        framed_client.connect(framed_client_group, Endpoint::Tcp(framed_addr));
    let mut raw_client = std::net::TcpStream::connect(raw_addr).unwrap();
    raw_client.set_nonblocking(true).unwrap();
    raw_client.write_all(b"raw").unwrap();

    let mut framed_server_token = None;
    let mut framed_connected = false;
    let mut framed_sent = false;
    let mut framed_reply = Vec::new();
    let mut raw_reply = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline &&
        (framed_server_token.is_none() ||
            !framed_connected ||
            framed_reply != b"framed" ||
            raw_reply != b"raw")
    {
        let mut echoes = Vec::new();
        server.poll_with(|event| match event {
            StreamEvent::Accepted { group, token, .. } if group == framed_server => {
                framed_server_token = Some(token);
            }
            StreamEvent::Message { group, token, payload, .. }
                if group == framed_server || group == raw_server =>
            {
                echoes.push((token, payload.to_vec()));
            }
            _ => {}
        });
        for (token, payload) in echoes {
            assert!(server.send_with(token, |buf| buf.extend_from_slice(&payload)));
        }
        framed_client.poll_with(|event| match event {
            StreamEvent::Connected { token, .. } => {
                assert_eq!(token, framed_client_token);
                framed_connected = true;
            }
            StreamEvent::Message { payload, .. } => framed_reply.extend_from_slice(payload),
            _ => {}
        });
        if framed_connected && framed_server_token.is_some() && !framed_sent {
            assert!(
                framed_client
                    .send_with(framed_client_token, |buf| buf.extend_from_slice(b"framed"))
            );
            framed_sent = true;
        }
        let mut buffer = [0; 32];
        match raw_client.read(&mut buffer) {
            Ok(read) => raw_reply.extend_from_slice(&buffer[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => panic!("raw client read failed: {err}"),
        }
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(framed_reply, b"framed");
    assert_eq!(raw_reply, b"raw");
}

#[test]
fn raw_disconnect_when_drained_flushes_queue() {
    let addr = unused_addr();
    let payload = vec![0xA5; 8 * 1024 * 1024];
    let mut network = StreamNetwork::default();
    let group = network.add_group(ConnectionGroupConfig {
        name: "raw-drain",
        framing: Framing::Raw,
        socket_buf_size: Some(1024),
        max_frame_size: payload.len(),
        ..ConnectionGroupConfig::default()
    });
    network.listen(group, Endpoint::Tcp(addr)).unwrap();

    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    let mut token = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && token.is_none() {
        network.poll_with(|event| {
            if let StreamEvent::Accepted { group: event_group, token: accepted, .. } = event {
                assert_eq!(event_group, group);
                token = Some(accepted);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    let token = token.expect("raw connection was not accepted");

    assert!(network.send_with(token, |buf| buf.extend_from_slice(&payload)));
    assert!(network.disconnect_when_drained(token));
    assert!(!network.send_with(token, |buf| buf.extend_from_slice(b"late response")));

    let mut received = Vec::with_capacity(payload.len());
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let mut buffer = [0; 16 * 1024];
        match client.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => received.extend_from_slice(&buffer[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "raw response did not reach EOF");
                network.poll_with(|_| {});
                thread::sleep(Duration::from_millis(1));
            }
            Err(err) => panic!("client read failed: {err}"),
        }
    }

    assert_eq!(received, payload);
}
