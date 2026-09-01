mod common;

use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use common::{RawEvent, RawService, Record};
use flux_network::{
    Token,
    stream::{ConnectionGroupConfig, Endpoint, Framing, StreamNetwork},
};

const TIMEOUT: Duration = Duration::from_secs(5);

/// How long one iteration of a test loop is allowed to wait in the poll.
fn poll_slice() -> flux_timing::Duration {
    flux_timing::Duration::from_millis(1)
}

/// A loopback endpoint whose port the kernel picks when the listener binds,
/// so no address is handed out before something holds it.
fn ephemeral() -> Endpoint {
    Endpoint::Tcp((Ipv4Addr::LOCALHOST, 0).into())
}

/// The TCP address a listener bound, port included.
fn bound_addr(bound: Endpoint) -> SocketAddr {
    match bound {
        Endpoint::Tcp(addr) => addr,
        Endpoint::Unix(path) => panic!("a TCP listener bound {}", path.display()),
    }
}

fn raw_group(name: &'static str) -> ConnectionGroupConfig {
    ConnectionGroupConfig { name, framing: Framing::Raw, ..ConnectionGroupConfig::default() }
}

/// Sends every payload a service read back down the connection it arrived on,
/// reporting how many that was.
fn echo(service: &mut RawService) -> usize {
    let mut echoed = 0;
    let leftovers = service.spin(usize::MAX, |event| match event {
        RawEvent::Message { payload, reply, .. } => {
            assert!(reply.send(payload), "the echo reached its connection");
            echoed += 1;
        }
    });
    assert!(!leftovers, "an unbounded drain leaves nothing behind");
    echoed
}

/// Moves every payload a service read into `out`, in arrival order.
fn drain(service: &mut RawService, out: &mut Vec<u8>) {
    let leftovers = service.spin(usize::MAX, |event| match event {
        RawEvent::Message { payload, .. } => out.extend_from_slice(payload),
    });
    assert!(!leftovers, "an unbounded drain leaves nothing behind");
}

/// Whether a service has reported `token` connected.
fn connected(service: &RawService, token: Token) -> bool {
    service
        .records()
        .iter()
        .any(|record| matches!(record, Record::Connected { token: at, .. } if *at == token))
}

#[test]
fn raw_roundtrip() {
    let request = b"raw request bytes";
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(network.add_group(raw_group("raw-server")));
    let addr = bound_addr(server.listen(ephemeral()).unwrap());

    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    let mut received = Vec::new();
    let mut echoed = false;
    let deadline = Instant::now() + TIMEOUT;
    client.write_all(request).unwrap();

    while Instant::now() < deadline && received.len() < request.len() {
        network.drive(Some(poll_slice()), &mut [&mut server]);
        echoed |= echo(&mut server) > 0;
        let mut buffer = [0; 128];
        match client.read(&mut buffer) {
            Ok(read) => received.extend_from_slice(&buffer[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => panic!("client read failed: {err}"),
        }
    }

    assert!(echoed);
    assert_eq!(received, request);
}

#[test]
fn raw_batch_concatenates_payloads() {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(network.add_group(raw_group("raw-server")));
    let addr = bound_addr(server.listen(ephemeral()).unwrap());

    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && server.accepted().is_none() {
        network.drive(Some(poll_slice()), &mut [&mut server]);
    }
    let token = server.accepted().expect("connection was not accepted");

    // Raw framing has no boundaries to keep: a batch is one run of bytes.
    let parts: [&[u8]; 3] = [b"raw-", b"batch-", b"bytes"];
    let mut group = server.into_group();
    assert!(group.send_many_with(token, parts, |buf, part| buf.extend_from_slice(part)));
    let mut server = RawService::new(group);

    let expected = b"raw-batch-bytes";
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && received.len() < expected.len() {
        network.drive(Some(poll_slice()), &mut [&mut server]);
        let mut buffer = [0; 128];
        match client.read(&mut buffer) {
            Ok(read) => received.extend_from_slice(&buffer[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => panic!("client read failed: {err}"),
        }
    }

    assert_eq!(received, expected);
}

#[test]
fn http_get_smoke() {
    let request = b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(network.add_group(raw_group("http")));
    let addr = bound_addr(server.listen(ephemeral()).unwrap());

    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    client.write_all(request).unwrap();
    let mut request_bytes = Vec::new();
    let mut response_token = None;
    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;

    while Instant::now() < deadline && response_token.is_none() {
        network.drive(Some(poll_slice()), &mut [&mut server]);
        let mut reply_to = None;
        let leftovers = server.spin(usize::MAX, |event| match event {
            RawEvent::Message { token, payload, .. } => {
                request_bytes.extend_from_slice(payload);
                if request_bytes.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    reply_to = Some(token);
                }
            }
        });
        assert!(!leftovers, "an unbounded drain leaves nothing behind");
        if let Some(token) = reply_to {
            assert!(server.send(token, response));
            response_token = Some(token);
        }
    }
    let token = response_token.expect("HTTP request was not received");
    assert!(server.disconnect(token));

    let deadline = Instant::now() + TIMEOUT;
    loop {
        let mut buffer = [0; 256];
        match client.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => received.extend_from_slice(&buffer[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "HTTP response did not reach EOF");
                network.drive(Some(poll_slice()), &mut [&mut server]);
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
    let mut client = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "raw-client",
        framing: Framing::Raw,
        on_connect_msg: Some(hello.to_vec()),
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..ConnectionGroupConfig::default()
    }));
    let token = client.connect(Endpoint::Tcp(addr));
    let mut peer = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && peer.is_none() {
        network.drive(Some(poll_slice()), &mut [&mut client]);
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(true).unwrap();
                peer = Some(stream);
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => panic!("accept failed: {err}"),
        }
    }
    let mut peer = peer.expect("outbound connection was not accepted");

    let mut received = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && received.len() < hello.len() {
        network.drive(Some(poll_slice()), &mut [&mut client]);
        let mut buffer = [0; 128];
        match peer.read(&mut buffer) {
            Ok(read) => received.extend_from_slice(&buffer[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => panic!("peer read failed: {err}"),
        }
    }
    assert_eq!(received, hello);

    assert!(client.send(token, message));
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && received.len() < hello.len() + message.len() {
        network.drive(Some(poll_slice()), &mut [&mut client]);
        let mut buffer = [0; 128];
        match peer.read(&mut buffer) {
            Ok(read) => received.extend_from_slice(&buffer[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => panic!("peer read failed: {err}"),
        }
    }
    assert_eq!(received, [hello.as_slice(), message.as_slice()].concat());
}

#[test]
fn framed_and_raw_coexist() {
    let mut server_net = StreamNetwork::default();
    let mut framed_server = RawService::new(
        server_net.add_group(ConnectionGroupConfig { name: "framed-server", ..Default::default() }),
    );
    let mut raw_server = RawService::new(server_net.add_group(raw_group("raw-server")));
    let framed_addr = bound_addr(framed_server.listen(ephemeral()).unwrap());
    let raw_addr = bound_addr(raw_server.listen(ephemeral()).unwrap());

    let mut client_net = StreamNetwork::default();
    let mut framed_client = RawService::new(client_net.add_group(ConnectionGroupConfig {
        name: "framed-client",
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    }));
    let framed_client_token = framed_client.connect(Endpoint::Tcp(framed_addr));
    let mut raw_client = std::net::TcpStream::connect(raw_addr).unwrap();
    raw_client.set_nonblocking(true).unwrap();
    raw_client.write_all(b"raw").unwrap();

    let mut framed_sent = false;
    let mut framed_reply = Vec::new();
    let mut raw_reply = Vec::new();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline &&
        (framed_server.accepted().is_none() ||
            !connected(&framed_client, framed_client_token) ||
            framed_reply != b"framed" ||
            raw_reply != b"raw")
    {
        // Each group echoes what its own connections sent, whichever framing
        // that group speaks.
        server_net.drive(Some(poll_slice()), &mut [&mut framed_server, &mut raw_server]);
        echo(&mut framed_server);
        echo(&mut raw_server);

        client_net.drive(Some(poll_slice()), &mut [&mut framed_client]);
        drain(&mut framed_client, &mut framed_reply);
        if connected(&framed_client, framed_client_token) &&
            framed_server.accepted().is_some() &&
            !framed_sent
        {
            assert!(framed_client.send(framed_client_token, b"framed"));
            framed_sent = true;
        }
        let mut buffer = [0; 32];
        match raw_client.read(&mut buffer) {
            Ok(read) => raw_reply.extend_from_slice(&buffer[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => panic!("raw client read failed: {err}"),
        }
    }

    assert_eq!(framed_reply, b"framed");
    assert_eq!(raw_reply, b"raw");
}

#[test]
fn raw_disconnect_when_drained_flushes_queue() {
    let payload = vec![0xA5; 8 * 1024 * 1024];
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "raw-drain",
        framing: Framing::Raw,
        socket_buf_size: Some(1024),
        max_frame_size: payload.len(),
        ..ConnectionGroupConfig::default()
    }));
    let addr = bound_addr(server.listen(ephemeral()).unwrap());

    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && server.accepted().is_none() {
        network.drive(Some(poll_slice()), &mut [&mut server]);
    }
    let token = server.accepted().expect("raw connection was not accepted");

    assert!(server.send(token, &payload));
    assert!(server.disconnect_when_drained(token));
    assert!(!server.send(token, b"late response"));

    let mut received = Vec::with_capacity(payload.len());
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let mut buffer = [0; 16 * 1024];
        match client.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => received.extend_from_slice(&buffer[..read]),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "raw response did not reach EOF");
                network.drive(Some(poll_slice()), &mut [&mut server]);
            }
            Err(err) => panic!("client read failed: {err}"),
        }
    }

    assert_eq!(received, payload);
}
