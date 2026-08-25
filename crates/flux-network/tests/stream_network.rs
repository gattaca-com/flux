use std::{
    cell::Cell,
    io::Write,
    net::{Ipv4Addr, SocketAddr},
    thread,
    time::{Duration, Instant},
};

use flux_network::stream::{
    ConnectionGroupConfig, Endpoint, Peer, PollEvent, SendBehavior, StreamEvent, StreamNetwork,
    TcpConnector,
};

const CLIENT_HELLO: &[u8] = b"client-hello";
const SERVER_HELLO: &[u8] = b"server-hello";
const REQUEST: &[u8] = b"request-payload";
const RESPONSE: &[u8] = b"response-payload";
const BATCH_MESSAGES: [&[u8]; 3] = [b"batch-one", b"batch-two", b"batch-three"];

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

/// Connects to `endpoint` the way an unrelated peer would, bypassing flux's
/// framing so a test can write partial or malformed frames.
fn raw_peer(endpoint: &Endpoint) -> Box<dyn Write> {
    match endpoint {
        Endpoint::Tcp(addr) => Box::new(std::net::TcpStream::connect(addr).unwrap()),
        Endpoint::Unix(path) => Box::new(std::os::unix::net::UnixStream::connect(path).unwrap()),
    }
}

fn contains(messages: &[Vec<u8>], expected: &[u8]) -> bool {
    messages.iter().any(|message| message == expected)
}

fn wait_for_accept(
    network: &mut StreamNetwork,
    group: flux_network::stream::ConnectionGroup,
) -> mio::Token {
    let mut accepted = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && accepted.is_none() {
        network.poll_with(|event| {
            if let StreamEvent::Accepted { group: event_group, token, .. } = event {
                assert_eq!(event_group, group);
                accepted = Some(token);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    accepted.expect("connection was not accepted")
}

fn encoded_frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(12 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&123_u64.to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

over_both_transports!(
    groups_route_events_and_messages,
    groups_route_events_and_messages_tcp,
    groups_route_events_and_messages_unix
);
fn groups_route_events_and_messages(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let server_group = network.add_group(ConnectionGroupConfig {
        name: "server",
        on_connect_msg: Some(SERVER_HELLO.to_vec()),
        ..ConnectionGroupConfig::default()
    });
    let client_group = network.add_group(ConnectionGroupConfig {
        name: "client",
        on_connect_msg: Some(CLIENT_HELLO.to_vec()),
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..ConnectionGroupConfig::default()
    });
    network.listen(server_group, endpoint.clone()).unwrap();
    let client_token = network.connect(client_group, endpoint.clone());

    let mut server_token = None;
    let mut connected = false;
    let mut server_messages = Vec::new();
    let mut client_messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        network.poll_with(|event| match event {
            StreamEvent::Accepted { group, token, .. } => {
                assert_eq!(group, server_group);
                server_token = Some(token);
            }
            StreamEvent::Connected { group, token, .. } => {
                assert_eq!(group, client_group);
                assert_eq!(token, client_token);
                connected = true;
            }
            StreamEvent::Message { group, payload, .. } if group == server_group => {
                server_messages.push(payload.to_vec());
            }
            StreamEvent::Message { group, payload, .. } if group == client_group => {
                client_messages.push(payload.to_vec());
            }
            StreamEvent::Disconnected { .. } => panic!("unexpected disconnect"),
            StreamEvent::Message { group, .. } => panic!("message for unknown group {group:?}"),
        });
        if connected &&
            server_token.is_some() &&
            contains(&server_messages, CLIENT_HELLO) &&
            contains(&client_messages, SERVER_HELLO)
        {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    assert!(connected);
    assert!(contains(&server_messages, CLIENT_HELLO));
    assert!(contains(&client_messages, SERVER_HELLO));

    assert!(network.send_with(client_token, |buf| buf.extend_from_slice(REQUEST)));
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !contains(&server_messages, REQUEST) {
        network.poll_with(|event| {
            if let StreamEvent::Message { group, payload, .. } = event {
                assert_eq!(group, server_group);
                server_messages.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(contains(&server_messages, REQUEST));

    assert!(network.send_with(server_token.unwrap(), |buf| buf.extend_from_slice(RESPONSE)));
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !contains(&client_messages, RESPONSE) {
        network.poll_with(|event| {
            if let StreamEvent::Message { group, payload, .. } = event {
                assert_eq!(group, client_group);
                client_messages.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(contains(&client_messages, RESPONSE));
}

#[test]
fn batch_send_preserves_framed_messages() {
    let addr = unused_addr();
    let mut network = StreamNetwork::default();
    let server_group = network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() });
    let client_group = network.add_group(ConnectionGroupConfig { name: "client", ..Default::default() });
    network.listen(server_group, Endpoint::Tcp(addr)).unwrap();
    let _ = network.connect(client_group, Endpoint::Tcp(addr));
    let server_token = wait_for_accept(&mut network, server_group);

    assert!(network.send_many_with(server_token, BATCH_MESSAGES, |buf, message| {
        buf.extend_from_slice(message);
    }));
    assert!(network.send_many_with(server_token, [RESPONSE], |buf, message| {
        buf.extend_from_slice(message);
    }));

    let mut messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && messages.len() < BATCH_MESSAGES.len() + 1 {
        network.poll_with(|event| {
            if let StreamEvent::Message { group, payload, .. } = event &&
                group == client_group
            {
                messages.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }

    let expected = BATCH_MESSAGES.into_iter().chain([RESPONSE]);
    assert!(messages.iter().map(Vec::as_slice).eq(expected));
}

#[test]
fn payload_buffer_is_relative_to_its_own_frame() {
    let addr = unused_addr();
    let mut network = StreamNetwork::default();
    let server_group = network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() });
    let client_group = network.add_group(ConnectionGroupConfig { name: "client", ..Default::default() });
    network.listen(server_group, Endpoint::Tcp(addr)).unwrap();
    let _ = network.connect(client_group, Endpoint::Tcp(addr));
    let server_token = wait_for_accept(&mut network, server_group);

    // The middle serialiser rewrites its payload from scratch. Every
    // operation must stay inside its own frame and leave the first intact.
    assert!(network.send_many_with(server_token, BATCH_MESSAGES, |buf, message| {
        buf.extend_from_slice(b"scratch");
        assert_eq!(buf.len(), b"scratch".len());
        buf.clear();
        assert!(buf.is_empty());
        buf.resize(message.len(), 0);
        buf.copy_from_slice(message);
    }));

    let mut messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && messages.len() < BATCH_MESSAGES.len() {
        network.poll_with(|event| {
            if let StreamEvent::Message { group, payload, .. } = event &&
                group == client_group
            {
                messages.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }

    assert!(messages.iter().map(Vec::as_slice).eq(BATCH_MESSAGES));
}

#[cfg(feature = "wincode")]
#[test]
fn payload_buffer_is_a_wincode_writer() {
    let addr = unused_addr();
    let mut network = StreamNetwork::default();
    let server_group = network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() });
    let client_group = network.add_group(ConnectionGroupConfig { name: "client", ..Default::default() });
    network.listen(server_group, Endpoint::Tcp(addr)).unwrap();
    let _ = network.connect(client_group, Endpoint::Tcp(addr));
    let server_token = wait_for_accept(&mut network, server_group);

    let values = [7u64, 11, 13];
    assert!(network.send_many_with(server_token, values, |buf, value| {
        wincode::serialize_into(buf, &value).unwrap();
    }));

    let mut decoded = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && decoded.len() < values.len() {
        network.poll_with(|event| {
            if let StreamEvent::Message { group, payload, .. } = event &&
                group == client_group
            {
                decoded.push(wincode::deserialize::<u64>(payload).unwrap());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(decoded, values);
}

#[test]
fn batch_skips_oversized_payloads_and_keeps_the_rest() {
    let addr = unused_addr();
    let mut network = StreamNetwork::default();
    let server_group = network.add_group(ConnectionGroupConfig {
        name: "server",
        max_frame_size: 32,
        ..Default::default()
    });
    let client_group = network.add_group(ConnectionGroupConfig { name: "client", ..Default::default() });
    network.listen(server_group, Endpoint::Tcp(addr)).unwrap();
    let _ = network.connect(client_group, Endpoint::Tcp(addr));
    let server_token = wait_for_accept(&mut network, server_group);

    let oversized = [7u8; 64];
    let items: [&[u8]; 3] = [b"fits-one", &oversized, b"fits-two"];
    assert!(network.send_many_with(server_token, items, |buf, item| buf.extend_from_slice(item)));

    let mut messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && messages.len() < 2 {
        network.poll_with(|event| match event {
            StreamEvent::Message { group, payload, .. } if group == client_group => {
                messages.push(payload.to_vec());
            }
            StreamEvent::Disconnected { .. } => panic!("oversized payload disconnected the peer"),
            _ => {}
        });
        thread::sleep(Duration::from_millis(1));
    }

    let expected: [&[u8]; 2] = [b"fits-one", b"fits-two"];
    assert!(messages.iter().map(Vec::as_slice).eq(expected));
}

#[test]
fn broadcast_many_serializes_each_payload_once() {
    let addr = unused_addr();
    let mut network = StreamNetwork::default();
    let server_group = network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() });
    let client_group = network.add_group(ConnectionGroupConfig {
        name: "clients",
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    });
    network.listen(server_group, Endpoint::Tcp(addr)).unwrap();
    let _first_client = network.connect(client_group, Endpoint::Tcp(addr));
    let _second_client = network.connect(client_group, Endpoint::Tcp(addr));

    let mut accepted = 0;
    let mut connected = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (accepted != 2 || connected != 2) {
        network.poll_with(|event| match event {
            StreamEvent::Accepted { group, .. } => {
                assert_eq!(group, server_group);
                accepted += 1;
            }
            StreamEvent::Connected { group, .. } => {
                assert_eq!(group, client_group);
                connected += 1;
            }
            StreamEvent::Disconnected { .. } => panic!("unexpected disconnect"),
            StreamEvent::Message { .. } => {}
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(accepted, 2);
    assert_eq!(connected, 2);

    let serializations = Cell::new(0);
    let recipients = network.broadcast_many_with(server_group, BATCH_MESSAGES, |buf, message| {
        serializations.set(serializations.get() + 1);
        buf.extend_from_slice(message);
    });
    assert_eq!(recipients, 2);
    assert_eq!(serializations.get(), BATCH_MESSAGES.len());

    let mut per_client = std::collections::HashMap::<mio::Token, Vec<Vec<u8>>>::new();
    let mut received = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && received != 2 * BATCH_MESSAGES.len() {
        network.poll_with(|event| {
            if let StreamEvent::Message { group, token, payload, .. } = event &&
                group == client_group
            {
                per_client.entry(token).or_default().push(payload.to_vec());
                received += 1;
            }
        });
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(per_client.len(), 2);
    for messages in per_client.values() {
        assert!(messages.iter().map(Vec::as_slice).eq(BATCH_MESSAGES));
    }
}

over_both_transports!(
    partial_header_and_payload_are_not_delivered_early,
    partial_header_and_payload_are_not_delivered_early_tcp,
    partial_header_and_payload_are_not_delivered_early_unix
);
fn partial_header_and_payload_are_not_delivered_early(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let group = network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() });
    network.listen(group, endpoint.clone()).unwrap();

    let mut peer = raw_peer(endpoint);
    let token = wait_for_accept(&mut network, group);
    let frame = encoded_frame(REQUEST);
    let mut messages = Vec::new();

    for chunk in [&frame[..3], &frame[3..12], &frame[12..frame.len() - 1]] {
        peer.write_all(chunk).unwrap();
        for _ in 0..5 {
            network.poll_with(|event| match event {
                StreamEvent::Message { payload, .. } => messages.push(payload.to_vec()),
                StreamEvent::Disconnected { .. } => panic!("peer disconnected unexpectedly"),
                _ => {}
            });
            thread::sleep(Duration::from_millis(1));
        }
        assert!(messages.is_empty());
    }

    peer.write_all(&frame[frame.len() - 1..]).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && messages.is_empty() {
        network.poll_with(|event| {
            if let StreamEvent::Message { token: event_token, payload, .. } = event {
                assert_eq!(event_token, token);
                messages.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(messages, [REQUEST]);
}

over_both_transports!(
    oversized_frame_disconnects_the_peer,
    oversized_frame_disconnects_the_peer_tcp,
    oversized_frame_disconnects_the_peer_unix
);
fn oversized_frame_disconnects_the_peer(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let group = network.add_group(ConnectionGroupConfig {
        name: "server",
        max_frame_size: 32,
        ..Default::default()
    });
    network.listen(group, endpoint.clone()).unwrap();

    let mut peer = raw_peer(endpoint);
    let token = wait_for_accept(&mut network, group);
    let mut header = Vec::with_capacity(12);
    header.extend_from_slice(&33_u32.to_le_bytes());
    header.extend_from_slice(&123_u64.to_le_bytes());
    peer.write_all(&header).unwrap();

    let mut disconnected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !disconnected {
        network.poll_with(|event| match event {
            StreamEvent::Disconnected { token: event_token, .. } => {
                assert_eq!(event_token, token);
                disconnected = true;
            }
            StreamEvent::Message { .. } => panic!("oversized frame was delivered"),
            _ => {}
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(disconnected);
}

/// TCP only: the backlog only grows once a write goes short, which this test
/// arranges through `SO_SNDBUF` on a TCP socket.
#[test]
fn hard_backlog_limit_disconnects_the_peer() {
    let addr = unused_addr();
    let mut network = StreamNetwork::default();
    let server_group =
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() });
    let client_group = network.add_group(ConnectionGroupConfig {
        name: "client",
        socket_buf_size: Some(1024),
        backlog_warn_bytes: None,
        max_backlog_bytes: Some(1),
        max_frame_size: 2 * 1024 * 1024,
        ..Default::default()
    });
    network.listen(server_group, Endpoint::Tcp(addr)).unwrap();
    let client_token = network.connect(client_group, Endpoint::Tcp(addr));

    let mut connected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !connected {
        network.poll_with(|event| {
            if let StreamEvent::Connected { group, token, .. } = event {
                assert_eq!(group, client_group);
                assert_eq!(token, client_token);
                connected = true;
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(connected);

    assert!(!network.send_with(client_token, |buffer| buffer.resize(1024 * 1024, 7)));

    let mut disconnected = false;
    network.poll_with(|event| {
        if let StreamEvent::Disconnected { group, token, .. } = event &&
            group == client_group
        {
            assert_eq!(token, client_token);
            disconnected = true;
        }
    });
    assert!(disconnected);
}

over_both_transports!(
    broadcast_serializes_once_for_multiple_connections,
    broadcast_serializes_once_for_multiple_connections_tcp,
    broadcast_serializes_once_for_multiple_connections_unix
);
fn broadcast_serializes_once_for_multiple_connections(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let server_group =
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() });
    let client_group = network.add_group(ConnectionGroupConfig {
        name: "clients",
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    });
    network.listen(server_group, endpoint.clone()).unwrap();
    let _first_client = network.connect(client_group, endpoint.clone());
    let _second_client = network.connect(client_group, endpoint.clone());

    let mut accepted = 0;
    let mut connected = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (accepted != 2 || connected != 2) {
        network.poll_with(|event| match event {
            StreamEvent::Accepted { group, .. } => {
                assert_eq!(group, server_group);
                accepted += 1;
            }
            StreamEvent::Connected { group, .. } => {
                assert_eq!(group, client_group);
                connected += 1;
            }
            StreamEvent::Disconnected { .. } => panic!("unexpected disconnect"),
            StreamEvent::Message { .. } => {}
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(accepted, 2);
    assert_eq!(connected, 2);

    let serializations = Cell::new(0);
    let recipients = network.broadcast_with(server_group, |buf| {
        serializations.set(serializations.get() + 1);
        buf.extend_from_slice(RESPONSE);
    });
    assert_eq!(recipients, 2);
    assert_eq!(serializations.get(), 1);

    let mut received = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && received != 2 {
        network.poll_with(|event| {
            if let StreamEvent::Message { group, payload, .. } = event {
                assert_eq!(group, client_group);
                assert_eq!(payload, RESPONSE);
                received += 1;
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(received, 2);
}

over_both_transports!(
    disconnected_messages_are_dropped_and_token_survives_reconnect,
    disconnected_messages_are_dropped_and_token_survives_reconnect_tcp,
    disconnected_messages_are_dropped_and_token_survives_reconnect_unix
);
fn disconnected_messages_are_dropped_and_token_survives_reconnect(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let server_group =
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() });
    let client_group = network.add_group(ConnectionGroupConfig {
        name: "client",
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    });
    network.listen(server_group, endpoint.clone()).unwrap();
    let client_token = network.connect(client_group, endpoint.clone());

    let mut server_token = None;
    let mut connected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (server_token.is_none() || !connected) {
        network.poll_with(|event| match event {
            StreamEvent::Accepted { token, .. } => server_token = Some(token),
            StreamEvent::Connected { token, .. } => {
                assert_eq!(token, client_token);
                connected = true;
            }
            _ => {}
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(network.disconnect(server_token.unwrap()));

    let mut client_disconnected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !client_disconnected {
        network.poll_with(|event| {
            if let StreamEvent::Disconnected { group, token, .. } = event &&
                group == client_group
            {
                assert_eq!(token, client_token);
                client_disconnected = true;
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(client_disconnected);

    let serialization_called = Cell::new(false);
    assert!(!network.send_with(client_token, |buf| {
        serialization_called.set(true);
        buf.extend_from_slice(REQUEST);
    }));
    assert!(!serialization_called.get());

    let mut reconnected = false;
    let mut new_server_token = None;
    let mut server_messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (!reconnected || new_server_token.is_none()) {
        network.poll_with(|event| match event {
            StreamEvent::Accepted { group, token, .. } if group == server_group => {
                new_server_token = Some(token);
            }
            StreamEvent::Connected { group, token, .. } if group == client_group => {
                assert_eq!(token, client_token);
                reconnected = true;
            }
            StreamEvent::Message { group, payload, .. } if group == server_group => {
                server_messages.push(payload.to_vec());
            }
            _ => {}
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(reconnected);
    assert!(server_messages.is_empty());

    assert!(network.send_with(client_token, |buf| buf.extend_from_slice(RESPONSE)));
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !contains(&server_messages, RESPONSE) {
        network.poll_with(|event| {
            if let StreamEvent::Message { group, payload, .. } = event &&
                group == server_group
            {
                server_messages.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(contains(&server_messages, RESPONSE));
}

/// TCP only: `TcpConnector` speaks TCP alone.
#[test]
fn stream_network_is_wire_compatible_with_tcp_connector() {
    let addr = unused_addr();
    let mut network = StreamNetwork::default();
    let server_group = network.add_group(ConnectionGroupConfig {
        name: "network-server",
        on_connect_msg: Some(SERVER_HELLO.to_vec()),
        ..Default::default()
    });
    network.listen(server_group, Endpoint::Tcp(addr)).unwrap();

    let mut connector = TcpConnector::default();
    let connector_token = connector.connect(addr).expect("connector failed to connect");
    connector.write_or_enqueue_with(SendBehavior::Single(connector_token), |buf| {
        buf.extend_from_slice(REQUEST);
    });

    let mut network_token = None;
    let mut network_messages = Vec::new();
    let mut connector_messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline &&
        (!contains(&network_messages, REQUEST) || !contains(&connector_messages, SERVER_HELLO))
    {
        network.poll_with(|event| match event {
            StreamEvent::Accepted { token, .. } => network_token = Some(token),
            StreamEvent::Message { payload, .. } => network_messages.push(payload.to_vec()),
            StreamEvent::Disconnected { .. } => panic!("network disconnected unexpectedly"),
            StreamEvent::Connected { .. } => unreachable!(),
        });
        connector.poll_with(|event| {
            if let PollEvent::Message { payload, .. } = event {
                connector_messages.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(contains(&network_messages, REQUEST));
    assert!(contains(&connector_messages, SERVER_HELLO));

    assert!(network.send_with(network_token.unwrap(), |buf| buf.extend_from_slice(RESPONSE)));
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !contains(&connector_messages, RESPONSE) {
        network.poll_with(|_| {});
        connector.poll_with(|event| {
            if let PollEvent::Message { payload, .. } = event {
                connector_messages.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(contains(&connector_messages, RESPONSE));
}

/// TCP only: `TcpConnector` speaks TCP alone.
#[test]
fn stream_network_client_is_wire_compatible_with_tcp_connector_server() {
    let addr = unused_addr();
    let mut connector = TcpConnector::default().with_on_connect_msg(SERVER_HELLO.to_vec());
    connector.listen_at(addr).expect("connector failed to listen");

    let mut network = StreamNetwork::default();
    let client_group = network.add_group(ConnectionGroupConfig {
        name: "network-client",
        on_connect_msg: Some(CLIENT_HELLO.to_vec()),
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    });
    let client_token = network.connect(client_group, Endpoint::Tcp(addr));

    let mut connector_token = None;
    let mut connector_messages = Vec::new();
    let mut network_messages = Vec::new();
    let mut connected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline &&
        (!connected ||
            !contains(&connector_messages, CLIENT_HELLO) ||
            !contains(&network_messages, SERVER_HELLO))
    {
        connector.poll_with(|event| match event {
            PollEvent::Accept { stream, .. } => connector_token = Some(stream),
            PollEvent::Message { payload, .. } => connector_messages.push(payload.to_vec()),
            PollEvent::Disconnect { .. } => panic!("connector disconnected unexpectedly"),
            PollEvent::Reconnect { .. } => unreachable!(),
        });
        network.poll_with(|event| match event {
            StreamEvent::Connected { token, .. } => {
                assert_eq!(token, client_token);
                connected = true;
            }
            StreamEvent::Message { payload, .. } => network_messages.push(payload.to_vec()),
            StreamEvent::Disconnected { .. } => panic!("network disconnected unexpectedly"),
            StreamEvent::Accepted { .. } => unreachable!(),
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(connected);
    assert!(contains(&connector_messages, CLIENT_HELLO));
    assert!(contains(&network_messages, SERVER_HELLO));

    assert!(network.send_with(client_token, |buf| buf.extend_from_slice(REQUEST)));
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !contains(&connector_messages, REQUEST) {
        connector.poll_with(|event| {
            if let PollEvent::Message { payload, .. } = event {
                connector_messages.push(payload.to_vec());
            }
        });
        network.poll_with(|_| {});
        thread::sleep(Duration::from_millis(1));
    }
    assert!(contains(&connector_messages, REQUEST));

    connector.write_or_enqueue_with(SendBehavior::Single(connector_token.unwrap()), |buf| {
        buf.extend_from_slice(RESPONSE);
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !contains(&network_messages, RESPONSE) {
        connector.poll_with(|_| {});
        network.poll_with(|event| {
            if let StreamEvent::Message { payload, .. } = event {
                network_messages.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(contains(&network_messages, RESPONSE));
}

over_both_transports!(
    accepted_peer_identifies_the_transport,
    accepted_peer_identifies_the_transport_tcp,
    accepted_peer_identifies_the_transport_unix
);
fn accepted_peer_identifies_the_transport(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let server_group =
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() });
    let client_group = network.add_group(ConnectionGroupConfig {
        name: "client",
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    });
    network.listen(server_group, endpoint.clone()).unwrap();
    let client_token = network.connect(client_group, endpoint.clone());

    let mut accepted = None;
    let mut connected = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (accepted.is_none() || connected.is_none()) {
        network.poll_with(|event| match event {
            StreamEvent::Accepted { group, peer, .. } => {
                assert_eq!(group, server_group);
                accepted = Some(peer);
            }
            StreamEvent::Connected { token, peer, .. } => {
                assert_eq!(token, client_token);
                connected = Some(peer);
            }
            _ => {}
        });
        thread::sleep(Duration::from_millis(1));
    }

    match endpoint {
        Endpoint::Tcp(addr) => {
            // The accepted peer is the client's ephemeral port, not the
            // listening address; only the outbound side knows its target.
            assert!(matches!(accepted, Some(Peer::Tcp(_))), "{accepted:?}");
            assert_eq!(connected, Some(Peer::Tcp(*addr)));
        }
        Endpoint::Unix(_) => {
            assert_eq!(accepted, Some(Peer::Unix));
            assert_eq!(connected, Some(Peer::Unix));
        }
    }
}

/// A Unix-domain endpoint whose socket file does not exist yet retries at the
/// group's interval, exactly as a refused TCP endpoint does.
#[test]
fn unix_endpoint_connects_once_its_listener_appears() {
    let dir = tempfile::tempdir().unwrap();
    let endpoint = Endpoint::Unix(dir.path().join("late"));

    let mut client = StreamNetwork::default();
    let client_group = client.add_group(ConnectionGroupConfig {
        name: "client",
        reconnect_interval: flux_timing::Duration::from_millis(5),
        ..Default::default()
    });
    let client_token = client.connect(client_group, endpoint.clone());

    let deadline = Instant::now() + Duration::from_millis(100);
    while Instant::now() < deadline {
        client.poll_with(|event| {
            assert!(
                !matches!(event, StreamEvent::Connected { .. }),
                "connected before the listener existed"
            );
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(!dir.path().join("late").exists());

    let mut server = StreamNetwork::default();
    let server_group =
        server.add_group(ConnectionGroupConfig { name: "server", ..Default::default() });
    server.listen(server_group, endpoint).unwrap();

    let mut accepted = false;
    let mut connected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !(accepted && connected) {
        server.poll_with(|event| {
            if matches!(event, StreamEvent::Accepted { .. }) {
                accepted = true;
            }
        });
        client.poll_with(|event| {
            if let StreamEvent::Connected { token, .. } = event {
                assert_eq!(token, client_token);
                connected = true;
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(accepted);
    assert!(connected);
}
