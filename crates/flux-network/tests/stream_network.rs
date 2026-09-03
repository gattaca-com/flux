mod common;

use std::{
    cell::Cell,
    io::Write,
    net::{Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use common::{RawEvent, RawService, Record};
use flux_network::{
    Token,
    stream::{
        ConnectionGroupConfig, Endpoint, Peer, PollEvent, SendBehavior, StreamNetwork, TcpConnector,
    },
};

const CLIENT_HELLO: &[u8] = b"client-hello";
const SERVER_HELLO: &[u8] = b"server-hello";
const REQUEST: &[u8] = b"request-payload";
const RESPONSE: &[u8] = b"response-payload";
const BATCH_MESSAGES: [&[u8]; 3] = [b"batch-one", b"batch-two", b"batch-three"];

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

/// Runs one test body over both transports: a loopback address whose port the
/// listener's own bind decides, and a Unix-domain socket path under a
/// temporary directory that lives for the duration of the run.
macro_rules! over_both_transports {
    ($body:ident, $tcp:ident, $unix:ident) => {
        #[test]
        fn $tcp() {
            $body(&ephemeral());
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

/// Moves every message a service read into `out`, oldest first.
fn drain(service: &mut RawService, out: &mut Vec<Vec<u8>>) {
    let leftovers = service.spin(usize::MAX, |event| match event {
        RawEvent::Message { payload, .. } => out.push(payload.to_vec()),
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

/// Whether a service has reported `token` closed.
fn disconnected(service: &RawService, token: Token) -> bool {
    service
        .records()
        .iter()
        .any(|record| matches!(record, Record::Disconnected { token: at, .. } if *at == token))
}

/// Whether a service has reported any connection closed.
fn has_disconnect(service: &RawService) -> bool {
    service.records().iter().any(|record| matches!(record, Record::Disconnected { .. }))
}

/// The peer of the first connection a service accepted.
fn accepted_peer(service: &RawService) -> Option<Peer> {
    service.records().iter().find_map(|record| match record {
        Record::Accepted { peer, .. } => Some(*peer),
        _ => None,
    })
}

/// The peer of the first outbound connection a service established.
fn connected_peer(service: &RawService) -> Option<Peer> {
    service.records().iter().find_map(|record| match record {
        Record::Connected { peer, .. } => Some(*peer),
        _ => None,
    })
}

/// How many connections a service has accepted.
fn accepts(service: &RawService) -> usize {
    service.records().iter().filter(|record| matches!(record, Record::Accepted { .. })).count()
}

/// How many outbound connections a service has established.
fn connects(service: &RawService) -> usize {
    service.records().iter().filter(|record| matches!(record, Record::Connected { .. })).count()
}

fn wait_for_accept(network: &mut StreamNetwork, server: &mut RawService) -> Token {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && server.accepted().is_none() {
        network.drive(Some(poll_slice()), &mut [&mut *server]);
    }
    server.accepted().expect("connection was not accepted")
}

/// Drives a server and the client service connecting to it until the server
/// accepts a connection, returning the token it accepted under.
fn wait_for_accept_between(
    network: &mut StreamNetwork,
    server: &mut RawService,
    client: &mut RawService,
) -> Token {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && server.accepted().is_none() {
        network.drive(Some(poll_slice()), &mut [&mut *server, &mut *client]);
    }
    server.accepted().expect("connection was not accepted")
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
    let mut server = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "server",
        on_connect_msg: Some(SERVER_HELLO.to_vec()),
        ..ConnectionGroupConfig::default()
    }));
    let mut client = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "client",
        on_connect_msg: Some(CLIENT_HELLO.to_vec()),
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..ConnectionGroupConfig::default()
    }));
    let bound = server.listen(endpoint.clone()).unwrap();
    // A TCP listener on port 0 binds a port of the kernel's choosing.
    let endpoint = &bound;
    let client_token = client.connect(endpoint.clone());

    let mut server_messages = Vec::new();
    let mut client_messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
        // Each service holds the group it owns, so which of them a message
        // reached is what routing means here.
        drain(&mut server, &mut server_messages);
        drain(&mut client, &mut client_messages);
        assert!(!has_disconnect(&server), "unexpected disconnect");
        assert!(!has_disconnect(&client), "unexpected disconnect");
        if connected(&client, client_token) &&
            server.accepted().is_some() &&
            contains(&server_messages, CLIENT_HELLO) &&
            contains(&client_messages, SERVER_HELLO)
        {
            break;
        }
    }

    assert!(connected(&client, client_token));
    let server_token = server.accepted().expect("connection was not accepted");
    assert!(contains(&server_messages, CLIENT_HELLO));
    assert!(contains(&client_messages, SERVER_HELLO));

    assert!(client.send(client_token, REQUEST));
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !contains(&server_messages, REQUEST) {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
        drain(&mut server, &mut server_messages);
    }
    assert!(contains(&server_messages, REQUEST));

    assert!(server.send(server_token, RESPONSE));
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !contains(&client_messages, RESPONSE) {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
        drain(&mut client, &mut client_messages);
    }
    assert!(contains(&client_messages, RESPONSE));
}

over_both_transports!(
    batch_send_preserves_framed_messages,
    batch_send_preserves_framed_messages_tcp,
    batch_send_preserves_framed_messages_unix
);
fn batch_send_preserves_framed_messages(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    let mut client = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "client", ..Default::default() }),
    );
    let bound = server.listen(endpoint.clone()).unwrap();
    let _ = client.connect(bound);
    let server_token = wait_for_accept_between(&mut network, &mut server, &mut client);

    // A batch is the group's own operation, so the group comes out of its
    // service for the calls and goes back into one straight after.
    let mut group = server.into_group();
    assert!(group.send_many_with(server_token, BATCH_MESSAGES, |buf, message| {
        buf.extend_from_slice(message);
    }));
    assert!(group.send_many_with(server_token, [RESPONSE], |buf, message| {
        buf.extend_from_slice(message);
    }));
    let mut server = RawService::new(group);

    let mut messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && messages.len() < BATCH_MESSAGES.len() + 1 {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
        drain(&mut client, &mut messages);
    }

    let expected = BATCH_MESSAGES.into_iter().chain([RESPONSE]);
    assert!(messages.iter().map(Vec::as_slice).eq(expected), "{messages:?}");
}

over_both_transports!(
    payload_buffer_is_relative_to_its_own_frame,
    payload_buffer_is_relative_to_its_own_frame_tcp,
    payload_buffer_is_relative_to_its_own_frame_unix
);
fn payload_buffer_is_relative_to_its_own_frame(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    let mut client = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "client", ..Default::default() }),
    );
    let bound = server.listen(endpoint.clone()).unwrap();
    let _ = client.connect(bound);
    let server_token = wait_for_accept_between(&mut network, &mut server, &mut client);

    // Every serialiser rewrites its payload from scratch. Each operation must
    // stay inside its own frame and leave the frames staged before it intact.
    let mut group = server.into_group();
    assert!(group.send_many_with(server_token, BATCH_MESSAGES, |buf, message| {
        buf.extend_from_slice(b"scratch");
        assert_eq!(buf.len(), b"scratch".len());
        buf.clear();
        assert!(buf.is_empty());
        buf.resize(message.len(), 0);
        buf.copy_from_slice(message);
    }));
    let mut server = RawService::new(group);

    let mut messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && messages.len() < BATCH_MESSAGES.len() {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
        drain(&mut client, &mut messages);
    }

    assert!(messages.iter().map(Vec::as_slice).eq(BATCH_MESSAGES), "{messages:?}");
}

#[cfg(feature = "wincode")]
over_both_transports!(
    payload_buffer_is_a_wincode_writer,
    payload_buffer_is_a_wincode_writer_tcp,
    payload_buffer_is_a_wincode_writer_unix
);
#[cfg(feature = "wincode")]
fn payload_buffer_is_a_wincode_writer(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    let mut client = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "client", ..Default::default() }),
    );
    let bound = server.listen(endpoint.clone()).unwrap();
    let _ = client.connect(bound);
    let server_token = wait_for_accept_between(&mut network, &mut server, &mut client);

    let values = [7u64, 11, 13];
    let mut group = server.into_group();
    assert!(group.send_many_with(server_token, values, |buf, value| {
        wincode::serialize_into(buf, &value).unwrap();
    }));
    let mut server = RawService::new(group);

    let mut messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && messages.len() < values.len() {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
        drain(&mut client, &mut messages);
    }

    let decoded: Vec<u64> =
        messages.iter().map(|payload| wincode::deserialize::<u64>(payload).unwrap()).collect();
    assert_eq!(decoded, values);
}

over_both_transports!(
    batch_skips_oversized_payloads_and_keeps_the_rest,
    batch_skips_oversized_payloads_and_keeps_the_rest_tcp,
    batch_skips_oversized_payloads_and_keeps_the_rest_unix
);
fn batch_skips_oversized_payloads_and_keeps_the_rest(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "server",
        max_frame_size: 32,
        ..Default::default()
    }));
    let mut client = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "client", ..Default::default() }),
    );
    let bound = server.listen(endpoint.clone()).unwrap();
    let _ = client.connect(bound);
    let server_token = wait_for_accept_between(&mut network, &mut server, &mut client);

    let oversized = [7u8; 64];
    let items: [&[u8]; 3] = [b"fits-one", &oversized, b"fits-two"];
    let mut group = server.into_group();
    assert!(group.send_many_with(server_token, items, |buf, item| buf.extend_from_slice(item)));
    let mut server = RawService::new(group);

    let mut messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && messages.len() < 2 {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
        drain(&mut client, &mut messages);
        assert!(
            !has_disconnect(&server) && !has_disconnect(&client),
            "oversized payload disconnected the peer"
        );
    }

    let expected: [&[u8]; 2] = [b"fits-one", b"fits-two"];
    assert!(messages.iter().map(Vec::as_slice).eq(expected), "{messages:?}");
}

over_both_transports!(
    broadcast_many_serializes_each_payload_once,
    broadcast_many_serializes_each_payload_once_tcp,
    broadcast_many_serializes_each_payload_once_unix
);
fn broadcast_many_serializes_each_payload_once(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    let mut clients = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "clients",
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    }));
    let bound = server.listen(endpoint.clone()).unwrap();
    // A TCP listener on port 0 binds a port of the kernel's choosing.
    let endpoint = &bound;
    let _first_client = clients.connect(endpoint.clone());
    let _second_client = clients.connect(endpoint.clone());

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (accepts(&server) != 2 || connects(&clients) != 2) {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut clients]);
        assert!(!has_disconnect(&server), "unexpected disconnect");
        assert!(!has_disconnect(&clients), "unexpected disconnect");
    }
    assert_eq!(accepts(&server), 2);
    assert_eq!(connects(&clients), 2);

    // Each payload of the batch is serialised once for the whole group, so
    // the count is the batch length rather than its multiple by recipient.
    let mut group = server.into_group();
    let mut serializations = 0;
    let recipients = group.broadcast_many_with(BATCH_MESSAGES, |buf, message| {
        serializations += 1;
        buf.extend_from_slice(message);
    });
    let mut server = RawService::new(group);
    assert_eq!(recipients, 2);
    assert_eq!(serializations, BATCH_MESSAGES.len());

    let mut per_client = std::collections::HashMap::<Token, Vec<Vec<u8>>>::new();
    let mut received = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && received != 2 * BATCH_MESSAGES.len() {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut clients]);
        let leftovers = clients.spin(usize::MAX, |event| match event {
            RawEvent::Message { token, payload, .. } => {
                per_client.entry(token).or_default().push(payload.to_vec());
                received += 1;
            }
        });
        assert!(!leftovers, "an unbounded drain leaves nothing behind");
    }

    assert_eq!(per_client.len(), 2);
    for messages in per_client.values() {
        assert!(messages.iter().map(Vec::as_slice).eq(BATCH_MESSAGES), "{messages:?}");
    }
}

over_both_transports!(
    batch_skips_empty_payloads_and_sends_nothing_for_an_empty_batch,
    batch_skips_empty_payloads_and_sends_nothing_for_an_empty_batch_tcp,
    batch_skips_empty_payloads_and_sends_nothing_for_an_empty_batch_unix
);
fn batch_skips_empty_payloads_and_sends_nothing_for_an_empty_batch(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    let mut client = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "client", ..Default::default() }),
    );
    let bound = server.listen(endpoint.clone()).unwrap();
    let _ = client.connect(bound);
    let server_token = wait_for_accept_between(&mut network, &mut server, &mut client);

    // Every item reaches the serialiser; one that leaves its payload empty
    // is dropped from the batch, and a batch with nothing left is no send.
    let mut group = server.into_group();
    let mut serialised = 0;
    let items: [&[u8]; 3] = [b"first", b"", b"second"];
    assert!(group.send_many_with(server_token, items, |buf, item| {
        serialised += 1;
        buf.extend_from_slice(item);
    }));
    assert_eq!(serialised, items.len());
    let empties: [&[u8]; 2] = [b"", b""];
    assert!(!group.send_many_with(server_token, empties, |buf, item| buf.extend_from_slice(item)));
    assert!(group.send_with(server_token, |buf| buf.extend_from_slice(RESPONSE)));
    let mut server = RawService::new(group);

    let mut messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && messages.len() < 3 {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
        drain(&mut client, &mut messages);
    }

    let expected: [&[u8]; 3] = [b"first", b"second", RESPONSE];
    assert!(messages.iter().map(Vec::as_slice).eq(expected), "{messages:?}");
}

over_both_transports!(
    a_batch_is_not_capped_as_a_whole_by_the_backlog_limit,
    a_batch_is_not_capped_as_a_whole_by_the_backlog_limit_tcp,
    a_batch_is_not_capped_as_a_whole_by_the_backlog_limit_unix
);
fn a_batch_is_not_capped_as_a_whole_by_the_backlog_limit(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    let mut client = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "client",
        backlog_warn_bytes: None,
        max_backlog_bytes: Some(1),
        ..Default::default()
    }));
    let bound = server.listen(endpoint.clone()).unwrap();
    let client_token = client.connect(bound);

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !connected(&client, client_token) {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
    }
    assert!(connected(&client, client_token));

    // The cap measures what a partial write leaves behind, never the batch
    // itself: a batch far above the cap goes out whole when the socket takes
    // it in one write.
    let mut group = client.into_group();
    assert!(group.send_many_with(client_token, BATCH_MESSAGES, |buf, message| {
        buf.extend_from_slice(message);
    }));
    let mut client = RawService::new(group);

    let mut messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && messages.len() < BATCH_MESSAGES.len() {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
        drain(&mut server, &mut messages);
    }

    assert!(messages.iter().map(Vec::as_slice).eq(BATCH_MESSAGES), "{messages:?}");
    assert!(!has_disconnect(&server) && !has_disconnect(&client), "unexpected disconnect");
}

/// The batch twin of `hard_backlog_limit_disconnects_the_peer`: what the socket
/// does not take of a batch is queued as one remainder, and the cap judges
/// that remainder.
#[test]
fn a_batch_remainder_beyond_the_backlog_limit_disconnects_the_peer() {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    let mut client = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "client",
        socket_buf_size: Some(1024),
        backlog_warn_bytes: None,
        max_backlog_bytes: Some(1),
        max_frame_size: 2 * 1024 * 1024,
        ..Default::default()
    }));
    let addr = bound_addr(server.listen(ephemeral()).unwrap());
    let client_token = client.connect(Endpoint::Tcp(addr));

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !connected(&client, client_token) {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
    }
    assert!(connected(&client, client_token));

    let mut group = client.into_group();
    let halves = [vec![7u8; 512 * 1024], vec![8u8; 512 * 1024]];
    assert!(!group.send_many_with(client_token, &halves, |buf, half| buf.extend_from_slice(half)));
    let mut client = RawService::new(group);

    // The refused send closed the connection there and then, and the
    // disconnect it queued is delivered by the very next iteration.
    network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
    assert!(disconnected(&client, client_token));
}

over_both_transports!(
    partial_header_and_payload_are_not_delivered_early,
    partial_header_and_payload_are_not_delivered_early_tcp,
    partial_header_and_payload_are_not_delivered_early_unix
);
fn partial_header_and_payload_are_not_delivered_early(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    let bound = server.listen(endpoint.clone()).unwrap();
    // A TCP listener on port 0 binds a port of the kernel's choosing.
    let endpoint = &bound;

    let mut peer = raw_peer(endpoint);
    let token = wait_for_accept(&mut network, &mut server);
    let frame = encoded_frame(REQUEST);
    let mut messages = Vec::new();

    for chunk in [&frame[..3], &frame[3..12], &frame[12..frame.len() - 1]] {
        peer.write_all(chunk).unwrap();
        for _ in 0..5 {
            network.drive(Some(poll_slice()), &mut [&mut server]);
            drain(&mut server, &mut messages);
            assert!(!has_disconnect(&server), "peer disconnected unexpectedly");
        }
        assert!(messages.is_empty());
    }

    peer.write_all(&frame[frame.len() - 1..]).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && messages.is_empty() {
        network.drive(Some(poll_slice()), &mut [&mut server]);
        let leftovers = server.spin(usize::MAX, |event| match event {
            RawEvent::Message { token: at, payload, .. } => {
                assert_eq!(at, token);
                messages.push(payload.to_vec());
            }
        });
        assert!(!leftovers, "an unbounded drain leaves nothing behind");
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
    let mut server = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "server",
        max_frame_size: 32,
        ..Default::default()
    }));
    let bound = server.listen(endpoint.clone()).unwrap();
    // A TCP listener on port 0 binds a port of the kernel's choosing.
    let endpoint = &bound;

    let mut peer = raw_peer(endpoint);
    let token = wait_for_accept(&mut network, &mut server);
    let mut header = Vec::with_capacity(12);
    header.extend_from_slice(&33_u32.to_le_bytes());
    header.extend_from_slice(&123_u64.to_le_bytes());
    peer.write_all(&header).unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !disconnected(&server, token) {
        network.drive(Some(poll_slice()), &mut [&mut server]);
        // Nothing drains the service, so a payload awaiting a drain is a
        // frame that was delivered.
        assert_eq!(server.pending(), 0, "oversized frame was delivered");
    }
    assert!(disconnected(&server, token));
}

/// TCP only: the backlog only grows once a write goes short, which this test
/// arranges through `SO_SNDBUF` on a TCP socket.
#[test]
fn hard_backlog_limit_disconnects_the_peer() {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    let mut client = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "client",
        socket_buf_size: Some(1024),
        backlog_warn_bytes: None,
        max_backlog_bytes: Some(1),
        max_frame_size: 2 * 1024 * 1024,
        ..Default::default()
    }));
    let addr = bound_addr(server.listen(ephemeral()).unwrap());
    let client_token = client.connect(Endpoint::Tcp(addr));

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !connected(&client, client_token) {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
    }
    assert!(connected(&client, client_token));

    assert!(!client.send(client_token, &vec![7; 1024 * 1024]));

    // The refused send closed the connection there and then, and the
    // disconnect it queued is delivered by the very next iteration.
    network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
    assert!(disconnected(&client, client_token));
}

over_both_transports!(
    broadcast_serializes_once_for_multiple_connections,
    broadcast_serializes_once_for_multiple_connections_tcp,
    broadcast_serializes_once_for_multiple_connections_unix
);
fn broadcast_serializes_once_for_multiple_connections(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    let mut clients = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "clients",
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    }));
    let bound = server.listen(endpoint.clone()).unwrap();
    // A TCP listener on port 0 binds a port of the kernel's choosing.
    let endpoint = &bound;
    let _first_client = clients.connect(endpoint.clone());
    let _second_client = clients.connect(endpoint.clone());

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (accepts(&server) != 2 || connects(&clients) != 2) {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut clients]);
        assert!(!has_disconnect(&server), "unexpected disconnect");
        assert!(!has_disconnect(&clients), "unexpected disconnect");
    }
    assert_eq!(accepts(&server), 2);
    assert_eq!(connects(&clients), 2);

    // One serialization feeds every connection of the group, however many
    // there are. That promise is the group's own, so the group comes out of
    // its service for the call and goes back into one straight after.
    let mut group = server.into_group();
    let serializations = Cell::new(0);
    let recipients = group.broadcast_with(|buf| {
        serializations.set(serializations.get() + 1);
        buf.extend_from_slice(RESPONSE);
    });
    let mut server = RawService::new(group);
    assert_eq!(recipients, 2);
    assert_eq!(serializations.get(), 1);

    let mut received = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && received.len() != 2 {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut clients]);
        drain(&mut clients, &mut received);
    }
    assert_eq!(received.len(), 2);
    assert!(received.iter().all(|payload| payload == RESPONSE), "{received:?}");
}

over_both_transports!(
    disconnected_messages_are_dropped_and_token_survives_reconnect,
    disconnected_messages_are_dropped_and_token_survives_reconnect_tcp,
    disconnected_messages_are_dropped_and_token_survives_reconnect_unix
);
fn disconnected_messages_are_dropped_and_token_survives_reconnect(endpoint: &Endpoint) {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    let mut client = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "client",
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    }));
    let bound = server.listen(endpoint.clone()).unwrap();
    // A TCP listener on port 0 binds a port of the kernel's choosing.
    let endpoint = &bound;
    let client_token = client.connect(endpoint.clone());

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline &&
        (server.accepted().is_none() || !connected(&client, client_token))
    {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
    }
    let server_token = server.accepted().expect("connection was not accepted");
    assert!(connected(&client, client_token));
    assert!(server.disconnect(server_token));

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !disconnected(&client, client_token) {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
    }
    assert!(disconnected(&client, client_token));

    // A send to a token that is down never reaches its serializer. That
    // promise is the group's own, so the group comes out of its service for
    // the call and goes back into one straight after.
    let mut group = client.into_group();
    let serialization_called = Cell::new(false);
    assert!(!group.send_with(client_token, |buf| {
        serialization_called.set(true);
        buf.extend_from_slice(REQUEST);
    }));
    assert!(!serialization_called.get());
    let mut client = RawService::new(group);

    // The reconnect is a fresh accept on the server, and the client's token
    // is the one it started with.
    let accepted_before = accepts(&server);
    let mut server_messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline &&
        (!connected(&client, client_token) || accepts(&server) == accepted_before)
    {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
        drain(&mut server, &mut server_messages);
    }
    assert!(connected(&client, client_token));
    assert!(accepts(&server) > accepted_before);
    assert!(server_messages.is_empty());

    assert!(client.send(client_token, RESPONSE));
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !contains(&server_messages, RESPONSE) {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
        drain(&mut server, &mut server_messages);
    }
    assert!(contains(&server_messages, RESPONSE));
}

/// TCP only: `TcpConnector` speaks TCP alone.
#[test]
fn stream_network_is_wire_compatible_with_tcp_connector() {
    let mut network = StreamNetwork::default();
    let mut server = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "network-server",
        on_connect_msg: Some(SERVER_HELLO.to_vec()),
        ..Default::default()
    }));
    let addr = bound_addr(server.listen(ephemeral()).unwrap());

    let mut connector = TcpConnector::default();
    let connector_token = connector.connect(addr).expect("connector failed to connect");
    connector.write_or_enqueue_with(SendBehavior::Single(connector_token), |buf| {
        buf.extend_from_slice(REQUEST);
    });

    let mut network_messages = Vec::new();
    let mut connector_messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline &&
        (!contains(&network_messages, REQUEST) || !contains(&connector_messages, SERVER_HELLO))
    {
        network.drive(Some(poll_slice()), &mut [&mut server]);
        drain(&mut server, &mut network_messages);
        connector.poll_with(|event| {
            if let PollEvent::Message { payload, .. } = event {
                connector_messages.push(payload.to_vec());
            }
        });
    }
    assert!(contains(&network_messages, REQUEST));
    assert!(contains(&connector_messages, SERVER_HELLO));
    // A listening group with no outbound endpoint only ever accepts, and
    // nothing here closes a connection.
    assert!(
        server.records().iter().all(|record| matches!(record, Record::Accepted { .. })),
        "{:?}",
        server.records()
    );

    let network_token = server.accepted().expect("connection was not accepted");
    assert!(server.send(network_token, RESPONSE));
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !contains(&connector_messages, RESPONSE) {
        network.drive(Some(poll_slice()), &mut [&mut server]);
        connector.poll_with(|event| {
            if let PollEvent::Message { payload, .. } = event {
                connector_messages.push(payload.to_vec());
            }
        });
    }
    assert!(contains(&connector_messages, RESPONSE));
}

/// A loopback address for the one listener this file cannot bind through the
/// network: `TcpConnector::listen_at` takes an address and reports none back,
/// so the port is chosen here and bound a moment later.
fn probed_addr() -> SocketAddr {
    let probe = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    addr
}

/// TCP only: `TcpConnector` speaks TCP alone.
#[test]
fn stream_network_client_is_wire_compatible_with_tcp_connector_server() {
    let addr = probed_addr();
    let mut connector = TcpConnector::default().with_on_connect_msg(SERVER_HELLO.to_vec());
    connector.listen_at(addr).expect("connector failed to listen");

    let mut network = StreamNetwork::default();
    let mut client = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "network-client",
        on_connect_msg: Some(CLIENT_HELLO.to_vec()),
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    }));
    let client_token = client.connect(Endpoint::Tcp(addr));

    let mut connector_token = None;
    let mut connector_messages = Vec::new();
    let mut network_messages = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline &&
        (!connected(&client, client_token) ||
            !contains(&connector_messages, CLIENT_HELLO) ||
            !contains(&network_messages, SERVER_HELLO))
    {
        connector.poll_with(|event| match event {
            PollEvent::Accept { stream, .. } => connector_token = Some(stream),
            PollEvent::Message { payload, .. } => connector_messages.push(payload.to_vec()),
            PollEvent::Disconnect { .. } => panic!("connector disconnected unexpectedly"),
            PollEvent::Reconnect { .. } => unreachable!(),
        });
        network.drive(Some(poll_slice()), &mut [&mut client]);
        drain(&mut client, &mut network_messages);
    }
    assert!(connected(&client, client_token));
    assert!(contains(&connector_messages, CLIENT_HELLO));
    assert!(contains(&network_messages, SERVER_HELLO));
    // An outbound group with no listener only ever connects, and nothing here
    // closes a connection.
    assert!(
        client.records().iter().all(|record| matches!(record, Record::Connected { .. })),
        "{:?}",
        client.records()
    );

    assert!(client.send(client_token, REQUEST));
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !contains(&connector_messages, REQUEST) {
        connector.poll_with(|event| {
            if let PollEvent::Message { payload, .. } = event {
                connector_messages.push(payload.to_vec());
            }
        });
        network.drive(Some(poll_slice()), &mut [&mut client]);
    }
    assert!(contains(&connector_messages, REQUEST));

    connector.write_or_enqueue_with(SendBehavior::Single(connector_token.unwrap()), |buf| {
        buf.extend_from_slice(RESPONSE);
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !contains(&network_messages, RESPONSE) {
        connector.poll_with(|_| {});
        network.drive(Some(poll_slice()), &mut [&mut client]);
        drain(&mut client, &mut network_messages);
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
    let mut server = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    let mut client = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "client",
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    }));
    let bound = server.listen(endpoint.clone()).unwrap();
    // A TCP listener on port 0 binds a port of the kernel's choosing.
    let endpoint = &bound;
    let client_token = client.connect(endpoint.clone());

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline &&
        (accepted_peer(&server).is_none() || connected_peer(&client).is_none())
    {
        network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
    }
    assert!(connected(&client, client_token), "the outbound connection is the token's own");
    let accepted = accepted_peer(&server);
    let connected = connected_peer(&client);

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

    let mut client_net = StreamNetwork::default();
    let mut client = RawService::new(client_net.add_group(ConnectionGroupConfig {
        name: "client",
        reconnect_interval: flux_timing::Duration::from_millis(5),
        ..Default::default()
    }));
    let client_token = client.connect(endpoint.clone());

    let deadline = Instant::now() + Duration::from_millis(100);
    while Instant::now() < deadline {
        client_net.drive(Some(poll_slice()), &mut [&mut client]);
        assert!(client.records().is_empty(), "connected before the listener existed");
    }
    assert!(!dir.path().join("late").exists());

    let mut server_net = StreamNetwork::default();
    let mut server = RawService::new(
        server_net.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    server.listen(endpoint).unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline &&
        (server.accepted().is_none() || !connected(&client, client_token))
    {
        server_net.drive(Some(poll_slice()), &mut [&mut server]);
        client_net.drive(Some(poll_slice()), &mut [&mut client]);
    }
    assert!(server.accepted().is_some());
    assert!(connected(&client, client_token));
}
