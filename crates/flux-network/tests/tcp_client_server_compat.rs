use std::{
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpStream as StdTcpStream},
    thread,
    time::{Duration, Instant},
};

use flux_network::tcp::{
    ClientEvent, PollEvent, SendBehavior, ServerEvent, TcpClient, TcpConnector, TcpServer,
};

const CLIENT_HELLO: &[u8] = b"client-hello";
const SERVER_HELLO: &[u8] = b"server-hello";
const REQUEST: &[u8] = b"request-payload";
const RESPONSE: &[u8] = b"response-payload";
const FRAME_HEADER_SIZE: usize = size_of::<u32>() + size_of::<u64>();

fn unused_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn contains(messages: &[Vec<u8>], expected: &[u8]) -> bool {
    messages.iter().any(|message| message == expected)
}

fn write_raw_frame(stream: &mut StdTcpStream, payload: &[u8]) {
    let mut frame = Vec::with_capacity(FRAME_HEADER_SIZE + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&0u64.to_le_bytes());
    frame.extend_from_slice(payload);
    stream.write_all(&frame).unwrap();
}

fn read_raw_frame(stream: &mut StdTcpStream) -> Vec<u8> {
    let mut header = [0; FRAME_HEADER_SIZE];
    stream.read_exact(&mut header).unwrap();
    let payload_len = u32::from_le_bytes(header[..size_of::<u32>()].try_into().unwrap()) as usize;
    let mut payload = vec![0; payload_len];
    stream.read_exact(&mut payload).unwrap();
    payload
}

#[test]
fn legacy_client_is_wire_compatible_with_new_server() {
    let addr = unused_addr();
    let mut server = TcpServer::default().with_on_connect_msg(SERVER_HELLO.to_vec());
    server.listen_at(addr).expect("new server failed to listen");

    let mut client = TcpConnector::default();
    let client_token = client.connect(addr).expect("legacy client failed to connect");
    client.write_or_enqueue_with(SendBehavior::Single(client_token), |buf| {
        buf.extend_from_slice(REQUEST);
    });

    let mut server_stream = None;
    let mut server_messages = Vec::new();
    let mut client_messages = Vec::new();
    let mut response_sent = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        server.poll_with(|event| match event {
            ServerEvent::Accept { stream, peer_addr, .. } => {
                assert_eq!(peer_addr.ip(), addr.ip());
                server_stream = Some(stream);
            }
            ServerEvent::Message { payload, .. } => server_messages.push(payload.to_vec()),
            ServerEvent::Disconnect { .. } => panic!("new server disconnected unexpectedly"),
        });
        client.poll_with(|event| {
            if let PollEvent::Message { payload, .. } = event {
                client_messages.push(payload.to_vec());
            }
        });

        if !response_sent && contains(&server_messages, REQUEST) {
            let stream = server_stream.expect("request arrived before accept event");
            server.write_or_enqueue_with(SendBehavior::Single(stream), |buf| {
                buf.extend_from_slice(RESPONSE);
            });
            response_sent = true;
        }
        if contains(&client_messages, SERVER_HELLO) &&
            contains(&client_messages, RESPONSE) &&
            contains(&server_messages, REQUEST)
        {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    assert!(contains(&server_messages, REQUEST));
    assert!(contains(&client_messages, SERVER_HELLO));
    assert!(contains(&client_messages, RESPONSE));
}

#[test]
fn new_client_is_wire_compatible_with_legacy_server() {
    let addr = unused_addr();
    let mut server = TcpConnector::default().with_on_connect_msg(SERVER_HELLO.to_vec());
    server.listen_at(addr).expect("legacy server failed to listen");

    let mut client = TcpClient::default().with_on_connect_msg(CLIENT_HELLO.to_vec());
    let client_token = client.connect(addr);
    // This must be retained behind the on-connect frame until establishment.
    client.write_or_enqueue_with(SendBehavior::Single(client_token), |buf| {
        buf.extend_from_slice(REQUEST);
    });

    let mut server_stream = None;
    let mut server_messages = Vec::new();
    let mut client_messages = Vec::new();
    let mut connected = None;
    let mut response_sent = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        server.poll_with(|event| match event {
            PollEvent::Accept { stream, .. } => server_stream = Some(stream),
            PollEvent::Message { payload, .. } => server_messages.push(payload.to_vec()),
            PollEvent::Disconnect { .. } => panic!("legacy server disconnected unexpectedly"),
            PollEvent::Reconnect { .. } => panic!("legacy server cannot reconnect inbound streams"),
        });
        client.poll_with(|event| match event {
            ClientEvent::Connected { token, peer_addr } => connected = Some((token, peer_addr)),
            ClientEvent::Message { payload, .. } => client_messages.push(payload.to_vec()),
            ClientEvent::Disconnect { .. } => panic!("new client disconnected unexpectedly"),
        });

        if !response_sent && contains(&server_messages, REQUEST) {
            let stream = server_stream.expect("request arrived before accept event");
            server.write_or_enqueue_with(SendBehavior::Single(stream), |buf| {
                buf.extend_from_slice(RESPONSE);
            });
            response_sent = true;
        }
        if connected == Some((client_token, addr)) &&
            server_messages.first().is_some_and(|message| message == CLIENT_HELLO) &&
            contains(&server_messages, REQUEST) &&
            contains(&client_messages, SERVER_HELLO) &&
            contains(&client_messages, RESPONSE)
        {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(connected, Some((client_token, addr)));
    assert_eq!(server_messages.first().map(Vec::as_slice), Some(CLIENT_HELLO));
    assert!(contains(&server_messages, REQUEST));
    assert!(contains(&client_messages, SERVER_HELLO));
    assert!(contains(&client_messages, RESPONSE));
}

#[test]
fn new_client_retries_initial_failure_with_the_same_token() {
    let addr = unused_addr();
    let mut client =
        TcpClient::default().with_reconnect_interval(flux_timing::Duration::from_millis(1));
    let token = client.connect(addr);
    client.write_or_enqueue_with(SendBehavior::Single(token), |buf| {
        buf.extend_from_slice(REQUEST);
    });

    let no_server_deadline = Instant::now() + Duration::from_millis(20);
    let mut connected_early = false;
    while Instant::now() < no_server_deadline {
        client.poll_with(|event| {
            if let ClientEvent::Connected { .. } = event {
                connected_early = true;
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(!connected_early);
    assert!(client.currently_disconnected().any(|candidate| candidate == token));

    let mut server = TcpServer::default();
    server.listen_at(addr).expect("new server failed to listen");
    let mut connected = None;
    let mut received = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (connected.is_none() || received.is_none()) {
        client.poll_with(|event| {
            if let ClientEvent::Connected { token, peer_addr } = event {
                connected = Some((token, peer_addr));
            }
        });
        server.poll_with(|event| {
            if let ServerEvent::Message { payload, .. } = event {
                received = Some(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(connected, Some((token, addr)));
    assert_eq!(received.as_deref(), Some(REQUEST));
    client.disconnect(token);
    assert!(client.currently_disconnected().any(|candidate| candidate == token));
    client.remove(token);
    assert!(!client.currently_disconnected().any(|candidate| candidate == token));
}

fn receive_after_reconnect(drop_backlog: bool) -> (Option<Vec<u8>>, bool) {
    let addr = unused_addr();
    let mut server = TcpServer::default();
    server.listen_at(addr).expect("new server failed to listen");
    let mut client = TcpClient::default()
        .with_reconnect_interval(flux_timing::Duration::from_millis(1))
        .with_drop_backlog_on_disconnect(drop_backlog);
    let token = client.connect(addr);

    let mut server_stream = None;
    let mut initially_connected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (server_stream.is_none() || !initially_connected) {
        server.poll_with(|event| {
            if let ServerEvent::Accept { stream, .. } = event {
                server_stream = Some(stream);
            }
        });
        client.poll_with(|event| {
            if let ClientEvent::Connected { token: event_token, .. } = event {
                assert_eq!(event_token, token);
                initially_connected = true;
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(initially_connected);
    server.disconnect(server_stream.expect("server did not accept initial connection"));

    let mut disconnected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !disconnected {
        client.poll_with(|event| {
            if let ClientEvent::Disconnect { token: event_token, .. } = event {
                assert_eq!(event_token, token);
                disconnected = true;
            }
        });
        server.poll_with(|_| {});
        thread::sleep(Duration::from_millis(1));
    }
    assert!(disconnected, "client did not observe server disconnect");

    client.write_or_enqueue_with(SendBehavior::Single(token), |buf| {
        buf.extend_from_slice(REQUEST);
    });
    client.force_reconnect();

    let mut received = None;
    let mut reconnected = false;
    let mut reconnected_at = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        server.poll_with(|event| {
            if let ServerEvent::Message { payload, .. } = event {
                received = Some(payload.to_vec());
            }
        });
        client.poll_with(|event| {
            if let ClientEvent::Connected { token: event_token, peer_addr } = event {
                assert_eq!(event_token, token);
                assert_eq!(peer_addr, addr);
                reconnected = true;
                reconnected_at = Some(Instant::now());
            }
        });
        if received.is_some() ||
            reconnected_at.is_some_and(|at| at.elapsed() >= Duration::from_millis(20))
        {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    (received, reconnected)
}

#[test]
fn new_client_replays_disconnected_backlog_by_default() {
    assert_eq!(receive_after_reconnect(false), (Some(REQUEST.to_vec()), true));
}

#[test]
fn new_client_can_drop_disconnected_backlog() {
    assert_eq!(receive_after_reconnect(true), (None, true));
}

#[test]
fn server_token_lookup_survives_middle_stream_removal() {
    let addr = unused_addr();
    let mut server = TcpServer::default();
    server.listen_at(addr).expect("server failed to listen");

    let mut clients = Vec::new();
    let mut server_tokens = Vec::new();
    for _ in 0..3 {
        let client = StdTcpStream::connect(addr).unwrap();
        client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        clients.push(client);

        let deadline = Instant::now() + Duration::from_secs(5);
        while server_tokens.len() != clients.len() && Instant::now() < deadline {
            server.poll_with(|event| {
                if let ServerEvent::Accept { stream, .. } = event {
                    server_tokens.push(stream);
                }
            });
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(server_tokens.len(), clients.len());
    }

    server.disconnect(server_tokens[1]);
    server.write_or_enqueue_with(SendBehavior::Single(server_tokens[2]), |output| {
        output.extend_from_slice(RESPONSE);
    });
    assert_eq!(read_raw_frame(&mut clients[2]), RESPONSE);

    write_raw_frame(&mut clients[2], REQUEST);
    let mut received = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while received.is_none() && Instant::now() < deadline {
        server.poll_with(|event| {
            if let ServerEvent::Message { token, payload, .. } = event {
                received = Some((token, payload.to_vec()));
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(received, Some((server_tokens[2], REQUEST.to_vec())));
}
