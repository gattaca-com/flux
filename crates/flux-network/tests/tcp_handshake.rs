use std::{
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use flux_network::tcp::{ClientEvent, SendBehavior, ServerEvent, TcpClient, TcpServer};

const HANDSHAKE: &[u8] = b"secret-handshake";
const CLIENT_HELLO: &[u8] = b"client-hello";
const SERVER_HELLO: &[u8] = b"server-hello";
const REQUEST: &[u8] = b"request";
const PRECONNECT_REQUEST: &[u8] = b"preconnect-request";
const WAITING_TARGETED: &[u8] = b"waiting-targeted";
const WAITING_BROADCAST: &[u8] = b"waiting-broadcast";
const HANDSHAKE_ACCEPTED: &[u8] = b"\0flux-handshake-v1\0accepted";
const RAW_FRAME_HEADER_SIZE: usize =
    core::mem::size_of::<u32>() + core::mem::size_of::<flux_timing::Nanos>();

fn unused_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn read_raw_frame(socket: &mut std::net::TcpStream) -> Vec<u8> {
    let mut header = [0_u8; RAW_FRAME_HEADER_SIZE];
    socket.read_exact(&mut header).expect("failed to read frame header");
    let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
    let mut payload = vec![0; len];
    socket.read_exact(&mut payload).expect("failed to read frame payload");
    payload
}

fn write_raw_frame(socket: &mut std::net::TcpStream, payload: &[u8]) {
    let mut header = [0_u8; RAW_FRAME_HEADER_SIZE];
    header[..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    socket.write_all(&header).expect("failed to write frame header");
    socket.write_all(payload).expect("failed to write frame payload");
}

#[test]
fn valid_handshake_is_consumed_before_accept_and_application_messages() {
    let addr = unused_addr();
    let handshakes = Arc::new(Mutex::new(Vec::new()));
    let seen = handshakes.clone();
    let mut server = TcpServer::default()
        .with_handshake_handler(move |message| {
            seen.lock().unwrap().push(message.to_vec());
            message == HANDSHAKE
        })
        .with_on_connect_msg(SERVER_HELLO.to_vec());
    server.listen_at(addr).expect("server failed to listen");

    let mut client = TcpClient::default()
        .with_handshake(HANDSHAKE.to_vec())
        .with_on_connect_msg(CLIENT_HELLO.to_vec());
    let client_token = client.connect(addr);
    client.write_or_enqueue_with(SendBehavior::Single(client_token), |buf| {
        buf.extend_from_slice(REQUEST);
    });

    let mut accepted = None;
    let mut server_messages = Vec::new();
    let mut client_messages = Vec::new();
    let mut event_order = Vec::new();
    let mut client_connected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        server.poll_with(|event| match event {
            ServerEvent::Accept { stream, .. } => {
                event_order.push("accept");
                accepted = Some(stream);
            }
            ServerEvent::Message { payload, .. } => {
                event_order.push("message");
                server_messages.push(payload.to_vec());
            }
            ServerEvent::Disconnect { .. } => panic!("server disconnected unexpectedly"),
        });
        client.poll_with(|event| match event {
            ClientEvent::Connected { .. } => client_connected = true,
            ClientEvent::HandshakeRejected { .. } => panic!("valid handshake was rejected"),
            ClientEvent::Message { payload, .. } => client_messages.push(payload.to_vec()),
            ClientEvent::Disconnect { .. } => panic!("client disconnected unexpectedly"),
        });

        if client_connected &&
            accepted.is_some() &&
            server_messages == [CLIENT_HELLO, REQUEST] &&
            client_messages.iter().any(|message| message == SERVER_HELLO)
        {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    assert!(accepted.is_some(), "server did not admit the client");
    assert!(client_connected, "client did not receive handshake acceptance");
    assert_eq!(*handshakes.lock().unwrap(), [HANDSHAKE]);
    assert_eq!(server_messages, [CLIENT_HELLO, REQUEST]);
    assert_eq!(event_order, ["accept", "message", "message"]);
    assert!(client_messages.iter().any(|message| message == SERVER_HELLO));
}

#[test]
fn client_queues_targeted_and_broadcast_messages_until_handshake_acceptance() {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let (handshake_seen_tx, handshake_seen_rx) = mpsc::channel();
    let (inspect_tx, inspect_rx) = mpsc::channel();

    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().expect("server failed to accept client");
        assert_eq!(read_raw_frame(&mut socket), HANDSHAKE);
        handshake_seen_tx.send(()).unwrap();
        inspect_rx.recv().unwrap();

        socket.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
        let mut byte = [0_u8; 1];
        match socket.read(&mut byte) {
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            result => panic!("application data was sent before handshake acceptance: {result:?}"),
        }
        socket.set_read_timeout(None).unwrap();

        write_raw_frame(&mut socket, HANDSHAKE_ACCEPTED);
        (0..4).map(|_| read_raw_frame(&mut socket)).collect::<Vec<_>>()
    });

    let mut client = TcpClient::default()
        .with_reconnect_interval(flux_timing::Duration::from_secs(60))
        .with_handshake(HANDSHAKE.to_vec())
        .with_on_connect_msg(CLIENT_HELLO.to_vec());
    let token = client.connect(addr);
    client.write_or_enqueue_with(SendBehavior::Single(token), |buf| {
        buf.extend_from_slice(PRECONNECT_REQUEST);
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    while handshake_seen_rx.try_recv().is_err() {
        client.poll_with(|event| match event {
            ClientEvent::Connected { .. } => panic!("client connected before server acceptance"),
            ClientEvent::HandshakeRejected { .. } => panic!("server did not reject handshake"),
            ClientEvent::Disconnect { .. } => panic!("client disconnected unexpectedly"),
            ClientEvent::Message { .. } => panic!("client received an unexpected message"),
        });
        assert!(Instant::now() < deadline, "server did not receive handshake");
        thread::sleep(Duration::from_millis(1));
    }

    client.write_or_enqueue_with(SendBehavior::Single(token), |buf| {
        buf.extend_from_slice(WAITING_TARGETED);
    });
    client.write_or_enqueue_with(SendBehavior::Broadcast, |buf| {
        buf.extend_from_slice(WAITING_BROADCAST);
    });
    let wait_until = Instant::now() + Duration::from_millis(20);
    while Instant::now() < wait_until {
        client.poll_with(|event| match event {
            ClientEvent::Connected { .. } => panic!("client connected before server acceptance"),
            ClientEvent::HandshakeRejected { .. } => panic!("server did not reject handshake"),
            ClientEvent::Disconnect { .. } => panic!("client disconnected unexpectedly"),
            ClientEvent::Message { .. } => panic!("client received an unexpected message"),
        });
        thread::sleep(Duration::from_millis(1));
    }
    inspect_tx.send(()).unwrap();

    let mut connected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !server.is_finished() {
        client.poll_with(|event| match event {
            ClientEvent::Connected { token: event_token, .. } => {
                assert_eq!(event_token, token);
                connected = true;
            }
            ClientEvent::HandshakeRejected { .. } => panic!("valid handshake was rejected"),
            ClientEvent::Disconnect { .. } => panic!("client disconnected unexpectedly"),
            ClientEvent::Message { .. } => panic!("client received an unexpected message"),
        });
        assert!(Instant::now() < deadline, "queued messages were not flushed after acceptance");
        thread::sleep(Duration::from_millis(1));
    }

    assert!(connected, "client did not report handshake acceptance");
    assert_eq!(server.join().unwrap(), [
        CLIENT_HELLO,
        PRECONNECT_REQUEST,
        WAITING_TARGETED,
        WAITING_BROADCAST
    ]);
}

#[test]
fn rejected_handshake_emits_client_event_without_server_events() {
    let addr = unused_addr();
    let attempts = Arc::new(AtomicUsize::new(0));
    let seen = attempts.clone();
    let mut server = TcpServer::default().with_handshake_handler(move |message| {
        seen.fetch_add(1, Ordering::Relaxed);
        message == HANDSHAKE
    });
    server.listen_at(addr).expect("server failed to listen");

    let mut client = TcpClient::default()
        .with_reconnect_interval(flux_timing::Duration::from_secs(60))
        .with_handshake(b"wrong-handshake".to_vec());
    let client_token = client.connect(addr);

    let mut server_events = 0;
    let mut handshake_rejected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !handshake_rejected {
        server.poll_with(|_| server_events += 1);
        client.poll_with(|event| match event {
            ClientEvent::Connected { .. } => panic!("rejected client was reported as connected"),
            ClientEvent::HandshakeRejected { token, peer_addr } => {
                assert_eq!(token, client_token);
                assert_eq!(peer_addr, addr);
                handshake_rejected = true;
            }
            ClientEvent::Disconnect { .. } => {
                panic!("explicit rejection was reported as a transport disconnect")
            }
            ClientEvent::Message { .. } => panic!("rejected client received a message"),
        });
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(attempts.load(Ordering::Relaxed), 1);
    assert_eq!(server_events, 0);
    assert!(handshake_rejected, "client did not receive HandshakeRejected");
}

#[test]
fn idle_and_partial_handshakes_time_out_silently() {
    for partial_header in [false, true] {
        let addr = unused_addr();
        let mut server = TcpServer::default()
            .with_handshake_handler(|message| message == HANDSHAKE)
            .with_handshake_timeout(flux_timing::Duration::from_millis(5));
        server.listen_at(addr).expect("server failed to listen");

        let mut socket = std::net::TcpStream::connect(addr).expect("client failed to connect");
        if partial_header {
            socket.write_all(&[1, 0]).expect("failed to write partial frame header");
        }

        let mut server_events = 0;
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut accepted_transport = false;
        while Instant::now() < deadline && !accepted_transport {
            accepted_transport = server.poll_with(|_| server_events += 1);
            thread::sleep(Duration::from_millis(1));
        }
        assert!(accepted_transport, "server did not accept the transport");

        thread::sleep(Duration::from_millis(20));
        assert!(
            server.poll_with(|_| server_events += 1),
            "server did not expire the pending handshake"
        );
        assert_eq!(server_events, 0);

        socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let mut byte = [0_u8; 1];
        match socket.read(&mut byte) {
            Ok(0) => {}
            Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
            result => panic!("timed-out handshake socket remained open: {result:?}"),
        }
    }
}

#[test]
fn transport_failure_while_awaiting_response_is_not_rejection() {
    let addr = unused_addr();
    let mut server = TcpServer::default();
    server.listen_at(addr).expect("server failed to listen");

    let mut client = TcpClient::default()
        .with_reconnect_interval(flux_timing::Duration::from_secs(60))
        .with_handshake(HANDSHAKE.to_vec());
    let client_token = client.connect(addr);

    let mut server_stream = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && server_stream.is_none() {
        client.poll_with(|event| match event {
            ClientEvent::Connected { .. } => {
                panic!("client connected without a handshake response")
            }
            ClientEvent::HandshakeRejected { .. } => panic!("server did not reject the handshake"),
            ClientEvent::Disconnect { .. } | ClientEvent::Message { .. } => {}
        });
        server.poll_with(|event| {
            if let ServerEvent::Accept { stream, .. } = event {
                server_stream = Some(stream);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    server.disconnect(server_stream.expect("server did not accept the transport"));

    let mut disconnected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !disconnected {
        client.poll_with(|event| match event {
            ClientEvent::Connected { .. } => {
                panic!("client connected without a handshake response")
            }
            ClientEvent::HandshakeRejected { .. } => {
                panic!("transport failure was reported as handshake rejection")
            }
            ClientEvent::Disconnect { token, peer_addr } => {
                assert_eq!(token, client_token);
                assert_eq!(peer_addr, addr);
                disconnected = true;
            }
            ClientEvent::Message { .. } => {}
        });
        server.poll_with(|_| {});
        thread::sleep(Duration::from_millis(1));
    }

    assert!(disconnected, "client did not report the transport failure");
}

#[test]
fn handshake_is_sent_again_after_reconnect() {
    let addr = unused_addr();
    let attempts = Arc::new(AtomicUsize::new(0));
    let seen = attempts.clone();
    let mut server = TcpServer::default().with_handshake_handler(move |message| {
        seen.fetch_add(1, Ordering::Relaxed);
        message == HANDSHAKE
    });
    server.listen_at(addr).expect("server failed to listen");

    let mut client = TcpClient::default()
        .with_reconnect_interval(flux_timing::Duration::from_secs(60))
        .with_handshake(HANDSHAKE.to_vec());
    let client_token = client.connect(addr);

    let mut first_stream = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && first_stream.is_none() {
        server.poll_with(|event| {
            if let ServerEvent::Accept { stream, .. } = event {
                first_stream = Some(stream);
            }
        });
        client.poll_with(|_| {});
        thread::sleep(Duration::from_millis(1));
    }
    server.disconnect(first_stream.expect("server did not admit the first connection"));

    let mut disconnected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !disconnected {
        server.poll_with(|_| {});
        client.poll_with(|event| {
            if let ClientEvent::Disconnect { token, .. } = event {
                assert_eq!(token, client_token);
                disconnected = true;
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(disconnected, "client did not observe the forced disconnect");
    client.force_reconnect();

    let mut second_stream = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && second_stream.is_none() {
        server.poll_with(|event| {
            if let ServerEvent::Accept { stream, .. } = event {
                second_stream = Some(stream);
            }
        });
        client.poll_with(|_| {});
        thread::sleep(Duration::from_millis(1));
    }

    assert!(second_stream.is_some(), "server did not admit the reconnected client");
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
}
