use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    thread,
    time::Duration,
};

use flux_network::tcp::{PollEvent, SendBehavior, TcpConnector};
use wincode_derive::{SchemaRead, SchemaWrite};

#[derive(Debug, PartialEq, SchemaRead, SchemaWrite)]
struct TestMsg(u32);

fn wincode_ser_into_vec<T>(buf: &mut Vec<u8>, value: &T)
where
    T: wincode::SchemaWrite<wincode::config::DefaultConfig, Src = T>,
{
    wincode::serialize_into(buf, value).unwrap();
}

#[test]
fn tcp_roundtrip() {
    let bind_addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 24712));

    let mut listener = TcpConnector::default();
    let _listening_token = listener.listen_at(bind_addr).unwrap();

    let server = thread::spawn(move || {
        let mut accepted_stream = None;

        while accepted_stream.is_none() {
            listener.poll_with(|event| match event {
                PollEvent::Accept { stream, .. } => accepted_stream = Some(stream),
                PollEvent::Message { .. } => panic!("shouldn't have gotten here"),
                _ => {}
            });
        }

        let stream_token = accepted_stream.unwrap();

        let mut recv = None;
        loop {
            listener.poll_with(|event| {
                if let PollEvent::Message { token, payload: bytes, .. } = event {
                    assert_eq!(token, stream_token);
                    let msg: TestMsg = wincode::deserialize(bytes).unwrap();
                    recv = Some(msg);
                }
            });
            if recv.is_some() {
                break;
            }
            thread::sleep(Duration::from_micros(50));
        }
        listener.write_or_enqueue_with(SendBehavior::Single(stream_token), |buf| {
            wincode_ser_into_vec(buf, &TestMsg(111));
        });
        listener.poll_with(|event| {
            if let PollEvent::Message { .. } = event {
                panic!("shouldn't have gotten here");
            }
        });
        assert_eq!(recv, Some(TestMsg(222)));
    });

    let client = thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(10));
        let mut conn = TcpConnector::default();
        let tok = conn.connect(bind_addr).unwrap();
        // Then responds
        conn.write_or_enqueue_with(SendBehavior::Single(tok), |buf| {
            wincode_ser_into_vec(buf, &TestMsg(222));
        });

        // Client waits for server message
        let mut recv = None;
        loop {
            conn.poll_with(|event| {
                if let PollEvent::Message { payload: bytes, .. } = event {
                    let msg: TestMsg = wincode::deserialize(bytes).unwrap();
                    recv = Some(msg);
                }
            });
            if recv.is_some() {
                break;
            }
            thread::sleep(Duration::from_micros(50));
        }
        assert_eq!(recv, Some(TestMsg(111)));
    });

    server.join().unwrap();
    client.join().unwrap();
}

#[test]
fn backlog_disconnect_is_reported() {
    let probe =
        std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("probe");
    let bind_addr = probe.local_addr().unwrap();
    drop(probe);

    let mut listener = TcpConnector::default()
        .with_socket_buf_size(1024)
        .with_max_backlog(0, flux_timing::Duration::ZERO);
    listener.listen_at(bind_addr).unwrap();

    let client = TcpStream::connect(bind_addr).expect("failed to connect client");

    let mut stream_token = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while stream_token.is_none() && std::time::Instant::now() < deadline {
        listener.poll_with(|event| {
            if let PollEvent::Accept { stream, .. } = event {
                stream_token = Some(stream);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }

    let stream_token = stream_token.expect("listener did not accept client");

    let payload = vec![7_u8; 16 * 1024 * 1024];
    listener.write_or_enqueue_with(SendBehavior::Single(stream_token), |buf| {
        buf.extend_from_slice(&payload);
    });

    let mut disconnected = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !disconnected && std::time::Instant::now() < deadline {
        listener.poll_with(|event| {
            if let PollEvent::Disconnect { token } = event {
                assert_eq!(token, stream_token);
                disconnected = true;
            }
        });
        thread::sleep(Duration::from_millis(1));
    }

    drop(client);
    assert!(disconnected, "backlog disconnect was not reported");
}

fn receive_after_reconnect(drop_backlog: bool) -> Option<TestMsg> {
    let probe =
        std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("probe");
    let bind_addr = probe.local_addr().unwrap();
    drop(probe);

    let mut listener = TcpConnector::default();
    listener.listen_at(bind_addr).unwrap();

    let mut client = TcpConnector::default()
        .with_drop_outbound_backlog_on_disconnect(drop_backlog)
        .with_reconnect_interval(flux_timing::Duration::from_millis(1));
    let token = client.connect(bind_addr).unwrap();

    let mut accepted = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while accepted == 0 && std::time::Instant::now() < deadline {
        listener.poll_with(|event| {
            if let PollEvent::Accept { .. } = event {
                accepted += 1;
            }
        });
        client.poll_with(|_| {});
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(accepted, 1, "listener did not accept initial client");

    client.disconnect(token);
    client.write_or_enqueue_with(SendBehavior::Single(token), |buf| {
        wincode_ser_into_vec(buf, &TestMsg(333));
    });
    client.force_reconnect();

    let mut recv = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while recv.is_none() && std::time::Instant::now() < deadline {
        listener.poll_with(|event| {
            if let PollEvent::Message { payload: bytes, .. } = event {
                recv = Some(wincode::deserialize(bytes).unwrap());
            }
        });
        client.poll_with(|_| {});
        thread::sleep(Duration::from_millis(1));
    }

    recv
}

#[test]
fn disconnected_outbound_replays_backlog_by_default() {
    assert_eq!(receive_after_reconnect(false), Some(TestMsg(333)));
}

#[test]
fn disconnected_outbound_can_drop_backlog() {
    assert_eq!(receive_after_reconnect(true), None);
}
