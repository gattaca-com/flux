use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    thread,
    time::Duration,
};

use flux_network::tcp::{PollEvent, SendBehavior, TcpConnector};
use wincode_derive::{SchemaRead, SchemaWrite};

#[derive(Debug, PartialEq, SchemaRead, SchemaWrite)]
struct TestMsg(u32);

fn wincode_ser_into_vec<T>(buf: &mut Vec<u8>, value: &T)
where
    T: wincode::SchemaWrite<Src = T>,
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
                if let PollEvent::Message { token, payload, .. } = event {
                    assert_eq!(token, stream_token);
                    let msg: TestMsg = wincode::deserialize(payload).unwrap();
                    recv = Some(msg);
                }
            });
            if recv.is_some() {
                break;
            }
            thread::sleep(Duration::from_micros(50));
        }
        listener.write_or_enqueue_with(SendBehavior::Single(stream_token), |buf| {
            wincode_ser_into_vec(buf, &TestMsg(111))
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
        let _tok = conn.connect(bind_addr).unwrap();
        // Then responds
        conn.write_or_enqueue_with(SendBehavior::Single(_tok), |buf| {
            wincode_ser_into_vec(buf, &TestMsg(222))
        });

        // Client waits for server message
        let mut recv = None;
        loop {
            conn.poll_with(|event| {
                if let PollEvent::Message { payload, .. } = event {
                    let msg: TestMsg = wincode::deserialize(payload).unwrap();
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

/// Client connects before the server is listening. The client's reconnect
/// loop should establish the connection once the server comes up.
#[test]
fn tcp_client_before_server() {
    let bind_addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 24713));

    let client = thread::spawn(move || {
        let mut conn =
            TcpConnector::default().with_reconnect_interval(Duration::from_millis(50).into());

        assert!(conn.connect(bind_addr).is_none());

        // Poll until we receive Reconnect.
        let mut tok = None;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while tok.is_none() {
            assert!(std::time::Instant::now() < deadline, "timed out waiting for reconnect");
            conn.poll_with(|event| {
                if let PollEvent::Reconnect { stream, .. } = event {
                    tok = Some(stream);
                }
            });
            thread::sleep(Duration::from_millis(10));
        }
        let tok = tok.unwrap();

        conn.write_or_enqueue_with(SendBehavior::Single(tok), |buf| {
            wincode_ser_into_vec(buf, &TestMsg(444))
        });
        conn.poll_with(|_| {});

        // Wait for server's reply.
        let mut recv = None;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            conn.poll_with(|event| {
                if let PollEvent::Message { payload, .. } = event {
                    recv = Some(wincode::deserialize::<TestMsg>(payload).unwrap());
                }
            });
            if recv.is_some() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "timed out waiting for server msg");
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(recv, Some(TestMsg(333)));
    });

    // Give client time to call connect() first.
    thread::sleep(Duration::from_millis(50));

    let mut listener = TcpConnector::default();
    let _listening_token = listener.listen_at(bind_addr).unwrap();

    let server = thread::spawn(move || {
        let mut accepted_stream = None;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while accepted_stream.is_none() {
            assert!(std::time::Instant::now() < deadline, "timed out waiting for accept");
            listener.poll_with(|event| {
                if let PollEvent::Accept { stream, .. } = event {
                    accepted_stream = Some(stream);
                }
            });
            thread::sleep(Duration::from_millis(10));
        }

        let stream_token = accepted_stream.unwrap();

        // Wait for client's message.
        let mut recv = None;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            listener.poll_with(|event| {
                if let PollEvent::Message { payload, .. } = event {
                    recv = Some(wincode::deserialize::<TestMsg>(payload).unwrap());
                }
            });
            if recv.is_some() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "timed out waiting for client msg");
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(recv, Some(TestMsg(444)));

        // Reply to client.
        listener.write_or_enqueue_with(SendBehavior::Single(stream_token), |buf| {
            wincode_ser_into_vec(buf, &TestMsg(333))
        });
        listener.poll_with(|_| {});
    });

    server.join().unwrap();
    client.join().unwrap();
}
