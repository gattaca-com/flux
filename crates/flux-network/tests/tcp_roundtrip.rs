use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    thread,
    time::Duration,
};

use flux_network::tcp::{SendBehavior, TcpConnector};
use wincode_derive::{SchemaRead, SchemaWrite};

#[derive(Debug, PartialEq, SchemaRead, SchemaWrite)]
struct TestMsg(u32);

/// Helper fn to be used in the closure for
/// `TcpConnection::write_or_enqueue_with`
#[inline(always)]
pub fn wincode_ser_into_slice<T>(buf: &mut [u8], value: &T) -> usize
where
    T: wincode::SchemaWrite<Src = T>,
{
    let len = buf.len();
    let mut cursor = buf;
    wincode::serialize_into(&mut cursor, value).unwrap();
    len - cursor.len()
}

#[test]
fn tcp_roundtrip() {
    let bind_addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 24712));

    let mut listener = TcpConnector::default();
    let listening_token = listener.listen_at(bind_addr).unwrap();

    let server = thread::spawn(move || {
        let mut connected_stream = None;

        while connected_stream.is_none() {
            listener.poll_with(
                |connection| connected_stream = Some(connection),
                |_, _, _| panic!("shouldn't have gotten here"),
            );
        }
        assert_eq!(connected_stream.as_ref().unwrap().listener, listening_token);

        let mut recv = None;
        loop {
            listener.poll_with(
                |_| {},
                |token, frame, _send_ts| {
                    assert_eq!(token, connected_stream.as_ref().unwrap().incoming_stream);
                    let msg: TestMsg = wincode::deserialize(frame).unwrap();
                    recv = Some(msg);
                },
            );
            if recv.is_some() {
                break;
            }
            thread::sleep(Duration::from_micros(50));
        }
        listener.write_or_enqueue_with(
            SendBehavior::Single(connected_stream.as_ref().unwrap().incoming_stream),
            |buf| wincode_ser_into_slice(buf, &TestMsg(111)),
        );
        listener.poll_with(|_| {}, |_, _, _send_ts| panic!("shouldn't have gotten here"));
        assert_eq!(recv, Some(TestMsg(222)));
    });

    let client = thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(10));
        let mut conn = TcpConnector::default();
        let tok = conn.connect(bind_addr).unwrap();
        // Then responds
        conn.write_or_enqueue_with(SendBehavior::Single(tok), |buf| {
            wincode_ser_into_slice(buf, &TestMsg(222))
        });

        // Client waits for server message
        let mut recv = None;
        loop {
            conn.poll_with(
                |_| {},
                |_, frame, _send_ts| {
                    let msg: TestMsg = wincode::deserialize(frame).unwrap();
                    recv = Some(msg);
                },
            );
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
