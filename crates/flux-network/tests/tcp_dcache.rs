use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    thread,
    time::{Duration, Instant},
};

use flux_network::tcp::{MessagePayload, PollEvent, SendBehavior, TcpConnector};
use flux_utils::DCache;

/// Two inbound connections both write into the same connector dcache.
/// Verifies that both payloads are readable via the single shared reader.
#[test]
fn dcache_multi_stream() {
    let bind_addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 24712));

    const MSG_A: &[u8] = b"stream-a";
    const MSG_B: &[u8] = b"stream-b";
    const CAPACITY: usize = 4096;

    let writer = DCache::new(CAPACITY);
    let reader = writer.clone();

    let server = thread::spawn(move || {
        let mut conn = TcpConnector::default().with_dcache(writer);
        conn.listen_at(bind_addr).unwrap();

        let mut accepted = 0usize;
        let mut received = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        while (accepted < 2 || received.len() < 2) && Instant::now() < deadline {
            conn.poll_with(|ev| match ev {
                PollEvent::Accept { .. } => accepted += 1,
                PollEvent::Message { payload: MessagePayload::Cached(r), .. } => {
                    received.push(reader.map(r, |b| b.to_vec()).unwrap());
                }
                _ => {}
            });
            thread::sleep(Duration::from_micros(50));
        }
        assert_eq!(received.len(), 2);
        assert!(received.contains(&MSG_A.to_vec()));
        assert!(received.contains(&MSG_B.to_vec()));
    });

    for msg in [MSG_A, MSG_B] {
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let mut conn = TcpConnector::default();
            let tok = conn.connect(bind_addr).unwrap();
            conn.write_or_enqueue_with(SendBehavior::Single(tok), |buf| {
                buf.extend_from_slice(msg);
            });
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                conn.poll_with(|_| {});
                thread::sleep(Duration::from_micros(50));
            }
        });
    }

    server.join().unwrap();
}
