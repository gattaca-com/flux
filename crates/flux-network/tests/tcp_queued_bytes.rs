use std::{
    io::Read,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    thread,
    time::{Duration, Instant},
};

use flux_network::{Network, ReplayPolicy, TcpGroupConfig};

fn pump_until(conn: &mut Network, deadline: Duration, mut done: impl FnMut(&Network) -> bool) {
    let until = Instant::now() + deadline;
    while Instant::now() < until && !done(conn) {
        while conn.poll_with(|_| {}) {}
        thread::sleep(Duration::from_millis(1));
    }
}

/// A peer that has not started reading leaves sends queued; they drain to zero
/// once it reads.
#[test]
fn queued_bytes_follow_the_send_queue() {
    let listener =
        TcpListener::bind(SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 0))).unwrap();
    let addr = listener.local_addr().unwrap();
    let reader = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        thread::sleep(Duration::from_millis(300));
        let mut sink = Vec::new();
        let _ = stream.read_to_end(&mut sink);
        sink.len()
    });

    let mut conn = Network::default();
    let group = conn.add_group(TcpGroupConfig {
        aligned_payloads: true,
        replay: ReplayPolicy::Replay,
        socket_buf_size: Some(1024),
        ..Default::default()
    });
    let token = conn.connect(group, addr);
    pump_until(&mut conn, Duration::from_secs(5), |c| c.currently_disconnected().next().is_none());

    let payload = vec![7u8; 64 * 1024];
    for _ in 0..64 {
        conn.send_with(token, |buf| buf.extend_from_slice(&payload));
    }
    assert!(
        conn.queued_bytes(token) > 1024 * 1024,
        "sends stay queued while the peer is not reading"
    );

    pump_until(&mut conn, Duration::from_secs(10), |c| c.queued_bytes(token) == 0);
    assert_eq!(conn.queued_bytes(token), 0);
    drop(conn);
    assert!(reader.join().unwrap() >= 64 * payload.len());
}
