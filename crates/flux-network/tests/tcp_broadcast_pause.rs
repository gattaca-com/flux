use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    thread,
    time::Duration,
};

use flux_network::{NetworkDriver, PollEvent, SendBehavior, TcpConfig, Transport};

fn pump(conn: &mut NetworkDriver, collect: &mut Vec<Vec<u8>>, for_how_long: Duration) {
    let deadline = std::time::Instant::now() + for_how_long;
    while std::time::Instant::now() < deadline {
        conn.poll_with(|event| {
            if let PollEvent::Message { payload, .. } = event {
                collect.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
}

fn send(sender: &mut NetworkDriver, body: &[u8]) {
    sender.write_or_enqueue_with(SendBehavior::Broadcast, |buf| buf.extend_from_slice(body));
    let deadline = std::time::Instant::now() + Duration::from_millis(200);
    while std::time::Instant::now() < deadline {
        while sender.poll_with(|_| {}) {}
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn a_paused_connection_sits_out_broadcasts() {
    let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 24731));
    let mut sender = NetworkDriver::default();
    sender.listen_at(addr).expect("couldn't listen");

    let mut a = NetworkDriver::default();
    let mut b = NetworkDriver::default();
    a.connect(addr).expect("a: couldn't connect");
    b.connect(addr).expect("b: couldn't connect");

    // Both accepted connections are in the broadcast set to begin with.
    let mut accepted = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while accepted.len() < 2 && std::time::Instant::now() < deadline {
        sender.poll_with(|event| {
            if let PollEvent::Accept { stream, .. } = event {
                accepted.push(stream);
            }
        });
        a.poll_with(|_| {});
        b.poll_with(|_| {});
        thread::sleep(Duration::from_millis(1));
    }
    let [first, second] = accepted[..] else { panic!("expected two accepted connections") };

    let (mut got_a, mut got_b) = (Vec::new(), Vec::new());
    send(&mut sender, b"live-1");
    pump(&mut a, &mut got_a, Duration::from_millis(200));
    pump(&mut b, &mut got_b, Duration::from_millis(200));
    assert_eq!(got_a, vec![b"live-1".to_vec()]);
    assert_eq!(got_b, vec![b"live-1".to_vec()]);

    // The second connection is paused: the broadcast reaches only the first,
    // while a write addressed to the paused token still gets through.
    sender.pause_broadcast(second);
    assert!(sender.is_broadcast_paused(second));
    assert!(!sender.is_broadcast_paused(first));

    send(&mut sender, b"live-2");
    sender.write_or_enqueue_with(SendBehavior::Single(second), |buf| {
        buf.extend_from_slice(b"replay");
    });
    send(&mut sender, b"live-3");

    got_a.clear();
    got_b.clear();
    pump(&mut a, &mut got_a, Duration::from_millis(300));
    pump(&mut b, &mut got_b, Duration::from_millis(300));
    assert_eq!(got_a, vec![b"live-2".to_vec(), b"live-3".to_vec()]);
    assert_eq!(got_b, vec![b"replay".to_vec()]);

    // Resumed, it is back in the broadcast set and has missed only what was
    // sent while it was out.
    sender.resume_broadcast(second);
    assert!(!sender.is_broadcast_paused(second));

    send(&mut sender, b"live-4");
    got_a.clear();
    got_b.clear();
    pump(&mut a, &mut got_a, Duration::from_millis(300));
    pump(&mut b, &mut got_b, Duration::from_millis(300));
    assert_eq!(got_a, vec![b"live-4".to_vec()]);
    assert_eq!(got_b, vec![b"live-4".to_vec()]);
}

#[test]
fn a_paused_connection_is_left_out_of_the_reconnect_backlog() {
    let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 24733));
    let mut peer = NetworkDriver::default();
    peer.listen_at(addr).expect("couldn't listen");

    let mut sender = NetworkDriver::default().with_transport(Transport::Tcp(TcpConfig {
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    }));
    let outbound = sender.connect(addr).expect("couldn't connect");

    let mut got = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while got.is_empty() && std::time::Instant::now() < deadline {
        sender.poll_with(|_| {});
        pump(&mut peer, &mut got, Duration::from_millis(10));
        send(&mut sender, b"connected");
    }
    assert_eq!(got, vec![b"connected".to_vec()]);

    // Take the peer away: the outbound connection moves to the reconnect list,
    // where broadcasts are queued rather than written.
    drop(peer);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut disconnected = false;
    while !disconnected && std::time::Instant::now() < deadline {
        sender.poll_with(|event| {
            if matches!(event, PollEvent::Disconnect { .. }) {
                disconnected = true;
            }
        });
        sender.write_or_enqueue_with(SendBehavior::Broadcast, |buf| buf.extend_from_slice(b"probe"));
        thread::sleep(Duration::from_millis(1));
    }
    assert!(disconnected, "sender never noticed the peer going away");

    sender.pause_broadcast(outbound);
    send(&mut sender, b"while-paused");
    sender.resume_broadcast(outbound);
    send(&mut sender, b"after-resume");

    // Bring the peer back; the queued backlog replays on reconnect.
    let mut peer = NetworkDriver::default();
    peer.listen_at(addr).expect("couldn't re-listen");

    let mut replayed = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !replayed.contains(&b"after-resume".to_vec()) && std::time::Instant::now() < deadline {
        sender.poll_with(|_| {});
        pump(&mut peer, &mut replayed, Duration::from_millis(10));
    }
    assert!(replayed.contains(&b"after-resume".to_vec()), "backlog never replayed");
    assert!(!replayed.contains(&b"while-paused".to_vec()), "paused broadcast was queued");
}
