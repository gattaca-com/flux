use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    thread,
    time::Duration,
};

use flux_network::{Network, NetworkEvent, ReplayPolicy, TcpGroupConfig};

fn pump(conn: &mut Network, collect: &mut Vec<Vec<u8>>, for_how_long: Duration) {
    let deadline = std::time::Instant::now() + for_how_long;
    while std::time::Instant::now() < deadline {
        conn.poll_with(|event| {
            if let NetworkEvent::Message { payload, .. } = event {
                collect.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
}

fn send(sender: &mut Network, sender_group: flux_network::Group, body: &[u8]) {
    sender.broadcast_with(sender_group, |buf| buf.extend_from_slice(body));
    let deadline = std::time::Instant::now() + Duration::from_millis(200);
    while std::time::Instant::now() < deadline {
        while sender.poll_with(|_| {}) {}
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn a_paused_connection_sits_out_broadcasts() {
    let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 24731));
    let mut sender = Network::default();
    let sender_group = sender.add_group(TcpGroupConfig {
        aligned_payloads: true,
        replay: ReplayPolicy::Replay,
        ..Default::default()
    });
    sender.listen(sender_group, addr).expect("couldn't listen");

    let mut a = Network::default();
    let a_group = a.add_group(TcpGroupConfig {
        aligned_payloads: true,
        replay: ReplayPolicy::Replay,
        ..Default::default()
    });
    let mut b = Network::default();
    let b_group = b.add_group(TcpGroupConfig {
        aligned_payloads: true,
        replay: ReplayPolicy::Replay,
        ..Default::default()
    });
    let _ = a.connect(a_group, addr);
    let _ = b.connect(b_group, addr);

    // Both accepted connections are in the broadcast set to begin with.
    let mut accepted = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while accepted.len() < 2 && std::time::Instant::now() < deadline {
        sender.poll_with(|event| {
            if let NetworkEvent::Accepted { token: stream, .. } = event {
                accepted.push(stream);
            }
        });
        a.poll_with(|_| {});
        b.poll_with(|_| {});
        thread::sleep(Duration::from_millis(1));
    }
    let [first, second] = accepted[..] else { panic!("expected two accepted connections") };

    let (mut got_a, mut got_b) = (Vec::new(), Vec::new());
    send(&mut sender, sender_group, b"live-1");
    pump(&mut a, &mut got_a, Duration::from_millis(200));
    pump(&mut b, &mut got_b, Duration::from_millis(200));
    assert_eq!(got_a, vec![b"live-1".to_vec()]);
    assert_eq!(got_b, vec![b"live-1".to_vec()]);

    // The second connection is paused: the broadcast reaches only the first,
    // while a write addressed to the paused token still gets through.
    sender.pause_broadcast(second);
    assert!(sender.is_broadcast_paused(second));
    assert!(!sender.is_broadcast_paused(first));

    send(&mut sender, sender_group, b"live-2");
    sender.send_with(second, |buf| {
        buf.extend_from_slice(b"replay");
    });
    send(&mut sender, sender_group, b"live-3");

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

    send(&mut sender, sender_group, b"live-4");
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
    let mut peer = Network::default();
    let peer_group = peer.add_group(TcpGroupConfig {
        aligned_payloads: true,
        replay: ReplayPolicy::Replay,
        ..Default::default()
    });
    peer.listen(peer_group, addr).expect("couldn't listen");

    let mut sender = Network::default();
    let sender_group = sender.add_group(TcpGroupConfig {
        aligned_payloads: true,
        replay: ReplayPolicy::Replay,
        reconnect_interval: flux_timing::Duration::from_millis(1),
        ..Default::default()
    });
    let outbound = sender.connect(sender_group, addr);

    let mut got = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while got.is_empty() && std::time::Instant::now() < deadline {
        sender.poll_with(|_| {});
        pump(&mut peer, &mut got, Duration::from_millis(10));
        send(&mut sender, sender_group, b"connected");
    }
    assert_eq!(got, vec![b"connected".to_vec()]);

    // Take the peer away: the outbound connection moves to the reconnect list,
    // where broadcasts are queued rather than written.
    drop(peer);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut disconnected = false;
    while !disconnected && std::time::Instant::now() < deadline {
        sender.poll_with(|event| {
            if matches!(event, NetworkEvent::Disconnected { .. }) {
                disconnected = true;
            }
        });
        sender.broadcast_with(sender_group, |buf| buf.extend_from_slice(b"probe"));
        thread::sleep(Duration::from_millis(1));
    }
    assert!(disconnected, "sender never noticed the peer going away");

    sender.pause_broadcast(outbound);
    send(&mut sender, sender_group, b"while-paused");
    sender.resume_broadcast(outbound);
    send(&mut sender, sender_group, b"after-resume");

    // Bring the peer back; the queued backlog replays on reconnect.
    let mut peer = Network::default();
    let peer_group = peer.add_group(TcpGroupConfig {
        aligned_payloads: true,
        replay: ReplayPolicy::Replay,
        ..Default::default()
    });
    peer.listen(peer_group, addr).expect("couldn't re-listen");

    let mut replayed = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !replayed.contains(&b"after-resume".to_vec()) && std::time::Instant::now() < deadline {
        sender.poll_with(|_| {});
        pump(&mut peer, &mut replayed, Duration::from_millis(10));
    }
    assert!(replayed.contains(&b"after-resume".to_vec()), "backlog never replayed");
    assert!(!replayed.contains(&b"while-paused".to_vec()), "paused broadcast was queued");
}
