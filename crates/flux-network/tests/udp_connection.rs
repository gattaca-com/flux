use std::{
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use flux_network::{Network, NetworkEvent, UdpConfig, UdpGroupConfig, UdpGroupConfigV2};
use mio::Token;

fn free_addr() -> SocketAddr {
    UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap()
}

/// Handshake between a fresh listener and client, returning the accepted and
/// the outbound token. `dial` differs from `addr` when a relay sits between.
fn connect_via(
    server: &mut Network,
    server_group: flux_network::Group,
    client: &mut Network,
    client_group: flux_network::Group,
    addr: SocketAddr,
    dial: SocketAddr,
) -> (Token, Token) {
    server.listen(server_group, addr).unwrap();
    let client_token = client.connect(client_group, dial);
    let mut accepted = None;
    let mut connected = 0;
    let mut on_client = |event: NetworkEvent<'_>| {
        if let NetworkEvent::Connected { token, .. } = event {
            assert_eq!(token, client_token);
            connected += 1;
        }
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted.is_none() {
        assert!(Instant::now() < deadline, "no accept");
        server.poll_with(|e| {
            if let NetworkEvent::Accepted { token: stream, .. } = e {
                accepted = Some(stream);
            }
        });
        client.poll_with(&mut on_client);
        thread::sleep(Duration::from_micros(50));
    }
    while client.currently_disconnected().count() != 0 {
        assert!(Instant::now() < deadline, "handshake");
        server.poll_with(|_| {});
        client.poll_with(&mut on_client);
        thread::sleep(Duration::from_micros(50));
    }
    // A hello retry may still be in flight if the ack took longer than one
    // RTO. Let it land now, while the peer it belongs to still exists.
    for _ in 0..5 {
        server.poll_with(|_| {});
        client.poll_with(&mut on_client);
    }
    assert_eq!(connected, 1, "initial handshake must emit Connected exactly once");
    (accepted.unwrap(), client_token)
}

fn connect_pair(
    server: &mut Network,
    server_group: flux_network::Group,
    client: &mut Network,
    client_group: flux_network::Group,
    addr: SocketAddr,
) -> (Token, Token) {
    connect_via(server, server_group, client, client_group, addr, addr)
}

fn checksum(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| (h ^ u64::from(*b)).wrapping_mul(0x100_0000_01b3))
}

/// Test message: 4-byte id, then `len` bytes derived from the id.
fn make_msg(id: u32, len: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + len);
    v.extend_from_slice(&id.to_le_bytes());
    v.extend((0..len).map(|i| (id as usize).wrapping_mul(31).wrapping_add(i * 7) as u8));
    v
}

fn msg_id(payload: &[u8]) -> u32 {
    u32::from_le_bytes(payload[..4].try_into().unwrap())
}

#[test]
fn udp_roundtrip() {
    let addr = free_addr();
    let mut server = Network::default();
    let server_group =
        server.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    server.listen(server_group, addr).unwrap();
    let mut client = Network::default();
    let client_group =
        client.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let tok = client.connect(client_group, addr);
    assert!(!client.send_with(tok, |b| b.extend_from_slice(b"early")), "sent before connecting");
    let mut accepted = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while client.currently_disconnected().count() != 0 {
        assert!(Instant::now() < deadline, "handshake");
        server.poll_with(|e| {
            if let NetworkEvent::Accepted { token, .. } = e {
                accepted = Some(token);
            }
        });
        client.poll_with(|_| {});
    }
    assert!(client.send_with(tok, |b| b.extend_from_slice(b"ping")));

    let mut request_seen = false;
    let mut reply_seen = false;
    while !reply_seen {
        assert!(Instant::now() < deadline, "roundtrip timed out");
        server.poll_with(|e| match e {
            NetworkEvent::Accepted { token: stream, .. } => accepted = Some(stream),
            NetworkEvent::Message { token, payload, .. } => {
                assert_eq!(Some(token), accepted);
                assert_eq!(payload, b"ping");
                request_seen = true;
            }
            _ => {}
        });
        if request_seen && !reply_seen {
            server.send_with(accepted.unwrap(), |b| {
                b.extend_from_slice(b"pong");
            });
            request_seen = false;
        }
        client.poll_with(|e| {
            if let NetworkEvent::Message { token, payload, .. } = e {
                assert_eq!(token, tok);
                assert_eq!(payload, b"pong");
                reply_seen = true;
            }
        });
        thread::sleep(Duration::from_micros(50));
    }
}

/// The migration path: one network serves v1 and v2 peers side by side.
#[test]
fn udp_v1_and_v2_groups_run_side_by_side() {
    let mut server = Network::default();
    let v1 = server.add_group(UdpGroupConfig { udp: UdpConfig::lan(), ..Default::default() });
    let v2 = server.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let mut old = Network::default();
    let old_group = old.add_group(UdpGroupConfig { udp: UdpConfig::lan(), ..Default::default() });
    let mut new = Network::default();
    let new_group = new.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let (_, old_token) = connect_pair(&mut server, v1, &mut old, old_group, free_addr());
    let (_, new_token) = connect_pair(&mut server, v2, &mut new, new_group, free_addr());
    assert!(old.send_with(old_token, |b| b.extend_from_slice(b"v1")));
    assert!(new.send_with(new_token, |b| b.extend_from_slice(b"v2")));

    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while got.len() < 2 {
        assert!(Instant::now() < deadline, "got only {got:?}");
        old.poll_with(|_| {});
        new.poll_with(|_| {});
        server.poll_with(|e| {
            if let NetworkEvent::Message { group, payload, .. } = e {
                got.push((group, payload.to_vec()));
            }
        });
    }
    got.sort_by_key(|(group, _)| *group == v2);
    assert_eq!(got, [(v1, b"v1".to_vec()), (v2, b"v2".to_vec())]);
}

#[test]
fn udp_broadcast_mixed_sizes_to_two_subscribers() {
    let addr = free_addr();
    let mut server = Network::default();
    let server_group =
        server.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    server.listen(server_group, addr).unwrap();
    let mut a = Network::default();
    let a_group = a.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let mut b = Network::default();
    let b_group = b.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let _ = a.connect(a_group, addr);
    let _ = b.connect(b_group, addr);
    let mut accepted = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted < 2 {
        assert!(Instant::now() < deadline, "accepts");
        server.poll_with(|e| {
            if let NetworkEvent::Accepted { .. } = e {
                accepted += 1;
            }
        });
        a.poll_with(|_| {});
        b.poll_with(|_| {});
    }

    // 1 byte, one datagram, one stride exactly, and a 2 MiB message.
    let sizes = [1usize, 100, 1171, 1172, 5000, 2 * 1024 * 1024];
    let msgs: Vec<Vec<u8>> =
        sizes.iter().enumerate().map(|(i, s)| make_msg(i as u32, *s)).collect();
    for m in &msgs {
        server.broadcast_with(server_group, |buf| buf.extend_from_slice(m));
    }

    let expected: Vec<u64> = msgs.iter().map(|m| checksum(m)).collect();
    let mut got_a = Vec::new();
    let mut got_b = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while got_a.len() < msgs.len() || got_b.len() < msgs.len() {
        assert!(Instant::now() < deadline, "broadcast delivery");
        server.poll_with(|_| {});
        a.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                got_a.push((msg_id(payload), checksum(payload)));
            }
        });
        b.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                got_b.push((msg_id(payload), checksum(payload)));
            }
        });
    }
    for got in [got_a, got_b] {
        assert_eq!(got.len(), msgs.len());
        for (id, sum) in got {
            assert_eq!(sum, expected[id as usize], "message {id} corrupted");
        }
    }
}

/// Forwards datagrams between one client and the server, dropping every
/// `drop_every`-th datagram in each direction.
struct LossyRelay {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
    handle: Option<thread::JoinHandle<()>>,
}

impl LossyRelay {
    fn start(server: SocketAddr, drop_every: usize) -> Self {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        socket.set_read_timeout(Some(Duration::from_millis(5))).unwrap();
        let addr = socket.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicUsize::new(0));
        let (stop_c, dropped_c) = (stop.clone(), dropped.clone());
        let handle = thread::spawn(move || {
            let mut buf = vec![0u8; 65_536];
            let mut client: Option<SocketAddr> = None;
            let mut count = 0usize;
            while !stop_c.load(Ordering::Relaxed) {
                let Ok((n, from)) = socket.recv_from(&mut buf) else { continue };
                let to = if from == server {
                    let Some(c) = client else { continue };
                    c
                } else {
                    client = Some(from);
                    server
                };
                count += 1;
                if count.is_multiple_of(drop_every) {
                    dropped_c.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let _ = socket.send_to(&buf[..n], to);
            }
        });
        Self { addr, stop, dropped, handle: Some(handle) }
    }
}

impl Drop for LossyRelay {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.take().unwrap().join().unwrap();
    }
}

#[test]
fn udp_delivers_everything_exactly_once_under_loss() {
    const N: u32 = 400;
    let server_addr = free_addr();
    let relay = LossyRelay::start(server_addr, 7);

    let mut server = Network::default();
    let server_group =
        server.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let mut client = Network::default();
    let client_group =
        client.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let (accepted, _) =
        connect_via(&mut server, server_group, &mut client, client_group, server_addr, relay.addr);

    // Both directions at once: server pushes to the client, client replies.
    let msgs: Vec<Vec<u8>> = (0..N).map(|i| make_msg(i, 1 + (i as usize * 613) % 4000)).collect();
    for m in &msgs {
        server.send_with(accepted, |b| b.extend_from_slice(m));
    }

    let mut seen = vec![false; N as usize];
    let mut received = 0;
    let mut echoed_back = 0;
    let deadline = Instant::now() + Duration::from_secs(20);
    while received < N || echoed_back < N {
        assert!(Instant::now() < deadline, "loss recovery: {received} rx, {echoed_back} echoed");
        let mut echo = Vec::new();
        client.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                let id = msg_id(payload) as usize;
                assert_eq!(checksum(payload), checksum(&msgs[id]), "message {id} corrupted");
                assert!(!seen[id], "message {id} delivered twice");
                seen[id] = true;
                received += 1;
                echo.push(id as u32);
            }
        });
        for id in echo {
            client.broadcast_with(client_group, |b| {
                b.extend_from_slice(&id.to_le_bytes());
            });
        }
        server.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                assert_eq!(payload.len(), 4);
                echoed_back += 1;
            }
        });
        thread::sleep(Duration::from_micros(20));
    }
    assert!(relay.dropped.load(Ordering::Relaxed) > 0, "relay dropped nothing");
}

#[test]
fn udp_client_disconnect_is_a_new_session() {
    let addr = free_addr();
    let mut server = Network::default();
    let server_group = server.add_group(UdpGroupConfigV2 {
        udp: UdpConfig::lan(),
        on_connect_msg: Some(b"welcome".to_vec()),
        ..Default::default()
    });
    let mut client = Network::default();
    let client_group =
        client.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let (first, tok) = connect_pair(&mut server, server_group, &mut client, client_group, addr);

    client.disconnect(tok);
    assert_eq!(client.currently_disconnected().count(), 1);

    let mut events = Vec::new();
    let mut payload_on = None;
    let mut reconnected = 0;
    let mut greeting = false;
    let mut sent = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while payload_on.is_none() || reconnected == 0 || !greeting {
        if !sent && reconnected == 1 {
            sent = client.send_with(tok, |b| b.extend_from_slice(b"after"));
        }
        assert!(Instant::now() < deadline, "reconnect");
        server.poll_with(|e| match e {
            NetworkEvent::Disconnected { token, .. } => events.push(("disconnect", token)),
            NetworkEvent::Accepted { token: stream, .. } => events.push(("accept", stream)),
            NetworkEvent::Message { token, payload, .. } => {
                assert_eq!(payload, b"after");
                payload_on = Some(token);
            }
            NetworkEvent::Connected { .. } => unreachable!(),
        });
        client.poll_with(|e| match e {
            NetworkEvent::Connected { token, .. } => {
                assert_eq!(token, tok);
                reconnected += 1;
                assert_eq!(reconnected, 1);
            }
            NetworkEvent::Message { payload, .. } => {
                assert_eq!(reconnected, 1, "Connected must precede session messages");
                assert_eq!(payload, b"welcome");
                greeting = true;
            }
            _ => {}
        });
        thread::sleep(Duration::from_micros(50));
    }
    assert_eq!(events[0], ("disconnect", first));
    assert_eq!(events[1].0, "accept");
    assert_ne!(events[1].1, first);
    assert_eq!(payload_on, Some(events[1].1), "sent on the new session");
    assert_eq!(client.currently_disconnected().count(), 0);
}

#[test]
fn udp_sends_while_reconnecting_are_refused() {
    let addr = free_addr();
    let mut server = Network::default();
    let server_group =
        server.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let mut client = Network::default();
    let client_group =
        client.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let (_, tok) = connect_pair(&mut server, server_group, &mut client, client_group, addr);

    client.disconnect(tok);
    assert!(!client.send_with(tok, |b| b.extend_from_slice(b"lost")));
    let mut reconnected = false;
    let mut got = 0;
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        server.poll_with(|e| {
            if let NetworkEvent::Message { .. } = e {
                got += 1;
            }
        });
        client.poll_with(|e| {
            if let NetworkEvent::Connected { .. } = e {
                reconnected = true;
            }
        });
    }
    assert!(reconnected);
    assert_eq!(got, 0);
}

#[test]
fn udp_server_restart_reconnects_client() {
    let addr = free_addr();
    let mut client = Network::default();
    let client_group = client.add_group(UdpGroupConfigV2 {
        udp: UdpConfig::lan(),
        peer_timeout: flux_timing::Duration::from_millis(2_000),
        ..Default::default()
    });
    let tok;
    {
        let mut server = Network::default();
        let server_group =
            server.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
        let (_, t) = connect_pair(&mut server, server_group, &mut client, client_group, addr);
        tok = t;
    }
    // Old server gone. A new one on the same port must be rejoined without
    // waiting for the 2s peer timeout: its reset acks trigger renegotiation.
    let mut server = Network::default();
    let server_group =
        server.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    server.listen(server_group, addr).unwrap();
    let start = Instant::now();
    let mut disconnected = false;
    let mut reconnected = false;
    let mut accepted = false;
    while !(disconnected && reconnected && accepted) {
        assert!(start.elapsed() < Duration::from_secs(5), "server restart recovery");
        client.poll_with(|e| match e {
            NetworkEvent::Disconnected { token, .. } => {
                assert_eq!(token, tok);
                disconnected = true;
            }
            NetworkEvent::Connected { token, .. } => {
                assert_eq!(token, tok);
                reconnected = true;
            }
            _ => {}
        });
        server.poll_with(|e| {
            if let NetworkEvent::Accepted { .. } = e {
                accepted = true;
            }
        });
        thread::sleep(Duration::from_micros(50));
    }
    assert!(start.elapsed() < Duration::from_millis(1500), "took the slow timeout path");
}

#[test]
fn udp_peer_timeout_disconnects_silent_client() {
    let addr = free_addr();
    let mut server = Network::default();
    let server_group = server.add_group(UdpGroupConfigV2 {
        udp: UdpConfig::lan(),
        peer_timeout: flux_timing::Duration::from_millis(300),
        ..Default::default()
    });
    let mut client = Network::default();
    let client_group =
        client.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let (accepted, _) = connect_pair(&mut server, server_group, &mut client, client_group, addr);
    drop(client);

    let mut disconnected = None;
    let start = Instant::now();
    while disconnected.is_none() {
        assert!(start.elapsed() < Duration::from_secs(5), "peer timeout");
        server.poll_with(|e| {
            if let NetworkEvent::Disconnected { token, .. } = e {
                disconnected = Some(token);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(disconnected, Some(accepted));
    assert!(start.elapsed() >= Duration::from_millis(250));
}

#[test]
fn udp_ignores_junk_datagrams() {
    let addr = free_addr();
    let mut server = Network::default();
    let server_group =
        server.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let mut client = Network::default();
    let client_group =
        client.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let (accepted, _) = connect_pair(&mut server, server_group, &mut client, client_group, addr);
    let junk = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    junk.send_to(b"not flux", addr).unwrap();
    junk.send_to(&[0xFF; 1200], addr).unwrap();
    junk.send_to(&[0; 2000], addr).unwrap();
    server.send_with(accepted, |b| b.extend_from_slice(b"ok"));
    let mut got = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !got {
        assert!(Instant::now() < deadline, "junk tolerance");
        server.poll_with(|e| assert!(!matches!(e, NetworkEvent::Accepted { .. })));
        client.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                assert_eq!(payload, b"ok");
                got = true;
            }
        });
    }
}

/// A peer dropped mid-broadcast must not take the shared payload with it.
#[test]
fn udp_broadcast_survives_dropping_a_peer_mid_way() {
    let addr = free_addr();
    let mut server = Network::default();
    let server_group = server.add_group(UdpGroupConfigV2 {
        udp: UdpConfig { send_window: 64, max_message_size: 8192, ..UdpConfig::lan() },
        ..Default::default()
    });
    server.listen(server_group, addr).unwrap();
    let mut stalled = Network::default();
    let stalled_group =
        stalled.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let mut live = Network::default();
    let live_group =
        live.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let _ = stalled.connect(stalled_group, addr);
    let _ = live.connect(live_group, addr);
    let mut accepted = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted < 2 || live.currently_disconnected().count() != 0 {
        assert!(Instant::now() < deadline, "accepts");
        server.poll_with(|e| accepted += usize::from(matches!(e, NetworkEvent::Accepted { .. })));
        stalled.poll_with(|_| {});
        live.poll_with(|_| {});
    }
    // `stalled` stops polling: its send window fills while `live` keeps
    // consuming.
    let msgs: Vec<Vec<u8>> = (0..12).map(|i| make_msg(i, 8000)).collect();
    let mut got = Vec::new();
    let mut dropped = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    for m in &msgs {
        server.broadcast_with(server_group, |b| b.extend_from_slice(m));
        let until = Instant::now() + Duration::from_millis(20);
        while Instant::now() < until {
            server.poll_with(|e| {
                dropped += usize::from(matches!(e, NetworkEvent::Disconnected { .. }));
            });
            live.poll_with(|e| {
                if let NetworkEvent::Message { payload, .. } = e {
                    got.push((msg_id(payload), checksum(payload)));
                }
            });
        }
    }
    while got.len() < msgs.len() {
        assert!(Instant::now() < deadline, "live peer delivery: got {}", got.len());
        server.poll_with(|_| {});
        live.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                got.push((msg_id(payload), checksum(payload)));
            }
        });
    }
    assert_eq!(dropped, 1, "the stalled peer is dropped exactly once");
    for (id, sum) in got {
        assert_eq!(sum, checksum(&msgs[id as usize]), "message {id} corrupted");
    }
}

/// A server that drops an accepted peer makes the client renegotiate.
#[test]
fn udp_server_disconnect_reconnects_client() {
    let addr = free_addr();
    let mut server = Network::default();
    let server_group = server.add_group(UdpGroupConfigV2 {
        udp: UdpConfig::lan(),
        peer_timeout: flux_timing::Duration::from_millis(5_000),
        ..Default::default()
    });
    let mut client = Network::default();
    let client_group = client.add_group(UdpGroupConfigV2 {
        udp: UdpConfig::lan(),
        peer_timeout: flux_timing::Duration::from_millis(5_000),
        ..Default::default()
    });
    let (accepted, tok) = connect_pair(&mut server, server_group, &mut client, client_group, addr);
    server.disconnect(accepted);
    // Client traffic hits a listener with no session for it and gets reset.
    client.send_with(tok, |b| b.extend_from_slice(b"x"));

    let start = Instant::now();
    let (mut disconnected, mut reconnected, mut reaccepted) = (false, false, false);
    while !(disconnected && reconnected && reaccepted) {
        assert!(start.elapsed() < Duration::from_secs(5), "server-side disconnect recovery");
        client.poll_with(|e| match e {
            NetworkEvent::Disconnected { token, .. } => {
                assert_eq!(token, tok);
                disconnected = true;
            }
            NetworkEvent::Connected { token, .. } => {
                assert_eq!(token, tok);
                reconnected = true;
            }
            _ => {}
        });
        server.poll_with(|e| reaccepted |= matches!(e, NetworkEvent::Accepted { .. }));
        thread::sleep(Duration::from_micros(50));
    }
    assert!(start.elapsed() < Duration::from_secs(1), "did not wait for the peer timeout");
}

/// A message that cannot fit the send window drops the peer instead of
/// vanishing silently.
#[test]
fn udp_window_exhaustion_disconnects_instead_of_dropping() {
    let addr = free_addr();
    let config = UdpConfig { send_window: 64, max_message_size: 4096, ..UdpConfig::lan() };
    let mut server = Network::default();
    let server_group = server.add_group(UdpGroupConfigV2 { udp: config, ..Default::default() });
    let mut client = Network::default();
    let client_group = client.add_group(UdpGroupConfigV2 { udp: config, ..Default::default() });
    let (accepted, _) = connect_pair(&mut server, server_group, &mut client, client_group, addr);
    // Client never polls: nothing is acked and the window fills.
    let mut disconnected = None;
    for _ in 0..70 {
        server.send_with(accepted, |b| b.extend_from_slice(b"m"));
        server.poll_with(|e| {
            if let NetworkEvent::Disconnected { token, .. } = e {
                disconnected = Some(token);
            }
        });
    }
    assert_eq!(disconnected, Some(accepted));
    let _ = &client;
}

/// Unreliable mode under loss: every message that arrives is intact and
/// unique, delivery keeps going past lost fragments, and the receiver only
/// speaks when a heartbeat is due.
#[test]
fn udp_unreliable_keeps_streaming_through_loss() {
    const N: u32 = 600;
    let config = UdpConfig {
        reliable: false,
        send_window: 64,
        recv_window: 64,
        max_message_size: 4 * 1171,
        ..UdpConfig::lan()
    };
    let server_addr = free_addr();
    let relay = LossyRelay::start(server_addr, 5);

    let mut server = Network::default();
    let server_group = server.add_group(UdpGroupConfigV2 { udp: config, ..Default::default() });
    let mut client = Network::default();
    let client_group = client.add_group(UdpGroupConfigV2 { udp: config, ..Default::default() });
    let (accepted, _) =
        connect_via(&mut server, server_group, &mut client, client_group, server_addr, relay.addr);

    // Mostly two-fragment messages: losing either fragment loses the message.
    let stride = config.max_datagram_size - 29;
    let msgs: Vec<Vec<u8>> =
        (0..N).map(|i| make_msg(i, 1 + (i as usize * 613) % (2 * stride))).collect();
    let mut seen = vec![false; N as usize];
    let mut received = 0usize;
    let mut last = 0usize;
    for m in &msgs {
        server.send_with(accepted, |b| b.extend_from_slice(m));
        // Paced so the relay's socket buffer is not a second source of loss.
        // Draining as we go still leaves bursts past a lost hole in the
        // 64-datagram receive window, which is what slides it.
        thread::sleep(Duration::from_micros(50));
        client.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                let id = msg_id(payload) as usize;
                assert_eq!(checksum(payload), checksum(&msgs[id]), "message {id} corrupted");
                assert!(!seen[id], "message {id} delivered twice");
                seen[id] = true;
                received += 1;
                last = last.max(id);
            }
        });
        server.poll_with(|_| {});
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        client.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                let id = msg_id(payload) as usize;
                assert!(!seen[id], "message {id} delivered twice");
                seen[id] = true;
                received += 1;
                last = last.max(id);
            }
        });
        server.poll_with(|_| {});
        thread::sleep(Duration::from_micros(50));
    }
    let dropped = relay.dropped.load(Ordering::Relaxed);
    assert!(dropped > 0, "relay dropped nothing");
    assert!(received < N as usize, "loss must lose messages: {received}/{N}");
    assert!(received > N as usize / 2, "too few delivered: {received}/{N}");
    assert!(last > N as usize - 64, "delivery stalled at {last}");
    assert_eq!(client.currently_disconnected().count(), 0, "client dropped its peer");
}

/// Resetting an outbound peer while it connects does not send another
/// greeting: the remote sees the on-connect message once.
#[test]
fn udp_resets_while_down_send_one_greeting() {
    let addr = free_addr();
    let mut client = Network::default();
    let client_group = client.add_group(UdpGroupConfigV2 {
        udp: UdpConfig::lan(),
        on_connect_msg: Some(b"hello".to_vec()),
        ..Default::default()
    });
    let tok = client.connect(client_group, addr);
    for _ in 0..3 {
        client.poll_with(|_| {});
        client.disconnect(tok);
    }

    let mut server = Network::default();
    let server_group =
        server.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    server.listen(server_group, addr).unwrap();
    let mut got = Vec::new();
    let mut sent = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !got.contains(&b"after".to_vec()) {
        assert!(Instant::now() < deadline, "delivery");
        client.poll_with(|_| {});
        if !sent && client.currently_disconnected().count() == 0 {
            sent = client.send_with(tok, |b| b.extend_from_slice(b"after"));
        }
        server.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                got.push(payload.to_vec());
            }
        });
        thread::sleep(Duration::from_micros(50));
    }
    // Let anything still in flight land, then count greetings.
    let settle = Instant::now() + Duration::from_millis(200);
    while Instant::now() < settle {
        client.poll_with(|_| {});
        server.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                got.push(payload.to_vec());
            }
        });
    }
    assert_eq!(got.iter().filter(|m| m.as_slice() == b"hello").count(), 1, "{got:?}");
    assert_eq!(got.len(), 2, "{got:?}");
}

fn lan_pair(
    addr: SocketAddr,
) -> (Network, flux_network::Group, Network, flux_network::Group, Token, Token) {
    let mut server = Network::default();
    let server_group =
        server.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let mut client = Network::default();
    let client_group =
        client.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let (accepted, tok) = connect_pair(&mut server, server_group, &mut client, client_group, addr);
    (server, server_group, client, client_group, accepted, tok)
}

/// A graceful close waits for the last ack, refuses sends meanwhile, and
/// reports the session gone afterwards, like its TCP counterpart.
#[test]
fn udp_disconnect_when_drained_waits_for_acks() {
    let (mut server, _, mut client, _, accepted, _) = lan_pair(free_addr());
    let msg = make_msg(1, 64 * 1024);
    assert!(server.send_with(accepted, |b| b.extend_from_slice(&msg)));
    assert!(server.disconnect_when_drained(accepted));
    assert!(!server.send_with(accepted, |b| b.extend_from_slice(b"refused")));

    // The client is not polling, so nothing is acked and the session stays.
    let hold = Instant::now() + Duration::from_millis(100);
    while Instant::now() < hold {
        server.poll_with(|e| assert!(!matches!(e, NetworkEvent::Disconnected { .. })));
    }

    let mut got = false;
    let mut disconnected = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while disconnected.is_none() {
        assert!(Instant::now() < deadline, "drained close");
        client.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                assert_eq!(checksum(payload), checksum(&msg));
                got = true;
            }
        });
        server.poll_with(|e| {
            if let NetworkEvent::Disconnected { token, .. } = e {
                disconnected = Some(token);
            }
        });
        thread::sleep(Duration::from_micros(50));
    }
    assert!(got, "the queued message was delivered before the close");
    assert_eq!(disconnected, Some(accepted));
}

/// A listener token is not a session: `disconnect` leaves it alone, while
/// `remove` closes it and every session accepted on it.
#[test]
fn udp_listener_ignores_disconnect_and_remove_closes_its_sessions() {
    let addr = free_addr();
    let mut server = Network::default();
    let server_group =
        server.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let listener = server.listen(server_group, addr).unwrap();
    let mut client = Network::default();
    let client_group =
        client.add_group(UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() });
    let tok = client.connect(client_group, addr);
    let mut accepted = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted.is_none() || client.currently_disconnected().count() != 0 {
        assert!(Instant::now() < deadline, "handshake");
        server.poll_with(|e| {
            if let NetworkEvent::Accepted { token, .. } = e {
                accepted = Some(token);
            }
        });
        client.poll_with(|_| {});
        thread::sleep(Duration::from_micros(50));
    }
    let accepted = accepted.unwrap();

    assert!(!server.disconnect(listener), "a listener has no session to drop");
    assert!(server.send_with(accepted, |b| b.extend_from_slice(b"still-up")));
    let mut got = false;
    while !got {
        assert!(Instant::now() < deadline, "session survives listener disconnect");
        server.poll_with(|_| {});
        client
            .poll_with(|e| got |= matches!(e, NetworkEvent::Message { payload: b"still-up", .. }));
        thread::sleep(Duration::from_micros(50));
    }

    assert!(server.remove(listener));
    let mut disconnected = None;
    while disconnected.is_none() {
        assert!(Instant::now() < deadline, "sessions close with their listener");
        server.poll_with(|e| {
            if let NetworkEvent::Disconnected { token, .. } = e {
                disconnected = Some(token);
            }
        });
    }
    assert_eq!(disconnected, Some(accepted));
    assert!(!server.send_with(accepted, |b| b.extend_from_slice(b"gone")));
    assert!(!server.remove(listener), "already removed");
    let _ = tok;
}

/// Batched sends queue every item before one flush and lose none.
#[test]
fn udp_batched_sends_deliver_every_item() {
    let (mut server, server_group, mut client, _, accepted, _) = lan_pair(free_addr());
    assert!(
        server.send_many_with(accepted, 0..40u32, |b, i| b.extend_from_slice(&make_msg(i, 700)))
    );
    assert_eq!(
        server.broadcast_many_with(server_group, 40..80u32, |b, i| {
            b.extend_from_slice(&make_msg(i, 700));
        }),
        1
    );
    let mut seen = [false; 80];
    let mut count = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while count < 80 {
        assert!(Instant::now() < deadline, "batched delivery: {count}");
        server.poll_with(|_| {});
        client.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                let id = msg_id(payload) as usize;
                assert!(!seen[id], "message {id} delivered twice");
                seen[id] = true;
                count += 1;
            }
        });
        thread::sleep(Duration::from_micros(50));
    }
}
