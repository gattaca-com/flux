use std::{
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use flux_network::{NetworkDriver, PollEvent, SendBehavior, Transport, UdpConfig};
use mio::Token;

fn free_addr() -> SocketAddr {
    UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap()
}

fn udp(config: UdpConfig) -> NetworkDriver {
    NetworkDriver::default().with_transport(Transport::Udp(config))
}

/// Handshake between a fresh listener and client, returning the accepted and
/// the outbound token. `dial` differs from `addr` when a relay sits between.
fn connect_via(
    server: &mut NetworkDriver,
    client: &mut NetworkDriver,
    addr: SocketAddr,
    dial: SocketAddr,
) -> (Token, Token) {
    server.listen_at(addr).unwrap();
    let client_token = client.connect(dial).unwrap();
    let mut accepted = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted.is_none() {
        assert!(Instant::now() < deadline, "no accept");
        server.poll_with(|e| {
            if let PollEvent::Accept { stream, .. } = e {
                accepted = Some(stream);
            }
        });
        client.poll_with(|_| {});
        thread::sleep(Duration::from_micros(50));
    }
    while client.currently_disconnected().count() != 0 {
        assert!(Instant::now() < deadline, "handshake");
        server.poll_with(|_| {});
        client.poll_with(|_| {});
        thread::sleep(Duration::from_micros(50));
    }
    (accepted.unwrap(), client_token)
}

fn connect_pair(
    server: &mut NetworkDriver,
    client: &mut NetworkDriver,
    addr: SocketAddr,
) -> (Token, Token) {
    connect_via(server, client, addr, addr)
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
fn udp_roundtrip_before_handshake_completes() {
    let addr = free_addr();
    let mut server = udp(UdpConfig::lan());
    server.listen_at(addr).unwrap();
    let mut client = udp(UdpConfig::lan());
    let tok = client.connect(addr).unwrap();
    // Queued before the hello ack arrives; must go out once it does.
    client.write_or_enqueue_with(SendBehavior::Single(tok), |b| b.extend_from_slice(b"ping"));

    let mut accepted = None;
    let mut request_seen = false;
    let mut reply_seen = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !reply_seen {
        assert!(Instant::now() < deadline, "roundtrip timed out");
        server.poll_with(|e| match e {
            PollEvent::Accept { stream, .. } => accepted = Some(stream),
            PollEvent::Message { token, payload, .. } => {
                assert_eq!(Some(token), accepted);
                assert_eq!(payload, b"ping");
                request_seen = true;
            }
            _ => {}
        });
        if request_seen && !reply_seen {
            server.write_or_enqueue_with(SendBehavior::Single(accepted.unwrap()), |b| {
                b.extend_from_slice(b"pong");
            });
            request_seen = false;
        }
        client.poll_with(|e| {
            if let PollEvent::Message { token, payload, .. } = e {
                assert_eq!(token, tok);
                assert_eq!(payload, b"pong");
                reply_seen = true;
            }
        });
        thread::sleep(Duration::from_micros(50));
    }
}

#[test]
fn udp_broadcast_mixed_sizes_to_two_subscribers() {
    let addr = free_addr();
    let mut server = udp(UdpConfig::lan());
    server.listen_at(addr).unwrap();
    let mut a = udp(UdpConfig::lan());
    let mut b = udp(UdpConfig::lan());
    a.connect(addr).unwrap();
    b.connect(addr).unwrap();
    let mut accepted = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted < 2 {
        assert!(Instant::now() < deadline, "accepts");
        server.poll_with(|e| {
            if let PollEvent::Accept { .. } = e {
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
        server.write_or_enqueue_with(SendBehavior::Broadcast, |buf| buf.extend_from_slice(m));
    }

    let expected: Vec<u64> = msgs.iter().map(|m| checksum(m)).collect();
    let mut got_a = Vec::new();
    let mut got_b = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while got_a.len() < msgs.len() || got_b.len() < msgs.len() {
        assert!(Instant::now() < deadline, "broadcast delivery");
        server.poll_with(|_| {});
        a.poll_with(|e| {
            if let PollEvent::Message { payload, .. } = e {
                got_a.push((msg_id(payload), checksum(payload)));
            }
        });
        b.poll_with(|e| {
            if let PollEvent::Message { payload, .. } = e {
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

    let mut server = udp(UdpConfig::lan());
    let mut client = udp(UdpConfig::lan());
    let (accepted, _) = connect_via(&mut server, &mut client, server_addr, relay.addr);

    // Both directions at once: server pushes to the client, client replies.
    let msgs: Vec<Vec<u8>> = (0..N).map(|i| make_msg(i, 1 + (i as usize * 613) % 4000)).collect();
    for m in &msgs {
        server.write_or_enqueue_with(SendBehavior::Single(accepted), |b| b.extend_from_slice(m));
    }

    let mut seen = vec![false; N as usize];
    let mut received = 0;
    let mut echoed_back = 0;
    let deadline = Instant::now() + Duration::from_secs(20);
    while received < N || echoed_back < N {
        assert!(Instant::now() < deadline, "loss recovery: {received} rx, {echoed_back} echoed");
        let mut echo = Vec::new();
        client.poll_with(|e| {
            if let PollEvent::Message { payload, .. } = e {
                let id = msg_id(payload) as usize;
                assert_eq!(checksum(payload), checksum(&msgs[id]), "message {id} corrupted");
                assert!(!seen[id], "message {id} delivered twice");
                seen[id] = true;
                received += 1;
                echo.push(id as u32);
            }
        });
        for id in echo {
            client.write_or_enqueue_with(SendBehavior::Broadcast, |b| {
                b.extend_from_slice(&id.to_le_bytes());
            });
        }
        server.poll_with(|e| {
            if let PollEvent::Message { payload, .. } = e {
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
    let mut server = udp(UdpConfig::lan());
    let mut client = udp(UdpConfig::lan());
    let (first, tok) = connect_pair(&mut server, &mut client, addr);

    client.disconnect(tok);
    assert_eq!(client.currently_disconnected().count(), 1);
    client.write_or_enqueue_with(SendBehavior::Single(tok), |b| b.extend_from_slice(b"after"));

    let mut events = Vec::new();
    let mut payload_on = None;
    let mut reconnected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while payload_on.is_none() || !reconnected {
        assert!(Instant::now() < deadline, "reconnect");
        server.poll_with(|e| match e {
            PollEvent::Disconnect { token } => events.push(("disconnect", token)),
            PollEvent::Accept { stream, .. } => events.push(("accept", stream)),
            PollEvent::Message { token, payload, .. } => {
                assert_eq!(payload, b"after");
                payload_on = Some(token);
            }
            PollEvent::Reconnect { .. } => unreachable!(),
        });
        client.poll_with(|e| {
            if let PollEvent::Reconnect { token } = e {
                assert_eq!(token, tok);
                reconnected = true;
            }
        });
        thread::sleep(Duration::from_micros(50));
    }
    assert_eq!(events[0], ("disconnect", first));
    assert_eq!(events[1].0, "accept");
    assert_ne!(events[1].1, first);
    assert_eq!(payload_on, Some(events[1].1), "backlog replayed on the new session");
    assert_eq!(client.currently_disconnected().count(), 0);
}

#[test]
fn udp_drop_backlog_on_disconnect_discards_queued() {
    let addr = free_addr();
    let mut server = udp(UdpConfig::lan());
    let mut client = udp(UdpConfig::lan()).with_drop_outbound_backlog_on_disconnect(true);
    let (_, tok) = connect_pair(&mut server, &mut client, addr);

    client.disconnect(tok);
    client.write_or_enqueue_with(SendBehavior::Single(tok), |b| b.extend_from_slice(b"lost"));
    let mut reconnected = false;
    let mut got = 0;
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        server.poll_with(|e| {
            if let PollEvent::Message { .. } = e {
                got += 1;
            }
        });
        client.poll_with(|e| {
            if let PollEvent::Reconnect { .. } = e {
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
    let mut client = udp(UdpConfig::lan()).with_user_timeout(2_000);
    let tok;
    {
        let mut server = udp(UdpConfig::lan());
        let (_, t) = connect_pair(&mut server, &mut client, addr);
        tok = t;
    }
    // Old server gone. A new one on the same port must be rejoined without
    // waiting for the 2s peer timeout: its reset acks trigger renegotiation.
    let mut server = udp(UdpConfig::lan());
    server.listen_at(addr).unwrap();
    let start = Instant::now();
    let mut disconnected = false;
    let mut reconnected = false;
    let mut accepted = false;
    while !(disconnected && reconnected && accepted) {
        assert!(start.elapsed() < Duration::from_secs(5), "server restart recovery");
        client.poll_with(|e| match e {
            PollEvent::Disconnect { token } => {
                assert_eq!(token, tok);
                disconnected = true;
            }
            PollEvent::Reconnect { token } => {
                assert_eq!(token, tok);
                reconnected = true;
            }
            _ => {}
        });
        server.poll_with(|e| {
            if let PollEvent::Accept { .. } = e {
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
    let mut server = udp(UdpConfig::lan()).with_user_timeout(300);
    let mut client = udp(UdpConfig::lan());
    let (accepted, _) = connect_pair(&mut server, &mut client, addr);
    drop(client);

    let mut disconnected = None;
    let start = Instant::now();
    while disconnected.is_none() {
        assert!(start.elapsed() < Duration::from_secs(5), "peer timeout");
        server.poll_with(|e| {
            if let PollEvent::Disconnect { token } = e {
                disconnected = Some(token);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(disconnected, Some(accepted));
    assert!(start.elapsed() >= Duration::from_millis(250));
}

#[test]
fn udp_backlog_limit_disconnects_non_consuming_peer() {
    let addr = free_addr();
    let mut server =
        udp(UdpConfig::lan()).with_max_backlog(8, flux_timing::Duration::from_millis(50));
    let mut client = udp(UdpConfig::lan());
    let (accepted, _) = connect_pair(&mut server, &mut client, addr);
    // Client stops polling: nothing gets acked.

    let mut disconnected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !disconnected {
        assert!(Instant::now() < deadline, "backlog disconnect");
        server.write_or_enqueue_with(SendBehavior::Single(accepted), |b| {
            b.extend_from_slice(&[0; 1000]);
        });
        server.poll_with(|e| {
            if let PollEvent::Disconnect { token } = e {
                assert_eq!(token, accepted);
                disconnected = true;
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    let _ = &client;
}

#[test]
fn udp_ignores_junk_datagrams() {
    let addr = free_addr();
    let mut server = udp(UdpConfig::lan());
    let mut client = udp(UdpConfig::lan());
    let (accepted, _) = connect_pair(&mut server, &mut client, addr);
    let junk = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    junk.send_to(b"not flux", addr).unwrap();
    junk.send_to(&[0xFF; 1200], addr).unwrap();
    junk.send_to(&[0; 2000], addr).unwrap();
    server.write_or_enqueue_with(SendBehavior::Single(accepted), |b| b.extend_from_slice(b"ok"));
    let mut got = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !got {
        assert!(Instant::now() < deadline, "junk tolerance");
        server.poll_with(|e| assert!(!matches!(e, PollEvent::Accept { .. })));
        client.poll_with(|e| {
            if let PollEvent::Message { payload, .. } = e {
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
    let mut server = udp(UdpConfig::lan()).with_max_backlog(4, flux_timing::Duration::ZERO);
    server.listen_at(addr).unwrap();
    let mut stalled = udp(UdpConfig::lan());
    let mut live = udp(UdpConfig::lan());
    stalled.connect(addr).unwrap();
    live.connect(addr).unwrap();
    let mut accepted = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted < 2 || live.currently_disconnected().count() != 0 {
        assert!(Instant::now() < deadline, "accepts");
        server.poll_with(|e| accepted += usize::from(matches!(e, PollEvent::Accept { .. })));
        stalled.poll_with(|_| {});
        live.poll_with(|_| {});
    }
    // `stalled` stops polling: its unacked count grows past the backlog limit
    // while `live` keeps consuming.
    let msgs: Vec<Vec<u8>> = (0..12).map(|i| make_msg(i, 3000)).collect();
    let mut got = Vec::new();
    let mut dropped = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    for m in &msgs {
        server.write_or_enqueue_with(SendBehavior::Broadcast, |b| b.extend_from_slice(m));
        let until = Instant::now() + Duration::from_millis(20);
        while Instant::now() < until {
            server.poll_with(|e| dropped += usize::from(matches!(e, PollEvent::Disconnect { .. })));
            live.poll_with(|e| {
                if let PollEvent::Message { payload, .. } = e {
                    got.push((msg_id(payload), checksum(payload)));
                }
            });
        }
    }
    while got.len() < msgs.len() {
        assert!(Instant::now() < deadline, "live peer delivery: got {}", got.len());
        server.poll_with(|_| {});
        live.poll_with(|e| {
            if let PollEvent::Message { payload, .. } = e {
                got.push((msg_id(payload), checksum(payload)));
            }
        });
    }
    assert_eq!(dropped, 1, "the stalled peer is dropped exactly once");
    for (id, sum) in got {
        assert_eq!(sum, checksum(&msgs[id as usize]), "message {id} corrupted");
    }
}

/// Messages in flight when the session is cut arrive whole under the new
/// session, however much of them the receiver had already acked.
#[test]
fn udp_reconnect_replays_messages_queued_before_disconnect() {
    let addr = free_addr();
    let mut server = udp(UdpConfig::lan());
    let mut client = udp(UdpConfig::lan());
    let (_, tok) = connect_pair(&mut server, &mut client, addr);
    let big = make_msg(7, 500_000);
    client.write_or_enqueue_with(SendBehavior::Single(tok), |b| b.extend_from_slice(&big));
    client.write_or_enqueue_with(SendBehavior::Single(tok), |b| b.extend_from_slice(b"small"));
    // Let some fragments through and get acked, then cut the session. How
    // much lands first depends on the receive buffer; either message may.
    let mut got = Vec::new();
    for _ in 0..3 {
        client.poll_with(|_| {});
        server.poll_with(|e| {
            if let PollEvent::Message { payload, .. } = e {
                got.push(payload.to_vec());
            }
        });
    }
    client.disconnect(tok);

    // Everything not cumulatively acked is replayed, so the small message may
    // arrive twice: delivery across a reconnect is at-least-once.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !got.iter().any(|m| m.len() == big.len()) {
        assert!(Instant::now() < deadline, "replay");
        server.poll_with(|e| {
            if let PollEvent::Message { payload, .. } = e {
                got.push(payload.to_vec());
            }
        });
        client.poll_with(|_| {});
    }
    assert!(got.iter().any(|m| m == b"small"));
    assert!(got.iter().any(|m| checksum(m) == checksum(&big)), "big message replayed intact");
}

/// A server that drops an accepted peer makes the client renegotiate.
#[test]
fn udp_server_disconnect_reconnects_client() {
    let addr = free_addr();
    let mut server = udp(UdpConfig::lan()).with_user_timeout(5_000);
    let mut client = udp(UdpConfig::lan()).with_user_timeout(5_000);
    let (accepted, tok) = connect_pair(&mut server, &mut client, addr);
    server.disconnect(accepted);
    // Client traffic hits a listener with no session for it and gets reset.
    client.write_or_enqueue_with(SendBehavior::Single(tok), |b| b.extend_from_slice(b"x"));

    let start = Instant::now();
    let (mut disconnected, mut reconnected, mut reaccepted) = (false, false, false);
    while !(disconnected && reconnected && reaccepted) {
        assert!(start.elapsed() < Duration::from_secs(5), "server-side disconnect recovery");
        client.poll_with(|e| match e {
            PollEvent::Disconnect { token } => {
                assert_eq!(token, tok);
                disconnected = true;
            }
            PollEvent::Reconnect { token } => {
                assert_eq!(token, tok);
                reconnected = true;
            }
            _ => {}
        });
        server.poll_with(|e| reaccepted |= matches!(e, PollEvent::Accept { .. }));
        thread::sleep(Duration::from_micros(50));
    }
    assert!(start.elapsed() < Duration::from_secs(1), "did not wait for the peer timeout");
}

/// A message that cannot fit the send window drops the peer instead of
/// vanishing silently.
#[test]
fn udp_window_exhaustion_disconnects_instead_of_dropping() {
    let addr = free_addr();
    let config = UdpConfig { send_window: 64, max_message_size: 64 * 1171, ..UdpConfig::lan() };
    let mut server = udp(config);
    let mut client = udp(config);
    let (accepted, _) = connect_pair(&mut server, &mut client, addr);
    // Client never polls: nothing is acked, the window fills, the 65th
    // single-fragment message cannot be queued.
    let mut disconnected = None;
    for _ in 0..70 {
        server.write_or_enqueue_with(SendBehavior::Single(accepted), |b| b.extend_from_slice(b"m"));
        server.poll_with(|e| {
            if let PollEvent::Disconnect { token } = e {
                disconnected = Some(token);
            }
        });
    }
    assert_eq!(disconnected, Some(accepted));
    let _ = &client;
}
