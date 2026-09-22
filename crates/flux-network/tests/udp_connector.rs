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
    // A hello retry may still be in flight if the ack took longer than one
    // RTO. Let it land now, while the peer it belongs to still exists.
    for _ in 0..5 {
        server.poll_with(|_| {});
        client.poll_with(|_| {});
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
    fn start(server: SocketAddr, drop_every: usize, reorder_and_duplicate: bool) -> Self {
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
            let mut held: Option<(Vec<u8>, SocketAddr)> = None;
            while !stop_c.load(Ordering::Relaxed) {
                let Ok((n, from)) = socket.recv_from(&mut buf) else {
                    if let Some((bytes, to)) = held.take() {
                        let _ = socket.send_to(&bytes, to);
                    }
                    continue;
                };
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
                if reorder_and_duplicate && n >= 29 && buf[2] == 0x11 {
                    if let Some((bytes, dest)) = held.take() {
                        for _ in 0..2 {
                            let _ = socket.send_to(&buf[..n], to);
                            let _ = socket.send_to(&bytes, dest);
                        }
                    } else {
                        held = Some((buf[..n].to_vec(), to));
                    }
                } else {
                    let _ = socket.send_to(&buf[..n], to);
                }
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
    delivery_under_loss(false);
}

#[test]
fn udp_ordered_delivery_under_loss_reordering_and_duplicates() {
    delivery_under_loss(true);
}

fn delivery_under_loss(ordered: bool) {
    const N: u32 = 400;
    let server_addr = free_addr();
    let relay = LossyRelay::start(server_addr, 7, ordered);

    let config = UdpConfig { ordered, ..UdpConfig::lan() };
    let mut server = udp(config);
    let mut client = udp(config);
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
                if ordered {
                    assert_eq!(id, received as usize, "delivery out of send order");
                }
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
                if ordered {
                    assert_eq!(msg_id(payload), echoed_back, "echo out of send order");
                }
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
    reconnect_replays_messages(false);
}

#[test]
fn udp_ordered_reconnect_replays_messages_queued_before_disconnect() {
    reconnect_replays_messages(true);
}

fn reconnect_replays_messages(ordered: bool) {
    let addr = free_addr();
    let config = UdpConfig { ordered, ..UdpConfig::lan() };
    let mut server = udp(config);
    let mut client = udp(config);
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
    while !got.iter().any(|m| m.len() == big.len()) || !got.iter().any(|m| m == b"small") {
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
    if ordered {
        assert_eq!(got.first(), Some(&big), "later message bypassed the partial message");
    }
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

#[test]
fn udp_ordered_conflicting_fragments_do_not_consume_valid_retries() {
    let addr = free_addr();
    let config = UdpConfig { ordered: true, ..UdpConfig::lan() };
    let mut server = udp(config);
    server.listen_at(addr).unwrap();
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    socket.set_nonblocking(true).unwrap();
    // A wire peer lets us inject individually valid but conflicting fragments.
    let send = |kind: u8, seq: u64, len: u32, index: u16, payload: &[u8]| {
        let mut bytes = vec![0; 29];
        bytes[..2].copy_from_slice(b"FX");
        bytes[2] = 0x10 | kind;
        bytes[3..7].copy_from_slice(&7u32.to_le_bytes());
        bytes[7..15].copy_from_slice(&seq.to_le_bytes());
        bytes[15..19].copy_from_slice(&len.to_le_bytes());
        bytes[19..21].copy_from_slice(&index.to_le_bytes());
        bytes.extend_from_slice(payload);
        socket.send_to(&bytes, addr).unwrap();
    };
    send(3, 0, 0, 0, &[]);
    send(1, 0, 2342, 0, &[b'a'; 1171]);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline, "initial fragment not acknowledged");
        server.poll_with(|event| assert!(!matches!(event, PollEvent::Message { .. })));
        let mut ack = [0; 1200];
        if let Ok((n, _)) = socket.recv_from(&mut ack) {
            if n >= 29 && ack[2] == 0x12 && u64::from_le_bytes(ack[7..15].try_into().unwrap()) == 1
            {
                break;
            }
        }
    }
    send(1, 1, 3513, 1, &[b'x'; 1171]);
    send(1, 1, 1, 0, b"x");
    send(1, 2, 1, 0, b"c");
    send(1, 1, 2342, 1, &[b'b'; 1171]);
    send(1, 2, 1, 0, b"c");
    let mut received = Vec::new();
    while received.len() < 2 {
        assert!(Instant::now() < deadline, "valid retry lost behind conflicting fragments");
        server.poll_with(|event| {
            if let PollEvent::Message { payload, .. } = event {
                received.push(payload.to_vec());
            }
        });
    }
    let mut first = vec![b'a'; 1171];
    first.extend_from_slice(&[b'b'; 1171]);
    assert_eq!(received, [first, b"c".to_vec()]);
}

#[test]
fn udp_queued_send_delivers_a_burst_larger_than_the_window() {
    let config =
        UdpConfig { ordered: true, max_pending_bytes: 8 * 1024 * 1024, ..UdpConfig::lan() };
    let addr = free_addr();
    let mut server = udp(config).with_socket_buf_size(32 * 1024 * 1024);
    let mut client = udp(config);
    let (_, token) = connect_pair(&mut server, &mut client, addr);
    let oversized = vec![0; config.max_message_size + 1];
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            client.write_or_enqueue_with(SendBehavior::Single(token), |b| {
                b.extend_from_slice(&oversized);
            });
        }))
        .is_err()
    );

    let messages = [
        make_msg(0, config.max_message_size - 4),
        make_msg(1, 4 * 1024 * 1024 - 4),
        make_msg(2, 4 * 1024 * 1024 - 4),
    ];
    for message in &messages {
        client.write_or_enqueue_with(SendBehavior::Single(token), |b| {
            b.extend_from_slice(message);
        });
    }
    assert_eq!(client.currently_disconnected().count(), 0);
    let mut received = 0;
    let deadline = Instant::now() + Duration::from_secs(30);
    while received != messages.len() {
        assert!(Instant::now() < deadline, "queued burst received {received} messages");
        server.poll_with(|event| {
            if let PollEvent::Message { payload, .. } = event {
                assert_eq!(payload, messages[received]);
                received += 1;
            }
        });
        client.poll_with(|event| assert!(!matches!(event, PollEvent::Disconnect { .. })));
    }
}

#[test]
fn udp_queued_send_obeys_limits_and_disconnect_policy() {
    for drop_backlog in [false, true] {
        let config = UdpConfig {
            ordered: true,
            send_window: 64,
            recv_window: 64,
            max_message_size: 1171,
            max_pending_bytes: 2000,
            ..UdpConfig::lan()
        };
        let addr = free_addr();
        let mut server = udp(config);
        let mut client = udp(config).with_drop_outbound_backlog_on_disconnect(drop_backlog);
        let (old_token, token) = connect_pair(&mut server, &mut client, addr);
        for id in 0..66 {
            let message = make_msg(id, 996);
            client.write_or_enqueue_with(SendBehavior::Single(token), |b| {
                b.extend_from_slice(&message);
            });
        }
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                client.write_or_enqueue_with(SendBehavior::Single(token), |b| b.resize(1000, 0));
            }))
            .is_err()
        );
        client.disconnect(token);

        let mut new_token = None;
        let mut reconnected = false;
        let mut received = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !reconnected || new_token.is_none() || (!drop_backlog && received.len() < 66) {
            assert!(Instant::now() < deadline, "queued reconnect: {received:?}");
            server.poll_with(|event| match event {
                PollEvent::Accept { stream, .. } => {
                    assert_ne!(stream, old_token);
                    new_token = Some(stream);
                }
                PollEvent::Message { token, payload, .. } if Some(token) == new_token => {
                    let id = msg_id(payload);
                    assert_eq!(payload, make_msg(id, 996));
                    received.push(id);
                }
                _ => {}
            });
            client.poll_with(|event| reconnected |= matches!(event, PollEvent::Reconnect { .. }));
        }
        assert_eq!(received, if drop_backlog { Vec::new() } else { (0..66).collect() });
        client.write_or_enqueue_with(SendBehavior::Single(token), |b| b.extend_from_slice(b"end"));
        let mut ended = false;
        while !ended {
            assert!(Instant::now() < deadline, "queued reconnect tail");
            server.poll_with(|event| {
                if let PollEvent::Message { payload, .. } = event {
                    assert_eq!(payload, b"end", "discarded backlog reappeared");
                    ended = true;
                }
            });
            client.poll_with(|_| {});
        }
    }
}

#[test]
fn udp_clear_backlog_renegotiates_even_while_locally_disconnected() {
    let config = UdpConfig {
        send_window: 64,
        recv_window: 64,
        max_message_size: 1171,
        max_pending_bytes: 2000,
        ..UdpConfig::lan()
    };
    let addr = free_addr();
    let mut server = udp(UdpConfig { ordered: true, ..config });
    let mut client = udp(config);
    let (old_token, token) = connect_pair(&mut server, &mut client, addr);
    client.disconnect(token);

    // The server observes the Hello, but the client has not read its HelloAck.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut before_clear = None;
    while before_clear.is_none() {
        assert!(Instant::now() < deadline, "server did not observe the pre-clear session");
        server.poll_with(|event| {
            if let PollEvent::Accept { stream, .. } = event {
                assert_ne!(stream, old_token);
                before_clear = Some(stream);
            }
        });
    }
    assert_eq!(client.currently_disconnected().count(), 1);
    for _ in 0..66 {
        client.write_or_enqueue_with(SendBehavior::Single(token), |b| b.resize(1000, 0));
    }
    assert_eq!(client.clear_backlog(token), 66);
    assert_eq!(client.clear_backlog(token), 0);
    client.write_or_enqueue_with(SendBehavior::Single(token), |b| b.extend_from_slice(b"resumed"));

    let mut after_clear = None;
    let mut received = false;
    while !received {
        assert!(Instant::now() < deadline, "delivery stuck on discarded sequence numbers");
        server.poll_with(|event| match event {
            PollEvent::Accept { stream, .. } => {
                assert_ne!(Some(stream), before_clear);
                after_clear = Some(stream);
            }
            PollEvent::Message { token, payload, .. } => {
                assert_eq!(Some(token), after_clear);
                assert_eq!(payload, b"resumed");
                assert!(!received);
                received = true;
            }
            _ => {}
        });
        client.poll_with(|_| {});
    }
    assert!(after_clear.is_some());
    assert_eq!(client.currently_disconnected().count(), 0);
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
