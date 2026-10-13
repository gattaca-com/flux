use std::{
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Publisher, PublisherEvent, Subscriber, SubscriberEvent, UdpConfig, UdpGroupConfig,
};
use mio::Token;

/// A 1200-byte packet minus its header and one message header.
const STRIDE: usize = 1200 - 25 - 4;

fn free_addr() -> SocketAddr {
    UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap()
}

fn lan() -> UdpGroupConfig {
    UdpGroupConfig { udp: UdpConfig::lan(), ..Default::default() }
}

/// `dial` differs from `addr` when a relay sits between.
fn subscribe_via(
    server: &mut Publisher,
    client: &mut Subscriber,
    addr: SocketAddr,
    dial: SocketAddr,
) -> (Token, Token) {
    server.listen(addr).unwrap();
    let client_token = client.subscribe(dial).unwrap();
    let mut accepted = None;
    let mut subscribed = 0;
    let mut on_client = |event: SubscriberEvent<'_>| {
        if let SubscriberEvent::Subscribed { token, .. } = event {
            assert_eq!(token, client_token);
            subscribed += 1;
        }
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted.is_none() || !client.is_subscribed(client_token) {
        assert!(Instant::now() < deadline, "handshake");
        server.poll_with(|e| {
            if let PublisherEvent::Subscribed { token, .. } = e {
                accepted = Some(token);
            }
        });
        client.poll_with(&mut on_client);
        thread::sleep(Duration::from_micros(50));
    }
    // A hello retry may still be in flight if the ack took longer than one
    // RTO. Let it land now, while the subscriber it belongs to still exists.
    for _ in 0..5 {
        server.poll_with(|_| {});
        client.poll_with(&mut on_client);
    }
    assert_eq!(subscribed, 1, "the handshake must emit Subscribed exactly once");
    (accepted.unwrap(), client_token)
}

fn subscribe_pair(
    server: &mut Publisher,
    client: &mut Subscriber,
    addr: SocketAddr,
) -> (Token, Token) {
    subscribe_via(server, client, addr, addr)
}

fn lan_pair(addr: SocketAddr) -> (Publisher, Subscriber, Token, Token) {
    let mut server = Publisher::new(lan());
    let mut client = Subscriber::new(lan());
    let (accepted, tok) = subscribe_pair(&mut server, &mut client, addr);
    (server, client, accepted, tok)
}

fn checksum(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| (h ^ u64::from(*b)).wrapping_mul(0x100_0000_01b3))
}

/// 4-byte id, then `len` bytes derived from it.
fn make_msg(id: u32, len: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + len);
    v.extend_from_slice(&id.to_le_bytes());
    v.extend((0..len).map(|i| (id as usize).wrapping_mul(31).wrapping_add(i * 7) as u8));
    v
}

fn msg_id(payload: &[u8]) -> u32 {
    u32::from_le_bytes(payload[..4].try_into().unwrap())
}

fn messages(events: &mut Vec<Vec<u8>>) -> impl FnMut(SubscriberEvent<'_>) + '_ {
    |e| {
        if let SubscriberEvent::Message { payload, .. } = e {
            events.push(payload.to_vec());
        }
    }
}

#[test]
fn udp_publishes_to_a_subscriber_once_it_joins() {
    let addr = free_addr();
    let mut server = Publisher::new(lan());
    server.listen(addr).unwrap();
    let mut client = Subscriber::new(lan());
    let tok = client.subscribe(addr).unwrap();
    assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"too early")), 0);

    let mut joined = false;
    let mut sent = false;
    let mut got = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !got {
        assert!(Instant::now() < deadline, "delivery timed out");
        server.poll_with(|e| joined |= matches!(e, PublisherEvent::Subscribed { .. }));
        if joined && !sent {
            assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"live")), 1);
            sent = true;
        }
        client.poll_with(|e| {
            if let SubscriberEvent::Message { token, payload, .. } = e {
                assert_eq!(token, tok);
                assert_eq!(payload, b"live");
                got = true;
            }
        });
        thread::sleep(Duration::from_micros(50));
    }
}

#[test]
fn udp_greeting_comes_with_the_subscription_and_can_turn_it_away() {
    let addr = free_addr();
    let mut server = Publisher::new(lan());
    server.listen(addr).unwrap();
    let mut client =
        Subscriber::new(UdpGroupConfig { on_connect_msg: Some(b"credentials".to_vec()), ..lan() });
    let _ = client.subscribe(addr).unwrap();
    let mut subscribed = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while subscribed.is_none() {
        assert!(Instant::now() < deadline, "greeting");
        server.poll_with(|e| {
            if let PublisherEvent::Subscribed { token, msg, .. } = e {
                subscribed = Some((token, msg.to_vec()));
            }
        });
        client.poll_with(|_| {});
    }
    let (token, msg) = subscribed.unwrap();
    assert_eq!(msg, b"credentials");
    assert!(server.disconnect(token));
    assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"secret")), 0);
}

#[test]
fn udp_broadcast_mixed_sizes_to_two_subscribers() {
    let addr = free_addr();
    let mut server = Publisher::new(lan());
    server.listen(addr).unwrap();
    let mut a = Subscriber::new(lan());
    let mut b = Subscriber::new(lan());
    let _ = a.subscribe(addr).unwrap();
    let _ = b.subscribe(addr).unwrap();
    let mut accepted = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted < 2 {
        assert!(Instant::now() < deadline, "accepts");
        server
            .poll_with(|e| accepted += usize::from(matches!(e, PublisherEvent::Subscribed { .. })));
        a.poll_with(|_| {});
        b.poll_with(|_| {});
    }

    // Around the packet boundary, counting `make_msg`'s id.
    let sizes = [1usize, 100, STRIDE - 4, STRIDE - 3, 5000, 2 * 1024 * 1024];
    let msgs: Vec<Vec<u8>> =
        sizes.iter().enumerate().map(|(i, s)| make_msg(i as u32, *s)).collect();
    for m in &msgs {
        server.broadcast_with(|buf| buf.extend_from_slice(m));
    }

    let expected: Vec<u64> = msgs.iter().map(|m| checksum(m)).collect();
    let mut got_a = Vec::new();
    let mut got_b = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while got_a.len() < msgs.len() || got_b.len() < msgs.len() {
        assert!(Instant::now() < deadline, "broadcast delivery");
        server.poll_with(|_| {});
        a.poll_with(messages(&mut got_a));
        b.poll_with(messages(&mut got_b));
    }
    for got in [got_a, got_b] {
        assert_eq!(got.len(), msgs.len());
        for payload in got {
            let id = msg_id(&payload);
            assert_eq!(checksum(&payload), expected[id as usize], "message {id} corrupted");
        }
    }
}

/// Drops every `drop_every`-th datagram in either direction.
struct LossyRelay {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
    handle: Option<thread::JoinHandle<()>>,
}

impl LossyRelay {
    fn start(server: SocketAddr, drop_every: usize) -> Self {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        for opt in [libc::SO_RCVBUF, libc::SO_SNDBUF] {
            let size: libc::c_int = 64 * 1024 * 1024;
            unsafe {
                libc::setsockopt(
                    std::os::fd::AsRawFd::as_raw_fd(&socket),
                    libc::SOL_SOCKET,
                    opt,
                    std::ptr::from_ref(&size).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }
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

    let mut server = Publisher::new(lan());
    let mut client = Subscriber::new(lan());
    let _ = subscribe_via(&mut server, &mut client, server_addr, relay.addr);

    // Batches of mixed sizes: packets carry several records and records
    // straddle packets.
    let msgs: Vec<Vec<u8>> = (0..N).map(|i| make_msg(i, 1 + (i as usize * 613) % 4000)).collect();
    for chunk in msgs.chunks(25) {
        assert_eq!(server.broadcast_many_with(chunk, |b, m| b.extend_from_slice(m)), 1);
    }

    let mut seen = vec![false; N as usize];
    let mut received = 0;
    let deadline = Instant::now() + Duration::from_secs(20);
    while received < N {
        assert!(Instant::now() < deadline, "loss recovery: {received} rx");
        client.poll_with(|e| {
            if let SubscriberEvent::Message { payload, .. } = e {
                let id = msg_id(payload) as usize;
                assert_eq!(checksum(payload), checksum(&msgs[id]), "message {id} corrupted");
                assert!(!seen[id], "message {id} delivered twice");
                seen[id] = true;
                received += 1;
            }
        });
        server.poll_with(|_| {});
        thread::sleep(Duration::from_micros(20));
    }
    assert!(relay.dropped.load(Ordering::Relaxed) > 0, "relay dropped nothing");
}

#[test]
fn udp_resubscribe_is_a_new_session() {
    let addr = free_addr();
    let mut server =
        Publisher::new(UdpGroupConfig { on_connect_msg: Some(b"welcome".to_vec()), ..lan() });
    let mut client = Subscriber::new(lan());
    let (first, tok) = subscribe_pair(&mut server, &mut client, addr);

    assert!(client.resubscribe(tok));
    assert!(!client.is_subscribed(tok));

    let mut events = Vec::new();
    let mut resubscribed = 0;
    let mut sent = false;
    let mut got = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !got {
        assert!(Instant::now() < deadline, "resubscribe");
        server.poll_with(|e| match e {
            PublisherEvent::Unsubscribed { token, .. } => events.push(("unsubscribed", token)),
            PublisherEvent::Subscribed { token, .. } => events.push(("subscribed", token)),
        });
        if events.len() == 2 && !sent {
            assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"after")), 1);
            sent = true;
        }
        client.poll_with(|e| match e {
            SubscriberEvent::Subscribed { token, msg, .. } => {
                assert_eq!(token, tok);
                assert_eq!(msg, b"welcome");
                resubscribed += 1;
                assert_eq!(resubscribed, 1);
            }
            SubscriberEvent::Message { payload, .. } => {
                assert_eq!(resubscribed, 1, "Subscribed must precede its messages");
                assert_eq!(payload, b"after");
                got = true;
            }
            SubscriberEvent::Unsubscribed { .. } => {}
        });
        thread::sleep(Duration::from_micros(50));
    }
    assert_eq!(events[0], ("unsubscribed", first));
    assert_eq!(events[1].0, "subscribed");
    assert_ne!(events[1].1, first);
    assert!(client.is_subscribed(tok));
}

#[test]
fn udp_late_subscriber_joins_at_the_live_end() {
    let addr = free_addr();
    let (mut server, mut early, _, _) = lan_pair(addr);
    assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"before")), 1);
    let mut late = Subscriber::new(lan());
    let late_tok = late.subscribe(addr).unwrap();
    let mut accepted = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !accepted || !late.is_subscribed(late_tok) {
        assert!(Instant::now() < deadline, "late handshake");
        server.poll_with(|e| accepted |= matches!(e, PublisherEvent::Subscribed { .. }));
        late.poll_with(|_| {});
    }
    assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"after")), 2);

    let (mut got_early, mut got_late) = (Vec::new(), Vec::new());
    let settle = Instant::now() + Duration::from_millis(300);
    while Instant::now() < settle {
        server.poll_with(|_| {});
        early.poll_with(messages(&mut got_early));
        late.poll_with(messages(&mut got_late));
    }
    assert_eq!(got_early, [b"before".to_vec(), b"after".to_vec()]);
    assert_eq!(got_late, [b"after".to_vec()]);
}

#[test]
fn udp_publisher_restart_resubscribes() {
    let addr = free_addr();
    let mut client = Subscriber::new(lan());
    let tok;
    {
        let mut server = Publisher::new(lan());
        let (_, t) = subscribe_pair(&mut server, &mut client, addr);
        tok = t;
    }
    let mut server = Publisher::new(lan());
    server.listen(addr).unwrap();
    let start = Instant::now();
    let mut unsubscribed = false;
    let mut resubscribed = false;
    let mut accepted = false;
    while !(unsubscribed && resubscribed && accepted) {
        assert!(start.elapsed() < Duration::from_secs(5), "publisher restart recovery");
        client.poll_with(|e| match e {
            SubscriberEvent::Unsubscribed { token, .. } => {
                assert_eq!(token, tok);
                unsubscribed = true;
            }
            SubscriberEvent::Subscribed { token, .. } => {
                assert_eq!(token, tok);
                resubscribed = true;
            }
            SubscriberEvent::Message { .. } => {}
        });
        server.poll_with(|e| accepted |= matches!(e, PublisherEvent::Subscribed { .. }));
        thread::sleep(Duration::from_micros(50));
    }
    assert!(start.elapsed() < Duration::from_millis(1500), "took the slow timeout path");
}

#[test]
fn udp_peer_timeout_drops_a_silent_subscriber() {
    let addr = free_addr();
    let mut server = Publisher::new(UdpGroupConfig {
        peer_timeout: flux_timing::Duration::from_millis(300),
        ..lan()
    });
    let mut client = Subscriber::new(lan());
    let (accepted, _) = subscribe_pair(&mut server, &mut client, addr);
    drop(client);

    let mut unsubscribed = None;
    let start = Instant::now();
    while unsubscribed.is_none() {
        assert!(start.elapsed() < Duration::from_secs(5), "peer timeout");
        server.poll_with(|e| {
            if let PublisherEvent::Unsubscribed { token, .. } = e {
                unsubscribed = Some(token);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(unsubscribed, Some(accepted));
    assert!(start.elapsed() >= Duration::from_millis(250));
}

#[test]
fn udp_ignores_junk_datagrams() {
    let addr = free_addr();
    let (mut server, mut client, _, _) = lan_pair(addr);
    let junk = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    junk.send_to(b"not flux", addr).unwrap();
    junk.send_to(&[0xFF; 1200], addr).unwrap();
    junk.send_to(&[0; 2000], addr).unwrap();
    // A connection's hello, which this publisher must not take for its own.
    junk.send_to(&[b'F', b'C', 0x21, 1, 2, 3, 4], addr).unwrap();
    server.broadcast_with(|b| b.extend_from_slice(b"ok"));
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while got.is_empty() {
        assert!(Instant::now() < deadline, "junk tolerance");
        server.poll_with(|e| assert!(!matches!(e, PublisherEvent::Subscribed { .. })));
        client.poll_with(messages(&mut got));
    }
    assert_eq!(got, [b"ok".to_vec()]);
}

#[test]
fn udp_broadcast_survives_dropping_a_subscriber_mid_way() {
    let addr = free_addr();
    let mut server = Publisher::new(UdpGroupConfig {
        udp: UdpConfig { send_window: 64, max_message_size: 8192, ..UdpConfig::lan() },
        ..Default::default()
    });
    server.listen(addr).unwrap();
    let mut stalled = Subscriber::new(lan());
    let mut live = Subscriber::new(lan());
    let _ = stalled.subscribe(addr).unwrap();
    let live_tok = live.subscribe(addr).unwrap();
    let mut accepted = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted < 2 || !live.is_subscribed(live_tok) {
        assert!(Instant::now() < deadline, "accepts");
        server
            .poll_with(|e| accepted += usize::from(matches!(e, PublisherEvent::Subscribed { .. })));
        stalled.poll_with(|_| {});
        live.poll_with(|_| {});
    }
    // `stalled` stops polling: the send window fills while `live` keeps consuming.
    let msgs: Vec<Vec<u8>> = (0..12).map(|i| make_msg(i, 8000)).collect();
    let mut got = Vec::new();
    let mut dropped = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    for (i, m) in msgs.iter().enumerate() {
        server.broadcast_with(|b| b.extend_from_slice(m));
        while got.len() <= i {
            assert!(Instant::now() < deadline, "live subscriber delivery: got {}", got.len());
            server.poll_with(|e| {
                dropped += usize::from(matches!(e, PublisherEvent::Unsubscribed { .. }));
            });
            live.poll_with(messages(&mut got));
        }
    }
    assert_eq!(dropped, 1, "the stalled subscriber is dropped exactly once");
    for payload in got {
        let id = msg_id(&payload);
        assert_eq!(checksum(&payload), checksum(&msgs[id as usize]), "message {id} corrupted");
    }
}

#[test]
fn udp_publisher_disconnect_resubscribes() {
    let addr = free_addr();
    let (mut server, mut client, accepted, tok) = lan_pair(addr);
    assert!(server.disconnect(accepted));

    let start = Instant::now();
    let (mut unsubscribed, mut resubscribed, mut reaccepted) = (false, false, false);
    while !(unsubscribed && resubscribed && reaccepted) {
        assert!(start.elapsed() < Duration::from_secs(5), "publisher-side disconnect recovery");
        client.poll_with(|e| match e {
            SubscriberEvent::Unsubscribed { token, .. } => {
                assert_eq!(token, tok);
                unsubscribed = true;
            }
            SubscriberEvent::Subscribed { token, .. } => {
                assert_eq!(token, tok);
                resubscribed = true;
            }
            SubscriberEvent::Message { .. } => {}
        });
        server.poll_with(|e| reaccepted |= matches!(e, PublisherEvent::Subscribed { .. }));
        thread::sleep(Duration::from_micros(50));
    }
    assert!(start.elapsed() < Duration::from_secs(1), "did not wait for the peer timeout");
}

#[test]
fn udp_window_exhaustion_drops_instead_of_losing() {
    let addr = free_addr();
    let config = UdpGroupConfig {
        udp: UdpConfig { send_window: 64, max_message_size: 4096, ..UdpConfig::lan() },
        ..Default::default()
    };
    let mut server = Publisher::new(config.clone());
    let mut client = Subscriber::new(config);
    let (accepted, _) = subscribe_pair(&mut server, &mut client, addr);
    // The client never acks, so the 65th packet does not fit.
    let mut unsubscribed = None;
    for _ in 0..70 {
        server.broadcast_with(|b| b.extend_from_slice(b"m"));
        server.poll_with(|e| {
            if let PublisherEvent::Unsubscribed { token, .. } = e {
                unsubscribed = Some(token);
            }
        });
    }
    assert_eq!(unsubscribed, Some(accepted));

    // The dropped client resubscribes and gets more than a window.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut rejoined = false;
    while !rejoined {
        assert!(Instant::now() < deadline, "resubscribe");
        client.poll_with(|_| {});
        server.poll_with(|e| rejoined |= matches!(e, PublisherEvent::Subscribed { .. }));
    }
    let n = 200;
    let mut received = 0;
    for _ in 0..n {
        assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"m")), 1);
        server.poll_with(|_| {});
        client.poll_with(|e| received += usize::from(matches!(e, SubscriberEvent::Message { .. })));
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while received < n {
        assert!(Instant::now() < deadline, "received {received} of {n}");
        server.poll_with(|_| {});
        client.poll_with(|e| received += usize::from(matches!(e, SubscriberEvent::Message { .. })));
    }
}

#[test]
fn udp_unreliable_keeps_streaming_through_loss() {
    const N: u32 = 600;
    let config = UdpGroupConfig {
        udp: UdpConfig {
            reliable: false,
            send_window: 64,
            recv_window: 64,
            max_message_size: 4 * STRIDE,
            ..UdpConfig::lan()
        },
        ..Default::default()
    };
    let server_addr = free_addr();
    let relay = LossyRelay::start(server_addr, 5);

    let mut server = Publisher::new(config.clone());
    let mut client = Subscriber::new(config);
    let (_, tok) = subscribe_via(&mut server, &mut client, server_addr, relay.addr);

    let msgs: Vec<Vec<u8>> =
        (0..N).map(|i| make_msg(i, 1 + (i as usize * 613) % (2 * STRIDE))).collect();
    let mut seen = vec![false; N as usize];
    let mut received = 0usize;
    let mut last = 0usize;
    let mut on_client = |e: SubscriberEvent<'_>| {
        if let SubscriberEvent::Message { payload, .. } = e {
            let id = msg_id(payload) as usize;
            assert_eq!(checksum(payload), checksum(&msgs[id]), "message {id} corrupted");
            assert!(!seen[id], "message {id} delivered twice");
            seen[id] = true;
            received += 1;
            last = last.max(id);
        }
    };
    for m in &msgs {
        server.broadcast_with(|b| b.extend_from_slice(m));
        // Paced so the relay's socket buffer is not a second source of loss.
        thread::sleep(Duration::from_micros(50));
        client.poll_with(&mut on_client);
        server.poll_with(|_| {});
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        client.poll_with(&mut on_client);
        server.poll_with(|_| {});
        thread::sleep(Duration::from_micros(50));
    }
    let dropped = relay.dropped.load(Ordering::Relaxed);
    assert!(dropped > 0, "relay dropped nothing");
    assert!(received < N as usize, "loss must lose messages: {received}/{N}");
    assert!(received > N as usize / 2, "too few delivered: {received}/{N}");
    assert!(last > N as usize - 64, "delivery stalled at {last}");
    assert!(client.is_subscribed(tok), "subscription dropped");
}

#[test]
fn udp_resubscribes_while_down_send_one_greeting() {
    let addr = free_addr();
    let mut client =
        Subscriber::new(UdpGroupConfig { on_connect_msg: Some(b"hello".to_vec()), ..lan() });
    let tok = client.subscribe(addr).unwrap();
    for _ in 0..3 {
        client.poll_with(|_| {});
        client.resubscribe(tok);
    }

    let mut server = Publisher::new(lan());
    server.listen(addr).unwrap();
    let mut got = Vec::new();
    let mut on_server = |e: PublisherEvent<'_>| {
        if let PublisherEvent::Subscribed { msg, .. } = e {
            got.push(msg.to_vec());
        }
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !client.is_subscribed(tok) {
        assert!(Instant::now() < deadline, "delivery");
        client.poll_with(|_| {});
        server.poll_with(&mut on_server);
        thread::sleep(Duration::from_micros(50));
    }
    let settle = Instant::now() + Duration::from_millis(200);
    while Instant::now() < settle {
        client.poll_with(|_| {});
        server.poll_with(&mut on_server);
    }
    assert_eq!(got, [b"hello".to_vec()]);
}

#[test]
fn udp_disconnect_when_drained_waits_for_acks() {
    let (mut server, mut client, accepted, _) = lan_pair(free_addr());
    let msg = make_msg(1, 64 * 1024);
    assert_eq!(server.broadcast_with(|b| b.extend_from_slice(&msg)), 1);
    assert!(server.disconnect_when_drained(accepted));
    assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"refused")), 0);

    let hold = Instant::now() + Duration::from_millis(100);
    while Instant::now() < hold {
        server.poll_with(|e| assert!(!matches!(e, PublisherEvent::Unsubscribed { .. })));
    }

    let mut got = Vec::new();
    let mut unsubscribed = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while unsubscribed.is_none() {
        assert!(Instant::now() < deadline, "drained close");
        client.poll_with(messages(&mut got));
        server.poll_with(|e| {
            if let PublisherEvent::Unsubscribed { token, .. } = e {
                unsubscribed = Some(token);
            }
        });
        thread::sleep(Duration::from_micros(50));
    }
    assert_eq!(got, [msg], "the queued message was delivered before the close");
    assert_eq!(unsubscribed, Some(accepted));
}

#[test]
fn udp_listener_ignores_disconnect_and_remove_drops_its_subscribers() {
    let addr = free_addr();
    let mut server = Publisher::new(lan());
    let mut client = Subscriber::new(lan());
    let (accepted, _) = subscribe_pair(&mut server, &mut client, addr);
    let listener = Token(0);

    assert!(!server.disconnect(listener), "a listener has no subscription to drop");
    assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"still-up")), 1);
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while got.is_empty() {
        assert!(Instant::now() < deadline, "subscription survives listener disconnect");
        server.poll_with(|_| {});
        client.poll_with(messages(&mut got));
        thread::sleep(Duration::from_micros(50));
    }

    assert!(server.remove(listener));
    let mut unsubscribed = None;
    while unsubscribed.is_none() {
        assert!(Instant::now() < deadline, "subscribers go with their listener");
        server.poll_with(|e| {
            if let PublisherEvent::Unsubscribed { token, .. } = e {
                unsubscribed = Some(token);
            }
        });
    }
    assert_eq!(unsubscribed, Some(accepted));
    assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"gone")), 0);
    assert!(!server.remove(listener), "already removed");
}

#[test]
fn udp_broadcast_bursts_to_many_subscribers_survive_loss() {
    broadcast_bursts(97, 1, 16_000);
}

/// Rarely polled receivers make round trips long; a rare loss must still be
/// repaired before the window fills.
#[test]
fn udp_slow_receivers_repair_a_rare_loss_in_time() {
    broadcast_bursts(5000, 40, 6000);
}

fn broadcast_bursts(drop_every: usize, poll_every: usize, max_lag: u32) {
    const PEERS: usize = 5;
    const N: u32 = 60_000;
    let server_addr = free_addr();
    let relays: Vec<LossyRelay> =
        (0..PEERS).map(|i| LossyRelay::start(server_addr, drop_every + i)).collect();
    let config = UdpGroupConfig { socket_buf_size: Some(64 * 1024 * 1024), ..lan() };
    let mut server = Publisher::new(config.clone());
    let mut clients: Vec<Subscriber> =
        (0..PEERS).map(|_| Subscriber::new(config.clone())).collect();
    server.listen(server_addr).unwrap();
    for (c, relay) in clients.iter_mut().zip(&relays) {
        let _ = c.subscribe(relay.addr).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut accepted = 0;
    while accepted < PEERS {
        assert!(Instant::now() < deadline, "handshake");
        server
            .poll_with(|e| accepted += usize::from(matches!(e, PublisherEvent::Subscribed { .. })));
        for c in &mut clients {
            c.poll_with(|_| {});
        }
    }
    let sizes: Vec<usize> = (0..N as usize)
        .map(|i| match i % 11 {
            0 => 160 + (i * 613) % 5800,
            _ => 160 + (i * 613) % 1100,
        })
        .collect();
    let mut seen: Vec<Vec<bool>> = vec![vec![false; N as usize]; PEERS];
    let mut received = vec![0u32; PEERS];
    let mut next = 0usize;
    let mut burst = 1usize;
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut drops = 0;
    let mut iteration = 0usize;
    while received.iter().any(|&r| r < N) {
        iteration += 1;
        assert!(Instant::now() < deadline, "delivery stalled: received={received:?} sent={next}");
        let lag = next as u32 - *received.iter().min().unwrap();
        if next < N as usize && lag < max_lag {
            let end = (next + burst).min(N as usize);
            server.broadcast_many_with(next..end, |b, i| {
                b.extend_from_slice(&make_msg(i as u32, sizes[i]));
            });
            next = end;
            burst = burst % 512 + 7;
        }
        server
            .poll_with(|e| drops += usize::from(matches!(e, PublisherEvent::Unsubscribed { .. })));
        for (p, c) in clients.iter_mut().enumerate() {
            if !iteration.is_multiple_of(poll_every) {
                continue;
            }
            c.poll_with(|e| match e {
                SubscriberEvent::Message { payload, .. } => {
                    let id = msg_id(payload) as usize;
                    assert_eq!(
                        checksum(payload),
                        checksum(&make_msg(id as u32, sizes[id])),
                        "corrupted"
                    );
                    assert!(!seen[p][id], "subscriber {p} got {id} twice");
                    seen[p][id] = true;
                    received[p] += 1;
                }
                SubscriberEvent::Unsubscribed { .. } => drops += 1,
                SubscriberEvent::Subscribed { .. } => {}
            });
        }
        assert_eq!(drops, 0, "a subscriber was dropped: received={received:?} sent={next}");
    }
    assert!(relays.iter().all(|r| r.dropped.load(Ordering::Relaxed) > 0));
}

#[test]
fn udp_publisher_and_subscriber_share_one_external_poll() {
    let addr = free_addr();
    let mut poll = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(64);
    let mut server =
        Publisher::with_external_poll(lan(), poll.registry().try_clone().unwrap(), 0..100);
    let mut client =
        Subscriber::with_external_poll(lan(), poll.registry().try_clone().unwrap(), 100..200);
    server.listen(addr).unwrap();
    let tok = client.subscribe(addr).unwrap();
    let mut joined = false;
    let mut sent = false;
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while got.is_empty() {
        assert!(Instant::now() < deadline, "shared poll delivery");
        let mut on_server =
            |e: PublisherEvent<'_>| joined |= matches!(e, PublisherEvent::Subscribed { .. });
        let mut on_client = messages(&mut got);
        server.pre_poll(&mut on_server);
        client.pre_poll(&mut on_client);
        poll.poll(&mut events, Some(server.max_poll_interval().min(client.max_poll_interval())))
            .unwrap();
        for event in &events {
            if event.token().0 < 100 {
                server.handle_event(event, &mut on_server);
            } else {
                client.handle_event(event, &mut on_client);
            }
        }
        server.post_poll(&mut on_server);
        client.post_poll(&mut on_client);
        if joined && client.is_subscribed(tok) && !sent {
            assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"shared")), 1);
            sent = true;
        }
    }
    assert_eq!(got, [b"shared".to_vec()]);
}
