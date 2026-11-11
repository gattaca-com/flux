use std::{
    collections::HashSet,
    io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Group, GroupConfig, Network, NetworkEvent, NetworkWithExternalPoll, PayloadBuf, UdpConfig,
    UdpGroupConfigV2,
};
use mio::Token;

/// A network with one publisher group.
struct Publisher {
    net: Network,
    group: Group,
}

enum PublisherEvent<'a> {
    Subscribed {
        token: Token,
    },
    Unsubscribed {
        token: Token,
    },
    /// A subscriber's on-connect message.
    Message {
        token: Token,
        payload: &'a [u8],
    },
}

impl Publisher {
    fn new(config: UdpGroupConfigV2) -> Self {
        let mut net = Network::default();
        let group = net.add_group(GroupConfig::UdpPublisher(config));
        Self { net, group }
    }

    fn listen(&mut self, addr: SocketAddr) -> io::Result<Token> {
        self.net.listen(self.group, addr)
    }

    fn broadcast_with(&mut self, serialise: impl FnOnce(&mut PayloadBuf<'_>)) -> usize {
        self.net.broadcast_with(self.group, serialise)
    }

    fn broadcast_many_with<I: IntoIterator>(
        &mut self,
        items: I,
        serialise: impl FnMut(&mut PayloadBuf<'_>, I::Item),
    ) -> usize {
        self.net.broadcast_many_with(self.group, items, serialise)
    }

    fn disconnect(&mut self, token: Token) -> bool {
        self.net.disconnect(token)
    }

    fn disconnect_when_drained(&mut self, token: Token) -> bool {
        self.net.disconnect_when_drained(token)
    }

    fn remove(&mut self, token: Token) -> bool {
        self.net.remove(token)
    }

    fn poll_with(&mut self, mut handler: impl FnMut(PublisherEvent<'_>)) {
        self.net.poll_with(|event| {
            handler(match event {
                NetworkEvent::Accepted { token, .. } => PublisherEvent::Subscribed { token },
                NetworkEvent::Disconnected { token, .. } => PublisherEvent::Unsubscribed { token },
                NetworkEvent::Message { token, payload, .. } => {
                    PublisherEvent::Message { token, payload }
                }
                NetworkEvent::Connected { .. } => unreachable!("a publisher doesn't connect"),
            });
        });
    }
}

/// A network with one subscriber group.
struct Subscriber {
    net: Network,
    group: Group,
    subscribed: HashSet<Token>,
}

enum SubscriberEvent<'a> {
    Subscribed { token: Token },
    Unsubscribed { token: Token },
    Message { token: Token, payload: &'a [u8] },
}

impl Subscriber {
    fn new(config: UdpGroupConfigV2) -> Self {
        let mut net = Network::default();
        let group = net.add_group(GroupConfig::UdpSubscriber(config));
        Self { net, group, subscribed: HashSet::new() }
    }

    fn subscribe(&mut self, addr: SocketAddr) -> Token {
        self.net.connect(self.group, addr)
    }

    fn is_subscribed(&self, token: Token) -> bool {
        self.subscribed.contains(&token)
    }

    fn resubscribe(&mut self, token: Token) -> bool {
        self.subscribed.remove(&token);
        self.net.disconnect(token)
    }

    fn poll_with(&mut self, mut handler: impl FnMut(SubscriberEvent<'_>)) {
        let subscribed = &mut self.subscribed;
        self.net.poll_with(|event| {
            handler(match event {
                NetworkEvent::Connected { token, .. } => {
                    subscribed.insert(token);
                    SubscriberEvent::Subscribed { token }
                }
                NetworkEvent::Disconnected { token, .. } => {
                    subscribed.remove(&token);
                    SubscriberEvent::Unsubscribed { token }
                }
                NetworkEvent::Message { token, payload, .. } => {
                    SubscriberEvent::Message { token, payload }
                }
                NetworkEvent::Accepted { .. } => unreachable!("a subscriber doesn't listen"),
            });
        });
    }
}

/// A 1200-byte packet minus its header and one message header.
const STRIDE: usize = 1200 - 25 - 4;

fn free_addr() -> SocketAddr {
    UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap()
}

fn lan() -> UdpGroupConfigV2 {
    UdpGroupConfigV2 { udp: UdpConfig::lan(), ..Default::default() }
}

/// `dial` differs from `addr` when a relay sits between.
fn subscribe_via(
    server: &mut Publisher,
    client: &mut Subscriber,
    addr: SocketAddr,
    dial: SocketAddr,
) -> (Token, Token) {
    server.listen(addr).unwrap();
    let client_token = client.subscribe(dial);
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
    let tok = client.subscribe(addr);
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
    let mut client = Subscriber::new(UdpGroupConfigV2 {
        on_connect_msg: Some(b"credentials".to_vec()),
        ..lan()
    });
    let _ = client.subscribe(addr);
    let mut subscribed = None;
    let mut greeting = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while greeting.is_none() {
        assert!(Instant::now() < deadline, "greeting");
        server.poll_with(|e| match e {
            PublisherEvent::Subscribed { token } => subscribed = Some(token),
            PublisherEvent::Message { token, payload } => {
                assert_eq!(Some(token), subscribed, "the greeting follows its Subscribed");
                greeting = Some(payload.to_vec());
            }
            PublisherEvent::Unsubscribed { .. } => {}
        });
        client.poll_with(|_| {});
    }
    let token = subscribed.unwrap();
    assert_eq!(greeting.unwrap(), b"credentials");
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
    let _ = a.subscribe(addr);
    let _ = b.subscribe(addr);
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

/// Holds every datagram for `delay` in either direction, and counts the
/// stream packets it carries to the subscriber.
struct DelayRelay {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    packets: Arc<AtomicUsize>,
    handle: Option<thread::JoinHandle<()>>,
}

impl DelayRelay {
    fn start(server: SocketAddr, delay: Duration) -> Self {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        socket.set_read_timeout(Some(Duration::from_micros(200))).unwrap();
        let addr = socket.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let packets = Arc::new(AtomicUsize::new(0));
        let (stop_c, packets_c) = (stop.clone(), packets.clone());
        let handle = thread::spawn(move || {
            let mut buf = vec![0u8; 65_536];
            let mut client: Option<SocketAddr> = None;
            let mut held = std::collections::VecDeque::new();
            while !stop_c.load(Ordering::Relaxed) {
                while held.front().is_some_and(|(at, _, _): &(Instant, _, _)| *at <= Instant::now())
                {
                    let (_, to, bytes): (_, SocketAddr, Vec<u8>) = held.pop_front().unwrap();
                    let _ = socket.send_to(&bytes, to);
                }
                let Ok((n, from)) = socket.recv_from(&mut buf) else { continue };
                let to = if from == server {
                    let Some(c) = client else { continue };
                    // Pub/sub magic, then kind 3: a stream packet.
                    if n > 2 && buf[..2] == *b"FP" && buf[2] & 0x0f == 3 {
                        packets_c.fetch_add(1, Ordering::Relaxed);
                    }
                    c
                } else {
                    client = Some(from);
                    server
                };
                held.push_back((Instant::now() + delay, to, buf[..n].to_vec()));
            }
        });
        Self { addr, stop, packets, handle: Some(handle) }
    }
}

impl Drop for DelayRelay {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.take().unwrap().join().unwrap();
    }
}

/// A packet is resent until an ack comes back without a resend in between,
/// so a link slower than `initial_rto` never teaches the timeout its round
/// trip. Pings do.
#[test]
fn udp_pings_teach_a_slow_link_its_retransmit_timeout() {
    const N: usize = 20;
    let config = UdpGroupConfigV2 {
        udp: UdpConfig { initial_rto: flux_timing::Duration::from_millis(5), ..UdpConfig::lan() },
        ..Default::default()
    };
    let server_addr = free_addr();
    let relay = DelayRelay::start(server_addr, Duration::from_millis(30));
    let mut server = Publisher::new(config.clone());
    let mut client = Subscriber::new(config);
    let _ = subscribe_via(&mut server, &mut client, server_addr, relay.addr);

    // A few pings at the 100 ms heartbeat.
    let settle = Instant::now() + Duration::from_millis(400);
    while Instant::now() < settle {
        server.poll_with(|_| {});
        client.poll_with(|_| {});
        thread::sleep(Duration::from_micros(200));
    }

    relay.packets.store(0, Ordering::Relaxed);
    let mut got = Vec::new();
    for i in 0..N as u32 {
        assert_eq!(server.broadcast_with(|b| b.extend_from_slice(&make_msg(i, 16))), 1);
        let next = Instant::now() + Duration::from_millis(30);
        while Instant::now() < next {
            server.poll_with(|_| {});
            client.poll_with(messages(&mut got));
            thread::sleep(Duration::from_micros(200));
        }
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while got.len() < N {
        assert!(Instant::now() < deadline, "delivered {} of {N}", got.len());
        server.poll_with(|_| {});
        client.poll_with(messages(&mut got));
        thread::sleep(Duration::from_micros(200));
    }
    let packets = relay.packets.load(Ordering::Relaxed);
    assert!(packets < N * 3 / 2, "{packets} packets for {N} messages: resent before their acks");
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
        Publisher::new(UdpGroupConfigV2 { on_connect_msg: Some(b"welcome".to_vec()), ..lan() });
    let mut client = Subscriber::new(lan());
    let (first, tok) = subscribe_pair(&mut server, &mut client, addr);

    assert!(client.resubscribe(tok));
    assert!(!client.is_subscribed(tok));

    let mut events = Vec::new();
    let mut resubscribed = 0;
    let mut greeted = false;
    let mut sent = false;
    let mut got = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !got {
        assert!(Instant::now() < deadline, "resubscribe");
        server.poll_with(|e| match e {
            PublisherEvent::Unsubscribed { token } => events.push(("unsubscribed", token)),
            PublisherEvent::Subscribed { token } => events.push(("subscribed", token)),
            PublisherEvent::Message { .. } => {}
        });
        if events.len() == 2 && !sent {
            assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"after")), 1);
            sent = true;
        }
        client.poll_with(|e| match e {
            SubscriberEvent::Subscribed { token } => {
                assert_eq!(token, tok);
                resubscribed += 1;
                assert_eq!(resubscribed, 1);
            }
            SubscriberEvent::Message { payload, .. } => {
                assert_eq!(resubscribed, 1, "Subscribed must precede its messages");
                if payload == b"welcome" {
                    assert!(!greeted, "one greeting per subscription");
                    greeted = true;
                } else {
                    assert!(greeted, "the greeting comes first");
                    assert_eq!(payload, b"after");
                    got = true;
                }
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
    let late_tok = late.subscribe(addr);
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
    let mut server = Publisher::new(UdpGroupConfigV2 {
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
fn udp_peer_timeout_resubscribes_from_a_dead_publisher() {
    let addr = free_addr();
    let mut server = Publisher::new(lan());
    let mut client = Subscriber::new(UdpGroupConfigV2 {
        peer_timeout: flux_timing::Duration::from_millis(300),
        ..lan()
    });
    let (_, tok) = subscribe_pair(&mut server, &mut client, addr);

    // Pings alone keep an idle subscription alive.
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(600) {
        server.poll_with(|_| {});
        client.poll_with(|e| assert!(!matches!(e, SubscriberEvent::Unsubscribed { .. })));
        thread::sleep(Duration::from_millis(1));
    }
    drop(server);

    let mut unsubscribed = None;
    let start = Instant::now();
    while unsubscribed.is_none() {
        assert!(start.elapsed() < Duration::from_secs(5), "publisher timeout");
        client.poll_with(|e| {
            if let SubscriberEvent::Unsubscribed { token, .. } = e {
                unsubscribed = Some(token);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(unsubscribed, Some(tok));
    // The last ping may have landed up to a heartbeat before the drop.
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(150), "{elapsed:?}");
    assert!(!client.is_subscribed(tok));
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
    let mut server = Publisher::new(UdpGroupConfigV2 {
        udp: UdpConfig { send_window: 64, max_message_size: 8192, ..UdpConfig::lan() },
        ..Default::default()
    });
    server.listen(addr).unwrap();
    let mut stalled = Subscriber::new(lan());
    let mut live = Subscriber::new(lan());
    let _ = stalled.subscribe(addr);
    let live_tok = live.subscribe(addr);
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
    let config = UdpGroupConfigV2 {
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
    let config = UdpGroupConfigV2 {
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
        Subscriber::new(UdpGroupConfigV2 { on_connect_msg: Some(b"hello".to_vec()), ..lan() });
    let tok = client.subscribe(addr);
    for _ in 0..3 {
        client.poll_with(|_| {});
        client.resubscribe(tok);
    }

    let mut server = Publisher::new(lan());
    server.listen(addr).unwrap();
    let mut got = Vec::new();
    let mut on_server = |e: PublisherEvent<'_>| {
        if let PublisherEvent::Message { payload, .. } = e {
            got.push(payload.to_vec());
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
fn udp_a_draining_subscriber_gets_nothing_newer() {
    let addr = free_addr();
    let mut server = Publisher::new(lan());
    let mut draining = Subscriber::new(lan());
    let mut live = Subscriber::new(lan());
    let (drained, _) = subscribe_pair(&mut server, &mut draining, addr);
    let live_token = live.subscribe(addr);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !live.is_subscribed(live_token) {
        assert!(Instant::now() < deadline, "second subscriber");
        server.poll_with(|_| {});
        live.poll_with(|_| {});
    }
    assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"old")), 2);
    assert!(server.disconnect_when_drained(drained));
    assert_eq!(server.broadcast_with(|b| b.extend_from_slice(b"new")), 1);

    let mut from_draining = Vec::new();
    let mut from_live = Vec::new();
    let mut unsubscribed = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !unsubscribed || from_live.len() < 2 {
        assert!(Instant::now() < deadline, "drain with a live subscriber");
        draining.poll_with(messages(&mut from_draining));
        live.poll_with(messages(&mut from_live));
        server.poll_with(|e| unsubscribed |= matches!(e, PublisherEvent::Unsubscribed { .. }));
        thread::sleep(Duration::from_micros(50));
    }
    assert_eq!(from_draining, [b"old".to_vec()]);
    assert_eq!(from_live, [b"old".to_vec(), b"new".to_vec()]);
}

#[test]
fn udp_an_exhausted_token_range_refuses_subscribers() {
    let addr = free_addr();
    let mut poll = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(64);
    // The listener and one subscriber.
    let mut net = NetworkWithExternalPoll::new(poll.registry().try_clone().unwrap(), 0..2);
    let publisher = net.add_group(GroupConfig::UdpPublisher(lan()));
    net.listen(publisher, addr).unwrap();
    let mut first = Subscriber::new(lan());
    let mut second = Subscriber::new(lan());
    let first_token = first.subscribe(addr);
    let second_token = second.subscribe(addr);
    let mut joined = 0;
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        let mut on_server =
            |e: NetworkEvent<'_>| joined += usize::from(matches!(e, NetworkEvent::Accepted { .. }));
        net.pre_poll(&mut on_server);
        poll.poll(&mut events, Some(std::time::Duration::from_millis(1))).unwrap();
        for event in &events {
            net.handle_event(event, &mut on_server);
        }
        net.post_poll(&mut on_server);
        first.poll_with(|_| {});
        second.poll_with(|_| {});
    }
    assert_eq!(joined, 1);
    assert_ne!(first.is_subscribed(first_token), second.is_subscribed(second_token));
}

#[test]
fn udp_listener_ignores_disconnect_and_remove_drops_its_subscribers() {
    let addr = free_addr();
    let mut server = Publisher::new(lan());
    let mut client = Subscriber::new(lan());
    let listener = server.listen(addr).unwrap();
    let tok = client.subscribe(addr);
    let mut accepted = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted.is_none() || !client.is_subscribed(tok) {
        assert!(Instant::now() < deadline, "handshake");
        server.poll_with(|e| {
            if let PublisherEvent::Subscribed { token } = e {
                accepted = Some(token);
            }
        });
        client.poll_with(|_| {});
    }
    let accepted = accepted.unwrap();

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
    let config = UdpGroupConfigV2 { socket_buf_size: Some(64 * 1024 * 1024), ..lan() };
    let mut server = Publisher::new(config.clone());
    let mut clients: Vec<Subscriber> =
        (0..PEERS).map(|_| Subscriber::new(config.clone())).collect();
    server.listen(server_addr).unwrap();
    for (c, relay) in clients.iter_mut().zip(&relays) {
        let _ = c.subscribe(relay.addr);
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

#[derive(Debug, PartialEq)]
enum Seen {
    Accepted(Group, Token),
    Connected(Group, Token),
    Message(Group, Token, Vec<u8>),
    Disconnected(Group, Token),
}

fn poll_until(net: &mut Network, seen: &mut Vec<Seen>, done: impl Fn(&[Seen]) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done(seen) {
        assert!(Instant::now() < deadline, "timed out with {seen:?}");
        net.poll_with(|event| {
            seen.push(match event {
                NetworkEvent::Accepted { group, token, .. } => Seen::Accepted(group, token),
                NetworkEvent::Connected { group, token, .. } => Seen::Connected(group, token),
                NetworkEvent::Message { group, token, payload, .. } => {
                    Seen::Message(group, token, payload.to_vec())
                }
                NetworkEvent::Disconnected { group, token, .. } => Seen::Disconnected(group, token),
            });
        });
        thread::sleep(Duration::from_micros(50));
    }
}

#[test]
fn udp_pubsub_runs_as_network_groups() {
    let addr = free_addr();
    let mut net = Network::default();
    let publisher = net.add_group(GroupConfig::UdpPublisher(UdpGroupConfigV2 {
        on_connect_msg: Some(b"hi".to_vec()),
        ..lan()
    }));
    let subscriber = net.add_group(GroupConfig::UdpSubscriber(lan()));
    net.listen(publisher, addr).unwrap();
    let sub = net.connect(subscriber, addr);

    let greeting = Seen::Message(subscriber, sub, b"hi".to_vec());
    let mut seen = Vec::new();
    poll_until(&mut net, &mut seen, |seen| seen.contains(&greeting));
    let Some(&Seen::Accepted(_, accepted)) =
        seen.iter().find(|e| matches!(e, Seen::Accepted(group, _) if *group == publisher))
    else {
        panic!("no Accepted in {seen:?}")
    };
    let connected = seen.iter().position(|e| *e == Seen::Connected(subscriber, sub));
    assert!(connected < seen.iter().position(|e| *e == greeting), "{seen:?}");

    assert_eq!(net.broadcast_with(publisher, |b| b.extend_from_slice(b"live")), 1);
    assert!(!net.send_with(accepted, |b| b.extend_from_slice(b"no")));
    let live = Seen::Message(subscriber, sub, b"live".to_vec());
    poll_until(&mut net, &mut seen, |seen| seen.contains(&live));

    // Dropping the subscriber reaches both groups, and it rejoins on its own.
    seen.clear();
    assert!(net.disconnect(accepted));
    poll_until(&mut net, &mut seen, |seen| {
        seen.contains(&Seen::Disconnected(publisher, accepted)) &&
            seen.contains(&Seen::Disconnected(subscriber, sub)) &&
            seen.iter().any(
                |e| matches!(e, Seen::Accepted(group, t) if *group == publisher && *t != accepted),
            ) &&
            seen.contains(&Seen::Connected(subscriber, sub))
    });
    assert!(!net.disconnect(accepted), "a dropped subscriber's token is retired");
}
