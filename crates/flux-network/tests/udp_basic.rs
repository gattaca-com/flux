use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};

use flux_network::udp::{UdpPublisher, UdpSubscriber, wire};
use flux_timing::{Duration, Instant};

fn free_udp_addr() -> SocketAddr {
    UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap()
}

/// A pair, subscribed through the dial the publisher requires.
fn pair() -> (UdpPublisher, UdpSubscriber, SocketAddr) {
    let pub_addr = free_udp_addr();
    let mut publisher = UdpPublisher::bind(pub_addr, None).unwrap();
    let subscriber = UdpSubscriber::bind((Ipv4Addr::LOCALHOST, 0).into(), None).unwrap();
    subscriber.dial(pub_addr).unwrap();
    assert_eq!(publisher.poll_subscriptions(|_| true), 1, "the dial was not honoured");
    (publisher, subscriber, pub_addr)
}

/// Models what a real publisher cannot: a chosen session, and a message cut
/// short at `max_fragments` so a partial is left behind.
fn raw_send(
    raw: &UdpSocket,
    to: SocketAddr,
    session: u32,
    seq: u64,
    msg: &[u8],
    max_fragments: usize,
) {
    let mut buf = Vec::new();
    let mut sent = 0;
    wire::encode_fragments(wire::DEFAULT_MAX_DATAGRAM_SIZE, session, seq, 7, msg, &mut buf, |d| {
        if sent < max_fragments {
            raw.send_to(d, to).unwrap();
        }
        sent += 1;
    });
}

/// The deadline only bounds a failure; loopback is effectively synchronous.
fn collect(subscriber: &mut UdpSubscriber, want: usize) -> Vec<(u64, Vec<u8>, u64)> {
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(2);
    while got.len() < want && Instant::now() < deadline {
        subscriber.poll_with(|m| got.push((m.seq, m.payload.to_vec(), m.send_ns.0)));
    }
    got
}

#[test]
fn messages_arrive_intact_with_their_send_time() {
    let (mut publisher, mut subscriber, _) = pair();
    let before = flux_timing::Nanos::now().0;
    for i in 0..5u8 {
        assert_eq!(publisher.publish(&[i; 100]), Some(u64::from(i)));
    }
    let got = collect(&mut subscriber, 5);
    assert_eq!(got.len(), 5);
    for (i, (seq, payload, send_ns)) in got.iter().enumerate() {
        assert_eq!(*seq, i as u64);
        assert_eq!(payload, &vec![i as u8; 100]);
        assert!(*send_ns >= before, "send_ns must be the publisher's clock, not zero");
    }
}

#[test]
fn large_messages_are_fragmented_and_reassembled() {
    let (mut publisher, mut subscriber, _) = pair();
    let big: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    assert!(big.len() > wire::DEFAULT_MAX_DATAGRAM_SIZE, "must exceed one datagram");
    publisher.publish(&big);

    let got = collect(&mut subscriber, 1);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].1, big, "reassembled bytes must equal the original");
}

#[test]
fn foreign_datagrams_are_dropped_without_disturbing_the_feed() {
    let target = free_udp_addr();
    let mut sub = UdpSubscriber::bind(target, None).unwrap();
    let raw = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    raw.send_to(b"this is not a flux datagram", target).unwrap();
    raw.send_to(&[0u8; wire::UDP_HEADER_SIZE - 1], target).unwrap();
    // Loopback keeps order, so this landing proves the junk was handled first.
    raw_send(&raw, target, 99, 0, b"real", usize::MAX);

    let got = collect(&mut sub, 1);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].1, b"real".to_vec(), "junk is dropped, the good message still lands");
}

#[test]
fn a_restart_clears_a_half_assembled_message_instead_of_writing_into_it() {
    let target = free_udp_addr();
    let mut sub = UdpSubscriber::bind(target, None).unwrap();
    let raw = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();

    // Session 1 leaves seq 5 half-assembled: the first of two fragments only.
    raw_send(&raw, target, 1, 5, &vec![1u8; 2_000], 1);
    // Session 2 reuses seq 5 for a message the stale buffer cannot hold.
    let big = vec![2u8; 40_000];
    raw_send(&raw, target, 2, 5, &big, usize::MAX);

    let got = collect(&mut sub, 1);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].1, big, "the stale partial must be dropped, not written into");
}

#[test]
fn a_feed_goes_nowhere_until_it_is_dialed() {
    let pub_addr = free_udp_addr();
    let mut publisher = UdpPublisher::bind(pub_addr, None).unwrap();
    let mut subscriber = UdpSubscriber::bind((Ipv4Addr::LOCALHOST, 0).into(), None).unwrap();

    // No dial, so no target: nothing is sent and no sequence is consumed.
    assert_eq!(publisher.publish(b"before"), None);

    subscriber.dial(pub_addr).unwrap();
    publisher.poll_subscriptions(|_| true);
    assert_eq!(publisher.publish(b"after"), Some(0));
    let got = collect(&mut subscriber, 1);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].1, b"after".to_vec(), "the first message after a dial is seq 0");
}

#[test]
fn a_dial_the_caller_declines_is_ignored() {
    let pub_addr = free_udp_addr();
    let mut publisher = UdpPublisher::bind(pub_addr, None).unwrap();
    let subscriber = UdpSubscriber::bind((Ipv4Addr::LOCALHOST, 0).into(), None).unwrap();
    subscriber.dial(pub_addr).unwrap();

    // What the RPC does for a source with no TCP connection to the feed.
    assert_eq!(publisher.poll_subscriptions(|ip| ip != IpAddr::V4(Ipv4Addr::LOCALHOST)), 0);
    // No targets, so nothing is sent and no sequence is consumed.
    assert_eq!(publisher.publish(b"denied"), None);
}

#[test]
fn a_refreshed_subscription_is_one_subscriber_not_many() {
    let (mut publisher, mut subscriber, pub_addr) = pair();
    for _ in 0..5 {
        subscriber.dial(pub_addr).unwrap();
        assert_eq!(publisher.poll_subscriptions(|_| true), 0, "a refresh is not a new subscriber");
    }
    publisher.publish(b"x");
    assert_eq!(collect(&mut subscriber, 1).len(), 1);
    // Duplicates would be queued behind the first, so no deadline is needed.
    let extra: usize = (0..10).map(|_| subscriber.poll_with(|_| {})).sum();
    assert_eq!(extra, 0, "delivered once, not five times");
}
