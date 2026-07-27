#![cfg(target_os = "linux")]

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener, UdpSocket},
    thread,
    time::{Duration, Instant},
};

use flux_network::udp::{
    DEFAULT_IPV4_MAX_DATAGRAM_SIZE, PublisherEvent, SubscriberEvent, UdpConfig, UdpPublisher,
    UdpSubscriber,
};

fn unused_addr() -> SocketAddr {
    let tcp = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = tcp.local_addr().unwrap();
    let udp = UdpSocket::bind(addr).unwrap();
    drop((tcp, udp));
    addr
}

fn poll_network(
    publisher: &mut UdpPublisher,
    subscriber: &mut UdpSubscriber,
    received: &mut Vec<Vec<u8>>,
) {
    publisher.poll_with(|event| {
        if let PublisherEvent::Disconnect { addr } = event {
            panic!("publisher disconnected subscriber {addr}");
        }
    });
    subscriber.poll_with(|event| match event {
        SubscriberEvent::Connected { .. } => {}
        SubscriberEvent::Disconnect { peer_addr } => {
            panic!("subscriber disconnected from publisher {peer_addr}");
        }
        SubscriberEvent::Message { payload, .. } => received.push(payload.to_vec()),
    });
}

#[test]
fn publisher_subscriber_roundtrip() {
    let publisher_addr = unused_addr();
    let subscriber_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let config = UdpConfig::default_for_addr(publisher_addr);
    let mut publisher = UdpPublisher::new_with_config(publisher_addr, config).unwrap();
    let mut subscriber =
        UdpSubscriber::new_with_config(publisher_addr, subscriber_addr, config).unwrap();
    let mut received = Vec::new();

    let deadline = Instant::now() + Duration::from_secs(5);
    while publisher.active_subscribers() != 1 && Instant::now() < deadline {
        poll_network(&mut publisher, &mut subscriber, &mut received);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(publisher.active_subscribers(), 1, "subscriber did not complete subscription");

    // The publisher can activate a subscriber before its initial State frame is
    // consumed. UDP arriving in that accepted startup window is recovered by
    // progress and repair; wait here so this test exercises direct UDP delivery.
    for _ in 0..10 {
        poll_network(&mut publisher, &mut subscriber, &mut received);
        thread::sleep(Duration::from_millis(1));
    }

    let small = b"single UDP datagram".to_vec();
    let fragmented = vec![0x5a; DEFAULT_IPV4_MAX_DATAGRAM_SIZE * 3 + 17];
    publisher.publish_with(|_| {}, |payload| payload.extend_from_slice(&small));
    publisher.publish_with(|_| {}, |payload| payload.extend_from_slice(&fragmented));

    let deadline = Instant::now() + Duration::from_secs(5);
    while received.len() < 2 && Instant::now() < deadline {
        poll_network(&mut publisher, &mut subscriber, &mut received);
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(received.len(), 2, "subscriber did not receive both published messages");
    assert!(received.iter().any(|payload| payload == &small));
    assert!(received.iter().any(|payload| payload == &fragmented));
}
