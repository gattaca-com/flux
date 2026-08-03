#![cfg(target_os = "linux")]

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, UdpSocket},
    thread,
    time::{Duration, Instant},
};

use flux_network::udp::{
    DEFAULT_IPV4_MAX_DATAGRAM_SIZE, PublisherEvent, SubscriberEvent, UdpConfig, UdpMulticastConfig,
    UdpPublisher, UdpSendBatchMode, UdpSubscriber,
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

fn poll_network_split(
    publisher: &mut UdpPublisher,
    subscriber: &mut UdpSubscriber,
    received: &mut Vec<Vec<u8>>,
) {
    publisher.poll_control_with(|event| {
        if let PublisherEvent::Disconnect { addr } = event {
            panic!("publisher disconnected subscriber {addr}");
        }
    });
    subscriber.poll_control_with(|event| match event {
        SubscriberEvent::Connected { .. } => {}
        SubscriberEvent::Disconnect { peer_addr } => {
            panic!("subscriber disconnected from publisher {peer_addr}");
        }
        SubscriberEvent::Message { payload, .. } => received.push(payload.to_vec()),
    });
    publisher.poll_data_with(|event| {
        if let PublisherEvent::Disconnect { addr } = event {
            panic!("publisher disconnected subscriber {addr}");
        }
    });
    subscriber.poll_data_with(|event| match event {
        SubscriberEvent::Connected { .. } => {}
        SubscriberEvent::Disconnect { peer_addr } => {
            panic!("subscriber disconnected from publisher {peer_addr}");
        }
        SubscriberEvent::Message { payload, .. } => received.push(payload.to_vec()),
    });
}

#[test]
fn publisher_subscriber_split_control_data_poll_roundtrip() {
    let publisher_addr = unused_addr();
    let subscriber_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let config = UdpConfig::default_for_addr(publisher_addr);
    let mut publisher = UdpPublisher::new_with_config(publisher_addr, config).unwrap();
    let mut subscriber =
        UdpSubscriber::new_with_config(publisher_addr, subscriber_addr, config).unwrap();
    let mut received = Vec::new();

    let deadline = Instant::now() + Duration::from_secs(5);
    while publisher.active_subscribers() != 1 && Instant::now() < deadline {
        poll_network_split(&mut publisher, &mut subscriber, &mut received);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(publisher.active_subscribers(), 1, "subscriber did not complete subscription");
    for _ in 0..10 {
        poll_network_split(&mut publisher, &mut subscriber, &mut received);
        thread::sleep(Duration::from_millis(1));
    }

    let message = b"split data and control poll".to_vec();
    publisher.publish_with(|_| {}, |payload| payload.extend_from_slice(&message));
    let deadline = Instant::now() + Duration::from_secs(5);
    while received.is_empty() && Instant::now() < deadline {
        poll_network_split(&mut publisher, &mut subscriber, &mut received);
    }
    assert_eq!(received, [message]);
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

#[allow(clippy::fn_params_excessive_bools)]
fn publisher_subscriber_batched_roundtrip(
    use_udp_segment: bool,
    use_udp_gro: bool,
    copy_udp_segment_payloads: bool,
    use_multicast: bool,
) {
    let publisher_addr = unused_addr();
    let subscriber_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let mut config = UdpConfig::default_for_addr(publisher_addr);
    config.send_batch_size = 8;
    config.send_batch_max_delay = Duration::from_millis(100);
    config.use_udp_segment = use_udp_segment;
    config.use_udp_gro = use_udp_gro;
    config.copy_udp_segment_payloads = copy_udp_segment_payloads;
    if use_multicast {
        let multicast_port = loop {
            let port = unused_addr().port();
            if port != publisher_addr.port() {
                break port;
            }
        };
        config.multicast = Some(UdpMulticastConfig {
            group: SocketAddrV4::new(Ipv4Addr::new(239, 255, 42, 43), multicast_port),
            interface: Ipv4Addr::LOCALHOST,
            ttl: 1,
            loopback: true,
        });
    }
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
    for _ in 0..10 {
        poll_network(&mut publisher, &mut subscriber, &mut received);
        thread::sleep(Duration::from_millis(1));
    }

    let expected: Vec<Vec<u8>> = [9, 64, 1_232, 1_376, 1_377, 2_752, 4_096, 5_504]
        .into_iter()
        .enumerate()
        .map(|(id, len)| {
            let id = id as u8;
            let mut payload = vec![id; len];
            payload[..8].copy_from_slice(b"batched!");
            payload[8] = id;
            payload
        })
        .collect();
    for message in &expected {
        publisher.publish_with(|_| {}, |payload| payload.extend_from_slice(message));
        poll_network(&mut publisher, &mut subscriber, &mut received);
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while received.len() < expected.len() && Instant::now() < deadline {
        poll_network(&mut publisher, &mut subscriber, &mut received);
        thread::sleep(Duration::from_millis(1));
    }

    received.sort();
    let mut expected_sorted = expected;
    expected_sorted.sort();
    assert_eq!(received, expected_sorted);
    let stats = publisher.stats();
    assert_eq!(stats.max_publications_per_flush, 8);
    assert_eq!(stats.full_batch_flushes, 1);
    assert_eq!(stats.deadline_flushes, 0);
    assert_eq!(stats.explicit_flushes, 0);
    assert_eq!(stats.wire_datagrams, 15);
    if use_udp_segment {
        assert!(stats.send_entries < stats.wire_datagrams);
    } else {
        assert_eq!(stats.send_entries, stats.wire_datagrams);
    }
    let subscriber_stats = subscriber.stats();
    assert_eq!(subscriber_stats.datagrams_received, 15);
    if use_udp_gro {
        assert!(subscriber_stats.gro_packets_received != 0);
        assert!(subscriber_stats.max_gro_segments > 1);
    }
}

#[test]
fn publisher_subscriber_send_batch_roundtrip() {
    publisher_subscriber_batched_roundtrip(false, false, false, false);
}

#[test]
fn publisher_subscriber_udp_segment_roundtrip() {
    publisher_subscriber_batched_roundtrip(true, false, false, false);
}

#[test]
fn publisher_subscriber_udp_segment_gro_roundtrip() {
    publisher_subscriber_batched_roundtrip(true, true, false, false);
}

#[test]
fn publisher_subscriber_copied_udp_segment_gro_roundtrip() {
    publisher_subscriber_batched_roundtrip(true, true, true, false);
}

#[test]
fn publisher_subscriber_multicast_udp_segment_gro_roundtrip() {
    publisher_subscriber_batched_roundtrip(true, true, true, true);
}

#[test]
fn publisher_spin_poll_flushes_partial_batch_at_deadline() {
    let publisher_addr = unused_addr();
    let subscriber_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let batch_delay = Duration::from_millis(100);
    let mut config = UdpConfig::default_for_addr(publisher_addr);
    config.send_batch_size = 4;
    config.send_batch_max_delay = batch_delay;
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
    for _ in 0..10 {
        poll_network(&mut publisher, &mut subscriber, &mut received);
        thread::sleep(Duration::from_millis(1));
    }

    publisher.reset_stats();
    publisher.publish_with(|_| {}, |payload| payload.extend_from_slice(b"deadline"));
    poll_network(&mut publisher, &mut subscriber, &mut received);
    assert_eq!(publisher.stats().publication_flushes, 0);

    thread::sleep(batch_delay + Duration::from_millis(20));
    let deadline = Instant::now() + Duration::from_secs(5);
    while received.is_empty() && Instant::now() < deadline {
        poll_network(&mut publisher, &mut subscriber, &mut received);
    }

    assert_eq!(received, [b"deadline".to_vec()]);
    let stats = publisher.stats();
    assert_eq!(stats.publication_flushes, 1);
    assert_eq!(stats.deadline_flushes, 1);
    assert_eq!(stats.full_batch_flushes, 0);
    assert_eq!(stats.explicit_flushes, 0);
    assert!(stats.max_batch_dwell_ns >= batch_delay.as_nanos() as u64);

    publisher.reset_stats();
    publisher.publish_with(|_| {}, |payload| payload.extend_from_slice(b"explicit"));
    publisher.flush_with(|_| {});
    let stats = publisher.stats();
    assert_eq!(stats.publication_flushes, 1);
    assert_eq!(stats.explicit_flushes, 1);
    assert_eq!(stats.deadline_flushes, 0);
}

#[test]
fn publisher_adaptive_batching_sends_sparse_and_batches_dense_publications() {
    let publisher_addr = unused_addr();
    let subscriber_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let mut config = UdpConfig::default_for_addr(publisher_addr);
    config.send_batch_size = 4;
    config.send_batch_max_delay = Duration::from_millis(100);
    config.send_batch_mode = UdpSendBatchMode::Adaptive;
    config.use_udp_segment = true;
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
    for _ in 0..10 {
        poll_network(&mut publisher, &mut subscriber, &mut received);
        thread::sleep(Duration::from_millis(1));
    }

    publisher.reset_stats();
    publisher.publish_with(|_| {}, |payload| payload.extend_from_slice(b"sparse"));
    for id in 0_u8..4 {
        publisher.publish_with(|_| {}, |payload| payload.extend_from_slice(&[id; 64]));
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while received.len() < 5 && Instant::now() < deadline {
        poll_network(&mut publisher, &mut subscriber, &mut received);
    }

    assert_eq!(received.len(), 5);
    let stats = publisher.stats();
    assert_eq!(stats.adaptive_immediate_flushes, 1);
    assert_eq!(stats.full_batch_flushes, 1);
    assert_eq!(stats.deadline_flushes, 0);
}
