#![cfg(target_os = "linux")]

const IO: flux_network::udp::UdpIo =
    flux_network::udp::UdpIo::Uring(flux_network::udp::UringConfig {
        send_entries: 64,
        recv_entries: 32,
    });

include!("support/udp_connector.rs");

#[test]
fn duplex_progress_with_minimum_ring_capacity() {
    let config = UdpConfig {
        io: flux_network::udp::UdpIo::Uring(flux_network::udp::UringConfig {
            send_entries: 1,
            recv_entries: 1,
        }),
        ..UdpConfig::lan()
    };
    let mut server = NetworkDriver::default().with_transport(Transport::Udp(config));
    let mut client = NetworkDriver::default().with_transport(Transport::Udp(config));
    let (accepted, outbound) = connect_pair(&mut server, &mut client, free_addr());
    for id in 0..32 {
        let payload = make_msg(id, 64 * 1024);
        server.write_or_enqueue_with(SendBehavior::Single(accepted), |buf| {
            buf.extend_from_slice(&payload);
        });
        client.write_or_enqueue_with(SendBehavior::Single(outbound), |buf| {
            buf.extend_from_slice(&payload);
        });
        let (mut server_got, mut client_got) = (false, false);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !server_got || !client_got {
            for (driver, got) in [(&mut server, &mut server_got), (&mut client, &mut client_got)] {
                driver.poll_with(|event| match event {
                    PollEvent::Message { payload: bytes, .. } => {
                        assert_eq!(bytes, payload);
                        assert!(!*got);
                        *got = true;
                    }
                    PollEvent::Disconnect { .. } => panic!("duplex peer disconnected"),
                    _ => {}
                });
            }
            assert!(Instant::now() < deadline, "duplex message {id} stalled");
        }
    }
}

#[test]
fn mixed_backends_interoperate() {
    for server_uring in [false, true] {
        let config = |uring| UdpConfig {
            io: if uring { IO } else { flux_network::udp::UdpIo::Syscall },
            ..UdpConfig::lan()
        };
        let mut server =
            NetworkDriver::default().with_transport(Transport::Udp(config(server_uring)));
        let mut client =
            NetworkDriver::default().with_transport(Transport::Udp(config(!server_uring)));
        let addr = free_addr();
        let (accepted, outbound) = connect_pair(&mut server, &mut client, addr);
        let payload = make_msg(17, 256 * 1024);
        server.write_or_enqueue_with(SendBehavior::Single(accepted), |buf| {
            buf.extend_from_slice(&payload);
        });
        client.write_or_enqueue_with(SendBehavior::Single(outbound), |buf| {
            buf.extend_from_slice(&payload);
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        let (mut server_got, mut client_got) = (false, false);
        while !server_got || !client_got {
            server.poll_with(|e| {
                if let PollEvent::Message { payload: got, .. } = e {
                    assert_eq!(got, payload);
                    assert!(!server_got);
                    server_got = true;
                }
            });
            client.poll_with(|e| {
                if let PollEvent::Message { payload: got, .. } = e {
                    assert_eq!(got, payload);
                    assert!(!client_got);
                    client_got = true;
                }
            });
            assert!(Instant::now() < deadline);
        }
    }
}

#[test]
fn sustained_large_messages_keep_acknowledgements_current() {
    const COUNT: usize = 64;
    let addr = free_addr();
    let mut server = udp(UdpConfig::lan()).with_socket_buf_size(16 * 1024 * 1024);
    let mut client = udp(UdpConfig::lan()).with_socket_buf_size(16 * 1024 * 1024);
    let (accepted, _) = connect_pair(&mut server, &mut client, addr);
    let got = Arc::new(AtomicUsize::new(0));
    let progress = got.clone();
    let deadline = Instant::now() + Duration::from_secs(10);
    let receiver = thread::spawn(move || {
        let mut seen = [false; COUNT];
        while progress.load(Ordering::Relaxed) < COUNT {
            client.poll_with(|event| {
                if let PollEvent::Message { payload, .. } = event {
                    assert_eq!(payload.len(), 2 * 1024 * 1024);
                    let id = msg_id(payload) as usize;
                    assert!(!seen[id]);
                    assert!(payload[4..].iter().all(|b| *b == 0x5a));
                    seen[id] = true;
                    progress.fetch_add(1, Ordering::Relaxed);
                }
            });
            assert!(
                Instant::now() < deadline,
                "large-message receiver stalled at {}",
                progress.load(Ordering::Relaxed)
            );
        }
    });
    let mut sent = 0;
    let mut payload = vec![0x5a; 2 * 1024 * 1024];
    while got.load(Ordering::Relaxed) < COUNT {
        if sent < COUNT && sent - got.load(Ordering::Relaxed) < 4 {
            payload[..4].copy_from_slice(&(sent as u32).to_le_bytes());
            server.write_or_enqueue_with(SendBehavior::Single(accepted), |buf| {
                buf.extend_from_slice(&payload);
            });
            sent += 1;
        }
        server.poll_with(|event| {
            assert!(
                !matches!(event, PollEvent::Disconnect { .. }),
                "sender disconnected at {sent} sent, {} received",
                got.load(Ordering::Relaxed)
            );
        });
        assert!(
            Instant::now() < deadline,
            "large-message sender stalled at {sent} sent, {} received",
            got.load(Ordering::Relaxed)
        );
    }
    receiver.join().unwrap();
}
