use std::{
    net::{Ipv4Addr, SocketAddr},
    thread,
    time::Duration,
};

use flux_network::tcp::{PollEvent, SendBehavior, TcpConnector};

const NUM_RECEIVERS: usize = 4;
const BURST_SIZE: usize = 20;
const PAYLOAD_SIZE: usize = 256 * 1024; // 256 KiB per message

/// Spawns a receiver thread that connects to `addr` via TcpConnector and
/// collects frames via poll_with until the sender disconnects.
fn spawn_receiver(addr: SocketAddr) -> thread::JoinHandle<Vec<Vec<u8>>> {
    thread::spawn(move || {
        // 32 KiB socket buf constrains the receiver (2× smaller than the
        // default 128 KiB recv buf) while staying >= loopback MSS (~32 KiB)
        // so TCP window updates still fire.
        let mut conn = TcpConnector::default().with_socket_buf_size(32768);
        conn.connect(addr).expect("receiver: failed to connect");

        let mut frames: Vec<Vec<u8>> = Vec::new();
        let mut disconnected = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);

        while !disconnected && std::time::Instant::now() < deadline {
            conn.poll_with(|event| match event {
                PollEvent::Message { payload, .. } => {
                    frames.push(payload.to_vec());
                }
                PollEvent::Disconnect { .. } => {
                    disconnected = true;
                }
                _ => {}
            });
            thread::sleep(Duration::from_millis(1));
        }

        frames
    })
}

fn pump(conn: &mut TcpConnector, for_how_long: Duration) {
    let deadline = std::time::Instant::now() + for_how_long;
    while std::time::Instant::now() < deadline {
        while conn.poll_with(|_| {}) {}
        thread::sleep(Duration::from_millis(1));
    }
}

/// Broadcast a burst of large messages to multiple receivers.
///
/// Sender listens via TcpConnector, receivers connect via TcpConnector.
/// The sender uses a 4 KiB socket buffer to force backpressure and backlog
/// queueing on the send side.  Receivers use a 32 KiB socket buffer —
/// small enough to constrain the pipe (2× below the default 128 KiB) but
/// at or above the loopback MSS (~32 KiB) so TCP window updates still fire.
///
/// Verifies that every receiver gets every frame intact.
#[test]
fn broadcast_burst_to_multiple_receivers() {
    let probe =
        std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("probe");
    let addr = probe.local_addr().unwrap();
    drop(probe);

    // Small send buffer on the sender forces backpressure after the first
    // partial write of each 256 KiB frame.
    let mut sender = TcpConnector::default().with_socket_buf_size(4096);
    sender.listen_at(addr).expect("failed to listen");

    let handles: Vec<_> = (0..NUM_RECEIVERS).map(|_| spawn_receiver(addr)).collect();

    // Accept all inbound connections.
    let mut accepted = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while accepted < NUM_RECEIVERS && std::time::Instant::now() < deadline {
        sender.poll_with(|event| {
            if let PollEvent::Accept { .. } = event {
                accepted += 1;
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(accepted, NUM_RECEIVERS, "not all receivers connected");

    // Fire a burst: 20 broadcasts of 256 KiB each.
    // Each payload is tagged with its sequence number so we can verify order
    // and completeness on the receiver side.
    for seq in 0..BURST_SIZE {
        sender.write_or_enqueue_with(SendBehavior::Broadcast, |buf| {
            buf.extend_from_slice(&(seq as u32).to_le_bytes());
            buf.resize(PAYLOAD_SIZE, (seq & 0xFF) as u8);
        });
        // Pump between writes so mio can flush backlogs.
        while sender.poll_with(|_| {}) {}
    }

    // Pump until all backlogs are drained.
    pump(&mut sender, Duration::from_secs(5));

    // Drop sender so receivers see disconnect.
    drop(sender);

    for (i, handle) in handles.into_iter().enumerate() {
        let frames = handle.join().unwrap_or_else(|_| panic!("receiver {i} panicked"));

        assert_eq!(
            frames.len(),
            BURST_SIZE,
            "receiver {i}: expected {BURST_SIZE} frames, got {}",
            frames.len()
        );

        for (seq, frame) in frames.iter().enumerate() {
            assert_eq!(frame.len(), PAYLOAD_SIZE, "receiver {i} frame {seq}: wrong payload size");

            let got_seq = u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
            assert_eq!(got_seq, seq, "receiver {i}: frame out of order");

            let expected_fill = (seq & 0xFF) as u8;
            assert!(
                frame[4..].iter().all(|&b| b == expected_fill),
                "receiver {i} frame {seq}: payload corrupted"
            );
        }
    }
}
