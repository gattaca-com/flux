//! End-to-end tests for raw (unframed) TCP streams via `TcpConnector::with_raw()`.
//!
//! Raw mode means:
//! - No 12-byte frame header is added on sends.
//! - Receives deliver whatever bytes the OS returned in one `read()` call.
//! - Multiple sends may coalesce; one send may be split across reads.
//!
//! Tests therefore accumulate bytes and assert on total content, not message
//! boundaries.

use std::{
    net::{Ipv4Addr, SocketAddr},
    thread,
    time::{Duration, Instant},
};

use flux_network::tcp::{MessagePayload, PollEvent, SendBehavior, TcpConnector};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Bind port 0 and return the allocated address, then drop the probe socket so
/// the address is free for our connectors to use.
fn free_addr() -> SocketAddr {
    let probe =
        std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("probe");
    let addr = probe.local_addr().unwrap();
    drop(probe);
    addr
}

/// Drive `conn`'s event loop, passing every raw-payload chunk to `handler`.
///
/// `handler` receives the bytes and returns `true` when it is satisfied and
/// the loop should stop. Stops unconditionally at `deadline`.
fn pump_until<F>(conn: &mut TcpConnector, deadline: Instant, mut handler: F)
where
    F: FnMut(&[u8]) -> bool,
{
    let mut done = false;
    while !done && Instant::now() < deadline {
        conn.poll_with(|event| {
            if let PollEvent::Message { payload, .. } = event {
                if let MessagePayload::Raw(bytes) = payload {
                    if handler(bytes) {
                        done = true;
                    }
                }
            }
        });
        if !done {
            thread::sleep(Duration::from_millis(1));
        }
    }
}

// ---------------------------------------------------------------------------
// 1. raw_roundtrip
// ---------------------------------------------------------------------------

/// Basic bidirectional send/receive between two raw connectors.
#[test]
fn raw_roundtrip() {
    let addr = free_addr();

    // --- server thread ---
    let server = thread::spawn(move || {
        let mut srv = TcpConnector::default().with_raw();
        srv.listen_at(addr).expect("listen failed");

        // Accept one client.
        let mut stream_tok = None;
        let deadline = Instant::now() + Duration::from_secs(10);
        while stream_tok.is_none() && Instant::now() < deadline {
            srv.poll_with(|event| {
                if let PollEvent::Accept { stream, .. } = event {
                    stream_tok = Some(stream);
                }
            });
            thread::sleep(Duration::from_millis(1));
        }
        let stream_tok = stream_tok.expect("server: no client connected");

        // Receive "hello" from client.
        let mut received = Vec::<u8>::new();
        pump_until(&mut srv, Instant::now() + Duration::from_secs(10), |bytes| {
            received.extend_from_slice(bytes);
            received.len() >= 5
        });
        assert_eq!(&received[..5], b"hello", "server: wrong bytes from client");

        // Send "world" reply.
        srv.write_or_enqueue_with(SendBehavior::Single(stream_tok), |buf| {
            buf.extend_from_slice(b"world");
        });
        // Pump so the send can flush.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            srv.poll_with(|_| {});
            thread::sleep(Duration::from_millis(1));
        }
    });

    // --- client thread ---
    let client = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        let mut cli = TcpConnector::default().with_raw();
        let tok = cli.connect(addr).expect("client: connect failed");

        // Send to server.
        cli.write_or_enqueue_with(SendBehavior::Single(tok), |buf| {
            buf.extend_from_slice(b"hello");
        });

        // Receive "world" reply.
        let mut received = Vec::<u8>::new();
        pump_until(&mut cli, Instant::now() + Duration::from_secs(10), |bytes| {
            received.extend_from_slice(bytes);
            received.len() >= 5
        });
        assert_eq!(&received[..5], b"world", "client: wrong bytes from server");
    });

    server.join().expect("server panicked");
    client.join().expect("client panicked");
}

// ---------------------------------------------------------------------------
// 2. raw_multi_message
// ---------------------------------------------------------------------------

/// Send multiple messages rapidly; verify all bytes arrive even if they
/// coalesce into a single delivery.
#[test]
fn raw_multi_message() {
    let addr = free_addr();

    let messages: &[&[u8]] = &[b"alpha", b"beta", b"gamma", b"delta", b"epsilon"];
    let expected: Vec<u8> = messages.concat();
    let total_len = expected.len();
    let expected_clone = expected.clone();

    let messages_owned: Vec<Vec<u8>> = messages.iter().map(|m| m.to_vec()).collect();

    let server = thread::spawn(move || {
        let mut srv = TcpConnector::default().with_raw();
        srv.listen_at(addr).expect("listen failed");

        // Accept client.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut accepted = false;
        while !accepted && Instant::now() < deadline {
            srv.poll_with(|event| {
                if let PollEvent::Accept { .. } = event {
                    accepted = true;
                }
            });
            thread::sleep(Duration::from_millis(1));
        }
        assert!(accepted, "server: client did not connect");

        // Accumulate until we have all bytes.
        let mut received = Vec::<u8>::new();
        pump_until(&mut srv, Instant::now() + Duration::from_secs(10), |bytes| {
            received.extend_from_slice(bytes);
            received.len() >= total_len
        });

        assert_eq!(
            received.len(),
            total_len,
            "server: wrong total byte count (got {}, want {})",
            received.len(),
            total_len,
        );
        assert_eq!(received, expected_clone, "server: bytes don't match concatenation");
    });

    let client = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        let mut cli = TcpConnector::default().with_raw();
        let tok = cli.connect(addr).expect("client: connect failed");

        for msg in &messages_owned {
            cli.write_or_enqueue_with(SendBehavior::Single(tok), |buf| {
                buf.extend_from_slice(msg);
            });
        }
        // Pump so backlogs can flush.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            cli.poll_with(|_| {});
            thread::sleep(Duration::from_millis(1));
        }
    });

    server.join().expect("server panicked");
    client.join().expect("client panicked");
}

// ---------------------------------------------------------------------------
// 3. raw_large_payload
// ---------------------------------------------------------------------------

/// Send a payload larger than the 32 KiB RX buffer; verify all bytes arrive
/// across potentially multiple `MessagePayload::Raw` callbacks.
#[test]
fn raw_large_payload() {
    const PAYLOAD_LEN: usize = 128 * 1024; // 128 KiB > 32 KiB rx buf

    let addr = free_addr();

    // Build the payload once (sequential bytes so corruption is obvious).
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xFF) as u8).collect();
    let payload_clone = payload.clone();

    let server = thread::spawn(move || {
        let mut srv = TcpConnector::default().with_raw();
        srv.listen_at(addr).expect("listen failed");

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut accepted = false;
        while !accepted && Instant::now() < deadline {
            srv.poll_with(|event| {
                if let PollEvent::Accept { .. } = event {
                    accepted = true;
                }
            });
            thread::sleep(Duration::from_millis(1));
        }
        assert!(accepted, "server: client did not connect");

        let mut received = Vec::<u8>::new();
        pump_until(&mut srv, Instant::now() + Duration::from_secs(15), |bytes| {
            received.extend_from_slice(bytes);
            received.len() >= PAYLOAD_LEN
        });

        assert_eq!(received.len(), PAYLOAD_LEN, "server: wrong payload length");
        assert_eq!(received, payload, "server: payload corrupted");
    });

    let client = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        let mut cli = TcpConnector::default().with_raw();
        let tok = cli.connect(addr).expect("client: connect failed");

        cli.write_or_enqueue_with(SendBehavior::Single(tok), |buf| {
            buf.extend_from_slice(&payload_clone);
        });

        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            cli.poll_with(|_| {});
            thread::sleep(Duration::from_millis(1));
        }
    });

    server.join().expect("server panicked");
    client.join().expect("client panicked");
}

// ---------------------------------------------------------------------------
// 4. raw_broadcast
// ---------------------------------------------------------------------------

/// One listener, multiple clients; broadcast raw bytes to all; verify each
/// client receives the full payload.
#[test]
fn raw_broadcast() {
    const NUM_CLIENTS: usize = 4;
    const MSG: &[u8] = b"broadcast-raw-payload";

    let addr = free_addr();

    // Spawn clients.
    let handles: Vec<_> = (0..NUM_CLIENTS)
        .map(|i| {
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(20));
                let mut cli = TcpConnector::default().with_raw();
                cli.connect(addr).expect("client: connect failed");

                let mut received = Vec::<u8>::new();
                pump_until(&mut cli, Instant::now() + Duration::from_secs(15), |bytes| {
                    received.extend_from_slice(bytes);
                    received.len() >= MSG.len()
                });
                assert_eq!(
                    &received[..MSG.len()],
                    MSG,
                    "client {i}: broadcast payload mismatch"
                );
            })
        })
        .collect();

    let mut srv = TcpConnector::default().with_raw();
    srv.listen_at(addr).expect("listen failed");

    // Accept all clients.
    let mut accepted = 0usize;
    let deadline = Instant::now() + Duration::from_secs(10);
    while accepted < NUM_CLIENTS && Instant::now() < deadline {
        srv.poll_with(|event| {
            if let PollEvent::Accept { .. } = event {
                accepted += 1;
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(accepted, NUM_CLIENTS, "not all clients connected");

    // Broadcast.
    srv.write_or_enqueue_with(SendBehavior::Broadcast, |buf| {
        buf.extend_from_slice(MSG);
    });

    // Pump until all backlogs flush.
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        srv.poll_with(|_| {});
        thread::sleep(Duration::from_millis(1));
    }

    for (i, h) in handles.into_iter().enumerate() {
        h.join().unwrap_or_else(|_| panic!("client {i} panicked"));
    }
}

// ---------------------------------------------------------------------------
// 5. raw_backpressure
// ---------------------------------------------------------------------------

/// Use a tiny socket buffer to force the `SendBacklog` path, then verify all
/// data arrives intact.
#[test]
fn raw_backpressure() {
    const PAYLOAD_LEN: usize = 512 * 1024; // 512 KiB, well above any small buffer

    let addr = free_addr();

    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i & 0xFF) as u8).collect();
    let payload_clone = payload.clone();

    let server = thread::spawn(move || {
        // Tiny socket buffer forces backpressure on sender.
        let mut srv = TcpConnector::default().with_raw().with_socket_buf_size(4096);
        srv.listen_at(addr).expect("listen failed");

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut accepted = false;
        while !accepted && Instant::now() < deadline {
            srv.poll_with(|event| {
                if let PollEvent::Accept { .. } = event {
                    accepted = true;
                }
            });
            thread::sleep(Duration::from_millis(1));
        }
        assert!(accepted, "server: client did not connect");

        let mut received = Vec::<u8>::new();
        pump_until(&mut srv, Instant::now() + Duration::from_secs(30), |bytes| {
            received.extend_from_slice(bytes);
            received.len() >= PAYLOAD_LEN
        });

        assert_eq!(received.len(), PAYLOAD_LEN, "server: wrong payload length");
        assert_eq!(received, payload, "server: payload corrupted");
    });

    let client = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        // Tiny socket buffer on the client side too.
        let mut cli = TcpConnector::default().with_raw().with_socket_buf_size(4096);
        let tok = cli.connect(addr).expect("client: connect failed");

        cli.write_or_enqueue_with(SendBehavior::Single(tok), |buf| {
            buf.extend_from_slice(&payload_clone);
        });

        // Pump until the backlog is fully drained (may take a while with 4 KiB buffers).
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            cli.poll_with(|_| {});
            thread::sleep(Duration::from_millis(1));
        }
    });

    server.join().expect("server panicked");
    client.join().expect("client panicked");
}

// ---------------------------------------------------------------------------
// 6. raw_reconnect
// ---------------------------------------------------------------------------

/// Outbound raw connector auto-reconnects after the server disappears.
///
/// 1. Start listener A; connect client; exchange data.
/// 2. Drop listener A — client sees a disconnect.
/// 3. Start listener B on the same port.
/// 4. Client auto-reconnects; verify `PollEvent::Reconnect` fires.
/// 5. Send data again and verify it arrives on the new server.
#[test]
fn raw_reconnect() {
    let addr = free_addr();

    // ---- first server leg ----
    let leg1 = thread::spawn(move || {
        let mut srv = TcpConnector::default().with_raw();
        srv.listen_at(addr).expect("listen_at leg1 failed");

        // Accept client.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut stream_tok = None;
        while stream_tok.is_none() && Instant::now() < deadline {
            srv.poll_with(|event| {
                if let PollEvent::Accept { stream, .. } = event {
                    stream_tok = Some(stream);
                }
            });
            thread::sleep(Duration::from_millis(1));
        }
        let stream_tok = stream_tok.expect("leg1: client never connected");

        // Receive "ping1".
        let mut received = Vec::<u8>::new();
        pump_until(&mut srv, Instant::now() + Duration::from_secs(5), |bytes| {
            received.extend_from_slice(bytes);
            received.len() >= 5
        });
        assert_eq!(&received[..5], b"ping1", "leg1: wrong first message");

        // Reply "pong1".
        srv.write_or_enqueue_with(SendBehavior::Single(stream_tok), |buf| {
            buf.extend_from_slice(b"pong1");
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            srv.poll_with(|_| {});
            thread::sleep(Duration::from_millis(1));
        }
        // Dropping `srv` closes the connection — client sees disconnect.
    });

    // ---- client (runs for the full test) ----
    let client = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));

        // with_reconnect_interval takes flux_timing::Duration — convert via Into.
        let reconnect_ms: flux_timing::Duration = Duration::from_millis(100).into();
        let mut cli =
            TcpConnector::default().with_raw().with_reconnect_interval(reconnect_ms);
        let tok = cli.connect(addr).expect("client: connect failed");

        // Send "ping1".
        cli.write_or_enqueue_with(SendBehavior::Single(tok), |buf| {
            buf.extend_from_slice(b"ping1");
        });

        // Wait for "pong1".
        let mut received = Vec::<u8>::new();
        pump_until(&mut cli, Instant::now() + Duration::from_secs(10), |bytes| {
            received.extend_from_slice(bytes);
            received.len() >= 5
        });
        assert_eq!(&received[..5], b"pong1", "client: expected pong1 from leg1");

        // Leg1 will drop — wait for Reconnect event (new listener starts shortly).
        let mut reconnected = false;
        let deadline = Instant::now() + Duration::from_secs(15);
        while !reconnected && Instant::now() < deadline {
            cli.poll_with(|event| {
                if let PollEvent::Reconnect { .. } = event {
                    reconnected = true;
                }
            });
            thread::sleep(Duration::from_millis(10));
        }
        assert!(reconnected, "client: never received Reconnect event");

        // Send "ping2" to the new server.
        cli.write_or_enqueue_with(SendBehavior::Single(tok), |buf| {
            buf.extend_from_slice(b"ping2");
        });

        // Keep pumping so the send can flush.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            cli.poll_with(|_| {});
            thread::sleep(Duration::from_millis(1));
        }
    });

    // Wait for leg1 to finish (it drops after sending pong1).
    leg1.join().expect("leg1 panicked");

    // ---- second server leg on the same port ----
    let leg2 = thread::spawn(move || {
        let mut srv2 = TcpConnector::default().with_raw();
        srv2.listen_at(addr).expect("listen_at leg2 failed");

        // Accept the reconnected client.
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut accepted = false;
        while !accepted && Instant::now() < deadline {
            srv2.poll_with(|event| {
                if let PollEvent::Accept { .. } = event {
                    accepted = true;
                }
            });
            thread::sleep(Duration::from_millis(5));
        }
        assert!(accepted, "leg2: client never reconnected");

        // Receive "ping2".
        let mut received = Vec::<u8>::new();
        pump_until(&mut srv2, Instant::now() + Duration::from_secs(15), |bytes| {
            received.extend_from_slice(bytes);
            received.len() >= 5
        });
        assert_eq!(&received[..5], b"ping2", "leg2: wrong second message");
    });

    client.join().expect("client panicked");
    leg2.join().expect("leg2 panicked");
}
