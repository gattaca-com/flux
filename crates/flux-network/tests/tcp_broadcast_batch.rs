use std::{
    net::{Ipv4Addr, SocketAddr},
    thread,
    time::{Duration, Instant},
};

use flux_network::{Group, Network, NetworkEvent, TcpGroupConfig, Token};

const READERS: usize = 3;
const BATCH: usize = 32;
/// Cycled per frame so every batch mixes frames far smaller and larger than
/// the socket buffers.
const SIZES: [usize; 8] = [5, 13, 512, 4096, 1500, 32_768, 200, 9000];
const MAX_BACKLOG: usize = 32;
const BACKLOG_TIMEOUT_MS: u64 = 500;
/// Frames a stalling subscriber takes before it stops reading.
const STALL_AFTER: usize = 10;
/// Well inside the backlog timeout, so the paused subscriber is kept.
const PAUSE: Duration = Duration::from_millis(100);
const BATCH_INTERVAL: Duration = Duration::from_millis(20);
const END: &[u8] = b"end";

/// Frame `seq`: its sequence number, then filler derived from it.
fn frame(seq: usize) -> Vec<u8> {
    let mut frame = vec![(seq & 0xFF) as u8; SIZES[seq % SIZES.len()]];
    frame[..4].copy_from_slice(&(seq as u32).to_le_bytes());
    frame
}

fn subscriber(socket_buf_size: usize, addr: SocketAddr) -> Network {
    let mut conn = Network::default();
    let group = conn
        .add_group(TcpGroupConfig { socket_buf_size: Some(socket_buf_size), ..Default::default() });
    let _ = conn.connect(group, addr);
    conn
}

/// Spawns a subscriber that collects frames until `END`, pausing once for
/// `PAUSE` after `STALL_AFTER` frames if `pause` is set. Returns the frames and
/// how many it held when it paused.
fn spawn_receiver(addr: SocketAddr, pause: bool) -> thread::JoinHandle<(Vec<Vec<u8>>, usize)> {
    thread::spawn(move || {
        let mut conn = subscriber(32768, addr);
        let mut frames: Vec<Vec<u8>> = Vec::new();
        let mut paused_at = 0;
        let mut done = false;
        let deadline = Instant::now() + Duration::from_secs(30);
        while !done && Instant::now() < deadline {
            let worked = conn.poll_with(|event| {
                if let NetworkEvent::Message { payload, .. } = event {
                    if payload == END {
                        done = true;
                    } else {
                        frames.push(payload.to_vec());
                    }
                }
            });
            if pause && paused_at == 0 && frames.len() >= STALL_AFTER {
                paused_at = frames.len();
                thread::sleep(PAUSE);
            } else if !worked {
                thread::sleep(Duration::from_millis(1));
            }
        }
        (frames, paused_at)
    })
}

/// Drives the sender for `for_how_long`, recording every connection it drops.
fn pump(sender: &mut Network, for_how_long: Duration, dropped: &mut Vec<Token>) {
    let deadline = Instant::now() + for_how_long;
    while Instant::now() < deadline {
        let worked = sender.poll_with(|event| {
            if let NetworkEvent::Disconnected { token, .. } = event {
                dropped.push(token);
            }
        });
        if !worked {
            thread::sleep(Duration::from_millis(1));
        }
    }
}

fn accept(sender: &mut Network) -> Token {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let mut accepted = None;
        sender.poll_with(|event| {
            if let NetworkEvent::Accepted { token, .. } = event {
                accepted = Some(token);
            }
        });
        if let Some(token) = accepted {
            return token;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("subscriber never connected");
}

fn broadcast(sender: &mut Network, group: Group, frames: &[Vec<u8>]) {
    sender.broadcast_many_with(group, frames, |buf, frame| buf.extend_from_slice(frame));
}

/// Batches of mixed-size frames go to subscribers reading at different paces:
/// three keep up, one pauses mid-batch and resumes, one stops mid-batch for
/// good. Every reading subscriber gets every frame intact and in order, and
/// only the one that never resumes is evicted by the backlog limit.
#[test]
fn batched_broadcast_survives_stalled_subscribers() {
    let probe =
        std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("probe");
    let addr = probe.local_addr().unwrap();
    drop(probe);

    // Small kernel buffers so a stalled subscriber backs frames up in the
    // sender's own backlog within the first few batches.
    let mut sender = Network::default();
    let group = sender.add_group(TcpGroupConfig {
        socket_buf_size: Some(32 * 1024),
        max_backlog_frames: Some((
            MAX_BACKLOG,
            flux_timing::Duration::from_millis(BACKLOG_TIMEOUT_MS),
        )),
        ..Default::default()
    });
    sender.listen(group, addr).expect("failed to listen");

    // Driven here, and only until it has read part of the first batch.
    let mut stalled = subscriber(4096, addr);
    let stalled_token = accept(&mut sender);

    let mut handles = Vec::new();
    for pause in std::iter::repeat_n(false, READERS).chain([true]) {
        handles.push(spawn_receiver(addr, pause));
        accept(&mut sender);
    }

    let mut dropped = Vec::new();
    let mut stalled_frames = 0;
    let mut seq = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !dropped.contains(&stalled_token) && Instant::now() < deadline {
        let batch: Vec<Vec<u8>> = (seq..seq + BATCH).map(frame).collect();
        seq += BATCH;
        broadcast(&mut sender, group, &batch);
        while stalled_frames < STALL_AFTER && Instant::now() < deadline {
            stalled.poll_with(|event| {
                if let NetworkEvent::Message { .. } = event {
                    stalled_frames += 1;
                }
            });
            pump(&mut sender, Duration::from_millis(1), &mut dropped);
        }
        pump(&mut sender, BATCH_INTERVAL, &mut dropped);
    }
    assert_eq!(dropped, vec![stalled_token], "only the stalled subscriber may be evicted");

    broadcast(&mut sender, group, &[END.to_vec()]);
    let deadline = Instant::now() + Duration::from_secs(10);
    while handles.iter().any(|h| !h.is_finished()) && Instant::now() < deadline {
        pump(&mut sender, Duration::from_millis(1), &mut dropped);
    }
    drop(stalled);

    for (i, handle) in handles.into_iter().enumerate() {
        let (frames, paused_at) = handle.join().unwrap_or_else(|_| panic!("receiver {i} panicked"));
        if i == READERS {
            assert!(paused_at < BATCH, "receiver {i} paused after the first batch");
        }
        assert_eq!(frames.len(), seq, "receiver {i}: missing frames");
        for (expected, got) in frames.iter().enumerate() {
            assert!(
                *got == frame(expected),
                "receiver {i} frame {expected}: corrupted or reordered"
            );
        }
    }
}
