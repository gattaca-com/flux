//! Replay edge cases: what survives when the write itself finds the peer gone.

use std::{
    io::Read,
    net::{Ipv4Addr, TcpListener, TcpStream},
    thread,
    time::{Duration, Instant},
};

use flux_network::{Network, NetworkEvent, ReplayPolicy, TcpGroupConfig};

const FRAME_HEADER: usize = 12;

fn frames_from(socket: &mut TcpStream, count: usize) -> Vec<Vec<u8>> {
    let mut frames = Vec::with_capacity(count);
    for _ in 0..count {
        let mut header = [0; FRAME_HEADER];
        socket.read_exact(&mut header).unwrap();
        let length = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let mut bytes = vec![0; length];
        socket.read_exact(&mut bytes).unwrap();
        frames.push(bytes);
    }
    frames
}

fn wait_connected(client: &mut Network, deadline: Instant) {
    let mut connected = false;
    while !connected {
        assert!(Instant::now() < deadline, "no connect");
        client.poll_with(|event| connected |= matches!(event, NetworkEvent::Connected { .. }));
        thread::yield_now();
    }
}

/// A send whose socket write fails because the peer reset the connection is
/// kept for replay, ahead of sends made while disconnected.
#[test]
fn a_send_that_fails_on_the_wire_is_replayed() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = Network::default();
    let group =
        client.add_group(TcpGroupConfig { replay: ReplayPolicy::Replay, ..Default::default() });
    let token = client.connect(group, addr);
    let (first, _) = listener.accept().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    wait_connected(&mut client, deadline);

    // Closed with nothing unread, so the peer answers the first write after
    // its FIN with a reset and the write after that fails outright. Without
    // polling in between, the client only learns of the reset from that
    // failing write. Probes before it vanish into the dead socket; the probe
    // whose write fails must be the first one replayed.
    drop(first);
    let mut probe = 0u32;
    let first_failed;
    loop {
        assert!(Instant::now() < deadline, "the peer's reset never surfaced");
        thread::sleep(Duration::from_millis(10));
        assert!(client.send_with(token, |buf| buf.extend_from_slice(&probe.to_le_bytes())));
        if client.currently_disconnected().count() == 1 {
            first_failed = probe;
            break;
        }
        probe += 1;
    }
    let last = probe + 1;
    assert!(client.send_with(token, |buf| buf.extend_from_slice(&last.to_le_bytes())));

    client.force_reconnect();
    let (mut second, _) = listener.accept().unwrap();
    second.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let reader = thread::spawn(move || frames_from(&mut second, 2));
    while !reader.is_finished() {
        assert!(Instant::now() < deadline, "replay never finished");
        client.poll_with(|_| {});
        thread::yield_now();
    }
    let frames = reader.join().unwrap();
    assert_eq!(frames[0], first_failed.to_le_bytes(), "the send that failed on the wire");
    assert_eq!(frames[1], last.to_le_bytes(), "then the send made while disconnected");
}

/// Sends while disconnected are rejected under `Drop`, and a failing write
/// keeps nothing.
#[test]
fn drop_policy_rejects_offline_sends() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = Network::default();
    let group =
        client.add_group(TcpGroupConfig { replay: ReplayPolicy::Drop, ..Default::default() });
    let token = client.connect(group, addr);
    let (first, _) = listener.accept().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    wait_connected(&mut client, deadline);
    client.disconnect(token);
    drop(first);
    assert!(!client.send_with(token, |buf| buf.extend_from_slice(b"lost")));
    assert_eq!(client.currently_disconnected().count(), 1);
}
