use std::{
    io::Read,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    sync::mpsc::{self, Receiver, Sender, TryRecvError},
    thread,
    time::{Duration, Instant},
};

use flux_network::stream::{SendBehavior, TcpConnector};
use mio::Token;

const FRAME_HEADER_SIZE: usize = core::mem::size_of::<u32>() + core::mem::size_of::<u64>();

/// A collector that reads frames until the connection closes, announcing the
/// arrival of `marker` on the channel it returns.
fn spawn_frame_collector(
    read_delay: Duration,
    marker: Vec<u8>,
) -> (SocketAddr, Receiver<()>, thread::JoinHandle<Vec<Vec<u8>>>) {
    let listener = TcpListener::bind(SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 0)))
        .expect("failed to bind test listener");
    let addr = listener.local_addr().expect("failed to fetch listener addr");
    let (arrived, arrival) = mpsc::channel();

    let handle = thread::spawn(move || {
        let arrived: Sender<()> = arrived;
        let (mut stream, _) = listener.accept().expect("failed to accept connection");
        if !read_delay.is_zero() {
            thread::sleep(read_delay);
        }

        let mut frames = Vec::new();
        loop {
            let mut header = [0_u8; FRAME_HEADER_SIZE];
            match stream.read_exact(&mut header) {
                Ok(()) => {
                    let frame_len = u32::from_le_bytes(
                        header[..core::mem::size_of::<u32>()].try_into().unwrap(),
                    ) as usize;
                    let mut payload = vec![0_u8; frame_len];
                    if stream.read_exact(&mut payload).is_err() {
                        break;
                    }
                    if payload == marker {
                        let _ = arrived.send(());
                    }
                    frames.push(payload);
                }
                Err(_) => break,
            }
        }

        frames
    });

    (addr, arrival, handle)
}

/// Drives the connector until every collector has announced its marker.
///
/// How long the queued bytes take to drain depends on the receiver, so the
/// arrivals are what the test waits for; the deadline is only there to fail
/// loudly instead of hanging.
fn pump_until_arrivals(conn: &mut TcpConnector, arrivals: &[&Receiver<()>]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut arrived = vec![false; arrivals.len()];
    while !arrived.iter().all(|arrived| *arrived) {
        assert!(Instant::now() < deadline, "markers never arrived: {arrived:?}");
        while conn.poll_with(|_| {}) {}
        for (index, arrival) in arrivals.iter().enumerate() {
            arrived[index] |= match arrival.try_recv() {
                Ok(()) => true,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => false,
            };
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn send_payload(conn: &mut TcpConnector, token: Token, payload: &[u8]) {
    conn.write_or_enqueue_with(SendBehavior::Single(token), |buf| {
        buf.extend_from_slice(payload);
    });
}

#[test]
fn queued_messages_flush_on_second_connection_after_backpressure() {
    let marker = b"marker-after-backpressure".to_vec();
    let keepalive = b"fast-keepalive".to_vec();
    let (fast_addr, fast_arrival, fast_handle) =
        spawn_frame_collector(Duration::from_millis(0), keepalive.clone());
    let (slow_addr, slow_arrival, slow_handle) =
        spawn_frame_collector(Duration::from_millis(700), marker.clone());

    let mut conn = TcpConnector::default().with_socket_buf_size(1024);
    let fast_token = conn.connect(fast_addr).expect("failed to connect to fast collector");
    let slow_token = conn.connect(slow_addr).expect("failed to connect to slow collector");
    assert_ne!(fast_token, slow_token);

    // Fill the 2nd socket while the receiver is paused, forcing queue/backpressure
    // path.
    let big = vec![7_u8; 8 * 1024 * 1024];
    send_payload(&mut conn, slow_token, &big);

    send_payload(&mut conn, slow_token, &marker);
    send_payload(&mut conn, fast_token, &keepalive);

    // The slow side starts reading after the delay. If token handling is correct,
    // queued frames should eventually flush and marker should be observed.
    pump_until_arrivals(&mut conn, &[&fast_arrival, &slow_arrival]);
    drop(conn);

    let fast_frames = fast_handle.join().expect("fast collector thread panicked");
    let slow_frames = slow_handle.join().expect("slow collector thread panicked");

    assert!(
        fast_frames.contains(&keepalive),
        "sanity check failed: fast collector did not receive data"
    );
    assert!(
        slow_frames.contains(&marker),
        "slow collector never received marker after backpressure was released"
    );
}
