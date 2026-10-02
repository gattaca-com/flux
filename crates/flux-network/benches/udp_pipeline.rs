//! Sender and receiver on separate pinned threads, TCP vs reliable UDP.
//!
//! Reports one-way latency (receiver clock minus the sender timestamp carried
//! in every message, same host so no clock skew) as p50/p99, throughput, and
//! for the loss scenario how many datagrams were retransmitted.
//!
//! Scenarios:
//! - `paced`: at most one message every 100 µs, with bounded outstanding sends.
//! - `burst`: as fast as the send window allows. Throughput and queueing.
//! - `loss1`: UDP through a relay that drops exactly one data datagram of a 2
//!   MiB message. Recovery should resend one datagram, not the message.
//! - `bcast`: one sender broadcasting to 8 receivers on one listener socket.
//!
//! Run with `cargo bench -p flux-network --bench udp_pipeline`.

use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use flux_communication::cleanup_shmem;
use flux_network::{
    Group, GroupConfig, Network, NetworkEvent, NetworkTelemetry, ReplayPolicy, TcpGroupConfig,
    UdpConfig, UdpGroupConfig,
};
use flux_timing::Nanos;
use flux_utils::directories::shmem_dir;

/// The largest size fits in half a 4 MiB receive buffer (`net.core.rmem_max`
/// default on many hosts), so no scenario but `loss1` ever drops a datagram.
const SIZES: [(&str, usize); 3] = [("2k", 2 * 1024), ("64k", 64 * 1024), ("1m", 1024 * 1024)];
const PACED_MSGS: usize = 2000;
const PACE: Duration = Duration::from_micros(100);
const BCAST_PEERS: usize = 8;
const BIG_SOCKET_BUF: usize = 16 * 1024 * 1024;
/// Shared-memory telemetry queues of the `udp+tel` transport live here.
const APP_NAME: &str = "udp-pipeline-bench";

fn free_addr() -> SocketAddr {
    loop {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        if UdpSocket::bind(addr).is_ok() {
            return addr;
        }
    }
}

/// Core list captured before any thread is pinned: `get_core_ids` reports the
/// calling thread's own mask, which shrinks to one core once pinned.
static CORES: OnceLock<Vec<core_affinity::CoreId>> = OnceLock::new();

fn pin(index_from_end: usize) {
    let ids = CORES.get_or_init(|| core_affinity::get_core_ids().unwrap_or_default());
    if ids.len() > index_from_end {
        core_affinity::set_for_current(ids[ids.len() - 1 - index_from_end]);
    }
}

fn udp_config() -> UdpConfig {
    UdpConfig { max_message_size: 4 * 1024 * 1024, ..UdpConfig::lan() }
}

fn transports() -> [(&'static str, GroupConfig); 3] {
    [
        (
            "tcp",
            TcpGroupConfig {
                aligned_payloads: true,
                replay: ReplayPolicy::Replay,
                ..Default::default()
            }
            .into(),
        ),
        ("udp", UdpGroupConfig { udp: udp_config(), ..Default::default() }.into()),
        (
            "udp+tel",
            UdpGroupConfig {
                udp: udp_config(),
                telemetry: NetworkTelemetry::Enabled { app_name: APP_NAME },
                ..Default::default()
            }
            .into(),
        ),
    ]
}

fn connector(config: GroupConfig) -> (Network, Group) {
    let mut network = Network::default();
    let group = network.add_group(config.with_socket_buf_size(BIG_SOCKET_BUF));
    (network, group)
}

/// Payload bytes a receive buffer holds for ~1.2 KB datagrams. The kernel
/// caps the grant at `net.core.rmem_max`, reports twice the granted size, and
/// spends about half of it on skb bookkeeping.
fn recv_capacity() -> usize {
    use std::os::fd::AsRawFd;
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    big_buffers(&socket);
    let mut granted: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            std::ptr::from_mut(&mut granted).cast(),
            std::ptr::from_mut(&mut len),
        );
    }
    granted as usize / 4
}

/// Message count and bound on outstanding messages for a burst of `size`.
/// At most half the receive buffer is in flight, so a burst never overflows
/// the receiver and the numbers measure the transport, not loss recovery.
fn burst_plan(size: usize) -> (usize, usize) {
    let count = (64 * 1024 * 1024 / size).clamp(16, 4096);
    let window = (recv_capacity() / 2 / size).clamp(1, 256);
    (count, window)
}

struct Stats {
    latencies_ns: Vec<u64>,
    elapsed: Duration,
    bytes: usize,
}

impl Stats {
    fn row(&self, name: &str, extra: &str) {
        let mut l = self.latencies_ns.clone();
        l.sort_unstable();
        let pct = |p: f64| l[((l.len() - 1) as f64 * p) as usize] as f64 / 1000.0;
        let mibps = self.bytes as f64 / self.elapsed.as_secs_f64() / (1024.0 * 1024.0);
        println!(
            "{name:<28} n={:<6} p50={:>9.1}µs p99={:>9.1}µs max={:>9.1}µs {mibps:>9.0} MiB/s {extra}",
            l.len(),
            pct(0.5),
            pct(0.99),
            pct(1.0),
        );
    }
}

/// Server listens, `clients` dial it; returns once every client is accepted
/// and connected. The server is the sender for every scenario.
fn connect(
    transport: &GroupConfig,
    listen: SocketAddr,
    dial: SocketAddr,
    clients: usize,
) -> (Network, Group, Vec<Network>, Vec<mio::Token>) {
    let (mut server, server_group) = connector(transport.clone());
    server.listen(server_group, listen).unwrap();
    let mut receivers: Vec<Network> = (0..clients)
        .map(|_| {
            let (mut network, group) = connector(transport.clone());
            let _ = network.connect(group, dial);
            network
        })
        .collect();
    let mut accepted = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted.len() < clients ||
        receivers.iter().any(|r| r.currently_disconnected().count() != 0)
    {
        assert!(Instant::now() < deadline, "handshake");
        server.poll_with(|e| {
            if let NetworkEvent::Accepted { token: stream, .. } = e {
                accepted.push(stream);
            }
        });
        for r in &mut receivers {
            r.poll_with(|_| {});
        }
    }
    (server, server_group, receivers, accepted)
}

#[derive(Clone)]
struct Scenario {
    transport: GroupConfig,
    listen: SocketAddr,
    dial: SocketAddr,
    clients: usize,
    size: usize,
    count: usize,
    /// Minimum gap between sends; `None` sends as fast as `window` allows.
    pace: Option<Duration>,
    /// Bound on messages sent but not yet received by every client.
    window: usize,
}

/// Sends `count` messages from the server while the receiver thread counts
/// them and records one-way latency.
fn run(sc: Scenario) -> Stats {
    let Scenario { transport, listen, dial, clients, size, count, pace, window } = sc;
    let (mut server, server_group, receivers, _accepted) =
        connect(&transport, listen, dial, clients);
    let msg = vec![0x5Au8; size];
    let stop = Arc::new(AtomicBool::new(false));
    let got = Arc::new(AtomicUsize::new(0));
    // Set once the receiver is pinned and polling, so the first burst does
    // not sit in the socket buffer long enough to trip the initial RTO.
    let ready = Arc::new(AtomicBool::new(false));
    let (done_tx, done_rx) = mpsc::channel();
    let expected = count * clients;
    let rx_thread = {
        let stop = stop.clone();
        let got = got.clone();
        let ready = ready.clone();
        let mut receivers = receivers;
        thread::spawn(move || {
            pin(0);
            let mut lat = Vec::with_capacity(expected);
            for r in &mut receivers {
                r.poll_with(|_| {});
            }
            ready.store(true, Ordering::Release);
            while lat.len() < expected && !stop.load(Ordering::Relaxed) {
                for r in &mut receivers {
                    r.poll_with(|e| {
                        if let NetworkEvent::Message { send_ts, .. } = e {
                            lat.push(Nanos::now().0.saturating_sub(send_ts.0));
                        }
                    });
                }
                got.store(lat.len(), Ordering::Relaxed);
            }
            let _ = done_tx.send(());
            lat
        })
    };

    pin(1);
    while !ready.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }
    let start = Instant::now();
    let mut sent = 0;
    let mut next_send = start;
    while done_rx.try_recv().is_err() {
        let received = got.load(Ordering::Relaxed) / clients;
        let now = Instant::now();
        if sent < count && sent - received < window && pace.is_none_or(|_| now >= next_send) {
            server.broadcast_with(server_group, |b| b.extend_from_slice(&msg));
            sent += 1;
            if let Some(p) = pace {
                next_send = now + p;
            }
        }
        server.poll_with(|_| {});
        if now.duration_since(start) > Duration::from_secs(60) {
            stop.store(true, Ordering::Relaxed);
        }
    }
    let elapsed = start.elapsed();
    let latencies_ns = rx_thread.join().unwrap();
    assert_eq!(latencies_ns.len(), expected, "receiver timed out");
    Stats { latencies_ns, elapsed, bytes: expected * size }
}

/// Raises the relay's socket buffers so it never drops a burst itself.
fn big_buffers(socket: &UdpSocket) {
    use std::os::fd::AsRawFd;
    let size: libc::c_int = 16 * 1024 * 1024;
    for opt in [libc::SO_RCVBUF, libc::SO_SNDBUF] {
        unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                opt,
                std::ptr::from_ref(&size).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

/// Forwards between one client and the server. Drops the `drop_nth` data
/// datagram it sees exactly once, and counts data datagrams and how many of
/// them repeat an already-seen sequence.
struct Relay {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    data: Arc<AtomicUsize>,
    retransmits: Arc<AtomicUsize>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Relay {
    fn start(server: SocketAddr, drop_nth: usize) -> Self {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        big_buffers(&socket);
        socket.set_read_timeout(Some(Duration::from_millis(5))).unwrap();
        let addr = socket.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let data = Arc::new(AtomicUsize::new(0));
        let retransmits = Arc::new(AtomicUsize::new(0));
        let (stop_c, data_c, retx_c) = (stop.clone(), data.clone(), retransmits.clone());
        let handle = thread::spawn(move || {
            let mut buf = vec![0u8; 65_536];
            let mut client: Option<SocketAddr> = None;
            let mut seen: HashSet<(u32, u64)> = HashSet::new();
            let mut n_data = 0;
            while !stop_c.load(Ordering::Relaxed) {
                let Ok((n, from)) = socket.recv_from(&mut buf) else { continue };
                let to = if from == server {
                    let Some(c) = client else { continue };
                    c
                } else {
                    client = Some(from);
                    server
                };
                // Wire layout: magic, kind in the low nibble of byte 2, session
                // at [3..7], sequence at [7..15]. Only data carries a payload.
                if n >= 29 && buf[2] & 0x0f == 1 {
                    n_data += 1;
                    data_c.fetch_add(1, Ordering::Relaxed);
                    let session = u32::from_le_bytes(buf[3..7].try_into().unwrap());
                    let seq = u64::from_le_bytes(buf[7..15].try_into().unwrap());
                    if !seen.insert((session, seq)) {
                        retx_c.fetch_add(1, Ordering::Relaxed);
                    }
                    if n_data == drop_nth {
                        continue;
                    }
                }
                let _ = socket.send_to(&buf[..n], to);
            }
        });
        Self { addr, stop, data, retransmits, handle: Some(handle) }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.take().unwrap().join().unwrap();
    }
}

fn main() {
    pin(usize::MAX);
    cleanup_shmem(&shmem_dir(APP_NAME));
    println!("== paced: one message per 100µs, one receiver ==");
    for (size_name, size) in SIZES {
        for (name, transport) in transports() {
            let addr = free_addr();
            let s = run(Scenario {
                transport,
                listen: addr,
                dial: addr,
                clients: 1,
                size,
                count: PACED_MSGS,
                pace: Some(PACE),
                window: burst_plan(size).1,
            });
            s.row(&format!("paced/{name}/{size_name}"), "");
        }
    }

    println!("\n== burst: bounded outstanding, one receiver ==");
    for (size_name, size) in SIZES {
        let (count, window) = burst_plan(size);
        for (name, transport) in transports() {
            let addr = free_addr();
            let s = run(Scenario {
                transport,
                listen: addr,
                dial: addr,
                clients: 1,
                size,
                count,
                pace: None,
                window,
            });
            s.row(&format!("burst/{name}/{size_name}"), &format!("window={window}"));
        }
    }

    println!("\n== loss1: one 2 MiB message, exactly one datagram dropped by a relay ==");
    {
        let server_addr = free_addr();
        // Drop the 900th of 1789 data datagrams.
        let relay = Relay::start(server_addr, 900);
        let s = run(Scenario {
            transport: UdpGroupConfig { udp: udp_config(), ..Default::default() }.into(),
            listen: server_addr,
            dial: relay.addr,
            clients: 1,
            size: 2 * 1024 * 1024,
            count: 1,
            pace: None,
            window: 1,
        });
        let data = relay.data.load(Ordering::Relaxed);
        let retx = relay.retransmits.load(Ordering::Relaxed);
        drop(relay);
        s.row("loss1/udp/2m", &format!("datagrams={data} retransmitted={retx}"));
    }

    println!("\n== bcast: one sender, {BCAST_PEERS} receivers on one listener ==");
    for (size_name, size) in SIZES {
        let (count, window) = burst_plan(size);
        let count = count / BCAST_PEERS;
        for (name, transport) in transports() {
            let addr = free_addr();
            let s = run(Scenario {
                transport,
                listen: addr,
                dial: addr,
                clients: BCAST_PEERS,
                size,
                count,
                pace: None,
                window,
            });
            s.row(&format!("bcast/{name}/{size_name}"), &format!("window={window}"));
        }
    }
    cleanup_shmem(&shmem_dir(APP_NAME));
}
