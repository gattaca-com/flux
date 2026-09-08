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
//! `FLUX_BENCH_TRANSPORT=udp,uring` selects backends; `FLUX_BENCH_REVERSE=1`
//! reverses their order. `FLUX_BENCH_SCALE=16` increases burst/broadcast sample
//! counts. `FLUX_BENCH_SIZE=2m` filters paced/burst/broadcast sizes. CPU ns/B
//! sums sender and receiver thread CPU time; it excludes the loss relay.
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

use flux_network::{NetworkDriver, PollEvent, SendBehavior, Transport, UdpConfig};
use flux_timing::Nanos;

const SIZES: [(&str, usize); 3] = [("2k", 2 * 1024), ("64k", 64 * 1024), ("2m", 2 * 1024 * 1024)];
const PACED_MSGS: usize = 2000;
const PACE: Duration = Duration::from_micros(100);
const BCAST_PEERS: usize = 8;
const BIG_SOCKET_BUF: usize = 16 * 1024 * 1024;

fn free_addr() -> SocketAddr {
    UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap()
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

fn transports() -> Vec<(&'static str, Transport)> {
    let mut transports = vec![("tcp", Transport::default()), ("udp", Transport::Udp(udp_config()))];
    #[cfg(target_os = "linux")]
    transports.push((
        "uring",
        Transport::Udp(UdpConfig {
            io: flux_network::udp::UdpIo::Uring(flux_network::udp::UringConfig::default()),
            ..udp_config()
        }),
    ));
    if let Ok(filter) = std::env::var("FLUX_BENCH_TRANSPORT") {
        transports.retain(|(name, _)| filter.split(',').any(|selected| selected == *name));
        assert!(!transports.is_empty(), "unknown FLUX_BENCH_TRANSPORT");
    }
    if std::env::var_os("FLUX_BENCH_REVERSE").is_some() {
        transports.reverse();
    }
    transports
}

fn sizes() -> impl Iterator<Item = (&'static str, usize)> {
    let filter = std::env::var("FLUX_BENCH_SIZE").ok();
    SIZES.into_iter().filter(move |(name, _)| filter.as_deref().is_none_or(|s| s == *name))
}

fn connector(transport: Transport) -> NetworkDriver {
    NetworkDriver::default().with_transport(transport).with_socket_buf_size(BIG_SOCKET_BUF)
}

/// Message count and bound on outstanding messages for a burst of `size`.
fn burst_plan(size: usize) -> (usize, usize) {
    let count = (64 * 1024 * 1024 / size).clamp(16, 4096);
    let window = (UdpConfig::default().send_window / 2 / size.div_ceil(1171)).clamp(1, 256);
    let scale: usize = std::env::var("FLUX_BENCH_SCALE")
        .map_or(1, |s| s.parse().expect("invalid FLUX_BENCH_SCALE"));
    assert!((1..=64).contains(&scale));
    (count * scale, window)
}

struct Stats {
    latencies_ns: Vec<u64>,
    elapsed: Duration,
    bytes: usize,
    cpu: Duration,
}

impl Stats {
    fn row(&self, name: &str, extra: &str) {
        let mut l = self.latencies_ns.clone();
        l.sort_unstable();
        let pct = |p: f64| l[((l.len() - 1) as f64 * p) as usize] as f64 / 1000.0;
        let mibps = self.bytes as f64 / self.elapsed.as_secs_f64() / (1024.0 * 1024.0);
        println!(
            "{name:<28} n={:<6} p50={:>9.1}µs p99={:>9.1}µs max={:>9.1}µs {mibps:>9.0} MiB/s cpu={:.3}ns/B {extra}",
            l.len(),
            pct(0.5),
            pct(0.99),
            pct(1.0),
            self.cpu.as_nanos() as f64 / self.bytes as f64,
        );
    }
}

/// Server listens, `clients` dial it; returns once every client is accepted
/// and connected. The server is the sender for every scenario.
fn connect(
    transport: Transport,
    listen: SocketAddr,
    dial: SocketAddr,
    clients: usize,
) -> (NetworkDriver, Vec<NetworkDriver>, Vec<mio::Token>) {
    let mut server = connector(transport);
    server.listen_at(listen).unwrap();
    let mut receivers: Vec<NetworkDriver> = (0..clients).map(|_| connector(transport)).collect();
    for r in &mut receivers {
        r.connect(dial).unwrap();
    }
    let mut accepted = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted.len() < clients ||
        receivers.iter().any(|r| r.currently_disconnected().count() != 0)
    {
        assert!(Instant::now() < deadline, "handshake");
        server.poll_with(|e| {
            if let PollEvent::Accept { stream, .. } = e {
                accepted.push(stream);
            }
        });
        for r in &mut receivers {
            r.poll_with(|_| {});
        }
    }
    (server, receivers, accepted)
}

#[derive(Clone, Copy)]
struct Scenario {
    transport: Transport,
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

fn thread_cpu_time() -> Duration {
    let mut time: libc::timespec = unsafe { std::mem::zeroed() };
    assert_eq!(
        unsafe {
            libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, std::ptr::from_mut(&mut time))
        },
        0
    );
    Duration::new(time.tv_sec as u64, time.tv_nsec as u32)
}

/// Sends `count` messages from the server while the receiver thread counts
/// them and records one-way latency.
fn run(sc: Scenario) -> Stats {
    let Scenario { transport, listen, dial, clients, size, count, pace, window } = sc;
    let (mut server, receivers, _accepted) = connect(transport, listen, dial, clients);
    let msg = vec![0x5Au8; size];
    let stop = Arc::new(AtomicBool::new(false));
    let got = Arc::new(AtomicUsize::new(0));
    let (done_tx, done_rx) = mpsc::channel();
    let expected = count * clients;
    let rx_thread = {
        let stop = stop.clone();
        let got = got.clone();
        let mut receivers = receivers;
        thread::spawn(move || {
            pin(0);
            let mut lat = Vec::with_capacity(expected);
            let cpu_start = thread_cpu_time();
            while lat.len() < expected && !stop.load(Ordering::Relaxed) {
                for r in &mut receivers {
                    r.poll_with(|e| {
                        if let PollEvent::Message { send_ts, .. } = e {
                            lat.push(Nanos::now().0.saturating_sub(send_ts.0));
                        }
                    });
                }
                got.store(lat.len(), Ordering::Relaxed);
            }
            let _ = done_tx.send(());
            (lat, thread_cpu_time() - cpu_start)
        })
    };

    pin(1);
    let start = Instant::now();
    let cpu_start = thread_cpu_time();
    let mut sent = 0;
    let mut next_send = start;
    while done_rx.try_recv().is_err() {
        let received = got.load(Ordering::Relaxed) / clients;
        let now = Instant::now();
        if sent < count && sent - received < window && pace.is_none_or(|_| now >= next_send) {
            server.write_or_enqueue_with(SendBehavior::Broadcast, |b| b.extend_from_slice(&msg));
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
    let sender_cpu = thread_cpu_time() - cpu_start;
    let (latencies_ns, receiver_cpu) = rx_thread.join().unwrap();
    assert_eq!(latencies_ns.len(), expected, "receiver timed out after {sent} sends");
    Stats { latencies_ns, elapsed, bytes: expected * size, cpu: sender_cpu + receiver_cpu }
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
    println!("== paced: one message per 100µs, one receiver ==");
    for (size_name, size) in sizes() {
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
    for (size_name, size) in sizes() {
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
    for (name, transport) in
        transports().into_iter().filter(|(_, t)| matches!(t, Transport::Udp(_)))
    {
        let server_addr = free_addr();
        // Drop the 900th of 1789 data datagrams.
        let relay = Relay::start(server_addr, 900);
        let s = run(Scenario {
            transport,
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
        s.row(&format!("loss1/{name}/2m"), &format!("datagrams={data} retransmitted={retx}"));
    }

    println!("\n== bcast: one sender, {BCAST_PEERS} receivers on one listener ==");
    for (size_name, size) in sizes() {
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
}
