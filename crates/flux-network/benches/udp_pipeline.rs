//! Sender and receiver on separate pinned threads, TCP vs reliable UDP.
//!
//! Traffic crosses a veth pair instead of loopback. Loopback hands a GSO
//! buffer to the receiver whole, which hides every per-segment cost; the pair
//! segments in software on transmit and runs real GRO on receive, like a NIC
//! without UDP segmentation offload. The bench creates the pair itself and
//! removes it when it exits, including on panic or Ctrl-C. It needs `ip`,
//! `ethtool` and `sudo`. Outside the pair it touches two things for the run
//! and undoes both afterwards: the `local` routing rule moves from priority 0
//! to 1, and when `iptables` exists an accept rule for each end goes to the
//! top of the INPUT chain, since a host firewall drops traffic arriving on
//! them. A killed run leaves that state behind, which the next run cleans
//! up, or by hand:
//!
//! ```text
//! sudo ip rule add pref 0 lookup local
//! sudo ip rule del pref 0 iif lo to 10.77.0.1 lookup 50
//! sudo ip rule del pref 0 iif lo to 10.77.0.2 lookup 51
//! sudo ip rule del pref 1 lookup local
//! sudo iptables -D INPUT -i fluxbench0 -j ACCEPT
//! sudo iptables -D INPUT -i fluxbench1 -j ACCEPT
//! sudo ip link del fluxbench0
//! ```
//!
//! Reports one-way latency (receiver clock minus the sender timestamp carried
//! in every message, same host so no clock skew) as p50/p99, throughput, and
//! for the loss scenario how many datagrams were retransmitted.
//!
//! Scenarios:
//! - `paced`: one send per 100 µs, or slower so that no size exceeds 1 GiB/s.
//!   Latency of an unloaded transport; the run asserts the pace was kept.
//! - `burst`: as fast as the send window allows. Throughput and queueing.
//! - `loss1`: UDP through a relay that drops exactly one data datagram of a 2
//!   MiB message. Recovery should resend one datagram, not the message.
//! - `bcast`: one sender broadcasting to 8 receivers on one listener socket.
//! - `many`: batches of small or few-KiB messages per `send_many_with` or
//!   `broadcast_many_with` call, against the same batch as a loop of single
//!   sends, reporting the time spent in the call as well.
//!
//! Run with `cargo bench -p flux-network --bench udp_pipeline`.

use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    process::Command,
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
const PACE_FLOOR: Duration = Duration::from_micros(100);
const PACE_RATE: usize = 1024 * 1024 * 1024;

/// Interval between paced sends of `bytes`: [`PACE_FLOOR`], or longer so the
/// rate stays at [`PACE_RATE`].
fn pace_for(bytes: usize) -> Duration {
    PACE_FLOOR.max(Duration::from_secs(1) * bytes as u32 / PACE_RATE as u32)
}
const BCAST_PEERS: usize = 8;
const BIG_SOCKET_BUF: usize = 16 * 1024 * 1024;
/// Message sizes of the `many` scenario; a batch cycles through its range.
const MANY_SIZES: [(&str, std::ops::RangeInclusive<usize>); 2] =
    [("small", 64..=256), ("few-kb", 2048..=4096)];
/// Shared-memory telemetry queues of the `udp+tel` transport live here.
const APP_NAME: &str = "udp-pipeline-bench";

/// The veth pair: clients and the relay live on end 0, the server on end 1.
/// Packets to an end's address leave through the other end, via a routing
/// table consulted before the `local` one for locally generated traffic.
const VETH: [&str; 2] = ["fluxbench0", "fluxbench1"];
const VETH_MAC: [&str; 2] = ["02:fb:00:00:00:01", "02:fb:00:00:00:02"];
const VETH_IP: [Ipv4Addr; 2] = [Ipv4Addr::new(10, 77, 0, 1), Ipv4Addr::new(10, 77, 0, 2)];
const VETH_TABLE: [u32; 2] = [50, 51];
const CLIENT_IP: Ipv4Addr = VETH_IP[0];
const SERVER_IP: Ipv4Addr = VETH_IP[1];

static INTERRUPTED: AtomicBool = AtomicBool::new(false);

extern "C" fn on_interrupt(_: libc::c_int) {
    INTERRUPTED.store(true, Ordering::Relaxed);
}

/// Runs a whitespace-separated command through `sudo`, inheriting the
/// terminal for the password prompt. Reports failure instead of panicking so
/// teardown can go on with its remaining steps.
fn sudo(command: &str) -> bool {
    match Command::new("sudo").args(command.split_whitespace()).status() {
        Ok(status) if status.success() => true,
        Ok(status) => {
            eprintln!("sudo {command} failed: {status}");
            false
        }
        Err(err) => {
            eprintln!("sudo {command} failed: {err}");
            false
        }
    }
}

fn has_iptables() -> bool {
    Command::new("iptables").arg("--version").output().is_ok()
}

/// Whether the INPUT chain accepts everything arriving on `dev`.
fn has_firewall_rule(dev: &str) -> bool {
    let out = Command::new("sudo").args(["iptables", "-S", "INPUT"]).output().expect("iptables");
    String::from_utf8_lossy(&out.stdout).contains(&format!("-A INPUT -i {dev} -j ACCEPT"))
}

/// Whether an `ip rule` at `pref` contains `needle`.
fn has_rule(pref: u32, needle: &str) -> bool {
    let out = Command::new("ip").args(["rule", "show"]).output().expect("ip rule show");
    let prefix = format!("{pref}:");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .any(|line| line.starts_with(&prefix) && line.contains(needle))
}

fn link_exists() -> bool {
    Command::new("ip")
        .args(["-o", "link", "show", VETH[0]])
        .output()
        .is_ok_and(|out| out.status.success())
}

/// The veth pair and routing for the run; dropping it removes them again.
struct Link;

impl Link {
    fn up() -> Self {
        assert!(
            Command::new("ethtool").arg("--version").output().is_ok(),
            "ethtool is required to configure the veth pair"
        );
        unsafe { libc::signal(libc::SIGINT, on_interrupt as libc::sighandler_t) };
        println!("setting up veth pair {}/{} (sudo)", VETH[0], VETH[1]);
        let link = Self;
        Self::down();
        let (v0, v1) = (VETH[0], VETH[1]);
        let mut steps = vec![format!(
            "ip link add {v0} address {} type veth peer name {v1} address {}",
            VETH_MAC[0], VETH_MAC[1]
        )];
        for i in 0..2 {
            let (dev, ip, peer_ip) = (VETH[i], VETH_IP[i], VETH_IP[1 - i]);
            steps.push(format!("ip addr add {ip}/24 dev {dev}"));
            steps.push(format!("ip link set {dev} up"));
            steps.push(format!(
                "ip neigh replace {peer_ip} lladdr {} dev {dev} nud permanent",
                VETH_MAC[1 - i]
            ));
            steps.push(format!("sysctl -q -w net.ipv4.conf.{dev}.accept_local=1"));
            steps.push(format!("ethtool -K {dev} tx-udp-segmentation off tso off gro on"));
            if has_iptables() {
                steps.push(format!("iptables -I INPUT 1 -i {dev} -j ACCEPT"));
            }
        }
        for i in 0..2 {
            let (ip, via, src, table) = (VETH_IP[i], VETH[1 - i], VETH_IP[1 - i], VETH_TABLE[i]);
            steps.push(format!("ip route replace {ip}/32 dev {via} src {src} table {table}"));
        }
        if !has_rule(1, "lookup local") {
            steps.push("ip rule add pref 1 lookup local".into());
        }
        if has_rule(0, "lookup local") {
            steps.push("ip rule del pref 0 lookup local".into());
        }
        for i in 0..2 {
            let (ip, table) = (VETH_IP[i], VETH_TABLE[i]);
            if !has_rule(0, &format!("to {ip} ")) {
                steps.push(format!("ip rule add pref 0 iif lo to {ip}/32 lookup {table}"));
            }
        }
        for step in &steps {
            assert!(sudo(step), "veth setup failed");
        }
        link
    }

    /// Undoes `up`, skipping whatever is already undone.
    fn down() {
        if !has_rule(0, "lookup local") {
            sudo("ip rule add pref 0 lookup local");
        }
        for i in 0..2 {
            let (ip, table) = (VETH_IP[i], VETH_TABLE[i]);
            if has_rule(0, &format!("to {ip} ")) {
                sudo(&format!("ip rule del pref 0 iif lo to {ip}/32 lookup {table}"));
            }
        }
        if has_rule(1, "lookup local") {
            sudo("ip rule del pref 1 lookup local");
        }
        if has_iptables() {
            for dev in VETH {
                if has_firewall_rule(dev) {
                    sudo(&format!("iptables -D INPUT -i {dev} -j ACCEPT"));
                }
            }
        }
        if link_exists() {
            sudo(&format!("ip link del {}", VETH[0]));
        }
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        println!("removing veth pair (sudo)");
        Self::down();
    }
}

/// A free port on the server address.
fn free_addr() -> SocketAddr {
    loop {
        let listener = std::net::TcpListener::bind((SERVER_IP, 0)).unwrap();
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
    /// Time spent sending each batch; only recorded by the `many` scenario.
    send_ns: Vec<u64>,
    elapsed: Duration,
    bytes: usize,
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * p) as usize] as f64 / 1000.0
}

impl Stats {
    fn row(&self, name: &str, extra: &str) {
        let mut l = self.latencies_ns.clone();
        l.sort_unstable();
        let mibps = self.bytes as f64 / self.elapsed.as_secs_f64() / (1024.0 * 1024.0);
        let send = if self.send_ns.is_empty() {
            String::new()
        } else {
            let mut s = self.send_ns.clone();
            s.sort_unstable();
            let per_sec = self.latencies_ns.len() as f64 / self.elapsed.as_secs_f64() / 1e6;
            format!(
                " send p50={:>6.1}µs p99={:>6.1}µs {per_sec:>5.2} Mmsg/s",
                percentile(&s, 0.5),
                percentile(&s, 0.99),
            )
        };
        println!(
            "{name:<28} n={:<6} p50={:>9.1}µs p99={:>9.1}µs max={:>9.1}µs {mibps:>9.0} MiB/s{send} {extra}",
            l.len(),
            percentile(&l, 0.5),
            percentile(&l, 0.99),
            percentile(&l, 1.0),
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
    /// Message sizes, cycled through within a batch.
    sizes: std::ops::RangeInclusive<usize>,
    count: usize,
    /// Minimum gap between sends; `None` sends as fast as `window` allows.
    pace: Option<Duration>,
    /// Bound on messages sent but not yet received by every client.
    window: usize,
    /// Messages per send step.
    batch: usize,
    /// Send a batch with one `send_many_with` or `broadcast_many_with` call
    /// rather than one `send_with` or `broadcast_with` call per message.
    grouped: bool,
}

/// Sends `count` messages from the server while the receiver thread counts
/// them and records one-way latency.
fn run(sc: Scenario) -> Stats {
    let Scenario { transport, listen, dial, clients, sizes, count, pace, window, batch, grouped } =
        sc;
    assert!(count.is_multiple_of(batch));
    let (mut server, server_group, receivers, accepted) =
        connect(&transport, listen, dial, clients);
    let span = sizes.end() - sizes.start() + 1;
    let msgs: Vec<Vec<u8>> = (0..batch).map(|j| vec![0x5Au8; sizes.start() + j % span]).collect();
    let bytes = count / batch * clients * msgs.iter().map(Vec::len).sum::<usize>();
    let mut send_ns = Vec::with_capacity(if batch == 1 { 0 } else { count / batch });
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
        if INTERRUPTED.load(Ordering::Relaxed) {
            stop.store(true, Ordering::Relaxed);
            let _ = rx_thread.join();
            // Unwinds through `main`, so the `Link` guard still tears down.
            panic!("interrupted");
        }
        let received = got.load(Ordering::Relaxed) / clients;
        let now = Instant::now();
        if sent < count && sent - received < window && pace.is_none_or(|_| now >= next_send) {
            let t = Instant::now();
            match (grouped, clients) {
                (false, _) => {
                    for m in &msgs {
                        server.broadcast_with(server_group, |b| b.extend_from_slice(m));
                    }
                }
                (true, 1) => {
                    server.send_many_with(accepted[0], &msgs, |b, m| b.extend_from_slice(m));
                }
                (true, _) => {
                    server.broadcast_many_with(server_group, &msgs, |b, m| {
                        b.extend_from_slice(m);
                    });
                }
            }
            if batch > 1 {
                send_ns.push(t.elapsed().as_nanos() as u64);
            }
            sent += batch;
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
    if let Some(p) = pace {
        let ideal = p * (count / batch) as u32;
        assert!(elapsed < ideal + ideal / 10 + Duration::from_millis(10), "pace of {p:?} not kept");
    }
    Stats { latencies_ns, send_ns, elapsed, bytes }
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
        let socket = UdpSocket::bind((CLIENT_IP, 0)).unwrap();
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
                // Wire layout: byte 2 holds the kind in its low nibble, 1 for
                // a packet of records, with the session at [3..7] and the
                // packet sequence at [7..15].
                let records = usize::from(n >= 25 && buf[2] & 0x0f == 1);
                if records != 0 {
                    let session = u32::from_le_bytes(buf[3..7].try_into().unwrap());
                    let seq = u64::from_le_bytes(buf[7..15].try_into().unwrap());
                    if !seen.insert((session, seq)) {
                        retx_c.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if records != 0 {
                    n_data += 1;
                    data_c.fetch_add(1, Ordering::Relaxed);
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
    let _link = Link::up();
    println!("== paced: one message per 100µs or 1 GiB/s, one receiver ==");
    for (size_name, size) in SIZES {
        for (name, transport) in transports() {
            let addr = free_addr();
            let s = run(Scenario {
                transport,
                listen: addr,
                dial: addr,
                clients: 1,
                sizes: size..=size,
                count: PACED_MSGS,
                pace: Some(pace_for(size)),
                window: burst_plan(size).1,
                batch: 1,
                grouped: false,
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
                sizes: size..=size,
                count,
                pace: None,
                window,
                batch: 1,
                grouped: false,
            });
            s.row(&format!("burst/{name}/{size_name}"), &format!("window={window}"));
        }
    }

    println!("\n== loss1: one 2 MiB message, exactly one datagram dropped by a relay ==");
    {
        let server_addr = free_addr();
        // Drop the 900th of about 1810 packets.
        let relay = Relay::start(server_addr, 900);
        let s = run(Scenario {
            transport: UdpGroupConfig { udp: udp_config(), ..Default::default() }.into(),
            listen: server_addr,
            dial: relay.addr,
            clients: 1,
            sizes: 2 * 1024 * 1024..=2 * 1024 * 1024,
            count: 1,
            pace: None,
            window: 1,
            batch: 1,
            grouped: false,
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
                sizes: size..=size,
                count,
                pace: None,
                window,
                batch: 1,
                grouped: false,
            });
            s.row(&format!("bcast/{name}/{size_name}"), &format!("window={window}"));
        }
    }
    cleanup_shmem(&shmem_dir(APP_NAME));

    many();
}

/// The `many` scenario: each size range and batch as one grouped call and as
/// a loop of single sends, burst to one receiver, grouped burst to
/// [`BCAST_PEERS`], and paced grouped to one receiver.
fn many() {
    println!("\n== many: messages per call, grouped (send_many_with) vs loop of single sends ==");
    let udp: GroupConfig = UdpGroupConfig { udp: udp_config(), ..Default::default() }.into();
    let tcp = transports().into_iter().next().unwrap().1;
    let row = |name: String,
               transport: &GroupConfig,
               sizes: std::ops::RangeInclusive<usize>,
               clients: usize,
               batch: usize,
               grouped: bool,
               pace: Option<Duration>| {
        let addr = free_addr();
        let calls = if pace.is_some() { 1000 } else { 400 };
        let s = run(Scenario {
            transport: transport.clone(),
            listen: addr,
            dial: addr,
            clients,
            sizes,
            count: calls * batch,
            pace,
            window: 10 * batch,
            batch,
            grouped,
        });
        s.row(&name, "");
    };
    for (size_name, sizes) in MANY_SIZES {
        for batch in [5, 50] {
            let name = |what: &str| format!("many/{size_name}x{batch}/{what}");
            row(name("udp-grouped"), &udp, sizes.clone(), 1, batch, true, None);
            row(name("udp-loop"), &udp, sizes.clone(), 1, batch, false, None);
            row(name("tcp-grouped"), &tcp, sizes.clone(), 1, batch, true, None);
            let pace = pace_for(batch * (sizes.start() + sizes.end()) / 2);
            row(name("udp-grouped-paced"), &udp, sizes.clone(), 1, batch, true, Some(pace));
            row(name("udp-grouped-8rx"), &udp, sizes.clone(), BCAST_PEERS, batch, true, None);
            row(name("udp-loop-8rx"), &udp, sizes.clone(), BCAST_PEERS, batch, false, None);
        }
    }
}
