#![allow(clippy::doc_markdown)]
//! Two-box send/broadcast benchmark over the real network, UDP or TCP.
//!
//! One sender (one box, one core) sends to `clients` receivers (another box,
//! one core each) and reports the send-call time (wall and CPU share). Each
//! receiver reports delivered rate, loss, and one-way latency (sender
//! send-stamp to receiver delivery). TRANSPORT=udp|tcp picks the stack; both
//! ends must agree on TRANSPORT and MODE.
//!
//! One-way latency crosses two machines, so the receiver first measures the
//! clock offset to the sender with a min-RTT ping-pong (NTP's estimator) on a
//! side UDP socket and subtracts it; the boxes are only NTP-synced, whose error
//! is larger than the latency being measured.
//!
//! Scenario knobs (env, same on both ends where they affect the wire):
//!   TRANSPORT=udp|tcp   MODE=loop|grouped|packed
//!   BATCH_N=<n>         messages per send-batch (default 100)
//!   SIZE=<lo>-<hi>      message size range, cycled (default 512-2048)
//!   PACE_MBPS=<n>       cap offered payload at n MB/s aggregate; unset floods
//!
//! Start the receivers first (each on its own core), then the sender:
//!   remote_bcast receiver <sender_ip> <port> <bind_ip> <core> <secs>
//!   remote_bcast sender   <bind_ip> <port> <clients> <core> <secs>
//! Pace below the slowest NIC's line rate to stay lossless. The sender prints
//! SEND (call time, throughput), each receiver prints RECV (loss, one-way).

use std::{
    net::{IpAddr, SocketAddr, UdpSocket},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use flux_network::{
    Framing, Group, GroupConfig, Network, NetworkEvent, TcpGroupConfig, Token, UdpConfig,
    UdpGroupConfig,
};

const BATCH: usize = 100;
const DATAGRAM: usize = 1472; // 1500 MTU - 20 (IP) - 8 (UDP)
const SOCKET_BUF: usize = 32 * 1024 * 1024;
const PORT_FALLBACK: u16 = 46000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Loop,
    Grouped,
    Packed,
}

fn mode() -> Mode {
    match std::env::var("MODE").as_deref() {
        Ok("loop") => Mode::Loop,
        Ok("packed") => Mode::Packed,
        _ => Mode::Grouped,
    }
}

fn tcp() -> bool {
    std::env::var("TRANSPORT").is_ok_and(|t| t.eq_ignore_ascii_case("tcp"))
}

fn kind() -> String {
    let m = match mode() {
        Mode::Loop => "loop",
        Mode::Grouped => "grouped",
        Mode::Packed => "packed",
    };
    format!("{}/{m}", if tcp() { "tcp" } else { "udp" })
}

/// Wall-clock nanoseconds, NTP-disciplined so comparable across boxes once the
/// residual offset is removed.
fn realtime_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// This thread's consumed CPU time in nanoseconds, to tell a CPU-bound send
/// call apart from one waiting in the syscall.
fn cpu_ns() -> i64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, std::ptr::from_mut(&mut ts)) };
    ts.tv_sec * 1_000_000_000 + ts.tv_nsec
}

/// Pins the current thread to one absolute CPU via `sched_setaffinity`, which
/// works even when the inherited affinity mask is a single other core.
fn pin(core: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        let r = libc::sched_setaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            std::ptr::from_ref(&set),
        );
        assert!(r == 0, "failed to pin to core {core}: {}", std::io::Error::last_os_error());
    }
}

fn size_range() -> (usize, usize) {
    std::env::var("SIZE")
        .ok()
        .and_then(|s| {
            let (lo, hi) = s.split_once('-')?;
            Some((lo.parse().ok()?, hi.parse().ok()?))
        })
        .unwrap_or((512, 2048))
}

fn config() -> GroupConfig {
    let group = if tcp() {
        GroupConfig::from(TcpGroupConfig {
            nodelay: true,
            framing: Framing::LengthPrefixed,
            max_frame_size: 8 << 20,
            ..Default::default()
        })
    } else {
        let udp = UdpConfig {
            max_datagram_size: DATAGRAM,
            max_message_size: 8 << 20,
            send_window: 1 << 15,
            recv_window: 1 << 15,
            ..UdpConfig::lan()
        };
        GroupConfig::from(UdpGroupConfig { udp, ..Default::default() })
    };
    group.with_socket_buf_size(SOCKET_BUF)
}

fn percentile(sorted: &[i64], p: f64) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("sender") => sender(&args),
        Some("receiver") => receiver(&args),
        _ => {
            eprintln!("usage: remote_bcast sender|receiver ...");
            std::process::exit(2);
        }
    }
}

/// Answers clock-offset probes forever, stamping each reply with the sender's
/// wall clock. The process exiting ends the thread.
fn spawn_echo(bind: IpAddr, probe_port: u16) {
    std::thread::spawn(move || {
        let sock = UdpSocket::bind(SocketAddr::new(bind, probe_port)).expect("echo bind");
        let mut buf = [0u8; 16];
        while let Ok((_, from)) = sock.recv_from(&mut buf) {
            let _ = sock.send_to(&realtime_ns().to_le_bytes(), from);
        }
    });
}

/// Sends one batch per the mode; returns recipients reached by the last call.
fn send_batch(
    net: &mut Network,
    group: Group,
    tokens: &[Token],
    msgs: &[Vec<u8>],
    packed: &mut Vec<u8>,
) -> usize {
    let unicast = tokens.len() == 1;
    match mode() {
        Mode::Loop => {
            let mut r = 0;
            for m in msgs {
                r = if unicast {
                    usize::from(net.send_with(tokens[0], |b| b.extend_from_slice(m)))
                } else {
                    net.broadcast_with(group, |b| b.extend_from_slice(m))
                };
            }
            r
        }
        Mode::Grouped if unicast => {
            usize::from(net.send_many_with(tokens[0], msgs, |b, m| b.extend_from_slice(m)))
        }
        Mode::Grouped => net.broadcast_many_with(group, msgs, |b, m| b.extend_from_slice(m)),
        Mode::Packed => {
            packed.clear();
            for m in msgs {
                packed.extend_from_slice(&(m.len() as u16).to_le_bytes());
                packed.extend_from_slice(m);
            }
            if unicast {
                usize::from(net.send_with(tokens[0], |b| b.extend_from_slice(packed)))
            } else {
                net.broadcast_with(group, |b| b.extend_from_slice(packed))
            }
        }
    }
}

fn sender(args: &[String]) {
    let bind: IpAddr = args[2].parse().expect("bind_ip");
    let port: u16 = args[3].parse().unwrap_or(PORT_FALLBACK);
    let clients: usize = args[4].parse().expect("clients");
    let core: usize = args[5].parse().expect("core");
    let secs: u64 = args.get(6).and_then(|s| s.parse().ok()).unwrap_or(10);
    pin(core);
    spawn_echo(bind, port + 1);

    let mut net = Network::default();
    let group = net.add_group(config());
    net.listen(group, SocketAddr::new(bind, port)).expect("listen");
    eprintln!("sender ({}): on {bind}:{port}, waiting for {clients} receivers", kind());
    let mut tokens: Vec<Token> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(40);
    while tokens.len() < clients {
        assert!(Instant::now() < deadline, "only {}/{clients} connected", tokens.len());
        net.poll_with(|e| {
            if let NetworkEvent::Accepted { token, .. } = e {
                tokens.push(token);
            }
        });
    }
    eprintln!("sender: all {clients} connected, running {secs}s");

    let (lo, hi) = size_range();
    let batch: usize = std::env::var("BATCH_N").ok().and_then(|s| s.parse().ok()).unwrap_or(BATCH);
    let span = hi - lo + 1;
    let mut msgs: Vec<Vec<u8>> = (0..batch).map(|j| vec![0x5a_u8; lo + (j * 101) % span]).collect();
    run_sender(&mut net, group, &tokens, &mut msgs, clients, secs);
}

fn run_sender(
    net: &mut Network,
    group: Group,
    tokens: &[Token],
    msgs: &mut [Vec<u8>],
    clients: usize,
    secs: u64,
) {
    let batch = msgs.len();
    let mut send_ns: Vec<i64> = Vec::with_capacity(1 << 20);
    let mut send_cpu = 0i64;
    let mut packed = Vec::new();
    let mut sent = 0u64;
    let mut seq = 0u64;
    let mut short = 0u64;
    // PACE_MBPS caps offered payload with a token bucket holding one batch; unset
    // floods.
    let bcast_bytes = msgs.iter().map(Vec::len).sum::<usize>() as f64 * clients as f64;
    let rate =
        std::env::var("PACE_MBPS").ok().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0) * 1e6;
    let mut tokens_bucket = bcast_bytes;
    let start = Instant::now();
    let run = Duration::from_secs(secs);
    let mut last = start;
    while start.elapsed() < run {
        let now = Instant::now();
        if rate > 0.0 {
            tokens_bucket = now
                .saturating_duration_since(last)
                .as_secs_f64()
                .mul_add(rate, tokens_bucket)
                .min(bcast_bytes);
            last = now;
            if tokens_bucket < bcast_bytes {
                net.poll_with(|_| {});
                continue;
            }
            tokens_bucket -= bcast_bytes;
        }
        let ts = realtime_ns();
        for (k, m) in msgs.iter_mut().enumerate() {
            m[0..8].copy_from_slice(&(seq + k as u64).to_le_bytes());
            m[8..16].copy_from_slice(&ts.to_le_bytes());
        }
        let c0 = cpu_ns();
        let t = Instant::now();
        let recipients = send_batch(net, group, tokens, msgs, &mut packed);
        send_ns.push(t.elapsed().as_nanos() as i64);
        send_cpu += cpu_ns() - c0;
        if recipients > 0 {
            sent += batch as u64;
            seq += batch as u64;
        }
        short += u64::from(recipients < clients);
        net.poll_with(|_| {});
    }
    report_send(&send_ns, send_cpu, sent, short, start.elapsed());
}

fn report_send(send_ns: &[i64], cpu: i64, sent: u64, short: u64, elapsed: Duration) {
    let mut s = send_ns.to_vec();
    s.sort_unstable();
    let wall: i64 = s.iter().sum();
    let calls = s.len().max(1) as i64;
    println!(
        "SEND {} calls={} p50={}ns p99={}ns max={}ns cpu={}ns/call({}%) short={short} \
         sent={sent} {:.3} Mmsg/s",
        kind(),
        s.len(),
        percentile(&s, 0.50),
        percentile(&s, 0.99),
        percentile(&s, 1.0),
        cpu / calls,
        100 * cpu / wall.max(1),
        sent as f64 / elapsed.as_secs_f64() / 1e6,
    );
}

/// Min-RTT clock-offset estimate to the sender's echo port, in nanoseconds
/// (receiver clock minus sender clock). Retries until the sender is up.
fn measure_offset(sender_ip: IpAddr, probe_port: u16) -> i64 {
    let sock = UdpSocket::bind("0.0.0.0:0").expect("probe bind");
    sock.connect(SocketAddr::new(sender_ip, probe_port)).expect("probe connect");
    sock.set_read_timeout(Some(Duration::from_millis(100))).expect("probe timeout");
    let deadline = Instant::now() + Duration::from_secs(40);
    let mut best_rtt = u64::MAX;
    let mut theta = 0i64;
    let mut samples = 0u32;
    while samples < 200 && Instant::now() < deadline {
        let t1 = realtime_ns();
        if sock.send(&[0u8; 8]).is_err() {
            continue;
        }
        let mut buf = [0u8; 8];
        if sock.recv(&mut buf).is_ok() {
            let t3 = realtime_ns();
            let t2 = u64::from_le_bytes(buf);
            let rtt = t3.saturating_sub(t1);
            if rtt < best_rtt {
                best_rtt = rtt;
                theta = (i128::midpoint(i128::from(t1), i128::from(t3)) - i128::from(t2)) as i64;
            }
            samples += 1;
        }
    }
    eprintln!("receiver: offset theta={theta}ns (min rtt {best_rtt}ns over {samples} samples)");
    theta
}

fn receiver(args: &[String]) {
    let sender_ip: IpAddr = args[2].parse().expect("sender_ip");
    let port: u16 = args[3].parse().unwrap_or(PORT_FALLBACK);
    let core: usize = args[5].parse().expect("core");
    let secs: u64 = args.get(6).and_then(|s| s.parse().ok()).unwrap_or(12);
    pin(core);
    let theta = measure_offset(sender_ip, port + 1);
    let packed = mode() == Mode::Packed;

    let mut net = Network::default();
    let group: Group = net.add_group(config());
    let _ = net.connect(group, SocketAddr::new(sender_ip, port));
    eprintln!("receiver core {core} ({}): connecting to {sender_ip}:{port}", kind());

    let mut got = 0u64;
    let mut bytes = 0u64;
    let mut highest = 0u64;
    let mut oneway: Vec<i64> = Vec::with_capacity(1 << 21);
    let mut started = None;
    let hard_deadline = Instant::now() + Duration::from_secs(secs + 50);
    loop {
        if Instant::now() > hard_deadline ||
            started.is_some_and(|t0| Instant::now() >= t0 + Duration::from_secs(secs))
        {
            break;
        }
        net.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                let now = realtime_ns();
                bytes += payload.len() as u64;
                started.get_or_insert_with(Instant::now);
                for (seq, ts) in records(payload, packed) {
                    got += 1;
                    highest = highest.max(seq);
                    oneway.push(now as i64 - ts as i64 - theta);
                }
            }
        });
    }
    report_recv(core, got, bytes, highest, &oneway, started, secs);
}

/// (seq, send_ts) of each logical message in a delivered payload. A packed
/// payload holds `[len u16][bytes]` records; otherwise it is one message.
fn records(payload: &[u8], packed: bool) -> Vec<(u64, u64)> {
    if !packed {
        return vec![(le8(payload, 0), le8(payload, 8))];
    }
    let mut out = Vec::new();
    let mut at = 0;
    while at + 2 <= payload.len() {
        let n = usize::from(u16::from_le_bytes([payload[at], payload[at + 1]]));
        at += 2;
        if at + n > payload.len() {
            break;
        }
        out.push((le8(payload, at), le8(payload, at + 8)));
        at += n;
    }
    out
}

fn le8(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

fn report_recv(
    core: usize,
    got: u64,
    bytes: u64,
    highest: u64,
    oneway: &[i64],
    started: Option<Instant>,
    secs: u64,
) {
    let secs_run = started.map_or(0.0, |t| t.elapsed().as_secs_f64().min(secs as f64));
    let mbps = if secs_run > 0.0 { bytes as f64 / secs_run / (1024.0 * 1024.0) } else { 0.0 };
    let mmsg = if secs_run > 0.0 { got as f64 / secs_run / 1e6 } else { 0.0 };
    let mut ow = oneway.to_vec();
    ow.sort_unstable();
    println!(
        "RECV {} core={core} got={got} highest={highest} loss={} {mmsg:.3} Mmsg/s {mbps:.0} MiB/s \
         oneway_us p50={:.1} p99={:.1} p999={:.1}",
        kind(),
        highest + 1 - got.min(highest + 1),
        percentile(&ow, 0.50) as f64 / 1000.0,
        percentile(&ow, 0.99) as f64 / 1000.0,
        percentile(&ow, 0.999) as f64 / 1000.0,
    );
}
