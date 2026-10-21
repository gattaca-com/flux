//! Plumbing shared by the two-box benchmarks: env knobs, clocks, the paced
//! send loop and the receive-side report.

#![allow(dead_code)]

use std::{
    net::{IpAddr, SocketAddr, UdpSocket},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use flux_network::{Framing, TcpGroupConfig, UdpConfig, UdpGroupConfigV2};

pub const DATAGRAM: usize = 1472; // 1500 MTU - 20 (IP) - 8 (UDP)
pub const SOCKET_BUF: usize = 32 * 1024 * 1024;
pub const PORT_FALLBACK: u16 = 46000;
const BATCH: usize = 100;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Grouped,
    Packed,
}

pub fn mode() -> Mode {
    match std::env::var("MODE").as_deref() {
        Ok("packed") => Mode::Packed,
        _ => Mode::Grouped,
    }
}

pub fn tcp() -> bool {
    std::env::var("TRANSPORT").is_ok_and(|t| t.eq_ignore_ascii_case("tcp"))
}

pub fn kind() -> String {
    let m = match mode() {
        Mode::Grouped => "grouped",
        Mode::Packed => "packed",
    };
    format!("{}/{m}", if tcp() { "tcp" } else { "udp" })
}

pub fn tcp_config() -> TcpGroupConfig {
    TcpGroupConfig {
        nodelay: true,
        framing: Framing::LengthPrefixed,
        max_frame_size: 8 << 20,
        socket_buf_size: Some(SOCKET_BUF),
        ..Default::default()
    }
}

pub fn udp_config() -> UdpGroupConfigV2 {
    let udp = UdpConfig {
        max_datagram_size: DATAGRAM,
        max_message_size: 8 << 20,
        send_window: 1 << 15,
        recv_window: 1 << 15,
        ..UdpConfig::lan()
    };
    UdpGroupConfigV2 { udp, socket_buf_size: Some(SOCKET_BUF), ..Default::default() }
}

pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
}

fn realtime_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn cpu_ns() -> i64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, std::ptr::from_mut(&mut ts)) };
    ts.tv_sec * 1_000_000_000 + ts.tv_nsec
}

/// Unlike `core_affinity`, works when the inherited mask is a single other
/// core.
pub fn pin(core: usize) {
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

/// One send call's worth of messages, sized per `SIZE` and `BATCH_N`.
pub fn messages() -> Vec<Vec<u8>> {
    let (lo, hi) = size_range();
    let batch: usize = std::env::var("BATCH_N").ok().and_then(|s| s.parse().ok()).unwrap_or(BATCH);
    let span = hi - lo + 1;
    (0..batch).map(|j| vec![0x5a_u8; lo + (j * 101) % span]).collect()
}

/// `MODE=packed`: the batch as one message of `[len u16][bytes]` records.
pub fn pack(msgs: &[Vec<u8>], packed: &mut Vec<u8>) {
    packed.clear();
    for m in msgs {
        packed.extend_from_slice(&(m.len() as u16).to_le_bytes());
        packed.extend_from_slice(m);
    }
}

fn percentile(sorted: &[i64], p: f64) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}

pub fn spawn_echo(bind: IpAddr, probe_port: u16) {
    std::thread::spawn(move || {
        let sock = UdpSocket::bind(SocketAddr::new(bind, probe_port)).expect("echo bind");
        let mut buf = [0u8; 16];
        while let Ok((_, from)) = sock.recv_from(&mut buf) {
            let _ = sock.send_to(&realtime_ns().to_le_bytes(), from);
        }
    });
}

/// Receiver clock minus sender clock, from the min-RTT probe (NTP's
/// estimator): NTP sync alone is coarser than the latency measured.
pub fn measure_offset(sender_ip: IpAddr, probe_port: u16) -> i64 {
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

/// The sending end of a transport.
pub trait Sink {
    /// Sends the batch per `MODE`; returns how many peers it went to.
    fn send(&mut self, msgs: &[Vec<u8>]) -> usize;
    fn poll(&mut self);
}

/// Floods, or paces at `PACE_MBPS`, for `secs`, then prints the send report.
pub fn run_sender(sink: &mut impl Sink, msgs: &mut [Vec<u8>], peers: usize, secs: u64) {
    let batch = msgs.len();
    let mut send_ns: Vec<i64> = Vec::with_capacity(1 << 20);
    let mut send_cpu = 0i64;
    let mut sent = 0u64;
    let mut seq = 0u64;
    let mut short = 0u64;
    let batch_bytes = msgs.iter().map(Vec::len).sum::<usize>() as f64 * peers as f64;
    let rate =
        std::env::var("PACE_MBPS").ok().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0) * 1e6;
    let mut bucket = batch_bytes;
    let start = Instant::now();
    let run = Duration::from_secs(secs);
    let mut last = start;
    while start.elapsed() < run {
        let now = Instant::now();
        if rate > 0.0 {
            bucket = now
                .saturating_duration_since(last)
                .as_secs_f64()
                .mul_add(rate, bucket)
                .min(batch_bytes);
            last = now;
            if bucket < batch_bytes {
                sink.poll();
                continue;
            }
            bucket -= batch_bytes;
        }
        let ts = realtime_ns();
        for (k, m) in msgs.iter_mut().enumerate() {
            m[0..8].copy_from_slice(&(seq + k as u64).to_le_bytes());
            m[8..16].copy_from_slice(&ts.to_le_bytes());
        }
        let c0 = cpu_ns();
        let t = Instant::now();
        let recipients = sink.send(msgs);
        send_ns.push(t.elapsed().as_nanos() as i64);
        send_cpu += cpu_ns() - c0;
        if recipients > 0 {
            sent += batch as u64;
            seq += batch as u64;
        }
        short += u64::from(recipients < peers);
        sink.poll();
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

/// The receiving end of a transport.
pub trait Source {
    /// Polls once, passing each received message to `on_message`.
    fn poll(&mut self, on_message: &mut dyn FnMut(&[u8]));
}

/// Receives for `secs` from the first message, then prints the receive
/// report. `theta` is from [`measure_offset`].
pub fn run_receiver(source: &mut impl Source, core: usize, secs: u64, theta: i64) {
    let packed = mode() == Mode::Packed;
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
        source.poll(&mut |payload| {
            let now = realtime_ns();
            bytes += payload.len() as u64;
            started.get_or_insert_with(Instant::now);
            for (seq, ts) in records(payload, packed) {
                got += 1;
                highest = highest.max(seq);
                oneway.push(now as i64 - ts as i64 - theta);
            }
        });
    }
    let secs_run = started.map_or(0.0, |t| t.elapsed().as_secs_f64().min(secs as f64));
    let mbps = if secs_run > 0.0 { bytes as f64 / secs_run / (1024.0 * 1024.0) } else { 0.0 };
    let mmsg = if secs_run > 0.0 { got as f64 / secs_run / 1e6 } else { 0.0 };
    oneway.sort_unstable();
    println!(
        "RECV {} core={core} got={got} highest={highest} loss={} {mmsg:.3} Mmsg/s {mbps:.0} MiB/s \
         oneway_us p50={:.1} p99={:.1} p999={:.1}",
        kind(),
        highest + 1 - got.min(highest + 1),
        percentile(&oneway, 0.50) as f64 / 1000.0,
        percentile(&oneway, 0.99) as f64 / 1000.0,
        percentile(&oneway, 0.999) as f64 / 1000.0,
    );
}

/// `(seq, send_ts)` of each message in a payload.
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
