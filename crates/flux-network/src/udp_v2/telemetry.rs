//! UDP telemetry uses a fixed pool of producer-owned SPMC lanes per group.
//! Each writer holds one startup ownership lock; replacement writers repair
//! and reuse the same queues without detaching readers. Pool mappings and
//! files persist until application-level cleanup. Each group has eight slots
//! of four queues (about 64 MiB total); claiming a ninth live slot panics.

use std::{
    fmt::Write as _,
    fs::{File, OpenOptions},
    io,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

use flux::{type_hash::TypeHash, type_hash_derive::TypeHash};
use flux_communication::{
    ReadError,
    queue::{ConsumerBare, Producer, Queue},
};
use flux_timing::{Instant, Nanos};
use flux_utils::directories::{local_share_dir, shmem_dir_queues_with_base};
use mio::Token;
use rustc_hash::FxHashMap;
use tracing::warn;

use crate::NetworkTelemetry;

const QUEUE_SIZE: usize = 2usize.pow(15);
const LANE_SEPARATOR: &str = ".lane.";
const LANE_DRAIN_BUDGET: usize = 64;
const MAX_LANES: usize = 8;

type LanePools = FxHashMap<PathBuf, Box<[Lane]>>;
static LANE_POOLS: OnceLock<Mutex<LanePools>> = OnceLock::new();

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, TypeHash)]
pub struct PeerId {
    pub token: u64,
    /// IPv4 addresses are stored mapped.
    pub ip: [u8; 16],
    pub port: u16,
}

impl PeerId {
    pub(crate) fn new(token: Token, addr: SocketAddr) -> Self {
        let ip = match addr.ip() {
            IpAddr::V4(ip) => ip.to_ipv6_mapped(),
            IpAddr::V6(ip) => ip,
        };
        Self { token: token.0 as u64, ip: ip.octets(), port: addr.port() }
    }

    pub fn token(&self) -> Token {
        Token(self.token as usize)
    }

    pub fn addr(&self) -> SocketAddr {
        SocketAddr::new(Ipv6Addr::from(self.ip).to_canonical(), self.port)
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, TypeHash)]
pub struct UdpSend {
    pub peer: PeerId,
    pub sent_at: Instant,
    pub bytes: u32,
    pub datagrams: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, TypeHash)]
pub struct UdpRetransmit {
    pub peer: PeerId,
    pub sent_at: Instant,
    pub bytes: u32,
    pub retries: u8,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, TypeHash)]
pub struct UdpLatency {
    pub peer: PeerId,
    pub sent_at: Nanos,
    pub received_at: Nanos,
}

/// A round trip on the sender's clock: from sending a packet, or a
/// publisher's ping, to processing the ack that covers it.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, TypeHash)]
pub struct UdpRtt {
    pub peer: PeerId,
    pub sent_at: Instant,
    pub received_at: Instant,
    /// A ping's round trip, rather than a data packet's, which also counts
    /// the queueing and processing before the receiver acks.
    pub ping: bool,
}

fn sends_queue(group_name: &str) -> String {
    format!("udp_sends_{group_name}")
}

fn retransmits_queue(group_name: &str) -> String {
    format!("udp_retransmits_{group_name}")
}

fn latency_queue(group_name: &str) -> String {
    format!("udp_latency_{group_name}")
}

fn rtt_queue(group_name: &str) -> String {
    format!("udp_rtt_{group_name}")
}

pub(crate) struct Telemetry {
    producers: Option<TelemetryProducers>,
}

struct TelemetryProducers {
    sends: Producer<UdpSend>,
    retransmits: Producer<UdpRetransmit>,
    // TODO: a `Timer` here, once timers stop costing two shmem files per name
    // (see the TODO on `flux_communication::Timer`).
    latency: Producer<UdpLatency>,
    rtt: Producer<UdpRtt>,
    _owner: File,
}

struct Lane {
    sends: Queue<UdpSend>,
    retransmits: Queue<UdpRetransmit>,
    latency: Queue<UdpLatency>,
    rtt: Queue<UdpRtt>,
    owner_file: PathBuf,
}

impl Lane {
    fn new(dir: &Path, index: usize) -> Self {
        let lane = index.to_string();
        Self {
            sends: Queue::create_or_open_shared_spmc_lane(
                lane_file(dir, "udp_sends", &lane),
                QUEUE_SIZE,
            ),
            retransmits: Queue::create_or_open_shared_spmc_lane(
                lane_file(dir, "udp_retransmits", &lane),
                QUEUE_SIZE,
            ),
            latency: Queue::create_or_open_shared_spmc_lane(
                lane_file(dir, "udp_latency", &lane),
                QUEUE_SIZE,
            ),
            rtt: Queue::create_or_open_shared_spmc_lane(
                lane_file(dir, "udp_rtt", &lane),
                QUEUE_SIZE,
            ),
            owner_file: dir.join(format!("owner{LANE_SEPARATOR}{index}.lock")),
        }
    }

    fn try_claim(&self) -> io::Result<Option<TelemetryProducers>> {
        // A fresh open file description excludes other threads as well as processes.
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&self.owner_file)?;
        loop {
            if unsafe { libc::flock(owner.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                return Ok(Some(TelemetryProducers {
                    sends: Producer::from(self.sends),
                    retransmits: Producer::from(self.retransmits),
                    latency: Producer::from(self.latency),
                    rtt: Producer::from(self.rtt),
                    _owner: owner,
                }));
            }
            let err = io::Error::last_os_error();
            match err.kind() {
                io::ErrorKind::Interrupted => {}
                io::ErrorKind::WouldBlock => return Ok(None),
                _ => return Err(err),
            }
        }
    }
}

impl Telemetry {
    pub(crate) fn new(telemetry: NetworkTelemetry, group_name: &str) -> Self {
        let NetworkTelemetry::Enabled { app_name } = telemetry else {
            return Self { producers: None };
        };
        let dir = shmem_dir_queues_with_base(local_share_dir(), app_name);
        Self::enabled_in_dir(&dir, group_name)
    }

    fn enabled_in_dir(dir: &Path, group_name: &str) -> Self {
        let dir = pool_dir(dir, group_name);
        std::fs::create_dir_all(&dir).expect("couldn't create UDP telemetry directory");
        let dir = dir.canonicalize().expect("couldn't resolve UDP telemetry directory");
        let producers = {
            let mut pools = LANE_POOLS
                .get_or_init(|| Mutex::new(FxHashMap::default()))
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let pool = pools
                .entry(dir.clone())
                .or_insert_with(|| (0..MAX_LANES).map(|index| Lane::new(&dir, index)).collect());
            let claimed = pool
                .iter()
                .find_map(|lane| lane.try_claim().expect("couldn't claim UDP telemetry lane"));
            drop(pools);
            claimed
        };
        Self {
            producers: Some(producers.unwrap_or_else(|| {
                panic!("all {MAX_LANES} UDP telemetry lanes for {group_name} are owned")
            })),
        }
    }

    #[inline]
    pub(crate) fn send(&mut self, event: &UdpSend) {
        if let Some(producers) = &mut self.producers {
            producers.sends.produce(event);
        }
    }

    #[inline]
    pub(crate) fn retransmit(&mut self, event: &UdpRetransmit) {
        if let Some(producers) = &mut self.producers {
            producers.retransmits.produce(event);
        }
    }

    #[inline]
    pub(crate) fn latency(&mut self, peer: PeerId, sent_at: Nanos) {
        if let Some(producers) = &mut self.producers {
            producers.latency.produce(&UdpLatency { peer, sent_at, received_at: Nanos::now() });
        }
    }

    #[inline]
    pub(crate) fn rtt(&mut self, event: &UdpRtt) {
        if let Some(producers) = &mut self.producers {
            producers.rtt.produce(event);
        }
    }
}

fn pool_dir(dir: &Path, group: &str) -> PathBuf {
    let mut encoded = String::with_capacity(group.len() * 2);
    for byte in group.bytes() {
        write!(encoded, "{byte:02x}").unwrap();
    }
    dir.join("udp_telemetry").join(encoded)
}

fn lane_file(dir: &Path, name: &str, lane: &str) -> PathBuf {
    dir.join(format!("{name}{LANE_SEPARATOR}{lane}"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TelemetryEvent {
    Send(UdpSend),
    Retransmit(UdpRetransmit),
    Latency(UdpLatency),
    Rtt(UdpRtt),
    /// Legacy uncounted loss marker. New readers emit [`Self::Gap`].
    Lapped,
    /// The reader fell behind on one producer lane.
    Gap {
        lost: u64,
    },
}

pub struct TelemetryReader {
    sends: LaneConsumers<UdpSend>,
    retransmits: LaneConsumers<UdpRetransmit>,
    latency: LaneConsumers<UdpLatency>,
    rtt: LaneConsumers<UdpRtt>,
}

struct LaneConsumers<T> {
    consumers: Vec<ConsumerBare<T>>,
    next_lane: usize,
}

impl<T> LaneConsumers<T> {
    fn new() -> Self {
        Self { consumers: Vec::new(), next_lane: 0 }
    }
}

impl<T: Copy + TypeHash> LaneConsumers<T> {
    fn discover(&mut self, files: &[PathBuf], queue_name: &str) {
        let lane_prefix = format!("{queue_name}{LANE_SEPARATOR}");
        for file in files {
            if !file.exists() {
                continue;
            }
            let Some(name) = file.file_name().and_then(|name| name.to_str()) else { continue };
            let pool_lane =
                name.strip_prefix(&lane_prefix).is_some_and(|index| index.parse::<usize>().is_ok());
            if name != queue_name && !pool_lane {
                continue;
            }
            let queue = if pool_lane {
                Queue::try_open_shared_spmc_lane(file)
            } else {
                Queue::try_open_shared(file)
            };
            match queue {
                Ok(queue) => {
                    let mut consumer = ConsumerBare::new(queue, "udp_telemetry");
                    consumer.subscribe_broadcast();
                    self.consumers.push(consumer);
                }
                Err(err) => warn!(?err, file = %file.display(), "invalid UDP telemetry lane"),
            }
        }
    }
}

impl TelemetryReader {
    /// Opens the fixed producer-lane pool for `group_name`.
    ///
    /// Create at least one writer first to initialize the whole pool. Later
    /// writers and replacements use lanes already mapped by this reader.
    /// Ownership files and queues must remain linked while the pool is in use.
    pub fn open(app_name: &str, group_name: &str) -> Self {
        let dir = shmem_dir_queues_with_base(local_share_dir(), app_name);
        Self::open_in_dir(&dir, group_name)
    }

    fn open_in_dir(dir: &Path, group_name: &str) -> Self {
        let mut reader = Self {
            sends: LaneConsumers::new(),
            retransmits: LaneConsumers::new(),
            latency: LaneConsumers::new(),
            rtt: LaneConsumers::new(),
        };
        reader.discover_lanes(dir, group_name);
        reader
    }

    fn discover_lanes(&mut self, dir: &Path, group_name: &str) {
        let entries = std::fs::read_dir(pool_dir(dir, group_name));
        // Directory scanning and temporary path allocation happen once during
        // reader startup; draining retains and reuses only the consumers.
        let mut files: Vec<_> =
            entries.into_iter().flatten().flatten().map(|entry| entry.path()).collect();
        files.sort_unstable();
        self.sends.discover(&files, "udp_sends");
        self.retransmits.discover(&files, "udp_retransmits");
        self.latency.discover(&files, "udp_latency");
        self.rtt.discover(&files, "udp_rtt");
        let legacy = [
            dir.join(sends_queue(group_name)),
            dir.join(retransmits_queue(group_name)),
            dir.join(latency_queue(group_name)),
            dir.join(rtt_queue(group_name)),
        ];
        self.sends.discover(&legacy[..1], &sends_queue(group_name));
        self.retransmits.discover(&legacy[1..2], &retransmits_queue(group_name));
        self.latency.discover(&legacy[2..3], &latency_queue(group_name));
        self.rtt.discover(&legacy[3..], &rtt_queue(group_name));
    }

    pub fn drain(&mut self, mut on_event: impl FnMut(TelemetryEvent)) {
        drain(&mut self.sends, &mut on_event, TelemetryEvent::Send);
        drain(&mut self.retransmits, &mut on_event, TelemetryEvent::Retransmit);
        drain(&mut self.latency, &mut on_event, TelemetryEvent::Latency);
        drain(&mut self.rtt, &mut on_event, TelemetryEvent::Rtt);
    }
}

/// Drains producer-owned lanes in rotating, bounded batches. UDP writers are
/// normally created once during startup, so the lane count `L` is fixed and
/// the work for `E` events is `O(E + L * ceil(E / LANE_DRAIN_BUDGET))`, which
/// is effectively linear in `E`. Rotating the first lane prevents starvation.
fn drain<T: Copy + Default>(
    lanes: &mut LaneConsumers<T>,
    on_event: &mut impl FnMut(TelemetryEvent),
    wrap: fn(T) -> TelemetryEvent,
) {
    if lanes.consumers.is_empty() {
        return;
    }
    let mut event = T::default();
    loop {
        let mut progressed = false;
        let n_lanes = lanes.consumers.len();
        for offset in 0..n_lanes {
            let lane = (lanes.next_lane + offset) % n_lanes;
            let consumer = &mut lanes.consumers[lane];
            for _ in 0..LANE_DRAIN_BUDGET {
                match consumer.try_consume(&mut event) {
                    Ok(()) => {
                        progressed = true;
                        on_event(wrap(event));
                    }
                    Err(ReadError::SpedPast) => {
                        progressed = true;
                        let lost = consumer.recover_oldest_after_error();
                        if lost == 0 {
                            on_event(TelemetryEvent::Lapped);
                        } else {
                            on_event(TelemetryEvent::Gap { lost: lost as u64 });
                        }
                    }
                    Err(ReadError::Empty) => break,
                }
            }
        }
        lanes.next_lane = (lanes.next_lane + 1) % n_lanes;
        if !progressed {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telemetry_reuses_lanes_without_detaching_reader() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writers: Vec<_> =
            (0..MAX_LANES).map(|_| Telemetry::enabled_in_dir(tmp.path(), "group")).collect();
        let mut reader = TelemetryReader::open_in_dir(tmp.path(), "group");
        assert!(
            std::panic::catch_unwind(|| { Telemetry::enabled_in_dir(tmp.path(), "group") })
                .is_err()
        );

        for (index, writer) in writers.iter_mut().enumerate() {
            writer.send(&UdpSend { bytes: index as u32, ..UdpSend::default() });
        }
        let mut bytes = Vec::new();
        reader.drain(|event| {
            if let TelemetryEvent::Send(event) = event {
                bytes.push(event.bytes);
            }
        });
        bytes.sort_unstable();
        assert_eq!(bytes, (0..MAX_LANES as u32).collect::<Vec<_>>());

        for round in 0..16 {
            drop(writers.remove(0));
            let mut replacement = Telemetry::enabled_in_dir(tmp.path(), "group");
            replacement.send(&UdpSend { bytes: 100 + round, ..UdpSend::default() });
            bytes.clear();
            reader.drain(|event| {
                if let TelemetryEvent::Send(event) = event {
                    bytes.push(event.bytes);
                }
            });
            assert_eq!(bytes, [100 + round]);
            writers.insert(0, replacement);
        }
        drop(writers);
        flux_communication::cleanup_shmem(tmp.path());
    }

    #[test]
    fn telemetry_crash_writer_child() {
        let Some(dir) = std::env::var_os("FLUX_TELEMETRY_CRASH_DIR") else { return };
        let dir = Path::new(&dir);
        let mut writer = Telemetry::enabled_in_dir(dir, "group");
        writer.send(&UdpSend { bytes: 11, ..UdpSend::default() });
        std::fs::write(dir.join("ready"), []).unwrap();
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn telemetry_replaces_killed_writer_without_detaching_reader() {
        use std::{
            process::{Command, Stdio},
            time::{Duration, Instant},
        };

        let tmp = tempfile::tempdir().unwrap();
        let writer = Telemetry::enabled_in_dir(tmp.path(), "group");
        let mut reader = TelemetryReader::open_in_dir(tmp.path(), "group");
        let test_module = module_path!().split_once("::").unwrap().1;
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(format!("{test_module}::telemetry_crash_writer_child"))
            .env("FLUX_TELEMETRY_CRASH_DIR", tmp.path())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !tmp.path().join("ready").exists() {
                assert!(child.try_wait().unwrap().is_none(), "writer exited before publishing");
                assert!(Instant::now() < deadline, "writer did not publish before deadline");
                std::thread::yield_now();
            }
            let mut bytes = Vec::new();
            reader.drain(|event| {
                if let TelemetryEvent::Send(event) = event {
                    bytes.push(event.bytes);
                }
            });
            assert_eq!(bytes, [11]);
            child.kill().unwrap();
            child.wait().unwrap();
            let mut replacement = Telemetry::enabled_in_dir(tmp.path(), "group");
            replacement.send(&UdpSend { bytes: 22, ..UdpSend::default() });
            bytes.clear();
            reader.drain(|event| {
                if let TelemetryEvent::Send(event) = event {
                    bytes.push(event.bytes);
                }
            });
            assert_eq!(bytes, [22]);
        }));
        let _ = child.kill();
        let _ = child.wait();
        drop(writer);
        flux_communication::cleanup_shmem(tmp.path());
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    }
}
