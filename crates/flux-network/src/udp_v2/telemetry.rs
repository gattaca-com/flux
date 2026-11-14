//! UDP telemetry gives each writer a producer-owned SPMC lane for each event
//! stream, avoiding contention on a shared MPMC reservation cursor. Writers
//! must create their lanes before [`TelemetryReader::open`] discovers the fixed
//! lane set; readers then merge those lanes in rotating, bounded batches. The
//! rings are bounded and report exact gaps when a writer laps a reader.

use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use flux::{type_hash::TypeHash, type_hash_derive::TypeHash};
use flux_communication::{
    ReadError, cleanup_flink,
    queue::{ConsumerBare, Producer, Queue, QueueType},
};
use flux_timing::{Instant, Nanos};
use flux_utils::directories::{local_share_dir, shmem_dir_queues_with_base};
use mio::Token;
use tracing::warn;

use crate::NetworkTelemetry;

const QUEUE_SIZE: usize = 2usize.pow(15);
const LANE_SEPARATOR: &str = ".lane.";
const LANE_DRAIN_BUDGET: usize = 64;
static NEXT_LANE_ID: AtomicU64 = AtomicU64::new(0);

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
    _lane_files: LaneFiles,
}

struct LaneFiles([PathBuf; 4]);

impl Drop for LaneFiles {
    fn drop(&mut self) {
        for file in &self.0 {
            if let Err(err) = cleanup_flink(file) {
                warn!(%err, file = %file.display(), "failed to clean UDP telemetry lane");
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
        let _ = std::fs::create_dir_all(dir);
        let lane = lane_id();
        let lane_files = LaneFiles([
            lane_file(dir, &sends_queue(group_name), &lane),
            lane_file(dir, &retransmits_queue(group_name), &lane),
            lane_file(dir, &latency_queue(group_name), &lane),
            lane_file(dir, &rtt_queue(group_name), &lane),
        ]);
        let producers = TelemetryProducers {
            sends: producer(&lane_files.0[0]),
            retransmits: producer(&lane_files.0[1]),
            latency: producer(&lane_files.0[2]),
            rtt: producer(&lane_files.0[3]),
            _lane_files: lane_files,
        };
        Self { producers: Some(producers) }
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

fn lane_id() -> String {
    let serial = NEXT_LANE_ID.fetch_add(1, Ordering::Relaxed);
    format!("{}-{}-{serial}", std::process::id(), process_instance_token())
}

fn process_instance_token() -> &'static str {
    static TOKEN: OnceLock<String> = OnceLock::new();
    TOKEN.get_or_init(|| {
        if let Ok(Some(process)) = read_process_instance(std::process::id()) {
            return format!("p{}", process.start_ticks);
        }

        let created = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos());
        format!("t{created}")
    })
}

struct ProcessInstance {
    state: u8,
    start_ticks: u64,
}

fn read_process_instance(pid: u32) -> std::io::Result<Option<ProcessInstance>> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let Some((_, after_comm)) = stat.rsplit_once(')') else { return Ok(None) };
    let mut fields = after_comm.split_whitespace();
    let Some(state) = fields.next().and_then(|field| field.as_bytes().first()).copied() else {
        return Ok(None);
    };
    let Some(start_ticks) = fields.nth(18).and_then(|field| field.parse().ok()) else {
        return Ok(None);
    };
    Ok(Some(ProcessInstance { state, start_ticks }))
}

fn lane_is_stale(lane: &str) -> bool {
    let mut parts = lane.splitn(3, '-');
    let Some(pid) = parts.next().and_then(|part| part.parse().ok()) else { return false };
    let Some(instance) = parts.next() else { return false };
    let Some(_serial) = parts.next().and_then(|part| part.parse::<u64>().ok()) else {
        return false;
    };
    match read_process_instance(pid) {
        Err(err) => err.kind() == std::io::ErrorKind::NotFound,
        Ok(Some(process)) if matches!(process.state, b'Z' | b'X') => true,
        Ok(Some(process)) => instance
            .strip_prefix('p')
            .and_then(|expected| expected.parse::<u64>().ok())
            .is_some_and(|expected| expected != process.start_ticks),
        Ok(None) => false,
    }
}

fn reap_stale_lane(file: &Path, lane: &str) -> bool {
    if !lane_is_stale(lane) {
        return false;
    }
    if let Err(err) = cleanup_flink(file) {
        warn!(%err, file = %file.display(), "failed to reap stale UDP telemetry lane");
    }
    true
}

fn lane_file(dir: &Path, name: &str, lane: &str) -> PathBuf {
    dir.join(format!("{name}{LANE_SEPARATOR}{lane}"))
}

fn producer<T: Copy + TypeHash>(file: &Path) -> Producer<T> {
    Producer::from(Queue::create_or_open_shared(file, QUEUE_SIZE, QueueType::SPMC))
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
            let Some(name) = file.file_name().and_then(|name| name.to_str()) else { continue };
            if name != queue_name && !name.starts_with(&lane_prefix) {
                continue;
            }
            match Queue::try_open_shared(file) {
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
    /// Opens every producer lane currently present for `group_name`.
    ///
    /// UDP writers must be created before the reader. Lane discovery happens
    /// once here, so writers created after this call are not observed.
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
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        // Directory scanning and temporary path allocation happen once during
        // reader startup; draining retains and reuses only the consumers.
        let mut files: Vec<_> = entries.flatten().map(|entry| entry.path()).collect();
        files.sort_unstable();
        files.retain(|file| {
            let lane = file
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.rsplit_once(LANE_SEPARATOR))
                .map(|(_, lane)| lane);
            !lane.is_some_and(|lane| reap_stale_lane(file, lane))
        });
        self.sends.discover(&files, &sends_queue(group_name));
        self.retransmits.discover(&files, &retransmits_queue(group_name));
        self.latency.discover(&files, &latency_queue(group_name));
        self.rtt.discover(&files, &rtt_queue(group_name));
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
    fn telemetry_uses_one_spmc_lane_per_writer() {
        let tmp = tempfile::tempdir().unwrap();
        let mut first = Telemetry::enabled_in_dir(tmp.path(), "group");
        let mut second = Telemetry::enabled_in_dir(tmp.path(), "group");
        let mut reader = TelemetryReader::open_in_dir(tmp.path(), "group");

        first.send(&UdpSend { bytes: 11, ..UdpSend::default() });
        second.send(&UdpSend { bytes: 22, ..UdpSend::default() });

        let mut bytes = Vec::new();
        reader.drain(|event| {
            if let TelemetryEvent::Send(event) = event {
                bytes.push(event.bytes);
            }
        });
        bytes.sort_unstable();
        assert_eq!(bytes, [11, 22]);

        flux_communication::cleanup_shmem(tmp.path());
    }
}
