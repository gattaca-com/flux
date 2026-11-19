use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    path::Path,
};

use flux::{type_hash::TypeHash, type_hash_derive::TypeHash};
use flux_communication::{
    ReadError,
    queue::{MappedBroadcastConsumer, Producer, Queue, QueueType},
};
use flux_timing::{Instant, Nanos};
use flux_utils::directories::{local_share_dir, shmem_dir_queues_with_base};
use mio::Token;
use tracing::warn;

use crate::NetworkTelemetry;

const QUEUE_SIZE: usize = 2usize.pow(15);

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
    sends: Option<Producer<UdpSend>>,
    retransmits: Option<Producer<UdpRetransmit>>,
    // TODO: a `Timer` here, once timers stop costing two shmem files per name
    // (see the TODO on `flux_communication::Timer`).
    latency: Option<Producer<UdpLatency>>,
    rtt: Option<Producer<UdpRtt>>,
}

impl Telemetry {
    pub(crate) fn new(telemetry: NetworkTelemetry, group_name: &str) -> Self {
        let NetworkTelemetry::Enabled { app_name } = telemetry else {
            return Self { sends: None, retransmits: None, latency: None, rtt: None };
        };
        let dir = shmem_dir_queues_with_base(local_share_dir(), app_name);
        let _ = std::fs::create_dir_all(&dir);
        Self {
            sends: Some(producer(&dir, &sends_queue(group_name))),
            retransmits: Some(producer(&dir, &retransmits_queue(group_name))),
            latency: Some(producer(&dir, &latency_queue(group_name))),
            rtt: Some(producer(&dir, &rtt_queue(group_name))),
        }
    }

    #[inline]
    pub(crate) fn send(&mut self, event: &UdpSend) {
        if let Some(sends) = &mut self.sends {
            sends.produce(event);
        }
    }

    #[inline]
    pub(crate) fn retransmit(&mut self, event: &UdpRetransmit) {
        if let Some(retransmits) = &mut self.retransmits {
            retransmits.produce(event);
        }
    }

    #[inline]
    pub(crate) fn latency(&mut self, peer: PeerId, sent_at: Nanos) {
        if let Some(latency) = &mut self.latency {
            latency.produce(&UdpLatency { peer, sent_at, received_at: Nanos::now() });
        }
    }

    #[inline]
    pub(crate) fn rtt(&mut self, event: &UdpRtt) {
        if let Some(rtt) = &mut self.rtt {
            rtt.produce(event);
        }
    }
}

fn producer<T: Copy + TypeHash>(dir: &Path, name: &str) -> Producer<T> {
    // Most apps poll the network from one tile, so a single producer would
    // do, but nothing guarantees that while queues are named per group: two
    // tiles running the same group share them.
    // TODO: per-core shmem, so each core's writer gets SPMC queues of its own.
    Producer::from(Queue::create_or_open_shared(dir.join(name), QUEUE_SIZE, QueueType::MPMC))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TelemetryEvent {
    Send(UdpSend),
    Retransmit(UdpRetransmit),
    Latency(UdpLatency),
    Rtt(UdpRtt),
    /// The reader fell behind and missed events.
    Lapped,
}

pub struct TelemetryReader {
    sends: Option<MappedBroadcastConsumer<UdpSend>>,
    retransmits: Option<MappedBroadcastConsumer<UdpRetransmit>>,
    latency: Option<MappedBroadcastConsumer<UdpLatency>>,
    rtt: Option<MappedBroadcastConsumer<UdpRtt>>,
}

impl TelemetryReader {
    pub fn open(app_name: &str, group_name: &str) -> Self {
        let dir = shmem_dir_queues_with_base(local_share_dir(), app_name);
        Self {
            sends: consumer(&dir, &sends_queue(group_name)),
            retransmits: consumer(&dir, &retransmits_queue(group_name)),
            latency: consumer(&dir, &latency_queue(group_name)),
            rtt: consumer(&dir, &rtt_queue(group_name)),
        }
    }

    pub fn drain(&mut self, mut on_event: impl FnMut(TelemetryEvent)) {
        drain(&mut self.sends, &mut on_event, TelemetryEvent::Send);
        drain(&mut self.retransmits, &mut on_event, TelemetryEvent::Retransmit);
        drain(&mut self.latency, &mut on_event, TelemetryEvent::Latency);
        drain(&mut self.rtt, &mut on_event, TelemetryEvent::Rtt);
    }
}

fn consumer<T: Copy + TypeHash>(dir: &Path, name: &str) -> Option<MappedBroadcastConsumer<T>> {
    let file = dir.join(name);
    match MappedBroadcastConsumer::open(&file, "udp_telemetry") {
        Ok(consumer) => Some(consumer),
        Err(err) => {
            warn!(?err, file = %file.display(), "no udp telemetry");
            None
        }
    }
}

fn drain<T: Copy + Default>(
    consumer: &mut Option<MappedBroadcastConsumer<T>>,
    on_event: &mut impl FnMut(TelemetryEvent),
    wrap: fn(T) -> TelemetryEvent,
) {
    let Some(consumer) = consumer else { return };
    let mut event = T::default();
    loop {
        match consumer.try_consume(&mut event) {
            Ok(()) => on_event(wrap(event)),
            Err(ReadError::SpedPast) => {
                on_event(TelemetryEvent::Lapped);
                consumer.recover_after_error();
            }
            Err(ReadError::Empty) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlapping_readers_share_mapping_until_last_reader_drops() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("send");
        Queue::<u64>::create_or_open_shared(&file, 4, QueueType::MPMC);
        let name = std::fs::read_to_string(&file).unwrap();
        let object = Path::new("/dev/shm").join(name.trim_start_matches('/'));
        let counts = || {
            let mappings = std::fs::read_to_string("/proc/self/maps").unwrap();
            let maps = mappings
                .lines()
                .filter(|line| line.split_whitespace().next_back() == object.to_str())
                .count();
            let fds = std::fs::read_dir("/proc/self/fd")
                .unwrap()
                .flatten()
                .filter_map(|entry| std::fs::read_link(entry.path()).ok())
                .filter(|target| target == &object)
                .count();
            (maps, fds)
        };
        let initial = counts();
        assert!(initial.0 > 0 && initial.1 > 0);
        let with_one_reader = (initial.0 + 1, initial.1 + 1);
        let open_reader = || consumer::<u64>(tmp.path(), "send").unwrap();
        let reader_cycle = || {
            let first = open_reader();
            let first_open = counts();
            let second = open_reader();
            let both_open = counts();
            drop(first);
            let first_dropped = counts();
            drop(second);
            [first_open, both_open, first_dropped, counts()]
        };
        let observed = [reader_cycle(), reader_cycle()];
        flux_communication::cleanup_flink(&file).unwrap();
        assert_eq!(observed, [[with_one_reader, with_one_reader, with_one_reader, initial]; 2]);
    }

    #[test]
    fn reopened_readers_follow_replacement() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("send");
        let writer =
            || Producer::<u64>::from(Queue::create_or_open_shared(&file, 4, QueueType::MPMC));
        let reader = || consumer::<u64>(tmp.path(), "send").unwrap();
        let mut original = writer();
        let first = reader();
        flux_communication::cleanup_flink(&file).unwrap();
        let mut replacement = writer();
        let second = reader();
        let third = reader();
        original.produce(&1);
        replacement.produce(&2);
        for (mut reader, expected) in [(first, 1), (second, 2), (third, 2)] {
            let mut value = 0;
            reader.try_consume(&mut value).unwrap();
            assert_eq!(value, expected);
        }
        flux_communication::cleanup_flink(&file).unwrap();
    }
}
