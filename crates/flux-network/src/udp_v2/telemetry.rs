use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    path::Path,
};

use flux_communication::queue::{Producer, Queue, QueueType};
use flux_timing::{Instant, Nanos};
use flux_utils::directories::{local_share_dir, shmem_dir_queues_with_base};
use mio::Token;

use crate::NetworkTelemetry;

const QUEUE_SIZE: usize = 2usize.pow(15);

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UdpSend {
    pub peer: PeerId,
    pub sent_at: Instant,
    pub bytes: u32,
    pub datagrams: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UdpRetransmit {
    pub peer: PeerId,
    pub sent_at: Instant,
    pub bytes: u32,
    pub retries: u8,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UdpLatency {
    pub peer: PeerId,
    pub sent_at: Nanos,
    pub received_at: Nanos,
}

pub fn sends_queue(group_name: &str) -> String {
    format!("udp_sends_{group_name}")
}

pub fn retransmits_queue(group_name: &str) -> String {
    format!("udp_retransmits_{group_name}")
}

pub fn latency_queue(group_name: &str) -> String {
    format!("udp_latency_{group_name}")
}

pub(crate) struct Telemetry {
    sends: Option<Producer<UdpSend>>,
    retransmits: Option<Producer<UdpRetransmit>>,
    // TODO: a `Timer` here, once timers stop costing two shmem files per name
    // (see the TODO on `flux_communication::Timer`).
    latency: Option<Producer<UdpLatency>>,
}

impl Telemetry {
    pub(crate) fn new(telemetry: NetworkTelemetry, group_name: &str) -> Self {
        let NetworkTelemetry::Enabled { app_name } = telemetry else {
            return Self { sends: None, retransmits: None, latency: None };
        };
        let dir = shmem_dir_queues_with_base(local_share_dir(), app_name);
        let _ = std::fs::create_dir_all(&dir);
        Self {
            sends: Some(producer(&dir, &sends_queue(group_name))),
            retransmits: Some(producer(&dir, &retransmits_queue(group_name))),
            latency: Some(producer(&dir, &latency_queue(group_name))),
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
}

fn producer<T: Copy>(dir: &Path, name: &str) -> Producer<T> {
    // Most apps poll the network from one tile, so a single producer would
    // do, but nothing guarantees that while queues are named per group: two
    // tiles running the same group share them.
    // TODO: per-core shmem, so each core's writer gets SPMC queues of its own.
    Producer::from(Queue::create_or_open_shared(dir.join(name), QUEUE_SIZE, QueueType::MPMC))
}
