use std::{io, net::SocketAddr};

use flux_timing::{Duration, Instant};
use mio::net::UdpSocket;
use tracing::debug;

mod inbound;
mod manager;
mod stream;
mod subscriber;
mod subscription;
mod sys;
mod wire;

pub(crate) use manager::UdpManager;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SendOutcome {
    Done,
    WouldBlock,
}

#[inline]
fn send_datagram(socket: &UdpSocket, addr: SocketAddr, bytes: &[u8]) -> SendOutcome {
    match socket.send_to(bytes, addr) {
        Ok(_) => SendOutcome::Done,
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => SendOutcome::WouldBlock,
        Err(e) => {
            debug!(?e, %addr, "udp send failed");
            SendOutcome::Done
        }
    }
}

fn new_session(salt: usize) -> u32 {
    let t = Instant::now().0;
    let mut x = t ^
        (t >> 32) ^
        (u64::from(std::process::id()) << 20) ^
        (salt as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x as u32
}

/// The retransmit timeout is measured from acks (RFC 6298) and clamped to
/// `[min_rto, max_rto]`; `initial_rto` only applies before the first sample.
#[derive(Clone, Copy, Debug)]
pub struct UdpConfig {
    /// Datagram size including the 25-byte packet header: at most the path MTU
    /// minus the IP and UDP headers, 28 bytes over IPv4 and 48 over IPv6. A
    /// publisher and its subscribers must agree.
    pub max_datagram_size: usize,
    /// Packets a publisher holds for its slowest subscriber. Power of two, at
    /// least 64. There is no flow control, so a subscriber's socket buffer
    /// should hold a full window.
    pub send_window: usize,
    /// Packets a subscriber tracks above its ack point. Power of two, at least
    /// 64. Should be at least the publisher's `send_window`.
    pub recv_window: usize,
    /// Largest message accepted or sent. Must fit in `send_window`.
    pub max_message_size: usize,
    pub initial_rto: Duration,
    pub min_rto: Duration,
    pub max_rto: Duration,
    /// How often an idle subscriber acks. Must be well below the publisher's
    /// `peer_timeout`.
    pub heartbeat_interval: Duration,
    /// Off: no retransmits, a lost packet loses the messages it carries. A
    /// publisher and its subscribers must agree.
    pub reliable: bool,
}

impl Default for UdpConfig {
    fn default() -> Self {
        Self {
            max_datagram_size: 1200,
            send_window: 16 * 1024,
            recv_window: 16 * 1024,
            max_message_size: 16 * 1024 * 1024,
            initial_rto: Duration::from_millis(20),
            min_rto: Duration::from_micros(500),
            max_rto: Duration::from_secs(1),
            heartbeat_interval: Duration::from_millis(250),
            reliable: true,
        }
    }
}

impl UdpConfig {
    /// Same datacenter: sub-millisecond RTT.
    pub fn lan() -> Self {
        Self {
            initial_rto: Duration::from_millis(1),
            min_rto: Duration::from_micros(250),
            max_rto: Duration::from_millis(50),
            heartbeat_interval: Duration::from_millis(100),
            ..Self::default()
        }
    }

    /// Cross-region: tens to hundreds of milliseconds RTT.
    pub fn wan() -> Self {
        Self {
            initial_rto: Duration::from_millis(100),
            min_rto: Duration::from_millis(5),
            max_rto: Duration::from_secs(2),
            ..Self::default()
        }
    }

    pub(crate) fn packets_per_message(&self) -> usize {
        (self.max_message_size + wire::MESSAGE_HEADER) /
            (self.max_datagram_size - wire::PACKET_HEADER) +
            2
    }

    pub(crate) fn validate(&self) {
        assert!(
            self.max_datagram_size > wire::PACKET_HEADER + wire::MESSAGE_HEADER &&
                self.max_datagram_size <= wire::MAX_DATAGRAM_SIZE,
            "udp max_datagram_size {} out of range",
            self.max_datagram_size
        );
        for (name, w) in [("send_window", self.send_window), ("recv_window", self.recv_window)] {
            assert!(w.is_power_of_two() && w >= 64, "udp {name} must be a power of two >= 64");
        }
        assert!(
            self.max_message_size > 0 && u32::try_from(self.max_message_size).is_ok(),
            "udp max_message_size out of range"
        );
        assert!(
            self.packets_per_message() <= self.send_window,
            "udp max_message_size does not fit in send_window"
        );
        assert!(self.min_rto <= self.max_rto, "udp min_rto exceeds max_rto");
    }
}
