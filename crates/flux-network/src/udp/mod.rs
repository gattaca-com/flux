//! Reliable, unordered UDP transport for [`crate::Network`].
//!
//! Messages are cut into packets of `max_datagram_size`: several small
//! messages share a packet, a large one spans several. The packet is the
//! unit of acks, retention and retransmission, and every packet decodes on
//! its own, so datagrams may arrive in any order. The receiver acks with a
//! cumulative point plus a selective bitmap covering everything it holds
//! above it. The sender resends holes below the highest acked sequence at
//! once, and after an RTO of ack silence probes the oldest unacked packet,
//! doubling the probe size per round. Messages are delivered as soon as all
//! their bytes are in, so a lost packet delays only the messages in it.
//!
//! With [`UdpConfig::reliable`] off the same wire format carries a fire-and-
//! forget stream: the sender releases a packet once the kernel takes it and
//! never resends, the receiver acks only as a heartbeat and skips past holes.
//! A lost packet loses the messages in it and nothing else.

use flux_timing::Duration;

mod inbound;
mod manager;
mod outbound;
mod peer;
mod sys;
mod wire;

pub(crate) use manager::UdpManager;

/// Tuning for [`crate::UdpGroupConfig`].
///
/// The retransmit timeout is measured from acks (RFC 6298) and clamped to
/// `[min_rto, max_rto]`; `initial_rto` only applies before the first sample.
/// [`Self::lan`] and [`Self::wan`] preset the clamps for co-located and
/// cross-region pairs.
#[derive(Clone, Copy, Debug)]
pub struct UdpConfig {
    /// Datagram size including the 25-byte packet header. 1200 stays under
    /// the 1280-byte IPv6 minimum MTU. A receiver must have been configured
    /// with at least the sender's size.
    pub max_datagram_size: usize,
    /// Packets a sender may have in flight per peer, counted from the oldest
    /// message not yet fully acked. Power of two, at least 64. A message that
    /// does not fit disconnects the peer, like an exceeded backlog. There is
    /// no flow control, so the receiver's socket buffer should hold a full
    /// window.
    pub send_window: usize,
    /// Packets a receiver tracks above its ack point. Power of two, at least
    /// 64. Should be at least the peer's `send_window`.
    pub recv_window: usize,
    /// Largest message accepted or sent. Must fit in `send_window`.
    pub max_message_size: usize,
    pub initial_rto: Duration,
    pub min_rto: Duration,
    pub max_rto: Duration,
    /// Idle interval after which a connected peer sends a bare ack so the
    /// other side can tell it is alive. Peer death is detected by the
    /// connector's user timeout.
    pub heartbeat_interval: Duration,
    /// Off: no retransmits, no per-packet acks; a packet is done once the
    /// kernel takes it and the receiver drops whatever a lost packet leaves
    /// incomplete. Acks still flow at `heartbeat_interval` for liveness. Both
    /// ends of a link must agree. Nothing throttles a burst, so the receiver's
    /// socket buffer must hold the largest one or its overflow is loss too.
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

    /// Most packets one message can occupy.
    pub(crate) fn packets_per_message(&self) -> usize {
        self.max_message_size / (self.max_datagram_size - wire::PACKET_HEADER) + 2
    }

    pub(crate) fn validate(&self) {
        assert!(
            self.max_datagram_size > wire::PACKET_HEADER + wire::LONG_HEADER &&
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
