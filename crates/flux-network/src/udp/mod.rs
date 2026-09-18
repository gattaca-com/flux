//! Reliable, unordered UDP transport for [`crate::NetworkDriver`].
//!
//! Each message is fragmented into datagrams that carry consecutive sequence
//! numbers. The receiver acks with a cumulative point plus a selective bitmap
//! covering everything it holds above it. The sender resends holes below the
//! highest acked sequence at once, and after an RTO of ack silence probes the
//! oldest unacked datagram, doubling the probe size per round. Messages are
//! delivered as soon as all their fragments are in, so a lost datagram delays
//! only its own message.

use flux_timing::Duration;

mod connector;
mod peer;
mod sys;
mod wire;

pub(crate) use connector::UdpManager;

/// Socket I/O implementation. The wire protocol is identical for both backends.
#[derive(Clone, Copy, Debug, Default)]
pub enum UdpIo {
    #[default]
    Syscall,
    /// Linux `io_uring` with bounded per-socket buffers. Requires synchronous
    /// cancellation support (Linux 6.0+); socket creation fails if unavailable.
    #[cfg(target_os = "linux")]
    Uring(UringConfig),
}

/// Per-socket operation limits. Buffers are allocated when the socket opens.
///
/// Each entry reserves approximately 64 KiB; defaults use about 6 MiB/socket.
/// `recv_entries` must be a power of two, both counts must be nonzero, and
/// their sum must not exceed 4096.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug)]
pub struct UringConfig {
    /// Outstanding sends, including GSO groups and control packets.
    pub send_entries: u16,
    /// Buffers shared by the socket's multishot receive.
    pub recv_entries: u16,
}

#[cfg(target_os = "linux")]
impl Default for UringConfig {
    fn default() -> Self {
        Self { send_entries: 64, recv_entries: 32 }
    }
}

/// Tuning for [`crate::Transport::Udp`].
///
/// The retransmit timeout is measured from acks (RFC 6298) and clamped to
/// `[min_rto, max_rto]`; `initial_rto` only applies before the first sample.
/// [`Self::lan`] and [`Self::wan`] preset the clamps for co-located and
/// cross-region pairs. Both ends of a link must agree on
/// `max_datagram_size`.
#[derive(Clone, Copy, Debug)]
pub struct UdpConfig {
    /// Local I/O backend; peers may use different backends. Queued `io_uring`
    /// sends progress through `NetworkDriver::poll_with`, like other backlogs.
    pub io: UdpIo,
    /// Datagram size including the 29-byte header. 1200 stays under the
    /// 1280-byte IPv6 minimum MTU.
    pub max_datagram_size: usize,
    /// Datagrams a sender may have in flight per peer, counted from the oldest
    /// message not yet fully acked. Power of two, at least 64. A message that
    /// does not fit disconnects the peer, like an exceeded backlog. There is
    /// no flow control, so the receiver's socket buffer should hold a full
    /// window.
    pub send_window: usize,
    /// Datagrams a receiver tracks above its ack point. Power of two, at least
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
}

impl Default for UdpConfig {
    fn default() -> Self {
        Self {
            io: UdpIo::Syscall,
            max_datagram_size: 1200,
            send_window: 16 * 1024,
            recv_window: 16 * 1024,
            max_message_size: 16 * 1024 * 1024,
            initial_rto: Duration::from_millis(20),
            min_rto: Duration::from_micros(500),
            max_rto: Duration::from_secs(1),
            heartbeat_interval: Duration::from_millis(250),
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

    /// Payload bytes per datagram.
    #[inline]
    pub(crate) fn stride(&self) -> usize {
        self.max_datagram_size - wire::HEADER_SIZE
    }

    pub(crate) fn validate(&self) {
        #[cfg(target_os = "linux")]
        if let UdpIo::Uring(config) = self.io {
            assert!(config.send_entries > 0 && config.recv_entries.is_power_of_two());
            assert!(u32::from(config.send_entries) + u32::from(config.recv_entries) <= 4096);
        }
        assert!(
            self.max_datagram_size > wire::HEADER_SIZE &&
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
        let fragments = wire::fragment_count(self.max_message_size, self.stride());
        assert!(fragments <= self.send_window, "udp max_message_size does not fit in send_window");
        assert!(
            fragments <= wire::MAX_FRAGMENTS,
            "udp max_message_size exceeds u16 fragment index"
        );
        assert!(self.min_rto <= self.max_rto, "udp min_rto exceeds max_rto");
    }
}
