//! Reliable, unordered UDP connections for a [`crate::Network`] group.
//!
//! Messages are packed into a stream of packets, so a lost packet delays only
//! the messages it carries.

use std::{
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    os::fd::AsRawFd,
};

use flux_communication::Timer;
use flux_timing::{Duration, Instant, Nanos};
use flux_utils::DCache;
use mio::{Interest, Registry, Token, net::UdpSocket};
use tracing::{debug, warn};

mod conn;
mod inbound;
mod outbound;
mod stream;
mod sys;
mod wire;

pub(crate) use conn::UdpManager;
use sys::{RecvBatch, SendBatch};

use crate::{
    NetworkTelemetry,
    network::{RxPayload, UdpGroupConfig, set_socket_buf_size},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SendOutcome {
    Done,
    WouldBlock,
}

#[inline]
pub(crate) fn send_datagram(socket: &UdpSocket, addr: SocketAddr, bytes: &[u8]) -> SendOutcome {
    match socket.send_to(bytes, addr) {
        Ok(_) => SendOutcome::Done,
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => SendOutcome::WouldBlock,
        Err(e) => {
            debug!(?e, %addr, "udp send failed");
            SendOutcome::Done
        }
    }
}

pub(crate) fn new_session(salt: usize) -> u32 {
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

/// Tuning for UDP connections and pub/sub. Both ends must agree on
/// `max_datagram_size` and `reliable`.
///
/// The retransmit timeout is measured from acks (RFC 6298) and clamped to
/// `[min_rto, max_rto]`; `initial_rto` only applies before the first sample.
#[derive(Clone, Copy, Debug)]
pub struct UdpConfig {
    /// Datagram size including the 25-byte packet header: at most the path MTU
    /// minus the IP and UDP headers, 28 bytes over IPv4 and 48 over IPv6.
    pub max_datagram_size: usize,
    /// Packets a sender holds until its slowest receiver acks them. Power of
    /// two, at least 64. A receiver that falls a window behind is dropped.
    /// There is no flow control, so a receiver's socket buffer should hold a
    /// full window.
    pub send_window: usize,
    /// Packets a receiver tracks above its ack point. Power of two, at least
    /// 64. Should be at least the sender's `send_window`.
    pub recv_window: usize,
    /// Largest message accepted or sent. Must fit in `send_window`.
    pub max_message_size: usize,
    pub initial_rto: Duration,
    pub min_rto: Duration,
    pub max_rto: Duration,
    /// How often an idle receiver acks. Must be well below the sender's
    /// `peer_timeout`.
    pub heartbeat_interval: Duration,
    /// Off: no retransmits, a lost packet loses the messages it carries.
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

pub(crate) fn latency_timer(telemetry: NetworkTelemetry, label: &str) -> Option<Timer> {
    let NetworkTelemetry::Enabled { app_name } = telemetry else { return None };
    Some(Timer::new(app_name, format!("udp_latency_{label}")))
}

#[inline]
pub(crate) fn deliver_message<F>(
    latency: &mut Option<Timer>,
    bytes: &[u8],
    send_ts: Nanos,
    dcache: Option<&DCache>,
    deliver: &mut F,
) where
    F: FnMut(RxPayload<'_>, Nanos),
{
    if let Some(t) = latency {
        t.emit_latency_from_nanos(send_ts, Nanos::now());
    }
    match dcache {
        None => deliver(RxPayload::Raw(bytes), send_ts),
        Some(dc) => match dc.write(bytes.len(), |buf| buf.copy_from_slice(bytes)) {
            Ok(dref) => deliver(RxPayload::DCache(dref), send_ts),
            Err(e) => warn!("dcache write failed: {e}"),
        },
    }
}

pub(crate) fn unspecified(peer: SocketAddr) -> SocketAddr {
    match peer {
        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
    }
}

pub(crate) struct Socket {
    pub(crate) token: Token,
    pub(crate) udp: UdpSocket,
    writable_armed: bool,
}

impl Socket {
    pub(crate) fn open(
        registry: &Registry,
        bind: SocketAddr,
        token: Token,
        batch: &mut SendBatch,
        buf_size: Option<usize>,
    ) -> io::Result<Self> {
        let mut socket = UdpSocket::bind(bind)?;
        #[cfg(target_os = "linux")]
        if let Err(err) = RecvBatch::enable_gro(socket.as_raw_fd()) {
            debug!(?err, "UDP GRO unavailable");
        }
        #[cfg(target_os = "linux")]
        if let Err(err) = batch.enable_gso(socket.as_raw_fd()) {
            debug!(?err, "UDP GSO unavailable");
        }
        if let Some(size) = buf_size {
            set_socket_buf_size(&socket, size);
        }
        registry.register(&mut socket, token, Interest::READABLE)?;
        Ok(Self { token, udp: socket, writable_armed: false })
    }

    #[inline]
    pub(crate) fn fd(&self) -> i32 {
        self.udp.as_raw_fd()
    }

    #[inline]
    pub(crate) fn arm_writable(&mut self, registry: &Registry) {
        if self.writable_armed {
            return;
        }
        if let Err(err) =
            registry.reregister(&mut self.udp, self.token, Interest::READABLE | Interest::WRITABLE)
        {
            debug!(?err, "udp: reregister writable");
            return;
        }
        self.writable_armed = true;
    }

    /// Before handling a writable event.
    #[inline]
    pub(crate) fn on_writable(&mut self) {
        self.writable_armed = false;
    }

    /// After handling a writable event: stop waking up for it unless the
    /// handling re-armed it.
    #[inline]
    pub(crate) fn disarm(&mut self, registry: &Registry) {
        if !self.writable_armed &&
            let Err(err) = registry.reregister(&mut self.udp, self.token, Interest::READABLE)
        {
            debug!(?err, "udp: reregister drop writable");
        }
    }

    pub(crate) fn close(&mut self, registry: &Registry) {
        let _ = registry.deregister(&mut self.udp);
    }
}

/// Drains readable datagrams into `on_datagram`.
pub(crate) fn recv_all(
    fd: i32,
    recv: &mut RecvBatch,
    mut on_datagram: impl FnMut(&[u8], SocketAddr),
) {
    loop {
        let n = match recv.recv(fd) {
            Ok(n) => n,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => {
                debug!(?err, "udp recv failed");
                break;
            }
        };
        for i in 0..n {
            let Some((datagrams, from)) = recv.datagrams(i) else { continue };
            for bytes in datagrams {
                on_datagram(bytes, from);
            }
        }
    }
}

/// `handshake`: the bytes before the on-connect message in the datagram
/// carrying it.
pub(crate) fn validate_group(config: &UdpGroupConfig, handshake: usize) {
    let udp = config.udp;
    udp.validate();
    if let Some(msg) = &config.on_connect_msg {
        assert!(!msg.is_empty(), "udp on_connect_msg must be nonempty");
        assert!(msg.len() <= udp.max_message_size, "udp on_connect_msg exceeds max_message_size");
        assert!(
            handshake + msg.len() <= udp.max_datagram_size,
            "udp on_connect_msg does not fit in a handshake datagram of max_datagram_size"
        );
    }
}

pub(crate) fn on_connect_fits(config: &UdpGroupConfig, msg: &[u8], from: SocketAddr) -> bool {
    let fits = msg.len() <= config.udp.max_message_size;
    if !fits {
        warn!(addr = %from, len = msg.len(), "udp on-connect message exceeds max_message_size");
    }
    fits
}
