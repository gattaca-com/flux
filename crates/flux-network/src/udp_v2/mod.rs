//! Reliable, unordered UDP connections for a [`crate::Network`] group, wire
//! v2. Doesn't interoperate with [`crate::udp`] (v1); both run side by side
//! until every peer is on v2.
//!
//! Messages are packed into a stream of packets, so a lost packet delays only
//! the messages it carries.

use std::{
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    os::fd::AsRawFd,
};

use flux_timing::{Instant, Nanos};
use flux_utils::DCache;
use mio::{Interest, Registry, Token, net::UdpSocket};
use tracing::{debug, warn};

mod conn;
mod inbound;
mod outbound;
mod stream;
mod sys;
mod telemetry;
mod wire;

pub(crate) use conn::UdpManager;
pub use telemetry::{
    PeerId, UdpLatency, UdpRetransmit, UdpSend, latency_queue, retransmits_queue, sends_queue,
};
use sys::{RecvBatch, SendBatch};

use crate::{
    network::{RxPayload, UdpGroupConfigV2, set_socket_buf_size},
    udp::UdpConfig,
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

fn packets_per_message(udp: &UdpConfig) -> usize {
    (udp.max_message_size + wire::MESSAGE_HEADER) / (udp.max_datagram_size - wire::PACKET_HEADER) +
        2
}

fn validate(udp: &UdpConfig) {
    assert!(
        udp.max_datagram_size > wire::PACKET_HEADER + wire::MESSAGE_HEADER &&
            udp.max_datagram_size <= wire::MAX_DATAGRAM_SIZE,
        "udp max_datagram_size {} out of range",
        udp.max_datagram_size
    );
    for (name, w) in [("send_window", udp.send_window), ("recv_window", udp.recv_window)] {
        assert!(w.is_power_of_two() && w >= 64, "udp {name} must be a power of two >= 64");
    }
    assert!(
        udp.max_message_size > 0 && u32::try_from(udp.max_message_size).is_ok(),
        "udp max_message_size out of range"
    );
    assert!(
        packets_per_message(udp) <= udp.send_window,
        "udp max_message_size does not fit in send_window"
    );
    assert!(udp.min_rto <= udp.max_rto, "udp min_rto exceeds max_rto");
}

#[inline]
pub(crate) fn deliver_message<F>(
    bytes: &[u8],
    send_ts: Nanos,
    dcache: Option<&DCache>,
    deliver: &mut F,
) where
    F: FnMut(RxPayload<'_>, Nanos),
{
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
pub(crate) fn validate_group(config: &UdpGroupConfigV2, handshake: usize) {
    let udp = config.udp;
    validate(&udp);
    if let Some(msg) = &config.on_connect_msg {
        assert!(!msg.is_empty(), "udp on_connect_msg must be nonempty");
        assert!(msg.len() <= udp.max_message_size, "udp on_connect_msg exceeds max_message_size");
        assert!(
            handshake + msg.len() <= udp.max_datagram_size,
            "udp on_connect_msg does not fit in a handshake datagram of max_datagram_size"
        );
    }
}

pub(crate) fn on_connect_fits(config: &UdpGroupConfigV2, msg: &[u8], from: SocketAddr) -> bool {
    let fits = msg.len() <= config.udp.max_message_size;
    if !fits {
        warn!(addr = %from, len = msg.len(), "udp on-connect message exceeds max_message_size");
    }
    fits
}
