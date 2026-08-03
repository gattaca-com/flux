use std::{
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    time::Duration,
};

mod control;
mod publisher;
mod subscriber;
mod wire;
mod xdp;

pub use publisher::{PublisherEvent, UdpPublisher, UdpPublisherStats};
pub use subscriber::{SubscriberEvent, UdpSubscriber, UdpSubscriberStats};
pub use wire::{DEFAULT_IPV4_MAX_DATAGRAM_SIZE, DEFAULT_IPV6_MAX_DATAGRAM_SIZE};
use wire::{MAX_DATAGRAM_SIZE, UDP_HEADER_SIZE, default_max_datagram_size_for};

const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
pub const MAX_XDP_UNICAST_DESTINATIONS: usize = 64;

/// Controls whether the publisher always waits for its configured batch or
/// bypasses that wait when publications are arriving sparsely.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UdpSendBatchMode {
    /// Always coalesce up to `send_batch_size`, bounded cooperatively by
    /// `send_batch_max_delay`.
    #[default]
    Fixed,
    /// Send the first publication immediately after an idle interval, then
    /// batch while publications continue to arrive within the batch delay.
    Adaptive,
}

/// IPv4 multicast data-plane configuration. TCP subscription, progress, and
/// repair connections remain unicast per subscriber.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UdpMulticastConfig {
    /// Group and destination port used for normal UDP publications.
    pub group: SocketAddrV4,
    /// Local IPv4 interface address used to send or join the group.
    pub interface: Ipv4Addr,
    /// Multicast TTL. One confines traffic to the directly connected network.
    pub ttl: u8,
    /// Deliver transmitted multicast packets to local group members. Usually
    /// false in production; useful for same-host integration tests.
    pub loopback: bool,
}

impl UdpMulticastConfig {
    pub const fn new(group: SocketAddrV4, interface: Ipv4Addr) -> Self {
        Self { group, interface, ttl: 1, loopback: false }
    }
}

/// One static IPv4 unicast destination for direct `AF_XDP` transmission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UdpXdpUnicastDestination {
    /// Receiver IPv4 address and UDP port.
    pub address: SocketAddrV4,
    /// Receiver Ethernet address on the directly connected network.
    pub destination_mac: [u8; 6],
}

impl UdpXdpUnicastDestination {
    pub const fn new(address: SocketAddrV4, destination_mac: [u8; 6]) -> Self {
        Self { address, destination_mac }
    }
}

/// Linux `AF_XDP` sender configuration.
///
/// The backend binds one `AF_XDP` socket to one physical NIC transmit queue.
/// It either derives one destination from [`UdpConfig::multicast`] or fans
/// each datagram out to `unicast_destinations`. `source_mac` and `vlan_id`
/// describe the Ethernet frame placed directly on that physical interface.
///
/// Native zero-copy TX should use a dedicated hardware queue excluded from
/// normal receive-side RSS. Some drivers rebuild their queues while attaching
/// XDP, so queue/RSS isolation must be applied after the XDP link is active and
/// before production traffic starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UdpXdpConfig {
    /// Physical interface index, as returned by `if_nametoindex`.
    pub interface_index: u32,
    /// Physical NIC queue bound by the XSK. CPU affinity is configured
    /// separately by the caller.
    pub queue_id: u32,
    /// Source Ethernet address of the physical interface.
    pub source_mac: [u8; 6],
    /// Source IPv4 address used for unicast fanout. Multicast derives this
    /// from [`UdpMulticastConfig::interface`].
    pub source_ip: Option<Ipv4Addr>,
    /// Static unicast fanout destinations. Leave empty when using multicast.
    /// Direct `AF_XDP` transmission bypasses kernel routing and neighbor
    /// lookup, so every destination MAC must be supplied explicitly.
    pub unicast_destinations: [Option<UdpXdpUnicastDestination>; MAX_XDP_UNICAST_DESTINATIONS],
    /// Optional 802.1Q VLAN identifier inserted into every frame.
    pub vlan_id: Option<u16>,
    /// Number of descriptors in both the TX and completion rings.
    pub ring_size: u32,
    /// Number of fixed-size frames in UMEM.
    pub frame_count: u32,
    /// Size of each UMEM frame. 2048 is sufficient for the default MTU.
    pub frame_size: u32,
    /// Requested driver data path. Copy mode is the conservative default.
    pub mode: UdpXdpMode,
    /// Temporarily attach a native-mode `XDP_PASS` program for drivers that
    /// require an active XDP hook before zero-copy TX can be initialized.
    pub attach_xdp_pass: bool,
    /// Continue with the configured kernel UDP sender if `AF_XDP` setup fails.
    pub fallback_to_socket: bool,
}

impl UdpXdpConfig {
    pub const fn new(interface_index: u32, queue_id: u32, source_mac: [u8; 6]) -> Self {
        Self {
            interface_index,
            queue_id,
            source_mac,
            source_ip: None,
            unicast_destinations: [None; MAX_XDP_UNICAST_DESTINATIONS],
            vlan_id: None,
            ring_size: 4096,
            frame_count: 8192,
            frame_size: 2048,
            mode: UdpXdpMode::Copy,
            attach_xdp_pass: false,
            fallback_to_socket: true,
        }
    }
}

/// Driver mode requested when binding an `AF_XDP` publisher socket.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UdpXdpMode {
    #[default]
    Copy,
    ZeroCopy,
}

pub(crate) struct NativeSocketAddr {
    pub(crate) address: libc::sockaddr_storage,
    pub(crate) address_length: libc::socklen_t,
}

impl NativeSocketAddr {
    pub(crate) fn encode(address: SocketAddr) -> Self {
        let mut storage: libc::sockaddr_storage = unsafe { core::mem::zeroed() };
        let address_length = match address {
            SocketAddr::V4(address) => {
                let encoded = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: address.port().to_be(),
                    sin_addr: libc::in_addr { s_addr: u32::from_ne_bytes(address.ip().octets()) },
                    sin_zero: [0; 8],
                };
                unsafe {
                    core::ptr::write(
                        core::ptr::from_mut(&mut storage).cast::<libc::sockaddr_in>(),
                        encoded,
                    );
                }
                core::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
            }
            SocketAddr::V6(address) => {
                let encoded = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: address.port().to_be(),
                    sin6_flowinfo: address.flowinfo().to_be(),
                    sin6_addr: libc::in6_addr { s6_addr: address.ip().octets() },
                    sin6_scope_id: address.scope_id(),
                };
                unsafe {
                    core::ptr::write(
                        core::ptr::from_mut(&mut storage).cast::<libc::sockaddr_in6>(),
                        encoded,
                    );
                }
                core::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
            }
        };
        Self { address: storage, address_length }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct UdpConfig {
    /// Largest UDP datagram, including the Flux UDP header. This must match
    /// across the publisher and all subscribers because it defines the
    /// fragment stride.
    pub max_datagram_size: usize,
    /// Largest complete application message accepted by either endpoint.
    /// This bounds subscriber reassembly allocations.
    pub max_message_size: usize,
    /// Number of recent message sequences retained by the publisher and
    /// tracked by subscribers. Must be a power of two.
    pub sequence_window: usize,
    /// Optional kernel send and receive buffer size for UDP and TCP repair
    /// sockets.
    pub socket_buf_size: Option<usize>,
    /// How often the publisher broadcasts its next sequence number.
    pub progress_interval: Duration,
    /// Minimum time a missing or partially assembled message remains pending
    /// before the subscriber requests full-message repair.
    pub repair_delay: Duration,
    /// Maximum message sequences with an outstanding repair request per
    /// subscriber.
    pub max_inflight_repair_requests: usize,
    /// Delay between subscriber TCP repair reconnection attempts.
    pub reconnect_interval: Duration,
    /// Maximum logical publications coalesced by the publisher before it
    /// enters the UDP send path. One preserves the immediate legacy path.
    pub send_batch_size: usize,
    /// Cooperative upper bound for a partial publisher batch. The publisher
    /// checks it on every publish and poll; callers must continue invoking one
    /// of those methods for a queued tail to make progress.
    pub send_batch_max_delay: Duration,
    /// Policy used to decide whether a new publication should wait for more
    /// publications. Fixed preserves the original batching behavior.
    pub send_batch_mode: UdpSendBatchMode,
    /// Use Linux `UDP_SEGMENT` to submit equal-sized wire datagrams as one GSO
    /// packet. This is a publisher-only setting and remains disabled by
    /// default.
    pub use_udp_segment: bool,
    /// Copy encoded GSO datagrams into one contiguous buffer per segment-size
    /// group instead of using header/payload scatter-gather iovecs. This is a
    /// tuning fallback; scatter-gather remains the default.
    pub copy_udp_segment_payloads: bool,
    /// Use Linux `UDP_GRO` to receive coalesced wire datagrams and split them
    /// in userspace. This is a subscriber-only setting and remains disabled
    /// by default.
    pub use_udp_gro: bool,
    /// Send normal UDP publications once to an IPv4 multicast group. The
    /// control and repair plane remains per-subscriber TCP.
    pub multicast: Option<UdpMulticastConfig>,
    /// Optional `AF_XDP` publisher. The kernel UDP sender remains the default
    /// and can be retained as a setup-time fallback.
    pub xdp: Option<UdpXdpConfig>,
}

impl UdpConfig {
    pub fn default_for_addr(publisher_addr: SocketAddr) -> Self {
        Self {
            max_datagram_size: default_max_datagram_size_for(publisher_addr),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            sequence_window: 65_536,
            socket_buf_size: Some(64 * 1024 * 1024),
            progress_interval: Duration::from_millis(100),
            repair_delay: Duration::from_millis(1),
            max_inflight_repair_requests: 64,
            reconnect_interval: Duration::from_secs(1),
            send_batch_size: 1,
            send_batch_max_delay: Duration::ZERO,
            send_batch_mode: UdpSendBatchMode::Fixed,
            use_udp_segment: false,
            copy_udp_segment_payloads: false,
            use_udp_gro: false,
            multicast: None,
            xdp: None,
        }
    }

    fn validate(&self) -> io::Result<()> {
        if !self.sequence_window.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "UDP sequence window {} must be a nonzero power of two",
                    self.sequence_window
                ),
            ));
        }

        if self.progress_interval.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP progress interval must be nonzero",
            ));
        }

        if self.max_message_size == 0 || self.max_message_size >= u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "maximum UDP message size {} must be between 1 and {} bytes",
                    self.max_message_size,
                    u32::MAX as usize - 1,
                ),
            ));
        }

        if self.repair_delay.as_nanos() > u64::MAX as u128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP repair delay is too large",
            ));
        }

        if self.max_inflight_repair_requests == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "maximum inflight UDP repair requests must be nonzero",
            ));
        }

        if !(1..=64).contains(&self.send_batch_size) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("UDP send batch size {} must be between 1 and 64", self.send_batch_size),
            ));
        }

        if self.send_batch_size > 1 && self.send_batch_max_delay.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP send batch delay must be nonzero when batching publications",
            ));
        }

        if self.send_batch_size > self.sequence_window {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP send batch size must not exceed the sequence window",
            ));
        }

        if self.max_datagram_size <= UDP_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "maximum UDP datagram size {} is smaller than required minimum: {}",
                    self.max_datagram_size,
                    UDP_HEADER_SIZE + 1
                ),
            ));
        }

        if self.max_datagram_size > MAX_DATAGRAM_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "maximum UDP datagram size {} exceeds UDP payload maximum: {MAX_DATAGRAM_SIZE}",
                    self.max_datagram_size,
                ),
            ));
        }

        if let Some(multicast) = self.multicast {
            if !multicast.group.ip().is_multicast() || multicast.interface.is_multicast() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "UDP multicast requires a multicast group and unicast local interface",
                ));
            }
            if multicast.group.port() == 0 || multicast.interface.is_unspecified() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "UDP multicast requires a nonzero group port and local interface address",
                ));
            }
        }

        if let Some(xdp) = &self.xdp {
            self.validate_xdp(xdp)?;
        }

        Ok(())
    }

    fn validate_xdp(&self, xdp: &UdpXdpConfig) -> io::Result<()> {
        if self.multicast.is_some() && xdp.unicast_destinations.iter().any(Option::is_some) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AF_XDP multicast and unicast fanout destinations are mutually exclusive",
            ));
        }
        if self.multicast.is_none() {
            let Some(source_ip) = xdp.source_ip else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "AF_XDP unicast fanout requires a source IPv4 address",
                ));
            };
            if source_ip.is_unspecified() || source_ip.is_multicast() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "AF_XDP unicast source must be a unicast IPv4 address",
                ));
            }
            if xdp.unicast_destinations.iter().all(Option::is_none) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "AF_XDP requires multicast or at least one unicast destination",
                ));
            }
            for destination in xdp.unicast_destinations.iter().flatten() {
                if destination.address.ip().is_unspecified() ||
                    destination.address.ip().is_multicast() ||
                    destination.address.port() == 0
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "AF_XDP unicast destination must have a unicast IPv4 address and nonzero port",
                    ));
                }
                if destination.destination_mac == [0; 6] || destination.destination_mac[0] & 1 != 0
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "AF_XDP destination MAC must be a nonzero unicast address",
                    ));
                }
            }
        }
        if xdp.interface_index == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AF_XDP interface index must be nonzero",
            ));
        }
        if !xdp.ring_size.is_power_of_two() || xdp.ring_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AF_XDP ring size must be a nonzero power of two",
            ));
        }
        if xdp.frame_count < xdp.ring_size || xdp.frame_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AF_XDP frame count must be at least the ring size",
            ));
        }
        if !xdp.frame_size.is_power_of_two() || xdp.frame_size < 2048 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AF_XDP frame size must be a power of two of at least 2048 bytes",
            ));
        }
        if xdp.vlan_id.is_some_and(|vlan_id| vlan_id > 4094) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AF_XDP VLAN identifier must be between 0 and 4094",
            ));
        }
        if xdp.source_mac == [0; 6] || xdp.source_mac[0] & 1 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AF_XDP source MAC must be a nonzero unicast address",
            ));
        }
        let ethernet_header_size = if xdp.vlan_id.is_some() { 18 } else { 14 };
        let frame_size = ethernet_header_size + 20 + 8 + self.max_datagram_size;
        if frame_size > xdp.frame_size as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "AF_XDP frame size {} is too small for a {}-byte Ethernet frame",
                    xdp.frame_size, frame_size
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn xdp_config() -> UdpXdpConfig {
        let mut xdp = UdpXdpConfig::new(2, 7, [0x02, 0, 0, 0, 0, 1]);
        xdp.source_ip = Some(Ipv4Addr::new(10, 9, 0, 1));
        xdp.unicast_destinations[0] = Some(UdpXdpUnicastDestination::new(
            SocketAddrV4::new(Ipv4Addr::new(10, 9, 0, 2), 20_000),
            [0x02, 0, 0, 0, 0, 2],
        ));
        xdp
    }

    #[test]
    fn accepts_static_xdp_unicast_fanout() {
        let publisher = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 20_000));
        let mut config = UdpConfig::default_for_addr(publisher);
        config.xdp = Some(xdp_config());

        config.validate().unwrap();
    }

    #[test]
    fn rejects_xdp_multicast_and_unicast_fanout_together() {
        let publisher = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 20_000));
        let mut config = UdpConfig::default_for_addr(publisher);
        config.multicast = Some(UdpMulticastConfig::new(
            SocketAddrV4::new(Ipv4Addr::new(239, 255, 42, 42), 20_000),
            Ipv4Addr::new(10, 9, 0, 1),
        ));
        config.xdp = Some(xdp_config());

        let error = config.validate().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("mutually exclusive"));
    }
}
