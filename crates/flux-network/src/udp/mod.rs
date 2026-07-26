use std::{io, net::SocketAddr, time::Duration};

mod control;
mod publisher;
mod subscriber;
mod wire;

pub use publisher::{PublisherEvent, UdpPublisher};
pub use subscriber::{SubscriberEvent, UdpSubscriber};
pub use wire::{DEFAULT_IPV4_MAX_DATAGRAM_SIZE, DEFAULT_IPV6_MAX_DATAGRAM_SIZE};

use wire::{MAX_DATAGRAM_SIZE, UDP_HEADER_SIZE, default_max_datagram_size_for};

const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

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

        Ok(())
    }
}
