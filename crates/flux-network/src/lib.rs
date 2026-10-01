//! TCP and reliable, unordered UDP groups sharing one poll.
//!
//! Use [`Network`] for an owned poll or [`NetworkWithExternalPoll`] for an
//! existing one.

pub mod grpc;
pub mod http;
pub mod http2;
pub mod network;
mod tcp;
pub mod tls;
pub mod udp;

pub use mio::Token;
pub use network::{
    Event, Framing, Group, GroupConfig, Network, NetworkCore, NetworkEvent, NetworkTelemetry,
    NetworkWithExternalPoll, PayloadBuf, ReplayPolicy, TcpGroupConfig, UdpGroupConfig,
};
pub use udp::UdpConfig;
