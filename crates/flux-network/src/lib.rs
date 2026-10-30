//! TCP and reliable, unordered UDP connections sharing one poll, and UDP
//! pub/sub.
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
pub mod udp_v2;
pub mod websocket;

pub use mio::Token;
pub use network::{
    Event, Framing, Group, GroupConfig, Network, NetworkCore, NetworkEvent, NetworkTelemetry,
    NetworkWithExternalPoll, PayloadBuf, ReplayPolicy, TcpGroupConfig, UdpGroupConfig,
    UdpGroupConfigV2,
};
pub use udp::UdpConfig;
pub use udp_v2::{Publisher, PublisherEvent, Subscriber, SubscriberEvent};
