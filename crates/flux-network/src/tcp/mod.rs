mod client;
mod connector;
mod server;
mod stream;

pub(crate) const HANDSHAKE_ACCEPTED: &[u8] = b"\0flux-handshake-v1\0accepted";
pub(crate) const HANDSHAKE_REJECTED: &[u8] = b"\0flux-handshake-v1\0rejected";

pub use client::{ClientEvent, TcpClient};
pub use connector::{PollEvent, SendBehavior, TcpConnector};
pub use server::{ServerEvent, TcpServer};
pub use stream::{ConnState, TcpStream, TcpTelemetry};
