mod connector;
mod network;
mod stream;

pub(crate) use connector::TcpManager;
pub use network::{
    Framing, PayloadBuf, TcpEvent, TcpGroup, TcpGroupConfig, TcpNetwork, TcpNetworkCore,
    TcpNetworkWithExternalPoll,
};
pub use stream::{ConnState, TcpStream, TcpTelemetry};
pub(crate) use stream::{
    DEFAULT_TCP_USER_TIMEOUT_MS, FRAME_HEADER_SIZE, set_keepalive, set_socket_buf_size,
    set_user_timeout, write_frame_header,
};
