mod connector;
mod stream;

pub use connector::{PollEvent, SendBehavior, TcpConnector};
pub use stream::{ConnState, TcpStream, TcpTelemetry};

const STREAM: mio::Token = mio::Token(0);
