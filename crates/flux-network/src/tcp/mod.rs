mod connector;
mod stream;

pub use connector::{SendBehavior, TcpConnector};
pub use stream::{ConnState, TcpStream, TcpTelemetry};

const STREAM: mio::Token = mio::Token(0);

#[cfg(feature = "wincode")]
#[inline]
pub fn wincode_ser_into_slice<T>(buf: &mut [u8], value: &T) -> usize
where
    T: wincode::SchemaWrite<Src = T>,
{
    let len = buf.len();
    let mut cursor = buf;
    wincode::serialize_into(&mut cursor, value).unwrap();
    len - cursor.len()
}
