mod framed;
mod raw;
mod send_backlog;

use std::net::SocketAddr;

use flux_communication::Timer;
use flux_timing::Nanos;
use flux_utils::{DCache, DCacheRef};
use framed::FramedStream;
use mio::{Registry, Token, event::Event};
use raw::RawStream;
use send_backlog::SendBacklog;

/// Response type for all external calls.
///
/// `Alive` means the connection is still usable.
/// `Disconnected` means the peer is gone and the connection must be rebuilt.
#[derive(Debug, PartialEq, Eq)]
pub enum ConnState {
    Alive,
    Disconnected,
}

pub enum MessagePayload<'a> {
    Raw(&'a [u8]),
    Cached(DCacheRef),
}

/// Controls emission of network latency and alloc telemetry.
///
/// Has no effect on framing or message delivery.
/// `send_ts` is always surfaced via `poll_with`.
#[derive(Clone, Copy)]
pub enum TcpTelemetry {
    Disabled,
    Enabled { app_name: &'static str },
}

/// Timers for TCP stream metrics.
#[derive(Clone, Copy, Debug)]
pub(super) struct TcpTimers {
    pub(super) latency: Timer,
    pub(super) alloc: Timer,
}

impl TcpTimers {
    pub(super) fn new(app_name: &'static str, label: &str) -> Self {
        Self {
            latency: Timer::new(app_name, format!("tcp_latency_{label}")),
            alloc: Timer::new(app_name, format!("tcp_alloc_{label}")),
        }
    }
}

pub(super) enum ReadOutcome<'a> {
    PayloadDone { payload: MessagePayload<'a>, send_ts: Nanos },
    WouldBlock,
    Disconnected,
}

enum TcpStreamInner {
    Framed(FramedStream),
    Raw(RawStream),
}

/// Single mio-backed TCP connection.
///
/// Two variants:
/// - `Framed`: length-prefixed framing with nanosecond send timestamp.
/// - `Raw`: no framing; delivers raw byte slices to the caller.
///
/// Outbound writes are non-blocking; any unwritten remainder is queued in a
/// backlog and flushed on the next writable event.
///
/// If `ConnState::Disconnected` is returned, the caller must treat the
/// connection as dead and rebuild it.
pub struct TcpStream(TcpStreamInner);

impl TcpStream {
    pub const SEND_BUF_SIZE: usize = FramedStream::SEND_BUF_SIZE;

    #[inline(never)]
    pub(crate) fn from_stream_with_telemetry(
        stream: mio::net::TcpStream,
        token: Token,
        peer_addr: SocketAddr,
        telemetry: TcpTelemetry,
        use_dcache: bool,
    ) -> Self {
        Self(TcpStreamInner::Framed(FramedStream::new(
            stream, token, peer_addr, telemetry, use_dcache,
        )))
    }

    #[inline(never)]
    pub fn raw(
        stream: mio::net::TcpStream,
        token: Token,
        peer_addr: SocketAddr,
        telemetry: TcpTelemetry,
    ) -> Self {
        Self(TcpStreamInner::Raw(RawStream::new(stream, token, peer_addr, telemetry)))
    }

    #[inline]
    pub fn reset_with_new_stream(
        &mut self,
        registry: &Registry,
        stream: mio::net::TcpStream,
        on_connect_msg: Option<&Vec<u8>>,
    ) -> ConnState {
        match &mut self.0 {
            TcpStreamInner::Framed(s) => s.reset_with_new_stream(registry, stream, on_connect_msg),
            TcpStreamInner::Raw(s) => s.reset_with_new_stream(registry, stream, on_connect_msg),
        }
    }

    #[inline]
    pub fn poll_with<F>(
        &mut self,
        registry: &Registry,
        ev: &Event,
        dcache: Option<&DCache>,
        on_msg: &mut F,
    ) -> ConnState
    where
        F: for<'a> FnMut(Token, MessagePayload<'a>, Nanos),
    {
        match &mut self.0 {
            TcpStreamInner::Framed(s) => s.poll_with(registry, ev, dcache, on_msg),
            TcpStreamInner::Raw(s) => s.poll_with(registry, ev, on_msg),
        }
    }

    #[inline]
    pub fn write_or_enqueue_with<F>(&mut self, registry: &Registry, serialise: F) -> ConnState
    where
        F: Fn(&mut Vec<u8>),
    {
        match &mut self.0 {
            TcpStreamInner::Framed(s) => s.write_or_enqueue_with(registry, serialise),
            TcpStreamInner::Raw(s) => s.write_or_enqueue_with(registry, serialise),
        }
    }

    #[inline]
    pub(crate) fn has_backlog(&self) -> bool {
        match &self.0 {
            TcpStreamInner::Framed(s) => s.has_backlog(),
            TcpStreamInner::Raw(s) => s.has_backlog(),
        }
    }

    #[inline]
    pub(crate) fn drain_backlog(&mut self, registry: &Registry) -> ConnState {
        match &mut self.0 {
            TcpStreamInner::Framed(s) => s.drain_backlog(registry),
            TcpStreamInner::Raw(s) => s.drain_backlog(registry),
        }
    }

    pub fn close(&mut self, registry: &Registry) {
        match &mut self.0 {
            TcpStreamInner::Framed(s) => s.close(registry),
            TcpStreamInner::Raw(s) => s.close(registry),
        }
    }

    pub(crate) fn peer(&self) -> SocketAddr {
        match &self.0 {
            TcpStreamInner::Framed(s) => s.peer(),
            TcpStreamInner::Raw(s) => s.peer(),
        }
    }
}

/// Set kernel SO_SNDBUF and SO_RCVBUF on a mio TcpStream.
pub(crate) fn set_socket_buf_size(stream: &mio::net::TcpStream, size: usize) {
    use std::os::fd::AsRawFd;
    let fd = stream.as_raw_fd();
    let size = size as libc::c_int;
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &size as *const _ as *const libc::c_void,
            core::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &size as *const _ as *const libc::c_void,
            core::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
