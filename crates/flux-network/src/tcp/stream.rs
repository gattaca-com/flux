use std::{
    collections::VecDeque,
    io::{self, IoSlice, Read, Write},
    net::SocketAddr,
};

use flux::spine::{SpineProducerWithDCache, SpineProducers};
use flux_communication::Timer;
use flux_timing::{Instant, Nanos};
use flux_utils::{DCache, DCacheRef};

pub const DEFAULT_TCP_USER_TIMEOUT_MS: u32 = 10_000;
enum RxBuf {
    Heap(Vec<u8>),
    DCache,
}
use mio::{Interest, Registry, Token, event::Event};
use tracing::{debug, warn};

enum MessagePayload<'a> {
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
struct TcpTimers {
    latency: Timer,
    alloc: Timer,
}

impl TcpTimers {
    fn new(app_name: &'static str, label: &str) -> Self {
        Self {
            latency: Timer::new(app_name, format!("tcp_latency_{label}")),
            alloc: Timer::new(app_name, format!("tcp_alloc_{label}")),
        }
    }
}

/// Frame length prefix.
const LEN_HEADER_SIZE: usize = core::mem::size_of::<u32>();
/// Nanos timestamp when the sender finished serialising and handed bytes to
/// kernel or enqueued in backlog.
const TS_HEADER_SIZE: usize = core::mem::size_of::<Nanos>();
const FRAME_HEADER_SIZE: usize = LEN_HEADER_SIZE + TS_HEADER_SIZE;
// TODO: might need to tweak these
const RX_BUF_SIZE: usize = 32 * 1024;

/// Response type for all external calls.
///
/// `Alive` means the connection is still usable.
/// `Disconnected` means the peer is gone and the connection must be rebuilt.
#[derive(Debug, PartialEq, Eq)]
pub enum ConnState {
    Alive,
    Disconnected,
}

enum ReadOutcome<'a> {
    PayloadDone { payload: MessagePayload<'a>, send_ts: Nanos },
    WouldBlock,
    Disconnected,
}

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy)]
enum RxState {
    ReadingHeader { buf: [u8; FRAME_HEADER_SIZE], have: usize },
    ReadingPayload { msg_len: usize, offset: usize, send_ts: Nanos, dc_offset: Option<usize> },
}
impl Default for RxState {
    fn default() -> Self {
        Self::ReadingHeader { buf: [0; FRAME_HEADER_SIZE], have: 0 }
    }
}

/// Single mio-backed TCP connection.
///
/// Frames are length-prefixed and contain the send ts:
///   - 4-byte LE length header
///   - 8-byte LE nanosecond ts
///   - payload bytes
///
/// Outbound:
///   - `write_or_enqueue(msg)` serialises `msg` into an internal staging
///     buffer.
///   - Attempts to write bytes (non-blocking) to socket.
///   - Any unwritten remainder is queued (this path allocates).
///   - Backlogged frames are flushed whenever the socket becomes writable.
///
/// Inbound:
///   - Reads the 4-byte length prefix, then reads exactly that many bytes.
///   - When a full frame is assembled, `poll_with` invokes the caller callback
///     with the deserialised T.
///   - Continues reading frames until `WouldBlock` (no more messages are
///     ready).
///
/// Recconect handling:
///   - If `ConnState::Disconnected` is returned caller must treat the
///     connection as dead and rebuild the state
pub struct TcpStream {
    stream: mio::net::TcpStream,
    peer_addr: SocketAddr,
    token: Token,

    rx_state: RxState,
    rx_buf: RxBuf,
    header_buf: [u8; FRAME_HEADER_SIZE],
    send_buf: Vec<u8>,
    /// Filled when send would block.
    /// First entry will either be a full message or the current partially
    /// written head.
    send_backlog: VecDeque<Vec<u8>>,
    /// We don't pop until the full message is written,
    /// so we use this cursor to know what slice to write
    send_cursor: usize,

    /// True if WRITABLE interest is currently registered in `poll`.
    /// Invariant: `writable_armed == !send_q.is_empty()`
    writable_armed: bool,

    /// When the send backlog first exceeded the configured `max_backlog`
    /// threshold.  Reset to `None` when the backlog drops back below the
    /// limit or on reconnect.
    pub(crate) backlog_exceeded_since: Option<Instant>,

    /// Timers for network latency and alloc telemetry.
    timers: Option<TcpTimers>,
}

impl TcpStream {
    pub const SEND_BUF_SIZE: usize = 32 * 1024;

    #[inline(never)]
    pub(crate) fn from_stream_with_telemetry(
        stream: mio::net::TcpStream,
        token: Token,
        peer_addr: SocketAddr,
        telemetry: TcpTelemetry,
        use_dcache: bool,
    ) -> Self {
        let timers = match telemetry {
            TcpTelemetry::Disabled => None,
            TcpTelemetry::Enabled { app_name } => {
                let local_port = stream.local_addr().map(|a| a.port()).unwrap_or(0);
                let peer = peer_addr.to_string();
                let steam_label = format!("{local_port}-{peer}");
                Some(TcpTimers::new(app_name, &steam_label))
            }
        };
        let rx_buf = if use_dcache { RxBuf::DCache } else { RxBuf::Heap(vec![0; RX_BUF_SIZE]) };

        Self {
            stream,
            peer_addr,
            token,
            rx_state: Default::default(),
            rx_buf,
            header_buf: [0; FRAME_HEADER_SIZE],
            send_buf: vec![0; Self::SEND_BUF_SIZE],
            send_backlog: VecDeque::with_capacity(64),
            send_cursor: 0,
            writable_armed: false,
            backlog_exceeded_since: None,
            timers,
        }
    }

    #[inline]
    pub fn reset_with_new_stream(
        &mut self,
        registry: &Registry,
        stream: mio::net::TcpStream,
        on_connect_msg: Option<&Vec<u8>>,
    ) -> ConnState {
        self.rx_state = Default::default();
        self.send_buf.clear();
        self.send_cursor = 0;
        self.header_buf.fill(0);
        self.backlog_exceeded_since = None;
        self.stream = stream;
        if !self.send_backlog.is_empty() {
            self.writable_armed = false;
            if let Some(message) = on_connect_msg {
                self.serialise_frame(|bytes| bytes.extend_from_slice(message));
                let data = self.alloc_vec(0);
                return self.enqueue_front(registry, data);
            }
            self.arm_writable(registry)
        } else if let Some(message) = on_connect_msg {
            self.write_or_enqueue_with(registry, |bytes| bytes.extend_from_slice(message))
        } else {
            ConnState::Alive
        }
    }

    /// Poll socket and calls `on_msg` for every fully assembled frame.
    ///
    /// The byte slice passed to `on_msg` is only valid for the duration of the
    /// callback. Use with non-dcache connectors; for dcache use
    /// [`poll_with_produce`].
    #[inline]
    pub fn poll_with<F>(
        &mut self,
        registry: &Registry,
        ev: &Event,
        dcache: Option<&DCache>,
        on_msg: &mut F,
    ) -> ConnState
    where
        F: for<'a> FnMut(Token, &'a [u8], Nanos),
    {
        if ev.is_readable() {
            loop {
                match self.read_frame(dcache) {
                    ReadOutcome::PayloadDone { payload: MessagePayload::Raw(bytes), send_ts } => {
                        on_msg(ev.token(), bytes, send_ts);
                    }
                    ReadOutcome::PayloadDone { payload: MessagePayload::Cached(_), .. } => {
                        flux_utils::safe_panic!(
                            "poll_with called on dcache stream; use poll_with_produce"
                        );
                    }
                    ReadOutcome::WouldBlock => break,
                    ReadOutcome::Disconnected => return ConnState::Disconnected,
                }
            }
        }

        if ev.is_writable() && self.drain_backlog(registry) == ConnState::Disconnected {
            return ConnState::Disconnected;
        }

        ConnState::Alive
    }

    /// Like [`poll_with`] but for dcache-backed streams. Calls `on_msg` with
    /// each `PollEvent<&[u8]>`; for `Message` events, returning `Some(T)`
    /// produces into the spine. Use with dcache connectors; for raw use
    /// [`poll_with`].
    #[inline]
    pub fn poll_with_produce<T, P, F>(
        &mut self,
        registry: &Registry,
        ev: &Event,
        dcache: &DCache,
        produce: &mut P,
        on_msg: &mut F,
    ) -> ConnState
    where
        T: 'static + Copy,
        P: SpineProducers + AsRef<SpineProducerWithDCache<T>>,
        F: for<'a> FnMut(Token, &'a [u8], Nanos) -> Option<T>,
    {
        if ev.is_readable() {
            loop {
                match self.read_frame(Some(dcache)) {
                    ReadOutcome::PayloadDone { payload: MessagePayload::Raw(_), .. } => {
                        flux_utils::safe_panic!(
                            "poll_with_produce called on non-dcache stream; use poll_with"
                        );
                    }
                    ReadOutcome::PayloadDone { payload: MessagePayload::Cached(dref), send_ts } => {
                        match dcache.map(dref, |bytes| on_msg(self.token, bytes, send_ts)) {
                            Ok(Some(t)) => produce.produce_with_dref(t, dref, send_ts),
                            Ok(None) => {}
                            Err(e) => warn!("dcache map failed: {e}"),
                        }
                    }
                    ReadOutcome::WouldBlock => break,
                    ReadOutcome::Disconnected => return ConnState::Disconnected,
                }
            }
        }

        if ev.is_writable() && self.drain_backlog(registry) == ConnState::Disconnected {
            return ConnState::Disconnected;
        }

        ConnState::Alive
    }

    /// Happy path: serialises into `self.send_buf`, writes frame to stream.
    /// If write would block or we have already blocked on a previous write,
    /// allocates a new vec and stores frame in the backlog to be flushed at
    /// the next writable event.
    ///
    /// TODO: avoid allocation by queueing offsets into `self.send_buf` instead
    /// of Vec<u8>.
    #[inline]
    pub fn write_or_enqueue_with<F>(&mut self, registry: &Registry, serialise: F) -> ConnState
    where
        F: Fn(&mut Vec<u8>),
    {
        self.serialise_frame(serialise);
        if self.send_buf.is_empty() {
            return ConnState::Alive;
        }

        if !self.send_backlog.is_empty() {
            if self.drain_backlog(registry) == ConnState::Disconnected {
                return ConnState::Disconnected;
            }
            if !self.send_backlog.is_empty() {
                let data = self.alloc_vec(0);
                return self.enqueue_back(registry, data);
            }
            // backlog drained, fall through to direct write
        }

        match self.stream.write_vectored(&[
            IoSlice::new(self.header_buf.as_slice()),
            IoSlice::new(&self.send_buf),
        ]) {
            Ok(0) => {
                warn!("tcp: stream failed to write, disconnecting");
                ConnState::Disconnected
            }
            Ok(n) if n == self.send_buf.len() + FRAME_HEADER_SIZE => ConnState::Alive,
            Ok(n) => {
                let data = self.alloc_vec(0);
                self.send_cursor = n;
                self.enqueue_back(registry, data)
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let data = self.alloc_vec(0);
                self.enqueue_back(registry, data)
            }
            Err(err) => {
                warn!(?err, "tcp: stream write fail");
                ConnState::Disconnected
            }
        }
    }

    /// Allocate send_buf[start..end] to vec. Times the alloc if telemetry
    /// enabled.
    #[inline]
    fn alloc_vec(&mut self, written: usize) -> Vec<u8> {
        match &mut self.timers {
            Some(timers) => {
                let t0 = Nanos::now();
                let mut v = Vec::with_capacity(
                    (FRAME_HEADER_SIZE + self.send_buf.len()).saturating_sub(written),
                );
                v.extend_from_slice(&self.header_buf[written.min(FRAME_HEADER_SIZE)..]);
                v.extend_from_slice(&self.send_buf[written.saturating_sub(FRAME_HEADER_SIZE)..]);
                timers.alloc.emit_latency_from_nanos(t0, Nanos::now());
                v
            }
            None => {
                let mut v = Vec::with_capacity(
                    (FRAME_HEADER_SIZE + self.send_buf.len()).saturating_sub(written),
                );
                v.extend_from_slice(&self.header_buf[written.min(FRAME_HEADER_SIZE)..]);
                v.extend_from_slice(&self.send_buf[written.saturating_sub(FRAME_HEADER_SIZE)..]);
                v
            }
        }
    }

    #[inline]
    pub(crate) fn has_backlog(&self) -> bool {
        !self.send_backlog.is_empty()
    }

    /// Number of framed messages waiting in the send backlog.
    #[inline]
    pub(crate) fn backlog_len(&self) -> usize {
        self.send_backlog.len()
    }

    /// Flush queued data until kernel blocks, queue empty or we've written the
    /// max bytes per iter.
    /// returns connstate and whether it should be deregistered from writable
    #[inline]
    pub(crate) fn drain_backlog(&mut self, registry: &Registry) -> ConnState {
        while let Some(front) = self.send_backlog.front() {
            match self.stream.write(&front[self.send_cursor..]) {
                Ok(0) => return ConnState::Disconnected,

                Ok(n) => {
                    self.send_cursor += n;
                    if self.send_cursor == front.len() {
                        self.send_backlog.pop_front();
                        self.send_cursor = 0
                    }
                }

                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,

                Err(err) => {
                    debug!(?err, "tcp: write from backlog");
                    return ConnState::Disconnected;
                }
            }
        }

        // Drop WRITABLE interest only when fully drained
        if self.send_backlog.is_empty() && self.writable_armed {
            if let Err(err) = registry.reregister(&mut self.stream, self.token, Interest::READABLE)
            {
                debug!(?err, "tcp: reregister drop writable");
                return ConnState::Disconnected;
            }
            self.writable_armed = false;
        }

        ConnState::Alive
    }

    /// Read a single complete frame if present.
    /// Loops until a frame is received or we've read everything and the read
    /// would block.
    #[inline]
    fn read_frame(&mut self, dcache: Option<&DCache>) -> ReadOutcome<'_> {
        loop {
            match self.rx_state {
                RxState::ReadingHeader { mut buf, mut have } => {
                    while have < FRAME_HEADER_SIZE {
                        match self.stream.read(&mut buf[have..]) {
                            Ok(0) => {
                                debug!(
                                    peer = %self.peer_addr,
                                    have,
                                    "tcp: connection closed by peer (reading header)",
                                );
                                return ReadOutcome::Disconnected;
                            }

                            Ok(n) => {
                                have += n;
                                if have == FRAME_HEADER_SIZE {
                                    let msg_len = u32::from_le_bytes(
                                        buf[..LEN_HEADER_SIZE].try_into().unwrap(),
                                    ) as usize;
                                    let send_ts = Nanos(u64::from_le_bytes(
                                        buf[LEN_HEADER_SIZE..FRAME_HEADER_SIZE].try_into().unwrap(),
                                    ));
                                    if msg_len == 0 {
                                        self.rx_state = RxState::ReadingHeader {
                                            buf: [0; FRAME_HEADER_SIZE],
                                            have: 0,
                                        };
                                        continue;
                                    }
                                    let dc_offset = match &mut self.rx_buf {
                                        RxBuf::DCache => {
                                            let writer =
                                                dcache.expect("dcache stream but no writer passed");
                                            match writer.reserve(msg_len) {
                                                Ok(r) => Some(r.offset),
                                                Err(e) => {
                                                    warn!("dcache reserve failed: {e}");
                                                    return ReadOutcome::Disconnected;
                                                }
                                            }
                                        }
                                        RxBuf::Heap(buf) => {
                                            if msg_len > buf.len() {
                                                debug!(
                                                    buf_len = buf.len(),
                                                    need_len = msg_len,
                                                    "tcp: buffer resized"
                                                );
                                                buf.resize(msg_len, 0);
                                            }
                                            None
                                        }
                                    };
                                    self.rx_state = RxState::ReadingPayload {
                                        msg_len,
                                        offset: 0,
                                        send_ts,
                                        dc_offset,
                                    };
                                }
                            }

                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                self.rx_state = RxState::ReadingHeader { buf, have };
                                return ReadOutcome::WouldBlock;
                            }

                            Err(err) => {
                                debug!(?err, "tcp: read header");
                                return ReadOutcome::Disconnected;
                            }
                        }
                    }
                }

                RxState::ReadingPayload { msg_len, mut offset, send_ts, dc_offset } => {
                    while offset < msg_len {
                        let result = if let Some(dc_offset) = dc_offset {
                            let dref = DCacheRef { offset: dc_offset, len: msg_len };
                            let writer = dcache.expect("dcache stream but no writer passed");
                            match writer.write_into(dref, offset, |buf| self.stream.read(buf)) {
                                Ok(r) => r,
                                Err(e) => {
                                    warn!("dcache write_into error: {e}");
                                    return ReadOutcome::Disconnected;
                                }
                            }
                        } else {
                            let RxBuf::Heap(buf) = &mut self.rx_buf else { unreachable!() };
                            self.stream.read(&mut buf[offset..msg_len])
                        };
                        match result {
                            Ok(0) => {
                                debug!(
                                    peer = %self.peer_addr,
                                    msg_len,
                                    offset,
                                    "tcp: connection closed by peer (reading payload)",
                                );
                                return ReadOutcome::Disconnected;
                            }
                            Ok(n) => {
                                offset += n;
                                if offset == msg_len {
                                    if let Some(timers) = &mut self.timers {
                                        timers
                                            .latency
                                            .emit_latency_from_nanos(send_ts, Nanos::now());
                                    }
                                    self.rx_state = RxState::ReadingHeader {
                                        buf: [0; FRAME_HEADER_SIZE],
                                        have: 0,
                                    };
                                    let payload = if let Some(dc_offset) = dc_offset {
                                        MessagePayload::Cached(DCacheRef {
                                            offset: dc_offset,
                                            len: msg_len,
                                        })
                                    } else {
                                        let RxBuf::Heap(buf) = &self.rx_buf else { unreachable!() };
                                        MessagePayload::Raw(&buf[..msg_len])
                                    };
                                    return ReadOutcome::PayloadDone { payload, send_ts };
                                }
                            }
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                self.rx_state =
                                    RxState::ReadingPayload { msg_len, offset, send_ts, dc_offset };
                                return ReadOutcome::WouldBlock;
                            }
                            Err(err) => {
                                debug!(?err, "tcp: read payload");
                                return ReadOutcome::Disconnected;
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline]
    fn enqueue_front(&mut self, registry: &Registry, data: Vec<u8>) -> ConnState {
        self.send_backlog.push_front(data);
        self.arm_writable(registry)
    }

    #[inline]
    fn enqueue_back(&mut self, registry: &Registry, data: Vec<u8>) -> ConnState {
        self.send_backlog.push_back(data);
        self.arm_writable(registry)
    }

    /// Arm WRITABLE notifications when transitioning from empty -> non-empty
    /// queue. `self.poll` will start polling for writable events.
    #[inline]
    fn arm_writable(&mut self, registry: &Registry) -> ConnState {
        if !self.writable_armed {
            if let Err(err) = registry.reregister(
                &mut self.stream,
                self.token,
                Interest::READABLE | Interest::WRITABLE,
            ) {
                debug!(?err, "tcp: poll reregister");
                return ConnState::Disconnected;
            }
            self.writable_armed = true;
        }
        ConnState::Alive
    }

    /// Serialise payload into send buffer and prepend frame header.
    #[inline(always)]
    fn serialise_frame<F>(&mut self, serialise: F)
    where
        F: Fn(&mut Vec<u8>),
    {
        self.send_buf.clear();
        serialise(&mut self.send_buf);
        // write frame header
        self.header_buf[..LEN_HEADER_SIZE]
            .copy_from_slice(&(self.send_buf.len() as u32).to_le_bytes());
        self.header_buf[LEN_HEADER_SIZE..FRAME_HEADER_SIZE]
            .copy_from_slice(&Nanos::now().0.to_le_bytes());
    }

    pub fn close(&mut self, registry: &Registry) {
        debug!("terminating connection");
        let _ = registry.deregister(&mut self.stream);
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }

    pub(crate) fn peer(&self) -> SocketAddr {
        self.peer_addr
    }
}

/// Set TCP_USER_TIMEOUT on a mio TcpStream.
/// After this duration of unacknowledged data the kernel closes the connection,
/// overriding the system-wide tcp_retries2 (~15 min default) for this socket.
pub(crate) fn set_user_timeout(stream: &mio::net::TcpStream, timeout_ms: u32) {
    use std::os::fd::AsRawFd;
    let fd = stream.as_raw_fd();
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_USER_TIMEOUT,
            &timeout_ms as *const _ as *const libc::c_void,
            core::mem::size_of::<u32>() as libc::socklen_t,
        );
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
