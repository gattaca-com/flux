use std::{
    io::{self, IoSlice, Read, Write},
    net::SocketAddr,
};

use flux_timing::Nanos;
use flux_utils::{DCache, DCacheRef};
use mio::{Registry, Token, event::Event};
use tracing::{debug, warn};

use super::{ConnState, MessagePayload, ReadOutcome, SendBacklog, TcpTelemetry, TcpTimers};

/// Frame length prefix.
const LEN_HEADER_SIZE: usize = core::mem::size_of::<u32>();
/// Nanos timestamp when the sender finished serialising and handed bytes to
/// kernel or enqueued in backlog.
const TS_HEADER_SIZE: usize = core::mem::size_of::<Nanos>();
const FRAME_HEADER_SIZE: usize = LEN_HEADER_SIZE + TS_HEADER_SIZE;
// TODO: might need to tweak these
const RX_BUF_SIZE: usize = 32 * 1024;

pub(super) enum RxBuf {
    Heap(Vec<u8>),
    DCache,
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

/// Single mio-backed TCP connection with length-prefixed framing.
///
/// Frame layout:
///   - 4-byte LE length
///   - 8-byte LE nanosecond send timestamp
///   - payload bytes
///
/// Outbound:
///   - `write_or_enqueue_with(msg)` serialises `msg` into an internal staging
///     buffer.
///   - Attempts to write bytes (non-blocking) to socket.
///   - Any unwritten remainder is queued (this path allocates).
///   - Backlogged frames are flushed whenever the socket becomes writable.
///
/// Inbound:
///   - Reads the 4-byte length prefix, then reads exactly that many bytes.
///   - When a full frame is assembled, `poll_with` invokes the caller callback
///     with the payload.
///   - Continues reading frames until `WouldBlock` (no more messages are
///     ready).
///
/// Reconnect handling:
///   - If `ConnState::Disconnected` is returned caller must treat the
///     connection as dead and rebuild the state.
pub(super) struct FramedStream {
    stream: mio::net::TcpStream,
    peer_addr: SocketAddr,
    rx_state: RxState,
    rx_buf: RxBuf,
    header_buf: [u8; FRAME_HEADER_SIZE],
    send_buf: Vec<u8>,
    backlog: SendBacklog,
    timers: Option<TcpTimers>,
}

impl FramedStream {
    pub(super) const SEND_BUF_SIZE: usize = 32 * 1024;

    #[inline(never)]
    pub(super) fn new(
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
                Some(TcpTimers::new(app_name, &format!("{local_port}-{peer}")))
            }
        };
        let rx_buf = if use_dcache { RxBuf::DCache } else { RxBuf::Heap(vec![0; RX_BUF_SIZE]) };
        Self {
            backlog: SendBacklog::new(token),
            stream,
            peer_addr,
            rx_state: Default::default(),
            rx_buf,
            header_buf: [0; FRAME_HEADER_SIZE],
            send_buf: vec![0; Self::SEND_BUF_SIZE],
            timers,
        }
    }

    #[inline]
    pub(super) fn reset_with_new_stream(
        &mut self,
        registry: &Registry,
        stream: mio::net::TcpStream,
        on_connect_msg: Option<&Vec<u8>>,
    ) -> ConnState {
        self.rx_state = Default::default();
        self.send_buf.clear();
        self.header_buf.fill(0);
        self.stream = stream;
        if !self.backlog.is_empty() {
            self.backlog.disarm();
            if let Some(message) = on_connect_msg {
                self.serialise_frame(|bytes| bytes.extend_from_slice(message));
                let data = self.alloc_vec(0);
                return self.backlog.enqueue_front(registry, &mut self.stream, data);
            }
            self.backlog.arm_writable(registry, &mut self.stream)
        } else if let Some(message) = on_connect_msg {
            self.write_or_enqueue_with(registry, |bytes| bytes.extend_from_slice(message))
        } else {
            ConnState::Alive
        }
    }

    /// Poll socket and calls `on_msg` for every fully assembled frame.
    ///
    /// When no DCache is set, `payload` is [`MessagePayload::Raw`] and the
    /// slice is only valid for the duration of the callback. When DCache is
    /// set, `payload` is [`MessagePayload::Cached`] and the ref may be kept.
    #[inline]
    pub(super) fn poll_with<F>(
        &mut self,
        registry: &Registry,
        ev: &Event,
        dcache: Option<&DCache>,
        on_msg: &mut F,
    ) -> ConnState
    where
        F: for<'a> FnMut(Token, MessagePayload<'a>, Nanos),
    {
        if ev.is_readable() {
            loop {
                match self.read_frame(dcache) {
                    ReadOutcome::PayloadDone { payload, send_ts } => {
                        on_msg(ev.token(), payload, send_ts);
                    }
                    ReadOutcome::WouldBlock => break,
                    ReadOutcome::Disconnected => return ConnState::Disconnected,
                }
            }
        }

        if ev.is_writable() &&
            self.backlog.drain(registry, &mut self.stream) == ConnState::Disconnected
        {
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
    pub(super) fn write_or_enqueue_with<F>(
        &mut self,
        registry: &Registry,
        serialise: F,
    ) -> ConnState
    where
        F: Fn(&mut Vec<u8>),
    {
        self.serialise_frame(serialise);
        if self.send_buf.is_empty() {
            return ConnState::Alive;
        }

        if !self.backlog.is_empty() {
            if self.backlog.drain(registry, &mut self.stream) == ConnState::Disconnected {
                return ConnState::Disconnected;
            }
            if !self.backlog.is_empty() {
                let data = self.alloc_vec(0);
                return self.backlog.enqueue_back(registry, &mut self.stream, data);
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
                self.backlog.cursor = n;
                self.backlog.enqueue_back(registry, &mut self.stream, data)
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let data = self.alloc_vec(0);
                self.backlog.enqueue_back(registry, &mut self.stream, data)
            }
            Err(err) => {
                warn!(?err, "tcp: stream write fail");
                ConnState::Disconnected
            }
        }
    }

    #[inline]
    pub(super) fn has_backlog(&self) -> bool {
        !self.backlog.is_empty()
    }

    #[inline]
    pub(super) fn drain_backlog(&mut self, registry: &Registry) -> ConnState {
        self.backlog.drain(registry, &mut self.stream)
    }

    pub(super) fn close(&mut self, registry: &Registry) {
        debug!("terminating connection");
        let _ = registry.deregister(&mut self.stream);
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }

    pub(super) fn peer(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Allocate the unwritten portion of header_buf + send_buf into a Vec.
    #[inline]
    fn alloc_vec(&mut self, written: usize) -> Vec<u8> {
        let total = FRAME_HEADER_SIZE + self.send_buf.len();
        let t0 = self.timers.as_ref().map(|_| Nanos::now());
        let mut v = Vec::with_capacity(total.saturating_sub(written));
        v.extend_from_slice(&self.header_buf[written.min(FRAME_HEADER_SIZE)..]);
        v.extend_from_slice(&self.send_buf[written.saturating_sub(FRAME_HEADER_SIZE)..]);
        if let (Some(timers), Some(t0)) = (&mut self.timers, t0) {
            timers.alloc.emit_latency_from_nanos(t0, Nanos::now());
        }
        v
    }

    #[inline(always)]
    fn serialise_frame<F>(&mut self, serialise: F)
    where
        F: Fn(&mut Vec<u8>),
    {
        self.send_buf.clear();
        serialise(&mut self.send_buf);
        self.header_buf[..LEN_HEADER_SIZE]
            .copy_from_slice(&(self.send_buf.len() as u32).to_le_bytes());
        self.header_buf[LEN_HEADER_SIZE..FRAME_HEADER_SIZE]
            .copy_from_slice(&Nanos::now().0.to_le_bytes());
    }

    #[inline]
    fn read_frame(&mut self, dcache: Option<&DCache>) -> ReadOutcome<'_> {
        loop {
            match self.rx_state {
                RxState::ReadingHeader { mut buf, mut have } => {
                    while have < FRAME_HEADER_SIZE {
                        match self.stream.read(&mut buf[have..]) {
                            Ok(0) => return ReadOutcome::Disconnected,
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
                            Ok(0) => return ReadOutcome::Disconnected,
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
}
