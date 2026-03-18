use std::{
    collections::VecDeque,
    io::{self, IoSlice, Write},
    net::SocketAddr,
    os::fd::AsRawFd,
    sync::Arc,
};

use flux_communication::Timer;
use flux_timing::Nanos;
use flux_utils::{DCache, DCacheRef};
use io_uring::{opcode, types};
use tracing::{debug, warn};

use crate::Token;

pub enum MessagePayload<'a> {
    Raw(&'a [u8]),
    Cached(DCacheRef),
}

/// Controls emission of network latency and alloc telemetry.
#[derive(Clone, Copy)]
pub enum TcpTelemetry {
    Disabled,
    Enabled { app_name: &'static str },
}

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

/// Frame layout: 4-byte LE length | 8-byte LE send_ts | payload bytes.
const LEN_HEADER_SIZE: usize = core::mem::size_of::<u32>();
const TS_HEADER_SIZE: usize = core::mem::size_of::<Nanos>();
pub(crate) const FRAME_HEADER_SIZE: usize = LEN_HEADER_SIZE + TS_HEADER_SIZE;

/// Phase tags packed into the upper 32 bits of SQE user_data.
pub(crate) const PHASE_ACCEPT: u32 = 0;
pub(crate) const PHASE_HEADER: u32 = 1;
pub(crate) const PHASE_PAYLOAD: u32 = 2;

pub(crate) fn encode_user_data(token: Token, phase: u32) -> u64 {
    (token.0 as u64) | ((phase as u64) << 32)
}

pub(crate) fn decode_user_data(ud: u64) -> (Token, u32) {
    (Token(ud as u32 as usize), (ud >> 32) as u32)
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConnState {
    Alive,
    Disconnected,
}

enum RxBuf {
    Heap(Vec<u8>),
    DCache(Arc<DCache>),
}

/// Per-connection receive state for the io_uring-driven read path.
pub(crate) enum IoState {
    /// No SQE in flight; connector will post a header RECV next iteration.
    Idle,
    /// RECV SQE in flight targeting `header_buf[have..]`.
    HeaderPending { have: usize },
    /// READ_FIXED (or RECV for the heap path) in flight into `dc_ref` at
    /// `offset` bytes into the payload.
    PayloadPending { msg_len: usize, offset: usize, send_ts: Nanos, dc_ref: Option<DCacheRef> },
}

/// Single TCP connection managed by the io_uring connector.
///
/// All receive I/O is driven externally via `post_header_sqe`, `post_payload_sqe`,
/// `handle_header_cqe`, and `handle_payload_cqe`. Send I/O uses direct
/// non-blocking writes; responses are small so no fixed-buffer path is needed
/// on the send side.
pub struct TcpStream {
    pub(crate) stream: std::net::TcpStream,
    peer_addr: SocketAddr,
    pub(crate) token: Token,

    pub(crate) io_state: IoState,
    rx_buf: RxBuf,
    /// Accumulates the 12-byte frame header across partial recvs.
    pub(crate) header_buf: [u8; FRAME_HEADER_SIZE],

    send_buf: Vec<u8>,
    send_backlog: VecDeque<Vec<u8>>,
    send_cursor: usize,

    timers: Option<TcpTimers>,
}

impl TcpStream {
    pub const SEND_BUF_SIZE: usize = 32 * 1024;

    #[inline(never)]
    pub(crate) fn new(
        stream: std::net::TcpStream,
        token: Token,
        peer_addr: SocketAddr,
        telemetry: TcpTelemetry,
        dcache: Option<Arc<DCache>>,
    ) -> Self {
        let timers = match telemetry {
            TcpTelemetry::Disabled => None,
            TcpTelemetry::Enabled { app_name } => {
                let local_port = stream.local_addr().map(|a| a.port()).unwrap_or(0);
                let peer = peer_addr.to_string();
                Some(TcpTimers::new(app_name, &format!("{local_port}-{peer}")))
            }
        };
        let rx_buf = match dcache {
            Some(dc) => RxBuf::DCache(dc),
            None => RxBuf::Heap(vec![0u8; 32 * 1024]),
        };
        Self {
            stream,
            peer_addr,
            token,
            io_state: IoState::Idle,
            rx_buf,
            header_buf: [0; FRAME_HEADER_SIZE],
            send_buf: Vec::with_capacity(Self::SEND_BUF_SIZE),
            send_backlog: VecDeque::with_capacity(64),
            send_cursor: 0,
            timers,
        }
    }

    pub(crate) fn reset_for_reconnect(
        &mut self,
        stream: std::net::TcpStream,
        on_connect_msg: Option<&Vec<u8>>,
    ) -> ConnState {
        self.stream = stream;
        self.io_state = IoState::Idle;
        self.header_buf.fill(0);
        self.send_buf.clear();
        self.send_cursor = 0;
        if let Some(msg) = on_connect_msg {
            return self.write_or_enqueue_with(|buf| buf.extend_from_slice(msg));
        }
        ConnState::Alive
    }

    /// Post a RECV SQE for the frame header (or the remainder of one).
    ///
    /// Only valid when `io_state` is `Idle` or `HeaderPending`.
    pub(crate) fn post_header_sqe(&mut self, sq: &mut io_uring::SubmissionQueue<'_>) {
        let have = match self.io_state {
            IoState::Idle => 0,
            IoState::HeaderPending { have } => have,
            _ => return,
        };
        let fd = self.stream.as_raw_fd();
        let ptr = unsafe { self.header_buf.as_mut_ptr().add(have) };
        let len = (FRAME_HEADER_SIZE - have) as u32;
        let ud = encode_user_data(self.token, PHASE_HEADER);
        let sqe = opcode::Recv::new(types::Fd(fd), ptr, len).build().user_data(ud);
        // SAFETY: fd lives as long as self.stream.
        if unsafe { sq.push(&sqe) }.is_err() {
            warn!(token = ?self.token, "SQ full, skipping header RECV");
            return;
        }
        self.io_state = IoState::HeaderPending { have };
    }

    /// Post the payload SQE. Uses READ_FIXED when `buf_index` is `Some`
    /// (DCache registered), plain RECV otherwise.
    pub(crate) fn post_payload_sqe(
        &mut self,
        sq: &mut io_uring::SubmissionQueue<'_>,
        buf_index: Option<u16>,
    ) {
        let (msg_len, offset, dc_ref) = match self.io_state {
            IoState::PayloadPending { msg_len, offset, dc_ref, .. } => (msg_len, offset, dc_ref),
            _ => return,
        };

        let fd = self.stream.as_raw_fd();
        let remaining = (msg_len - offset) as u32;
        let ud = encode_user_data(self.token, PHASE_PAYLOAD);

        let result = match (dc_ref, buf_index) {
            (Some(r), Some(buf_idx)) => {
                // Zero-copy: kernel writes directly into the registered DCache region.
                let RxBuf::DCache(dc) = &self.rx_buf else { unreachable!() };
                let ptr = dc.payload_ptr(r, offset);
                let sqe =
                    opcode::ReadFixed::new(types::Fd(fd), ptr, remaining, buf_idx)
                        .build()
                        .user_data(ud);
                unsafe { sq.push(&sqe) }
            }
            _ => {
                // Heap fallback or unregistered DCache: plain RECV.
                let ptr = match &mut self.rx_buf {
                    RxBuf::Heap(buf) => unsafe { buf.as_mut_ptr().add(offset) },
                    RxBuf::DCache(dc) => dc.payload_ptr(dc_ref.unwrap(), offset),
                };
                let sqe =
                    opcode::Recv::new(types::Fd(fd), ptr, remaining).build().user_data(ud);
                unsafe { sq.push(&sqe) }
            }
        };

        if result.is_err() {
            warn!(token = ?self.token, "SQ full, skipping payload SQE");
        }
    }

    /// Handle CQE for a header RECV.
    pub(crate) fn handle_header_cqe(&mut self, result: i32) -> HeaderCqeOutcome {
        let have = match self.io_state {
            IoState::HeaderPending { have } => have,
            _ => return HeaderCqeOutcome::Disconnect,
        };

        if result <= 0 {
            return HeaderCqeOutcome::Disconnect;
        }
        let new_have = have + result as usize;

        if new_have < FRAME_HEADER_SIZE {
            self.io_state = IoState::HeaderPending { have: new_have };
            return HeaderCqeOutcome::NeedHeaderSqe;
        }

        let msg_len =
            u32::from_le_bytes(self.header_buf[..LEN_HEADER_SIZE].try_into().unwrap()) as usize;
        let send_ts = Nanos(u64::from_le_bytes(
            self.header_buf[LEN_HEADER_SIZE..FRAME_HEADER_SIZE].try_into().unwrap(),
        ));
        self.header_buf.fill(0);

        if msg_len == 0 {
            self.io_state = IoState::Idle;
            return HeaderCqeOutcome::Idle;
        }

        let dc_ref = match &mut self.rx_buf {
            RxBuf::DCache(dc) => match dc.reserve(msg_len) {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!("dcache reserve failed: {e}");
                    return HeaderCqeOutcome::Disconnect;
                }
            },
            RxBuf::Heap(buf) => {
                if msg_len > buf.len() {
                    buf.resize(msg_len, 0);
                }
                None
            }
        };

        self.io_state = IoState::PayloadPending { msg_len, offset: 0, send_ts, dc_ref };
        HeaderCqeOutcome::PayloadReady
    }

    /// Handle CQE for a payload SQE.
    pub(crate) fn handle_payload_cqe(&mut self, result: i32) -> PayloadCqeOutcome<'_> {
        let (msg_len, offset, send_ts, dc_ref) = match self.io_state {
            IoState::PayloadPending { msg_len, offset, send_ts, dc_ref } => {
                (msg_len, offset, send_ts, dc_ref)
            }
            _ => return PayloadCqeOutcome::Disconnect,
        };

        if result <= 0 {
            return PayloadCqeOutcome::Disconnect;
        }
        let new_offset = offset + result as usize;

        if new_offset < msg_len {
            self.io_state =
                IoState::PayloadPending { msg_len, offset: new_offset, send_ts, dc_ref };
            return PayloadCqeOutcome::NeedPayloadSqe;
        }

        if let Some(timers) = &mut self.timers {
            timers.latency.emit_latency_from_nanos(send_ts, Nanos::now());
        }
        self.io_state = IoState::Idle;

        let payload = if let Some(r) = dc_ref {
            MessagePayload::Cached(r)
        } else {
            let RxBuf::Heap(buf) = &self.rx_buf else { unreachable!() };
            MessagePayload::Raw(&buf[..msg_len])
        };
        PayloadCqeOutcome::Done { payload, send_ts }
    }

    #[inline]
    pub fn write_or_enqueue_with<F>(&mut self, serialise: F) -> ConnState
    where
        F: Fn(&mut Vec<u8>),
    {
        self.serialise_frame(serialise);
        if self.send_buf.is_empty() {
            return ConnState::Alive;
        }

        if !self.send_backlog.is_empty() {
            if self.drain_backlog() == ConnState::Disconnected {
                return ConnState::Disconnected;
            }
            if !self.send_backlog.is_empty() {
                let data = self.alloc_vec(0);
                self.send_backlog.push_back(data);
                return ConnState::Alive;
            }
        }

        match self.stream.write_vectored(&[
            IoSlice::new(&self.header_buf),
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
                self.send_backlog.push_back(data);
                ConnState::Alive
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let data = self.alloc_vec(0);
                self.send_backlog.push_back(data);
                ConnState::Alive
            }
            Err(err) => {
                warn!(?err, "tcp: stream write fail");
                ConnState::Disconnected
            }
        }
    }

    #[inline]
    pub(crate) fn has_backlog(&self) -> bool {
        !self.send_backlog.is_empty()
    }

    #[inline]
    pub(crate) fn drain_backlog(&mut self) -> ConnState {
        while let Some(front) = self.send_backlog.front() {
            match self.stream.write(&front[self.send_cursor..]) {
                Ok(0) => return ConnState::Disconnected,
                Ok(n) => {
                    self.send_cursor += n;
                    if self.send_cursor == front.len() {
                        self.send_backlog.pop_front();
                        self.send_cursor = 0;
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => {
                    debug!(?err, "tcp: write from backlog");
                    return ConnState::Disconnected;
                }
            }
        }
        ConnState::Alive
    }

    #[inline]
    fn alloc_vec(&mut self, written: usize) -> Vec<u8> {
        let total = FRAME_HEADER_SIZE + self.send_buf.len();
        let t0 = self.timers.is_some().then(Nanos::now);
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

    pub fn close(&mut self) {
        debug!("terminating connection");
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }

    pub(crate) fn peer(&self) -> SocketAddr {
        self.peer_addr
    }
}

pub(crate) enum HeaderCqeOutcome {
    /// Full header parsed; stream is `PayloadPending`. Caller must post payload SQE.
    PayloadReady,
    /// Zero-length keepalive; stream returned to `Idle`.
    Idle,
    /// Partial; re-post header RECV for the remainder.
    NeedHeaderSqe,
    Disconnect,
}

pub(crate) enum PayloadCqeOutcome<'a> {
    Done { payload: MessagePayload<'a>, send_ts: Nanos },
    /// Partial; re-post payload SQE for the remainder.
    NeedPayloadSqe,
    Disconnect,
}

/// Set kernel SO_SNDBUF and SO_RCVBUF.
pub(crate) fn set_socket_buf_size(stream: &std::net::TcpStream, size: usize) {
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
