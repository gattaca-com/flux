use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    net::SocketAddr,
};

use flux_communication::Timer;
use flux_timing::Nanos;
use mio::{Interest, Registry, Token, event::Event};
use tracing::{debug, warn};

use crate::tcp::STREAM;

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
    PayloadDone { frame: &'a [u8], send_ts: Nanos },
    WouldBlock,
    Disconnected,
}

#[derive(Clone, Copy)]
enum RxState {
    /// Waiting for the frame header.
    ReadingHeader { buf: [u8; FRAME_HEADER_SIZE], have: usize },
    /// Reading the payload of `msg_len` bytes.
    ReadingPayload { msg_len: usize, offset: usize, send_ts: Nanos },
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

    rx_state: RxState,
    rx_buf: Vec<u8>,

    send_buf: Vec<u8>,
    /// Filled when send would block.
    /// First entry will either be a full message or the current partially
    /// written head.
    send_backlog: VecDeque<Vec<u8>>,

    /// True if WRITABLE interest is currently registered in `poll`.
    /// Invariant: `writable_armed == !send_q.is_empty()`
    writable_armed: bool,

    /// Timers for network latency and alloc telemetry.
    timers: Option<TcpTimers>,
}

impl TcpStream {
    pub const SEND_BUF_SIZE: usize = 32 * 1024;

    #[allow(clippy::needless_pass_by_value)]
    #[inline(never)]
    pub fn from_stream(stream: mio::net::TcpStream) -> io::Result<Self> {
        Self::from_stream_with_telemetry(stream, TcpTelemetry::Disabled)
    }

    #[inline(never)]
    pub fn from_stream_with_telemetry(
        stream: mio::net::TcpStream,
        telemetry: TcpTelemetry,
    ) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        let peer_addr = stream.peer_addr()?;

        let timers = match telemetry {
            TcpTelemetry::Disabled => None,
            TcpTelemetry::Enabled { app_name } => {
                Some(TcpTimers::new(app_name, &stream_label(&stream)))
            }
        };

        Ok(Self {
            stream,
            peer_addr,
            rx_state: RxState::ReadingHeader { buf: [0; FRAME_HEADER_SIZE], have: 0 },
            rx_buf: vec![0; RX_BUF_SIZE],
            send_buf: vec![0; Self::SEND_BUF_SIZE],
            send_backlog: VecDeque::with_capacity(64),
            writable_armed: false,
            timers,
        })
    }

    /// Poll socket and calls `on_msg` for every fully assembled frame.
    /// Frame data is only valid for the duration of the callback.
    #[inline]
    pub fn poll_with<F>(&mut self, registry: &Registry, ev: &Event, on_msg: &mut F) -> ConnState
    where
        F: for<'a> FnMut(Token, &'a [u8], Nanos),
    {
        if ev.is_readable() {
            loop {
                match self.read_frame() {
                    ReadOutcome::PayloadDone { frame, send_ts } => {
                        on_msg(ev.token(), frame, send_ts);
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
    /// See (or use) `wincode_ser_into_slice` for help with what to use for the
    /// `serialise` closure,.
    ///
    /// TODO: avoid allocation by queueing offsets into `self.send_buf` instead
    /// of Vec<u8>.
    #[inline]
    pub fn write_or_enqueue_with<F>(&mut self, registry: &Registry, serialise: F) -> ConnState
    where
        F: Fn(&mut [u8]) -> usize,
    {
        let len = self.serialise_frame(serialise);

        if !self.send_backlog.is_empty() {
            let data = self.alloc_vec(0, len);
            return self.enqueue_back(registry, data);
        }

        let frame = &self.send_buf[..len];
        match self.stream.write(frame) {
            Ok(0) => {
                warn!("tcp: stream failed to write, disconnecting");
                ConnState::Disconnected
            }
            Ok(n) if n == len => ConnState::Alive,
            Ok(n) => {
                let data = self.alloc_vec(n, len);
                self.enqueue_front(registry, data)
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let data = self.alloc_vec(0, len);
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
    fn alloc_vec(&mut self, start: usize, end: usize) -> Vec<u8> {
        match &mut self.timers {
            Some(timers) => {
                let t0 = Nanos::now();
                let v = self.send_buf[start..end].to_vec();
                timers.alloc.emit_latency_from_nanos(t0, Nanos::now());
                v
            }
            None => self.send_buf[start..end].to_vec(),
        }
    }

    /// Flush queued data until kernel blocks, queue empty or we've written the
    /// max bytes per iter.
    /// returns connstate and whether it should be deregistered from writable
    #[inline]
    fn drain_backlog(&mut self, registry: &Registry) -> ConnState {
        while let Some(front) = self.send_backlog.front_mut()        {
            match self.stream.write(front) {
                Ok(0) => return ConnState::Disconnected,

                Ok(n) => {
                    if n == front.len() {
                        self.send_backlog.pop_front();
                    } else {
                        front.drain(..n);
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
            if let Err(err) = registry.reregister(&mut self.stream, STREAM, Interest::READABLE) {
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
    fn read_frame(&mut self) -> ReadOutcome<'_> {
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
                                    if msg_len > self.rx_buf.len() {
                                        debug!(
                                            buf_len = self.rx_buf.len(),
                                            need_len = msg_len,
                                            "tcp: buffer resized"
                                        );
                                        self.rx_buf.resize(msg_len, 0);
                                    }
                                    let send_ts = Nanos(u64::from_le_bytes(
                                        buf[LEN_HEADER_SIZE..FRAME_HEADER_SIZE].try_into().unwrap(),
                                    ));

                                    self.rx_state =
                                        RxState::ReadingPayload { msg_len, offset: 0, send_ts };
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

                RxState::ReadingPayload { msg_len, mut offset, send_ts } => {
                    while offset < msg_len {
                        match self.stream.read(&mut self.rx_buf[offset..msg_len]) {
                            Ok(0) => return ReadOutcome::Disconnected,

                            Ok(n) => {
                                offset += n;

                                // offset can never be > msg_len as we pass a fixed length slice
                                // into rx_buf. stream will only ever read <= msg_len bytes.
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

                                    return ReadOutcome::PayloadDone {
                                        frame: &self.rx_buf[..msg_len],
                                        send_ts,
                                    };
                                }
                            }

                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                self.rx_state =
                                    RxState::ReadingPayload { msg_len, offset, send_ts };
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
                STREAM,
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
    /// Returns total frame length (header + payload).
    ///
    /// If the payload exceeds the current buffer, `send_buf` is grown and the
    /// closure is called a second time.  The closure MUST return the required
    /// payload size even when the buffer is too small (and skip the actual
    /// write in that case) so the retry can succeed.
    #[inline(always)]
    fn serialise_frame<F>(&mut self, serialise: F) -> usize
    where
        F: Fn(&mut [u8]) -> usize,
    {
        let payload_len = serialise(&mut self.send_buf[FRAME_HEADER_SIZE..]);
        let needed = FRAME_HEADER_SIZE + payload_len;

        if needed > self.send_buf.len() {
            debug!(buf_len = self.send_buf.len(), need_len = needed, "tcp: send buffer resized");
            self.send_buf.resize(needed, 0);
            serialise(&mut self.send_buf[FRAME_HEADER_SIZE..]);
        }

        // write frame header
        self.send_buf[..LEN_HEADER_SIZE].copy_from_slice(&(payload_len as u32).to_le_bytes());
        self.send_buf[LEN_HEADER_SIZE..FRAME_HEADER_SIZE]
            .copy_from_slice(&Nanos::now().0.to_le_bytes());

        needed
    }

    pub fn close(&mut self, registry: &Registry) -> Option<SocketAddr> {
        debug!("terminating connection");
        let _ = registry.deregister(&mut self.stream);
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
        self.peer_addr
    }
}

/// Format: local_port-remote_ip:remote_port (e.g. "8080-192.168.1.1:9000")
fn stream_label(stream: &mio::net::TcpStream) -> String {
    let local_port = stream.local_addr().map(|a| a.port()).unwrap_or(0);
    let peer = stream.peer_addr().map(|a| a.to_string()).unwrap_or_else(|_| "unknown".into());
    format!("{local_port}-{peer}")
}
