use std::{
    io::{self, IoSlice, Read, Write},
    net::{Shutdown, SocketAddr},
    ops::{Deref, DerefMut, Range},
};

use flux_communication::Timer;
use flux_timing::{Duration, Instant, Nanos, Repeater};
use mio::{Events, Interest, Poll, Registry, Token, event::Event, net::TcpListener};
use tracing::{debug, error, info, warn};

use super::{
    TcpManager, TcpTelemetry, set_socket_buf_size,
    stream::{
        DEFAULT_TCP_USER_TIMEOUT_MS, FRAME_HEADER_SIZE, frame_payload_len, frame_send_ts,
        set_keepalive, set_user_timeout, write_frame_header, write_frame_len, write_frame_ts,
    },
};
use crate::tls::Session;

const EVENTS_CAPACITY: usize = 128;
const INITIAL_CONNECTION_CAPACITY: usize = 8;
const INITIAL_GROUP_CAPACITY: usize = 4;
const INITIAL_LISTENER_CAPACITY: usize = 2;
const INITIAL_RX_BUFFER_SIZE: usize = 32 * 1024;
const INITIAL_SEND_BUFFER_SIZE: usize = 32 * 1024;
const DEFAULT_MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_BACKLOG_WARN_BYTES: usize = 64 * 1024 * 1024;
const BACKLOG_WARNING_INTERVAL_SECS: u64 = 10;

/// Identifies a set of connections using the same application protocol and
/// socket configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TcpGroup(usize);

/// Selects how a TCP group encodes messages on the wire.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Framing {
    /// Messages carry Flux's length and send-timestamp header.
    #[default]
    LengthPrefixed,
    /// Bytes pass through untouched. Received chunks do not preserve message
    /// boundaries, and their event timestamp is the local receive time.
    Raw,
}

/// The payload of one outgoing frame while a serialiser fills it.
///
/// Frames are staged back to back in one send buffer, so a serialiser must
/// not be able to reach the bytes of frames staged before its own. This
/// wrapper exposes only the payload region: every length, index, and
/// truncation is relative to the start of the payload, and the frame header
/// and earlier frames stay out of reach.
pub struct PayloadBuf<'a> {
    bytes: &'a mut Vec<u8>,
    start: usize,
}

impl<'a> PayloadBuf<'a> {
    fn new(bytes: &'a mut Vec<u8>) -> Self {
        let start = bytes.len();
        Self { bytes, start }
    }

    /// Bytes serialised into this payload so far.
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len() - self.start
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reserves room for at least `additional` more payload bytes.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.bytes.reserve(additional);
    }

    #[inline]
    pub fn push(&mut self, byte: u8) {
        self.bytes.push(byte);
    }

    #[inline]
    pub fn extend_from_slice(&mut self, other: &[u8]) {
        self.bytes.extend_from_slice(other);
    }

    /// Resizes the payload to `len` bytes, filling new bytes with `value`.
    ///
    /// # Panics
    ///
    /// Panics if the payload cannot fit in memory, as `Vec::resize` does.
    #[inline]
    pub fn resize(&mut self, len: usize, value: u8) {
        let end = self.start.checked_add(len).expect("payload length overflows usize");
        self.bytes.resize(end, value);
    }

    /// Shortens the payload to `len` bytes; no-op if already shorter.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        // Clamping keeps `start + len` from wrapping into earlier frames.
        let len = len.min(self.len());
        self.bytes.truncate(self.start + len);
    }

    /// Removes every payload byte serialised so far.
    #[inline]
    pub fn clear(&mut self) {
        self.bytes.truncate(self.start);
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[self.start..]
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes[self.start..]
    }
}

impl Deref for PayloadBuf<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl DerefMut for PayloadBuf<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl Extend<u8> for PayloadBuf<'_> {
    #[inline]
    fn extend<I: IntoIterator<Item = u8>>(&mut self, iter: I) {
        self.bytes.extend(iter);
    }
}

impl<'b> Extend<&'b u8> for PayloadBuf<'_> {
    #[inline]
    fn extend<I: IntoIterator<Item = &'b u8>>(&mut self, iter: I) {
        self.bytes.extend(iter);
    }
}

#[cfg(feature = "wincode")]
impl wincode::io::Writer for PayloadBuf<'_> {
    #[inline]
    fn write(&mut self, src: &[u8]) -> Result<(), wincode::io::WriteError> {
        self.bytes.extend_from_slice(src);
        Ok(())
    }

    #[inline]
    unsafe fn as_trusted_for(
        &mut self,
        n_bytes: usize,
    ) -> Result<impl wincode::io::Writer, wincode::io::WriteError> {
        // SAFETY: the caller upholds the `as_trusted_for` contract, and the
        // `Vec<u8>` writer only ever appends, so the payload start stays valid.
        unsafe { wincode::io::Writer::as_trusted_for(&mut *self.bytes, n_bytes) }
    }
}

impl Write for PayloadBuf<'_> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.bytes.extend_from_slice(buf);
        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Configuration shared by every listener and connection in a [`TcpGroup`].
#[derive(Clone)]
pub struct TcpGroupConfig {
    /// Stable label used in logs and telemetry.
    pub name: &'static str,
    /// Static payload sent on every newly established connection before its
    /// lifecycle event is emitted.
    pub on_connect_msg: Option<Vec<u8>>,
    /// Requested `SO_SNDBUF` and `SO_RCVBUF` size.
    pub socket_buf_size: Option<usize>,
    /// Whether to enable `TCP_NODELAY`.
    pub nodelay: bool,
    /// Whether to enable TCP keepalive.
    pub keepalive: bool,
    /// Linux `TCP_USER_TIMEOUT`, in milliseconds.
    pub user_timeout_ms: u32,
    /// Retry interval for persistent outbound endpoints.
    pub reconnect_interval: Duration,
    /// How long a TLS handshake may stay incomplete before the connection is
    /// dropped and, for outbound endpoints, redialled.
    pub handshake_timeout: Duration,
    /// Emit rate-limited warnings above this many queued bytes. The queue is
    /// allowed to continue growing.
    pub backlog_warn_bytes: Option<usize>,
    /// Disconnect the peer before its queued bytes would exceed this limit.
    /// `None` allows the queue to grow without a hard limit.
    pub max_backlog_bytes: Option<usize>,
    /// Largest accepted or emitted frame payload. For [`Framing::Raw`], this
    /// caps a single send and bounds each received read chunk.
    pub max_frame_size: usize,
    /// Wire encoding used by this group.
    pub framing: Framing,
    /// Per-connection latency and allocation telemetry.
    pub telemetry: TcpTelemetry,
}

impl Default for TcpGroupConfig {
    fn default() -> Self {
        Self {
            name: "tcp",
            on_connect_msg: None,
            socket_buf_size: None,
            nodelay: true,
            keepalive: false,
            user_timeout_ms: DEFAULT_TCP_USER_TIMEOUT_MS,
            reconnect_interval: Duration::from_secs(2),
            handshake_timeout: Duration::from_secs(10),
            backlog_warn_bytes: Some(DEFAULT_BACKLOG_WARN_BYTES),
            max_backlog_bytes: None,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            framing: Framing::LengthPrefixed,
            telemetry: TcpTelemetry::Disabled,
        }
    }
}

impl TcpGroupConfig {
    /// Enables TCP keepalive for every connection in this group.
    pub fn with_keepalive(mut self) -> Self {
        self.keepalive = true;
        self
    }
}

/// Event emitted by [`TcpNetwork::poll_with`].
pub enum TcpEvent<'a> {
    /// A listener accepted a new connection.
    Accepted { group: TcpGroup, token: Token, peer_addr: SocketAddr },
    /// A persistent outbound endpoint established a connection.
    Connected { group: TcpGroup, token: Token, peer_addr: SocketAddr },
    /// A complete length-prefixed message or a raw read chunk was received.
    /// For raw groups, chunks do not preserve message boundaries and `send_ts`
    /// is the local receive time.
    Message { group: TcpGroup, token: Token, payload: &'a [u8], send_ts: Nanos },
    /// An established connection was closed.
    Disconnected { group: TcpGroup, token: Token, peer_addr: SocketAddr },
}

struct GroupState {
    config: TcpGroupConfig,
    reconnector: Repeater,
}

struct Listener {
    token: Token,
    group: TcpGroup,
    socket: TcpListener,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectionKind {
    Accepted,
    Outbound,
}

#[allow(clippy::large_enum_variant)]
enum ConnectionState {
    Disconnected,
    Connecting(mio::net::TcpStream),
    Connected(FramedStream),
}

struct Connection {
    token: Token,
    group: TcpGroup,
    peer_addr: SocketAddr,
    kind: ConnectionKind,
    state: ConnectionState,
    close_when_drained: bool,
    timers: Option<NetworkTimers>,
    /// Client TLS for this endpoint; `None` leaves the wire in plaintext.
    /// Boxed so a plaintext connection carries a pointer, not a session.
    tls: Option<Box<Session>>,
}

impl Connection {
    /// Whether a TLS connection is still negotiating and cannot carry
    /// application bytes yet.
    fn is_handshaking(&self) -> bool {
        self.tls.as_ref().is_some_and(|tls| tls.is_handshaking())
    }
    /// Whether this connection has been reported as established. Plaintext
    /// connections are announced as soon as the socket connects.
    fn announced(&self) -> bool {
        self.tls.as_ref().is_none_or(|tls| tls.announced())
    }
}

#[derive(Clone, Copy)]
struct NetworkTimers {
    latency: Option<Timer>,
    alloc: Timer,
}

impl NetworkTimers {
    fn new(
        telemetry: TcpTelemetry,
        group_name: &str,
        token: Token,
        peer_addr: SocketAddr,
        framing: Framing,
    ) -> Option<Self> {
        let TcpTelemetry::Enabled { app_name } = telemetry else { return None };
        let label = format!("{group_name}-{}-{peer_addr}", token.0);
        Some(Self {
            latency: (framing == Framing::LengthPrefixed)
                .then(|| Timer::new(app_name, format!("tcp_latency_{label}"))),
            alloc: Timer::new(app_name, format!("tcp_alloc_{label}")),
        })
    }
}

#[derive(Clone, Copy)]
struct PendingDisconnect {
    group: TcpGroup,
    token: Token,
    peer_addr: SocketAddr,
}

struct NetworkState {
    registry: Registry,
    groups: Vec<GroupState>,
    listeners: Vec<Listener>,
    connections: Vec<Connection>,
    pending_disconnects: Vec<PendingDisconnect>,
    /// Range used to assign tokens to new listeners and connections.
    /// Assigned tokens are not reused.
    token_range: Range<usize>,
    next_token: usize,
    /// Frames staged for the next socket write, each as a contiguous
    /// `[header][payload]` for length-prefixed groups or bare bytes for raw
    /// groups.
    send_buffer: Vec<u8>,
    /// Staged frames encrypted for one TLS connection.
    tls_buffer: Vec<u8>,
    /// Socket reads issued, across all connections.
    read_syscalls: u64,
}

impl NetworkState {
    fn new(registry: Registry, tokens: Range<usize>) -> Self {
        Self {
            registry,
            groups: Vec::with_capacity(INITIAL_GROUP_CAPACITY),
            listeners: Vec::with_capacity(INITIAL_LISTENER_CAPACITY),
            connections: Vec::with_capacity(INITIAL_CONNECTION_CAPACITY),
            pending_disconnects: Vec::with_capacity(INITIAL_CONNECTION_CAPACITY),
            next_token: tokens.start,
            token_range: tokens,
            send_buffer: Vec::with_capacity(INITIAL_SEND_BUFFER_SIZE),
            tls_buffer: Vec::new(),
            read_syscalls: 0,
        }
    }

    fn next_token(&mut self) -> Token {
        // Reject allocations beyond the caller-provided range.
        assert!(
            self.next_token < self.token_range.end,
            "tcp token range {:?} exhausted",
            self.token_range
        );
        let token = Token(self.next_token);
        self.next_token += 1;
        token
    }

    fn config(&self, group: TcpGroup) -> &TcpGroupConfig {
        &self.groups[group.0].config
    }

    fn listen(&mut self, group: TcpGroup, addr: SocketAddr) -> io::Result<()> {
        if group.0 >= self.groups.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "unknown TCP group"));
        }
        let mut socket = TcpManager::bind_listener(addr, self.config(group).socket_buf_size)?;
        let token = self.next_token();
        self.registry.register(&mut socket, token, Interest::READABLE)?;
        self.listeners.push(Listener { token, group, socket });
        Ok(())
    }

    fn connect(
        &mut self,
        group: TcpGroup,
        peer_addr: SocketAddr,
        tls: Option<Box<Session>>,
    ) -> Token {
        assert!(group.0 < self.groups.len(), "unknown TCP group");
        let token = self.next_token();
        let config = self.config(group);
        let timers =
            NetworkTimers::new(config.telemetry, config.name, token, peer_addr, config.framing);
        self.connections.push(Connection {
            token,
            group,
            peer_addr,
            kind: ConnectionKind::Outbound,
            state: ConnectionState::Disconnected,
            close_when_drained: false,
            timers,
            tls,
        });
        self.start_connect(self.connections.len() - 1);
        token
    }

    fn start_connect(&mut self, index: usize) {
        let connection = &self.connections[index];
        if connection.kind != ConnectionKind::Outbound ||
            !matches!(connection.state, ConnectionState::Disconnected)
        {
            return;
        }

        let token = connection.token;
        let group = connection.group;
        let peer_addr = connection.peer_addr;
        let socket_buf_size = self.config(group).socket_buf_size;

        let Ok(mut socket) = mio::net::TcpStream::connect(peer_addr)
            .inspect_err(|err| debug!(?err, %peer_addr, "couldn't start tcp connection"))
        else {
            return;
        };
        if let Some(size) = socket_buf_size {
            set_socket_buf_size(&socket, size);
        }
        if let Err(err) = self.registry.register(&mut socket, token, Interest::WRITABLE) {
            warn!(?err, %peer_addr, "couldn't register connecting tcp stream");
            let _ = socket.shutdown(Shutdown::Both);
            return;
        }
        self.connections[index].state = ConnectionState::Connecting(socket);
    }

    /// Drops TLS connections whose handshake never finished. They are
    /// redialled by the usual reconnect sweep; no lifecycle event is emitted
    /// because none was ever announced.
    /// Drops a TLS connection whose handshake never finished, so the sweep
    /// below redials it. Nothing is emitted: it was never announced.
    fn drop_stalled_handshake(&mut self, index: usize) {
        let timeout = self.config(self.connections[index].group).handshake_timeout;
        if !self.connections[index].tls.as_ref().is_some_and(|tls| tls.handshake_stalled(timeout)) {
            return;
        }
        let peer_addr = self.connections[index].peer_addr;
        warn!(%peer_addr, ?timeout, "tls handshake timed out");
        self.disconnect_index(index, false);
    }

    fn maybe_reconnect(&mut self) {
        let now = Instant::now();
        for group_index in 0..self.groups.len() {
            if !self.groups[group_index].reconnector.fired_at(now) {
                continue;
            }
            let group = TcpGroup(group_index);
            // Swept on the reconnect tick, so a poll with no reconnect work
            // pays nothing for it; the timeout is coarse by that interval.
            for index in 0..self.connections.len() {
                if self.connections[index].group == group {
                    self.drop_stalled_handshake(index);
                }
            }
            for index in 0..self.connections.len() {
                if self.connections[index].group == group &&
                    self.connections[index].kind == ConnectionKind::Outbound &&
                    matches!(self.connections[index].state, ConnectionState::Disconnected)
                {
                    self.start_connect(index);
                }
            }
        }
    }

    fn connect_complete(socket: &mio::net::TcpStream) -> io::Result<bool> {
        if let Some(err) = socket.take_error()? {
            return Err(err);
        }
        match socket.peer_addr() {
            Ok(_) => Ok(true),
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::NotConnected | io::ErrorKind::WouldBlock
                ) || matches!(err.raw_os_error(), Some(libc::EINPROGRESS | libc::EALREADY)) =>
            {
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    fn finish_connect<F>(&mut self, index: usize, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(TcpEvent<'a>),
    {
        let ConnectionState::Connecting(socket) = &self.connections[index].state else {
            return false;
        };
        match Self::connect_complete(socket) {
            Ok(false) => return false,
            Err(err) => {
                let peer_addr = self.connections[index].peer_addr;
                debug!(?err, %peer_addr, "tcp connection attempt failed");
                self.close_connection_socket(index);
                return false;
            }
            Ok(true) => {}
        }

        let ConnectionState::Connecting(mut socket) =
            std::mem::replace(&mut self.connections[index].state, ConnectionState::Disconnected)
        else {
            unreachable!();
        };
        let token = self.connections[index].token;
        let group = self.connections[index].group;
        let peer_addr = self.connections[index].peer_addr;
        let mut timers = self.connections[index].timers;
        // Opens the handshake before the group config is borrowed below.
        let is_tls = self.connections[index].tls.is_some();
        let mut handshake = Vec::new();
        if let Some(tls) = self.connections[index].tls.as_mut() {
            tls.start(&mut handshake);
        }
        let config = self.config(group);
        let group_name = config.name;

        if config.nodelay &&
            let Err(err) = socket.set_nodelay(true)
        {
            warn!(?err, %peer_addr, "couldn't set nodelay on tcp stream");
            let _ = self.registry.deregister(&mut socket);
            let _ = socket.shutdown(Shutdown::Both);
            return false;
        }
        if config.keepalive &&
            let Err(err) = set_keepalive(&socket)
        {
            warn!(?err, %peer_addr, "couldn't set keepalive on tcp stream");
            let _ = self.registry.deregister(&mut socket);
            let _ = socket.shutdown(Shutdown::Both);
            return false;
        }
        set_user_timeout(&socket, config.user_timeout_ms);
        if let Err(err) = self.registry.reregister(&mut socket, token, Interest::READABLE) {
            warn!(?err, %peer_addr, "couldn't register connected tcp stream");
            let _ = socket.shutdown(Shutdown::Both);
            return false;
        }

        let mut stream =
            FramedStream::new(socket, token, peer_addr, config.framing, config.max_frame_size);
        // TLS sends its opening flight here; `on_connect_msg` waits for the
        // finished handshake so it travels encrypted.
        let first = if is_tls { Some(&handshake[..]) } else { config.on_connect_msg.as_deref() };
        if let Some(message) = first {
            // Handshake records are the wire itself and are never framed.
            let header = (!is_tls && config.framing == Framing::LengthPrefixed).then(|| {
                let mut header = [0; FRAME_HEADER_SIZE];
                write_frame_header(&mut header, message.len(), Nanos::now());
                header
            });
            if stream.write_frame(&self.registry, header.as_ref(), message, config, &mut timers) ==
                StreamState::Disconnected
            {
                stream.close(&self.registry);
                self.connections[index].timers = timers;
                return false;
            }
        }

        self.connections[index].timers = timers;
        self.connections[index].state = ConnectionState::Connected(stream);
        info!(group = group_name, %peer_addr, "tcp connection established");
        if !is_tls {
            handler(TcpEvent::Connected { group, token, peer_addr });
        }
        true
    }

    fn accept_connections<F>(&mut self, listener_index: usize, handler: &mut F)
    where
        F: for<'a> FnMut(TcpEvent<'a>),
    {
        let group = self.listeners[listener_index].group;
        loop {
            let accepted = self.listeners[listener_index].socket.accept();
            let (mut socket, peer_addr) = match accepted {
                Ok(accepted) => accepted,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => {
                    warn!(?err, group = self.config(group).name, "tcp accept failed");
                    break;
                }
            };
            let token = self.next_token();
            let (stream, timers, group_name) = {
                let config = self.config(group);
                if let Some(size) = config.socket_buf_size {
                    set_socket_buf_size(&socket, size);
                }
                if config.nodelay &&
                    let Err(err) = socket.set_nodelay(true)
                {
                    warn!(?err, %peer_addr, "couldn't set nodelay on accepted tcp stream");
                    let _ = socket.shutdown(Shutdown::Both);
                    continue;
                }
                if config.keepalive &&
                    let Err(err) = set_keepalive(&socket)
                {
                    warn!(?err, %peer_addr, "couldn't set keepalive on accepted tcp stream");
                    let _ = socket.shutdown(Shutdown::Both);
                    continue;
                }
                set_user_timeout(&socket, config.user_timeout_ms);
                if let Err(err) = self.registry.register(&mut socket, token, Interest::READABLE) {
                    warn!(?err, %peer_addr, "couldn't register accepted tcp stream");
                    let _ = socket.shutdown(Shutdown::Both);
                    continue;
                }

                let mut timers = NetworkTimers::new(
                    config.telemetry,
                    config.name,
                    token,
                    peer_addr,
                    config.framing,
                );
                let mut stream = FramedStream::new(
                    socket,
                    token,
                    peer_addr,
                    config.framing,
                    config.max_frame_size,
                );
                if let Some(message) = config.on_connect_msg.as_deref() {
                    let header = (config.framing == Framing::LengthPrefixed).then(|| {
                        let mut header = [0; FRAME_HEADER_SIZE];
                        write_frame_header(&mut header, message.len(), Nanos::now());
                        header
                    });
                    if stream.write_frame(
                        &self.registry,
                        header.as_ref(),
                        message,
                        config,
                        &mut timers,
                    ) == StreamState::Disconnected
                    {
                        stream.close(&self.registry);
                        continue;
                    }
                }
                (stream, timers, config.name)
            };

            self.connections.push(Connection {
                token,
                group,
                peer_addr,
                kind: ConnectionKind::Accepted,
                state: ConnectionState::Connected(stream),
                close_when_drained: false,
                timers,
                tls: None,
            });
            info!(group = group_name, %peer_addr, "tcp connection accepted");
            handler(TcpEvent::Accepted { group, token, peer_addr });
        }
    }

    fn handle_event<F>(&mut self, event: &Event, handler: &mut F)
    where
        F: for<'a> FnMut(TcpEvent<'a>),
    {
        let token = event.token();
        if let Some(index) = self.listeners.iter().position(|listener| listener.token == token) {
            self.accept_connections(index, handler);
            return;
        }
        let Some(index) = self.connections.iter().position(|connection| connection.token == token)
        else {
            debug!(?token, "ignoring stale tcp readiness event");
            return;
        };

        if matches!(self.connections[index].state, ConnectionState::Connecting(_)) &&
            !self.finish_connect(index, handler)
        {
            return;
        }
        if !matches!(self.connections[index].state, ConnectionState::Connected(_)) {
            return;
        }

        let group = self.connections[index].group;
        let peer_addr = self.connections[index].peer_addr;
        let config = &self.groups[group.0].config;
        let (state, queue_empty) = {
            let connection = &mut self.connections[index];
            let ConnectionState::Connected(stream) = &mut connection.state else { unreachable!() };
            let state = stream.poll_with(
                &self.registry,
                event,
                config,
                &mut connection.timers,
                &mut connection.tls,
                &mut self.read_syscalls,
                &mut |payload, send_ts| {
                    handler(TcpEvent::Message { group, token, payload, send_ts });
                },
            );
            (state, stream.send_queue.is_empty())
        };
        if state == StreamState::Disconnected {
            // A handshake that never completed was never announced.
            if self.connections[index].announced() {
                handler(TcpEvent::Disconnected { group, token, peer_addr });
            }
            self.disconnect_index(index, false);
        } else if self.connections[index].close_when_drained && queue_empty {
            self.disconnect_index(index, true);
        } else if self.connections[index].tls.as_ref().is_some_and(|tls| tls.handshake_completed())
        {
            // The encrypted connection is usable now; `on_connect_msg` goes
            // out through the normal send path so it is encrypted too.
            if let Some(message) = self.groups[group.0].config.on_connect_msg.clone() &&
                !self.send_with(token, |buf| buf.extend_from_slice(&message))
            {
                return;
            }
            // Marked only here, so anything dropped earlier stays silent.
            if let Some(tls) = self.connections[index].tls.as_mut() {
                tls.mark_announced();
            }
            handler(TcpEvent::Connected { group, token, peer_addr });
        }
    }

    fn close_connection_socket(&mut self, index: usize) -> bool {
        let old_state =
            std::mem::replace(&mut self.connections[index].state, ConnectionState::Disconnected);
        match old_state {
            ConnectionState::Disconnected => false,
            ConnectionState::Connecting(mut socket) => {
                let _ = self.registry.deregister(&mut socket);
                let _ = socket.shutdown(Shutdown::Both);
                false
            }
            ConnectionState::Connected(mut stream) => {
                stream.close(&self.registry);
                true
            }
        }
    }

    fn disconnect_index(&mut self, index: usize, notify: bool) {
        let event = PendingDisconnect {
            group: self.connections[index].group,
            token: self.connections[index].token,
            peer_addr: self.connections[index].peer_addr,
        };
        let kind = self.connections[index].kind;
        self.connections[index].close_when_drained = false;
        let was_connected = self.close_connection_socket(index);
        if kind == ConnectionKind::Accepted {
            self.connections.swap_remove(index);
        }
        if notify && was_connected {
            self.pending_disconnects.push(event);
        }
    }

    fn drain_pending_disconnects<F>(&mut self, handler: &mut F)
    where
        F: for<'a> FnMut(TcpEvent<'a>),
    {
        for event in self.pending_disconnects.drain(..) {
            handler(TcpEvent::Disconnected {
                group: event.group,
                token: event.token,
                peer_addr: event.peer_addr,
            });
        }
    }

    /// Finds the connection `token` can currently send on.
    fn sendable_index(&self, token: Token) -> Option<usize> {
        self.connections.iter().position(|connection| {
            connection.token == token &&
                !connection.close_when_drained &&
                !connection.is_handshaking() &&
                matches!(connection.state, ConnectionState::Connected(_))
        })
    }

    /// Serialises one payload as a frame at the end of `send_buffer`.
    /// Length-prefixed groups reserve a header ahead of the payload and fill
    /// in its length here; `stamp_frames` writes the timestamp just before the
    /// socket write. Empty or oversized payloads are removed again. Returns
    /// whether the frame was kept.
    fn append_frame<F>(&mut self, group: TcpGroup, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let config = &self.groups[group.0].config;
        let framed = config.framing == Framing::LengthPrefixed;
        let start = self.send_buffer.len();
        if framed {
            self.send_buffer.resize(start + FRAME_HEADER_SIZE, 0);
        }
        let payload_start = self.send_buffer.len();
        let mut payload = PayloadBuf::new(&mut self.send_buffer);
        serialise(&mut payload);
        let payload_len = payload.len();
        if payload_len == 0 {
            self.send_buffer.truncate(start);
            return false;
        }
        if payload_len > config.max_frame_size || (framed && u32::try_from(payload_len).is_err()) {
            error!(
                group = config.name,
                payload_len,
                max_frame_size = config.max_frame_size,
                "tcp payload exceeds maximum frame size"
            );
            self.send_buffer.truncate(start);
            return false;
        }
        if framed {
            write_frame_len(&mut self.send_buffer[start..payload_start], payload_len);
        }
        true
    }

    /// Writes `ts` into the header of every frame staged in `send_buffer`.
    /// Raw groups carry no headers and are left untouched.
    fn stamp_frames(&mut self, group: TcpGroup, ts: Nanos) {
        if self.groups[group.0].config.framing != Framing::LengthPrefixed {
            return;
        }
        let mut offset = 0;
        while offset < self.send_buffer.len() {
            let header = &mut self.send_buffer[offset..offset + FRAME_HEADER_SIZE];
            write_frame_ts(header, ts);
            offset += FRAME_HEADER_SIZE + frame_payload_len(header);
        }
    }

    /// Writes the staged frames to the connection at `index` in one socket
    /// write, disconnecting it on failure. Returns whether the write was
    /// accepted or queued.
    fn write_staged(&mut self, index: usize) -> bool {
        let group = self.connections[index].group;
        let state = {
            let Self { groups, connections, registry, send_buffer, tls_buffer, .. } = self;
            let config = &groups[group.0].config;
            let connection = &mut connections[index];
            // TLS carries the staged frames as records, so the whole buffer
            // is encrypted and written unframed.
            let encrypted = connection.tls.as_mut().map(|session| {
                tls_buffer.clear();
                session.encrypt(send_buffer, tls_buffer)
            });
            let payload = match encrypted {
                None => Some(&send_buffer[..]),
                Some(true) => Some(&tls_buffer[..]),
                // The session is dead and can no longer produce records.
                Some(false) => None,
            };
            let ConnectionState::Connected(stream) = &mut connection.state else { unreachable!() };
            match payload {
                Some(payload) => {
                    stream.write_frame(registry, None, payload, config, &mut connection.timers)
                }
                None => StreamState::Disconnected,
            }
        };
        if state == StreamState::Disconnected {
            self.disconnect_index(index, true);
            return false;
        }
        true
    }

    fn send_with<F>(&mut self, token: Token, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let Some(index) = self.sendable_index(token) else {
            return false;
        };
        let group = self.connections[index].group;
        self.send_buffer.clear();
        if !self.append_frame(group, serialise) {
            return false;
        }
        self.stamp_frames(group, Nanos::now());
        self.write_staged(index)
    }

    fn send_many_with<I, F>(&mut self, token: Token, items: I, mut serialise: F) -> bool
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        let Some(index) = self.sendable_index(token) else {
            return false;
        };
        let group = self.connections[index].group;
        self.send_buffer.clear();
        for item in items {
            self.append_frame(group, |buf| serialise(buf, item));
        }
        if self.send_buffer.is_empty() {
            return false;
        }
        self.stamp_frames(group, Nanos::now());
        self.write_staged(index)
    }

    /// Whether `group` exists and has at least one member that can receive a
    /// broadcast right now.
    fn has_broadcast_recipient(&self, group: TcpGroup) -> bool {
        group.0 < self.groups.len() &&
            self.connections.iter().any(|connection| {
                connection.group == group &&
                    !connection.close_when_drained &&
                    !connection.is_handshaking() &&
                    matches!(connection.state, ConnectionState::Connected(_))
            })
    }

    /// Writes the staged frames to every connected member of `group`,
    /// disconnecting members whose write fails. Returns the number of
    /// recipients attempted.
    fn broadcast_staged(&mut self, group: TcpGroup) -> usize {
        let mut attempted = 0;
        let mut index = self.connections.len();
        while index != 0 {
            index -= 1;
            if self.connections[index].group != group ||
                self.connections[index].close_when_drained ||
                self.connections[index].is_handshaking() ||
                !matches!(self.connections[index].state, ConnectionState::Connected(_))
            {
                continue;
            }
            attempted += 1;
            self.write_staged(index);
        }
        attempted
    }

    fn broadcast_with<F>(&mut self, group: TcpGroup, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        if !self.has_broadcast_recipient(group) {
            return 0;
        }
        self.send_buffer.clear();
        if !self.append_frame(group, serialise) {
            return 0;
        }
        self.stamp_frames(group, Nanos::now());
        self.broadcast_staged(group)
    }

    fn broadcast_many_with<I, F>(&mut self, group: TcpGroup, items: I, mut serialise: F) -> usize
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        if !self.has_broadcast_recipient(group) {
            return 0;
        }
        self.send_buffer.clear();
        for item in items {
            self.append_frame(group, |buf| serialise(buf, item));
        }
        if self.send_buffer.is_empty() {
            return 0;
        }
        self.stamp_frames(group, Nanos::now());
        self.broadcast_staged(group)
    }

    fn disconnect(&mut self, token: Token) -> bool {
        let Some(index) = self.connections.iter().position(|connection| connection.token == token)
        else {
            return false;
        };
        if matches!(self.connections[index].state, ConnectionState::Disconnected) {
            return false;
        }
        self.disconnect_index(index, true);
        true
    }

    fn disconnect_when_drained(&mut self, token: Token) -> bool {
        let Some(index) = self.connections.iter().position(|connection| connection.token == token)
        else {
            return false;
        };
        let ConnectionState::Connected(stream) = &self.connections[index].state else {
            return false;
        };
        if stream.send_queue.is_empty() {
            return self.disconnect(token);
        }
        self.connections[index].close_when_drained = true;
        true
    }

    fn remove(&mut self, token: Token) -> bool {
        let Some(index) = self.connections.iter().position(|connection| connection.token == token)
        else {
            return false;
        };
        self.close_connection_socket(index);
        self.connections.swap_remove(index);
        self.pending_disconnects.retain(|event| event.token != token);
        true
    }
}

impl Drop for NetworkState {
    fn drop(&mut self) {
        // Deregister before closing. On Linux, a duplicated descriptor can
        // keep an epoll registration alive after the original descriptor closes.
        for index in 0..self.connections.len() {
            self.close_connection_socket(index);
        }
        for listener in &mut self.listeners {
            let _ = self.registry.deregister(&mut listener.socket);
        }
    }
}

/// A grouped collection of TCP listeners and persistent outbound endpoints
/// with an internal nonblocking poll.
///
/// Shared network operations are provided by [`TcpNetworkCore`] through
/// `Deref`. Use [`TcpNetworkWithExternalPoll`] to register the sockets with a
/// caller-owned poll instead.
pub struct TcpNetwork {
    events: Events,
    core: TcpNetworkCore,
    poll: Poll,
}

impl Default for TcpNetwork {
    fn default() -> Self {
        let poll = Poll::new().expect("failed to create poll");
        let registry = poll.registry().try_clone().expect("failed to clone poll registry");
        // This network owns the poll, so its token range starts at zero.
        Self {
            events: Events::with_capacity(EVENTS_CAPACITY),
            core: TcpNetworkCore::new(registry, 0..usize::MAX),
            poll,
        }
    }
}

impl Deref for TcpNetwork {
    type Target = TcpNetworkCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl DerefMut for TcpNetwork {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
}

impl TcpNetwork {
    pub fn poll_with<F>(&mut self, mut handler: F)
    where
        F: for<'a> FnMut(TcpEvent<'a>),
    {
        self.core.state.drain_pending_disconnects(&mut handler);
        self.core.state.maybe_reconnect();
        if let Err(err) = self.poll.poll(&mut self.events, Some(std::time::Duration::ZERO)) {
            if err.kind() != io::ErrorKind::Interrupted {
                flux_utils::safe_panic!("couldn't poll tcp network: {err}");
            }
            return;
        }
        for event in &self.events {
            self.core.state.handle_event(event, &mut handler);
        }
        self.core.state.drain_pending_disconnects(&mut handler);
    }
}

/// Shared state and network operations used by [`TcpNetwork`] and
/// [`TcpNetworkWithExternalPoll`].
///
/// This type stores protocol groups, listeners, and connections, but does not
/// own or poll a [`Poll`].
///
/// Unlike [`crate::NetworkDriver`], queued bytes are never retained across a
/// disconnected socket. Use `NetworkDriver` when reconnect backlog replay is
/// required.
pub struct TcpNetworkCore {
    state: NetworkState,
}

impl TcpNetworkCore {
    fn new(registry: Registry, tokens: Range<usize>) -> Self {
        Self { state: NetworkState::new(registry, tokens) }
    }

    /// Socket reads issued since creation, across all connections. Difference
    /// it around a `poll_with` to count that poll's reads.
    #[must_use]
    pub fn read_syscalls(&self) -> u64 {
        self.state.read_syscalls
    }

    /// Adds a protocol group and returns its handle.
    #[must_use = "the group handle identifies listeners and outbound endpoints"]
    pub fn add_group(&mut self, config: TcpGroupConfig) -> TcpGroup {
        assert!(config.max_frame_size > 0, "max_frame_size must be nonzero");
        if config.framing == Framing::LengthPrefixed {
            assert!(
                u32::try_from(config.max_frame_size).is_ok(),
                "max_frame_size exceeds the TCP wire length field"
            );
        }
        if let Some(message) = &config.on_connect_msg {
            assert!(!message.is_empty(), "on_connect_msg must be nonempty");
            assert!(
                message.len() <= config.max_frame_size,
                "on_connect_msg exceeds max_frame_size"
            );
        }
        if let Some(max) = config.max_backlog_bytes {
            assert!(max > 0, "max_backlog_bytes must be nonzero");
            if let Some(warn) = config.backlog_warn_bytes {
                assert!(warn < max, "backlog_warn_bytes must be below max_backlog_bytes");
            }
        }
        let group = TcpGroup(self.state.groups.len());
        let reconnector = Repeater::every(config.reconnect_interval);
        self.state.groups.push(GroupState { config, reconnector });
        group
    }

    /// Adds a listener to `group`.
    pub fn listen(&mut self, group: TcpGroup, addr: SocketAddr) -> io::Result<()> {
        self.state.listen(group, addr)
    }

    /// Adds a persistent outbound endpoint and immediately starts connecting.
    /// The returned token remains stable across reconnects.
    #[must_use = "the token identifies the persistent outbound endpoint"]
    pub fn connect(&mut self, group: TcpGroup, peer_addr: SocketAddr) -> Token {
        self.state.connect(group, peer_addr, None)
    }

    /// Like [`Self::connect`] but negotiates client TLS once the socket
    /// connects. [`TcpEvent::Connected`] is emitted after the handshake, and
    /// every payload is encrypted; framing applies inside the session.
    #[must_use = "the token identifies the persistent outbound endpoint"]
    pub fn connect_tls(&mut self, group: TcpGroup, peer_addr: SocketAddr, tls: Session) -> Token {
        self.state.connect(group, peer_addr, Some(Box::new(tls)))
    }

    /// Serializes and sends one payload to a connected token. Length-prefixed
    /// groups add a frame header; raw groups send the payload unchanged. The
    /// closure is not called when the token is unknown or currently
    /// disconnected.
    pub fn send_with<F>(&mut self, token: Token, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        self.state.send_with(token, serialise)
    }

    /// Serializes and sends multiple payloads to a connected token.
    /// Length-prefixed groups preserve each payload as a separate frame and
    /// share one send timestamp. Raw groups concatenate the payloads. The
    /// batch uses one socket write when no backlog exists. Each payload is
    /// checked against `max_frame_size`. The caller must bound the item count
    /// or total batch size. `max_backlog_bytes` only limits bytes queued after
    /// a partial write. Invalid payloads are skipped. The closure is not called
    /// when the token is unknown or disconnected.
    pub fn send_many_with<I, F>(&mut self, token: Token, items: I, serialise: F) -> bool
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        self.state.send_many_with(token, items, serialise)
    }

    /// Serializes one payload and sends it to every connected member of
    /// `group`. Length-prefixed groups add a frame header; raw groups send
    /// the payload unchanged. Returns the number of recipients attempted.
    pub fn broadcast_with<F>(&mut self, group: TcpGroup, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        self.state.broadcast_with(group, serialise)
    }

    /// Serializes multiple payloads once and sends the batch to every
    /// connected member of `group`. Framing, size limits, and skipping of
    /// invalid payloads follow [`Self::send_many_with`]; each member
    /// receives the batch in one socket write when it has no backlog. The
    /// closure is not called when the group has no connected member. Returns
    /// the number of recipients attempted.
    pub fn broadcast_many_with<I, F>(&mut self, group: TcpGroup, items: I, serialise: F) -> usize
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        self.state.broadcast_many_with(group, items, serialise)
    }

    /// Closes a connection. Persistent outbound endpoints remain registered
    /// and will reconnect; accepted connections are removed. Returns whether
    /// the token identified an active socket.
    pub fn disconnect(&mut self, token: Token) -> bool {
        self.state.disconnect(token)
    }

    /// Closes a connected socket after its queued bytes have been written.
    /// Returns `false` for unknown or disconnected tokens; sends to a draining
    /// token are rejected. A peer that never drains is bounded only by
    /// `TCP_USER_TIMEOUT`.
    pub fn disconnect_when_drained(&mut self, token: Token) -> bool {
        self.state.disconnect_when_drained(token)
    }

    /// Permanently removes a connection or outbound endpoint. Returns whether
    /// the token was found.
    pub fn remove(&mut self, token: Token) -> bool {
        self.state.remove(token)
    }
}

/// A TCP network whose sockets are registered with a poll owned by the caller.
///
/// A polling pass consists of [`Self::pre_poll`], polling, calling
/// [`Self::handle_event`] for each event in this network's token range, and
/// [`Self::post_poll`]. The caller owns the poll, event buffer, and timeout.
/// Dropping this value attempts to deregister its sockets before closing them.
///
/// # Example
///
/// ```no_run
/// use std::time::Duration;
///
/// use flux_network::tcp::{TcpEvent, TcpGroupConfig, TcpNetworkWithExternalPoll};
/// use mio::{Events, Poll};
///
/// // Every source sharing the poll must use non-overlapping token range.
/// const NETWORK_TOKENS: std::ops::Range<usize> = (1 << 48)..(2 << 48);
///
/// let mut poll = Poll::new().unwrap();
/// let mut events = Events::with_capacity(128);
/// let mut network = TcpNetworkWithExternalPoll::new(
///     poll.registry().try_clone().unwrap(),
///     NETWORK_TOKENS,
/// );
/// let group = network.add_group(TcpGroupConfig::default());
/// network.listen(group, "127.0.0.1:9099".parse().unwrap()).unwrap();
///
/// let mut handle_tcp_event = |_event: TcpEvent<'_>| {
///     // Process the event here. Copy payload bytes if they must outlive the
///     // callback.
/// };
///
/// loop {
///     network.pre_poll(&mut handle_tcp_event);
///     poll.poll(&mut events, Some(Duration::from_millis(1))).unwrap();
///     for event in &events {
///         if NETWORK_TOKENS.contains(&event.token().0) {
///             network.handle_event(event, &mut handle_tcp_event);
///         } else {
///             // Route the event to another source registered with this poll.
///         }
///     }
///     network.post_poll(&mut handle_tcp_event);
/// }
/// ```
pub struct TcpNetworkWithExternalPoll {
    core: TcpNetworkCore,
}

impl Deref for TcpNetworkWithExternalPoll {
    type Target = TcpNetworkCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl DerefMut for TcpNetworkWithExternalPoll {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
}

impl TcpNetworkWithExternalPoll {
    /// Creates a network that assigns listener and connection tokens from
    /// `tokens` in ascending order without reuse.
    ///
    /// An operation that needs a new token panics if the range is exhausted.
    /// The caller is responsible for avoiding collisions with other tokens
    /// registered with the same poll.
    pub fn new(registry: Registry, tokens: Range<usize>) -> Self {
        Self { core: TcpNetworkCore::new(registry, tokens) }
    }

    /// Delivers pending disconnect notifications and attempts due reconnects.
    pub fn pre_poll<F>(&mut self, handler: &mut F)
    where
        F: for<'a> FnMut(TcpEvent<'a>),
    {
        self.core.state.drain_pending_disconnects(handler);
        self.core.state.maybe_reconnect();
    }

    /// Delivers pending disconnect notifications.
    ///
    /// If this step is skipped, notifications remain queued for a later call
    /// to this method or [`Self::pre_poll`].
    pub fn post_poll<F>(&mut self, handler: &mut F)
    where
        F: for<'a> FnMut(TcpEvent<'a>),
    {
        self.core.state.drain_pending_disconnects(handler);
    }

    /// Processes one readiness event routed to this network.
    ///
    /// The event token must be in the range supplied to [`Self::new`]. This is
    /// checked by an assertion in debug builds.
    pub fn handle_event<F>(&mut self, event: &Event, handler: &mut F)
    where
        F: for<'a> FnMut(TcpEvent<'a>),
    {
        debug_assert!(
            self.core.state.token_range.contains(&event.token().0),
            "event token {:?} lies outside this network's token range {:?}",
            event.token(),
            self.core.state.token_range
        );
        self.core.state.handle_event(event, handler);
    }
}

/// Reads plaintext from `socket`, decrypting through `tls` when present.
fn read_plaintext(
    socket: &mut mio::net::TcpStream,
    tls: &mut Option<Box<Session>>,
    buf: &mut [u8],
    read_syscalls: &mut u64,
) -> io::Result<usize> {
    *read_syscalls += 1;
    match tls {
        Some(session) => session.read_plain(socket, buf),
        None => socket.read(buf),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamState {
    Alive,
    Disconnected,
}

enum ReadOutcome<'a> {
    Message { payload: &'a [u8], send_ts: Nanos },
    WouldBlock,
    Disconnected,
}

/// Bytes read from one connection. Length-prefixed framing keeps unparsed
/// read-ahead in `bytes[head..tail]` across `poll_with` calls, so it must stay
/// per connection; raw framing uses `bytes` as scratch.
struct RxBuffer {
    bytes: Vec<u8>,
    head: usize,
    tail: usize,
}

/// Result of [`RxBuffer::next_frame`].
enum NextFrame {
    /// A whole frame was buffered and consumed; its payload is at
    /// `bytes[start..start + length]`.
    Ready { start: usize, length: usize, send_ts: Nanos },
    /// The frame at `head` is not all buffered. `frame_len` is its length, or
    /// the header's while that is incomplete.
    Incomplete { frame_len: usize },
    /// The header names a payload past `max_frame_size`.
    PayloadTooLarge(usize),
}

impl RxBuffer {
    fn pending(&self) -> usize {
        self.tail - self.head
    }

    /// Consumes the frame at the cursor if it is all here. Keepalives are
    /// skipped.
    fn next_frame(&mut self, max_frame_size: usize) -> NextFrame {
        loop {
            if self.pending() < FRAME_HEADER_SIZE {
                return NextFrame::Incomplete { frame_len: FRAME_HEADER_SIZE };
            }
            let at = self.head;
            let header = &self.bytes[at..at + FRAME_HEADER_SIZE];
            let length = frame_payload_len(header);
            let send_ts = frame_send_ts(header);

            // Checked before the length sizes any allocation.
            if max_frame_size < length {
                return NextFrame::PayloadTooLarge(length);
            }
            if length == 0 {
                self.head = at + FRAME_HEADER_SIZE;
                continue;
            }
            let frame_len = FRAME_HEADER_SIZE + length;
            if frame_len <= self.pending() {
                let start = at + FRAME_HEADER_SIZE;
                self.head = start + length;
                return NextFrame::Ready { start, length, send_ts };
            }
            return NextFrame::Incomplete { frame_len };
        }
    }

    /// Makes room for a `frame_len`-byte frame from `head`, and for at least
    /// one byte to read into: an empty read buffer returns `Ok(0)`, which
    /// reads as a closed peer.
    fn make_room(&mut self, frame_len: usize) {
        if self.head == self.tail {
            self.head = 0;
            self.tail = 0;
        }
        if frame_len <= self.bytes.len() - self.head && self.tail < self.bytes.len() {
            return;
        }
        if self.head != 0 {
            self.bytes.copy_within(self.head..self.tail, 0);
            self.tail -= self.head;
            self.head = 0;
        }
        if self.bytes.len() < frame_len || self.tail == self.bytes.len() {
            self.bytes.resize(frame_len.max(INITIAL_RX_BUFFER_SIZE).max(self.tail + 1), 0);
        }
    }
}

#[derive(Default)]
struct ByteQueue {
    bytes: Vec<u8>,
    head: usize,
    queued_since: Option<Instant>,
    last_warning: Option<Instant>,
}

impl ByteQueue {
    fn is_empty(&self) -> bool {
        self.head == self.bytes.len()
    }

    fn len(&self) -> usize {
        self.bytes.len() - self.head
    }

    fn remaining(&self) -> &[u8] {
        &self.bytes[self.head..]
    }

    fn would_exceed(&self, additional: usize, max: usize) -> bool {
        self.len().checked_add(additional).is_none_or(|total| total > max)
    }

    fn consume(&mut self, bytes: usize) {
        self.head += bytes;
        if self.is_empty() {
            self.bytes.clear();
            self.head = 0;
            self.queued_since = None;
            self.last_warning = None;
        }
    }

    fn append_frame_remainder(
        &mut self,
        header: &[u8; FRAME_HEADER_SIZE],
        payload: &[u8],
        written: usize,
    ) -> bool {
        let frame_len = FRAME_HEADER_SIZE + payload.len();
        if written >= frame_len {
            flux_utils::safe_assert!(written < frame_len);
            return false;
        }
        if written < FRAME_HEADER_SIZE {
            self.append_remainder(&header[written..], payload)
        } else {
            self.append_remainder(&[], &payload[written - FRAME_HEADER_SIZE..])
        }
    }

    fn append_raw_remainder(&mut self, payload: &[u8], written: usize) -> bool {
        if written >= payload.len() {
            flux_utils::safe_assert!(written < payload.len());
            return false;
        }
        self.append_remainder(&[], &payload[written..])
    }

    fn append_remainder(&mut self, prefix: &[u8], payload: &[u8]) -> bool {
        let additional = prefix.len() + payload.len();
        let old_capacity = self.bytes.capacity();

        if self.head != 0 && self.bytes.capacity() - self.bytes.len() < additional {
            let remaining = self.len();
            self.bytes.copy_within(self.head.., 0);
            self.bytes.truncate(remaining);
            self.head = 0;
        }
        self.bytes.reserve(additional);
        self.bytes.extend_from_slice(prefix);
        self.bytes.extend_from_slice(payload);
        self.queued_since.get_or_insert_with(Instant::now);
        self.bytes.capacity() != old_capacity
    }

    fn maybe_warn(&mut self, config: &TcpGroupConfig, token: Token, peer_addr: SocketAddr) {
        let Some(threshold) = config.backlog_warn_bytes else { return };
        if self.len() <= threshold {
            self.last_warning = None;
            return;
        }
        if self
            .last_warning
            .is_some_and(|last| last.elapsed() < Duration::from_secs(BACKLOG_WARNING_INTERVAL_SECS))
        {
            return;
        }
        let age = self.queued_since.map_or(Duration::ZERO, |since| since.elapsed());
        warn!(
            group = config.name,
            ?token,
            %peer_addr,
            queued_bytes = self.len(),
            %age,
            "tcp send backlog growing"
        );
        self.last_warning = Some(Instant::now());
    }
}

struct FramedStream {
    socket: mio::net::TcpStream,
    token: Token,
    peer_addr: SocketAddr,
    rx_buffer: RxBuffer,
    send_queue: ByteQueue,
    writable_armed: bool,
}

impl FramedStream {
    fn new(
        socket: mio::net::TcpStream,
        token: Token,
        peer_addr: SocketAddr,
        framing: Framing,
        max_frame_size: usize,
    ) -> Self {
        // Raw reads straight into `bytes`; length-prefixed sizes it on demand.
        let rx_len = match framing {
            Framing::Raw => INITIAL_RX_BUFFER_SIZE.min(max_frame_size),
            Framing::LengthPrefixed => 0,
        };
        Self {
            socket,
            token,
            peer_addr,
            rx_buffer: RxBuffer { bytes: vec![0; rx_len], head: 0, tail: 0 },
            send_queue: ByteQueue::default(),
            writable_armed: false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn poll_with<F>(
        &mut self,
        registry: &Registry,
        event: &Event,
        config: &TcpGroupConfig,
        timers: &mut Option<NetworkTimers>,
        tls: &mut Option<Box<Session>>,
        read_syscalls: &mut u64,
        on_message: &mut F,
    ) -> StreamState
    where
        F: for<'a> FnMut(&'a [u8], Nanos),
    {
        if event.is_readable() {
            if config.framing == Framing::Raw {
                loop {
                    match read_plaintext(
                        &mut self.socket,
                        tls,
                        &mut self.rx_buffer.bytes,
                        read_syscalls,
                    ) {
                        Ok(0) => return StreamState::Disconnected,
                        Ok(read) => on_message(&self.rx_buffer.bytes[..read], Nanos::now()),
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                        Err(err) => {
                            debug!(?err, %self.peer_addr, "tcp raw read failed");
                            return StreamState::Disconnected;
                        }
                    }
                }
            } else {
                loop {
                    match self.read_frame(config.max_frame_size, tls, read_syscalls) {
                        ReadOutcome::Message { payload, send_ts } => {
                            if let Some(timers) = timers {
                                if let Some(latency) = &mut timers.latency {
                                    latency.emit_latency_from_nanos(send_ts, Nanos::now());
                                }
                            }
                            on_message(payload, send_ts);
                        }
                        ReadOutcome::WouldBlock => break,
                        ReadOutcome::Disconnected => return StreamState::Disconnected,
                    }
                }
            }
            // Reads drive the handshake; flush whatever it wants to answer.
            if let Some(session) = tls.as_mut() {
                let mut pending = Vec::new();
                session.take_pending(&mut pending);
                if !pending.is_empty() &&
                    self.write_frame(registry, None, &pending, config, timers) ==
                        StreamState::Disconnected
                {
                    return StreamState::Disconnected;
                }
            }
        }
        if event.is_writable() && self.drain_queue(registry, config) == StreamState::Disconnected {
            return StreamState::Disconnected;
        }
        if event.is_error() || event.is_read_closed() || event.is_write_closed() {
            return StreamState::Disconnected;
        }
        StreamState::Alive
    }

    /// Returns the next buffered frame, reading from the socket only when none
    /// is buffered. `WouldBlock` therefore means nothing is left in either,
    /// which edge-triggered readiness requires.
    fn read_frame(
        &mut self,
        max_frame_size: usize,
        tls: &mut Option<Box<Session>>,
        read_syscalls: &mut u64,
    ) -> ReadOutcome<'_> {
        loop {
            let frame_len = match self.rx_buffer.next_frame(max_frame_size) {
                NextFrame::Ready { start, length, send_ts } => {
                    return ReadOutcome::Message {
                        payload: &self.rx_buffer.bytes[start..start + length],
                        send_ts,
                    };
                }
                NextFrame::PayloadTooLarge(length) => {
                    warn!(
                        %self.peer_addr,
                        payload_len = length,
                        max_frame_size,
                        "tcp frame exceeds configured maximum"
                    );
                    return ReadOutcome::Disconnected;
                }
                NextFrame::Incomplete { frame_len } => frame_len,
            };

            self.rx_buffer.make_room(frame_len);
            let tail = self.rx_buffer.tail;
            match read_plaintext(
                &mut self.socket,
                tls,
                &mut self.rx_buffer.bytes[tail..],
                read_syscalls,
            ) {
                Ok(0) => return ReadOutcome::Disconnected,
                Ok(read) => self.rx_buffer.tail += read,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    return ReadOutcome::WouldBlock;
                }
                Err(err) => {
                    debug!(?err, %self.peer_addr, "tcp frame read failed");
                    return ReadOutcome::Disconnected;
                }
            }
        }
    }

    fn write_frame(
        &mut self,
        registry: &Registry,
        header: Option<&[u8; FRAME_HEADER_SIZE]>,
        payload: &[u8],
        config: &TcpGroupConfig,
        timers: &mut Option<NetworkTimers>,
    ) -> StreamState {
        if !self.send_queue.is_empty() {
            if self.drain_queue(registry, config) == StreamState::Disconnected {
                return StreamState::Disconnected;
            }
            if !self.send_queue.is_empty() {
                return self.enqueue_remainder(registry, header, payload, 0, config, timers);
            }
        }

        let result = if let Some(header) = header {
            self.socket.write_vectored(&[IoSlice::new(header.as_slice()), IoSlice::new(payload)])
        } else {
            self.socket.write(payload)
        };
        let total = header.map_or(payload.len(), |_| FRAME_HEADER_SIZE + payload.len());
        match result {
            Ok(0) => StreamState::Disconnected,
            Ok(written) if written == total => StreamState::Alive,
            Ok(written) => {
                self.enqueue_remainder(registry, header, payload, written, config, timers)
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                self.enqueue_remainder(registry, header, payload, 0, config, timers)
            }
            Err(err) => {
                debug!(?err, %self.peer_addr, "tcp frame write failed");
                StreamState::Disconnected
            }
        }
    }

    fn enqueue_remainder(
        &mut self,
        registry: &Registry,
        header: Option<&[u8; FRAME_HEADER_SIZE]>,
        payload: &[u8],
        written: usize,
        config: &TcpGroupConfig,
        timers: &mut Option<NetworkTimers>,
    ) -> StreamState {
        let total = header.map_or(payload.len(), |_| FRAME_HEADER_SIZE + payload.len());
        if written >= total {
            flux_utils::safe_assert!(written < total);
            return StreamState::Disconnected;
        }
        let additional = total - written;
        if let Some(max) = config.max_backlog_bytes &&
            self.send_queue.would_exceed(additional, max)
        {
            warn!(
                group = config.name,
                ?self.token,
                %self.peer_addr,
                queued_bytes = self.send_queue.len(),
                additional_bytes = additional,
                max_backlog_bytes = max,
                "tcp send backlog would exceed configured maximum"
            );
            return StreamState::Disconnected;
        }

        let started = Nanos::now();
        let allocated = if let Some(header) = header {
            self.send_queue.append_frame_remainder(header, payload, written)
        } else {
            self.send_queue.append_raw_remainder(payload, written)
        };
        if allocated && let Some(timers) = timers {
            timers.alloc.emit_latency_from_nanos(started, Nanos::now());
        }
        self.send_queue.maybe_warn(config, self.token, self.peer_addr);
        self.arm_writable(registry)
    }

    fn drain_queue(&mut self, registry: &Registry, config: &TcpGroupConfig) -> StreamState {
        while !self.send_queue.is_empty() {
            match self.socket.write(self.send_queue.remaining()) {
                Ok(0) => return StreamState::Disconnected,
                Ok(written) => self.send_queue.consume(written),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => {
                    debug!(?err, %self.peer_addr, "tcp backlog write failed");
                    return StreamState::Disconnected;
                }
            }
        }
        self.send_queue.maybe_warn(config, self.token, self.peer_addr);
        if self.send_queue.is_empty() && self.writable_armed {
            if let Err(err) = registry.reregister(&mut self.socket, self.token, Interest::READABLE)
            {
                debug!(?err, %self.peer_addr, "couldn't disarm writable interest");
                return StreamState::Disconnected;
            }
            self.writable_armed = false;
        }
        StreamState::Alive
    }

    fn arm_writable(&mut self, registry: &Registry) -> StreamState {
        if self.writable_armed {
            return StreamState::Alive;
        }
        if let Err(err) = registry.reregister(
            &mut self.socket,
            self.token,
            Interest::READABLE | Interest::WRITABLE,
        ) {
            debug!(?err, %self.peer_addr, "couldn't arm writable interest");
            return StreamState::Disconnected;
        }
        self.writable_armed = true;
        StreamState::Alive
    }

    fn close(&mut self, registry: &Registry) {
        let _ = registry.deregister(&mut self.socket);
        let _ = self.socket.shutdown(Shutdown::Both);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        net::{Ipv4Addr, TcpListener, TcpStream as StdTcpStream},
    };

    use flux_timing::Nanos;
    use mio::{Poll, Token};

    use super::{
        ByteQueue, FRAME_HEADER_SIZE, FramedStream, Framing, PayloadBuf, StreamState,
        TcpGroupConfig, set_socket_buf_size, write_frame_header,
    };

    #[test]
    fn byte_queue_preserves_every_unwritten_suffix() {
        let header = [1; FRAME_HEADER_SIZE];
        let payload = [2; 16];
        let mut frame = header.to_vec();
        frame.extend_from_slice(&payload);

        for written in [0, 3, FRAME_HEADER_SIZE, FRAME_HEADER_SIZE + 7] {
            let mut queue = ByteQueue::default();
            queue.append_frame_remainder(&header, &payload, written);
            assert_eq!(queue.remaining(), &frame[written..]);
        }
    }

    #[test]
    fn byte_queue_preserves_raw_unwritten_suffix() {
        let payload = [2; 16];

        for written in [0, 3, 7] {
            let mut queue = ByteQueue::default();
            queue.append_raw_remainder(&payload, written);
            assert_eq!(queue.remaining(), &payload[written..]);
        }
    }

    #[test]
    fn byte_queue_compacts_consumed_prefix_before_growing() {
        let first_header = [1; FRAME_HEADER_SIZE];
        let first_payload = [2; 32];
        let second_header = [3; FRAME_HEADER_SIZE];
        let second_payload = [4; 16];
        let mut queue = ByteQueue::default();

        queue.append_frame_remainder(&first_header, &first_payload, 0);
        queue.bytes.shrink_to_fit();
        queue.consume(20);
        queue.append_frame_remainder(&second_header, &second_payload, 0);

        let mut expected = first_header.to_vec();
        expected.extend_from_slice(&first_payload);
        expected = expected[20..].to_vec();
        expected.extend_from_slice(&second_header);
        expected.extend_from_slice(&second_payload);
        assert_eq!(queue.remaining(), expected);
        assert_eq!(queue.head, 0);
    }

    #[test]
    fn byte_queue_checks_hard_limit_without_overflowing() {
        let mut queue = ByteQueue::default();
        let header = [1; FRAME_HEADER_SIZE];
        queue.append_frame_remainder(&header, &[2; 4], 0);

        assert!(!queue.would_exceed(8, FRAME_HEADER_SIZE + 12));
        assert!(queue.would_exceed(9, FRAME_HEADER_SIZE + 12));
        assert!(queue.would_exceed(usize::MAX, usize::MAX));
    }

    #[test]
    fn hard_limit_disconnects_when_queue_is_already_backed_up() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let client = StdTcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (_peer, peer_addr) = listener.accept().unwrap();
        client.set_nonblocking(true).unwrap();

        let socket = mio::net::TcpStream::from_std(client);
        set_socket_buf_size(&socket, 1024);
        let mut stream =
            FramedStream::new(socket, Token(0), peer_addr, Framing::LengthPrefixed, 1024);
        let fill = [0; 4096];
        loop {
            match stream.socket.write(&fill) {
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => panic!("failed to fill socket send buffer: {err}"),
            }
        }
        stream.send_queue.bytes.extend_from_slice(&[1; 8]);

        let config = TcpGroupConfig {
            backlog_warn_bytes: None,
            max_backlog_bytes: Some(16),
            ..Default::default()
        };
        let mut header = [0; FRAME_HEADER_SIZE];
        let payload = [2; 8];
        write_frame_header(&mut header, payload.len(), Nanos::now());

        assert_eq!(
            stream.write_frame(
                Poll::new().unwrap().registry(),
                Some(&header),
                &payload,
                &config,
                &mut None
            ),
            StreamState::Disconnected
        );
        assert_eq!(stream.send_queue.len(), 8);
    }

    #[test]
    fn payload_buf_truncate_clamps_to_its_own_frame() {
        let mut bytes = b"earlier".to_vec();
        let mut payload = PayloadBuf::new(&mut bytes);
        payload.extend_from_slice(b"payload");

        payload.truncate(usize::MAX);
        assert_eq!(payload.as_slice(), b"payload");
        payload.truncate(3);
        assert_eq!(payload.as_slice(), b"pay");
        payload.truncate(0);
        assert!(payload.is_empty());
        assert_eq!(bytes, b"earlier");
    }

    #[test]
    fn payload_buf_resize_and_clear_stay_relative() {
        let mut bytes = b"earlier".to_vec();
        let mut payload = PayloadBuf::new(&mut bytes);
        payload.resize(3, 7);
        assert_eq!(payload.as_slice(), &[7, 7, 7]);
        payload.resize(1, 0);
        assert_eq!(payload.as_slice(), &[7]);
        payload.clear();
        assert!(payload.is_empty());
        assert_eq!(bytes, b"earlier");
    }

    #[test]
    #[should_panic(expected = "payload length overflows usize")]
    fn payload_buf_resize_rejects_overflow() {
        let mut bytes = b"earlier".to_vec();
        let mut payload = PayloadBuf::new(&mut bytes);
        payload.resize(usize::MAX, 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn drop_removes_listener_and_endpoint_registrations() {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

        use flux_timing::{Duration, Repeater};

        use super::{GroupState, NetworkState, TcpGroup};

        fn registered_fds(epoll_fd: i32) -> usize {
            std::fs::read_to_string(format!("/proc/self/fdinfo/{epoll_fd}"))
                .unwrap()
                .lines()
                .filter(|line| line.starts_with("tfd:"))
                .count()
        }

        let poll = Poll::new().unwrap();
        let epoll_fd = poll.as_raw_fd();
        let registry = poll.registry().try_clone().unwrap();
        let mut state = NetworkState::new(registry, 100..200);
        state.groups.push(GroupState {
            config: TcpGroupConfig::default(),
            reconnector: Repeater::every(Duration::from_secs(2)),
        });
        let group = TcpGroup(0);
        state.listen(group, (Ipv4Addr::LOCALHOST, 0).into()).unwrap();
        let addr = state.listeners[0].socket.local_addr().unwrap();
        let _endpoint = state.connect(group, addr, None);
        assert_eq!(registered_fds(epoll_fd), 2);

        // Keep the listener's file description alive after its mio socket drops.
        // SAFETY: the listener owns this descriptor for the duration of the call.
        let dup_fd = unsafe { libc::dup(state.listeners[0].socket.as_raw_fd()) };
        assert!(dup_fd >= 0);
        // SAFETY: dup_fd is valid after the check above, and ownership is
        // transferred exactly once.
        let _dup = unsafe { OwnedFd::from_raw_fd(dup_fd) };

        drop(state);
        assert_eq!(registered_fds(epoll_fd), 0);
    }
}
