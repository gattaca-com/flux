use std::{
    io::{self, IoSlice, Read, Write},
    net::Shutdown,
    ops::{Deref, DerefMut},
};

use flux_communication::Timer;
use flux_timing::{Duration, Instant, Nanos, Repeater};
use mio::{Events, Interest, Poll, Registry, Token, Waker, event::Event};
use tracing::{debug, error, info, warn};

use super::{
    Endpoint, Peer, ServiceRef, TcpTelemetry, set_socket_buf_size,
    tcp_stream::{
        DEFAULT_TCP_USER_TIMEOUT_MS, FRAME_HEADER_SIZE, frame_payload_len, write_frame_header,
        write_frame_len, write_frame_ts,
    },
    transport::{ListenSocket, TransportStream},
};

const EVENTS_CAPACITY: usize = 128;
const INITIAL_CONNECTION_CAPACITY: usize = 8;
const INITIAL_GROUP_CAPACITY: usize = 4;
const INITIAL_LISTENER_CAPACITY: usize = 2;
const INITIAL_RX_BUFFER_SIZE: usize = 32 * 1024;
const INITIAL_SEND_BUFFER_SIZE: usize = 32 * 1024;
const DEFAULT_MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_BACKLOG_WARN_BYTES: usize = 64 * 1024 * 1024;
const BACKLOG_WARNING_INTERVAL_SECS: u64 = 10;
// Reserved for Owned-mode wakeups; allocation stops before it.
const WAKER_TOKEN: Token = Token(usize::MAX);
const EXTERNAL_POLLS: &str =
    "poll this external network yourself, then call next_deadline, handle_event and tick";
const OWNED_POLLS: &str = "this network polls itself: use StreamNetwork::drive";

/// Identifies a set of connections using the same application protocol and
/// socket configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConnectionGroup(usize);

/// Selects how a group encodes messages on the wire.
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

/// Socket options that exist only for TCP connections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpOptions {
    /// Whether to enable `TCP_NODELAY`.
    pub nodelay: bool,
    /// Whether to enable TCP keepalive.
    pub keepalive: bool,
    /// Linux `TCP_USER_TIMEOUT`, in milliseconds.
    pub user_timeout_ms: u32,
}

impl Default for TcpOptions {
    fn default() -> Self {
        Self { nodelay: true, keepalive: false, user_timeout_ms: DEFAULT_TCP_USER_TIMEOUT_MS }
    }
}

/// Configuration shared by every listener and connection in a
/// [`ConnectionGroup`].
#[derive(Clone)]
pub struct ConnectionGroupConfig {
    /// Stable label used in logs and telemetry.
    pub name: &'static str,
    /// Static payload sent on every newly established connection before its
    /// lifecycle event is emitted.
    pub on_connect_msg: Option<Vec<u8>>,
    /// Requested `SO_SNDBUF` and `SO_RCVBUF` size, applied to every transport.
    pub socket_buf_size: Option<usize>,
    /// Retry interval for persistent outbound endpoints.
    pub reconnect_interval: Duration,
    /// Emit rate-limited warnings above this many queued bytes. The queue is
    /// allowed to continue growing.
    pub backlog_warn_bytes: Option<usize>,
    /// Disconnect the peer before its queued bytes would exceed this limit.
    /// `None` allows the queue to grow without a hard limit.
    pub max_backlog_bytes: Option<usize>,
    /// Largest accepted or emitted frame payload. For [`Framing::Raw`], this
    /// caps a single send and bounds each received read chunk.
    pub max_frame_size: usize,
    /// Most accepted connections this group holds at once. A connection it
    /// is closing counts as one of them — draining or half-closed alike —
    /// while the outbound endpoints and listeners of the group count for
    /// nothing. `None` sets no limit. At the cap, a pending connection is
    /// accepted and dropped where it stands, without registration, bytes or
    /// an event, and counted by [`StreamNetwork::refused_connections`].
    pub max_connections: Option<usize>,
    /// Wire encoding used by this group.
    pub framing: Framing,
    /// Per-connection latency and allocation telemetry.
    pub telemetry: TcpTelemetry,
    /// TCP socket options. Unix-domain connections have no such options and
    /// ignore them.
    pub tcp: TcpOptions,
}

impl Default for ConnectionGroupConfig {
    fn default() -> Self {
        Self {
            name: "stream",
            on_connect_msg: None,
            socket_buf_size: None,
            reconnect_interval: Duration::from_secs(2),
            backlog_warn_bytes: Some(DEFAULT_BACKLOG_WARN_BYTES),
            max_backlog_bytes: None,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_connections: None,
            framing: Framing::LengthPrefixed,
            telemetry: TcpTelemetry::Disabled,
            tcp: TcpOptions::default(),
        }
    }
}

impl ConnectionGroupConfig {
    /// Enables TCP keepalive for every connection in this group.
    pub fn with_keepalive(mut self) -> Self {
        self.tcp.keepalive = true;
        self
    }
}

/// Event emitted by [`StreamNetwork::drive`].
pub enum StreamEvent<'a> {
    /// A listener accepted a new connection.
    Accepted { group: ConnectionGroup, token: Token, peer: Peer },
    /// A persistent outbound endpoint established a connection.
    Connected { group: ConnectionGroup, token: Token, peer: Peer },
    /// A complete length-prefixed message or a raw read chunk was received.
    /// For raw-framed groups, chunks do not preserve message boundaries and
    /// `send_ts` is the local receive time.
    Message { group: ConnectionGroup, token: Token, payload: &'a [u8], send_ts: Nanos },
    /// An established connection was closed.
    Disconnected { group: ConnectionGroup, token: Token, peer: Peer },
}

impl StreamEvent<'_> {
    /// The group whose service, or whose unclaimed handler, this event is for.
    pub(crate) fn group(&self) -> ConnectionGroup {
        match *self {
            Self::Accepted { group, .. } |
            Self::Connected { group, .. } |
            Self::Message { group, .. } |
            Self::Disconnected { group, .. } => group,
        }
    }
}

struct GroupState {
    config: ConnectionGroupConfig,
    reconnector: Repeater,
    /// Connections refused since the group was added because it was at its
    /// cap, and when the last of them was warned about.
    refused: u64,
    last_refusal_warning: Option<Instant>,
}

struct Listener {
    token: Token,
    group: ConnectionGroup,
    socket: ListenSocket,
}

#[allow(clippy::large_enum_variant)]
enum ConnectionState {
    Disconnected,
    Connecting(TransportStream),
    Connected(FramedStream),
}

struct Connection {
    token: Token,
    group: ConnectionGroup,
    peer: Peer,
    /// The endpoint a persistent outbound connection reconnects to; `None`
    /// for a connection the network accepted.
    endpoint: Option<Endpoint>,
    state: ConnectionState,
    close_when_drained: bool,
    /// How far the write side has got toward the half-close a caller asked
    /// for.
    write_side: WriteSide,
    timers: Option<NetworkTimers>,
}

/// The write side of a connected socket: what the half-close requested
/// through [`StreamNetwork::shutdown_write_when_drained`] is waiting for, and
/// whether it has happened.
#[derive(Clone, Copy, PartialEq, Eq)]
enum WriteSide {
    /// Open, and carrying whatever the caller sends.
    Open,
    /// To be shut as soon as the queued bytes reach the peer. Sends are
    /// refused from the request onward, so the queue only shrinks.
    ShutWhenDrained,
    /// Shut: the peer can read the end of the stream, and what it sends
    /// still arrives.
    Shut,
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
        peer: Peer,
        framing: Framing,
    ) -> Option<Self> {
        let TcpTelemetry::Enabled { app_name } = telemetry else { return None };
        let label = format!("{group_name}-{}-{peer}", token.0);
        Some(Self {
            latency: (framing == Framing::LengthPrefixed)
                .then(|| Timer::new(app_name, format!("tcp_latency_{label}"))),
            alloc: Timer::new(app_name, format!("tcp_alloc_{label}")),
        })
    }
}

#[derive(Clone, Copy)]
struct PendingDisconnect {
    group: ConnectionGroup,
    token: Token,
    peer: Peer,
}

/// Everything the network holds that never polls: it registers its sockets on
/// the registry it was given, whoever owns the poll behind it.
struct NetworkState {
    registry: Registry,
    groups: Vec<GroupState>,
    listeners: Vec<Listener>,
    connections: Vec<Connection>,
    pending_disconnects: Vec<PendingDisconnect>,
    /// The lowest token this network allocates. Every lower token belongs to
    /// a source the caller registered on its own poll.
    token_base: Token,
    next_token: usize,
    /// Frames staged for the next socket write, each as a contiguous
    /// `[header][payload]` for length-prefixed groups or bare bytes for raw
    /// groups.
    send_buffer: Vec<u8>,
}

impl NetworkState {
    fn new(registry: Registry, token_base: Token) -> Self {
        Self {
            registry,
            groups: Vec::with_capacity(INITIAL_GROUP_CAPACITY),
            listeners: Vec::with_capacity(INITIAL_LISTENER_CAPACITY),
            connections: Vec::with_capacity(INITIAL_CONNECTION_CAPACITY),
            pending_disconnects: Vec::with_capacity(INITIAL_CONNECTION_CAPACITY),
            token_base,
            next_token: token_base.0,
            send_buffer: Vec::with_capacity(INITIAL_SEND_BUFFER_SIZE),
        }
    }

    /// Whether `token` is one this network allocated: at or above its base,
    /// and below the high-water mark of its allocation. Tokens are never
    /// reused, so anything outside that range is a source the caller
    /// registered on its own poll — its waker included, wherever the caller
    /// put it.
    fn is_ours(&self, token: Token) -> bool {
        (self.token_base.0..self.next_token).contains(&token.0)
    }

    fn next_token(&mut self) -> Token {
        let token = Token(self.next_token);
        assert!(token != WAKER_TOKEN, "stream token space exhausted");
        self.next_token += 1;
        token
    }

    fn config(&self, group: ConnectionGroup) -> &ConnectionGroupConfig {
        &self.groups[group.0].config
    }

    fn listen(&mut self, group: ConnectionGroup, endpoint: Endpoint) -> io::Result<()> {
        if group.0 >= self.groups.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "unknown connection group"));
        }
        let mut socket = ListenSocket::bind(endpoint)?;
        let token = self.next_token();
        self.registry.register(&mut socket, token, Interest::READABLE)?;
        self.listeners.push(Listener { token, group, socket });
        Ok(())
    }

    fn connect(&mut self, group: ConnectionGroup, endpoint: Endpoint) -> Token {
        assert!(group.0 < self.groups.len(), "unknown connection group");
        let token = self.next_token();
        let peer = endpoint.peer();
        let config = self.config(group);
        let timers = NetworkTimers::new(config.telemetry, config.name, token, peer, config.framing);
        self.connections.push(Connection {
            token,
            group,
            peer,
            endpoint: Some(endpoint),
            state: ConnectionState::Disconnected,
            close_when_drained: false,
            write_side: WriteSide::Open,
            timers,
        });
        self.start_connect(self.connections.len() - 1);
        token
    }

    fn start_connect(&mut self, index: usize) {
        let connection = &self.connections[index];
        if !matches!(connection.state, ConnectionState::Disconnected) {
            return;
        }
        let Some(endpoint) = &connection.endpoint else { return };

        let token = connection.token;
        let group = connection.group;
        let peer = connection.peer;
        let socket_buf_size = self.config(group).socket_buf_size;

        let Ok(mut socket) = TransportStream::connect(endpoint)
            .inspect_err(|err| debug!(?err, %endpoint, "couldn't start connection"))
        else {
            return;
        };
        if let Some(size) = socket_buf_size {
            set_socket_buf_size(&socket, size);
        }
        if let Err(err) = self.registry.register(&mut socket, token, Interest::WRITABLE) {
            warn!(?err, %peer, "couldn't register connecting stream");
            let _ = socket.shutdown(Shutdown::Both);
            return;
        }
        self.connections[index].state = ConnectionState::Connecting(socket);
    }

    /// Hard-closes every listener, accepted connection and outbound endpoint
    /// of `group`, discarding the disconnect events that produces.
    ///
    /// Closing an [`Endpoint::Unix`] listener unlinks its socket file.
    fn close_group(&mut self, group: ConnectionGroup) {
        for index in (0..self.listeners.len()).rev() {
            if self.listeners[index].group == group {
                let mut listener = self.listeners.swap_remove(index);
                let _ = self.registry.deregister(&mut listener.socket);
            }
        }
        for index in (0..self.connections.len()).rev() {
            if self.connections[index].group == group {
                self.close_connection_socket(index);
                self.connections.swap_remove(index);
            }
        }
        self.pending_disconnects.retain(|event| event.group != group);
    }

    /// Retries every outbound endpoint whose group is due one, reporting
    /// whether an attempt was made.
    fn maybe_reconnect(&mut self, now: Instant) -> bool {
        let mut attempted = false;
        for group_index in 0..self.groups.len() {
            if !self.groups[group_index].reconnector.fired_at(now) {
                continue;
            }
            let group = ConnectionGroup(group_index);
            for index in 0..self.connections.len() {
                if self.connections[index].group == group &&
                    matches!(self.connections[index].state, ConnectionState::Disconnected)
                {
                    self.start_connect(index);
                    attempted = true;
                }
            }
        }
        attempted
    }

    /// The earliest instant the network's own timers need a call at: the next
    /// retry of a group holding an outbound endpoint that is down.
    fn next_deadline(&self) -> Option<Instant> {
        let mut next: Option<Instant> = None;
        for connection in &self.connections {
            if connection.endpoint.is_some() &&
                matches!(connection.state, ConnectionState::Disconnected)
            {
                let fire = self.groups[connection.group.0].reconnector.next_fire();
                next = Some(next.map_or(fire, |next: Instant| next.min(fire)));
            }
        }
        next
    }

    fn finish_connect<F>(&mut self, index: usize, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        let ConnectionState::Connecting(socket) = &self.connections[index].state else {
            return false;
        };
        match socket.is_connected() {
            Ok(false) => return false,
            Err(err) => {
                let peer = self.connections[index].peer;
                debug!(?err, %peer, "connection attempt failed");
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
        let peer = self.connections[index].peer;
        let mut timers = self.connections[index].timers;
        let config = self.config(group);
        let group_name = config.name;

        if config.tcp.nodelay &&
            let Err(err) = socket.set_nodelay()
        {
            warn!(?err, %peer, "couldn't set nodelay on tcp stream");
            let _ = self.registry.deregister(&mut socket);
            let _ = socket.shutdown(Shutdown::Both);
            return false;
        }
        if config.tcp.keepalive &&
            let Err(err) = socket.set_keepalive()
        {
            warn!(?err, %peer, "couldn't set keepalive on tcp stream");
            let _ = self.registry.deregister(&mut socket);
            let _ = socket.shutdown(Shutdown::Both);
            return false;
        }
        socket.set_user_timeout(config.tcp.user_timeout_ms);
        if let Err(err) = self.registry.reregister(&mut socket, token, Interest::READABLE) {
            warn!(?err, %peer, "couldn't register connected stream");
            let _ = socket.shutdown(Shutdown::Both);
            return false;
        }

        let mut stream = FramedStream::new(socket, token, peer, config.max_frame_size);
        if let Some(message) = config.on_connect_msg.as_deref() {
            let header = (config.framing == Framing::LengthPrefixed).then(|| {
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
        info!(group = group_name, %peer, "connection established");
        handler(StreamEvent::Connected { group, token, peer });
        true
    }

    fn accept_connections<F>(&mut self, listener_index: usize, handler: &mut F)
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        let group = self.listeners[listener_index].group;
        loop {
            let accepted = self.listeners[listener_index].socket.accept();
            let (mut socket, peer) = match accepted {
                Ok(accepted) => accepted,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => {
                    warn!(?err, group = self.config(group).name, "accept failed");
                    break;
                }
            };
            if let Some(max) = self.config(group).max_connections &&
                self.accepted_connections(group) >= max
            {
                // The refusal is all that happens to this connection: it
                // is never registered, never read from and never written
                // to, so the peer reads the end of the stream without ever
                // being sent a byte. The backlog still has to be drained to
                // `WouldBlock`, so the loop goes on.
                let _ = socket.shutdown(Shutdown::Both);
                self.refuse(group, peer, max);
                continue;
            }
            let token = self.next_token();
            let (stream, timers, group_name) = {
                let config = self.config(group);
                if let Some(size) = config.socket_buf_size {
                    set_socket_buf_size(&socket, size);
                }
                if config.tcp.nodelay &&
                    let Err(err) = socket.set_nodelay()
                {
                    warn!(?err, %peer, "couldn't set nodelay on accepted tcp stream");
                    let _ = socket.shutdown(Shutdown::Both);
                    continue;
                }
                if config.tcp.keepalive &&
                    let Err(err) = socket.set_keepalive()
                {
                    warn!(?err, %peer, "couldn't set keepalive on accepted tcp stream");
                    let _ = socket.shutdown(Shutdown::Both);
                    continue;
                }
                socket.set_user_timeout(config.tcp.user_timeout_ms);
                if let Err(err) = self.registry.register(&mut socket, token, Interest::READABLE) {
                    warn!(?err, %peer, "couldn't register accepted stream");
                    let _ = socket.shutdown(Shutdown::Both);
                    continue;
                }

                let mut timers =
                    NetworkTimers::new(config.telemetry, config.name, token, peer, config.framing);
                let mut stream = FramedStream::new(socket, token, peer, config.max_frame_size);
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
                peer,
                endpoint: None,
                state: ConnectionState::Connected(stream),
                close_when_drained: false,
                write_side: WriteSide::Open,
                timers,
            });
            info!(group = group_name, %peer, "connection accepted");
            handler(StreamEvent::Accepted { group, token, peer });
        }
    }

    /// How many connections `group` accepted and still holds. A connection
    /// it is closing is one of them — draining or half-closed alike — while
    /// an outbound endpoint of the group is a connection the network made
    /// rather than accepted, and counts for nothing.
    fn accepted_connections(&self, group: ConnectionGroup) -> usize {
        self.connections
            .iter()
            .filter(|connection| connection.group == group && connection.endpoint.is_none())
            .count()
    }

    /// Counts one refused connection, warning about the group at most once
    /// every [`BACKLOG_WARNING_INTERVAL_SECS`] seconds: a group at its cap
    /// refuses as often as clients arrive, and the count is what says how
    /// often that is.
    fn refuse(&mut self, group: ConnectionGroup, peer: Peer, max: usize) {
        let state = &mut self.groups[group.0];
        state.refused += 1;
        if state
            .last_refusal_warning
            .is_some_and(|last| last.elapsed() < Duration::from_secs(BACKLOG_WARNING_INTERVAL_SECS))
        {
            return;
        }
        warn!(
            group = state.config.name,
            %peer,
            max_connections = max,
            refused = state.refused,
            "refusing a connection: the group is at its connection cap"
        );
        state.last_refusal_warning = Some(Instant::now());
    }

    fn handle_event<F>(&mut self, event: &Event, handler: &mut F)
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        let token = event.token();
        if let Some(index) = self.listeners.iter().position(|listener| listener.token == token) {
            self.accept_connections(index, handler);
            return;
        }
        let Some(index) = self.connections.iter().position(|connection| connection.token == token)
        else {
            debug!(?token, "ignoring stale readiness event");
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
        let peer = self.connections[index].peer;
        let config = &self.groups[group.0].config;
        let (state, queue_empty) = {
            let connection = &mut self.connections[index];
            let ConnectionState::Connected(stream) = &mut connection.state else { unreachable!() };
            let state = stream.poll_with(
                &self.registry,
                event,
                config,
                &mut connection.timers,
                &mut |payload, send_ts| {
                    handler(StreamEvent::Message { group, token, payload, send_ts });
                },
            );
            (state, stream.send_queue.is_empty())
        };
        if state == StreamState::Disconnected {
            handler(StreamEvent::Disconnected { group, token, peer });
            self.disconnect_index(index, false);
        } else if queue_empty {
            // A connection asked for both closes outright: a peer about to
            // lose the connection gains nothing from reading the end of the
            // stream first.
            if self.connections[index].close_when_drained {
                self.disconnect_index(index, true);
            } else if self.connections[index].write_side == WriteSide::ShutWhenDrained {
                self.shut_write(index);
            }
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
            peer: self.connections[index].peer,
        };
        let accepted = self.connections[index].endpoint.is_none();
        self.connections[index].close_when_drained = false;
        self.connections[index].write_side = WriteSide::Open;
        let was_connected = self.close_connection_socket(index);
        if accepted {
            self.connections.swap_remove(index);
        }
        if notify && was_connected {
            self.pending_disconnects.push(event);
        }
    }

    fn drain_pending_disconnects<F>(&mut self, handler: &mut F)
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        for event in self.pending_disconnects.drain(..) {
            handler(StreamEvent::Disconnected {
                group: event.group,
                token: event.token,
                peer: event.peer,
            });
        }
    }

    /// Finds the connection `token` can currently send on.
    fn sendable_index(&self, token: Token) -> Option<usize> {
        self.connections.iter().position(|connection| {
            connection.token == token &&
                !connection.close_when_drained &&
                connection.write_side == WriteSide::Open &&
                matches!(connection.state, ConnectionState::Connected(_))
        })
    }

    /// Serialises one payload as a frame at the end of `send_buffer`.
    /// Length-prefixed groups reserve a header ahead of the payload and fill
    /// in its length here; `stamp_frames` writes the timestamp just before the
    /// socket write. Empty or oversized payloads are removed again. Returns
    /// whether the frame was kept.
    fn append_frame<F>(&mut self, group: ConnectionGroup, serialise: F) -> bool
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
                "payload exceeds maximum frame size"
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
    fn stamp_frames(&mut self, group: ConnectionGroup, ts: Nanos) {
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
        let config = &self.groups[group.0].config;
        let connection = &mut self.connections[index];
        let ConnectionState::Connected(stream) = &mut connection.state else { unreachable!() };
        let state = stream.write_frame(
            &self.registry,
            None,
            &self.send_buffer,
            config,
            &mut connection.timers,
        );
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
    fn has_broadcast_recipient(&self, group: ConnectionGroup) -> bool {
        group.0 < self.groups.len() &&
            self.connections.iter().any(|connection| {
                connection.group == group &&
                    !connection.close_when_drained &&
                    connection.write_side == WriteSide::Open &&
                    matches!(connection.state, ConnectionState::Connected(_))
            })
    }

    /// Writes the staged frames to every connected member of `group`,
    /// disconnecting members whose write fails. Returns the number of
    /// recipients attempted.
    fn broadcast_staged(&mut self, group: ConnectionGroup) -> usize {
        let mut attempted = 0;
        let mut index = self.connections.len();
        while index != 0 {
            index -= 1;
            if self.connections[index].group != group ||
                self.connections[index].close_when_drained ||
                self.connections[index].write_side != WriteSide::Open ||
                !matches!(self.connections[index].state, ConnectionState::Connected(_))
            {
                continue;
            }
            attempted += 1;
            self.write_staged(index);
        }
        attempted
    }

    fn broadcast_with<F>(&mut self, group: ConnectionGroup, serialise: F) -> usize
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

    fn broadcast_many_with<I, F>(
        &mut self,
        group: ConnectionGroup,
        items: I,
        mut serialise: F,
    ) -> usize
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

    fn shutdown_write_when_drained(&mut self, token: Token) -> bool {
        let Some(index) = self.connections.iter().position(|connection| connection.token == token)
        else {
            return false;
        };
        let ConnectionState::Connected(stream) = &self.connections[index].state else {
            return false;
        };
        let drained = stream.send_queue.is_empty();
        match self.connections[index].write_side {
            WriteSide::Open if drained => self.shut_write(index),
            WriteSide::Open => self.connections[index].write_side = WriteSide::ShutWhenDrained,
            WriteSide::ShutWhenDrained | WriteSide::Shut => {}
        }
        true
    }

    /// Shuts the write side of a connected socket, which stays registered and
    /// readable: the peer reads the end of the stream, and what it sends
    /// afterwards still arrives.
    fn shut_write(&mut self, index: usize) {
        let peer = self.connections[index].peer;
        if let ConnectionState::Connected(stream) = &self.connections[index].state &&
            let Err(err) = stream.socket.shutdown(Shutdown::Write)
        {
            // A peer that has gone already refuses the shutdown, and its own
            // end of stream is on its way here.
            debug!(?err, %peer, "couldn't shut the write side");
        }
        self.connections[index].write_side = WriteSide::Shut;
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

/// A grouped collection of listeners and persistent outbound endpoints driven
/// by one nonblocking poll.
///
/// One network mixes both transports of the [`Endpoint`] set: a TCP and a
/// Unix-domain listener can share a group.
///
/// Unlike [`super::TcpConnector`], queued bytes are never retained across a
/// disconnected socket. Use `TcpConnector` when reconnect backlog replay is
/// required.
///
/// Dropping the network closes every listener, which unlinks the socket file
/// of each [`Endpoint::Unix`] listener.
///
/// A group is either **service-owned** — a protocol layer claimed it and the
/// network routes its events to that service — or an **unclaimed group**, whose
/// events reach the handler passed to [`Self::drive`] as they arrive.
///
/// Who owns the poll is fixed at construction. [`StreamNetwork::default`]
/// makes an **Owned** network, which creates its own poll, drives it in
/// [`Self::drive`] and hands out a [`Self::waker`] for it.
/// [`Self::with_registry`] makes an **External** one, which registers on a
/// registry cloned from the caller's poll and never polls: the caller polls
/// and makes the three calls [`Self::next_deadline`], [`Self::handle_event`]
/// and [`Self::tick`]. The [`crate::stream`] module documents the External
/// loop in full.
pub struct StreamNetwork {
    poll: PollMode,
    state: NetworkState,
    /// Whether a service owns the group at each index.
    claimed: Vec<bool>,
}

/// Who owns the poll the network's sockets are registered with.
enum PollMode {
    /// The network made the poll and drives it in [`StreamNetwork::drive`].
    /// `waker_taken` records that its one waker has been handed out.
    Owned { poll: Poll, events: Events, waker_taken: bool },
    /// The caller polls; the network only registers on the registry it was
    /// handed, and never blocks.
    External,
}

/// Builds an Owned-mode network: it creates the poll, drives it in
/// [`Self::drive`] and hands out a [`Self::waker`] for it.
impl Default for StreamNetwork {
    fn default() -> Self {
        let poll = Poll::new().expect("couldn't set up a poll for the stream network");
        let registry = poll
            .registry()
            .try_clone()
            .expect("couldn't clone the registry of the stream network poll");
        Self {
            poll: PollMode::Owned {
                poll,
                events: Events::with_capacity(EVENTS_CAPACITY),
                waker_taken: false,
            },
            state: NetworkState::new(registry, Token(0)),
            claimed: Vec::with_capacity(INITIAL_GROUP_CAPACITY),
        }
    }
}

impl StreamNetwork {
    /// Builds a network on a poll the caller owns: External mode.
    ///
    /// `registry` is a `try_clone` of that poll's registry, which is where
    /// the network registers its listeners and connections; it never polls,
    /// so [`Self::drive`] and [`Self::waker`] are refused. The caller polls
    /// and makes the three calls a caller-held poll requires:
    /// [`Self::next_deadline`] to fold into its own timeout,
    /// [`Self::handle_event`] per readiness event and [`Self::tick`] once per
    /// iteration. The [`crate::stream`] module documents that loop in full.
    ///
    /// Tokens are allocated upward from `token_base`; every token the caller
    /// uses for a source of its own — its waker included — must stay below
    /// it, or above everything this network has allocated, which is what
    /// makes [`Self::handle_event`] able to hand those events back.
    ///
    /// One External network per poll is the model. A second network's tokens
    /// climb from a base of their own, and this one cannot tell a token above
    /// its own high-water mark from one it will allocate next; give two
    /// networks two polls.
    #[must_use]
    pub fn with_registry(registry: Registry, token_base: Token) -> Self {
        Self {
            poll: PollMode::External,
            state: NetworkState::new(registry, token_base),
            claimed: Vec::with_capacity(INITIAL_GROUP_CAPACITY),
        }
    }

    /// Adds a protocol group and returns its handle.
    #[must_use = "the group handle identifies listeners and outbound endpoints"]
    pub fn add_group(&mut self, config: ConnectionGroupConfig) -> ConnectionGroup {
        assert!(config.max_frame_size > 0, "max_frame_size must be nonzero");
        if config.framing == Framing::LengthPrefixed {
            assert!(
                u32::try_from(config.max_frame_size).is_ok(),
                "max_frame_size exceeds the wire length field"
            );
        }
        if let Some(message) = &config.on_connect_msg {
            assert!(!message.is_empty(), "on_connect_msg must be nonempty");
            assert!(
                message.len() <= config.max_frame_size,
                "on_connect_msg exceeds max_frame_size"
            );
        }
        assert!(config.max_connections != Some(0), "max_connections must be nonzero");
        if let Some(max) = config.max_backlog_bytes {
            assert!(max > 0, "max_backlog_bytes must be nonzero");
            if let Some(warn) = config.backlog_warn_bytes {
                assert!(warn < max, "backlog_warn_bytes must be below max_backlog_bytes");
            }
        }
        let group = ConnectionGroup(self.state.groups.len());
        let reconnector = Repeater::every(config.reconnect_interval);
        self.state.groups.push(GroupState {
            config,
            reconnector,
            refused: 0,
            last_refusal_warning: None,
        });
        self.claimed.push(false);
        group
    }

    /// Marks `group` as owned by the service making the call.
    pub(crate) fn claim_group(&mut self, group: ConnectionGroup) {
        assert!(group.0 < self.claimed.len(), "unknown connection group");
        assert!(!self.claimed[group.0], "connection group {} already has a service", group.0);
        self.claimed[group.0] = true;
    }

    /// Returns `group` to unclaimed status.
    pub(crate) fn release_group(&mut self, group: ConnectionGroup) {
        assert!(group.0 < self.claimed.len(), "unknown connection group");
        self.claimed[group.0] = false;
    }

    /// The wire encoding of `group`, which a service checks before claiming it.
    pub(crate) fn framing(&self, group: ConnectionGroup) -> Framing {
        assert!(group.0 < self.state.groups.len(), "unknown connection group");
        self.state.config(group).framing
    }

    /// Hard-closes every listener, accepted connection and outbound endpoint
    /// of `group` and returns it to unclaimed status, empty and reusable.
    pub(crate) fn close_group(&mut self, group: ConnectionGroup) {
        self.state.close_group(group);
        self.release_group(group);
    }

    /// Adds a listener to `group`.
    ///
    /// An [`Endpoint::Unix`] listener creates its socket file with mode `0777`
    /// less the umask bits; flux sets no mode of its own. Connecting to a
    /// Unix-domain socket requires *write* permission on that file, so the
    /// usual `022` umask yields `0755` and admits the owner alone. An operator
    /// who wants group or world access sets the umask before the bind or
    /// changes the mode after it.
    pub fn listen(&mut self, group: ConnectionGroup, endpoint: Endpoint) -> io::Result<()> {
        self.state.listen(group, endpoint)
    }

    /// Adds a persistent outbound endpoint and immediately starts connecting.
    /// The returned token remains stable across reconnects.
    #[must_use = "the token identifies the persistent outbound endpoint"]
    pub fn connect(&mut self, group: ConnectionGroup, endpoint: Endpoint) -> Token {
        self.state.connect(group, endpoint)
    }

    /// Runs one iteration of an Owned-mode network: maintenance, one poll,
    /// event delivery and one tick per service. Returns whether anything
    /// happened.
    ///
    /// The poll blocks for `max_timeout` at the longest, and for less when the
    /// network's own timers or a service's [`ServiceRef`] deadline fall sooner;
    /// `None` everywhere blocks until an event arrives. Events of a
    /// service-owned group go to that service, the rest to `unclaimed_handler`,
    /// which borrows each payload for the duration of the call. Protocol
    /// events the services produced are pulled from the services
    /// afterwards. A wake from [`Self::waker`] returns from the poll and is
    /// not work of its own.
    ///
    /// The poll wait is where an iteration spends its time, so the ticks that
    /// follow it are given the time after it: a timer a tick starts runs from
    /// the moment the wait ended, and one that expired during the wait is due
    /// in the same iteration rather than the next.
    ///
    /// # Panics
    /// A network built with [`Self::with_registry`] is driven from the
    /// caller's poll and refuses this call. Every service-owned group must
    /// appear exactly once among `services`; a group whose service is missing
    /// or doubled is a configuration error and panics before the call does
    /// anything else.
    pub fn drive<F>(
        &mut self,
        max_timeout: Option<Duration>,
        services: &mut [ServiceRef<'_>],
        mut unclaimed_handler: F,
    ) -> bool
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        assert!(matches!(self.poll, PollMode::Owned { .. }), "{EXTERNAL_POLLS}");
        let now = Instant::now();
        self.validate(services);

        let mut worked = self.maintenance(now, services, &mut unclaimed_handler);
        let timeout = self.poll_timeout(max_timeout, services, now);
        let PollMode::Owned { poll, events, .. } = &mut self.poll else {
            unreachable!("the mode is checked above");
        };
        match poll.poll(events, timeout) {
            Ok(()) => {
                for event in &*events {
                    // A wake asks the poll to return and nothing more, so it
                    // reaches neither a service nor the did-work result.
                    if event.token() != WAKER_TOKEN {
                        worked |= route_ready(
                            &mut self.state,
                            &self.claimed,
                            event,
                            services,
                            &mut unclaimed_handler,
                        );
                    }
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => flux_utils::safe_panic!("couldn't poll the stream network: {err}"),
        }

        // The instant this iteration began is older than the poll wait it has
        // just come out of, which may have been the whole of a timeout.
        let pullable = self.tick_services(services, Instant::now());
        worked || pullable
    }

    /// The earliest instant this network or one of `services` needs a call at:
    /// the fold of the network's own timers with every service deadline.
    ///
    /// An External-mode caller folds only its own timers against this and
    /// converts the result into the timeout of its poll; `None` leaves that
    /// poll free to block until a socket is ready.
    ///
    /// # Panics
    /// An Owned-mode network folds its deadlines inside [`Self::drive`], and
    /// refuses this call. Every service-owned group must appear exactly once
    /// among `services`, as for [`Self::drive`] — an omitted service is a
    /// configuration error the first call reports, not one that waits for a
    /// deadline to be missed.
    pub fn next_deadline(&self, services: &[ServiceRef<'_>]) -> Option<Instant> {
        assert!(matches!(self.poll, PollMode::External), "{OWNED_POLLS}");
        self.validate(services);
        self.fold_deadline(services)
    }

    /// Takes one readiness event from the caller's poll, reporting whether it
    /// was this network's.
    ///
    /// The network's tokens run from the base it was built with up to the
    /// high-water mark of its allocation. A token outside that range belongs
    /// to a source the caller registered — its waker included — and is handed
    /// straight back as `false`, untouched and unlogged. A token inside it is
    /// this network's: the event is routed to the service owning its group, or
    /// to `unclaimed_handler`, along with any disconnect that handling it
    /// produced, and the call reports `true`. A token the network no longer
    /// knows is one of its own that closed since the poll returned: ours,
    /// and ignored.
    ///
    /// `true` means this network owns the token, not that routing produced an
    /// event. Folding it into a did-work signal, as the [`crate::stream`] loop
    /// does, is conservative: stale or otherwise non-producing readiness may
    /// count as one busy iteration.
    ///
    /// # Panics
    /// An Owned-mode network polls itself, and refuses this call. An event of
    /// this network's validates `services` before it is routed, exactly as
    /// [`Self::drive`] does; an event of the caller's is handed back without
    /// looking at them, since [`Self::next_deadline`] and [`Self::tick`]
    /// validate every iteration anyway.
    pub fn handle_event<F>(
        &mut self,
        event: &Event,
        services: &mut [ServiceRef<'_>],
        mut unclaimed_handler: F,
    ) -> bool
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        assert!(matches!(self.poll, PollMode::External), "{OWNED_POLLS}");
        if !self.state.is_ours(event.token()) {
            return false;
        }
        self.validate(services);
        route_ready(&mut self.state, &self.claimed, event, services, &mut unclaimed_handler);
        true
    }

    /// Runs the iteration work that owns no poll: the maintenance due now —
    /// pending disconnects, routed to their services, and reconnect attempts —
    /// then one tick per service in slice order. Returns whether anything
    /// happened, a service with protocol events left to pull included.
    ///
    /// A transport event maintenance produces reaches its service before that
    /// service's tick, so protocol state never lags transport state by an
    /// iteration. An External-mode caller makes this call once per iteration,
    /// after handing over every event its poll returned.
    ///
    /// # Panics
    /// An Owned-mode network runs these phases inside [`Self::drive`], and
    /// refuses this call. Every service-owned group must appear exactly once
    /// among `services`, as for [`Self::drive`].
    pub fn tick<F>(&mut self, services: &mut [ServiceRef<'_>], mut unclaimed_handler: F) -> bool
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        assert!(matches!(self.poll, PollMode::External), "{OWNED_POLLS}");
        let now = Instant::now();
        self.validate(services);
        let worked = self.maintenance(now, services, &mut unclaimed_handler);
        let pullable = self.tick_services(services, now);
        worked || pullable
    }

    /// A waker for this network's own poll, on a token the event loop
    /// swallows: waking it makes a blocked [`Self::drive`] return, and counts
    /// as no work of its own. Under `flux/park` a tile hands this waker to
    /// `SpineAdapter::register_waker`, after which spine work interrupts the
    /// poll.
    ///
    /// A wake is delivered only while the waker that sent it is alive, so it
    /// belongs somewhere that outlives the poll it wakes —
    /// `register_waker` takes ownership of it, which is that somewhere.
    ///
    /// # Panics
    /// A network hands out one waker: a poll takes a single one, and asking
    /// twice panics. An External-mode network has no poll of its own, and
    /// refuses the call: build the waker on the caller's poll instead, on one
    /// of the caller's own tokens.
    pub fn waker(&mut self) -> io::Result<Waker> {
        let PollMode::Owned { poll, waker_taken, .. } = &mut self.poll else {
            panic!(
                "this network is driven from a caller-owned poll: build the waker on that poll, \
                 on a token below the base the network was built with"
            );
        };
        assert!(
            !*waker_taken,
            "a stream network hands out one waker; keep the first (SpineAdapter::register_waker \
             stores it for the process lifetime)"
        );
        let waker = Waker::new(poll.registry(), WAKER_TOKEN)?;
        *waker_taken = true;
        Ok(waker)
    }

    /// Runs the maintenance due at `now`: the disconnects left pending by
    /// work outside a driver call, routed to their services, and one reconnect
    /// attempt per outbound endpoint whose group is due one.
    fn maintenance<F>(
        &mut self,
        now: Instant,
        services: &mut [ServiceRef<'_>],
        unclaimed_handler: &mut F,
    ) -> bool
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        let mut routed = false;
        {
            let claimed = &self.claimed;
            let mut route = |event: StreamEvent<'_>| {
                route_event(claimed, services, unclaimed_handler, &mut routed, event);
            };
            self.state.drain_pending_disconnects(&mut route);
        }
        let reconnected = self.state.maybe_reconnect(now);
        routed || reconnected
    }

    /// Ticks every service once, in slice order, reporting whether any of them
    /// has protocol events left to pull.
    fn tick_services(&mut self, services: &mut [ServiceRef<'_>], now: Instant) -> bool {
        let mut pullable = false;
        for service in services.iter_mut() {
            pullable |= service.tick(self, now);
        }
        pullable
    }

    /// Runs one nonblocking iteration of an Owned-mode network holding
    /// unclaimed groups only.
    ///
    /// # Panics
    /// A network with services is driven with [`Self::drive`], which is what
    /// delivers their events; polling it without them panics. So does a
    /// network built with [`Self::with_registry`], which owns no poll.
    pub fn poll_with<F>(&mut self, handler: F)
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        assert!(matches!(self.poll, PollMode::Owned { .. }), "{EXTERNAL_POLLS}");
        if let Some(index) = self.claimed.iter().position(|claimed| *claimed) {
            panic!(
                "connection group {index} has a service — drive the network with \
                 StreamNetwork::drive, passing every service"
            );
        }
        self.drive(Some(Duration::ZERO), &mut [], handler);
    }

    /// Panics unless `services` names every service-owned group exactly once.
    fn validate(&self, services: &[ServiceRef<'_>]) {
        for (position, service) in services.iter().enumerate() {
            let group = service.group();
            assert!(
                self.claimed.get(group.0).copied().unwrap_or(false),
                "no service owns connection group {} — a service claims its group when it is built",
                group.0
            );
            assert!(
                services[..position].iter().all(|other| other.group() != group),
                "duplicate service for group {}",
                group.0
            );
        }
        for (index, claimed) in self.claimed.iter().enumerate() {
            assert!(
                !claimed || services.iter().any(|service| service.group().0 == index),
                "service-owned group {index} has no service — call HttpService::close before \
                 dropping it"
            );
        }
    }

    /// The earliest instant the network's own timers or one of `services`
    /// needs a call at, taking the service set as given.
    fn fold_deadline(&self, services: &[ServiceRef<'_>]) -> Option<Instant> {
        self.state
            .next_deadline()
            .into_iter()
            .chain(services.iter().filter_map(|service| service.next_deadline()))
            .min()
    }

    /// Folds the caller's cap, the network's own timers and every service's
    /// deadline into the timeout of one poll.
    fn poll_timeout(
        &self,
        max_timeout: Option<Duration>,
        services: &[ServiceRef<'_>],
        now: Instant,
    ) -> Option<std::time::Duration> {
        let deadline = self.fold_deadline(services);
        let timeout = match (max_timeout, deadline.map(|deadline| deadline.saturating_sub(now))) {
            (Some(max), Some(next)) => Some(max.min(next)),
            (max, next) => max.or(next),
        };
        timeout.map(std::time::Duration::from)
    }

    /// Serializes and sends one payload to a connected token. Length-prefixed
    /// groups add a frame header; raw-framed groups send the payload unchanged.
    /// The closure is not called when the token is unknown or currently
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
    /// `group`. Length-prefixed groups add a frame header; raw-framed groups
    /// send the payload unchanged. Returns the number of recipients
    /// attempted.
    pub fn broadcast_with<F>(&mut self, group: ConnectionGroup, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        self.state.broadcast_with(group, serialise)
    }

    /// Serializes multiple payloads once and sends the batch to every
    /// connected member of `group`. Framing, size limits, and skipping of
    /// invalid payloads follow [`StreamNetwork::send_many_with`]; each member
    /// receives the batch in one socket write when it has no backlog. The
    /// closure is not called when the group has no connected member. Returns
    /// the number of recipients attempted.
    pub fn broadcast_many_with<I, F>(
        &mut self,
        group: ConnectionGroup,
        items: I,
        serialise: F,
    ) -> usize
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
    /// token are rejected. A TCP peer that never drains is bounded only by
    /// `TCP_USER_TIMEOUT`; a Unix-domain peer has no such bound.
    pub fn disconnect_when_drained(&mut self, token: Token) -> bool {
        self.state.disconnect_when_drained(token)
    }

    /// Shuts the write side of a connection once its queued bytes have been
    /// written, and keeps reading it: the peer reads the end of the stream,
    /// while the bytes it sends afterwards still arrive as
    /// [`StreamEvent::Message`] and its own close still arrives as
    /// [`StreamEvent::Disconnected`]. Both transports of the [`Endpoint`] set
    /// half-close. Returns `false` for unknown or disconnected tokens; a
    /// connection whose write side is already shut, or already waiting to be,
    /// is left as it is.
    ///
    /// Sends to such a token are rejected and queue nothing, so the queue
    /// only shrinks from the call onward. [`Self::disconnect`] still closes
    /// the connection outright at any point, and a token that is draining as
    /// well ([`Self::disconnect_when_drained`]) closes when its queue empties
    /// rather than half-closing. A TCP peer that never drains is bounded only
    /// by `TCP_USER_TIMEOUT`; a Unix-domain peer has no such bound.
    pub fn shutdown_write_when_drained(&mut self, token: Token) -> bool {
        self.state.shutdown_write_when_drained(token)
    }

    /// How many connections `group` refused since it was added because it
    /// was already holding its [`ConnectionGroupConfig::max_connections`] cap.
    ///
    /// # Panics
    /// The group must be one this network added.
    #[must_use]
    pub fn refused_connections(&self, group: ConnectionGroup) -> u64 {
        assert!(group.0 < self.state.groups.len(), "unknown connection group");
        self.state.groups[group.0].refused
    }

    /// Permanently removes a connection or outbound endpoint. Returns whether
    /// the token was found.
    pub fn remove(&mut self, token: Token) -> bool {
        self.state.remove(token)
    }
}

/// Routes one readiness event of a socket this network owns, together with
/// the disconnects handling it produced, and reports whether anything reached
/// a service or the unclaimed handler.
fn route_ready<F>(
    state: &mut NetworkState,
    claimed: &[bool],
    event: &Event,
    services: &mut [ServiceRef<'_>],
    unclaimed_handler: &mut F,
) -> bool
where
    F: for<'a> FnMut(StreamEvent<'a>),
{
    let mut routed = false;
    let mut route = |event: StreamEvent<'_>| {
        route_event(claimed, services, unclaimed_handler, &mut routed, event);
    };
    state.handle_event(event, &mut route);
    state.drain_pending_disconnects(&mut route);
    routed
}

/// Hands one event to the service owning its group, or to the unclaimed handler
/// when no service owns it.
fn route_event<F>(
    claimed: &[bool],
    services: &mut [ServiceRef<'_>],
    unclaimed_handler: &mut F,
    routed: &mut bool,
    event: StreamEvent<'_>,
) where
    F: for<'a> FnMut(StreamEvent<'a>),
{
    *routed = true;
    let group = event.group();
    if let Some(service) = services.iter_mut().find(|service| service.group() == group) {
        service.on_event(&event);
    } else {
        debug_assert!(
            !claimed[group.0],
            "service-owned group {} has no service to route to",
            group.0
        );
        unclaimed_handler(event);
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

#[derive(Clone, Copy)]
enum RxState {
    Header { bytes: [u8; FRAME_HEADER_SIZE], have: usize },
    Payload { length: usize, have: usize, send_ts: Nanos },
}

impl Default for RxState {
    fn default() -> Self {
        Self::Header { bytes: [0; FRAME_HEADER_SIZE], have: 0 }
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

    fn maybe_warn(&mut self, config: &ConnectionGroupConfig, token: Token, peer: Peer) {
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
            %peer,
            queued_bytes = self.len(),
            %age,
            "send backlog growing"
        );
        self.last_warning = Some(Instant::now());
    }
}

struct FramedStream {
    socket: TransportStream,
    token: Token,
    peer: Peer,
    rx_state: RxState,
    rx_buffer: Vec<u8>,
    send_queue: ByteQueue,
    writable_armed: bool,
}

impl FramedStream {
    fn new(socket: TransportStream, token: Token, peer: Peer, max_frame_size: usize) -> Self {
        Self {
            socket,
            token,
            peer,
            rx_state: RxState::default(),
            rx_buffer: vec![0; INITIAL_RX_BUFFER_SIZE.min(max_frame_size)],
            send_queue: ByteQueue::default(),
            writable_armed: false,
        }
    }

    fn poll_with<F>(
        &mut self,
        registry: &Registry,
        event: &Event,
        config: &ConnectionGroupConfig,
        timers: &mut Option<NetworkTimers>,
        on_message: &mut F,
    ) -> StreamState
    where
        F: for<'a> FnMut(&'a [u8], Nanos),
    {
        if event.is_readable() {
            if config.framing == Framing::Raw {
                loop {
                    match self.socket.read(&mut self.rx_buffer) {
                        Ok(0) => return StreamState::Disconnected,
                        Ok(read) => on_message(&self.rx_buffer[..read], Nanos::now()),
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                        Err(err) => {
                            debug!(?err, %self.peer, "raw read failed");
                            return StreamState::Disconnected;
                        }
                    }
                }
            } else {
                loop {
                    match self.read_frame(config.max_frame_size) {
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
        }
        if event.is_writable() && self.drain_queue(registry, config) == StreamState::Disconnected {
            return StreamState::Disconnected;
        }
        if event.is_error() || event.is_read_closed() || event.is_write_closed() {
            return StreamState::Disconnected;
        }
        StreamState::Alive
    }

    fn read_frame(&mut self, max_frame_size: usize) -> ReadOutcome<'_> {
        loop {
            match self.rx_state {
                RxState::Header { mut bytes, mut have } => {
                    while have < FRAME_HEADER_SIZE {
                        match self.socket.read(&mut bytes[have..]) {
                            Ok(0) => return ReadOutcome::Disconnected,
                            Ok(read) => {
                                have += read;
                                if have != FRAME_HEADER_SIZE {
                                    continue;
                                }
                                let length =
                                    u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
                                let send_ts = Nanos(u64::from_le_bytes(
                                    bytes[4..FRAME_HEADER_SIZE].try_into().unwrap(),
                                ));
                                if length == 0 {
                                    self.rx_state = RxState::default();
                                    break;
                                }
                                if length > max_frame_size {
                                    warn!(
                                        %self.peer,
                                        payload_len = length,
                                        max_frame_size,
                                        "frame exceeds configured maximum"
                                    );
                                    return ReadOutcome::Disconnected;
                                }
                                if self.rx_buffer.len() < length {
                                    self.rx_buffer.resize(length, 0);
                                }
                                self.rx_state = RxState::Payload { length, have: 0, send_ts };
                            }
                            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                                self.rx_state = RxState::Header { bytes, have };
                                return ReadOutcome::WouldBlock;
                            }
                            Err(err) => {
                                debug!(?err, %self.peer, "header read failed");
                                return ReadOutcome::Disconnected;
                            }
                        }
                    }
                }
                RxState::Payload { length, mut have, send_ts } => {
                    while have < length {
                        match self.socket.read(&mut self.rx_buffer[have..length]) {
                            Ok(0) => return ReadOutcome::Disconnected,
                            Ok(read) => {
                                have += read;
                                if have == length {
                                    self.rx_state = RxState::default();
                                    return ReadOutcome::Message {
                                        payload: &self.rx_buffer[..length],
                                        send_ts,
                                    };
                                }
                            }
                            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                                self.rx_state = RxState::Payload { length, have, send_ts };
                                return ReadOutcome::WouldBlock;
                            }
                            Err(err) => {
                                debug!(?err, %self.peer, "payload read failed");
                                return ReadOutcome::Disconnected;
                            }
                        }
                    }
                }
            }
        }
    }

    fn write_frame(
        &mut self,
        registry: &Registry,
        header: Option<&[u8; FRAME_HEADER_SIZE]>,
        payload: &[u8],
        config: &ConnectionGroupConfig,
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
                debug!(?err, %self.peer, "frame write failed");
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
        config: &ConnectionGroupConfig,
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
                %self.peer,
                queued_bytes = self.send_queue.len(),
                additional_bytes = additional,
                max_backlog_bytes = max,
                "send backlog would exceed configured maximum"
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
        self.send_queue.maybe_warn(config, self.token, self.peer);
        self.arm_writable(registry)
    }

    fn drain_queue(&mut self, registry: &Registry, config: &ConnectionGroupConfig) -> StreamState {
        while !self.send_queue.is_empty() {
            match self.socket.write(self.send_queue.remaining()) {
                Ok(0) => return StreamState::Disconnected,
                Ok(written) => self.send_queue.consume(written),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => {
                    debug!(?err, %self.peer, "backlog write failed");
                    return StreamState::Disconnected;
                }
            }
        }
        self.send_queue.maybe_warn(config, self.token, self.peer);
        if self.send_queue.is_empty() && self.writable_armed {
            if let Err(err) = registry.reregister(&mut self.socket, self.token, Interest::READABLE)
            {
                debug!(?err, %self.peer, "couldn't disarm writable interest");
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
            debug!(?err, %self.peer, "couldn't arm writable interest");
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
        ByteQueue, ConnectionGroupConfig, DEFAULT_TCP_USER_TIMEOUT_MS, FRAME_HEADER_SIZE,
        FramedStream, PayloadBuf, Peer, StreamState, TcpOptions, TransportStream,
        set_socket_buf_size, write_frame_header,
    };

    #[test]
    fn tcp_options_default_to_nodelay_without_keepalive() {
        assert_eq!(TcpOptions::default(), TcpOptions {
            nodelay: true,
            keepalive: false,
            user_timeout_ms: DEFAULT_TCP_USER_TIMEOUT_MS,
        });
        assert_eq!(ConnectionGroupConfig::default().tcp, TcpOptions::default());
    }

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

        let socket = TransportStream::Tcp(mio::net::TcpStream::from_std(client));
        set_socket_buf_size(&socket, 1024);
        let mut stream = FramedStream::new(socket, Token(0), Peer::Tcp(peer_addr), 1024);
        let fill = [0; 4096];
        loop {
            match stream.socket.write(&fill) {
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => panic!("failed to fill socket send buffer: {err}"),
            }
        }
        stream.send_queue.bytes.extend_from_slice(&[1; 8]);

        let config = ConnectionGroupConfig {
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
}
