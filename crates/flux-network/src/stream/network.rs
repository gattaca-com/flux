use std::{
    io::{self, IoSlice, Read, Write},
    net::Shutdown,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use flux_communication::Timer;
use flux_timing::{Duration, Instant, Nanos, Repeater};
use mio::{Events, Interest, Poll, Registry, Token, Waker, event::Event};
use tracing::{debug, error, info, warn};

use super::{
    Endpoint, PayloadBuf, Peer, ReadinessOutcome, Service, TcpTelemetry, set_socket_buf_size,
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

/// Transport event a [`ConnectionGroup`] lends its service for the duration of
/// one callback.
pub enum StreamEvent<'a> {
    /// A listener accepted a new connection.
    Accepted { token: Token, peer: Peer },
    /// A persistent outbound endpoint established a connection.
    Connected { token: Token, peer: Peer },
    /// A complete length-prefixed message or a raw read chunk was received.
    /// For raw-framed groups, chunks do not preserve message boundaries and
    /// `send_ts` is the local receive time.
    Message { token: Token, payload: &'a [u8], send_ts: Nanos },
    /// An established connection was closed.
    Disconnected { token: Token, peer: Peer },
}

/// What every group of one network shares: the registration handle for the
/// poll behind it, and the token space it allocates from.
///
/// Registry access, token uniqueness and network identity stay shared once
/// group state moves into its service; nothing else does.
struct NetworkCore {
    registry: Registry,
    /// The lowest token this network allocates. Every lower token belongs to
    /// a source the caller registered on its own poll.
    token_base: Token,
    next_token: AtomicUsize,
}

impl NetworkCore {
    /// Whether `token` is one this network allocated: at or above its base,
    /// and below the high-water mark of its allocation. Tokens are never
    /// reused, so anything outside that range is a source the caller
    /// registered on its own poll — its waker included, wherever the caller
    /// put it.
    fn is_ours(&self, token: Token) -> bool {
        (self.token_base.0..self.next_token.load(Ordering::Relaxed)).contains(&token.0)
    }

    /// One token, allocated when a listener or connection is created and
    /// never on the byte path.
    fn next_token(&self) -> Token {
        let token = Token(self.next_token.fetch_add(1, Ordering::Relaxed));
        assert!(token != WAKER_TOKEN, "stream token space exhausted");
        token
    }
}

/// The scheduler arms an obligation before the call that must honour it, and
/// the group records having honoured it.
const MAINTAIN_ARMED: usize = 1 << 0;
const MAINTAIN_OBSERVED: usize = 1 << 1;
const DEADLINE_OBSERVED: usize = 1 << 2;
const DEADLINE_IS_SOME: usize = 1 << 3;

/// What a service reports to its network in place of the group it owns.
///
/// The identity says which network created the group and which of its slots
/// the group holds; it carries no transport state and lends none. It also
/// holds the private state through which [`StreamNetwork`] verifies that a
/// scheduled service reached the group operations its cadence depends on.
pub struct ConnectionGroupId {
    core: Arc<NetworkCore>,
    slot: usize,
    /// Which obligations are armed and which the group has honoured.
    flags: AtomicUsize,
    /// The deadline the group last reported, meaningful only alongside
    /// [`DEADLINE_OBSERVED`]; absence is [`DEADLINE_IS_SOME`] being clear,
    /// never a reserved instant.
    deadline: AtomicU64,
}

impl ConnectionGroupId {
    fn new(core: Arc<NetworkCore>, slot: usize) -> Self {
        Self {
            core,
            slot,
            flags: AtomicUsize::new(0),
            deadline: AtomicU64::new(0),
        }
    }

    /// Whether this identity belongs to the network holding `core`.
    fn belongs_to(&self, core: &Arc<NetworkCore>) -> bool {
        Arc::ptr_eq(&self.core, core)
    }

    /// Arms the maintenance obligation, clearing what the check will read.
    fn arm_maintain(&self) {
        self.flags.store(MAINTAIN_ARMED, Ordering::Relaxed);
    }

    /// Records that the group ran its due transport work.
    fn observe_maintain(&self) {
        self.flags.fetch_or(MAINTAIN_OBSERVED, Ordering::Relaxed);
    }

    fn assert_maintained(&self) {
        assert!(
            self.flags.load(Ordering::Relaxed) & MAINTAIN_OBSERVED != 0,
            "connection group {} ticked without reaching ConnectionGroup::maintain: a leaf \
             service runs it at the start of its tick",
            self.slot
        );
    }

    /// Arms the deadline obligation, clearing what the check will read.
    fn arm_deadline(&self) {
        self.flags.fetch_and(!(DEADLINE_OBSERVED | DEADLINE_IS_SOME), Ordering::Relaxed);
    }

    /// Records the transport deadline the group reported.
    fn observe_deadline(&self, deadline: Option<Instant>) {
        if let Some(deadline) = deadline {
            self.deadline.store(deadline.0, Ordering::Relaxed);
            self.flags.fetch_or(DEADLINE_OBSERVED | DEADLINE_IS_SOME, Ordering::Relaxed);
        } else {
            self.flags.fetch_or(DEADLINE_OBSERVED, Ordering::Relaxed);
        }
    }

    /// Panics unless the root folded in the deadline its group reported.
    fn assert_deadline_included(&self, root: Option<Instant>) {
        let flags = self.flags.load(Ordering::Relaxed);
        assert!(
            flags & DEADLINE_OBSERVED != 0,
            "connection group {} reported a deadline without reaching \
             ConnectionGroup::next_deadline: a leaf service folds it into its own",
            self.slot
        );
        if flags & DEADLINE_IS_SOME == 0 {
            return;
        }
        let group = Instant(self.deadline.load(Ordering::Relaxed));
        match root {
            Some(root) if root <= group => {}
            _ => panic!(
                "connection group {} needs a tick by {} but its service reported {:?}: a \
                 service folds its group's deadline into its own",
                self.slot, group.0, root
            ),
        }
    }
}


struct Listener {
    token: Token,
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
    token: Token,
    peer: Peer,
}

/// The listeners, outbound endpoints, connections and byte queues of one
/// protocol group, owned by the service that owns the group.
///
/// [`StreamNetwork::add_group`] creates one and hands it over; moving it into a
/// service is the claim, and the compiler is what keeps a group to one owner.
/// A group cannot be scheduled on its own: only a [`Service`] reaches the
/// driver.
///
/// # Ordering
/// A leaf service calls [`Self::maintain`] at the start of every
/// [`Service::tick`] and routes the events it emits before running its own
/// timers, and folds [`Self::next_deadline`] into the deadline it reports.
/// [`StreamNetwork`] verifies both and panics on either omission.
pub struct ConnectionGroup {
    identity: ConnectionGroupId,
    config: ConnectionGroupConfig,
    reconnector: Repeater,
    /// Accepted connections this group currently holds, including those
    /// draining or half-closed. Outbound connections do not count.
    accepted: usize,
    /// Connections refused since the group was added because it was at its
    /// cap, and when the last of them was warned about.
    refused: u64,
    last_refusal_warning: Option<Instant>,
    listeners: Vec<Listener>,
    connections: Vec<Connection>,
    pending_disconnects: Vec<PendingDisconnect>,
    /// The frames of the send in progress, staged back to back as
    /// `[header][payload]` (raw-framed groups stage payloads only), written to
    /// the socket in one call. Grown to the largest batch this group has sent
    /// and never shrunk.
    send_buffer: Vec<u8>,
}

impl ConnectionGroup {
    fn new(identity: ConnectionGroupId, config: ConnectionGroupConfig) -> Self {
        let reconnector = Repeater::every(config.reconnect_interval);
        Self {
            identity,
            config,
            reconnector,
            accepted: 0,
            refused: 0,
            last_refusal_warning: None,
            listeners: Vec::with_capacity(INITIAL_LISTENER_CAPACITY),
            connections: Vec::with_capacity(INITIAL_CONNECTION_CAPACITY),
            pending_disconnects: Vec::with_capacity(INITIAL_CONNECTION_CAPACITY),
            send_buffer: Vec::with_capacity(INITIAL_SEND_BUFFER_SIZE),
        }
    }

    /// What this group's service reports to its network.
    pub fn group_id(&self) -> &ConnectionGroupId {
        &self.identity
    }

    /// The wire encoding this group's messages use.
    pub fn framing(&self) -> Framing {
        self.config.framing
    }

    fn registry(&self) -> &Registry {
        &self.identity.core.registry
    }

    /// Adds a listener, and reports the endpoint it bound.
    ///
    /// That endpoint is the one asked for, except for a TCP address whose
    /// port is `0`: the kernel picks the port, and what comes back is the
    /// address a peer must dial.
    ///
    /// An [`Endpoint::Unix`] listener creates its socket file with mode `0777`
    /// less the umask bits; flux sets no mode of its own. Connecting to a
    /// Unix-domain socket requires *write* permission on that file, so the
    /// usual `022` umask yields `0755` and admits the owner alone.
    pub fn listen(&mut self, endpoint: Endpoint) -> io::Result<Endpoint> {
        let mut socket = ListenSocket::bind(endpoint)?;
        let bound = socket.endpoint()?;
        let token = self.identity.core.next_token();
        self.registry().register(&mut socket, token, Interest::READABLE)?;
        self.listeners.push(Listener { token, socket });
        Ok(bound)
    }

    /// Adds a persistent outbound endpoint and starts connecting to it. The
    /// token it returns identifies the connection for its whole life,
    /// reconnects included.
    pub fn connect(&mut self, endpoint: Endpoint) -> Token {
        let token = self.identity.core.next_token();
        let peer = endpoint.peer();
        let config = &self.config;
        let timers = NetworkTimers::new(config.telemetry, config.name, token, peer, config.framing);
        self.connections.push(Connection {
            token,
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
        let peer = connection.peer;
        let socket_buf_size = self.config.socket_buf_size;

        let Ok(mut socket) = TransportStream::connect(endpoint)
            .inspect_err(|err| debug!(?err, %endpoint, "couldn't start connection"))
        else {
            return;
        };
        if let Some(size) = socket_buf_size {
            set_socket_buf_size(&socket, size);
        }
        if let Err(err) = self.registry().register(&mut socket, token, Interest::WRITABLE) {
            warn!(?err, %peer, "couldn't register connecting stream");
            let _ = socket.shutdown(Shutdown::Both);
            return;
        }
        self.connections[index].state = ConnectionState::Connecting(socket);
    }

    /// Hard-closes every listener, accepted connection and outbound endpoint,
    /// discarding the disconnect events that produces.
    ///
    /// Closing an [`Endpoint::Unix`] listener unlinks its socket file.
    fn close(&mut self) {
        for index in (0..self.listeners.len()).rev() {
            let mut listener = self.listeners.swap_remove(index);
            let _ = self.registry().deregister(&mut listener.socket);
        }
        for index in (0..self.connections.len()).rev() {
            self.close_connection_socket(index);
            self.connections.swap_remove(index);
        }
        self.accepted = 0;
        self.pending_disconnects.clear();
    }

    /// Retries every outbound endpoint that is down if the group is due one,
    /// reporting whether an attempt was made.
    fn maybe_reconnect(&mut self, now: Instant) -> bool {
        if !self.reconnector.fired_at(now) {
            return false;
        }
        let mut attempted = false;
        for index in 0..self.connections.len() {
            if matches!(self.connections[index].state, ConnectionState::Disconnected) {
                self.start_connect(index);
                attempted = true;
            }
        }
        attempted
    }

    /// The instant this group's transport needs a tick by: the next retry of
    /// an outbound endpoint that is down, and immediately while a disconnect
    /// is queued for delivery, so the poll cannot sleep across work the next
    /// tick owes its service.
    ///
    /// Calling this is what satisfies the network's deadline audit, so a
    /// service folds it into the deadline it reports rather than skipping it
    /// when it has a nearer one of its own.
    pub fn next_deadline(&self) -> Option<Instant> {
        let mut next: Option<Instant> = None;
        for connection in &self.connections {
            if connection.endpoint.is_some() &&
                matches!(connection.state, ConnectionState::Disconnected)
            {
                let fire = self.reconnector.next_fire();
                next = Some(next.map_or(fire, |next: Instant| next.min(fire)));
            }
        }
        if !self.pending_disconnects.is_empty() {
            let now = Instant::now();
            next = Some(next.map_or(now, |next: Instant| next.min(now)));
        }
        self.identity.observe_deadline(next);
        next
    }

    /// Runs the transport work due now — queued disconnects and reconnect
    /// attempts — routing every event it produces into `on_event`, and
    /// reports whether anything happened.
    ///
    /// A leaf service calls this at the start of its [`Service::tick`] so
    /// transport state never lags its protocol state by an iteration.
    pub fn maintain<F>(&mut self, now: Instant, on_event: &mut F) -> bool
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        self.identity.observe_maintain();
        let mut worked = false;
        {
            let mut sink = |event: StreamEvent<'_>| {
                worked = true;
                on_event(event);
            };
            self.drain_pending_disconnects(&mut sink);
        }
        let reconnected = self.maybe_reconnect(now);
        worked || reconnected
    }

    /// Offers one readiness event to this group.
    ///
    /// A token this group does not hold produces
    /// [`ReadinessOutcome::not_owned`], which lets the scheduler try the next
    /// service. A token it holds is handled here, along with every disconnect
    /// that handling it produced, and the outcome reports whether an event
    /// reached `on_event`.
    pub fn handle_event<F>(&mut self, event: &Event, on_event: &mut F) -> ReadinessOutcome
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        let token = event.token();
        let listener = self.listeners.iter().position(|listener| listener.token == token);
        let connection =
            self.connections.iter().position(|connection| connection.token == token);
        if listener.is_none() && connection.is_none() {
            return ReadinessOutcome::not_owned();
        }

        let mut worked = false;
        {
            let mut sink = |event: StreamEvent<'_>| {
                worked = true;
                on_event(event);
            };
            if let Some(index) = listener {
                self.accept_connections(index, &mut sink);
            } else if let Some(index) = connection {
                self.serve_connection(index, event, &mut sink);
            }
            self.drain_pending_disconnects(&mut sink);
        }
        ReadinessOutcome::owned(worked)
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
        let peer = self.connections[index].peer;
        let mut timers = self.connections[index].timers;
        let config = &self.config;
        let group_name = config.name;

        if config.tcp.nodelay &&
            let Err(err) = socket.set_nodelay()
        {
            warn!(?err, %peer, "couldn't set nodelay on tcp stream");
            let _ = self.registry().deregister(&mut socket);
            let _ = socket.shutdown(Shutdown::Both);
            return false;
        }
        if config.tcp.keepalive &&
            let Err(err) = socket.set_keepalive()
        {
            warn!(?err, %peer, "couldn't set keepalive on tcp stream");
            let _ = self.registry().deregister(&mut socket);
            let _ = socket.shutdown(Shutdown::Both);
            return false;
        }
        socket.set_user_timeout(config.tcp.user_timeout_ms);
        if let Err(err) = self.registry().reregister(&mut socket, token, Interest::READABLE) {
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
            if stream.write_frame(self.registry(), header.as_ref(), message, config, &mut timers) ==
                StreamState::Disconnected
            {
                stream.close(self.registry());
                self.connections[index].timers = timers;
                return false;
            }
        }

        self.connections[index].timers = timers;
        self.connections[index].state = ConnectionState::Connected(stream);
        info!(group = group_name, %peer, "connection established");
        handler(StreamEvent::Connected { token, peer });
        true
    }

    fn accept_connections<F>(&mut self, listener_index: usize, handler: &mut F)
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        loop {
            let accepted = self.listeners[listener_index].socket.accept();
            let (mut socket, peer) = match accepted {
                Ok(accepted) => accepted,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => {
                    warn!(?err, group = self.config.name, "accept failed");
                    break;
                }
            };
            if let Some(max) = self.config.max_connections &&
                self.accepted >= max
            {
                // The refusal is all that happens to this connection: it
                // is never registered, never read from and never written
                // to, so the peer reads the end of the stream without ever
                // being sent a byte. The backlog still has to be drained to
                // `WouldBlock`, so the loop goes on.
                let _ = socket.shutdown(Shutdown::Both);
                self.refuse(peer, max);
                continue;
            }
            let token = self.identity.core.next_token();
            let (stream, timers, group_name) = {
                let config = &self.config;
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
                if let Err(err) =
                    self.identity.core.registry.register(&mut socket, token, Interest::READABLE)
                {
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
                        &self.identity.core.registry,
                        header.as_ref(),
                        message,
                        config,
                        &mut timers,
                    ) == StreamState::Disconnected
                    {
                        stream.close(&self.identity.core.registry);
                        continue;
                    }
                }
                (stream, timers, config.name)
            };

            self.connections.push(Connection {
                token,
                peer,
                endpoint: None,
                state: ConnectionState::Connected(stream),
                close_when_drained: false,
                write_side: WriteSide::Open,
                timers,
            });
            self.accepted += 1;
            info!(group = group_name, %peer, "connection accepted");
            handler(StreamEvent::Accepted { token, peer });
        }
    }

    /// Counts one refused connection, warning at most once every
    /// [`BACKLOG_WARNING_INTERVAL_SECS`] seconds: a group at its cap refuses
    /// as often as clients arrive, and the count is what says how often that
    /// is.
    fn refuse(&mut self, peer: Peer, max: usize) {
        self.refused += 1;
        if self
            .last_refusal_warning
            .is_some_and(|last| last.elapsed() < Duration::from_secs(BACKLOG_WARNING_INTERVAL_SECS))
        {
            return;
        }
        warn!(
            group = self.config.name,
            %peer,
            max_connections = max,
            refused = self.refused,
            "refusing a connection: the group is at its connection cap"
        );
        self.last_refusal_warning = Some(Instant::now());
    }

    /// Connections refused since the group was added because it was at its
    /// connection cap.
    pub fn refused_connections(&self) -> u64 {
        self.refused
    }

    fn serve_connection<F>(&mut self, index: usize, event: &Event, handler: &mut F)
    where
        F: for<'a> FnMut(StreamEvent<'a>),
    {
        if matches!(self.connections[index].state, ConnectionState::Connecting(_)) &&
            !self.finish_connect(index, handler)
        {
            return;
        }
        if !matches!(self.connections[index].state, ConnectionState::Connected(_)) {
            return;
        }

        let token = self.connections[index].token;
        let peer = self.connections[index].peer;
        let (state, queue_empty) = {
            let Self { config, connections, identity, .. } = self;
            let connection = &mut connections[index];
            let ConnectionState::Connected(stream) = &mut connection.state else { unreachable!() };
            let state = stream.poll_with(
                &identity.core.registry,
                event,
                config,
                &mut connection.timers,
                &mut |payload, send_ts| {
                    handler(StreamEvent::Message { token, payload, send_ts });
                },
            );
            (state, stream.send_queue.is_empty())
        };
        if state == StreamState::Disconnected {
            handler(StreamEvent::Disconnected { token, peer });
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
                let _ = self.registry().deregister(&mut socket);
                let _ = socket.shutdown(Shutdown::Both);
                false
            }
            ConnectionState::Connected(mut stream) => {
                stream.close(self.registry());
                true
            }
        }
    }

    fn disconnect_index(&mut self, index: usize, notify: bool) {
        let event = PendingDisconnect {
            token: self.connections[index].token,
            peer: self.connections[index].peer,
        };
        let accepted = self.connections[index].endpoint.is_none();
        self.connections[index].close_when_drained = false;
        self.connections[index].write_side = WriteSide::Open;
        let was_connected = self.close_connection_socket(index);
        if accepted {
            self.connections.swap_remove(index);
            self.accepted -= 1;
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
            handler(StreamEvent::Disconnected { token: event.token, peer: event.peer });
        }
    }

    /// The index of `token`'s connection when it can take a send right now:
    /// connected, its write side open, and not queued to close.
    fn sendable_index(&self, token: Token) -> Option<usize> {
        self.connections.iter().position(|connection| {
            connection.token == token &&
                !connection.close_when_drained &&
                connection.write_side == WriteSide::Open &&
                matches!(connection.state, ConnectionState::Connected(_))
        })
    }

    /// Whether at least one connection of this group can receive a broadcast
    /// right now.
    fn has_broadcast_recipient(&self) -> bool {
        self.connections.iter().any(|connection| {
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
    fn append_frame<F>(&mut self, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let Self { config, send_buffer, .. } = self;
        let framed = config.framing == Framing::LengthPrefixed;
        let start = send_buffer.len();
        if framed {
            send_buffer.resize(start + FRAME_HEADER_SIZE, 0);
        }
        let payload_start = send_buffer.len();
        let mut payload = PayloadBuf::new(send_buffer);
        serialise(&mut payload);
        let payload_len = payload.len();
        if payload_len == 0 {
            send_buffer.truncate(start);
            return false;
        }
        if payload_len > config.max_frame_size || (framed && u32::try_from(payload_len).is_err()) {
            error!(
                group = config.name,
                payload_len,
                max_frame_size = config.max_frame_size,
                "payload exceeds maximum frame size"
            );
            send_buffer.truncate(start);
            return false;
        }
        if framed {
            write_frame_len(&mut send_buffer[start..payload_start], payload_len);
        }
        true
    }

    /// Writes `ts` into the header of every frame staged in `send_buffer`.
    /// Raw-framed groups carry no headers and are left untouched.
    fn stamp_frames(&mut self, ts: Nanos) {
        if self.config.framing != Framing::LengthPrefixed {
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
        let state = {
            let Self { config, connections, identity, send_buffer, .. } = self;
            let connection = &mut connections[index];
            let ConnectionState::Connected(stream) = &mut connection.state else { unreachable!() };
            stream.write_frame(
                &identity.core.registry,
                None,
                send_buffer,
                config,
                &mut connection.timers,
            )
        };
        if state == StreamState::Disconnected {
            self.disconnect_index(index, true);
            return false;
        }
        true
    }

    /// Writes the staged frames to every connection that can receive a
    /// broadcast, disconnecting those whose write fails. Returns the number
    /// of recipients attempted.
    fn broadcast_staged(&mut self) -> usize {
        let mut attempted = 0;
        let mut index = self.connections.len();
        while index != 0 {
            index -= 1;
            if self.connections[index].close_when_drained ||
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

    /// Serializes and sends one payload to a connected token. Length-prefixed
    /// groups add a frame header; raw-framed groups send the payload
    /// unchanged. The closure is not called when the token is unknown or
    /// currently disconnected.
    pub fn send_with<F>(&mut self, token: Token, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let Some(index) = self.sendable_index(token) else {
            return false;
        };
        self.send_buffer.clear();
        if !self.append_frame(serialise) {
            return false;
        }
        self.stamp_frames(Nanos::now());
        self.write_staged(index)
    }

    /// Serializes and sends multiple payloads to a connected token.
    /// Length-prefixed groups preserve each payload as a separate frame and
    /// share one send timestamp; raw-framed groups concatenate the payloads.
    /// The batch uses one socket write when no backlog exists. Each payload
    /// is checked against `max_frame_size`, and invalid payloads are skipped;
    /// the caller must bound the item count or the total batch size, because
    /// `max_backlog_bytes` only limits bytes queued after a partial write. The
    /// closure is not called when the token is unknown or currently
    /// disconnected.
    pub fn send_many_with<I, F>(&mut self, token: Token, items: I, mut serialise: F) -> bool
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        let Some(index) = self.sendable_index(token) else {
            return false;
        };
        self.send_buffer.clear();
        for item in items {
            self.append_frame(|buf| serialise(buf, item));
        }
        if self.send_buffer.is_empty() {
            return false;
        }
        self.stamp_frames(Nanos::now());
        self.write_staged(index)
    }

    /// Serializes one payload and sends it to every connected token of this
    /// group, reporting how many were attempted. The closure is not called
    /// when no connection can receive it.
    pub fn broadcast_with<F>(&mut self, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        if !self.has_broadcast_recipient() {
            return 0;
        }
        self.send_buffer.clear();
        if !self.append_frame(serialise) {
            return 0;
        }
        self.stamp_frames(Nanos::now());
        self.broadcast_staged()
    }

    /// Serializes multiple payloads once and sends the batch to every
    /// connected token of this group, reporting how many were attempted.
    /// Framing, size limits and the skipping of invalid payloads follow
    /// [`ConnectionGroup::send_many_with`]; each recipient receives the batch
    /// in one socket write when it has no backlog. The closure is not called
    /// when no connection can receive it.
    pub fn broadcast_many_with<I, F>(&mut self, items: I, mut serialise: F) -> usize
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        if !self.has_broadcast_recipient() {
            return 0;
        }
        self.send_buffer.clear();
        for item in items {
            self.append_frame(|buf| serialise(buf, item));
        }
        if self.send_buffer.is_empty() {
            return 0;
        }
        self.stamp_frames(Nanos::now());
        self.broadcast_staged()
    }

    /// Closes one connection now, queueing its disconnect for delivery.
    pub fn disconnect(&mut self, token: Token) -> bool {
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

    /// Closes one connection once its queued bytes have reached the peer.
    pub fn disconnect_when_drained(&mut self, token: Token) -> bool {
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

    /// Shuts the write side of one connection once its queued bytes have
    /// reached the peer, leaving the read side open.
    pub fn shutdown_write_when_drained(&mut self, token: Token) -> bool {
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

    /// Whether the write side of one connection has been shut.
    pub(crate) fn write_side_shut(&self, token: Token) -> bool {
        self.connections
            .iter()
            .any(|connection| connection.token == token && connection.write_side == WriteSide::Shut)
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

    /// Drops one connection and any outbound endpoint behind it, without
    /// queueing a disconnect.
    pub fn remove(&mut self, token: Token) -> bool {
        let Some(index) = self.connections.iter().position(|connection| connection.token == token)
        else {
            return false;
        };
        self.close_connection_socket(index);
        let removed = self.connections.swap_remove(index);
        if removed.endpoint.is_none() {
            self.accepted -= 1;
        }
        self.pending_disconnects.retain(|event| event.token != token);
        true
    }
}

/// A poll, the token space behind it, and the group lifecycle of one network.
///
/// [`Self::default`] makes an **Owned** network, which creates its own poll and
/// drives it in [`Self::drive`]. [`Self::with_registry`] makes an **External**
/// one, which registers on a registry cloned from the caller's poll and never
/// polls: the caller polls and makes the three calls [`Self::next_deadline`],
/// [`Self::handle_event`] and [`Self::tick`]. The [`crate::stream`] module
/// documents the External loop in full.
///
/// The network owns no transport state. [`Self::add_group`] hands out a
/// [`ConnectionGroup`] that owns its own, and a service owns that group; the
/// network keeps only what every group shares and which of its group slots
/// are open.
pub struct StreamNetwork {
    poll: PollMode,
    core: Arc<NetworkCore>,
    /// Whether each group slot this network created is still open. Omission is
    /// not a lifecycle state: an open slot with no service in a scheduling
    /// call is a configuration error, not an idle group.
    active_groups: Vec<bool>,
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
            core: Arc::new(NetworkCore {
                registry,
                token_base: Token(0),
                next_token: AtomicUsize::new(0),
            }),
            active_groups: Vec::with_capacity(INITIAL_GROUP_CAPACITY),
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
            core: Arc::new(NetworkCore {
                registry,
                token_base,
                next_token: AtomicUsize::new(token_base.0),
            }),
            active_groups: Vec::with_capacity(INITIAL_GROUP_CAPACITY),
        }
    }

    /// Creates a group and hands over the transport state it owns.
    ///
    /// Moving the group into a service is the claim. Until then it can be
    /// configured and can register sockets, but it cannot be scheduled: only a
    /// [`Service`] reaches the driver.
    #[must_use = "the group is the transport state a service owns"]
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
        let slot = self.active_groups.len();
        self.active_groups.push(true);
        ConnectionGroup::new(ConnectionGroupId::new(Arc::clone(&self.core), slot), config)
    }

    /// Hard-closes a group's sockets, discards its queued events and closes
    /// its slot, consuming the group.
    ///
    /// Closing an [`Endpoint::Unix`] listener unlinks its socket file. The
    /// slot does not return to a reusable state: a closed group is gone, and
    /// the remaining services carry on.
    ///
    /// # Panics
    /// A group of another network, or one already closed.
    pub fn remove_group(&mut self, mut group: ConnectionGroup) {
        assert!(
            group.identity.belongs_to(&self.core),
            "this connection group belongs to another network"
        );
        let slot = group.identity.slot;
        assert!(self.active_groups[slot], "connection group {slot} is already closed");
        group.close();
        self.active_groups[slot] = false;
    }

    /// Runs one iteration of an Owned-mode network: fold the deadlines, poll
    /// once, route every readiness event to the service that owns it, then
    /// tick each service once. Reports whether anything happened, a service
    /// with events left to pull included.
    ///
    /// `max_timeout` caps the poll wait; `None` leaves it bounded only by the
    /// services' own deadlines.
    ///
    /// # Panics
    /// An External-mode network owns no poll and refuses this call. Every open
    /// group must appear exactly once among `services`, and each service must
    /// reach its group's `maintain` and fold its group's deadline.
    pub fn drive<S: Service>(
        &mut self,
        max_timeout: Option<Duration>,
        services: &mut [S],
    ) -> bool {
        assert!(matches!(self.poll, PollMode::Owned { .. }), "{EXTERNAL_POLLS}");
        let now = Instant::now();
        self.validate(services);

        let timeout = Self::poll_timeout(max_timeout, services, now);
        let mut worked = false;
        {
            let PollMode::Owned { poll, events, .. } = &mut self.poll else {
                unreachable!("the mode is checked above");
            };
            match poll.poll(events, timeout) {
                Ok(()) => {
                    for event in &*events {
                        // A wake asks the poll to return and nothing more, so
                        // it reaches neither a service nor the did-work
                        // result.
                        if event.token() != WAKER_TOKEN {
                            worked |= route_ready(event, services);
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => flux_utils::safe_panic!("couldn't poll the stream network: {err}"),
            }
        }

        // The instant this iteration began is older than the poll wait it has
        // just come out of, which may have been the whole of a timeout.
        let ticked = Self::tick_services(services, Instant::now());
        worked || ticked
    }

    /// The earliest instant one of `services` needs a tick at: the fold of
    /// every service's combined transport and protocol deadline.
    ///
    /// An External-mode caller folds only its own timers against this and
    /// converts the result into the timeout of its poll; `None` leaves that
    /// poll free to block until a socket is ready.
    ///
    /// # Panics
    /// An Owned-mode network folds its deadlines inside [`Self::drive`], and
    /// refuses this call. Every open group must appear exactly once among
    /// `services` — an omitted service is a configuration error the first
    /// call reports, not one that waits for a deadline to be missed.
    pub fn next_deadline<S: Service>(&self, services: &[S]) -> Option<Instant> {
        assert!(matches!(self.poll, PollMode::External), "{OWNED_POLLS}");
        self.validate(services);
        Self::fold_deadline(services)
    }

    /// Takes one readiness event from the caller's poll, reporting whether it
    /// was this network's.
    ///
    /// The network's tokens run from the base it was built with up to the
    /// high-water mark of its allocation. A token outside that range belongs
    /// to a source the caller registered — its waker included — and is handed
    /// straight back as `false`, untouched and unlogged. A token inside it is
    /// this network's: the event is offered to each service in slice order
    /// until one owns it, along with any disconnect that handling it
    /// produced, and the call reports `true`. A token no group holds is one of
    /// this network's that closed since the poll returned: ours, and ignored.
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
    pub fn handle_event<S: Service>(&mut self, event: &Event, services: &mut [S]) -> bool {
        assert!(matches!(self.poll, PollMode::External), "{OWNED_POLLS}");
        if !self.core.is_ours(event.token()) {
            return false;
        }
        self.validate(services);
        route_ready(event, services);
        true
    }

    /// Ticks each service once in slice order, after the caller has handed
    /// over every event its poll returned. Reports whether anything happened,
    /// a service with events left to pull included.
    ///
    /// Each service runs its group's due transport work and routes the events
    /// that produces into its protocol state before its own timers, so
    /// protocol state never lags transport state by an iteration.
    ///
    /// # Panics
    /// An Owned-mode network ticks inside [`Self::drive`], and refuses this
    /// call. Every open group must appear exactly once among `services`, as
    /// for [`Self::drive`].
    pub fn tick<S: Service>(&mut self, services: &mut [S]) -> bool {
        assert!(matches!(self.poll, PollMode::External), "{OWNED_POLLS}");
        self.validate(services);
        Self::tick_services(services, Instant::now())
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
        assert!(!*waker_taken, "this network has already handed out its waker");
        let waker = Waker::new(poll.registry(), WAKER_TOKEN)?;
        *waker_taken = true;
        Ok(waker)
    }

    /// Ticks every service once, in slice order, verifying that each reached
    /// its group's maintenance.
    fn tick_services<S: Service>(services: &mut [S], now: Instant) -> bool {
        let mut worked = false;
        for service in services.iter_mut() {
            service.group_id().arm_maintain();
            worked |= service.tick(now);
            service.group_id().assert_maintained();
        }
        worked
    }

    /// Panics unless `services` names every open group exactly once, and each
    /// of them belongs to this network.
    fn validate<S: Service>(&self, services: &[S]) {
        for (position, service) in services.iter().enumerate() {
            let id = service.group_id();
            assert!(
                id.belongs_to(&self.core),
                "a service of another network was passed to this one: its connection group was \
                 created by a different StreamNetwork"
            );
            assert!(
                self.active_groups.get(id.slot).copied().unwrap_or(false),
                "connection group {} is closed",
                id.slot
            );
            assert!(
                services[..position].iter().all(|other| other.group_id().slot != id.slot),
                "duplicate service for group {}",
                id.slot
            );
        }
        for (slot, active) in self.active_groups.iter().enumerate() {
            assert!(
                !active || services.iter().any(|service| service.group_id().slot == slot),
                "connection group {slot} has no service — pass every service, or close the group \
                 before dropping it"
            );
        }
    }

    /// The earliest instant one of `services` needs a tick at, verifying that
    /// each folded in the deadline its group reported.
    fn fold_deadline<S: Service>(services: &[S]) -> Option<Instant> {
        let mut next: Option<Instant> = None;
        for service in services {
            let id = service.group_id();
            id.arm_deadline();
            let deadline = service.next_deadline();
            id.assert_deadline_included(deadline);
            if let Some(deadline) = deadline {
                next = Some(next.map_or(deadline, |next: Instant| next.min(deadline)));
            }
        }
        next
    }

    /// Folds the caller's cap and every service's deadline into the timeout of
    /// one poll.
    fn poll_timeout<S: Service>(
        max_timeout: Option<Duration>,
        services: &[S],
        now: Instant,
    ) -> Option<std::time::Duration> {
        let deadline = Self::fold_deadline(services);
        let timeout = match (max_timeout, deadline.map(|deadline| deadline.saturating_sub(now))) {
            (Some(max), Some(next)) => Some(max.min(next)),
            (max, next) => max.or(next),
        };
        timeout.map(std::time::Duration::from)
    }
}

/// Offers one readiness event to each service in slice order until one owns
/// it, reporting whether handling it produced work.
fn route_ready<S: Service>(event: &Event, services: &mut [S]) -> bool {
    for service in services.iter_mut() {
        let outcome = service.handle_event(event);
        if outcome.is_owned() {
            return outcome.worked();
        }
    }
    // A token inside this network's range that no group holds is one of ours
    // that closed since the poll returned.
    debug!(token = ?event.token(), "ignoring stale readiness event");
    false
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
        FramedStream, Peer, StreamState, TcpOptions, TransportStream, set_socket_buf_size,
        write_frame_header,
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
}
