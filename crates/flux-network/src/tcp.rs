use std::{
    io::{self, IoSlice, Read, Write},
    net::{IpAddr, Shutdown, SocketAddr},
    os::fd::{AsRawFd, FromRawFd},
    ptr,
};

use flux_communication::Timer;
use flux_timing::{Duration, Instant, Nanos, Repeater};
use flux_utils::{DCache, DCacheRef};
use mio::{Interest, Registry, Token, event::Event as MioEvent, net::TcpListener};
use rustc_hash::{FxBuildHasher, FxHashMap};
use tracing::{debug, error, info, warn};

use crate::{
    network::{
        Event, Framing, Group, NetworkTelemetry, PayloadBuf, ReplayPolicy, RxPayload,
        TcpGroupConfig, Tokens, set_socket_buf_size,
    },
    tls::Session,
};

const INITIAL_CONNECTION_CAPACITY: usize = 8;
const INITIAL_LISTENER_CAPACITY: usize = 2;
const INITIAL_RX_BUFFER_SIZE: usize = 32 * 1024;
const INITIAL_SEND_BUFFER_SIZE: usize = 32 * 1024;
const BACKLOG_WARNING_INTERVAL_SECS: u64 = 10;

pub const DEFAULT_TCP_USER_TIMEOUT_MS: u32 = 10_000;
const DEFAULT_TCP_KEEPALIVE_IDLE_SECS: libc::c_int = 5;
const DEFAULT_TCP_KEEPALIVE_INTERVAL_SECS: libc::c_int = 2;
const DEFAULT_TCP_KEEPALIVE_PROBES: libc::c_int = 3;
/// Frame length prefix.
const LEN_HEADER_SIZE: usize = 4;
/// Nanos timestamp when the sender finished serialising and handed bytes to
/// kernel or enqueued in backlog.
const TS_HEADER_SIZE: usize = 8;
pub(crate) const FRAME_HEADER_SIZE: usize = LEN_HEADER_SIZE + TS_HEADER_SIZE;
// TODO: might need to tweak these

/// Write the `[len][ts]` frame header for a `payload_len`-byte payload.
///
/// Shared by the per-stream serialise path and the broadcast path
#[inline]
pub(crate) fn write_frame_header(
    header: &mut [u8; FRAME_HEADER_SIZE],
    payload_len: usize,
    ts: Nanos,
) {
    write_frame_len(header, payload_len);
    write_frame_ts(header, ts);
}

/// Write only the `[len]` half of a frame header. Used when frames are staged
/// ahead of the write and stamped with `write_frame_ts` once they go out.
///
/// Panics above `u32::MAX`: a truncated length would corrupt the stream.
#[inline]
pub(crate) fn write_frame_len(header: &mut [u8], payload_len: usize) {
    assert!(
        u32::try_from(payload_len).is_ok(),
        "tcp payload too large for 4-byte frame header: {payload_len} bytes"
    );
    header[..LEN_HEADER_SIZE].copy_from_slice(&(payload_len as u32).to_le_bytes());
}

/// Write only the `[ts]` half of a frame header.
#[inline]
pub(crate) fn write_frame_ts(header: &mut [u8], ts: Nanos) {
    header[LEN_HEADER_SIZE..FRAME_HEADER_SIZE].copy_from_slice(&ts.0.to_le_bytes());
}

/// Read the payload length from a frame header.
#[inline]
pub(crate) fn frame_payload_len(header: &[u8]) -> usize {
    u32::from_le_bytes(header[..LEN_HEADER_SIZE].try_into().unwrap()) as usize
}

/// Read the send timestamp from a frame header.
#[inline]
pub(crate) fn frame_send_ts(header: &[u8]) -> Nanos {
    Nanos(u64::from_le_bytes(header[LEN_HEADER_SIZE..FRAME_HEADER_SIZE].try_into().unwrap()))
}

#[cfg(target_os = "linux")]
pub(crate) fn set_user_timeout(socket: &impl AsRawFd, timeout_ms: u32) {
    let fd = socket.as_raw_fd();
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_USER_TIMEOUT,
            ptr::from_ref(&timeout_ms).cast::<libc::c_void>(),
            core::mem::size_of::<u32>() as libc::socklen_t,
        );
    }
}

/// Set `TCP_USER_TIMEOUT` on a socket. Stub for non-Linux platforms.
#[cfg(not(target_os = "linux"))]
pub(crate) fn set_user_timeout(_socket: &impl AsRawFd, _timeout_ms: u32) {
    // TCP_USER_TIMEOUT is not supported on non-Linux platforms.
}

/// Enable TCP keepalive with short failure detection for silent connections.
#[cfg(target_os = "linux")]
pub(crate) fn set_keepalive(socket: &impl AsRawFd) -> io::Result<()> {
    let fd = socket.as_raw_fd();
    for (level, option, value) in [
        (libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1),
        (libc::IPPROTO_TCP, libc::TCP_KEEPIDLE, DEFAULT_TCP_KEEPALIVE_IDLE_SECS),
        (libc::IPPROTO_TCP, libc::TCP_KEEPINTVL, DEFAULT_TCP_KEEPALIVE_INTERVAL_SECS),
        (libc::IPPROTO_TCP, libc::TCP_KEEPCNT, DEFAULT_TCP_KEEPALIVE_PROBES),
    ] {
        let result = unsafe {
            libc::setsockopt(
                fd,
                level,
                option,
                ptr::from_ref(&value).cast::<libc::c_void>(),
                core::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn set_keepalive(_socket: &impl AsRawFd) -> io::Result<()> {
    Ok(())
}

struct Listener {
    token: Token,
    socket: TcpListener,
    /// Stopped at `max_accepts_per_poll` with connections still queued.
    /// Readiness is edge-triggered, so no new event announces them.
    accept_deferred: bool,
    /// Connections accepted since this poll's `pre_poll`, across its
    /// `pre_poll` and readiness phases.
    accepts_this_poll: usize,
    #[cfg(feature = "tls")]
    tls: Option<std::sync::Arc<crate::tls::ServerConfig>>,
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
    peer_addr: SocketAddr,
    /// Source address every dial of this outbound connection binds first.
    local_ip: Option<IpAddr>,
    kind: ConnectionKind,
    state: ConnectionState,
    close_when_drained: bool,
    broadcast_paused: bool,
    backlog: ByteQueue,
    timers: Option<NetworkTimers>,
    /// TLS for this socket; `None` leaves the wire in plaintext.
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
        telemetry: NetworkTelemetry,
        group_name: &str,
        token: Token,
        peer_addr: SocketAddr,
        framing: Framing,
    ) -> Option<Self> {
        let NetworkTelemetry::Enabled { app_name } = telemetry else { return None };
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
    token: Token,
    peer_addr: SocketAddr,
}

pub(crate) struct TcpManager {
    pub(crate) config: TcpGroupConfig,
    group: Group,
    reconnector: Repeater,
    listeners: Vec<Listener>,
    connections: Vec<Connection>,
    by_token: FxHashMap<Token, usize>,
    pending_disconnects: Vec<PendingDisconnect>,
    send_buffer: Vec<u8>,
    tls_buffer: Vec<u8>,
}

impl TcpManager {
    pub(crate) fn new(config: TcpGroupConfig, group: Group) -> Self {
        assert!(
            !config.aligned_payloads || config.framing == Framing::LengthPrefixed,
            "aligned receive requires framed TCP"
        );
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
        assert!(config.max_accepts_per_poll != Some(0), "max_accepts_per_poll must be nonzero");
        if let Some(max) = config.max_backlog_bytes {
            assert!(max > 0, "max_backlog_bytes must be nonzero");
            if let Some(warn) = config.backlog_warn_bytes {
                assert!(warn < max, "backlog_warn_bytes must be below max_backlog_bytes");
            }
        }
        assert!(
            config.max_backlog_frames.is_none() || config.framing == Framing::LengthPrefixed,
            "frame backlog limit requires framed TCP"
        );
        assert!(
            config.replay == ReplayPolicy::Drop || config.framing == Framing::LengthPrefixed,
            "replay requires framed TCP"
        );
        let reconnector = Repeater::every(config.reconnect_interval);
        Self {
            config,
            group,
            reconnector,
            listeners: Vec::with_capacity(INITIAL_LISTENER_CAPACITY),
            connections: Vec::with_capacity(INITIAL_CONNECTION_CAPACITY),
            by_token: FxHashMap::with_capacity_and_hasher(
                INITIAL_CONNECTION_CAPACITY,
                FxBuildHasher,
            ),
            pending_disconnects: Vec::with_capacity(INITIAL_CONNECTION_CAPACITY),
            send_buffer: Vec::with_capacity(INITIAL_SEND_BUFFER_SIZE),
            tls_buffer: Vec::new(),
        }
    }

    /// Deregisters every socket before the group is dropped. On Linux, a
    /// duplicated descriptor can keep an epoll registration alive after the
    /// original descriptor closes.
    pub(crate) fn close_all(&mut self, registry: &Registry) {
        for index in 0..self.connections.len() {
            self.close_connection_socket(registry, index);
        }
        for listener in &mut self.listeners {
            let _ = registry.deregister(&mut listener.socket);
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.connections.is_empty() && self.listeners.is_empty()
    }

    pub(crate) fn pre_poll<F>(
        &mut self,
        registry: &Registry,
        tokens: &mut Tokens,
        dcache: Option<&DCache>,
        handler: &mut F,
    ) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let mut work = self.drain_pending_disconnects(handler);
        for index in 0..self.listeners.len() {
            self.listeners[index].accepts_this_poll = 0;
            if self.listeners[index].accept_deferred {
                work = true;
                self.accept_connections(registry, index, tokens, dcache, handler);
            }
        }
        if !self.connections.is_empty() {
            self.maybe_reconnect(registry, tokens);
            if let Some((max, timeout)) = self.config.max_backlog_frames {
                self.check_backlogs(registry, max, timeout, tokens);
            }
        }
        work
    }

    pub(crate) fn has_deferred_accepts(&self) -> bool {
        self.listeners.iter().any(|listener| listener.accept_deferred)
    }

    pub(crate) fn post_poll<F>(&mut self, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        self.drain_pending_disconnects(handler)
    }

    pub(crate) fn currently_disconnected(&self) -> impl Iterator<Item = Token> + '_ {
        self.connections
            .iter()
            .filter(|c| {
                c.kind == ConnectionKind::Outbound &&
                    (!matches!(c.state, ConnectionState::Connected(_)) || c.is_handshaking())
            })
            .map(|c| c.token)
    }

    pub(crate) fn force_reconnect(&mut self, registry: &Registry) {
        for index in 0..self.connections.len() {
            self.start_connect(registry, index);
        }
    }

    pub(crate) fn disconnect_outbound(&mut self, registry: &Registry, tokens: &mut Tokens) {
        for index in (0..self.connections.len()).rev() {
            if self.connections[index].kind == ConnectionKind::Outbound {
                self.disconnect_index(registry, index, true, tokens);
            }
        }
    }

    fn index_of(&self, token: Token) -> Option<usize> {
        self.by_token.get(&token).copied()
    }

    fn push_connection(&mut self, connection: Connection) -> usize {
        let index = self.connections.len();
        self.by_token.insert(connection.token, index);
        self.connections.push(connection);
        index
    }

    fn swap_remove_connection(&mut self, index: usize) {
        let removed = self.connections.swap_remove(index);
        self.by_token.remove(&removed.token);
        if let Some(moved) = self.connections.get(index) {
            self.by_token.insert(moved.token, index);
        }
    }

    pub(crate) fn pause_broadcast(&mut self, token: Token) {
        if let Some(index) = self.index_of(token) {
            self.connections[index].broadcast_paused = true;
        }
    }

    pub(crate) fn resume_broadcast(&mut self, token: Token) {
        if let Some(index) = self.index_of(token) {
            self.connections[index].broadcast_paused = false;
        }
    }

    pub(crate) fn is_broadcast_paused(&self, token: Token) -> bool {
        self.index_of(token).is_some_and(|index| self.connections[index].broadcast_paused)
    }

    pub(crate) fn listen(
        &mut self,
        registry: &Registry,
        addr: SocketAddr,
        tokens: &mut Tokens,
    ) -> io::Result<Token> {
        let group = self.group;
        let mut socket = bind_listener(addr, &self.config)?;
        let token = tokens.allocate(group);
        if let Err(err) = registry.register(&mut socket, token, Interest::READABLE) {
            tokens.retire(token);
            return Err(err);
        }
        self.listeners.push(Listener {
            token,
            socket,
            accept_deferred: false,
            accepts_this_poll: 0,
            #[cfg(feature = "tls")]
            tls: None,
        });
        Ok(token)
    }

    #[cfg(feature = "tls")]
    pub(crate) fn listen_tls(
        &mut self,
        registry: &Registry,
        addr: SocketAddr,
        tokens: &mut Tokens,
        config: std::sync::Arc<crate::tls::ServerConfig>,
    ) -> io::Result<Token> {
        if config.max_early_data_size != 0 ||
            self.config.on_connect_msg.is_some() ||
            self.config.max_backlog_frames.is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS listeners need no early data, no connect message and byte backlog limits",
            ));
        }
        let token = self.listen(registry, addr, tokens)?;
        self.listeners.last_mut().unwrap().tls = Some(config);
        Ok(token)
    }

    /// Queued wire bytes, or `None` if `token` cannot take writes yet.
    pub(crate) fn pending_write_bytes(&self, token: Token) -> Option<usize> {
        let index = self.sendable_index(token)?;
        match &self.connections[index].state {
            ConnectionState::Connected(stream) => Some(stream.send_queue.len()),
            _ => None,
        }
    }

    pub(crate) fn connect(
        &mut self,
        registry: &Registry,
        peer_addr: SocketAddr,
        local_ip: Option<IpAddr>,
        tls: Option<Box<Session>>,
        tokens: &mut Tokens,
    ) -> Token {
        let group = self.group;
        let token = tokens.allocate(group);
        let config = &self.config;
        let timers =
            NetworkTimers::new(config.telemetry, config.name, token, peer_addr, config.framing);
        let index = self.push_connection(Connection {
            token,
            peer_addr,
            local_ip,
            kind: ConnectionKind::Outbound,
            state: ConnectionState::Disconnected,
            close_when_drained: false,
            broadcast_paused: false,
            backlog: ByteQueue::framed(),
            timers,
            tls,
        });
        self.start_connect(registry, index);
        token
    }

    fn start_connect(&mut self, registry: &Registry, index: usize) {
        let connection = &self.connections[index];
        if connection.kind != ConnectionKind::Outbound ||
            !matches!(connection.state, ConnectionState::Disconnected)
        {
            return;
        }

        let token = connection.token;
        let peer_addr = connection.peer_addr;
        let local_ip = connection.local_ip;
        let socket_buf_size = self.config.socket_buf_size;

        let socket = local_ip.map_or_else(
            || mio::net::TcpStream::connect(peer_addr),
            |local_ip| connect_from(peer_addr, local_ip),
        );
        let Ok(mut socket) = socket.inspect_err(
            |err| debug!(?err, %peer_addr, ?local_ip, "couldn't start tcp connection"),
        ) else {
            return;
        };
        if let Some(size) = socket_buf_size {
            set_socket_buf_size(&socket, size);
        }
        if let Err(err) = registry.register(&mut socket, token, Interest::WRITABLE) {
            warn!(?err, %peer_addr, "couldn't register connecting tcp stream");
            let _ = socket.shutdown(Shutdown::Both);
            return;
        }
        self.connections[index].state = ConnectionState::Connecting(socket);
    }

    /// Drops TLS connections whose handshake never finished. Outbound ones
    /// are redialled by the usual reconnect sweep without an event, since none
    /// was announced. Accepted ones were announced, so they report the
    /// disconnect and leave the vector; returns true in that case.
    fn drop_stalled_handshake(
        &mut self,
        registry: &Registry,
        index: usize,
        tokens: &mut Tokens,
    ) -> bool {
        let timeout = self.config.handshake_timeout;
        if !self.connections[index].tls.as_ref().is_some_and(|tls| tls.handshake_stalled(timeout)) {
            return false;
        }
        let peer_addr = self.connections[index].peer_addr;
        warn!(%peer_addr, %timeout, "tls handshake timed out");
        let accepted = self.connections[index].kind == ConnectionKind::Accepted;
        let notify = self.connections[index].announced();
        self.disconnect_index(registry, index, notify, tokens);
        accepted
    }

    fn maybe_reconnect(&mut self, registry: &Registry, tokens: &mut Tokens) {
        if !self.reconnector.fired_at(Instant::now()) {
            return;
        }
        // Handshake timeouts are swept on the reconnect tick.
        let mut index = 0;
        while index < self.connections.len() {
            if !self.drop_stalled_handshake(registry, index, tokens) {
                index += 1;
            }
        }
        self.force_reconnect(registry);
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

    #[allow(clippy::too_many_lines)]
    fn finish_connect<F>(
        &mut self,
        registry: &Registry,
        index: usize,
        dcache: Option<&DCache>,
        handler: &mut F,
    ) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let ConnectionState::Connecting(socket) = &self.connections[index].state else {
            return false;
        };
        match Self::connect_complete(socket) {
            Ok(false) => return false,
            Err(err) => {
                let peer_addr = self.connections[index].peer_addr;
                debug!(?err, %peer_addr, "tcp connection attempt failed");
                self.close_connection_socket(registry, index);
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
        let group = self.group;
        let peer_addr = self.connections[index].peer_addr;
        let mut timers = self.connections[index].timers;
        // Opens the handshake before the group config is borrowed below.
        let is_tls = self.connections[index].tls.is_some();
        let mut handshake = Vec::new();
        if let Some(tls) = self.connections[index].tls.as_mut() {
            tls.start(&mut handshake);
        }
        let config = &self.config;
        let group_name = config.name;

        if config.nodelay &&
            let Err(err) = socket.set_nodelay(true)
        {
            warn!(?err, %peer_addr, "couldn't set nodelay on tcp stream");
            let _ = registry.deregister(&mut socket);
            let _ = socket.shutdown(Shutdown::Both);
            return false;
        }
        if config.keepalive &&
            let Err(err) = set_keepalive(&socket)
        {
            warn!(?err, %peer_addr, "couldn't set keepalive on tcp stream");
            let _ = registry.deregister(&mut socket);
            let _ = socket.shutdown(Shutdown::Both);
            return false;
        }
        set_user_timeout(&socket, config.user_timeout_ms);
        if let Err(err) = registry.reregister(&mut socket, token, Interest::READABLE) {
            warn!(?err, %peer_addr, "couldn't register connected tcp stream");
            let _ = socket.shutdown(Shutdown::Both);
            return false;
        }

        let mut stream = FramedStream::new(socket, token, peer_addr, config, dcache.is_some());
        if is_tls {
            stream.send_queue.framed = false;
        }
        // A replayed frame always restarts at its header, whatever a failed
        // drain left `head` at; done before the dedupe below reads the queue.
        self.connections[index].backlog.rewind_to_frame();
        // TLS sends its opening flight here; `on_connect_msg` waits for the
        // finished handshake so it travels encrypted.
        let queued_greeting = config.on_connect_msg.as_ref().is_some_and(|message| {
            let backlog = self.connections[index].backlog.remaining();
            backlog.len() >= FRAME_HEADER_SIZE &&
                frame_payload_len(backlog) == message.len() &&
                backlog.get(FRAME_HEADER_SIZE..FRAME_HEADER_SIZE + message.len()) ==
                    Some(message.as_slice())
        });
        let first = if is_tls {
            Some(&handshake[..])
        } else if queued_greeting {
            None
        } else {
            config.on_connect_msg.as_deref()
        };
        if let Some(message) = first {
            // Handshake records are the wire itself and are never framed.
            let header = (!is_tls && config.framing == Framing::LengthPrefixed).then(|| {
                let mut header = [0; FRAME_HEADER_SIZE];
                write_frame_header(&mut header, message.len(), Nanos::now());
                header
            });
            if stream.write_frame(registry, header.as_ref(), message, config, &mut timers) ==
                StreamState::Disconnected
            {
                stream.close(registry);
                self.connections[index].timers = timers;
                return false;
            }
        }

        let backlog = std::mem::replace(&mut self.connections[index].backlog, ByteQueue::framed());
        if !backlog.is_empty() {
            if stream.send_queue.is_empty() {
                // The usual case: the greeting went out whole, so the retained
                // queue becomes the live one without a copy.
                stream.send_queue = backlog;
            } else {
                // A queued greeting remainder precedes the retained frames.
                stream.send_queue.append_raw_remainder(backlog.remaining(), 0);
            }
            if stream.drain_queue(registry, &self.config) == StreamState::Disconnected {
                stream.close(registry);
                stream.send_queue.rewind_to_frame();
                self.connections[index].backlog = stream.send_queue;
                return false;
            }
            if !stream.send_queue.is_empty() {
                stream.arm_writable(registry);
            }
        }
        self.connections[index].timers = timers;
        self.connections[index].state = ConnectionState::Connected(stream);
        info!(group = group_name, %peer_addr, "tcp connection established");
        if !is_tls {
            handler(Event::Connected { group, token, peer_addr });
        }
        true
    }

    fn accept_connections<F>(
        &mut self,
        registry: &Registry,
        listener_index: usize,
        tokens: &mut Tokens,
        dcache: Option<&DCache>,
        handler: &mut F,
    ) where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let group = self.group;
        let max_accepts = self.config.max_accepts_per_poll.unwrap_or(usize::MAX);
        self.listeners[listener_index].accept_deferred = false;
        loop {
            if self.listeners[listener_index].accepts_this_poll == max_accepts {
                self.listeners[listener_index].accept_deferred = true;
                return;
            }
            let accepted = self.listeners[listener_index].socket.accept();
            let (mut socket, peer_addr) = match accepted {
                Ok(accepted) => accepted,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => {
                    warn!(?err, group = self.config.name, "tcp accept failed");
                    break;
                }
            };
            self.listeners[listener_index].accepts_this_poll += 1;
            #[cfg(feature = "tls")]
            let tls = match &self.listeners[listener_index].tls {
                Some(config) => match Session::accept(config.clone()) {
                    Ok(session) => Some(Box::new(session)),
                    Err(err) => {
                        warn!(?err, %peer_addr, "couldn't create server TLS session");
                        let _ = socket.shutdown(Shutdown::Both);
                        continue;
                    }
                },
                None => None,
            };
            #[cfg(not(feature = "tls"))]
            let tls = None;
            let (token, stream, timers, group_name) = {
                let config = &self.config;
                let token = tokens.allocate(group);
                if let Err(err) = registry.register(&mut socket, token, Interest::READABLE) {
                    warn!(?err, %peer_addr, "couldn't register accepted tcp stream");
                    let _ = socket.shutdown(Shutdown::Both);
                    tokens.retire(token);
                    continue;
                }

                let mut timers = NetworkTimers::new(
                    config.telemetry,
                    config.name,
                    token,
                    peer_addr,
                    config.framing,
                );
                let mut stream =
                    FramedStream::new(socket, token, peer_addr, config, dcache.is_some());
                if let Some(message) = config.on_connect_msg.as_deref() {
                    let header = (config.framing == Framing::LengthPrefixed).then(|| {
                        let mut header = [0; FRAME_HEADER_SIZE];
                        write_frame_header(&mut header, message.len(), Nanos::now());
                        header
                    });
                    if stream.write_frame(registry, header.as_ref(), message, config, &mut timers) ==
                        StreamState::Disconnected
                    {
                        stream.close(registry);
                        tokens.retire(token);
                        continue;
                    }
                }
                (token, stream, timers, config.name)
            };

            self.push_connection(Connection {
                token,
                peer_addr,
                local_ip: None,
                kind: ConnectionKind::Accepted,
                state: ConnectionState::Connected(stream),
                close_when_drained: false,
                broadcast_paused: false,
                backlog: ByteQueue::framed(),
                timers,
                tls,
            });
            info!(group = group_name, %peer_addr, "tcp connection accepted");
            handler(Event::Accepted { group, token, peer_addr });
        }
    }

    pub(crate) fn handle_event<F>(
        &mut self,
        registry: &Registry,
        event: &MioEvent,
        tokens: &mut Tokens,
        dcache: Option<&DCache>,
        handler: &mut F,
    ) where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let token = event.token();
        if let Some(index) = self.listeners.iter().position(|listener| listener.token == token) {
            self.accept_connections(registry, index, tokens, dcache, handler);
            return;
        }
        let Some(index) = self.index_of(token) else {
            debug!(?token, "ignoring stale tcp readiness event");
            return;
        };

        if matches!(self.connections[index].state, ConnectionState::Connecting(_)) &&
            !self.finish_connect(registry, index, dcache, handler)
        {
            return;
        }
        if !matches!(self.connections[index].state, ConnectionState::Connected(_)) {
            return;
        }

        let group = self.group;
        let peer_addr = self.connections[index].peer_addr;
        let config = &self.config;
        let (state, queue_empty) = {
            let connection = &mut self.connections[index];
            let ConnectionState::Connected(stream) = &mut connection.state else { unreachable!() };
            let state = stream.poll_with(
                registry,
                event,
                config,
                &mut connection.timers,
                &mut connection.tls,
                dcache,
                &mut |payload, send_ts| match payload {
                    Some(payload) => handler(Event::Message { group, token, payload, send_ts }),
                    None => handler(Event::Connected { group, token, peer_addr }),
                },
            );
            (state, stream.send_queue.is_empty())
        };
        if state == StreamState::Disconnected {
            // A handshake that never completed was never announced.
            if self.connections[index].announced() {
                handler(Event::Disconnected { group, token, peer_addr });
            }
            self.disconnect_index(registry, index, false, tokens);
        } else if self.connections[index].close_when_drained && queue_empty {
            self.disconnect_index(registry, index, true, tokens);
        }
    }

    fn close_connection_socket(&mut self, registry: &Registry, index: usize) -> bool {
        let old_state =
            std::mem::replace(&mut self.connections[index].state, ConnectionState::Disconnected);
        match old_state {
            ConnectionState::Disconnected => false,
            ConnectionState::Connecting(mut socket) => {
                let _ = registry.deregister(&mut socket);
                let _ = socket.shutdown(Shutdown::Both);
                false
            }
            ConnectionState::Connected(mut stream) => {
                stream.close(registry);
                if self.connections[index].kind == ConnectionKind::Outbound &&
                    self.config.replay == ReplayPolicy::Replay
                {
                    stream.send_queue.rewind_to_frame();
                    self.connections[index].backlog = stream.send_queue;
                }
                true
            }
        }
    }

    fn disconnect_index(
        &mut self,
        registry: &Registry,
        index: usize,
        notify: bool,
        tokens: &mut Tokens,
    ) {
        let event = PendingDisconnect {
            token: self.connections[index].token,
            peer_addr: self.connections[index].peer_addr,
        };
        let kind = self.connections[index].kind;
        self.connections[index].close_when_drained = false;
        let announced = self.connections[index].announced();
        let was_connected = self.close_connection_socket(registry, index);
        if kind == ConnectionKind::Accepted {
            tokens.retire(event.token);
            self.swap_remove_connection(index);
        }
        if notify && was_connected && announced {
            self.pending_disconnects.push(event);
        }
    }

    fn drain_pending_disconnects<F>(&mut self, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        if self.pending_disconnects.is_empty() {
            return false;
        }
        for event in self.pending_disconnects.drain(..) {
            handler(Event::Disconnected {
                group: self.group,
                token: event.token,
                peer_addr: event.peer_addr,
            });
        }
        true
    }

    /// Finds the connection `token` can currently send on.
    fn sendable_index(&self, token: Token) -> Option<usize> {
        let index = self.index_of(token)?;
        let connection = &self.connections[index];
        (!connection.close_when_drained &&
            !connection.is_handshaking() &&
            (matches!(connection.state, ConnectionState::Connected(_)) ||
                self.config.replay == ReplayPolicy::Replay))
            .then_some(index)
    }

    /// Serialises one payload as a frame at the end of `send_buffer`.
    /// Length-prefixed groups reserve a header ahead of the payload and fill
    /// in its length here; the caller writes the timestamp just before the
    /// socket write. Empty or oversized payloads are removed again. Returns
    /// whether the frame was kept.
    fn append_frame<F>(config: &TcpGroupConfig, buffer: &mut Vec<u8>, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let framed = config.framing == Framing::LengthPrefixed;
        let start = buffer.len();
        if framed {
            buffer.resize(start + FRAME_HEADER_SIZE, 0);
        }
        let payload_start = buffer.len();
        let mut payload = PayloadBuf::new(buffer);
        serialise(&mut payload);
        let payload_len = payload.len();
        if payload_len == 0 {
            buffer.truncate(start);
            return false;
        }
        if payload_len > config.max_frame_size {
            error!(
                group = config.name,
                payload_len,
                max_frame_size = config.max_frame_size,
                "tcp payload exceeds maximum frame size"
            );
            buffer.truncate(start);
            return false;
        }
        if framed {
            write_frame_len(&mut buffer[start..payload_start], payload_len);
        }
        true
    }

    /// Writes `ts` into the header of every frame staged in `send_buffer`.
    fn stamp_frames(buffer: &mut [u8], ts: Nanos) {
        let mut offset = 0;
        while offset < buffer.len() {
            let header = &mut buffer[offset..offset + FRAME_HEADER_SIZE];
            write_frame_ts(header, ts);
            offset += FRAME_HEADER_SIZE + frame_payload_len(header);
        }
    }

    /// Appends the staged frames to a disconnected outbound endpoint's
    /// backlog under the group's limits. Returns whether they were kept.
    fn queue_offline(config: &TcpGroupConfig, connection: &mut Connection, staged: &[u8]) -> bool {
        let backlog = &mut connection.backlog;
        if config.max_backlog_frames.is_some_and(|(max, _)| {
            backlog.frame_count().saturating_add(count_frames(staged)) > max
        }) || config.max_backlog_bytes.is_some_and(|max| backlog.would_exceed(staged.len(), max))
        {
            return false;
        }
        backlog.append_raw_remainder(staged, 0);
        backlog.maybe_warn(config, connection.token, connection.peer_addr);
        true
    }

    /// Writes the staged frames to the connection at `index` in one socket
    /// write, disconnecting it on failure. Returns whether the write was
    /// accepted or queued.
    fn write_staged(&mut self, registry: &Registry, index: usize, tokens: &mut Tokens) -> bool {
        let Self { config, connections, send_buffer, tls_buffer, .. } = self;
        let connection = &mut connections[index];
        let ConnectionState::Connected(stream) = &mut connection.state else {
            return Self::queue_offline(config, connection, send_buffer);
        };
        // TLS carries the staged frames as records, so the whole buffer
        // is encrypted and written unframed.
        let payload = if let Some(session) = &mut connection.tls {
            tls_buffer.clear();
            if !session.encrypt(send_buffer, tls_buffer) {
                self.disconnect_index(registry, index, true, tokens);
                return false;
            }
            &tls_buffer[..]
        } else {
            &send_buffer[..]
        };
        let state = stream.write_frame(registry, None, payload, config, &mut connection.timers);
        if state == StreamState::Disconnected {
            // A failed write never queues its frames, so they are not in the
            // queue the disconnect retains; under replay they belong behind
            // it, like a send made a moment later would. TLS never replays.
            let retain = connection.kind == ConnectionKind::Outbound &&
                connection.tls.is_none() &&
                config.replay == ReplayPolicy::Replay;
            self.disconnect_index(registry, index, true, tokens);
            if retain {
                let Self { config, connections, send_buffer, .. } = self;
                return Self::queue_offline(config, &mut connections[index], send_buffer);
            }
            return false;
        }
        true
    }

    pub(crate) fn send_with<F>(
        &mut self,
        registry: &Registry,
        token: Token,
        tokens: &mut Tokens,
        serialise: F,
    ) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let Some(index) = self.sendable_index(token) else {
            return false;
        };
        let config = &self.config;
        self.send_buffer.clear();
        if !Self::append_frame(config, &mut self.send_buffer, serialise) {
            return false;
        }
        if config.framing == Framing::LengthPrefixed {
            write_frame_ts(&mut self.send_buffer[..FRAME_HEADER_SIZE], Nanos::now());
        }
        self.write_staged(registry, index, tokens)
    }

    /// Connected non-TLS streams borrow the slices through the socket write.
    /// TLS, offline replay and large slice lists use the staging buffer.
    pub(crate) fn send_vectored(
        &mut self,
        registry: &Registry,
        token: Token,
        tokens: &mut Tokens,
        parts: &[IoSlice<'_>],
    ) -> bool {
        let Some(index) = self.sendable_index(token) else {
            return false;
        };
        let Some(total) = parts.iter().try_fold(0usize, |len, part| len.checked_add(part.len()))
        else {
            return false;
        };
        if total == 0 || total > self.config.max_frame_size {
            return false;
        }
        let config = &self.config;
        let connection = &mut self.connections[index];
        if connection.tls.is_none() &&
            (config.framing == Framing::Raw || parts.len() <= 16) &&
            let ConnectionState::Connected(stream) = &mut connection.state
        {
            let mut header = [0; FRAME_HEADER_SIZE];
            let mut framed = [IoSlice::new(&[]); 17];
            let slices = if config.framing == Framing::LengthPrefixed {
                write_frame_header(&mut header, total, Nanos::now());
                framed[0] = IoSlice::new(&header);
                framed[1..=parts.len()].copy_from_slice(parts);
                &framed[..=parts.len()]
            } else {
                parts
            };
            let wire_len = if config.framing == Framing::LengthPrefixed {
                total + FRAME_HEADER_SIZE
            } else {
                total
            };
            let state =
                stream.write_vectored(registry, slices, wire_len, config, &mut connection.timers);
            if state != StreamState::Disconnected {
                return true;
            }
            let retain = connection.kind == ConnectionKind::Outbound &&
                config.replay == ReplayPolicy::Replay;
            self.disconnect_index(registry, index, true, tokens);
            if !retain {
                return false;
            }
        }
        self.send_with(registry, token, tokens, |out| {
            out.reserve(total);
            for part in parts {
                out.extend_from_slice(part);
            }
        })
    }

    pub(crate) fn send_many_with<I, F>(
        &mut self,
        registry: &Registry,
        token: Token,
        tokens: &mut Tokens,
        items: I,
        mut serialise: F,
    ) -> bool
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        let Some(index) = self.sendable_index(token) else {
            return false;
        };
        let config = &self.config;
        self.send_buffer.clear();
        for item in items {
            Self::append_frame(config, &mut self.send_buffer, |buf| serialise(buf, item));
        }
        if self.send_buffer.is_empty() {
            return false;
        }
        if config.framing == Framing::LengthPrefixed {
            Self::stamp_frames(&mut self.send_buffer, Nanos::now());
        }
        self.write_staged(registry, index, tokens)
    }

    /// Whether a member can receive a broadcast right now.
    fn has_broadcast_recipient(&self) -> bool {
        self.connections.iter().any(|connection| {
            !connection.broadcast_paused &&
                !connection.close_when_drained &&
                !connection.is_handshaking() &&
                (matches!(connection.state, ConnectionState::Connected(_)) ||
                    self.config.replay == ReplayPolicy::Replay)
        })
    }

    /// Writes the staged frames to every connected member,
    /// disconnecting members whose write fails. Returns the number of
    /// recipients attempted.
    fn broadcast_staged(&mut self, registry: &Registry, tokens: &mut Tokens) -> usize {
        let mut attempted = 0;
        let mut index = self.connections.len();
        while index != 0 {
            index -= 1;
            if self.connections[index].broadcast_paused ||
                self.connections[index].close_when_drained ||
                self.connections[index].is_handshaking() ||
                (!matches!(self.connections[index].state, ConnectionState::Connected(_)) &&
                    self.config.replay == ReplayPolicy::Drop)
            {
                continue;
            }
            attempted += 1;
            self.write_staged(registry, index, tokens);
        }
        attempted
    }

    pub(crate) fn broadcast_with<F>(
        &mut self,
        registry: &Registry,
        tokens: &mut Tokens,
        serialise: F,
    ) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        if !self.has_broadcast_recipient() {
            return 0;
        }
        let config = &self.config;
        self.send_buffer.clear();
        if !Self::append_frame(config, &mut self.send_buffer, serialise) {
            return 0;
        }
        if config.framing == Framing::LengthPrefixed {
            write_frame_ts(&mut self.send_buffer[..FRAME_HEADER_SIZE], Nanos::now());
        }
        self.broadcast_staged(registry, tokens)
    }

    pub(crate) fn broadcast_many_with<I, F>(
        &mut self,
        registry: &Registry,
        tokens: &mut Tokens,
        items: I,
        mut serialise: F,
    ) -> usize
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        if !self.has_broadcast_recipient() {
            return 0;
        }
        let config = &self.config;
        self.send_buffer.clear();
        for item in items {
            Self::append_frame(config, &mut self.send_buffer, |buf| serialise(buf, item));
        }
        if self.send_buffer.is_empty() {
            return 0;
        }
        if config.framing == Framing::LengthPrefixed {
            Self::stamp_frames(&mut self.send_buffer, Nanos::now());
        }
        self.broadcast_staged(registry, tokens)
    }

    pub(crate) fn disconnect(
        &mut self,
        registry: &Registry,
        token: Token,
        tokens: &mut Tokens,
    ) -> bool {
        let Some(index) = self.index_of(token) else {
            return false;
        };
        if matches!(self.connections[index].state, ConnectionState::Disconnected) {
            return false;
        }
        self.disconnect_index(registry, index, true, tokens);
        true
    }

    pub(crate) fn disconnect_when_drained(
        &mut self,
        registry: &Registry,
        token: Token,
        tokens: &mut Tokens,
    ) -> bool {
        let Some(index) = self.index_of(token) else {
            return false;
        };
        let ConnectionState::Connected(stream) = &self.connections[index].state else {
            return false;
        };
        if stream.send_queue.is_empty() {
            return self.disconnect(registry, token, tokens);
        }
        self.connections[index].close_when_drained = true;
        true
    }

    pub(crate) fn remove(&mut self, registry: &Registry, token: Token) -> bool {
        if let Some(index) = self.listeners.iter().position(|l| l.token == token) {
            let mut listener = self.listeners.swap_remove(index);
            let _ = registry.deregister(&mut listener.socket);
            return true;
        }
        let Some(index) = self.index_of(token) else {
            return false;
        };
        self.close_connection_socket(registry, index);
        self.swap_remove_connection(index);
        self.pending_disconnects.retain(|event| event.token != token);
        true
    }
    pub(crate) fn queued_bytes(&self, token: Token) -> usize {
        let Some(index) = self.index_of(token) else {
            return 0;
        };
        let connection = &self.connections[index];
        let sending = match &connection.state {
            ConnectionState::Connected(stream) => stream.send_queue.len(),
            _ => 0,
        };
        sending + connection.backlog.len()
    }
    /// Raw streams have no frame boundaries and a TLS queue holds records
    /// that cannot be cut, so both keep everything and report 0.
    pub(crate) fn clear_backlog(&mut self, token: Token) -> usize {
        if self.config.framing != Framing::LengthPrefixed {
            return 0;
        }
        let Some(index) = self.index_of(token) else {
            return 0;
        };
        let connection = &mut self.connections[index];
        if connection.tls.is_some() {
            return 0;
        }
        let queue = match &mut connection.state {
            ConnectionState::Connected(stream) => &mut stream.send_queue,
            _ => &mut connection.backlog,
        };
        queue.clear_frames()
    }
    fn check_backlogs(
        &mut self,
        registry: &Registry,
        max: usize,
        timeout: Duration,
        tokens: &mut Tokens,
    ) {
        for index in (0..self.connections.len()).rev() {
            let ConnectionState::Connected(stream) = &mut self.connections[index].state else {
                continue;
            };
            if stream.send_queue.frame_count() > max {
                if stream.send_queue.exceeded_since.get_or_insert_with(Instant::now).elapsed() >=
                    timeout
                {
                    self.disconnect_index(registry, index, true, tokens);
                }
            } else {
                stream.send_queue.exceeded_since = None;
            }
        }
    }
}

fn bind_listener(addr: SocketAddr, config: &TcpGroupConfig) -> io::Result<TcpListener> {
    let domain = if addr.is_ipv4() { libc::AF_INET } else { libc::AF_INET6 };
    let fd = unsafe {
        libc::socket(domain, libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0)
    };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
    let enable: libc::c_int = 1;
    if unsafe {
        libc::setsockopt(
            listener.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            ptr::from_ref(&enable).cast(),
            size_of::<libc::c_int>() as libc::socklen_t,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if config.reuse_port {
        if unsafe {
            libc::setsockopt(
                listener.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                ptr::from_ref(&enable).cast(),
                size_of::<libc::c_int>() as libc::socklen_t,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    // Accepted sockets inherit these options, and the receive window
    // negotiated before accept(), so they are set once here, not per accept.
    if let Some(size) = config.socket_buf_size {
        set_socket_buf_size(&listener, size);
    }
    if config.nodelay &&
        unsafe {
            libc::setsockopt(
                listener.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                ptr::from_ref(&enable).cast(),
                size_of::<libc::c_int>() as libc::socklen_t,
            )
        } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if config.keepalive {
        set_keepalive(&listener)?;
    }
    set_user_timeout(&listener, config.user_timeout_ms);
    let (storage, len) = sockaddr(addr);
    if unsafe { libc::bind(fd, ptr::from_ref(&storage).cast(), len) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // Match mio's kernel-capped listener backlog on Linux.
    if unsafe { libc::listen(fd, -1) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(TcpListener::from_std(listener))
}

/// Starts a nonblocking connect to `peer_addr` from `local_ip` and an
/// ephemeral port.
fn connect_from(peer_addr: SocketAddr, local_ip: IpAddr) -> io::Result<mio::net::TcpStream> {
    let domain = if peer_addr.is_ipv4() { libc::AF_INET } else { libc::AF_INET6 };
    let fd = unsafe {
        libc::socket(domain, libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0)
    };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    let stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    let (local, local_len) = sockaddr(SocketAddr::new(local_ip, 0));
    if unsafe { libc::bind(fd, ptr::from_ref(&local).cast(), local_len) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let (peer, peer_len) = sockaddr(peer_addr);
    if unsafe { libc::connect(fd, ptr::from_ref(&peer).cast(), peer_len) } != 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINPROGRESS) {
            return Err(err);
        }
    }
    Ok(mio::net::TcpStream::from_std(stream))
}

fn sockaddr(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(addr) => {
            let address = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: addr.port().to_be(),
                sin_addr: libc::in_addr { s_addr: u32::from_ne_bytes(addr.ip().octets()) },
                sin_zero: [0; 8],
            };
            unsafe { ptr::write(ptr::from_mut(&mut storage).cast(), address) };
            size_of::<libc::sockaddr_in>()
        }
        SocketAddr::V6(addr) => {
            let address = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: addr.port().to_be(),
                sin6_flowinfo: addr.flowinfo(),
                sin6_addr: libc::in6_addr { s6_addr: addr.ip().octets() },
                sin6_scope_id: addr.scope_id(),
            };
            unsafe { ptr::write(ptr::from_mut(&mut storage).cast(), address) };
            size_of::<libc::sockaddr_in6>()
        }
    };
    (storage, len as libc::socklen_t)
}

/// Reads plaintext from `socket`, decrypting through `tls` when present.
#[inline]
fn read_plaintext(
    socket: &mut mio::net::TcpStream,
    tls: &mut Option<Box<Session>>,
    buf: &mut [u8],
) -> io::Result<usize> {
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
    #[inline]
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
    #[inline]
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

/// Bytes queued for one socket, written from `head`. A framed queue holds
/// whole `[header][payload]` frames and tracks `frame_start`, the header of
/// the frame `head` is in, so a lost connection can replay from it.
#[derive(Default)]
struct ByteQueue {
    bytes: Vec<u8>,
    head: usize,
    frame_start: usize,
    framed: bool,
    /// Frames from `frame_start` to the end, kept current so limits never
    /// walk the headers.
    frames: usize,
    exceeded_since: Option<Instant>,
    queued_since: Option<Instant>,
    last_warning: Option<Instant>,
}

impl ByteQueue {
    fn framed() -> Self {
        Self { framed: true, ..Self::default() }
    }
    /// Queued frames, counting a partially written one.
    fn frame_count(&self) -> usize {
        if self.framed { self.frames } else { usize::from(!self.is_empty()) }
    }
    /// Restarts the partially written frame from its header, for replay on
    /// a fresh connection.
    fn rewind_to_frame(&mut self) {
        self.head = self.frame_start;
        self.exceeded_since = None;
    }
    fn clear_frames(&mut self) -> usize {
        let count = self.frame_count();
        if self.framed && self.head > self.frame_start {
            self.bytes.truncate(
                self.frame_start +
                    FRAME_HEADER_SIZE +
                    frame_payload_len(&self.bytes[self.frame_start..]),
            );
            self.frames = 1;
            count.saturating_sub(1)
        } else {
            self.bytes.clear();
            self.head = 0;
            self.frame_start = 0;
            self.frames = 0;
            count
        }
    }

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
        if self.framed {
            while self.frame_start < self.head {
                let next = self.frame_start +
                    FRAME_HEADER_SIZE +
                    frame_payload_len(&self.bytes[self.frame_start..]);
                if next > self.head {
                    break;
                }
                self.frame_start = next;
                self.frames -= 1;
            }
        }
        if self.is_empty() {
            self.bytes.clear();
            self.head = 0;
            self.frame_start = 0;
            self.frames = 0;
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
        if self.framed {
            let allocated = self.append_remainder(header, payload);
            self.frames += 1;
            self.consume(written);
            return allocated;
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
        if self.framed {
            let allocated = self.append_remainder(&[], payload);
            self.frames += count_frames(payload);
            self.consume(written);
            allocated
        } else {
            self.append_remainder(&[], &payload[written..])
        }
    }

    fn append_remainder(&mut self, prefix: &[u8], payload: &[u8]) -> bool {
        self.append_vectored(&[IoSlice::new(prefix), IoSlice::new(payload)], 0)
    }

    fn append_vectored(&mut self, parts: &[IoSlice<'_>], mut written: usize) -> bool {
        let additional = parts.iter().map(|part| part.len()).sum::<usize>() - written;
        let old_capacity = self.bytes.capacity();

        if self.head != 0 && self.bytes.capacity() - self.bytes.len() < additional {
            let start = if self.framed { self.frame_start } else { self.head };
            self.bytes.copy_within(start.., 0);
            self.bytes.truncate(self.bytes.len() - start);
            self.head -= start;
            self.frame_start = 0;
        }
        self.bytes.reserve(additional);
        for part in parts {
            if written >= part.len() {
                written -= part.len();
                continue;
            }
            self.bytes.extend_from_slice(&part[written..]);
            written = 0;
        }
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
    direct_rx: Option<DirectRx>,
}

impl FramedStream {
    fn new(
        socket: mio::net::TcpStream,
        token: Token,
        peer_addr: SocketAddr,
        config: &TcpGroupConfig,
        dcache: bool,
    ) -> Self {
        // Receive buffers are allocated by the first read, so connections that
        // never send cost no buffer.
        let direct = dcache || config.aligned_payloads;
        Self {
            socket,
            token,
            peer_addr,
            rx_buffer: RxBuffer { bytes: Vec::new(), head: 0, tail: 0 },
            send_queue: ByteQueue {
                framed: config.framing == Framing::LengthPrefixed,
                ..ByteQueue::default()
            },
            writable_armed: false,
            direct_rx: direct.then(DirectRx::default),
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn poll_with<F>(
        &mut self,
        registry: &Registry,
        event: &MioEvent,
        config: &TcpGroupConfig,
        timers: &mut Option<NetworkTimers>,
        tls: &mut Option<Box<Session>>,
        dcache: Option<&DCache>,
        on_message: &mut F,
    ) -> StreamState
    where
        F: for<'a> FnMut(Option<RxPayload<'a>>, Nanos),
    {
        if event.is_readable() {
            if config.framing == Framing::Raw {
                // An empty buffer reads as a closed peer. Raw reads at most
                // `max_frame_size` at a time.
                if self.rx_buffer.bytes.is_empty() {
                    self.rx_buffer
                        .bytes
                        .resize(INITIAL_RX_BUFFER_SIZE.min(config.max_frame_size), 0);
                }
                loop {
                    match read_plaintext(&mut self.socket, tls, &mut self.rx_buffer.bytes) {
                        Ok(0) => return StreamState::Disconnected,
                        Ok(read) => {
                            if tls.is_some() &&
                                self.announce_tls(registry, config, timers, tls, on_message) ==
                                    StreamState::Disconnected
                            {
                                return StreamState::Disconnected;
                            }
                            on_message(
                                Some(RxPayload::Raw(&self.rx_buffer.bytes[..read])),
                                Nanos::now(),
                            );
                        }
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                        Err(err) => {
                            debug!(?err, %self.peer_addr, "tcp raw read failed");
                            return StreamState::Disconnected;
                        }
                    }
                }
            } else if self.direct_rx.is_some() {
                loop {
                    match self.direct_rx.as_mut().unwrap().read(
                        &mut self.socket,
                        tls,
                        dcache,
                        config.max_frame_size,
                    ) {
                        Ok(Some((dref, ts))) => {
                            if tls.is_some() &&
                                self.announce_tls(registry, config, timers, tls, on_message) ==
                                    StreamState::Disconnected
                            {
                                return StreamState::Disconnected;
                            }
                            if let Some(timers) = timers {
                                if let Some(latency) = &mut timers.latency {
                                    latency.emit_latency_from_nanos(ts, Nanos::now());
                                }
                            }
                            let payload = if dcache.is_some() {
                                RxPayload::DCache(dref)
                            } else {
                                RxPayload::Raw(
                                    &byte_stable::slice_as_bytes(
                                        &self.direct_rx.as_ref().unwrap().words,
                                    )[..dref.len],
                                )
                            };
                            on_message(Some(payload), ts);
                        }
                        Ok(None) => break,
                        Err(err) => {
                            match err.kind() {
                                // A closed peer, like `Ok(0)` on the buffered path.
                                io::ErrorKind::UnexpectedEof => {}
                                io::ErrorKind::InvalidData | io::ErrorKind::Other => {
                                    warn!(%err, %self.peer_addr, "tcp direct receive failed");
                                }
                                _ => debug!(?err, %self.peer_addr, "tcp frame read failed"),
                            }
                            return StreamState::Disconnected;
                        }
                    }
                }
            } else {
                loop {
                    // Drain buffered frames before reading again. A read can
                    // finish TLS, which must be announced before its plaintext.
                    let frame_len = match self.rx_buffer.next_frame(config.max_frame_size) {
                        NextFrame::Ready { start, length, send_ts } => {
                            if let Some(timers) = timers {
                                if let Some(latency) = &mut timers.latency {
                                    latency.emit_latency_from_nanos(send_ts, Nanos::now());
                                }
                            }
                            on_message(
                                Some(RxPayload::Raw(&self.rx_buffer.bytes[start..start + length])),
                                send_ts,
                            );
                            continue;
                        }
                        NextFrame::PayloadTooLarge(length) => {
                            warn!(
                                %self.peer_addr,
                                payload_len = length,
                                max_frame_size = config.max_frame_size,
                                "tcp frame exceeds configured maximum"
                            );
                            return StreamState::Disconnected;
                        }
                        NextFrame::Incomplete { frame_len } => frame_len,
                    };
                    self.rx_buffer.make_room(frame_len);
                    let tail = self.rx_buffer.tail;
                    match read_plaintext(&mut self.socket, tls, &mut self.rx_buffer.bytes[tail..]) {
                        Ok(0) => return StreamState::Disconnected,
                        Ok(read) => self.rx_buffer.tail += read,
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                        Err(err) => {
                            debug!(?err, %self.peer_addr, "tcp frame read failed");
                            return StreamState::Disconnected;
                        }
                    }
                    if tls.is_some() &&
                        self.announce_tls(registry, config, timers, tls, on_message) ==
                            StreamState::Disconnected
                    {
                        return StreamState::Disconnected;
                    }
                }
            }
            if tls.is_some() &&
                self.announce_tls(registry, config, timers, tls, on_message) ==
                    StreamState::Disconnected
            {
                return StreamState::Disconnected;
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

    fn announce_tls<F>(
        &mut self,
        registry: &Registry,
        config: &TcpGroupConfig,
        timers: &mut Option<NetworkTimers>,
        tls: &mut Option<Box<Session>>,
        on_message: &mut F,
    ) -> StreamState
    where
        F: for<'a> FnMut(Option<RxPayload<'a>>, Nanos),
    {
        let Some(session) = tls.as_mut().filter(|session| session.handshake_completed()) else {
            return StreamState::Alive;
        };
        if let Some(message) = &config.on_connect_msg {
            let mut plain = Vec::new();
            if config.framing == Framing::LengthPrefixed {
                let mut header = [0; FRAME_HEADER_SIZE];
                write_frame_header(&mut header, message.len(), Nanos::now());
                plain.extend_from_slice(&header);
            }
            plain.extend_from_slice(message);
            let mut encrypted = Vec::new();
            if !session.encrypt(&plain, &mut encrypted) ||
                self.write_frame(registry, None, &encrypted, config, timers) ==
                    StreamState::Disconnected
            {
                return StreamState::Disconnected;
            }
        }
        session.mark_announced();
        on_message(None, Nanos::ZERO);
        StreamState::Alive
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
            return self.enqueue_remainder(registry, header, payload, 0, config, timers);
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

    /// Framed callers include the header as the first slice.
    fn write_vectored(
        &mut self,
        registry: &Registry,
        parts: &[IoSlice<'_>],
        total: usize,
        config: &TcpGroupConfig,
        timers: &mut Option<NetworkTimers>,
    ) -> StreamState {
        let written = if self.send_queue.is_empty() {
            match self.socket.write_vectored(parts) {
                Ok(0) => return StreamState::Disconnected,
                Ok(written) if written == total => return StreamState::Alive,
                Ok(written) => written,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => 0,
                Err(err) => {
                    debug!(?err, %self.peer_addr, "tcp write failed");
                    return StreamState::Disconnected;
                }
            }
        } else {
            0
        };
        self.enqueue(registry, total - written, config, timers, |queue| {
            if queue.framed {
                // Keep the complete partially written frame for reconnect replay.
                let allocated = queue.append_vectored(parts, 0);
                queue.frames += 1;
                queue.consume(written);
                allocated
            } else {
                queue.append_vectored(parts, written)
            }
        })
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
        self.enqueue(registry, total - written, config, timers, |queue| {
            if let Some(header) = header {
                queue.append_frame_remainder(header, payload, written)
            } else {
                queue.append_raw_remainder(payload, written)
            }
        })
    }

    /// Queues `additional` unwritten bytes through `append` under the
    /// backlog limit, arming writable interest first.
    fn enqueue(
        &mut self,
        registry: &Registry,
        additional: usize,
        config: &TcpGroupConfig,
        timers: &mut Option<NetworkTimers>,
        append: impl FnOnce(&mut ByteQueue) -> bool,
    ) -> StreamState {
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

        // Armed first: a `Disconnected` from this function then means the
        // bytes were not queued, which the replay path relies on.
        if self.arm_writable(registry) == StreamState::Disconnected {
            return StreamState::Disconnected;
        }
        let started = Nanos::now();
        let allocated = append(&mut self.send_queue);
        if allocated && let Some(timers) = timers {
            timers.alloc.emit_latency_from_nanos(started, Nanos::now());
        }
        self.send_queue.maybe_warn(config, self.token, self.peer_addr);
        StreamState::Alive
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
        ByteQueue, FRAME_HEADER_SIZE, FramedStream, StreamState, TcpGroupConfig,
        set_socket_buf_size, write_frame_header,
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
    fn framed_queue_counts_frames_without_walking() {
        fn frame(fill: u8, len: usize) -> ([u8; FRAME_HEADER_SIZE], Vec<u8>) {
            let mut header = [0; FRAME_HEADER_SIZE];
            write_frame_header(&mut header, len, Nanos::ZERO);
            (header, vec![fill; len])
        }
        let mut queue = ByteQueue::framed();
        assert_eq!(queue.frame_count(), 0);
        let (h1, p1) = frame(1, 16);
        queue.append_frame_remainder(&h1, &p1, 5);
        assert_eq!(queue.frame_count(), 1, "a partially written frame counts");

        let (h2, p2) = frame(2, 8);
        let (h3, p3) = frame(3, 4);
        let mut staged = h2.to_vec();
        staged.extend_from_slice(&p2);
        staged.extend_from_slice(&h3);
        staged.extend_from_slice(&p3);
        queue.append_raw_remainder(&staged, 0);
        assert_eq!(queue.frame_count(), 3, "raw appends count every staged frame");

        // Finish the first frame and half of the second.
        queue.consume(FRAME_HEADER_SIZE + 16 - 5 + 3);
        assert_eq!(queue.frame_count(), 2);
        assert_eq!(queue.frame_start, FRAME_HEADER_SIZE + 16);

        queue.rewind_to_frame();
        assert_eq!(queue.frame_count(), 2, "rewinding to the frame header keeps the count");
        assert_eq!(&queue.remaining()[..FRAME_HEADER_SIZE], &h2);

        queue.consume(3);
        assert_eq!(queue.clear_frames(), 1, "clearing keeps the partially written frame");
        assert_eq!(queue.frame_count(), 1);
        queue.consume(queue.len());
        assert_eq!(queue.frame_count(), 0);
        assert!(queue.is_empty());
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
        let config = TcpGroupConfig {
            backlog_warn_bytes: None,
            max_backlog_bytes: Some(16),
            max_frame_size: 1024,
            ..Default::default()
        };
        let mut stream = FramedStream::new(socket, Token(0), peer_addr, &config, false);
        let fill = [0; 4096];
        loop {
            match stream.socket.write(&fill) {
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => panic!("failed to fill socket send buffer: {err}"),
            }
        }
        stream.send_queue.bytes.extend_from_slice(&[1; 8]);

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

#[derive(Default)]
struct DirectRx {
    header: [u8; FRAME_HEADER_SIZE],
    have: usize,
    payload: Option<(DCacheRef, usize, Nanos)>,
    words: Vec<u64>,
}
impl DirectRx {
    fn read(
        &mut self,
        socket: &mut mio::net::TcpStream,
        tls: &mut Option<Box<Session>>,
        dcache: Option<&DCache>,
        max: usize,
    ) -> io::Result<Option<(DCacheRef, Nanos)>> {
        loop {
            if let Some((dref, offset, ts)) = &mut self.payload {
                let result = if let Some(dcache) = dcache {
                    dcache
                        .write_into(*dref, *offset, |bytes| read_plaintext(socket, tls, bytes))
                        .map_err(|err| io::Error::other(format!("dcache write: {err}")))?
                } else {
                    read_plaintext(
                        socket,
                        tls,
                        &mut byte_stable::words_as_bytes_mut(&mut self.words)[*offset..dref.len],
                    )
                };
                match result {
                    Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                    Ok(n) => {
                        *offset += n;
                        if *offset == dref.len {
                            let result = (*dref, *ts);
                            self.payload = None;
                            self.have = 0;
                            return Ok(Some(result));
                        }
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                    Err(err) => return Err(err),
                }
            } else {
                match read_plaintext(socket, tls, &mut self.header[self.have..]) {
                    Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                    Ok(n) => self.have += n,
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                    Err(err) => return Err(err),
                }
                if self.have == FRAME_HEADER_SIZE {
                    let len = frame_payload_len(&self.header);
                    if len > max {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("frame of {len} bytes exceeds the {max}-byte maximum"),
                        ));
                    }
                    if len == 0 {
                        self.have = 0;
                        continue;
                    }
                    let dref = if let Some(dcache) = dcache {
                        dcache
                            .reserve(len)
                            .map_err(|err| io::Error::other(format!("dcache reserve: {err}")))?
                    } else {
                        if self.words.len() < len.div_ceil(8) {
                            self.words
                                .resize(len.max(INITIAL_RX_BUFFER_SIZE.min(max)).div_ceil(8), 0);
                        }
                        DCacheRef { offset: 0, len }
                    };
                    self.payload = Some((dref, 0, frame_send_ts(&self.header)));
                }
            }
        }
    }
}

fn count_frames(bytes: &[u8]) -> usize {
    let mut at = 0;
    let mut count = 0;
    while at < bytes.len() {
        at += FRAME_HEADER_SIZE + frame_payload_len(&bytes[at..]);
        count += 1;
    }
    count
}
