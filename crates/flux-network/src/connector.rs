use std::net::SocketAddr;

use flux::spine::{SpineProducerWithDCache, SpineProducers};
use flux_timing::{Duration, Nanos};
use flux_utils::DCachePtr;
use mio::{Events, Poll, Token};

use crate::{
    tcp::{DEFAULT_TCP_USER_TIMEOUT_MS, TcpConfig, TcpManager, TcpStream, TcpTelemetry},
    udp::{UdpConfig, UdpManager},
};

const EVENTS_CAPACITY: usize = 128;

/// Wire transport used by a [`Connector`], with its transport-specific
/// settings. Settings both share are the `with_*` builders.
#[derive(Clone, Copy, Debug)]
pub enum Transport {
    Tcp(TcpConfig),
    Udp(UdpConfig),
}

impl Default for Transport {
    fn default() -> Self {
        Self::Tcp(TcpConfig::default())
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum SendBehavior {
    Broadcast,
    Single(Token),
}

/// Event emitted by [`Connector::poll_with`] and
/// [`Connector::poll_with_produce`] for each notable IO occurrence.
///
/// `Payload = &'a [u8]` for both variants.
pub enum PollEvent<Payload> {
    /// A new connection was accepted from a listener.
    ///
    /// - `listener`: token of the listening socket that accepted
    /// - `stream`: token assigned to the new inbound stream
    /// - `peer_addr`: remote address
    ///
    /// Use the `stream` token with [`SendBehavior::Single`] to write back.
    Accept { listener: Token, stream: Token, peer_addr: SocketAddr },
    /// Successfully reconnected to an outbound stream.
    Reconnect { token: Token },
    /// A connection was closed (by the remote or due to an IO error).
    Disconnect { token: Token },
    /// A complete framed message was received.
    Message { token: Token, payload: Payload, send_ts: Nanos },
}

/// Settings shared by both transports, set through the `with_*` builders.
pub(crate) struct Config {
    pub(crate) on_connect_msg: Option<Vec<u8>>,
    pub(crate) telemetry: TcpTelemetry,
    pub(crate) socket_buf_size: Option<usize>,
    /// `TCP_USER_TIMEOUT`, or the UDP peer silence timeout.
    pub(crate) user_timeout_ms: u32,
    pub(crate) dcache: Option<DCachePtr>,
    /// Connections whose send backlog exceeds `max` for longer than
    /// `timeout` are disconnected (outbound scheduled for reconnection).
    /// Counted in framed messages for TCP, unacked datagrams for UDP.
    pub(crate) max_backlog: Option<(usize, Duration)>,
    pub(crate) drop_outbound_backlog_on_disconnect: bool,
}

enum Inner {
    Tcp(TcpManager),
    /// Boxed: carries the recv and send batch buffers.
    Udp(Box<UdpManager>),
}

/// Non-blocking connector/acceptor built on `mio`, over TCP or reliable UDP
/// (see [`Transport`]). The API and events are identical for both.
///
/// Manages:
/// - **Outbound (client) connections** created via [`connect`]. These are
///   **auto-retried** on failure/disconnect: TCP on its configured reconnect
///   interval, UDP on its adaptive RTO.
/// - **Listeners** created via [`listen_at`] and **inbound (server)
///   connections** accepted from them. Inbound connections are **not**
///   reconnected.
///
/// Drive all IO by calling [`poll_with`] regularly (typically in your event
/// loop). Use [`write_or_enqueue_with`] to send to one connection or broadcast
/// to all.
///
/// ## Tokens
/// Every listener and stream is identified by a `mio::Token`.
/// - [`listen_at`] returns the listener token.
/// - Each accepted inbound stream receives a new token (reported via
///   [`PollEvent::Accept`]).
/// - [`connect`] returns the token for the outbound stream if the connection is
///   established.
///
/// ## on-connect message
/// If configured via [`with_on_connect_msg`], the provided bytes are sent once
/// after a connection is established (both outbound and newly accepted
/// inbound).
///
/// ## `DCache`
/// If built via [`with_dcache`], each received message payload is written
/// into the dcache and must be drained with [`poll_with_produce`].
///
/// ## UDP
/// Messages are delivered as soon as they are complete, in any order. On
/// reconnect everything still queued is resent under the new session unless
/// [`with_drop_outbound_backlog_on_disconnect`] is set.
pub struct Connector {
    poll: Poll,
    events: Events,
    config: Config,
    /// Scratch the caller serialises into; managers get it as a slice.
    payload: Vec<u8>,
    inner: Inner,
}

impl Default for Connector {
    fn default() -> Self {
        let poll = Poll::new().expect("couldn't set up a poll for connector");
        let registry = poll.registry().try_clone().expect("couldn't clone poll registry");
        Self {
            poll,
            events: Events::with_capacity(EVENTS_CAPACITY),
            config: Config {
                on_connect_msg: None,
                telemetry: TcpTelemetry::Disabled,
                socket_buf_size: None,
                user_timeout_ms: DEFAULT_TCP_USER_TIMEOUT_MS,
                dcache: None,
                max_backlog: None,
                drop_outbound_backlog_on_disconnect: false,
            },
            payload: Vec::with_capacity(TcpStream::SEND_BUF_SIZE),
            inner: Inner::Tcp(TcpManager::new(registry, TcpConfig::default())),
        }
    }
}

impl Connector {
    /// Selects the wire transport. Must precede [`connect`] and [`listen_at`].
    ///
    /// # Panics
    /// Panics on an invalid [`UdpConfig`] or if sockets already exist.
    pub fn with_transport(mut self, transport: Transport) -> Self {
        let empty = match &self.inner {
            Inner::Tcp(m) => m.is_empty(),
            Inner::Udp(m) => m.is_empty(),
        };
        assert!(empty, "with_transport must precede connect/listen_at");
        let registry = self.poll.registry().try_clone().expect("couldn't clone poll registry");
        self.inner = match transport {
            Transport::Tcp(config) => Inner::Tcp(TcpManager::new(registry, config)),
            Transport::Udp(config) => Inner::Udp(Box::new(UdpManager::new(registry, config))),
        };
        self
    }

    /// Sends this message once immediately after a connection becomes usable.
    ///
    /// Applied to:
    /// - outbound connections after a successful (re)connect
    /// - inbound connections right after accept
    ///
    /// # Panics
    /// Panics if `msg.len() > TcpStream::SEND_BUF_SIZE`.
    pub fn with_on_connect_msg(mut self, msg: Vec<u8>) -> Self {
        assert!(msg.len() <= TcpStream::SEND_BUF_SIZE, "on_connect_msg exceeds send buffer size");
        self.config.on_connect_msg = Some(msg);
        self
    }

    /// Attaches a dcache as the shared receive buffer for all streams.
    pub fn with_dcache(mut self, dcache: DCachePtr) -> Self {
        self.config.dcache = Some(dcache);
        self
    }

    /// Sets telemetry config for all streams created by this connector.
    pub fn with_telemetry(mut self, telemetry: TcpTelemetry) -> Self {
        self.config.telemetry = telemetry;
        self
    }

    /// Sets kernel `SO_SNDBUF` and `SO_RCVBUF` on all sockets (outbound and
    /// accepted).
    pub fn with_socket_buf_size(mut self, size: usize) -> Self {
        self.config.socket_buf_size = Some(size);
        self
    }

    /// Overrides the `TCP_USER_TIMEOUT` socket option applied to
    /// outbound connections. For UDP this is the silence after which a peer
    /// is considered gone.
    pub fn with_user_timeout(mut self, timeout_ms: u32) -> Self {
        self.config.user_timeout_ms = timeout_ms;
        self
    }

    /// Sets the maximum send backlog and how long it must stay exceeded
    /// before a connection is automatically disconnected. The backlog is
    /// counted in framed messages for TCP and unacked datagrams for UDP.
    ///
    /// Active connections are closed once their backlog exceeds `max` for
    /// `timeout`. If a disconnected outbound connection's backlog would exceed
    /// that same limit, additional messages are dropped until reconnect
    /// succeeds. The exceeded-since timer resets on reconnect.
    pub fn with_max_backlog(mut self, max: usize, timeout: Duration) -> Self {
        self.config.max_backlog = Some((max, timeout));
        self
    }

    /// Drops queued outbound messages when a connection is moved to reconnect.
    ///
    /// While that outbound connection is disconnected, sends to it are also
    /// dropped instead of being queued for replay after reconnect.
    pub fn with_drop_outbound_backlog_on_disconnect(mut self, enabled: bool) -> Self {
        self.config.drop_outbound_backlog_on_disconnect = enabled;
        self
    }

    /// Polls sockets once (non-blocking) and dispatches events via
    /// [`PollEvent`].
    ///
    /// This call:
    /// 1) attempts outbound reconnects if due
    /// 2) polls `mio` with a zero timeout
    /// 3) for each event calls `handler` with the appropriate [`PollEvent`]
    /// 4) returns whether any IO events were processed
    ///
    /// Writable events trigger retries of queued writes. Because this method
    /// performs a single non-blocking poll, calling it infrequently can slow
    /// backlog draining under backpressure, particularly when using small
    /// socket buffers configured via [`Self::with_socket_buf_size`].
    #[inline]
    pub fn poll_with<F>(&mut self, handler: F) -> bool
    where
        F: for<'a> FnMut(PollEvent<&'a [u8]>),
    {
        let Self { poll, events, config, inner, .. } = self;
        match inner {
            Inner::Tcp(m) => m.poll_with(config, poll, events, handler),
            Inner::Udp(m) => m.poll_with(config, poll, events, handler),
        }
    }

    /// Like [`poll_with`] but for dcache-backed streams. The handler receives
    /// `PollEvent<&[u8]>` for all events; for `Message` events, returning
    /// `Some(T)` produces into the spine.
    ///
    /// # Panics
    /// Panics if no dcache was configured via [`with_dcache`].
    #[inline]
    pub fn poll_with_produce<T, P, F>(&mut self, produce: &mut P, on_msg: F) -> bool
    where
        T: 'static + Copy,
        P: SpineProducers + AsRef<SpineProducerWithDCache<T>>,
        F: for<'a> FnMut(PollEvent<&'a [u8]>) -> Option<T>,
    {
        let Self { poll, events, config, inner, .. } = self;
        match inner {
            Inner::Tcp(m) => m.poll_with_produce(config, poll, events, produce, on_msg),
            Inner::Udp(m) => m.poll_with_produce(config, poll, events, produce, on_msg),
        }
    }

    /// Writes immediately or enqueues bytes for later sending.
    ///
    /// `serialise` is called with a mutable send buffer. Use
    /// [`SendBehavior::Broadcast`] to send to all active connections or
    /// [`SendBehavior::Single`] to target one token. Empty payloads are not
    /// sent.
    #[inline]
    pub fn write_or_enqueue_with<F>(&mut self, where_to: SendBehavior, serialise: F)
    where
        F: Fn(&mut Vec<u8>),
    {
        self.payload.clear();
        serialise(&mut self.payload);
        if self.payload.is_empty() {
            return;
        }
        match &mut self.inner {
            Inner::Tcp(m) => m.write(&self.config, where_to, &self.payload),
            Inner::Udp(m) => m.write(&self.config, where_to, &mut self.payload),
        }
    }

    /// Disconnects all outbound connections and schedules them for
    /// reconnection.
    ///
    /// Inbound connections and listeners are left untouched.
    pub fn disconnect_outbound(&mut self) {
        match &mut self.inner {
            Inner::Tcp(m) => m.disconnect_outbound(&self.config),
            Inner::Udp(m) => m.disconnect_outbound(&self.config),
        }
    }

    /// Disconnects a specific connection by token.
    ///
    /// If the token is an outbound connection, it will be scheduled for
    /// reconnection. If inbound, it's simply closed. No-op if token not found.
    pub fn disconnect(&mut self, token: Token) {
        match &mut self.inner {
            Inner::Tcp(m) => m.disconnect(&self.config, token),
            Inner::Udp(m) => m.disconnect(&self.config, token),
        }
    }

    /// Initiates (or schedules) an outbound connection to `addr`.
    ///
    /// Returns the token for this connection if the connection becomes
    /// established; otherwise returns `None` (the connector may still retry
    /// later). For UDP the token is returned once the socket is bound; the
    /// handshake completes inside [`poll_with`] and queued sends go out then.
    ///
    /// Note: reconnect attempts are driven by [`poll_with`].
    #[inline]
    pub fn connect(&mut self, addr: SocketAddr) -> Option<Token> {
        match &mut self.inner {
            Inner::Tcp(m) => m.connect(&self.config, addr),
            Inner::Udp(m) => m.connect(&self.config, addr),
        }
    }

    /// Starts listening on `addr` and registers the listener for readable
    /// events.
    ///
    /// Returns the token associated with the listener socket. When a client
    /// connects, `poll_with` will accept it, allocate a new token for the
    /// inbound stream, and emit a [`PollEvent::Accept`].
    pub fn listen_at(&mut self, addr: SocketAddr) -> Option<Token> {
        match &mut self.inner {
            Inner::Tcp(m) => m.listen_at(addr),
            Inner::Udp(m) => m.listen_at(&self.config, addr),
        }
    }

    /// Returns an iterator over tokens that are currently pending reconnection
    /// (outbound only).
    #[inline]
    pub fn currently_disconnected(&self) -> impl Iterator<Item = Token> {
        let (tcp, udp) = match &self.inner {
            Inner::Tcp(m) => (Some(m.currently_disconnected()), None),
            Inner::Udp(m) => (None, Some(m.currently_disconnected())),
        };
        tcp.into_iter().flatten().chain(udp.into_iter().flatten())
    }

    /// Forces the reconnect timer to fire and immediately attempts
    /// reconnections.
    #[inline]
    pub fn force_reconnect(&mut self) {
        match &mut self.inner {
            Inner::Tcp(m) => m.force_reconnect(&self.config),
            Inner::Udp(m) => m.force_reconnect(),
        }
    }
}
