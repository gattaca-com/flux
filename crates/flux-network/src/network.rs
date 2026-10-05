use std::{
    io::{self, Write},
    net::{IpAddr, SocketAddr},
    ops::{Deref, DerefMut, Range},
    os::fd::AsRawFd,
    ptr,
};

use flux::spine::{SpineProducerWithDCache, SpineProducers};
use flux_timing::{Duration, Nanos};
use flux_utils::{DCachePtr, DCacheRef};
use mio::{Events, Poll, Registry, Token};
use rustc_hash::FxHashMap;
use tracing::warn;

use crate::{
    tcp::{DEFAULT_TCP_USER_TIMEOUT_MS, TcpManager},
    tls::Session,
    udp::{UdpConfig, UdpManager},
};

/// Controls emission of network latency and alloc telemetry.
///
/// Has no effect on framing or message delivery.
/// `send_ts` is always surfaced via `poll_with`.
#[derive(Clone, Copy)]
pub enum NetworkTelemetry {
    Disabled,
    Enabled { app_name: &'static str },
}

const EVENTS_CAPACITY: usize = 128;
const INITIAL_GROUP_CAPACITY: usize = 4;
const DEFAULT_MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_BACKLOG_WARN_BYTES: usize = 64 * 1024 * 1024;

/// Identifies a set of connections using the same application protocol and
/// socket configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Group(usize);

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
    pub(crate) fn new(bytes: &'a mut Vec<u8>) -> Self {
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

/// Configuration shared by every listener and connection in a [`Group`].
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
    /// Read each framed payload directly into an 8-byte-aligned buffer. This
    /// uses separate header/payload reads instead of buffered read-ahead.
    pub aligned_payloads: bool,
    /// Per-connection latency and allocation telemetry.
    pub telemetry: NetworkTelemetry,
    /// Whether an outbound endpoint keeps queued frames across a lost
    /// connection. Defaults to [`ReplayPolicy::Replay`]; set
    /// [`ReplayPolicy::Drop`] explicitly for endpoints where replay makes no
    /// sense. Raw framing and TLS require `Drop`.
    pub replay: ReplayPolicy,
    /// Queued frames and how long a connected stream may stay above the
    /// threshold before it is disconnected. While an outbound endpoint is
    /// disconnected the limit is hard: sends that would exceed it are
    /// rejected at once.
    pub max_backlog_frames: Option<(usize, Duration)>,
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
            aligned_payloads: false,
            telemetry: NetworkTelemetry::Disabled,
            replay: ReplayPolicy::Replay,
            max_backlog_frames: None,
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

#[derive(Clone, Copy)]
pub(crate) enum RxPayload<'a> {
    Raw(&'a [u8]),
    DCache(DCacheRef),
}

/// Event emitted by [`Network::poll_with`].
pub type NetworkEvent<'a> = Event<&'a [u8]>;

/// A network notification with a borrowed message payload.
pub enum Event<Payload> {
    /// A listener established an inbound TCP connection or UDP session.
    Accepted { group: Group, token: Token, peer_addr: SocketAddr },
    /// An outbound TCP connection or UDP session became usable. Emitted once
    /// on initial establishment and each reconnect, before its messages.
    Connected { group: Group, token: Token, peer_addr: SocketAddr },
    /// A complete TCP frame, UDP message, or raw TCP chunk was received.
    /// UDP messages are reliable but unordered when reliability is enabled.
    /// For raw groups, chunks do not preserve message boundaries and `send_ts`
    /// is the local receive time.
    Message { group: Group, token: Token, payload: Payload, send_ts: Nanos },
    /// A TCP connection closed or a UDP session timed out/reset locally.
    /// A timeout is not proof that the remote process has stopped.
    Disconnected { group: Group, token: Token, peer_addr: SocketAddr },
}

impl<P> Event<P> {
    pub fn group(&self) -> Group {
        match self {
            Self::Accepted { group, .. } |
            Self::Connected { group, .. } |
            Self::Message { group, .. } |
            Self::Disconnected { group, .. } => *group,
        }
    }

    fn map_payload<Q>(self, map: impl FnOnce(P) -> Q) -> Event<Q> {
        match self {
            Self::Accepted { group, token, peer_addr } => {
                Event::Accepted { group, token, peer_addr }
            }
            Self::Connected { group, token, peer_addr } => {
                Event::Connected { group, token, peer_addr }
            }
            Self::Disconnected { group, token, peer_addr } => {
                Event::Disconnected { group, token, peer_addr }
            }
            Self::Message { group, token, payload, send_ts } => {
                Event::Message { group, token, payload: map(payload), send_ts }
            }
        }
    }
}

/// Whether queued outbound messages survive a lost session.
///
/// Both group configs default to `Replay`: an outbound endpoint is a durable
/// destination that is only temporarily unreachable. Choose `Drop` explicitly
/// for request/response protocols and anything else where a message is
/// worthless once its connection is gone.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReplayPolicy {
    /// Drop pending messages and reject sends while disconnected.
    Drop,
    /// Retain whole pending frames and accept offline sends for replay after
    /// reconnect. A frame that was partially written when the connection
    /// died is resent whole, so delivery is at least once, not exactly once.
    #[default]
    Replay,
}

/// Settings for one reliable, unordered UDP group.
#[derive(Clone)]
pub struct UdpGroupConfig {
    pub name: &'static str,
    pub udp: UdpConfig,
    pub on_connect_msg: Option<Vec<u8>>,
    pub socket_buf_size: Option<usize>,
    pub peer_timeout: Duration,
    /// Unacknowledged datagrams and how long the threshold may be exceeded.
    pub max_backlog_datagrams: Option<(usize, Duration)>,
    pub replay: ReplayPolicy,
    pub telemetry: NetworkTelemetry,
}

impl Default for UdpGroupConfig {
    fn default() -> Self {
        Self {
            name: "udp",
            udp: UdpConfig::default(),
            on_connect_msg: None,
            socket_buf_size: None,
            peer_timeout: Duration::from_secs(10),
            max_backlog_datagrams: None,
            replay: ReplayPolicy::Replay,
            telemetry: NetworkTelemetry::Disabled,
        }
    }
}

/// A group fixes its transport and configuration for its lifetime.
#[derive(Clone)]
pub enum GroupConfig {
    Tcp(TcpGroupConfig),
    Udp(UdpGroupConfig),
}

impl GroupConfig {
    /// Sets the requested `SO_SNDBUF` and `SO_RCVBUF` size in bytes.
    pub fn with_socket_buf_size(mut self, bytes: usize) -> Self {
        match &mut self {
            Self::Tcp(config) => config.socket_buf_size = Some(bytes),
            Self::Udp(config) => config.socket_buf_size = Some(bytes),
        }
        self
    }
}

impl From<TcpGroupConfig> for GroupConfig {
    fn from(config: TcpGroupConfig) -> Self {
        Self::Tcp(config)
    }
}
impl From<UdpGroupConfig> for GroupConfig {
    fn from(config: UdpGroupConfig) -> Self {
        Self::Udp(config)
    }
}

/// Only live tokens occupy the route map; token identities are never reused.
pub(crate) struct Tokens {
    range: Range<usize>,
    next: usize,
    groups: FxHashMap<Token, Group>,
}
impl Tokens {
    fn new(range: Range<usize>) -> Self {
        Self { next: range.start, range, groups: FxHashMap::default() }
    }
    pub(crate) fn allocate(&mut self, group: Group) -> Token {
        assert!(self.next < self.range.end, "network token range {:?} exhausted", self.range);
        let token = Token(self.next);
        self.next += 1;
        self.groups.insert(token, group);
        token
    }
    pub(crate) fn retire(&mut self, token: Token) {
        self.groups.remove(&token);
    }
}

enum GroupState {
    Tcp(Box<TcpManager>),
    Udp(Box<UdpManager>),
}

/// Groups of TCP connections and reliable UDP sessions sharing one poll.
///
/// Message payloads are borrowed for the synchronous callback. UDP delivery is
/// reliable but unordered; reconnect replay does not provide exactly-once
/// delivery.
pub struct Network {
    events: Events,
    core: NetworkCore,
}

impl Default for Network {
    fn default() -> Self {
        let poll = Poll::new().expect("failed to create poll");
        Self {
            events: Events::with_capacity(EVENTS_CAPACITY),
            core: NetworkCore::new(Poller::Owned(poll), 0..usize::MAX),
        }
    }
}

impl Deref for Network {
    type Target = NetworkCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl DerefMut for Network {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
}

impl Network {
    /// Reads framed payloads directly into this dcache. Drain with
    /// `poll_with_produce`; raw TCP groups cannot use this receive mode.
    /// Attach before creating any endpoints or listeners.
    pub fn with_dcache(mut self, dcache: DCachePtr) -> Self {
        for group in &self.core.groups {
            match group {
                GroupState::Tcp(tcp) => {
                    assert!(tcp.is_empty(), "attach dcache before creating endpoints");
                    assert!(
                        tcp.config.framing == Framing::LengthPrefixed,
                        "dcache requires framed TCP"
                    );
                }
                GroupState::Udp(udp) => {
                    assert!(udp.is_empty(), "attach dcache before creating endpoints");
                }
            }
        }
        self.core.dcache = Some(dcache);
        self
    }

    fn drive<F>(&mut self, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let mut work = self.core.pre_poll(handler);
        let Poller::Owned(poll) = &mut self.core.poller else {
            unreachable!("a Network always owns its poll")
        };
        if let Err(err) = poll.poll(&mut self.events, Some(std::time::Duration::ZERO)) {
            if err.kind() != io::ErrorKind::Interrupted {
                flux_utils::safe_panic!("couldn't poll network: {err}");
            }
            return work;
        }
        for event in &self.events {
            work = true;
            self.core.handle_event(event, handler);
        }
        work | self.core.post_poll(handler)
    }

    /// Performs one nonblocking poll and processes readiness and due timers.
    /// Returns whether readiness or lifecycle notifications were processed.
    pub fn poll_with<F>(&mut self, mut handler: F) -> bool
    where
        F: for<'a> FnMut(NetworkEvent<'a>),
    {
        assert!(self.core.dcache.is_none(), "use poll_with_produce with a dcache");
        self.drive(&mut |event| {
            handler(event.map_payload(|payload| match payload {
                RxPayload::Raw(bytes) => bytes,
                RxPayload::DCache(_) => unreachable!(),
            }));
        })
    }

    /// Produces references to messages received directly in the attached
    /// dcache. Returning `None` from the callback skips production for that
    /// message.
    pub fn poll_with_produce<T, P, F>(&mut self, produce: &mut P, mut handler: F) -> bool
    where
        T: 'static + Copy,
        P: SpineProducers + AsRef<SpineProducerWithDCache<T>>,
        F: for<'a> FnMut(NetworkEvent<'a>) -> Option<T>,
    {
        let dcache = self.core.dcache.expect("dcache required for poll_with_produce");
        self.drive(&mut |event| match event {
            Event::Message { group, token, payload: RxPayload::DCache(dref), send_ts } => {
                match dcache
                    .map(dref, |payload| handler(Event::Message { group, token, payload, send_ts }))
                {
                    Ok(Some(value)) => produce.produce_with_dref(value, dref, send_ts),
                    Ok(None) => {}
                    Err(err) => warn!(?err, "dcache map failed"),
                }
            }
            other => {
                handler(
                    other.map_payload(|_| unreachable!("dcache receive returned heap payload")),
                );
            }
        })
    }
}

/// Where a [`NetworkCore`] registers its sockets. Every group borrows this one
/// registry, so sockets are registered on the descriptor that is polled rather
/// than on a `Registry::try_clone` duplicate of it.
enum Poller {
    Owned(Poll),
    External(Registry),
}

impl Poller {
    #[inline]
    fn registry(&self) -> &Registry {
        match self {
            Self::Owned(poll) => poll.registry(),
            Self::External(registry) => registry,
        }
    }
}

/// Transport state and operations, without an owned poll. Components can borrow
/// this core to create their own groups and send through the shared network.
pub struct NetworkCore {
    poller: Poller,
    groups: Vec<GroupState>,
    tokens: Tokens,
    dcache: Option<DCachePtr>,
}

impl NetworkCore {
    fn new(poller: Poller, tokens: Range<usize>) -> Self {
        Self {
            poller,
            groups: Vec::with_capacity(INITIAL_GROUP_CAPACITY),
            tokens: Tokens::new(tokens),
            dcache: None,
        }
    }

    #[inline]
    fn route(&self, token: Token) -> Option<Group> {
        if self.groups.len() == 1 {
            Some(Group(0))
        } else {
            self.tokens.groups.get(&token).copied()
        }
    }

    #[must_use = "the group handle identifies listeners and outbound endpoints"]
    pub fn add_group(&mut self, config: impl Into<GroupConfig>) -> Group {
        let group = Group(self.groups.len());
        let state = match config.into() {
            GroupConfig::Tcp(config) => {
                assert!(
                    self.dcache.is_none() || config.framing == Framing::LengthPrefixed,
                    "dcache requires framed TCP"
                );
                GroupState::Tcp(Box::new(TcpManager::new(config, group)))
            }
            GroupConfig::Udp(config) => GroupState::Udp(Box::new(UdpManager::new(config, group))),
        };
        self.groups.push(state);
        group
    }

    /// Returns the listener token, which can be passed to `remove` to stop
    /// listening.
    pub fn listen(&mut self, group: Group, addr: SocketAddr) -> io::Result<Token> {
        match self.groups.get_mut(group.0) {
            Some(GroupState::Tcp(tcp)) => {
                tcp.listen(self.poller.registry(), addr, &mut self.tokens)
            }
            Some(GroupState::Udp(udp)) => {
                udp.listen(self.poller.registry(), addr, &mut self.tokens)
            }
            None => Err(io::Error::new(io::ErrorKind::InvalidInput, "unknown group")),
        }
    }
    /// Registers an outbound endpoint and starts establishment. Its token
    /// remains stable across retries, and a failed dial or socket open is
    /// retried from the poll. `Connected` reports every successful
    /// establishment.
    #[must_use]
    pub fn connect(&mut self, group: Group, addr: SocketAddr) -> Token {
        match &mut self.groups[group.0] {
            GroupState::Tcp(tcp) => {
                tcp.connect(self.poller.registry(), addr, None, None, &mut self.tokens)
            }
            GroupState::Udp(udp) => udp.connect(self.poller.registry(), addr, &mut self.tokens),
        }
    }
    /// Like [`Self::connect`], but every dial binds `local_ip` as the source
    /// address. TCP-only; panics when used with a UDP group or when `local_ip`
    /// and `addr` are of different families.
    #[must_use]
    pub fn connect_from(&mut self, group: Group, addr: SocketAddr, local_ip: IpAddr) -> Token {
        assert_eq!(addr.is_ipv4(), local_ip.is_ipv4(), "local and peer address families differ");
        let GroupState::Tcp(tcp) = &mut self.groups[group.0] else {
            panic!("operation requires a TCP group");
        };
        tcp.connect(self.poller.registry(), addr, Some(local_ip), None, &mut self.tokens)
    }
    /// Client TLS is TCP-only. Panics when used with a UDP or replay group.
    #[must_use]
    pub fn connect_tls(&mut self, group: Group, addr: SocketAddr, tls: Session) -> Token {
        self.connect_tls_with(group, addr, None, tls)
    }
    /// [`Self::connect_tls`] from `local_ip`; see [`Self::connect_from`].
    #[must_use]
    pub fn connect_tls_from(
        &mut self,
        group: Group,
        addr: SocketAddr,
        local_ip: IpAddr,
        tls: Session,
    ) -> Token {
        assert_eq!(addr.is_ipv4(), local_ip.is_ipv4(), "local and peer address families differ");
        self.connect_tls_with(group, addr, Some(local_ip), tls)
    }
    fn connect_tls_with(
        &mut self,
        group: Group,
        addr: SocketAddr,
        local_ip: Option<IpAddr>,
        tls: Session,
    ) -> Token {
        let GroupState::Tcp(tcp) = &mut self.groups[group.0] else {
            panic!("operation requires a TCP group");
        };
        assert!(
            tcp.config.replay == ReplayPolicy::Drop,
            "TLS does not support wire backlog replay"
        );
        assert!(tcp.config.max_backlog_frames.is_none(), "TLS requires byte backlog limits");
        tcp.connect(self.poller.registry(), addr, local_ip, Some(Box::new(tls)), &mut self.tokens)
    }

    /// Returns whether the message was accepted for sending or replay. Unknown,
    /// draining, or disconnected drop-policy tokens reject without serializing.
    /// Empty/oversized payloads reject after the serializer runs.
    pub fn send_with<F>(&mut self, token: Token, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let Some(group) = self.route(token) else { return false };
        match &mut self.groups[group.0] {
            GroupState::Tcp(tcp) => {
                tcp.send_with(self.poller.registry(), token, &mut self.tokens, serialise)
            }
            GroupState::Udp(udp) => udp.send_with(self.poller.registry(), token, serialise),
        }
    }
    /// Sends `head` then `body` as one message. A plain TCP connection
    /// writes them from the slices in one vectored write and copies only the
    /// unwritten remainder; elsewhere it behaves like [`Self::send_with`].
    pub fn send_parts(&mut self, token: Token, head: &[u8], body: &[u8]) -> bool {
        let Some(group) = self.route(token) else { return false };
        match &mut self.groups[group.0] {
            GroupState::Tcp(tcp) => {
                tcp.send_parts(self.poller.registry(), token, &mut self.tokens, head, body)
            }
            GroupState::Udp(udp) => udp.send_with(self.poller.registry(), token, |out| {
                out.extend_from_slice(head);
                out.extend_from_slice(body);
            }),
        }
    }
    /// Sends a bounded batch. TCP stages one write; UDP retains message
    /// boundaries. Invalid payloads are skipped. Returns whether any
    /// message was accepted.
    pub fn send_many_with<I, F>(&mut self, token: Token, items: I, serialise: F) -> bool
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        let Some(group) = self.route(token) else { return false };
        match &mut self.groups[group.0] {
            GroupState::Tcp(tcp) => tcp.send_many_with(
                self.poller.registry(),
                token,
                &mut self.tokens,
                items,
                serialise,
            ),
            GroupState::Udp(udp) => {
                udp.send_many_with(self.poller.registry(), token, items, serialise)
            }
        }
    }
    /// Serializes once for all eligible peers in this group. Returns the number
    /// attempted; a paused peer is excluded. No recipients means no
    /// serialization.
    pub fn broadcast_with<F>(&mut self, group: Group, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        match &mut self.groups[group.0] {
            GroupState::Tcp(tcp) => {
                tcp.broadcast_with(self.poller.registry(), &mut self.tokens, serialise)
            }
            GroupState::Udp(udp) => udp.broadcast_with(self.poller.registry(), serialise),
        }
    }
    pub fn broadcast_many_with<I, F>(&mut self, group: Group, items: I, serialise: F) -> usize
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        match &mut self.groups[group.0] {
            GroupState::Tcp(tcp) => {
                tcp.broadcast_many_with(self.poller.registry(), &mut self.tokens, items, serialise)
            }
            GroupState::Udp(udp) => {
                udp.broadcast_many_with(self.poller.registry(), items, serialise)
            }
        }
    }
    pub fn disconnect(&mut self, token: Token) -> bool {
        let Some(group) = self.tokens.groups.get(&token).copied() else { return false };
        match &mut self.groups[group.0] {
            GroupState::Tcp(tcp) => tcp.disconnect(self.poller.registry(), token, &mut self.tokens),
            GroupState::Udp(udp) => udp.disconnect(token),
        }
    }
    /// Closes the connection or session once everything queued for it has
    /// been written (TCP) or acked (UDP). Sends to it are refused meanwhile.
    /// Returns `false` for an unknown or disconnected token.
    pub fn disconnect_when_drained(&mut self, token: Token) -> bool {
        let Some(group) = self.tokens.groups.get(&token).copied() else { return false };
        match &mut self.groups[group.0] {
            GroupState::Tcp(tcp) => {
                tcp.disconnect_when_drained(self.poller.registry(), token, &mut self.tokens)
            }
            GroupState::Udp(udp) => udp.disconnect_when_drained(token),
        }
    }
    /// Permanently removes an endpoint or listener.
    pub fn remove(&mut self, token: Token) -> bool {
        let Some(group) = self.tokens.groups.get(&token).copied() else { return false };
        let removed = match &mut self.groups[group.0] {
            GroupState::Tcp(tcp) => tcp.remove(self.poller.registry(), token),
            GroupState::Udp(udp) => udp.remove(self.poller.registry(), token),
        };
        if removed {
            self.tokens.retire(token);
        }
        removed
    }
    pub fn pause_broadcast(&mut self, token: Token) {
        if let Some(group) = self.tokens.groups.get(&token).copied() {
            match &mut self.groups[group.0] {
                GroupState::Tcp(tcp) => tcp.pause_broadcast(token),
                GroupState::Udp(udp) => udp.pause_broadcast(token),
            }
        }
    }
    pub fn resume_broadcast(&mut self, token: Token) {
        if let Some(group) = self.tokens.groups.get(&token).copied() {
            match &mut self.groups[group.0] {
                GroupState::Tcp(tcp) => tcp.resume_broadcast(token),
                GroupState::Udp(udp) => udp.resume_broadcast(token),
            }
        }
    }
    pub fn is_broadcast_paused(&self, token: Token) -> bool {
        self.tokens.groups.get(&token).is_some_and(|group| match &self.groups[group.0] {
            GroupState::Tcp(tcp) => tcp.is_broadcast_paused(token),
            GroupState::Udp(udp) => udp.is_broadcast_paused(token),
        })
    }
    /// Drops the messages queued for `token` that have not started going out,
    /// keeping one that is partly on the wire. Returns how many were dropped.
    /// Raw and TLS streams carry no droppable message boundaries and return 0.
    pub fn clear_backlog(&mut self, token: Token) -> usize {
        let Some(group) = self.tokens.groups.get(&token).copied() else { return 0 };
        match &mut self.groups[group.0] {
            GroupState::Tcp(tcp) => tcp.clear_backlog(token),
            GroupState::Udp(udp) => udp.clear_backlog(token),
        }
    }
    pub fn currently_disconnected(&self) -> impl Iterator<Item = Token> + '_ {
        self.groups
            .iter()
            .filter_map(|group| match group {
                GroupState::Tcp(tcp) => Some(tcp.currently_disconnected()),
                GroupState::Udp(_) => None,
            })
            .flatten()
            .chain(
                self.groups
                    .iter()
                    .filter_map(|group| match group {
                        GroupState::Tcp(_) => None,
                        GroupState::Udp(udp) => Some(udp.currently_disconnected()),
                    })
                    .flatten(),
            )
    }
    pub fn force_reconnect(&mut self) {
        for group in &mut self.groups {
            match group {
                GroupState::Tcp(tcp) => tcp.force_reconnect(self.poller.registry()),
                GroupState::Udp(udp) => udp.force_reconnect(self.poller.registry()),
            }
        }
    }
    pub fn disconnect_outbound(&mut self) {
        for group in &mut self.groups {
            match group {
                GroupState::Tcp(tcp) => {
                    tcp.disconnect_outbound(self.poller.registry(), &mut self.tokens);
                }
                GroupState::Udp(udp) => udp.disconnect_outbound(),
            }
        }
    }

    fn pre_poll<F>(&mut self, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let mut work = false;
        for group in &mut self.groups {
            work |= match group {
                GroupState::Tcp(tcp) => {
                    tcp.pre_poll(self.poller.registry(), &mut self.tokens, handler)
                }
                GroupState::Udp(udp) => udp.pre_poll(self.poller.registry(), handler),
            };
        }
        work
    }
    fn post_poll<F>(&mut self, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let mut work = false;
        for group in &mut self.groups {
            work |= match group {
                GroupState::Tcp(tcp) => tcp.post_poll(handler),
                GroupState::Udp(udp) => udp.post_poll(&mut self.tokens, handler),
            };
        }
        work
    }
    fn handle_event<F>(&mut self, event: &mio::event::Event, handler: &mut F)
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let Some(group) = self.route(event.token()) else { return };
        match &mut self.groups[group.0] {
            GroupState::Tcp(tcp) => {
                tcp.handle_event(
                    self.poller.registry(),
                    event,
                    &mut self.tokens,
                    self.dcache.as_deref(),
                    handler,
                );
            }
            GroupState::Udp(udp) => {
                udp.handle_event(
                    self.poller.registry(),
                    event,
                    &mut self.tokens,
                    self.dcache.as_deref(),
                    handler,
                );
            }
        }
    }
}

impl Drop for NetworkCore {
    fn drop(&mut self) {
        let registry = self.poller.registry();
        for group in &mut self.groups {
            match group {
                GroupState::Tcp(tcp) => tcp.close_all(registry),
                GroupState::Udp(udp) => udp.close_all(registry),
            }
        }
    }
}

/// A network using a caller-owned poll and token range.
///
/// Each cycle calls
/// `pre_poll`, polls once, forwards readiness through `handle_event`, then
/// calls `post_poll`. Bound the poll timeout with `max_poll_interval` so UDP
/// maintenance runs even in the absence of readiness. Zero-timeout polling is
/// also supported.
pub struct NetworkWithExternalPoll {
    core: NetworkCore,
}

impl Deref for NetworkWithExternalPoll {
    type Target = NetworkCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl DerefMut for NetworkWithExternalPoll {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
}

impl NetworkWithExternalPoll {
    pub fn new(registry: Registry, tokens: Range<usize>) -> Self {
        Self { core: NetworkCore::new(Poller::External(registry), tokens) }
    }
    pub fn max_poll_interval(&self) -> std::time::Duration {
        self.core
            .groups
            .iter()
            .map(|group| match group {
                GroupState::Tcp(tcp) => {
                    tcp.config.reconnect_interval.min(tcp.config.handshake_timeout).min(
                        tcp.config.max_backlog_frames.map_or(Duration::MAX, |(_, timeout)| timeout),
                    )
                }
                GroupState::Udp(udp) => udp.tick_interval(),
            })
            .min()
            .unwrap_or_else(|| Duration::from_secs(1))
            .into()
    }
    pub fn pre_poll<F>(&mut self, handler: &mut F)
    where
        F: for<'a> FnMut(NetworkEvent<'a>),
    {
        self.core.pre_poll(&mut |event| handler(event.map_payload(raw_payload)));
    }
    pub fn post_poll<F>(&mut self, handler: &mut F)
    where
        F: for<'a> FnMut(NetworkEvent<'a>),
    {
        self.core.post_poll(&mut |event| handler(event.map_payload(raw_payload)));
    }
    pub fn handle_event<F>(&mut self, event: &mio::event::Event, handler: &mut F)
    where
        F: for<'a> FnMut(NetworkEvent<'a>),
    {
        debug_assert!(
            self.core.tokens.range.contains(&event.token().0),
            "readiness lies outside this network's token range"
        );
        self.core.handle_event(event, &mut |event| handler(event.map_payload(raw_payload)));
    }
}
fn raw_payload(payload: RxPayload<'_>) -> &[u8] {
    match payload {
        RxPayload::Raw(bytes) => bytes,
        RxPayload::DCache(_) => unreachable!(),
    }
}

/// Set kernel `SO_SNDBUF` and `SO_RCVBUF` on a socket.
///
/// The kernel silently clamps requests to `net.core.wmem_max` /
/// `net.core.rmem_max`; a warning is logged when that happens because an
/// undersized buffer shows up as packet loss rather than an error.
pub(crate) fn set_socket_buf_size(socket: &impl AsRawFd, size: usize) {
    let fd = socket.as_raw_fd();
    let requested = size as libc::c_int;
    for (option, name, limit) in [
        (libc::SO_SNDBUF, "SO_SNDBUF", "net.core.wmem_max"),
        (libc::SO_RCVBUF, "SO_RCVBUF", "net.core.rmem_max"),
    ] {
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                option,
                ptr::from_ref(&requested).cast::<libc::c_void>(),
                core::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }

        let mut granted: libc::c_int = 0;
        let mut granted_length = core::mem::size_of::<libc::c_int>() as libc::socklen_t;
        let result = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                option,
                ptr::from_mut(&mut granted).cast::<libc::c_void>(),
                ptr::from_mut(&mut granted_length),
            )
        };
        // Linux stores and reports double the granted value to account for
        // bookkeeping overhead, so an unclamped request reads back as exactly
        // `2 * size`.
        if result == 0 && i64::from(granted) < 2 * size as i64 {
            warn!(requested = size, granted, "kernel clamped {name}; raise {limit}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PayloadBuf;

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
