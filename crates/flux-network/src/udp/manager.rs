//! UDP group sockets and peers driven by the shared network poll.
//!
//! One socket per `listen` (shared by every peer that dials it) and one per
//! `connect` (owned by that single peer). Peers hold all session state, see
//! [`Peer`]. Message bytes live once in a [`MessageStore`] shared by every
//! peer sending them, and due packets of all peers on a socket go out in one
//! `sendmmsg` batch.

use std::{
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    os::fd::AsRawFd,
};

use flux_timing::{Duration, Instant};
use flux_utils::DCache;
use mio::{Interest, Registry, Token, event::Event as MioEvent, net::UdpSocket};
use tracing::{debug, info, warn};

use super::{
    outbound::{MessageStore, WindowFull},
    peer::{Peer, SendOutcome},
    sys::{RecvBatch, SendBatch},
    wire::{self, Datagram, LONG_HEADER, SHORT_HEADER},
};
use crate::network::{
    Event, Group, PayloadBuf, ReplayPolicy, RxPayload, Tokens, UdpGroupConfig, set_socket_buf_size,
};

struct Endpoint {
    token: Token,
    socket: UdpSocket,
    listener: bool,
    writable_armed: bool,
}

/// Session id for a new connection attempt: mixes the TSC, the pid and the
/// peer token. Called once per connect, accept and reconnect.
fn new_session(salt: usize) -> u32 {
    let t = Instant::now().0;
    let mut x = t ^
        (t >> 32) ^
        (u64::from(std::process::id()) << 20) ^
        (salt as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x as u32
}

#[inline]
fn arm_writable(registry: &Registry, entry: &mut Endpoint) {
    if entry.writable_armed {
        return;
    }
    if let Err(err) =
        registry.reregister(&mut entry.socket, entry.token, Interest::READABLE | Interest::WRITABLE)
    {
        debug!(?err, "udp: reregister writable");
        return;
    }
    entry.writable_armed = true;
}

/// Queues the on-connect message, as the stream `greeting`, for a peer
/// whose session was just set up.
fn push_on_connect(
    store: &mut MessageStore,
    greeting: Option<&[u8]>,
    peer: &mut Peer,
    now: Instant,
) {
    if let Some(stream) = greeting {
        let slot = store.insert(&mut stream.to_vec());
        let _ = peer.enqueue(store, slot, now);
        store.release(slot);
    }
}

/// The on-connect message as a one-record stream.
fn greeting_stream(msg: &[u8]) -> Vec<u8> {
    let mut header = [0; LONG_HEADER];
    let n = wire::message_header(&mut header, msg.len());
    let mut stream = Vec::with_capacity(n + msg.len());
    stream.extend_from_slice(&header[..n]);
    stream.extend_from_slice(msg);
    stream
}

/// The socket registered under `token`. An outbound peer whose bind failed
/// has none until the next retry.
#[inline]
fn socket_of(sockets: &[Endpoint], token: Token) -> Option<usize> {
    sockets.iter().position(|s| s.token == token)
}

fn outbound_bind_addr(peer: SocketAddr) -> SocketAddr {
    match peer {
        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
    }
}

pub(crate) struct UdpManager {
    pub(crate) config: UdpGroupConfig,
    group: Group,
    sockets: Vec<Endpoint>,
    peers: Vec<Peer>,
    store: MessageStore,
    send_buffer: Vec<u8>,
    /// A record that did not fit the stream being queued, kept for the next.
    spill: Vec<u8>,
    /// `on_connect_msg` as a stream.
    greeting: Option<Vec<u8>>,
    batch: SendBatch,
    /// Peers with packets in `batch` and how many each, in order.
    staged: Vec<(usize, usize)>,
    /// Taken out while datagrams are dispatched so peers can be borrowed.
    recv: Option<RecvBatch>,
    pending_disconnects: Vec<(Token, SocketAddr)>,
    retired: Vec<Token>,
    broadcast_paused: Vec<Token>,
    /// Peer maintenance runs at most this often; half the minimum RTO keeps
    /// recovery timing within tolerance while idle polls stay cheap.
    tick_interval: Duration,
    next_tick: Instant,
    /// Outbound peers whose socket could not be opened; retried on the
    /// heartbeat interval and by `force_reconnect`.
    unbound: usize,
    next_bind: Instant,
}

impl UdpManager {
    pub(crate) fn new(config: UdpGroupConfig, group: Group) -> Self {
        let udp = config.udp;
        udp.validate();
        assert!(
            config
                .on_connect_msg
                .as_ref()
                .is_none_or(|msg| !msg.is_empty() && msg.len() <= udp.max_message_size),
            "invalid UDP on-connect message length"
        );
        let greeting = config.on_connect_msg.as_deref().map(greeting_stream);
        Self {
            config,
            greeting,
            group,
            sockets: Vec::new(),
            peers: Vec::new(),
            store: MessageStore::new(),
            send_buffer: Vec::with_capacity(32 * 1024),
            spill: Vec::new(),
            batch: SendBatch::new(udp.max_datagram_size),
            staged: Vec::new(),
            recv: Some(RecvBatch::new(udp.max_datagram_size)),
            pending_disconnects: Vec::new(),
            broadcast_paused: Vec::new(),
            retired: Vec::new(),
            tick_interval: udp.min_rto / 2_u32,
            next_tick: Instant::ZERO,
            unbound: 0,
            next_bind: Instant::ZERO,
        }
    }

    /// Deregisters every socket before the group is dropped.
    pub(crate) fn close_all(&mut self, registry: &Registry) {
        for endpoint in &mut self.sockets {
            let _ = registry.deregister(&mut endpoint.socket);
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.sockets.is_empty()
    }

    /// Opens a socket on `bind` registered under `token` and adds it to the
    /// endpoints.
    fn open_socket(
        &mut self,
        registry: &Registry,
        bind: SocketAddr,
        token: Token,
        listener: bool,
    ) -> io::Result<()> {
        let mut socket = UdpSocket::bind(bind)?;
        #[cfg(target_os = "linux")]
        if let Err(err) = RecvBatch::enable_gro(socket.as_raw_fd()) {
            debug!(?err, "UDP GRO unavailable");
        }
        #[cfg(target_os = "linux")]
        if let Err(err) = self.batch.enable_gso(socket.as_raw_fd()) {
            debug!(?err, "UDP GSO unavailable");
        }
        if let Some(size) = self.config.socket_buf_size {
            set_socket_buf_size(&socket, size);
        }
        registry.register(&mut socket, token, Interest::READABLE)?;
        self.sockets.push(Endpoint { token, socket, listener, writable_armed: false });
        Ok(())
    }

    /// Registers an outbound peer. If its socket cannot be opened now, the
    /// peer waits unbound and the open is retried like a redial.
    pub(crate) fn connect(
        &mut self,
        registry: &Registry,
        addr: SocketAddr,
        tokens: &mut Tokens,
    ) -> Token {
        let token = tokens.allocate(self.group);
        let mut peer = Peer::new(addr, token, token, new_session(token.0), &self.config);
        let now = Instant::now();
        // First in the queue; nothing goes out before the handshake anyway.
        push_on_connect(&mut self.store, self.greeting.as_deref(), &mut peer, now);
        match self.open_socket(registry, outbound_bind_addr(addr), token, false) {
            Ok(()) => {
                peer.send_hello(&self.sockets.last().unwrap().socket, now);
            }
            Err(err) => {
                warn!(?err, %addr, "couldn't open udp socket, retrying");
                self.unbound += 1;
            }
        }
        self.peers.push(peer);
        token
    }

    /// Retries the socket open for every unbound outbound peer.
    fn bind_unbound(&mut self, registry: &Registry, now: Instant) {
        for i in 0..self.peers.len() {
            let peer = &self.peers[i];
            if !peer.is_outbound() || socket_of(&self.sockets, peer.token).is_some() {
                continue;
            }
            let (addr, token) = (peer.addr, peer.token);
            match self.open_socket(registry, outbound_bind_addr(addr), token, false) {
                Ok(()) => {
                    self.unbound -= 1;
                    let socket = &self.sockets.last().unwrap().socket;
                    self.peers[i].send_hello(socket, now);
                }
                Err(err) => debug!(?err, %addr, "udp socket open failed again"),
            }
        }
    }

    pub(crate) fn listen(
        &mut self,
        registry: &Registry,
        addr: SocketAddr,
        tokens: &mut Tokens,
    ) -> io::Result<Token> {
        let token = tokens.allocate(self.group);
        if let Err(err) = self.open_socket(registry, addr, token, true) {
            tokens.retire(token);
            return Err(err);
        }
        Ok(token)
    }

    /// Resets an outbound peer to a fresh session and starts dialling again.
    /// The on-connect message is queued behind any retained backlog unless a
    /// never-acked copy still heads it; UDP delivery is unordered regardless.
    fn reset_outbound(&mut self, index: usize, now: Instant) {
        let peer = &mut self.peers[index];
        let session = new_session(peer.token.0);
        peer.mark_disconnected(self.config.replay == ReplayPolicy::Drop, session, &mut self.store);
        let greeting_retained = self
            .greeting
            .as_deref()
            .is_some_and(|msg| peer.outbound.oldest_retained(&self.store) == Some(msg));
        if !greeting_retained {
            push_on_connect(&mut self.store, self.greeting.as_deref(), peer, now);
        }
        if let Some(k) = socket_of(&self.sockets, peer.token) {
            peer.send_hello(&self.sockets[k].socket, now);
        }
    }

    /// Removes an accepted peer, returning its token.
    fn remove_peer(&mut self, index: usize) -> Token {
        let mut peer = self.peers.swap_remove(index);
        peer.outbound.release_all(&mut self.store);
        self.resume_broadcast(peer.token);
        self.retired.push(peer.token);
        peer.token
    }

    pub(crate) fn pause_broadcast(&mut self, token: Token) {
        if !self.broadcast_paused.contains(&token) {
            self.broadcast_paused.push(token);
        }
    }

    pub(crate) fn resume_broadcast(&mut self, token: Token) {
        if let Some(i) = self.broadcast_paused.iter().position(|t| *t == token) {
            self.broadcast_paused.swap_remove(i);
        }
    }

    pub(crate) fn is_broadcast_paused(&self, token: Token) -> bool {
        self.broadcast_paused.contains(&token)
    }

    /// Outbound peers renegotiate; accepted peers are dropped.
    fn drop_peer(&mut self, index: usize, now: Instant) {
        if self.peers[index].is_outbound() {
            self.reset_outbound(index, now);
        } else {
            self.remove_peer(index);
        }
    }

    fn drop_peer_pending(&mut self, index: usize, now: Instant) {
        if self.peers[index].is_connected() {
            self.pending_disconnects.push((self.peers[index].token, self.peers[index].addr));
        }
        self.drop_peer(index, now);
    }

    /// Drops a peer's session: accepted peers go, outbound ones redial. A
    /// listener token is not a session and is left alone.
    pub(crate) fn disconnect(&mut self, token: Token) -> bool {
        let Some(i) = self.peers.iter().position(|p| p.token == token) else { return false };
        self.drop_peer_pending(i, Instant::now());
        true
    }

    /// Closes the session once everything queued for it is acked. Sends to
    /// it are refused meanwhile.
    pub(crate) fn disconnect_when_drained(&mut self, token: Token) -> bool {
        let Some(i) = self.peers.iter().position(|p| p.token == token) else { return false };
        if self.peers[i].close_when_drained() {
            self.drop_peer_pending(i, Instant::now());
        }
        true
    }

    /// Drops the messages queued for `token` that have not started going
    /// out. Returns how many.
    pub(crate) fn clear_backlog(&mut self, token: Token) -> usize {
        let Some(i) = self.peers.iter().position(|p| p.token == token) else { return 0 };
        self.peers[i].outbound.clear_unsent(&mut self.store)
    }

    /// Removes a listener and every session accepted on it.
    fn remove_listener(&mut self, registry: &Registry, token: Token) -> bool {
        let Some(k) = self.sockets.iter().position(|s| s.token == token && s.listener) else {
            return false;
        };
        let mut i = self.peers.len();
        while i != 0 {
            i -= 1;
            if self.peers[i].socket_token == token {
                let addr = self.peers[i].addr;
                let dropped = self.remove_peer(i);
                self.pending_disconnects.push((dropped, addr));
            }
        }
        let mut entry = self.sockets.swap_remove(k);
        let _ = registry.deregister(&mut entry.socket);
        self.retired.push(token);
        true
    }

    pub(crate) fn disconnect_outbound(&mut self) {
        let now = Instant::now();
        for i in 0..self.peers.len() {
            if self.peers[i].is_outbound() {
                self.drop_peer_pending(i, now);
            }
        }
    }

    pub(crate) fn send_with<F>(&mut self, registry: &Registry, token: Token, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let Some(index) = self.sendable_index(token) else { return false };
        let now = Instant::now();
        if self.append_record(serialise).is_none() {
            return false;
        }
        let sent = self.queue_stream_for(index, token, now);
        if sent {
            self.flush_peer_socket(registry, index, now);
        }
        sent
    }

    /// Serialises every item into one stream for `token`, queues it and
    /// flushes the socket once. A stream that outgrows `max_message_size`
    /// is queued in parts.
    pub(crate) fn send_many_with<I, F>(
        &mut self,
        registry: &Registry,
        token: Token,
        items: I,
        mut serialise: F,
    ) -> bool
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        let Some(index) = self.sendable_index(token) else { return false };
        let now = Instant::now();
        let mut sent = false;
        for item in items {
            if let Some(at) = self.append_record(|buf| serialise(buf, item)) &&
                self.spill_overflow(at)
            {
                sent |= self.queue_stream_for(index, token, now);
                std::mem::swap(&mut self.send_buffer, &mut self.spill);
                if !self.peer_is(index, token) {
                    // The peer was dropped for violating a limit.
                    self.send_buffer.clear();
                    return false;
                }
            }
        }
        sent |= self.queue_stream_for(index, token, now);
        if sent && self.peer_is(index, token) {
            self.flush_peer_socket(registry, index, now);
        }
        sent
    }

    pub(crate) fn broadcast_with<F>(&mut self, registry: &Registry, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        if !self.has_broadcast_recipient() {
            return 0;
        }
        let now = Instant::now();
        if self.append_record(serialise).is_none() {
            return 0;
        }
        let recipients = self.queue_stream_broadcast(now);
        self.flush_all(registry, now);
        recipients
    }

    /// Serialises every item into one stream for every recipient, queues it
    /// and flushes each socket once. A stream that outgrows
    /// `max_message_size` is queued in parts.
    pub(crate) fn broadcast_many_with<I, F>(
        &mut self,
        registry: &Registry,
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
        let now = Instant::now();
        let mut recipients = 0;
        for item in items {
            if let Some(at) = self.append_record(|buf| serialise(buf, item)) &&
                self.spill_overflow(at)
            {
                recipients = recipients.max(self.queue_stream_broadcast(now));
                std::mem::swap(&mut self.send_buffer, &mut self.spill);
            }
        }
        recipients = recipients.max(self.queue_stream_broadcast(now));
        self.flush_all(registry, now);
        recipients
    }

    /// Serialises one message behind its wire header at the end of the
    /// stream in `send_buffer`. Returns where the record starts, or `None`
    /// when the message was empty or too large and nothing was appended,
    /// which the TCP path also skips without sending.
    fn append_record<F>(&mut self, serialise: F) -> Option<usize>
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let at = self.send_buffer.len();
        self.send_buffer.extend_from_slice(&[0; SHORT_HEADER]);
        serialise(&mut PayloadBuf::new(&mut self.send_buffer));
        let len = self.send_buffer.len() - at - SHORT_HEADER;
        if len == 0 || len > self.config.udp.max_message_size {
            if len != 0 {
                warn!(
                    group = self.config.name,
                    len,
                    max = self.config.udp.max_message_size,
                    "udp message exceeds max_message_size"
                );
            }
            self.send_buffer.truncate(at);
            return None;
        }
        let mut header = [0; LONG_HEADER];
        let n = wire::message_header(&mut header, len);
        if n > SHORT_HEADER {
            self.send_buffer.resize(at + n + len, 0);
            self.send_buffer.copy_within(at + SHORT_HEADER..at + SHORT_HEADER + len, at + n);
        }
        self.send_buffer[at..at + n].copy_from_slice(&header[..n]);
        Some(at)
    }

    /// When the record at `at` took the stream past `max_message_size`, the
    /// size a window is sure to hold, moves it into `spill` so the stream
    /// before it can be queued on its own. Returns whether it did.
    fn spill_overflow(&mut self, at: usize) -> bool {
        if at == 0 || self.send_buffer.len() <= self.config.udp.max_message_size {
            return false;
        }
        self.spill.clear();
        self.spill.extend_from_slice(&self.send_buffer[at..]);
        self.send_buffer.truncate(at);
        true
    }

    #[inline]
    fn peer_is(&self, index: usize, token: Token) -> bool {
        self.peers.get(index).is_some_and(|p| p.token == token)
    }

    /// Moves the stream into the store and queues it for the peer at
    /// `index`, dropping the peer if it cannot take it. Leaves the stream
    /// empty. Returns whether it was queued.
    fn queue_stream_for(&mut self, index: usize, token: Token, now: Instant) -> bool {
        if self.send_buffer.is_empty() || !self.peer_is(index, token) {
            self.send_buffer.clear();
            return false;
        }
        let slot = self.store.insert(&mut self.send_buffer);
        let queued = self.enqueue(index, slot, now);
        self.store.release(slot);
        if !queued {
            self.drop_peer_pending(index, now);
        }
        queued
    }

    /// Moves the stream into the store and queues it for every eligible
    /// peer. Leaves the stream empty. Returns the number of recipients.
    fn queue_stream_broadcast(&mut self, now: Instant) -> usize {
        if self.send_buffer.is_empty() {
            return 0;
        }
        let slot = self.store.insert(&mut self.send_buffer);
        let mut recipients = 0;
        let mut i = self.peers.len();
        while i != 0 {
            i -= 1;
            if !self.is_broadcast_recipient(&self.peers[i]) {
                continue;
            }
            recipients += 1;
            if !self.enqueue(i, slot, now) {
                self.drop_peer_pending(i, now);
            }
        }
        self.store.release(slot);
        recipients
    }

    /// Queues the stored stream for one peer. `false` when the peer must be
    /// dropped: it violated the backlog limit or has no window room.
    #[inline]
    fn enqueue(&mut self, index: usize, slot: u32, now: Instant) -> bool {
        let peer = &mut self.peers[index];
        match peer.enqueue(&mut self.store, slot, now) {
            Ok(()) => !peer.outbound.backlog_exceeded(self.config.max_backlog_datagrams, now),
            Err(WindowFull) => false,
        }
    }

    fn flush_peer_socket(&mut self, registry: &Registry, index: usize, now: Instant) {
        if let Some(k) = socket_of(&self.sockets, self.peers[index].socket_token) {
            self.pump(registry, k, now, false);
        }
    }

    fn flush_all(&mut self, registry: &Registry, now: Instant) {
        for k in 0..self.sockets.len() {
            self.pump(registry, k, now, false);
        }
    }

    /// Sends every due packet of every peer on socket `k`, batched across
    /// peers. With `probing`, each peer may also resend what its timeout
    /// allows. Arms WRITABLE if the kernel stopped accepting.
    fn pump(&mut self, registry: &Registry, k: usize, now: Instant, probing: bool) {
        let token = self.sockets[k].token;
        let fd = self.sockets[k].socket.as_raw_fd();
        for i in 0..self.peers.len() {
            if self.peers[i].socket_token != token {
                continue;
            }
            let mut probes = if probing { self.peers[i].probe_budget(now) } else { 0 };
            loop {
                let n = self.peers[i].stage(&self.store, &mut self.batch, probes, now);
                probes = 0;
                if n != 0 {
                    self.staged.push((i, n));
                }
                if !self.batch.is_full() {
                    break;
                }
                if !self.dispatch(fd, now) {
                    arm_writable(registry, &mut self.sockets[k]);
                    return;
                }
            }
        }
        if !self.batch.is_empty() && !self.dispatch(fd, now) {
            arm_writable(registry, &mut self.sockets[k]);
        }
    }

    /// Sends the batch and tells each peer what the kernel took. `false` if
    /// it took less than everything.
    fn dispatch(&mut self, fd: i32, now: Instant) -> bool {
        let total: usize = self.staged.iter().map(|&(_, n)| n).sum();
        let accepted = match self.batch.send(fd) {
            Ok(k) => k,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
            // Other errors count as sent so the retransmit path retries.
            Err(e) => {
                debug!(?e, "udp send failed");
                total
            }
        };
        let mut remaining = accepted;
        for &(i, n) in &self.staged {
            let take = remaining.min(n);
            remaining -= take;
            self.peers[i].commit(&mut self.store, take, now);
        }
        self.staged.clear();
        accepted == total
    }

    pub(crate) fn currently_disconnected(&self) -> impl Iterator<Item = Token> {
        self.peers.iter().filter(|p| p.is_outbound() && !p.is_connected()).map(|p| p.token)
    }

    pub(crate) fn force_reconnect(&mut self, registry: &Registry) {
        let now = Instant::now();
        if self.unbound != 0 {
            self.bind_unbound(registry, now);
        }
        for peer in self.peers.iter_mut().filter(|p| p.is_outbound() && !p.is_connected()) {
            if let Some(k) = socket_of(&self.sockets, peer.token) {
                peer.send_hello(&self.sockets[k].socket, now);
            }
        }
    }

    /// Hello retries, heartbeats, peer timeouts, drained closes,
    /// retransmit probes and socket-open retries.
    fn tick(&mut self, registry: &Registry, now: Instant) {
        if self.unbound != 0 && now >= self.next_bind {
            self.next_bind = now + self.config.udp.heartbeat_interval;
            self.bind_unbound(registry, now);
        }
        let peer_timeout = self.config.peer_timeout;
        let mut i = self.peers.len();
        while i != 0 {
            i -= 1;
            let peer = &mut self.peers[i];
            if peer.drained() {
                self.drop_peer_pending(i, now);
                continue;
            }
            let Some(k) = socket_of(&self.sockets, peer.socket_token) else { continue };
            let entry = &mut self.sockets[k];
            match peer.tick(&entry.socket, now, peer_timeout) {
                Ok(SendOutcome::Done) => {}
                Ok(SendOutcome::WouldBlock) => arm_writable(registry, entry),
                Err(()) => {
                    warn!(addr = %peer.addr, "udp peer timed out");
                    self.drop_peer_pending(i, now);
                }
            }
        }
        for k in 0..self.sockets.len() {
            self.pump(registry, k, now, true);
        }
    }

    #[inline]
    fn peer_index(&self, k: usize, from: SocketAddr) -> Option<usize> {
        let socket_token = self.sockets[k].token;
        self.peers.iter().position(|p| p.socket_token == socket_token && p.addr == from)
    }

    /// One received datagram.
    #[allow(clippy::too_many_arguments)]
    fn on_datagram<F>(
        &mut self,
        registry: &Registry,
        k: usize,
        bytes: &[u8],
        from: SocketAddr,
        now: Instant,
        dcache: Option<&DCache>,
        tokens: &mut Tokens,
        deliver: &mut F,
    ) where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let Some(datagram) = wire::decode(bytes) else { return };
        let peer = self.peer_index(k, from);
        match datagram {
            Datagram::Packet(packet) => {
                let Some(i) = peer else {
                    // Unknown sender on a listener: a client from before our
                    // restart, or one we dropped. Tell it to renegotiate.
                    if self.sockets[k].listener {
                        Peer::send_reset(&self.sockets[k].socket, from, packet.session);
                    }
                    return;
                };
                let peer = &mut self.peers[i];
                let token = peer.token;
                let group = self.group;
                peer.on_packet(packet, dcache, now, &mut |payload, send_ts| {
                    deliver(Event::Message { group, token, payload, send_ts });
                });
            }
            Datagram::Ack { session, ack_next, bits, bitmap } => {
                let Some(i) = peer else {
                    if self.sockets[k].listener {
                        Peer::send_reset(&self.sockets[k].socket, from, session);
                    }
                    return;
                };
                self.peers[i].on_ack(session, ack_next, bits, bitmap, &mut self.store, now);
            }
            Datagram::Hello { session, base } => {
                if !self.sockets[k].listener {
                    return;
                }
                if let Some(i) = peer {
                    if self.peers[i].on_hello(session, base, &self.sockets[k].socket, now) {
                        return;
                    }
                    let old = self.remove_peer(i);
                    deliver(Event::Disconnected { group: self.group, token: old, peer_addr: from });
                }
                let token = tokens.allocate(self.group);
                let entry = &self.sockets[k];
                let mut peer =
                    Peer::new(from, token, entry.token, new_session(token.0), &self.config);
                peer.on_hello(session, base, &entry.socket, now);
                push_on_connect(&mut self.store, self.greeting.as_deref(), &mut peer, now);
                info!(addr = %from, "udp client connected");
                deliver(Event::Accepted { group: self.group, token, peer_addr: from });
                self.peers.push(peer);
                self.pump(registry, k, now, false);
            }
            Datagram::HelloAck { session, their_session, base } => {
                let Some(i) = peer.filter(|&i| self.peers[i].is_outbound()) else { return };
                let token = self.peers[i].token;
                if self.peers[i].lost_by_remote(their_session, Some(session)) {
                    warn!(addr = %from, "udp peer lost our session, reconnecting");
                    deliver(Event::Disconnected { group: self.group, token, peer_addr: from });
                    self.drop_peer(i, now);
                } else if self.peers[i].on_hello_ack(session, their_session, base, now) {
                    debug!(addr = %from, "udp connected");
                    deliver(Event::Connected { group: self.group, token, peer_addr: from });
                    self.pump(registry, k, now, false);
                }
            }
            Datagram::Reset { their_session } => {
                let Some(i) = peer.filter(|&i| self.peers[i].is_outbound()) else { return };
                if self.peers[i].lost_by_remote(their_session, None) {
                    let token = self.peers[i].token;
                    warn!(addr = %from, "udp peer lost our session, reconnecting");
                    deliver(Event::Disconnected { group: self.group, token, peer_addr: from });
                    self.drop_peer(i, now);
                }
            }
        }
    }

    /// Handles readiness for the socket identified by `event.token()`.
    pub(crate) fn handle_event<F>(
        &mut self,
        registry: &Registry,
        event: &MioEvent,
        tokens: &mut Tokens,
        dcache: Option<&DCache>,
        deliver: &mut F,
    ) where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let Some(k) = self.sockets.iter().position(|s| s.token == event.token()) else { return };
        let now = Instant::now();
        if event.is_readable() {
            let fd = self.sockets[k].socket.as_raw_fd();
            let mut recv = self.recv.take().expect("recv batch in use");
            loop {
                let n = match recv.recv(fd) {
                    Ok(n) => n,
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                    Err(err) => {
                        debug!(?err, "udp recv failed");
                        break;
                    }
                };
                for i in 0..n {
                    let Some((datagrams, from)) = recv.datagrams(i) else { continue };
                    for bytes in datagrams {
                        self.on_datagram(registry, k, bytes, from, now, dcache, tokens, deliver);
                    }
                }
            }
            self.recv = Some(recv);
        }
        if event.is_writable() {
            self.sockets[k].writable_armed = false;
        }
        // Acks just taken in may have exposed holes; packets just queued
        // behind a full buffer may fit now.
        self.pump(registry, k, now, false);
        let entry = &mut self.sockets[k];
        let token = entry.token;
        for peer in self.peers.iter_mut().filter(|p| p.socket_token == token) {
            if peer.take_ack_due() && peer.send_ack(&entry.socket, now) == SendOutcome::WouldBlock {
                arm_writable(registry, entry);
            }
        }
        if event.is_writable() && !entry.writable_armed {
            if let Err(err) =
                registry.reregister(&mut entry.socket, entry.token, Interest::READABLE)
            {
                debug!(?err, "udp: reregister drop writable");
            }
        }
    }

    #[inline]
    fn drain_pending_disconnects<F>(&mut self, deliver: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let had_pending = !self.pending_disconnects.is_empty();
        for (token, peer_addr) in self.pending_disconnects.drain(..) {
            deliver(Event::Disconnected { group: self.group, token, peer_addr });
        }
        had_pending
    }

    pub(crate) fn pre_poll<F>(&mut self, registry: &Registry, deliver: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let work = self.drain_pending_disconnects(deliver);
        let now = Instant::now();
        if now >= self.next_tick {
            self.next_tick = now + self.tick_interval;
            self.tick(registry, now);
        }
        work
    }

    pub(crate) fn post_poll<F>(&mut self, tokens: &mut Tokens, deliver: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        for token in self.retired.drain(..) {
            tokens.retire(token);
        }
        self.drain_pending_disconnects(deliver)
    }

    pub(crate) fn tick_interval(&self) -> Duration {
        self.tick_interval
    }

    /// Whether the peer can take a message now: connected, or an outbound
    /// peer whose group replays, and not closing.
    #[inline]
    fn is_sendable(&self, peer: &Peer) -> bool {
        !peer.is_draining() &&
            (peer.is_connected() ||
                (peer.is_outbound() && self.config.replay == ReplayPolicy::Replay))
    }

    #[inline]
    fn is_broadcast_recipient(&self, peer: &Peer) -> bool {
        self.is_sendable(peer) && !self.broadcast_paused.contains(&peer.token)
    }

    fn sendable_index(&self, token: Token) -> Option<usize> {
        self.peers.iter().position(|p| p.token == token && self.is_sendable(p))
    }

    fn has_broadcast_recipient(&self) -> bool {
        self.peers.iter().any(|p| self.is_broadcast_recipient(p))
    }

    /// Permanently removes a peer, or a listener with every session on it.
    pub(crate) fn remove(&mut self, registry: &Registry, token: Token) -> bool {
        if let Some(i) = self.peers.iter().position(|p| p.token == token) {
            let outbound = self.peers[i].is_outbound();
            self.remove_peer(i);
            self.pending_disconnects.retain(|(t, _)| *t != token);
            if outbound {
                if let Some(k) = socket_of(&self.sockets, token) {
                    let mut endpoint = self.sockets.swap_remove(k);
                    let _ = registry.deregister(&mut endpoint.socket);
                } else {
                    self.unbound -= 1;
                }
            }
            return true;
        }
        self.remove_listener(registry, token)
    }
}
