//! UDP group sockets and reliable session state driven by the shared network
//! poll.
//!
//! One socket per `listen` (shared by every peer that dials it) and one per
//! `connect` (owned by that single peer). Peers hold all reliability state,
//! see [`UdpPeer`]. Message bytes live once in a [`MsgStore`] shared by every
//! peer sending them, and unsent fragments of all peers on a socket go out in
//! one `sendmmsg` batch.

use std::{
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    os::fd::AsRawFd,
};

use flux_communication::Timer;
use flux_timing::{Duration, Instant, Nanos};
use flux_utils::DCache;
use mio::{Interest, Registry, Token, event::Event as MioEvent, net::UdpSocket};
use tracing::{debug, info, warn};

use super::{
    peer::{MsgStore, PushOutcome, SendOutcome, Staged, UdpPeer, send_batch},
    sys::{BATCH, RecvBatch, SendBatch},
    wire::{HEADER_SIZE, Header, Kind},
};
use crate::{
    NetworkTelemetry,
    network::{
        Event, Group, PayloadBuf, ReplayPolicy, RxPayload, Tokens, UdpGroupConfig,
        set_socket_buf_size,
    },
};

struct Endpoint {
    token: Token,
    socket: UdpSocket,
    listener: bool,
    writable_armed: bool,
}

/// One decoded datagram and where it came from.
struct Datagram<'a> {
    header: Header,
    payload: &'a [u8],
    from: SocketAddr,
    now: Instant,
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

fn latency_timer(telemetry: NetworkTelemetry, peer: SocketAddr) -> Option<Timer> {
    let NetworkTelemetry::Enabled { app_name } = telemetry else { return None };
    Some(Timer::new(app_name, format!("udp_latency_{peer}")))
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

/// Queues the on-connect message for a peer whose session was just set up.
fn push_on_connect(store: &mut MsgStore, cfg: &UdpGroupConfig, peer: &mut UdpPeer, now: Instant) {
    if let Some(msg) = &cfg.on_connect_msg {
        let slot = store.insert(&mut msg.clone());
        peer.push_message(store, slot, Nanos::now(), now);
        store.release(slot);
    }
}

/// The socket registered under `token`. An outbound peer whose bind failed
/// has none until the next retry.
#[inline]
fn socket_of(sockets: &[Endpoint], token: Token) -> Option<usize> {
    sockets.iter().position(|s| s.token == token)
}

/// Local address an outbound socket for `peer` binds to.
fn outbound_bind_addr(peer: SocketAddr) -> SocketAddr {
    match peer {
        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
    }
}

pub(crate) struct UdpManager {
    pub(crate) config: UdpGroupConfig,
    group: Group,
    registry: Registry,
    sockets: Vec<Endpoint>,
    peers: Vec<UdpPeer>,
    store: MsgStore,
    send_buffer: Vec<u8>,
    batch: SendBatch,
    staged: [Staged; BATCH],
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
    pub(crate) fn new(config: UdpGroupConfig, registry: Registry, group: Group) -> Self {
        let udp = config.udp;
        udp.validate();
        assert!(
            config
                .on_connect_msg
                .as_ref()
                .is_none_or(|msg| !msg.is_empty() && msg.len() <= udp.max_message_size),
            "invalid UDP on-connect message length"
        );
        Self {
            config,
            group,
            registry,
            sockets: Vec::new(),
            peers: Vec::new(),
            store: MsgStore::new(),
            send_buffer: Vec::with_capacity(32 * 1024),
            batch: SendBatch::new(),
            staged: [Staged { peer: 0, seq: 0 }; BATCH],
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

    pub(crate) fn is_empty(&self) -> bool {
        self.sockets.is_empty()
    }

    /// Opens a socket on `bind` registered under `token` and adds it to the
    /// endpoints.
    fn open_socket(&mut self, bind: SocketAddr, token: Token, listener: bool) -> io::Result<()> {
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
        self.registry.register(&mut socket, token, Interest::READABLE)?;
        self.sockets.push(Endpoint { token, socket, listener, writable_armed: false });
        Ok(())
    }

    /// Registers an outbound peer. If its socket cannot be opened now, the
    /// peer waits unbound and the open is retried like a redial.
    pub(crate) fn connect(&mut self, addr: SocketAddr, tokens: &mut Tokens) -> Token {
        let token = tokens.allocate(self.group);
        let mut peer = UdpPeer::new(
            addr,
            token,
            token,
            new_session(token.0),
            self.config.udp,
            latency_timer(self.config.telemetry, addr),
        );
        let now = Instant::now();
        // First in the queue; nothing goes out before the handshake anyway.
        push_on_connect(&mut self.store, &self.config, &mut peer, now);
        match self.open_socket(outbound_bind_addr(addr), token, false) {
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
    fn bind_unbound(&mut self, now: Instant) {
        for i in 0..self.peers.len() {
            let peer = &self.peers[i];
            if !peer.is_outbound() || socket_of(&self.sockets, peer.token).is_some() {
                continue;
            }
            let (addr, token) = (peer.addr, peer.token);
            match self.open_socket(outbound_bind_addr(addr), token, false) {
                Ok(()) => {
                    self.unbound -= 1;
                    let socket = &self.sockets.last().unwrap().socket;
                    self.peers[i].send_hello(socket, now);
                }
                Err(err) => debug!(?err, %addr, "udp socket open failed again"),
            }
        }
    }

    pub(crate) fn listen(&mut self, addr: SocketAddr, tokens: &mut Tokens) -> io::Result<Token> {
        let token = tokens.allocate(self.group);
        if let Err(err) = self.open_socket(addr, token, true) {
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
            .config
            .on_connect_msg
            .as_deref()
            .is_some_and(|msg| peer.oldest_retained(&self.store) == Some(msg));
        if !greeting_retained {
            push_on_connect(&mut self.store, &self.config, peer, now);
        }
        if let Some(k) = socket_of(&self.sockets, peer.token) {
            peer.send_hello(&self.sockets[k].socket, now);
        }
    }

    /// Removes an accepted peer, returning its store references.
    fn remove_peer(&mut self, index: usize) -> Token {
        let mut peer = self.peers.swap_remove(index);
        peer.release_all(&mut self.store);
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
        self.peers[i].clear_unsent(&mut self.store)
    }

    /// Removes a listener and every session accepted on it.
    fn remove_listener(&mut self, token: Token) -> bool {
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
        let _ = self.registry.deregister(&mut entry.socket);
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

    pub(crate) fn send_with<F>(&mut self, token: Token, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let Some(index) = self.sendable_index(token) else { return false };
        let now = Instant::now();
        let sent = self.stage_serialised(index, Nanos::now(), now, serialise);
        if sent {
            self.flush_peer_socket(index, now);
        }
        sent
    }

    /// Queues every item for `token`, then flushes its socket once.
    pub(crate) fn send_many_with<I, F>(&mut self, token: Token, items: I, mut serialise: F) -> bool
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        let Some(index) = self.sendable_index(token) else { return false };
        let now = Instant::now();
        let ts = Nanos::now();
        let mut sent = false;
        for item in items {
            if self.peers.get(index).is_none_or(|p| p.token != token) {
                // The peer was dropped for violating a limit.
                return false;
            }
            sent |= self.stage_serialised(index, ts, now, |buf| serialise(buf, item));
        }
        if sent {
            self.flush_peer_socket(index, now);
        }
        sent
    }

    pub(crate) fn broadcast_with<F>(&mut self, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        if !self.has_broadcast_recipient() {
            return 0;
        }
        let now = Instant::now();
        let recipients = self.stage_broadcast(Nanos::now(), now, serialise);
        self.flush_all(now);
        recipients
    }

    /// Queues every item for every recipient, then flushes each socket once.
    pub(crate) fn broadcast_many_with<I, F>(&mut self, items: I, mut serialise: F) -> usize
    where
        I: IntoIterator,
        F: FnMut(&mut PayloadBuf<'_>, I::Item),
    {
        if !self.has_broadcast_recipient() {
            return 0;
        }
        let now = Instant::now();
        let ts = Nanos::now();
        let mut recipients = 0;
        for item in items {
            recipients = recipients.max(self.stage_broadcast(ts, now, |buf| serialise(buf, item)));
        }
        self.flush_all(now);
        recipients
    }

    /// Serialises one message into the store. `None` if it is empty or too
    /// large, which the TCP path also skips without sending.
    fn serialise_into_store<F>(&mut self, serialise: F) -> Option<u32>
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        self.send_buffer.clear();
        serialise(&mut PayloadBuf::new(&mut self.send_buffer));
        if self.send_buffer.is_empty() {
            return None;
        }
        if self.send_buffer.len() > self.config.udp.max_message_size {
            warn!(
                group = self.config.name,
                len = self.send_buffer.len(),
                max = self.config.udp.max_message_size,
                "udp message exceeds max_message_size"
            );
            return None;
        }
        Some(self.store.insert(&mut self.send_buffer))
    }

    /// Serialises and queues one message for the peer at `index`, dropping
    /// the peer if it cannot take it. Returns whether it was queued.
    fn stage_serialised<F>(&mut self, index: usize, ts: Nanos, now: Instant, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let Some(slot) = self.serialise_into_store(serialise) else { return false };
        let queued = self.stage_message(index, slot, ts, now);
        self.store.release(slot);
        if !queued {
            self.drop_peer_pending(index, now);
        }
        queued
    }

    /// Serialises and queues one message for every eligible peer. Returns the
    /// number of recipients.
    fn stage_broadcast<F>(&mut self, ts: Nanos, now: Instant, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let Some(slot) = self.serialise_into_store(serialise) else { return 0 };
        let mut recipients = 0;
        let mut i = self.peers.len();
        while i != 0 {
            i -= 1;
            if !self.is_broadcast_recipient(&self.peers[i]) {
                continue;
            }
            recipients += 1;
            if !self.stage_message(i, slot, ts, now) {
                self.drop_peer_pending(i, now);
            }
        }
        self.store.release(slot);
        recipients
    }

    fn flush_peer_socket(&mut self, index: usize, now: Instant) {
        if let Some(k) = socket_of(&self.sockets, self.peers[index].socket_token) {
            self.flush_socket(k, now);
        }
    }

    fn flush_all(&mut self, now: Instant) {
        for k in 0..self.sockets.len() {
            self.flush_socket(k, now);
        }
    }

    /// Queues the stored message for one peer. `false` when the peer must be
    /// dropped: it violated the backlog limit or cannot hold the message.
    #[inline]
    fn stage_message(&mut self, index: usize, slot: u32, ts: Nanos, now: Instant) -> bool {
        let peer = &mut self.peers[index];
        match peer.push_message(&mut self.store, slot, ts, now) {
            PushOutcome::Queued => !peer.backlog_exceeded(self.config.max_backlog_datagrams),
            PushOutcome::WindowFull => false,
            PushOutcome::TooLarge => true,
        }
    }

    /// Sends every unsent fragment of every peer on socket `k`, batched across
    /// peers. Arms WRITABLE if the kernel stopped accepting.
    fn flush_socket(&mut self, k: usize, now: Instant) {
        let entry = &mut self.sockets[k];
        let fd = entry.socket.as_raw_fd();
        let mut n = 0;
        for i in 0..self.peers.len() {
            if self.peers[i].socket_token != entry.token {
                continue;
            }
            for seq in self.peers[i].unsent() {
                self.peers[i].stage(seq, &self.store, &mut self.batch);
                self.staged[n] = Staged { peer: i, seq };
                n += 1;
                if n == BATCH {
                    if !Self::dispatch(
                        &mut self.batch,
                        &self.staged,
                        &mut self.peers,
                        &mut self.store,
                        fd,
                        n,
                        now,
                    ) {
                        arm_writable(&self.registry, entry);
                        return;
                    }
                    n = 0;
                }
            }
        }
        if n != 0 &&
            !Self::dispatch(
                &mut self.batch,
                &self.staged,
                &mut self.peers,
                &mut self.store,
                fd,
                n,
                now,
            )
        {
            arm_writable(&self.registry, entry);
        }
    }

    /// Sends the staged batch and marks the accepted prefix. `false` if the
    /// kernel took less than everything.
    fn dispatch(
        batch: &mut SendBatch,
        staged: &[Staged; BATCH],
        peers: &mut [UdpPeer],
        store: &mut MsgStore,
        fd: i32,
        n: usize,
        now: Instant,
    ) -> bool {
        let accepted = send_batch(batch, fd, n);
        for s in &staged[..accepted] {
            peers[s.peer].mark_sent(s.seq, store, now);
        }
        accepted == n
    }

    pub(crate) fn currently_disconnected(&self) -> impl Iterator<Item = Token> {
        self.peers.iter().filter(|p| p.is_outbound() && !p.is_connected()).map(|p| p.token)
    }

    pub(crate) fn force_reconnect(&mut self) {
        let now = Instant::now();
        if self.unbound != 0 {
            self.bind_unbound(now);
        }
        for peer in self.peers.iter_mut().filter(|p| p.is_outbound() && !p.is_connected()) {
            if let Some(k) = socket_of(&self.sockets, peer.token) {
                peer.send_hello(&self.sockets[k].socket, now);
            }
        }
    }

    /// Hello retries, retransmits, heartbeats, peer timeouts, drained
    /// closes and socket-open retries.
    fn tick(&mut self, now: Instant) {
        if self.unbound != 0 && now >= self.next_bind {
            self.next_bind = now + self.config.udp.heartbeat_interval;
            self.bind_unbound(now);
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
            match peer.tick(&self.store, &entry.socket, &mut self.batch, now, peer_timeout) {
                Ok(SendOutcome::Done) => {}
                Ok(SendOutcome::WouldBlock) => arm_writable(&self.registry, entry),
                Err(()) => {
                    warn!(addr = %peer.addr, "udp peer timed out");
                    self.drop_peer_pending(i, now);
                }
            }
        }
    }

    /// Creates the accepted peer for a hello from `from`.
    fn accept<F>(&mut self, k: usize, dgram: &Datagram<'_>, tokens: &mut Tokens, deliver: &mut F)
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let token = tokens.allocate(self.group);
        let entry = &self.sockets[k];
        let mut peer = UdpPeer::new(
            dgram.from,
            token,
            entry.token,
            new_session(token.0),
            self.config.udp,
            latency_timer(self.config.telemetry, dgram.from),
        );
        peer.on_hello(&dgram.header, &entry.socket, dgram.now);
        push_on_connect(&mut self.store, &self.config, &mut peer, dgram.now);
        info!(addr = %dgram.from, "udp client connected");
        deliver(Event::Accepted { group: self.group, token, peer_addr: dgram.from });
        self.peers.push(peer);
        self.flush_socket(k, dgram.now);
    }

    /// One received datagram.
    fn on_datagram<F>(
        &mut self,
        k: usize,
        dgram: &Datagram<'_>,
        dcache: Option<&DCache>,
        tokens: &mut Tokens,
        deliver: &mut F,
    ) where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let Datagram { header, payload, from, now } = *dgram;
        let header = &header;
        let socket_token = self.sockets[k].token;
        let peer_index =
            self.peers.iter().position(|p| p.socket_token == socket_token && p.addr == from);

        match header.kind {
            Kind::Hello => {
                if !self.sockets[k].listener {
                    return;
                }
                if let Some(i) = peer_index {
                    if self.peers[i].on_hello(header, &self.sockets[k].socket, now) {
                        return;
                    }
                    let old = self.remove_peer(i);
                    deliver(Event::Disconnected { group: self.group, token: old, peer_addr: from });
                }
                self.accept(k, dgram, tokens, deliver);
            }
            Kind::HelloAck => {
                let Some(i) = peer_index else { return };
                let peer = &mut self.peers[i];
                if !peer.is_outbound() {
                    return;
                }
                let Some(_) = peer.on_hello_ack(header, now) else { return };
                let token = peer.token;
                debug!(addr = %from, "udp connected");
                deliver(Event::Connected { group: self.group, token, peer_addr: from });
                self.flush_socket(k, now);
            }
            Kind::Reset => {
                let Some(i) = peer_index else { return };
                let peer = &self.peers[i];
                if peer.is_outbound() && peer.on_reset(header) {
                    warn!(addr = %from, "udp peer reset us, reconnecting");
                    let token = peer.token;
                    deliver(Event::Disconnected { group: self.group, token, peer_addr: from });
                    self.drop_peer(i, now);
                }
            }
            Kind::Data | Kind::Ack => {
                let entry = &mut self.sockets[k];
                let Some(i) = peer_index else {
                    // Unknown sender on a listener: a client from before our
                    // restart, or one we dropped. Tell it to renegotiate.
                    if entry.listener {
                        UdpPeer::send_reset(&entry.socket, from, header.session);
                    }
                    return;
                };
                let peer = &mut self.peers[i];
                let token = peer.token;
                if header.kind == Kind::Data {
                    peer.on_data(header, payload, dcache, now, &mut |payload, send_ts| {
                        deliver(Event::Message { group: self.group, token, payload, send_ts });
                    });
                } else if peer.on_ack(
                    header,
                    payload,
                    &mut self.store,
                    &entry.socket,
                    &mut self.batch,
                    now,
                ) == SendOutcome::WouldBlock
                {
                    arm_writable(&self.registry, entry);
                }
            }
        }
    }

    /// Handles readiness for the socket identified by `event.token()`.
    pub(crate) fn handle_event<F>(
        &mut self,
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
                        let Some(header) = Header::decode(bytes) else { continue };
                        let dgram = Datagram { header, payload: &bytes[HEADER_SIZE..], from, now };
                        self.on_datagram(k, &dgram, dcache, tokens, deliver);
                    }
                }
            }
            self.recv = Some(recv);
        }

        if event.is_writable() {
            self.sockets[k].writable_armed = false;
            self.flush_socket(k, now);
        }
        let entry = &mut self.sockets[k];
        let token = entry.token;
        for peer in self.peers.iter_mut().filter(|p| p.socket_token == token) {
            if peer.take_ack_due() && peer.send_ack(&entry.socket, now) == SendOutcome::WouldBlock {
                arm_writable(&self.registry, entry);
            }
        }
        if event.is_writable() && !entry.writable_armed {
            if let Err(err) =
                self.registry.reregister(&mut entry.socket, entry.token, Interest::READABLE)
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

    pub(crate) fn pre_poll<F>(&mut self, deliver: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let work = self.drain_pending_disconnects(deliver);
        let now = Instant::now();
        if now >= self.next_tick {
            self.next_tick = now + self.tick_interval;
            self.tick(now);
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
    fn is_sendable(&self, peer: &UdpPeer) -> bool {
        !peer.is_draining() &&
            (peer.is_connected() ||
                (peer.is_outbound() && self.config.replay == ReplayPolicy::Replay))
    }
    #[inline]
    fn is_broadcast_recipient(&self, peer: &UdpPeer) -> bool {
        self.is_sendable(peer) && !self.broadcast_paused.contains(&peer.token)
    }
    fn sendable_index(&self, token: Token) -> Option<usize> {
        self.peers.iter().position(|p| p.token == token && self.is_sendable(p))
    }
    fn has_broadcast_recipient(&self) -> bool {
        self.peers.iter().any(|p| self.is_broadcast_recipient(p))
    }
    /// Permanently removes a peer, or a listener with every session on it.
    pub(crate) fn remove(&mut self, token: Token) -> bool {
        if let Some(i) = self.peers.iter().position(|p| p.token == token) {
            let outbound = self.peers[i].is_outbound();
            self.remove_peer(i);
            self.pending_disconnects.retain(|(t, _)| *t != token);
            if outbound {
                if let Some(k) = socket_of(&self.sockets, token) {
                    let mut endpoint = self.sockets.swap_remove(k);
                    let _ = self.registry.deregister(&mut endpoint.socket);
                } else {
                    self.unbound -= 1;
                }
            }
            return true;
        }
        self.remove_listener(token)
    }
}
impl Drop for UdpManager {
    fn drop(&mut self) {
        for endpoint in &mut self.sockets {
            let _ = self.registry.deregister(&mut endpoint.socket);
        }
    }
}
