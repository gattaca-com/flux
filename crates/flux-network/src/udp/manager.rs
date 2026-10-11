use std::{
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    ops::{Deref, DerefMut},
    os::fd::AsRawFd,
};

use flux_timing::{Duration, Instant, Nanos};
use flux_utils::{DCache, safe_assert};
use mio::{Interest, Registry, Token, event::Event as MioEvent, net::UdpSocket};
use tracing::{debug, error, info, warn};

use super::{
    outbound::MessageStore,
    peer::{Peer, SendOutcome, deliver_message},
    sys::{RecvBatch, SendBatch},
    wire::{self, Datagram, HELLO_ACK_SIZE, MESSAGE_HEADER},
};
use crate::network::{
    Event, Group, PayloadBuf, RxPayload, Tokens, UdpGroupConfig, set_socket_buf_size,
};

struct Endpoint {
    token: Token,
    socket: UdpSocket,
    listener: bool,
    writable_armed: bool,
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

#[inline]
fn on_connect_msg(config: &UdpGroupConfig) -> &[u8] {
    config.on_connect_msg.as_deref().unwrap_or_default()
}

fn outbound_bind_addr(peer: SocketAddr) -> SocketAddr {
    match peer {
        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
    }
}

struct StagedPeerBatch {
    peer_idx: usize,
    packets: usize,
}

pub(crate) struct UdpManager {
    // separate from `state` to avoid borrow-checker issues in handle_event
    recv: RecvBatch,
    state: UdpManagerState,
}

impl Deref for UdpManager {
    type Target = UdpManagerState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for UdpManager {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl UdpManager {
    pub(crate) fn new(config: UdpGroupConfig, group: Group) -> Self {
        Self {
            recv: RecvBatch::new(config.udp.max_datagram_size),
            state: UdpManagerState::new(config, group),
        }
    }

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
        self.state.handle_event(&mut self.recv, registry, event, tokens, dcache, deliver);
    }
}

pub(crate) struct UdpManagerState {
    config: UdpGroupConfig,
    group: Group,

    sockets: Vec<Endpoint>,
    peers: Vec<Peer>,

    store: MessageStore,
    /// users serialise here. and then it's moved to `MessageStore`
    send_buffer: Vec<u8>,

    batch: SendBatch,
    // entries here correspond to `SendBatch` order
    staged_peer_batches: Vec<StagedPeerBatch>,

    pending_disconnect_notifications: Vec<(Token, SocketAddr)>,
    // to avoid passing &mut Tokens everywhere
    retired: Vec<Token>,

    next_tick: Instant,
}

impl UdpManagerState {
    fn new(config: UdpGroupConfig, group: Group) -> Self {
        let udp = config.udp;
        udp.validate();
        if let Some(msg) = &config.on_connect_msg {
            assert!(!msg.is_empty(), "udp on_connect_msg must be nonempty");
            assert!(
                msg.len() <= udp.max_message_size,
                "udp on_connect_msg exceeds max_message_size"
            );
            assert!(
                HELLO_ACK_SIZE + msg.len() <= udp.max_datagram_size,
                "udp on_connect_msg does not fit in a hello ack of max_datagram_size"
            );
        }
        Self {
            config,
            group,
            sockets: Vec::new(),
            peers: Vec::new(),
            store: MessageStore::new(),
            send_buffer: Vec::with_capacity(32 * 1024),
            batch: SendBatch::new(udp.max_datagram_size),
            staged_peer_batches: Vec::new(),
            pending_disconnect_notifications: Vec::new(),
            retired: Vec::new(),
            next_tick: Instant::ZERO,
        }
    }

    /// Deregisters every socket before the group is dropped.
    pub(crate) fn close_all(&mut self, registry: &Registry) {
        for endpoint in &mut self.sockets {
            let _ = registry.deregister(&mut endpoint.socket);
        }
    }

    fn open_socket(
        &mut self,
        registry: &Registry,
        bind: SocketAddr,
        token: Token,
    ) -> io::Result<UdpSocket> {
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
        Ok(socket)
    }

    pub(crate) fn connect(
        &mut self,
        registry: &Registry,
        addr: SocketAddr,
        tokens: &mut Tokens,
    ) -> Token {
        let token = tokens.allocate(self.group);
        let socket = match self.open_socket(registry, outbound_bind_addr(addr), token) {
            Ok(socket) => socket,
            Err(err) => {
                error!(?err, %addr, ?token, "couldn't open udp socket");
                tokens.retire(token);
                return token;
            }
        };
        let mut peer = Peer::new(addr, token, token, &self.config);
        let now = Instant::now();
        peer.send_hello(&socket, on_connect_msg(&self.config), now);
        self.sockets.push(Endpoint { token, socket, listener: false, writable_armed: false });
        self.peers.push(peer);
        token
    }

    pub(crate) fn listen(
        &mut self,
        registry: &Registry,
        addr: SocketAddr,
        tokens: &mut Tokens,
    ) -> io::Result<Token> {
        let token = tokens.allocate(self.group);
        let socket = match self.open_socket(registry, addr, token) {
            Ok(socket) => socket,
            Err(err) => {
                tokens.retire(token);
                return Err(err);
            }
        };
        self.sockets.push(Endpoint { token, socket, listener: true, writable_armed: false });
        Ok(token)
    }

    fn reconnect_outbound(&mut self, index: usize, now: Instant) {
        let peer = &mut self.peers[index];
        if peer.is_connected() {
            self.pending_disconnect_notifications.push((peer.token, peer.addr));
        }
        peer.reset_connection(&mut self.store);
        let endpoint = self
            .sockets
            .iter()
            .find(|s| s.token == peer.socket_token)
            .expect("outbound peer must have a socket");
        peer.send_hello(&endpoint.socket, on_connect_msg(&self.config), now);
    }

    fn remove_peer(&mut self, index: usize) -> Token {
        let mut peer = self.peers.swap_remove(index);
        peer.outbound.release_all(&mut self.store);
        self.retired.push(peer.token);
        peer.token
    }

    fn disconnect_accepted(&mut self, index: usize) {
        let peer = &self.peers[index];
        self.pending_disconnect_notifications.push((peer.token, peer.addr));
        self.remove_peer(index);
    }

    /// Drops a peer's session: accepted peers go, outbound ones redial. A
    /// listener token is not a session and is left alone.
    pub(crate) fn disconnect(&mut self, token: Token) -> bool {
        let Some(i) = self.peers.iter().position(|p| p.token == token) else { return false };
        if self.peers[i].is_outbound() {
            self.reconnect_outbound(i, Instant::now());
        } else {
            self.disconnect_accepted(i);
        }
        true
    }

    /// Closes the session once everything queued for it is acked. Sends to
    /// it are refused meanwhile.
    pub(crate) fn disconnect_when_drained(&mut self, token: Token) -> bool {
        let Some(i) = self.peers.iter().position(|p| p.token == token) else { return false };
        if self.peers[i].close_when_drained() {
            if self.peers[i].is_outbound() {
                self.reconnect_outbound(i, Instant::now());
            } else {
                self.disconnect_accepted(i);
            }
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
                self.disconnect_accepted(i);
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
                self.reconnect_outbound(i, now);
            }
        }
    }

    pub(crate) fn send_with<F>(&mut self, registry: &Registry, token: Token, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let Some(index) = self.sendable_index(token) else { return false };
        let now = Instant::now();
        if !self.append_record(serialise) {
            return false;
        }
        let sent = self.queue_stream_for(index, now);
        if sent {
            self.flush_peer_socket(registry, index, now);
        }
        sent
    }

    /// Serialises every item into one stream for `token`, queues it and
    /// flushes the socket once.
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
        let mut appended = false;
        for item in items {
            appended |= self.append_record(|buf| serialise(buf, item));
        }
        if !appended {
            return false;
        }
        let sent = self.queue_stream_for(index, now);
        if sent {
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
        if !self.append_record(serialise) {
            return 0;
        }
        let recipients = self.queue_stream_broadcast(now);
        self.flush_all(registry, now);
        recipients
    }

    /// Serialises every item into one stream for every recipient, queues it
    /// and flushes each socket once.
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
        let mut appended = false;
        for item in items {
            appended |= self.append_record(|buf| serialise(buf, item));
        }
        if !appended {
            return 0;
        }
        let recipients = self.queue_stream_broadcast(now);
        self.flush_all(registry, now);
        recipients
    }

    fn append_record<F>(&mut self, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let start = self.send_buffer.len();
        self.send_buffer.extend_from_slice(&[0; MESSAGE_HEADER]);
        serialise(&mut PayloadBuf::new(&mut self.send_buffer));
        let len = self.send_buffer.len() - start - MESSAGE_HEADER;
        if len == 0 || len > self.config.udp.max_message_size {
            if len != 0 {
                // TODO: Emit telemetry.
                error!(
                    group = self.config.name,
                    len,
                    max = self.config.udp.max_message_size,
                    "udp message exceeds max_message_size"
                );
            }
            self.send_buffer.truncate(start);
            return false;
        }
        self.send_buffer[start..start + MESSAGE_HEADER]
            .copy_from_slice(&(len as u32).to_le_bytes());
        true
    }

    fn queue_stream_for(&mut self, index: usize, now: Instant) -> bool {
        safe_assert!(!self.send_buffer.is_empty());
        let slot = self.store.insert(&mut self.send_buffer);
        let queued = self.peers[index].outbound.enqueue(&mut self.store, slot);
        if !queued {
            if self.peers[index].is_outbound() {
                self.reconnect_outbound(index, now);
            } else {
                self.disconnect_accepted(index);
            }
        }
        self.store.release(slot);
        queued
    }

    /// Returns the number of recipients.
    fn queue_stream_broadcast(&mut self, now: Instant) -> usize {
        safe_assert!(!self.send_buffer.is_empty());
        let slot = self.store.insert(&mut self.send_buffer);
        let mut recipients = 0;
        // iterating in the reverse order not to skip anything because of swap_remove
        for i in (0..self.peers.len()).rev() {
            recipients += 1;
            if !self.peers[i].outbound.enqueue(&mut self.store, slot) {
                if self.peers[i].is_outbound() {
                    self.reconnect_outbound(i, now);
                } else {
                    self.disconnect_accepted(i);
                }
            }
        }
        self.store.release(slot);
        recipients
    }

    fn flush_peer_socket(&mut self, registry: &Registry, index: usize, now: Instant) {
        let k = self
            .sockets
            .iter()
            .position(|s| s.token == self.peers[index].socket_token)
            .expect("peer must have a socket");
        self.pump(registry, k, now, false);
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
                let packets = self.peers[i].stage(&self.store, &mut self.batch, probes, now);
                probes = 0;
                if packets != 0 {
                    self.staged_peer_batches.push(StagedPeerBatch { peer_idx: i, packets });
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
        let total_packets: usize =
            self.staged_peer_batches.iter().map(|staged| staged.packets).sum();
        let accepted_packets = match self.batch.send(fd) {
            Ok(k) => k,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
            // Other errors count as sent so the retransmit path retries.
            Err(e) => {
                debug!(?e, "udp send failed");
                total_packets
            }
        };
        let mut remaining_packets = accepted_packets;
        for staged in &self.staged_peer_batches {
            let take = remaining_packets.min(staged.packets);
            remaining_packets -= take;
            self.peers[staged.peer_idx].commit(&mut self.store, take, now);
        }
        self.staged_peer_batches.clear();
        accepted_packets == total_packets
    }

    pub(crate) fn currently_disconnected(&self) -> impl Iterator<Item = Token> {
        self.peers.iter().filter(|p| p.is_outbound() && !p.is_connected()).map(|p| p.token)
    }

    pub(crate) fn force_reconnect(&mut self) {
        let now = Instant::now();
        for peer in self.peers.iter_mut().filter(|p| p.is_outbound() && !p.is_connected()) {
            let endpoint = self
                .sockets
                .iter()
                .find(|s| s.token == peer.socket_token)
                .expect("outbound peer must have a socket");
            peer.send_hello(&endpoint.socket, on_connect_msg(&self.config), now);
        }
    }

    /// Hello retries, heartbeats, peer timeouts, drained closes and retransmit
    /// probes.
    fn tick(&mut self, registry: &Registry, now: Instant) {
        let peer_timeout = self.config.peer_timeout;
        let mut i = self.peers.len();
        while i != 0 {
            i -= 1;
            let peer = &mut self.peers[i];
            if peer.drained() {
                if peer.is_outbound() {
                    self.reconnect_outbound(i, now);
                } else {
                    self.disconnect_accepted(i);
                }
                continue;
            }
            let entry = self
                .sockets
                .iter_mut()
                .find(|s| s.token == peer.socket_token)
                .expect("peer must have a socket");
            match peer.tick(&entry.socket, on_connect_msg(&self.config), now, peer_timeout) {
                Ok(SendOutcome::Done) => {}
                Ok(SendOutcome::WouldBlock) => arm_writable(registry, entry),
                Err(()) => {
                    warn!(addr = %peer.addr, "udp peer timed out");
                    if peer.is_outbound() {
                        self.reconnect_outbound(i, now);
                    } else {
                        self.disconnect_accepted(i);
                    }
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
            Datagram::Hello { session, base, msg } => {
                if !self.sockets[k].listener || !self.on_connect_fits(msg, from) {
                    return;
                }
                if let Some(i) = peer {
                    let (socket, ours) = (&self.sockets[k].socket, on_connect_msg(&self.config));
                    if self.peers[i].on_hello(session, base, socket, ours, now) {
                        return;
                    }
                    let old = self.remove_peer(i);
                    deliver(Event::Disconnected { group: self.group, token: old, peer_addr: from });
                }
                let token = tokens.allocate(self.group);
                let entry = &self.sockets[k];
                let mut peer = Peer::new(from, token, entry.token, &self.config);
                peer.on_hello(session, base, &entry.socket, on_connect_msg(&self.config), now);
                info!(addr = %from, "udp client connected");
                deliver(Event::Accepted { group: self.group, token, peer_addr: from });
                self.deliver_on_connect(token, msg, dcache, deliver);
                self.peers.push(peer);
                self.pump(registry, k, now, false);
            }
            Datagram::HelloAck { session, their_session, base, msg } => {
                let Some(i) = peer.filter(|&i| self.peers[i].is_outbound()) else { return };
                if !self.on_connect_fits(msg, from) {
                    return;
                }
                let token = self.peers[i].token;
                if self.peers[i].lost_by_remote(their_session, Some(session)) {
                    warn!(addr = %from, "udp peer lost our session, reconnecting");
                    self.reconnect_outbound(i, now);
                } else if self.peers[i].on_hello_ack(session, their_session, base, now) {
                    // A reconnect can complete before post_poll drains the old disconnect.
                    self.drain_pending_disconnects(deliver);
                    debug!(addr = %from, "udp connected");
                    deliver(Event::Connected { group: self.group, token, peer_addr: from });
                    self.deliver_on_connect(token, msg, dcache, deliver);
                    self.pump(registry, k, now, false);
                }
            }
            Datagram::Reset { their_session } => {
                let Some(i) = peer.filter(|&i| self.peers[i].is_outbound()) else { return };
                if self.peers[i].lost_by_remote(their_session, None) {
                    warn!(addr = %from, "udp peer lost our session, reconnecting");
                    self.reconnect_outbound(i, now);
                }
            }
        }
    }

    fn handle_event<F>(
        &mut self,
        recv: &mut RecvBatch,
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

    pub(crate) fn tick_interval(&self) -> Duration {
        self.config.udp.min_rto / 2
    }

    /// Whether a received on-connect message is one we take; a hello that
    /// carries a larger one is ignored, so the handshake never completes.
    fn on_connect_fits(&self, msg: &[u8], from: SocketAddr) -> bool {
        let fits = msg.len() <= self.config.udp.max_message_size;
        if !fits {
            warn!(addr = %from, len = msg.len(), "udp on-connect message exceeds max_message_size");
        }
        fits
    }

    /// Delivers the remote's on-connect message, if it sent one.
    fn deliver_on_connect<F>(
        &self,
        token: Token,
        msg: &[u8],
        dcache: Option<&DCache>,
        deliver: &mut F,
    ) where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        if msg.is_empty() {
            return;
        }
        let group = self.group;
        deliver_message(&mut None, msg, Nanos::now(), dcache, &mut |payload, send_ts| {
            deliver(Event::Message { group, token, payload, send_ts });
        });
    }

    #[inline]
    fn drain_pending_disconnects<F>(&mut self, deliver: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let had_pending = !self.pending_disconnect_notifications.is_empty();
        for (token, peer_addr) in self.pending_disconnect_notifications.drain(..) {
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
            self.next_tick = now + self.tick_interval();
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

    #[inline]
    fn is_sendable(peer: &Peer) -> bool {
        peer.is_connected() && !peer.is_draining()
    }

    fn sendable_index(&self, token: Token) -> Option<usize> {
        self.peers.iter().position(|p| p.token == token && Self::is_sendable(p))
    }

    fn has_broadcast_recipient(&self) -> bool {
        self.peers.iter().any(Self::is_sendable)
    }

    /// Permanently removes a peer, or a listener with every session on it.
    pub(crate) fn remove(&mut self, registry: &Registry, token: Token) -> bool {
        if let Some(i) = self.peers.iter().position(|p| p.token == token) {
            let outbound = self.peers[i].is_outbound();
            self.remove_peer(i);
            self.pending_disconnect_notifications.retain(|(t, _)| *t != token);
            if outbound {
                let k = self
                    .sockets
                    .iter()
                    .position(|s| s.token == token)
                    .expect("outbound peer must have a socket");
                let mut endpoint = self.sockets.swap_remove(k);
                let _ = registry.deregister(&mut endpoint.socket);
            }
            return true;
        }
        self.remove_listener(registry, token)
    }
}
