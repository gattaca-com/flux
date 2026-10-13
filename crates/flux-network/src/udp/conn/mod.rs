//! Two-way UDP connections for a [`crate::Network`] group. Each side sends
//! its own stream and acks the other's; either side times out a silent
//! peer. A lost connection drops what was queued on it: an initiator
//! reconnects under a new session, an acceptor forgets it.

use std::{
    io,
    net::SocketAddr,
    ops::{Deref, DerefMut},
};

use flux_timing::{Duration, Instant, Nanos};
use flux_utils::DCache;
use mio::{Registry, Token, event::Event as MioEvent};
use tracing::{debug, error, info, warn};

mod peer;
mod wire;

use peer::{Peer, Role, send_reset};
use wire::{Datagram, HELLO_ACK_SIZE};

use super::{
    SendOutcome, Socket, deliver_message, new_session, on_connect_fits, recv_all,
    sys::{RecvBatch, SendBatch},
    unspecified, validate_group,
    wire::MESSAGE_HEADER,
};
use crate::network::{Event, Group, PayloadBuf, RxPayload, Tokens, UdpGroupConfig};

struct Endpoint {
    socket: Socket,
    listener: bool,
}

struct Staged {
    peer: usize,
    packets: usize,
}

fn endpoint(endpoints: &[Endpoint], token: Token) -> usize {
    endpoints.iter().position(|e| e.socket.token == token).expect("peer must have a socket")
}

pub(crate) struct UdpManager {
    // separate from `state` to avoid borrow-checker issues in handle_event
    recv: RecvBatch,
    state: State,
}

impl Deref for UdpManager {
    type Target = State;

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
            state: State::new(config, group),
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

pub(crate) struct State {
    config: UdpGroupConfig,
    group: Group,
    endpoints: Vec<Endpoint>,
    peers: Vec<Peer>,
    /// A late hello retry for a dropped connection gets a reset instead of
    /// being accepted again.
    dropped: Vec<(SocketAddr, u32, Instant)>,
    /// Records being serialised.
    send_buffer: Vec<u8>,
    /// Records being enqueued; `Stream::enqueue` takes its buffer.
    scratch: Vec<u8>,
    ctrl: Vec<u8>,
    batch: SendBatch,
    // entries here correspond to `SendBatch` order
    staged: Vec<Staged>,
    pending_disconnects: Vec<(Token, SocketAddr)>,
    // to avoid passing &mut Tokens everywhere
    retired: Vec<Token>,
    next_tick: Instant,
}

impl State {
    fn new(config: UdpGroupConfig, group: Group) -> Self {
        validate_group(&config, HELLO_ACK_SIZE);
        let udp = config.udp;
        Self {
            ctrl: vec![0; udp.max_datagram_size],
            batch: SendBatch::new(udp.max_datagram_size),
            config,
            group,
            endpoints: Vec::new(),
            peers: Vec::new(),
            dropped: Vec::new(),
            send_buffer: Vec::with_capacity(32 * 1024),
            scratch: Vec::new(),
            staged: Vec::new(),
            pending_disconnects: Vec::new(),
            retired: Vec::new(),
            next_tick: Instant::ZERO,
        }
    }

    pub(crate) fn close_all(&mut self, registry: &Registry) {
        for endpoint in &mut self.endpoints {
            endpoint.socket.close(registry);
        }
    }

    pub(crate) fn connect(
        &mut self,
        registry: &Registry,
        addr: SocketAddr,
        tokens: &mut Tokens,
    ) -> Token {
        let token = tokens.allocate(self.group);
        let socket = match Socket::open(
            registry,
            unspecified(addr),
            token,
            Some(&mut self.batch),
            self.config.socket_buf_size,
        ) {
            Ok(socket) => socket,
            Err(err) => {
                error!(?err, %addr, ?token, "couldn't open udp socket");
                tokens.retire(token);
                return token;
            }
        };
        let now = Instant::now();
        let mut peer =
            Peer::new(addr, token, token, Role::Initiator, new_session(token.0), &self.config, now);
        let msg = self.config.on_connect_msg.as_deref().unwrap_or_default();
        peer.send_hello(&socket.udp, &mut self.ctrl, msg, now);
        self.endpoints.push(Endpoint { socket, listener: false });
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
        match Socket::open(
            registry,
            addr,
            token,
            Some(&mut self.batch),
            self.config.socket_buf_size,
        ) {
            Ok(socket) => {
                self.endpoints.push(Endpoint { socket, listener: true });
                Ok(token)
            }
            Err(err) => {
                tokens.retire(token);
                Err(err)
            }
        }
    }

    fn remove_acceptor(&mut self, i: usize, now: Instant) -> Token {
        let peer = self.peers.swap_remove(i);
        self.dropped.push((peer.addr, peer.remote_session, now));
        self.retired.push(peer.token);
        peer.token
    }

    /// An initiator reconnects; an acceptor is dropped.
    fn lose(&mut self, i: usize, now: Instant) {
        let peer = &mut self.peers[i];
        if peer.connected {
            self.pending_disconnects.push((peer.token, peer.addr));
        }
        match peer.role {
            Role::Acceptor => {
                self.remove_acceptor(i, now);
            }
            Role::Initiator => {
                peer.restart();
                let socket = &self.endpoints[endpoint(&self.endpoints, peer.socket)].socket;
                let msg = self.config.on_connect_msg.as_deref().unwrap_or_default();
                peer.send_hello(&socket.udp, &mut self.ctrl, msg, now);
            }
        }
    }

    pub(crate) fn disconnect(&mut self, token: Token) -> bool {
        let Some(i) = self.peers.iter().position(|p| p.token == token) else { return false };
        self.lose(i, Instant::now());
        true
    }

    pub(crate) fn disconnect_when_drained(&mut self, token: Token) -> bool {
        let Some(i) = self.peers.iter().position(|p| p.token == token && p.connected) else {
            return false;
        };
        let peer = &mut self.peers[i];
        peer.close_at = Some(peer.stream.next());
        if peer.drained() {
            self.lose(i, Instant::now());
        }
        true
    }

    pub(crate) fn remove(&mut self, registry: &Registry, token: Token) -> bool {
        let now = Instant::now();
        if let Some(i) = self.peers.iter().position(|p| p.token == token) {
            self.pending_disconnects.retain(|(t, _)| *t != token);
            if self.peers[i].role == Role::Acceptor {
                self.remove_acceptor(i, now);
                return true;
            }
            self.peers.swap_remove(i);
        } else {
            if !self.endpoints.iter().any(|e| e.socket.token == token && e.listener) {
                return false;
            }
            let mut i = self.peers.len();
            while i != 0 {
                i -= 1;
                if self.peers[i].socket == token {
                    self.lose(i, now);
                }
            }
        }
        let k = endpoint(&self.endpoints, token);
        self.endpoints.swap_remove(k).socket.close(registry);
        self.retired.push(token);
        true
    }

    pub(crate) fn currently_disconnected(&self) -> impl Iterator<Item = Token> {
        self.peers.iter().filter(|p| p.role == Role::Initiator && !p.connected).map(|p| p.token)
    }

    pub(crate) fn force_reconnect(&mut self) {
        let now = Instant::now();
        let msg = self.config.on_connect_msg.as_deref().unwrap_or_default();
        for peer in self.peers.iter_mut().filter(|p| p.role == Role::Initiator && !p.connected) {
            let socket = &self.endpoints[endpoint(&self.endpoints, peer.socket)].socket;
            peer.send_hello(&socket.udp, &mut self.ctrl, msg, now);
        }
    }

    pub(crate) fn disconnect_outbound(&mut self) {
        let now = Instant::now();
        for i in 0..self.peers.len() {
            if self.peers[i].role == Role::Initiator {
                self.lose(i, now);
            }
        }
    }

    fn sendable(&self, token: Token) -> Option<usize> {
        self.peers.iter().position(|p| p.token == token && p.connected && p.close_at.is_none())
    }

    pub(crate) fn send_with<F>(&mut self, registry: &Registry, token: Token, serialise: F) -> bool
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        let Some(i) = self.sendable(token) else { return false };
        if !self.append_record(serialise) {
            return false;
        }
        self.send_buffered(registry, i)
    }

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
        let Some(i) = self.sendable(token) else { return false };
        let mut appended = false;
        for item in items {
            appended |= self.append_record(|buf| serialise(buf, item));
        }
        appended && self.send_buffered(registry, i)
    }

    fn send_buffered(&mut self, registry: &Registry, i: usize) -> bool {
        std::mem::swap(&mut self.send_buffer, &mut self.scratch);
        let now = Instant::now();
        if !self.enqueue(i, now) {
            return false;
        }
        let k = endpoint(&self.endpoints, self.peers[i].socket);
        self.pump(registry, k, now, false, Some(i));
        true
    }

    // Copies into every connection's stream. Fan-out most likely wants a
    // pubsub `Publisher` instead, which sends the same bytes to all, but
    // maybe not.
    pub(crate) fn broadcast_with<F>(&mut self, registry: &Registry, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        if !self.peers.iter().any(|p| p.connected && p.close_at.is_none()) ||
            !self.append_record(serialise)
        {
            return 0;
        }
        self.broadcast_buffered(registry)
    }

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
        if !self.peers.iter().any(|p| p.connected && p.close_at.is_none()) {
            return 0;
        }
        let mut appended = false;
        for item in items {
            appended |= self.append_record(|buf| serialise(buf, item));
        }
        if !appended {
            return 0;
        }
        self.broadcast_buffered(registry)
    }

    fn broadcast_buffered(&mut self, registry: &Registry) -> usize {
        let now = Instant::now();
        let mut sent = 0;
        let mut i = self.peers.len();
        while i != 0 {
            i -= 1;
            let peer = &self.peers[i];
            if !peer.connected || peer.close_at.is_some() {
                continue;
            }
            self.scratch.clear();
            self.scratch.extend_from_slice(&self.send_buffer);
            sent += usize::from(self.enqueue(i, now));
        }
        self.send_buffer.clear();
        for k in 0..self.endpoints.len() {
            self.pump(registry, k, now, false, None);
        }
        sent
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

    /// Takes the records in `scratch`. A peer they would push a window past
    /// its acks is lost.
    fn enqueue(&mut self, i: usize, now: Instant) -> bool {
        let len = self.scratch.len();
        let peer = &mut self.peers[i];
        let Some(min_base) = peer.stream.min_base(len) else {
            error!(group = self.config.name, len, "udp send batch exceeds send_window");
            self.scratch.clear();
            return false;
        };
        if peer.out.base() < min_base {
            warn!(addr = %peer.addr, "udp peer fell a window behind");
            self.scratch.clear();
            self.lose(i, now);
            return false;
        }
        peer.stream.enqueue(&mut self.scratch);
        true
    }

    /// Stages every peer on endpoint `k`, or just `only`.
    fn pump(
        &mut self,
        registry: &Registry,
        k: usize,
        now: Instant,
        probing: bool,
        only: Option<usize>,
    ) {
        let token = self.endpoints[k].socket.token;
        let fd = self.endpoints[k].socket.fd();
        let peers = only.map_or(0..self.peers.len(), |i| i..i + 1);
        for i in peers {
            let peer = &mut self.peers[i];
            if peer.socket != token || !peer.connected {
                continue;
            }
            let mut probes = if probing { peer.out.probe_budget(now) } else { 0 };
            loop {
                let peer = &mut self.peers[i];
                let packets = peer.out.stage(&mut peer.stream, &mut self.batch, probes, now);
                probes = 0;
                if packets != 0 {
                    self.staged.push(Staged { peer: i, packets });
                }
                if !self.batch.is_full() {
                    break;
                }
                if !self.dispatch(fd, now) {
                    self.endpoints[k].socket.arm_writable(registry);
                    return;
                }
            }
        }
        if !self.batch.is_empty() && !self.dispatch(fd, now) {
            self.endpoints[k].socket.arm_writable(registry);
        }
    }

    /// `false` if the kernel took less than everything.
    fn dispatch(&mut self, fd: i32, now: Instant) -> bool {
        let total_packets: usize = self.staged.iter().map(|staged| staged.packets).sum();
        let accepted_packets = match self.batch.send(fd) {
            Ok(k) => k,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
            // Other errors count as sent so the retransmit path retries. They are
            // per destination (route, firewall, path MTU), yet fail the whole
            // batch, so one bad peer also costs the others on its socket.
            Err(e) => {
                debug!(?e, "udp send failed");
                total_packets
            }
        };
        let mut remaining_packets = accepted_packets;
        for staged in &self.staged {
            let take = remaining_packets.min(staged.packets);
            remaining_packets -= take;
            let peer = &mut self.peers[staged.peer];
            peer.out.commit(&peer.stream, take, now);
            if !self.config.udp.reliable {
                peer.release();
            }
        }
        self.staged.clear();
        accepted_packets == total_packets
    }

    fn tick(&mut self, registry: &Registry, now: Instant) {
        let peer_timeout = self.config.peer_timeout;
        self.dropped.retain(|&(_, _, at)| now.saturating_sub(at) < peer_timeout);
        let mut i = self.peers.len();
        while i != 0 {
            i -= 1;
            let peer = &self.peers[i];
            if peer.drained() {
                self.lose(i, now);
            } else if peer.connected && now.saturating_sub(peer.last_recv) >= peer_timeout {
                warn!(addr = %peer.addr, "udp peer timed out");
                self.lose(i, now);
            }
        }
        let msg = self.config.on_connect_msg.as_deref().unwrap_or_default();
        for peer in &mut self.peers {
            let k = endpoint(&self.endpoints, peer.socket);
            if peer.tick(&self.endpoints[k].socket.udp, &mut self.ctrl, msg, now) ==
                SendOutcome::WouldBlock
            {
                self.endpoints[k].socket.arm_writable(registry);
            }
        }
        for k in 0..self.endpoints.len() {
            self.pump(registry, k, now, true, None);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_datagram<F>(
        &mut self,
        k: usize,
        bytes: &[u8],
        from: SocketAddr,
        now: Instant,
        tokens: &mut Tokens,
        dcache: Option<&DCache>,
        deliver: &mut F,
    ) where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let Some(datagram) = wire::decode(bytes) else { return };
        let socket = self.endpoints[k].socket.token;
        let group = self.group;
        let find = |peers: &[Peer], session: u32| {
            peers.iter().position(|p| p.socket == socket && p.addr == from && p.session == session)
        };
        match datagram {
            Datagram::Hello { initiator, msg } => {
                if self.endpoints[k].listener && on_connect_fits(&self.config, msg, from) {
                    self.on_connect(k, initiator, msg, from, now, tokens, dcache, deliver);
                }
            }
            Datagram::HelloAck { initiator, acceptor, msg } => {
                let Some(i) = find(&self.peers, initiator) else { return };
                let peer = &mut self.peers[i];
                if peer.role != Role::Initiator || !on_connect_fits(&self.config, msg, from) {
                    return;
                }
                if !peer.connected {
                    peer.establish(acceptor, now);
                    let token = peer.token;
                    // A reconnect can complete before post_poll drains the old disconnect.
                    self.drain_pending_disconnects(deliver);
                    debug!(addr = %from, "udp connected");
                    deliver(Event::Connected { group, token, peer_addr: from });
                    deliver_on_connect(group, token, msg, dcache, deliver);
                } else if acceptor != peer.remote_session {
                    warn!(addr = %from, "udp peer lost our connection, reconnecting");
                    self.lose(i, now);
                }
            }
            Datagram::Packet(packet) => {
                let Some(i) = find(&self.peers, packet.session) else {
                    send_reset(&self.endpoints[k].socket.udp, from, packet.session);
                    return;
                };
                let peer = &mut self.peers[i];
                // Ahead of a hello ack, which brings the peer's session to ack it with.
                if !peer.connected {
                    return;
                }
                peer.last_recv = now;
                let token = peer.token;
                peer.on_packet(packet, &mut |bytes, send_ts| {
                    deliver_message(&mut None, bytes, send_ts, dcache, &mut |payload, send_ts| {
                        deliver(Event::Message { group, token, payload, send_ts });
                    });
                });
            }
            Datagram::Ack { receiver, ack_next, bitmap } => {
                let Some(i) = find(&self.peers, receiver) else {
                    send_reset(&self.endpoints[k].socket.udp, from, receiver);
                    return;
                };
                let peer = &mut self.peers[i];
                if !peer.connected {
                    return;
                }
                peer.last_recv = now;
                peer.out.on_ack(ack_next, bitmap, now);
                peer.release();
                if peer.drained() {
                    self.lose(i, now);
                }
            }
            Datagram::Reset { session } => {
                let Some(i) = self.peers.iter().position(|p| {
                    p.socket == socket &&
                        p.addr == from &&
                        if p.connected {
                            p.remote_session == session
                        } else {
                            p.session == session
                        }
                }) else {
                    return;
                };
                warn!(addr = %from, "udp peer lost our connection");
                self.lose(i, now);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_connect<F>(
        &mut self,
        k: usize,
        initiator: u32,
        msg: &[u8],
        from: SocketAddr,
        now: Instant,
        tokens: &mut Tokens,
        dcache: Option<&DCache>,
        deliver: &mut F,
    ) where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let group = self.group;
        let listener = self.endpoints[k].socket.token;
        if self.dropped.iter().any(|&(addr, session, _)| addr == from && session == initiator) {
            send_reset(&self.endpoints[k].socket.udp, from, initiator);
            return;
        }
        let reply = self.config.on_connect_msg.as_deref().unwrap_or_default();
        if let Some(i) = self.peers.iter().position(|p| p.socket == listener && p.addr == from) {
            if self.peers[i].remote_session == initiator {
                // A retry: the hello ack was lost or is still on its way.
                self.peers[i].last_recv = now;
                self.peers[i].send_hello_ack(&self.endpoints[k].socket.udp, &mut self.ctrl, reply);
                return;
            }
            // The peer started over.
            let old = self.remove_acceptor(i, now);
            deliver(Event::Disconnected { group, token: old, peer_addr: from });
        }
        let token = tokens.allocate(group);
        let mut session = new_session(token.0);
        while session == 0 ||
            self.peers.iter().any(|p| p.socket == listener && p.session == session)
        {
            session = session.wrapping_add(1);
        }
        let mut peer = Peer::new(from, token, listener, Role::Acceptor, session, &self.config, now);
        peer.establish(initiator, now);
        let reply = self.config.on_connect_msg.as_deref().unwrap_or_default();
        peer.send_hello_ack(&self.endpoints[k].socket.udp, &mut self.ctrl, reply);
        self.peers.push(peer);
        info!(addr = %from, "udp peer connected");
        deliver(Event::Accepted { group, token, peer_addr: from });
        deliver_on_connect(group, token, msg, dcache, deliver);
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
        let Some(k) = self.endpoints.iter().position(|e| e.socket.token == event.token()) else {
            return;
        };
        let now = Instant::now();
        if event.is_readable() {
            let fd = self.endpoints[k].socket.fd();
            recv_all(fd, recv, |bytes, from| {
                self.on_datagram(k, bytes, from, now, tokens, dcache, deliver);
            });
        }
        if event.is_writable() {
            self.endpoints[k].socket.on_writable();
        }
        let token = self.endpoints[k].socket.token;
        for peer in &mut self.peers {
            if peer.socket == token &&
                peer.ack_due &&
                peer.send_ack(&self.endpoints[k].socket.udp, &mut self.ctrl, now) ==
                    SendOutcome::WouldBlock
            {
                self.endpoints[k].socket.arm_writable(registry);
            }
        }
        self.pump(registry, k, now, false, None);
        if event.is_writable() {
            self.endpoints[k].socket.disarm(registry);
        }
    }

    pub(crate) fn tick_interval(&self) -> Duration {
        self.config.udp.min_rto / 2
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
}

fn deliver_on_connect<F>(
    group: Group,
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
    deliver_message(&mut None, msg, Nanos::now(), dcache, &mut |payload, send_ts| {
        deliver(Event::Message { group, token, payload, send_ts });
    });
}
