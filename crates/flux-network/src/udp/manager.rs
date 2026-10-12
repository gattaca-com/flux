//! One UDP group: its listeners publish one stream to every subscriber, and
//! its `connect` tokens subscribe to other publishers. Data only flows from
//! a publisher to its subscribers.

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
    SendOutcome, new_session, send_datagram,
    stream::{MessageStore, Stream},
    subscriber::Subscriber,
    subscription::{Subscription, deliver_message},
    sys::{RecvBatch, SendBatch},
    wire::{self, Datagram, HELLO_ACK_SIZE, MESSAGE_HEADER, RESET_SIZE},
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

/// Tells the sender of a datagram naming `subscriber` that we hold no
/// subscriber for it.
fn send_reset(socket: &UdpSocket, addr: SocketAddr, subscriber: u32) {
    let mut buf = [0; RESET_SIZE];
    wire::reset(&mut buf, subscriber);
    send_datagram(socket, addr, &buf);
}

struct StagedBatch {
    subscriber: usize,
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
    stream: Stream,
    store: MessageStore,
    subscribers: Vec<Subscriber>,
    /// Subscribers dropped within `peer_timeout`, with when: a hello naming
    /// one is a stale retry, answered with a reset rather than a new
    /// subscriber.
    dropped: Vec<(SocketAddr, u32, Instant)>,
    subscriptions: Vec<Subscription>,

    /// users serialise here. and then it's moved to `MessageStore`
    send_buffer: Vec<u8>,
    /// Hello ack staging, sized for the on-connect message.
    ctrl: Vec<u8>,

    batch: SendBatch,
    // entries here correspond to `SendBatch` order
    staged: Vec<StagedBatch>,

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
            ctrl: vec![0; HELLO_ACK_SIZE + on_connect_msg(&config).len()],
            stream: Stream::new(&udp, new_session(0)),
            config,
            group,
            sockets: Vec::new(),
            store: MessageStore::new(),
            subscribers: Vec::new(),
            dropped: Vec::new(),
            subscriptions: Vec::new(),
            send_buffer: Vec::with_capacity(32 * 1024),
            batch: SendBatch::new(udp.max_datagram_size),
            staged: Vec::new(),
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

    /// Subscribes to the publisher at `addr` from a socket of its own.
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
        let mut subscription = Subscription::new(addr, token, &self.config);
        subscription.send_hello(&socket, on_connect_msg(&self.config), Instant::now());
        self.sockets.push(Endpoint { token, socket, listener: false, writable_armed: false });
        self.subscriptions.push(subscription);
        token
    }

    /// Publishes the group's stream on `addr`.
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

    /// Starts subscription `i` over with a fresh session.
    fn resubscribe(&mut self, i: usize, now: Instant) {
        let subscription = &mut self.subscriptions[i];
        if subscription.is_connected() {
            self.pending_disconnect_notifications.push((subscription.token, subscription.addr));
        }
        subscription.reset();
        let socket = &self
            .sockets
            .iter()
            .find(|s| s.token == subscription.token)
            .expect("subscription must have a socket")
            .socket;
        subscription.send_hello(socket, on_connect_msg(&self.config), now);
    }

    /// Lets the stream drop what no subscriber needs any more.
    fn release(&mut self) {
        let base = self
            .subscribers
            .iter()
            .map(Subscriber::base)
            .min()
            .unwrap_or_else(|| self.stream.next());
        if base > self.stream.base() {
            self.stream.release_to(base, &mut self.store);
        }
    }

    fn remove_subscriber(&mut self, index: usize) -> Token {
        let subscriber = self.subscribers.swap_remove(index);
        self.dropped.push((subscriber.addr, subscriber.session, Instant::now()));
        self.retired.push(subscriber.token);
        self.release();
        subscriber.token
    }

    fn drop_subscriber(&mut self, index: usize) {
        let subscriber = &self.subscribers[index];
        self.pending_disconnect_notifications.push((subscriber.token, subscriber.addr));
        self.remove_subscriber(index);
    }

    /// Drops a subscriber, which learns of it from the reset its next ack
    /// draws, or restarts a subscription. A listener token is not a session
    /// and is left alone.
    pub(crate) fn disconnect(&mut self, token: Token) -> bool {
        if let Some(i) = self.subscribers.iter().position(|s| s.token == token) {
            self.drop_subscriber(i);
            return true;
        }
        let Some(i) = self.subscriptions.iter().position(|s| s.token == token) else {
            return false;
        };
        self.resubscribe(i, Instant::now());
        true
    }

    /// Drops a subscriber once it has acked everything published so far. A
    /// subscription has nothing of its own to drain and restarts at once.
    pub(crate) fn disconnect_when_drained(&mut self, token: Token) -> bool {
        if let Some(i) = self.subscribers.iter().position(|s| s.token == token) {
            let end = self.stream.next();
            if self.subscribers[i].close_when_drained(end) {
                self.drop_subscriber(i);
            }
            return true;
        }
        self.disconnect(token)
    }

    /// Removes a listener and every subscriber that joined through it.
    fn remove_listener(&mut self, registry: &Registry, token: Token) -> bool {
        let Some(k) = self.sockets.iter().position(|s| s.token == token && s.listener) else {
            return false;
        };
        let mut i = self.subscribers.len();
        while i != 0 {
            i -= 1;
            if self.subscribers[i].socket_token == token {
                self.drop_subscriber(i);
            }
        }
        let mut entry = self.sockets.swap_remove(k);
        let _ = registry.deregister(&mut entry.socket);
        self.retired.push(token);
        true
    }

    pub(crate) fn disconnect_outbound(&mut self) {
        let now = Instant::now();
        for i in 0..self.subscriptions.len() {
            self.resubscribe(i, now);
        }
    }

    /// One-way: there is no per-token send. Publish with a broadcast.
    pub(crate) fn send_with(&self, token: Token) -> bool {
        error!(group = self.config.name, ?token, "udp is one-way: broadcast on a listening group");
        false
    }

    pub(crate) fn broadcast_with<F>(&mut self, registry: &Registry, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        if !self.can_publish() || !self.append_record(serialise) {
            return 0;
        }
        self.publish(registry)
    }

    /// Serialises every item into one stream, queues it and flushes each
    /// listener once.
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
        if !self.can_publish() {
            return 0;
        }
        let mut appended = false;
        for item in items {
            appended |= self.append_record(|buf| serialise(buf, item));
        }
        if !appended {
            return 0;
        }
        self.publish(registry)
    }

    /// Whether a broadcast has anyone to reach; nothing is serialised
    /// otherwise.
    fn can_publish(&self) -> bool {
        if !self.sockets.iter().any(|s| s.listener) {
            error!(group = self.config.name, "udp broadcast on a group that does not listen");
            return false;
        }
        self.subscribers.iter().any(|s| !s.is_draining())
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

    /// Queues the serialised records on the stream and sends them. Makes
    /// room by dropping every subscriber still holding the oldest packet.
    /// Returns how many subscribers it was queued for.
    fn publish(&mut self, registry: &Registry) -> usize {
        safe_assert!(!self.send_buffer.is_empty());
        let len = self.send_buffer.len();
        while !self.stream.fits(len) && !self.subscribers.is_empty() {
            let oldest = self.stream.base();
            let mut i = self.subscribers.len();
            while i != 0 {
                i -= 1;
                if self.subscribers[i].base() == oldest {
                    warn!(addr = %self.subscribers[i].addr, "udp subscriber fell a window behind");
                    self.drop_subscriber(i);
                }
            }
        }
        if !self.stream.fits(len) {
            error!(group = self.config.name, len, "udp broadcast batch exceeds send_window");
            self.send_buffer.clear();
            return 0;
        }
        let slot = self.store.insert(&mut self.send_buffer);
        self.stream.enqueue(&mut self.store, slot);
        self.store.release(slot);
        let recipients = self.subscribers.iter().filter(|s| !s.is_draining()).count();
        let now = Instant::now();
        for k in 0..self.sockets.len() {
            if self.sockets[k].listener {
                self.pump(registry, k, now, false);
            }
        }
        recipients
    }

    /// Sends every due packet of every subscriber on listener `k`, batched
    /// across subscribers. With `probing`, each may also be resent what its
    /// timeout allows. Arms WRITABLE if the kernel stopped accepting.
    fn pump(&mut self, registry: &Registry, k: usize, now: Instant, probing: bool) {
        let token = self.sockets[k].token;
        let fd = self.sockets[k].socket.as_raw_fd();
        for i in 0..self.subscribers.len() {
            if self.subscribers[i].socket_token != token {
                continue;
            }
            let mut probes = if probing { self.subscribers[i].probe_budget(now) } else { 0 };
            loop {
                let packets = self.subscribers[i].stage(
                    &mut self.stream,
                    &self.store,
                    &mut self.batch,
                    probes,
                    now,
                );
                probes = 0;
                if packets != 0 {
                    self.staged.push(StagedBatch { subscriber: i, packets });
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

    /// Sends the batch and tells each subscriber what the kernel took.
    /// `false` if it took less than everything.
    fn dispatch(&mut self, fd: i32, now: Instant) -> bool {
        let total_packets: usize = self.staged.iter().map(|staged| staged.packets).sum();
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
        for staged in &self.staged {
            let take = remaining_packets.min(staged.packets);
            remaining_packets -= take;
            self.subscribers[staged.subscriber].commit(&self.stream, take, now);
        }
        self.staged.clear();
        if !self.config.udp.reliable {
            self.release();
        }
        accepted_packets == total_packets
    }

    pub(crate) fn currently_disconnected(&self) -> impl Iterator<Item = Token> {
        self.subscriptions.iter().filter(|s| !s.is_connected()).map(|s| s.token)
    }

    pub(crate) fn force_reconnect(&mut self) {
        let now = Instant::now();
        for subscription in self.subscriptions.iter_mut().filter(|s| !s.is_connected()) {
            let socket = &self
                .sockets
                .iter()
                .find(|s| s.token == subscription.token)
                .expect("subscription must have a socket")
                .socket;
            subscription.send_hello(socket, on_connect_msg(&self.config), now);
        }
    }

    /// Hello retries, heartbeat acks, subscriber timeouts, drained closes and
    /// retransmit probes.
    fn tick(&mut self, registry: &Registry, now: Instant) {
        let peer_timeout = self.config.peer_timeout;
        self.dropped.retain(|&(_, _, at)| now.saturating_sub(at) < peer_timeout);
        let mut i = self.subscribers.len();
        while i != 0 {
            i -= 1;
            let subscriber = &self.subscribers[i];
            if subscriber.drained() {
                self.drop_subscriber(i);
            } else if now.saturating_sub(subscriber.last_recv) >= peer_timeout {
                warn!(addr = %subscriber.addr, "udp subscriber timed out");
                self.drop_subscriber(i);
            }
        }
        for i in 0..self.subscriptions.len() {
            let subscription = &mut self.subscriptions[i];
            let entry = self
                .sockets
                .iter_mut()
                .find(|s| s.token == subscription.token)
                .expect("subscription must have a socket");
            if subscription.tick(&entry.socket, on_connect_msg(&self.config), now) ==
                SendOutcome::WouldBlock
            {
                arm_writable(registry, entry);
            }
        }
        for k in 0..self.sockets.len() {
            if self.sockets[k].listener {
                self.pump(registry, k, now, true);
            }
        }
    }

    /// One received datagram on socket `k`.
    #[allow(clippy::too_many_arguments)]
    fn on_datagram<F>(
        &mut self,
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
        if self.sockets[k].listener {
            self.on_publisher_datagram(k, datagram, from, now, dcache, tokens, deliver);
            return;
        }
        let socket_token = self.sockets[k].token;
        let Some(i) =
            self.subscriptions.iter().position(|s| s.token == socket_token && s.addr == from)
        else {
            return;
        };
        let group = self.group;
        match datagram {
            Datagram::Packet(packet) => {
                let subscription = &mut self.subscriptions[i];
                let token = subscription.token;
                subscription.on_packet(packet, dcache, &mut |payload, send_ts| {
                    deliver(Event::Message { group, token, payload, send_ts });
                });
            }
            Datagram::HelloAck { session, subscriber, join, msg } => {
                if !self.on_connect_fits(msg, from) {
                    return;
                }
                let token = self.subscriptions[i].token;
                if self.subscriptions[i].lost(subscriber, Some(session)) {
                    warn!(addr = %from, "udp publisher lost our subscription, resubscribing");
                    self.resubscribe(i, now);
                } else if self.subscriptions[i].on_hello_ack(session, subscriber, join) {
                    // A resubscribe can complete before post_poll drains the old disconnect.
                    self.drain_pending_disconnects(deliver);
                    debug!(addr = %from, "udp subscribed");
                    deliver(Event::Connected { group, token, peer_addr: from });
                    self.deliver_on_connect(token, msg, dcache, deliver);
                }
            }
            Datagram::Reset { subscriber } => {
                if self.subscriptions[i].lost(subscriber, None) {
                    warn!(addr = %from, "udp publisher lost our subscription, resubscribing");
                    self.resubscribe(i, now);
                }
            }
            Datagram::Ack { .. } | Datagram::Hello { .. } => {}
        }
    }

    /// A datagram on listener `k`: a subscriber's hello or ack.
    #[allow(clippy::too_many_arguments)]
    fn on_publisher_datagram<F>(
        &mut self,
        k: usize,
        datagram: Datagram<'_>,
        from: SocketAddr,
        now: Instant,
        dcache: Option<&DCache>,
        tokens: &mut Tokens,
        deliver: &mut F,
    ) where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let socket_token = self.sockets[k].token;
        let index =
            self.subscribers.iter().position(|s| s.socket_token == socket_token && s.addr == from);
        match datagram {
            Datagram::Ack { session, subscriber, ack_next, bits, bitmap } => {
                let Some(i) = index.filter(|&i| {
                    session == self.stream.session() && self.subscribers[i].session == subscriber
                }) else {
                    // A subscriber we dropped, or one from before our restart.
                    send_reset(&self.sockets[k].socket, from, subscriber);
                    return;
                };
                let held_oldest = self.subscribers[i].base() == self.stream.base();
                self.subscribers[i].on_ack(ack_next, u64::from(bits), bitmap, now);
                if self.subscribers[i].drained() {
                    self.drop_subscriber(i);
                } else if held_oldest {
                    self.release();
                }
            }
            Datagram::Hello { subscriber, msg } => {
                if !self.on_connect_fits(msg, from) {
                    return;
                }
                if self
                    .dropped
                    .iter()
                    .any(|&(addr, session, _)| addr == from && session == subscriber)
                {
                    send_reset(&self.sockets[k].socket, from, subscriber);
                    return;
                }
                if let Some(i) = index {
                    if self.subscribers[i].session == subscriber {
                        // A retry: the hello ack was lost or is still on its way.
                        self.subscribers[i].last_recv = now;
                        let join = self.subscribers[i].join;
                        self.send_hello_ack(k, from, subscriber, join);
                        return;
                    }
                    let old = self.remove_subscriber(i);
                    deliver(Event::Disconnected { group: self.group, token: old, peer_addr: from });
                }
                // The open packet may already be on its way to others; a
                // newcomer starts after it.
                self.stream.seal();
                let join = self.stream.next();
                let token = tokens.allocate(self.group);
                self.subscribers.push(Subscriber::new(
                    from,
                    token,
                    socket_token,
                    subscriber,
                    join,
                    &self.config,
                    now,
                ));
                self.send_hello_ack(k, from, subscriber, join);
                info!(addr = %from, "udp subscriber joined");
                deliver(Event::Accepted { group: self.group, token, peer_addr: from });
                // Right after `Accepted`, so the caller can vet the subscriber
                // before its next broadcast.
                self.deliver_on_connect(token, msg, dcache, deliver);
            }
            Datagram::Packet(_) | Datagram::HelloAck { .. } | Datagram::Reset { .. } => {}
        }
    }

    fn send_hello_ack(&mut self, k: usize, to: SocketAddr, subscriber: u32, join: u64) {
        let n = wire::hello_ack(
            &mut self.ctrl,
            self.stream.session(),
            subscriber,
            join,
            on_connect_msg(&self.config),
        );
        send_datagram(&self.sockets[k].socket, to, &self.ctrl[..n]);
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
                        self.on_datagram(k, bytes, from, now, dcache, tokens, deliver);
                    }
                }
            }
        }
        if event.is_writable() {
            self.sockets[k].writable_armed = false;
        }
        if self.sockets[k].listener {
            // Acks just taken in may have exposed holes; packets just queued
            // behind a full buffer may fit now.
            self.pump(registry, k, now, false);
        } else {
            let entry = &mut self.sockets[k];
            let token = entry.token;
            if let Some(subscription) = self.subscriptions.iter_mut().find(|s| s.token == token) &&
                subscription.take_ack_due() &&
                subscription.send_ack(&entry.socket, now) == SendOutcome::WouldBlock
            {
                arm_writable(registry, entry);
            }
        }
        let entry = &mut self.sockets[k];
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

    /// Permanently removes a subscriber, a subscription, or a listener with
    /// every subscriber on it.
    pub(crate) fn remove(&mut self, registry: &Registry, token: Token) -> bool {
        if let Some(i) = self.subscribers.iter().position(|s| s.token == token) {
            self.remove_subscriber(i);
            self.pending_disconnect_notifications.retain(|(t, _)| *t != token);
            return true;
        }
        if let Some(i) = self.subscriptions.iter().position(|s| s.token == token) {
            self.subscriptions.swap_remove(i);
            self.pending_disconnect_notifications.retain(|(t, _)| *t != token);
            let k = self
                .sockets
                .iter()
                .position(|s| s.token == token)
                .expect("subscription must have a socket");
            let mut endpoint = self.sockets.swap_remove(k);
            let _ = registry.deregister(&mut endpoint.socket);
            self.retired.push(token);
            return true;
        }
        self.remove_listener(registry, token)
    }
}
