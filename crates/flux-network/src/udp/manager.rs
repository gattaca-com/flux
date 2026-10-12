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
    stream::Stream,
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
    subscribers: Vec<Subscriber>,
    /// A late hello retry from a dropped subscriber gets a reset instead of
    /// rejoining.
    dropped: Vec<(SocketAddr, u32, Instant)>,
    subscriptions: Vec<Subscription>,

    send_buffer: Vec<u8>,
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
        let mut subscription = Subscription::new(addr, token, &self.config);
        subscription.send_hello(&socket, on_connect_msg(&self.config), Instant::now());
        self.sockets.push(Endpoint { token, socket, listener: false, writable_armed: false });
        self.subscriptions.push(subscription);
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

    fn release(&mut self) {
        let base = self
            .subscribers
            .iter()
            .map(Subscriber::base)
            .min()
            .unwrap_or_else(|| self.stream.next());
        if base > self.stream.base() {
            self.stream.release_to(base);
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

    fn publish(&mut self, registry: &Registry) -> usize {
        safe_assert!(!self.send_buffer.is_empty());
        let len = self.send_buffer.len();
        let Some(min_base) = self.stream.min_base(len) else {
            error!(group = self.config.name, len, "udp broadcast batch exceeds send_window");
            self.send_buffer.clear();
            return 0;
        };
        let mut i = self.subscribers.len();
        while i != 0 {
            i -= 1;
            if self.subscribers[i].base() < min_base {
                warn!(addr = %self.subscribers[i].addr, "udp subscriber fell a window behind");
                self.drop_subscriber(i);
            }
        }
        // Enqueued with no one holding it, it would never be released.
        if self.subscribers.is_empty() {
            self.send_buffer.clear();
            return 0;
        }
        self.stream.enqueue(&mut self.send_buffer);
        let recipients = self.subscribers.iter().filter(|s| !s.is_draining()).count();
        let now = Instant::now();
        for k in 0..self.sockets.len() {
            if self.sockets[k].listener {
                self.pump(registry, k, now, false);
            }
        }
        recipients
    }

    fn pump(&mut self, registry: &Registry, k: usize, now: Instant, probing: bool) {
        let token = self.sockets[k].token;
        let fd = self.sockets[k].socket.as_raw_fd();
        for i in 0..self.subscribers.len() {
            if self.subscribers[i].socket_token != token {
                continue;
            }
            let mut probes = if probing { self.subscribers[i].probe_budget(now) } else { 0 };
            loop {
                let packets =
                    self.subscribers[i].stage(&mut self.stream, &mut self.batch, probes, now);
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

    /// `false` if the kernel took less than everything.
    fn dispatch(&mut self, fd: i32, now: Instant) -> bool {
        let total_packets: usize = self.staged.iter().map(|staged| staged.packets).sum();
        let accepted_packets = match self.batch.send(fd) {
            Ok(k) => k,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
            // Other errors count as sent so the retransmit path retries. They are
            // per destination (route, firewall, path MTU), yet fail the whole
            // batch, so one bad subscriber also costs the others.
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
            Datagram::HelloAck { publisher, subscriber, join, msg } => {
                if !self.on_connect_fits(msg, from) {
                    return;
                }
                let token = self.subscriptions[i].token;
                if self.subscriptions[i].lost(subscriber, Some(publisher)) {
                    warn!(addr = %from, "udp publisher lost our subscription, resubscribing");
                    self.resubscribe(i, now);
                } else if self.subscriptions[i].on_hello_ack(publisher, subscriber, join) {
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
            Datagram::Ack { publisher, subscriber, ack_next, bitmap } => {
                let Some(i) = index.filter(|&i| {
                    publisher == self.stream.session() && self.subscribers[i].session == subscriber
                }) else {
                    send_reset(&self.sockets[k].socket, from, subscriber);
                    return;
                };
                let held_oldest = self.subscribers[i].base() == self.stream.base();
                self.subscribers[i].on_ack(ack_next, bitmap, now);
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
                // The open packet may already be on its way to others.
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

    fn on_connect_fits(&self, msg: &[u8], from: SocketAddr) -> bool {
        let fits = msg.len() <= self.config.udp.max_message_size;
        if !fits {
            warn!(addr = %from, len = msg.len(), "udp on-connect message exceeds max_message_size");
        }
        fits
    }

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
