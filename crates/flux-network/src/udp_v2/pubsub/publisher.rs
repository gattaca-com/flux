use std::{io, net::SocketAddr};

use flux_timing::{Duration, Instant};
use flux_utils::safe_assert;
use mio::{Registry, Token, event::Event as MioEvent};
use tracing::{debug, error, info, warn};

use super::{
    PublisherEvent, TokenRange,
    wire::{self, Datagram, HELLO_ACK_SIZE, RESET_SIZE},
};
use crate::{
    network::{PayloadBuf, UdpGroupConfigV2},
    udp_v2::{
        Socket, new_session,
        outbound::Outbound,
        recv_all, send_datagram,
        stream::Stream,
        sys::{RecvBatch, SendBatch},
        telemetry::{PeerId, Telemetry},
        wire::MESSAGE_HEADER,
    },
};

struct Recipient {
    addr: SocketAddr,
    token: Token,
    listener: Token,
    session: u32,
    join: u64,
    last_recv: Instant,
    close_at: Option<u64>,
    out: Outbound,
}

impl Recipient {
    #[inline]
    fn base(&self) -> u64 {
        self.out.base()
    }

    #[inline]
    fn is_draining(&self) -> bool {
        self.close_at.is_some()
    }

    #[inline]
    fn drained(&self) -> bool {
        self.close_at.is_some_and(|end| self.base() >= end)
    }
}

struct Staged {
    recipient: usize,
    packets: usize,
}

pub(super) struct PublisherState {
    config: UdpGroupConfigV2,
    pub(super) tokens: TokenRange,
    listeners: Vec<Socket>,
    stream: Stream,
    recipients: Vec<Recipient>,
    /// A late hello retry from a dropped subscriber gets a reset instead of
    /// rejoining.
    dropped: Vec<(SocketAddr, u32, Instant)>,
    send_buffer: Vec<u8>,
    ctrl: Vec<u8>,
    batch: SendBatch,
    // entries here correspond to `SendBatch` order
    staged: Vec<Staged>,
    unsubscribed: Vec<(Token, SocketAddr)>,
    telemetry: Telemetry,
    next_tick: Instant,
}

fn on_connect_msg(config: &UdpGroupConfigV2) -> &[u8] {
    config.on_connect_msg.as_deref().unwrap_or_default()
}

impl PublisherState {
    pub(super) fn new(config: UdpGroupConfigV2, tokens: TokenRange) -> Self {
        let udp = config.udp;
        crate::udp_v2::validate_group(&config, HELLO_ACK_SIZE);
        Self {
            ctrl: vec![0; HELLO_ACK_SIZE + on_connect_msg(&config).len()],
            stream: Stream::new(&udp, wire::PACKET, new_session(0)),
            batch: SendBatch::new(udp.max_datagram_size),
            telemetry: Telemetry::new(config.telemetry, config.name),
            config,
            tokens,
            listeners: Vec::new(),
            recipients: Vec::new(),
            dropped: Vec::new(),
            send_buffer: Vec::with_capacity(32 * 1024),
            staged: Vec::new(),
            unsubscribed: Vec::new(),
            next_tick: Instant::ZERO,
        }
    }

    pub(super) fn close_all(&mut self, registry: &Registry) {
        for listener in &mut self.listeners {
            listener.close(registry);
        }
    }

    pub(super) fn listen(&mut self, registry: &Registry, addr: SocketAddr) -> io::Result<Token> {
        let token = self.tokens.allocate();
        let socket = Socket::open(
            registry,
            addr,
            token,
            Some(&mut self.batch),
            self.config.socket_buf_size,
        )?;
        self.listeners.push(socket);
        Ok(token)
    }

    fn release(&mut self) {
        let base =
            self.recipients.iter().map(Recipient::base).min().unwrap_or_else(|| self.stream.next());
        if base > self.stream.base() {
            self.stream.release_to(base);
        }
    }

    fn remove_recipient(&mut self, i: usize) {
        let recipient = self.recipients.swap_remove(i);
        self.dropped.push((recipient.addr, recipient.session, Instant::now()));
        self.release();
    }

    fn drop_recipient(&mut self, i: usize) {
        let recipient = &self.recipients[i];
        self.unsubscribed.push((recipient.token, recipient.addr));
        self.remove_recipient(i);
    }

    pub(super) fn disconnect(&mut self, token: Token) -> bool {
        let Some(i) = self.recipients.iter().position(|r| r.token == token) else { return false };
        self.drop_recipient(i);
        true
    }

    pub(super) fn disconnect_when_drained(&mut self, token: Token) -> bool {
        let Some(i) = self.recipients.iter().position(|r| r.token == token) else { return false };
        self.recipients[i].close_at = Some(self.stream.next());
        if self.recipients[i].drained() {
            self.drop_recipient(i);
        }
        true
    }

    pub(super) fn remove(&mut self, registry: &Registry, token: Token) -> bool {
        if let Some(i) = self.recipients.iter().position(|r| r.token == token) {
            self.remove_recipient(i);
            self.unsubscribed.retain(|(t, _)| *t != token);
            return true;
        }
        let Some(k) = self.listeners.iter().position(|s| s.token == token) else { return false };
        let mut i = self.recipients.len();
        while i != 0 {
            i -= 1;
            if self.recipients[i].listener == token {
                self.drop_recipient(i);
            }
        }
        self.listeners.swap_remove(k).close(registry);
        true
    }

    pub(super) fn broadcast_with<F>(&mut self, registry: &Registry, serialise: F) -> usize
    where
        F: FnOnce(&mut PayloadBuf<'_>),
    {
        if !self.can_publish() || !self.append_record(serialise) {
            return 0;
        }
        self.publish(registry)
    }

    pub(super) fn broadcast_many_with<I, F>(
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
        self.recipients.iter().any(|r| !r.is_draining())
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
                    name = self.config.name,
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
            error!(name = self.config.name, len, "udp broadcast batch exceeds send_window");
            self.send_buffer.clear();
            return 0;
        };
        let mut i = self.recipients.len();
        while i != 0 {
            i -= 1;
            if self.recipients[i].base() < min_base {
                warn!(addr = %self.recipients[i].addr, "udp subscriber fell a window behind");
                self.drop_recipient(i);
            }
        }
        // Enqueued with no one holding it, it would never be released.
        if self.recipients.is_empty() {
            self.send_buffer.clear();
            return 0;
        }
        self.stream.enqueue(&mut self.send_buffer);
        let recipients = self.recipients.iter().filter(|r| !r.is_draining()).count();
        let now = Instant::now();
        for k in 0..self.listeners.len() {
            self.pump(registry, k, now, false);
        }
        recipients
    }

    fn pump(&mut self, registry: &Registry, k: usize, now: Instant, probing: bool) {
        let token = self.listeners[k].token;
        let fd = self.listeners[k].fd();
        for i in 0..self.recipients.len() {
            if self.recipients[i].listener != token {
                continue;
            }
            let mut probes = if probing { self.recipients[i].out.probe_budget(now) } else { 0 };
            loop {
                let packets =
                    self.recipients[i].out.stage(&mut self.stream, &mut self.batch, probes, now);
                probes = 0;
                if packets != 0 {
                    self.staged.push(Staged { recipient: i, packets });
                }
                if !self.batch.is_full() {
                    break;
                }
                if !self.dispatch(fd, now) {
                    self.listeners[k].arm_writable(registry);
                    return;
                }
            }
        }
        if !self.batch.is_empty() && !self.dispatch(fd, now) {
            self.listeners[k].arm_writable(registry);
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
            self.recipients[staged.recipient].out.commit(
                &self.stream,
                take,
                now,
                &mut self.telemetry,
            );
        }
        self.staged.clear();
        if !self.config.udp.reliable {
            self.release();
        }
        accepted_packets == total_packets
    }

    fn tick(&mut self, registry: &Registry, now: Instant) {
        let peer_timeout = self.config.peer_timeout;
        self.dropped.retain(|&(_, _, at)| now.saturating_sub(at) < peer_timeout);
        let mut i = self.recipients.len();
        while i != 0 {
            i -= 1;
            let recipient = &self.recipients[i];
            if recipient.drained() {
                self.drop_recipient(i);
            } else if now.saturating_sub(recipient.last_recv) >= peer_timeout {
                warn!(addr = %recipient.addr, "udp subscriber timed out");
                self.drop_recipient(i);
            }
        }
        for k in 0..self.listeners.len() {
            self.pump(registry, k, now, true);
        }
    }

    fn send_reset(&self, k: usize, to: SocketAddr, subscriber: u32) {
        let mut buf = [0; RESET_SIZE];
        wire::reset(&mut buf, subscriber);
        send_datagram(&self.listeners[k].udp, to, &buf);
    }

    fn on_datagram<F>(
        &mut self,
        k: usize,
        bytes: &[u8],
        from: SocketAddr,
        now: Instant,
        deliver: &mut F,
    ) where
        F: FnMut(PublisherEvent<'_>),
    {
        let Some(datagram) = wire::decode(bytes) else { return };
        let listener = self.listeners[k].token;
        let index = self.recipients.iter().position(|r| r.listener == listener && r.addr == from);
        match datagram {
            Datagram::Ack { publisher, subscriber, ack_next, bitmap } => {
                let Some(i) = index.filter(|&i| {
                    publisher == self.stream.session() && self.recipients[i].session == subscriber
                }) else {
                    self.send_reset(k, from, subscriber);
                    return;
                };
                let recipient = &mut self.recipients[i];
                recipient.last_recv = now;
                let held_oldest = recipient.base() == self.stream.base();
                recipient.out.on_ack(ack_next, bitmap, now);
                if recipient.drained() {
                    self.drop_recipient(i);
                } else if held_oldest {
                    self.release();
                }
            }
            Datagram::Hello { subscriber, msg } => {
                if !crate::udp_v2::on_connect_fits(&self.config, msg, from) {
                    return;
                }
                if self
                    .dropped
                    .iter()
                    .any(|&(addr, session, _)| addr == from && session == subscriber)
                {
                    self.send_reset(k, from, subscriber);
                    return;
                }
                if let Some(i) = index {
                    if self.recipients[i].session == subscriber {
                        // A retry: the hello ack was lost or is still on its way.
                        self.recipients[i].last_recv = now;
                        let join = self.recipients[i].join;
                        self.send_hello_ack(k, from, subscriber, join);
                        return;
                    }
                    let old = self.recipients[i].token;
                    self.remove_recipient(i);
                    deliver(PublisherEvent::Unsubscribed { token: old, peer_addr: from });
                }
                // The open packet may already be on its way to others.
                self.stream.seal();
                let join = self.stream.next();
                let token = self.tokens.allocate();
                self.recipients.push(Recipient {
                    addr: from,
                    token,
                    listener,
                    session: subscriber,
                    join,
                    last_recv: now,
                    close_at: None,
                    out: Outbound::new(from, join, &self.config.udp, PeerId::new(token, from)),
                });
                self.send_hello_ack(k, from, subscriber, join);
                info!(addr = %from, "udp subscriber joined");
                deliver(PublisherEvent::Subscribed { token, peer_addr: from, msg });
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
        send_datagram(&self.listeners[k].udp, to, &self.ctrl[..n]);
    }

    pub(super) fn handle_event<F>(
        &mut self,
        recv: &mut RecvBatch,
        registry: &Registry,
        event: &MioEvent,
        deliver: &mut F,
    ) where
        F: FnMut(PublisherEvent<'_>),
    {
        let Some(k) = self.listeners.iter().position(|s| s.token == event.token()) else { return };
        let now = Instant::now();
        if event.is_readable() {
            let fd = self.listeners[k].fd();
            recv_all(fd, recv, |bytes, from| self.on_datagram(k, bytes, from, now, deliver));
        }
        if event.is_writable() {
            self.listeners[k].on_writable();
        }
        self.pump(registry, k, now, false);
        if event.is_writable() {
            self.listeners[k].disarm(registry);
        }
    }

    pub(super) fn tick_interval(&self) -> Duration {
        self.config.udp.min_rto / 2
    }

    fn drain_unsubscribed<F>(&mut self, deliver: &mut F) -> bool
    where
        F: FnMut(PublisherEvent<'_>),
    {
        let had = !self.unsubscribed.is_empty();
        for (token, peer_addr) in self.unsubscribed.drain(..) {
            deliver(PublisherEvent::Unsubscribed { token, peer_addr });
        }
        had
    }

    pub(super) fn pre_poll<F>(&mut self, registry: &Registry, deliver: &mut F) -> bool
    where
        F: FnMut(PublisherEvent<'_>),
    {
        let work = self.drain_unsubscribed(deliver);
        let now = Instant::now();
        if now >= self.next_tick {
            self.next_tick = now + self.tick_interval();
            self.tick(registry, now);
        }
        work
    }

    pub(super) fn post_poll<F>(&mut self, deliver: &mut F) -> bool
    where
        F: FnMut(PublisherEvent<'_>),
    {
        self.drain_unsubscribed(deliver)
    }
}
