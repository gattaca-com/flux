use std::{io, net::SocketAddr};

use flux_communication::Timer;
use flux_timing::{Duration, Instant, Nanos};
use mio::{Registry, Token, event::Event as MioEvent};
use tracing::{debug, warn};

use super::{
    SubscriberEvent, TokenRange,
    wire::{self, ACK_HEADER, Datagram, HELLO_SIZE},
};
use crate::{
    network::UdpGroupConfig,
    udp::{
        SendOutcome, Socket, inbound::Inbound, latency_timer, new_session, recv_all, send_datagram,
        sys::RecvBatch, unspecified, wire::Packet,
    },
};

struct Subscription {
    addr: SocketAddr,
    /// Its token is the subscription's.
    socket: Socket,
    reliable: bool,
    heartbeat_interval: Duration,
    initial_rto: Duration,
    max_rto: Duration,
    connected: bool,
    session: u32,
    publisher: Option<u32>,
    inbound: Inbound,
    last_send: Instant,
    hello_due: Instant,
    hello_backoff: u8,
    ack_due: bool,
    latency: Option<Timer>,
    ctrl: Vec<u8>,
}

impl Subscription {
    fn new(addr: SocketAddr, socket: Socket, config: &UdpGroupConfig) -> Self {
        let udp = config.udp;
        let inbound = Inbound::new(
            udp.recv_window,
            udp.max_datagram_size,
            ACK_HEADER,
            udp.max_message_size,
            !udp.reliable,
        );
        let on_connect_len = config.on_connect_msg.as_ref().map_or(0, Vec::len);
        let ctrl_len = (ACK_HEADER + inbound.bitmap_capacity()).max(HELLO_SIZE + on_connect_len);
        let token = socket.token;
        let label = format!("{}_{}_{addr}", config.name, token.0);
        Self {
            addr,
            socket,
            reliable: udp.reliable,
            heartbeat_interval: udp.heartbeat_interval,
            initial_rto: udp.initial_rto,
            max_rto: udp.max_rto,
            connected: false,
            session: new_session(token.0),
            publisher: None,
            inbound,
            last_send: Instant::now(),
            hello_due: Instant::ZERO,
            hello_backoff: 0,
            ack_due: false,
            latency: latency_timer(config.telemetry, &label),
            ctrl: vec![0; ctrl_len],
        }
    }

    #[inline]
    fn token(&self) -> Token {
        self.socket.token
    }

    fn reset(&mut self) {
        self.connected = false;
        self.publisher = None;
        self.session = new_session(self.token().0);
        self.hello_due = Instant::ZERO;
        self.hello_backoff = 0;
        self.ack_due = false;
    }

    fn send_hello(&mut self, msg: &[u8], now: Instant) -> SendOutcome {
        let interval = (self.initial_rto.0 << self.hello_backoff.min(6)).min(self.max_rto.0);
        self.hello_due = now + Duration(interval);
        self.hello_backoff = self.hello_backoff.saturating_add(1);
        let n = wire::hello(&mut self.ctrl, self.session, msg);
        self.last_send = now;
        send_datagram(&self.socket.udp, self.addr, &self.ctrl[..n])
    }

    fn on_hello_ack(&mut self, publisher: u32, subscriber: u32, join: u64) -> bool {
        if self.connected || subscriber != self.session {
            return false;
        }
        self.publisher = Some(publisher);
        self.inbound.reset(join);
        self.connected = true;
        self.hello_backoff = 0;
        true
    }

    #[inline]
    fn lost(&self, subscriber: u32, publisher: Option<u32>) -> bool {
        subscriber == self.session &&
            publisher.is_none_or(|s| self.connected && self.publisher != Some(s))
    }

    fn on_packet(&mut self, packet: Packet<'_>, deliver: &mut dyn FnMut(&[u8], Nanos)) {
        if !self.connected || self.publisher != Some(packet.session) {
            return;
        }
        let latency = &mut self.latency;
        let received = self.inbound.receive(packet, &mut |bytes, send_ts| {
            if let Some(t) = latency {
                t.emit_latency_from_nanos(Nanos(send_ts), Nanos::now());
            }
            deliver(bytes, Nanos(send_ts));
        });
        match received {
            Ok(()) => self.ack_due |= self.reliable,
            Err(why) => debug!(%self.addr, ?why, seq = packet.seq, "udp packet refused"),
        }
    }

    fn send_ack(&mut self, now: Instant) -> SendOutcome {
        let Some(publisher) = self.publisher else { return SendOutcome::Done };
        let n_bits = self.inbound.ack_bits();
        let header: &mut [u8; ACK_HEADER] = (&mut self.ctrl[..ACK_HEADER]).try_into().unwrap();
        wire::ack_header(header, publisher, self.session, self.inbound.ack_next());
        let n = self.inbound.write_bitmap(n_bits, &mut self.ctrl[ACK_HEADER..]);
        self.ack_due = false;
        self.last_send = now;
        send_datagram(&self.socket.udp, self.addr, &self.ctrl[..ACK_HEADER + n])
    }

    fn tick(&mut self, msg: &[u8], now: Instant) -> SendOutcome {
        if !self.connected {
            if now >= self.hello_due {
                return self.send_hello(msg, now);
            }
            return SendOutcome::Done;
        }
        if self.ack_due || now.saturating_sub(self.last_send) >= self.heartbeat_interval {
            return self.send_ack(now);
        }
        SendOutcome::Done
    }
}

pub(super) struct SubscriberState {
    config: UdpGroupConfig,
    pub(super) tokens: TokenRange,
    subscriptions: Vec<Subscription>,
    unsubscribed: Vec<(Token, SocketAddr)>,
    next_tick: Instant,
}

impl SubscriberState {
    pub(super) fn new(config: UdpGroupConfig, tokens: TokenRange) -> Self {
        crate::udp::validate_group(&config, HELLO_SIZE);
        Self {
            config,
            tokens,
            subscriptions: Vec::new(),
            unsubscribed: Vec::new(),
            next_tick: Instant::ZERO,
        }
    }

    pub(super) fn close_all(&mut self, registry: &Registry) {
        for subscription in &mut self.subscriptions {
            subscription.socket.close(registry);
        }
    }

    pub(super) fn subscribe(&mut self, registry: &Registry, addr: SocketAddr) -> io::Result<Token> {
        let token = self.tokens.allocate();
        let socket =
            Socket::open(registry, unspecified(addr), token, None, self.config.socket_buf_size)?;
        let mut subscription = Subscription::new(addr, socket, &self.config);
        let msg = self.config.on_connect_msg.as_deref().unwrap_or_default();
        subscription.send_hello(msg, Instant::now());
        self.subscriptions.push(subscription);
        Ok(token)
    }

    pub(super) fn is_subscribed(&self, token: Token) -> bool {
        self.subscriptions.iter().any(|s| s.token() == token && s.connected)
    }

    fn restart(&mut self, i: usize, now: Instant) {
        let subscription = &mut self.subscriptions[i];
        if subscription.connected {
            self.unsubscribed.push((subscription.token(), subscription.addr));
        }
        subscription.reset();
        let msg = self.config.on_connect_msg.as_deref().unwrap_or_default();
        subscription.send_hello(msg, now);
    }

    pub(super) fn resubscribe(&mut self, token: Token) -> bool {
        let Some(i) = self.subscriptions.iter().position(|s| s.token() == token) else {
            return false;
        };
        self.restart(i, Instant::now());
        true
    }

    pub(super) fn unsubscribe(&mut self, registry: &Registry, token: Token) -> bool {
        let Some(i) = self.subscriptions.iter().position(|s| s.token() == token) else {
            return false;
        };
        self.subscriptions.swap_remove(i).socket.close(registry);
        self.unsubscribed.retain(|(t, _)| *t != token);
        true
    }

    fn tick(&mut self, registry: &Registry, now: Instant) {
        let msg = self.config.on_connect_msg.as_deref().unwrap_or_default();
        for subscription in &mut self.subscriptions {
            if subscription.tick(msg, now) == SendOutcome::WouldBlock {
                subscription.socket.arm_writable(registry);
            }
        }
    }

    fn on_datagram<F>(
        &mut self,
        i: usize,
        bytes: &[u8],
        from: SocketAddr,
        now: Instant,
        deliver: &mut F,
    ) where
        F: FnMut(SubscriberEvent<'_>),
    {
        let Some(datagram) = wire::decode(bytes) else { return };
        if self.subscriptions[i].addr != from {
            return;
        }
        let token = self.subscriptions[i].token();
        match datagram {
            Datagram::Packet(packet) => {
                self.subscriptions[i].on_packet(packet, &mut |payload, send_ts| {
                    deliver(SubscriberEvent::Message { token, payload, send_ts });
                });
            }
            Datagram::HelloAck { publisher, subscriber, join, msg } => {
                if !crate::udp::on_connect_fits(&self.config, msg, from) {
                    return;
                }
                if self.subscriptions[i].lost(subscriber, Some(publisher)) {
                    warn!(addr = %from, "udp publisher lost our subscription, resubscribing");
                    self.restart(i, now);
                } else if self.subscriptions[i].on_hello_ack(publisher, subscriber, join) {
                    // A resubscribe can complete before post_poll drains the old unsubscribe.
                    self.drain_unsubscribed(deliver);
                    debug!(addr = %from, "udp subscribed");
                    deliver(SubscriberEvent::Subscribed { token, peer_addr: from, msg });
                }
            }
            Datagram::Reset { subscriber } => {
                if self.subscriptions[i].lost(subscriber, None) {
                    warn!(addr = %from, "udp publisher lost our subscription, resubscribing");
                    self.restart(i, now);
                }
            }
            Datagram::Ack { .. } | Datagram::Hello { .. } => {}
        }
    }

    pub(super) fn handle_event<F>(
        &mut self,
        recv: &mut RecvBatch,
        registry: &Registry,
        event: &MioEvent,
        deliver: &mut F,
    ) where
        F: FnMut(SubscriberEvent<'_>),
    {
        let Some(i) = self.subscriptions.iter().position(|s| s.token() == event.token()) else {
            return;
        };
        let now = Instant::now();
        if event.is_readable() {
            let fd = self.subscriptions[i].socket.fd();
            recv_all(fd, recv, |bytes, from| self.on_datagram(i, bytes, from, now, deliver));
        }
        let subscription = &mut self.subscriptions[i];
        if event.is_writable() {
            subscription.socket.on_writable();
        }
        if std::mem::take(&mut subscription.ack_due) &&
            subscription.send_ack(now) == SendOutcome::WouldBlock
        {
            subscription.socket.arm_writable(registry);
        }
        if event.is_writable() {
            subscription.socket.disarm(registry);
        }
    }

    pub(super) fn tick_interval(&self) -> Duration {
        self.config.udp.min_rto / 2
    }

    fn drain_unsubscribed<F>(&mut self, deliver: &mut F) -> bool
    where
        F: FnMut(SubscriberEvent<'_>),
    {
        let had = !self.unsubscribed.is_empty();
        for (token, peer_addr) in self.unsubscribed.drain(..) {
            deliver(SubscriberEvent::Unsubscribed { token, peer_addr });
        }
        had
    }

    pub(super) fn pre_poll<F>(&mut self, registry: &Registry, deliver: &mut F) -> bool
    where
        F: FnMut(SubscriberEvent<'_>),
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
        F: FnMut(SubscriberEvent<'_>),
    {
        self.drain_unsubscribed(deliver)
    }
}
