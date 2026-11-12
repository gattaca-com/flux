use std::{
    collections::{VecDeque, hash_map::RandomState},
    hash::BuildHasher,
    io,
    net::SocketAddr,
};

use flux_timing::{Duration, Instant};
use flux_utils::{DCache, safe_assert};
use mio::{Registry, Token, event::Event as MioEvent};
use tracing::{debug, error, info, warn};

use super::wire::{self, Datagram, HELLO_ACK_SIZE, RESET_SIZE};
use crate::{
    network::{Event, Group, PayloadBuf, RxPayload, Tokens, UdpGroupConfigV2},
    udp_v2::{
        Socket, deliver_on_connect, new_session,
        outbound::Outbound,
        recv_all, send_datagram,
        stream::Stream,
        sys::{RecvBatch, SendBatch},
        telemetry::{PeerId, Telemetry, UdpRtt},
        wire::MESSAGE_HEADER,
    },
};

/// A real subscriber echoes its challenge a round trip after its hello, so
/// forged hellos evict it only by outnumbering this within that round trip.
const MAX_PENDING: usize = 1024;
/// Set in every challenge and never in a ping's timestamp, a clock reading
/// that takes decades to reach it, so an echoed challenge is never taken for
/// a send time.
const CHALLENGE_BIT: u64 = 1 << 63;

/// A subscriber whose address is unproven. It holds the stream from `join`
/// but gets none of it, nor a token, until an ack echoes `challenge`.
struct Pending {
    listener: Token,
    addr: SocketAddr,
    session: u32,
    join: u64,
    challenge: u64,
    sent_at: Instant,
    /// Expiry counts from here, so hello retries cannot keep it alive.
    created: Instant,
    /// Its on-connect message, delivered once it is `Accepted`.
    msg: Vec<u8>,
}

struct Recipient {
    addr: SocketAddr,
    token: Token,
    listener: Token,
    session: u32,
    join: u64,
    last_recv: Instant,
    last_ping: Instant,
    close_at: Option<u64>,
    out: Outbound,
}

impl Recipient {
    /// Coarse, so round-trip jitter doesn't reorder subscribers: colocated,
    /// regional, far, then unmeasured.
    fn tier(&self) -> u8 {
        match self.out.srtt() {
            Some(rtt) if rtt.0 < Duration::from_millis(1).0 => 0,
            Some(rtt) if rtt.0 < Duration::from_millis(20).0 => 1,
            Some(_) => 2,
            None => 3,
        }
    }

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

    fn sample_ping_rtt(&mut self, sent_at: Instant, now: Instant, telemetry: &mut Telemetry) {
        self.out.sample_rtt(now.saturating_sub(sent_at));
        telemetry.rtt(&UdpRtt {
            peer: PeerId::new(self.token, self.addr),
            sent_at,
            received_at: now,
            ping: true,
        });
    }
}

struct Staged {
    recipient: usize,
    packets: usize,
}

pub(super) struct PublisherState {
    config: UdpGroupConfigV2,
    group: Group,
    listeners: Vec<Socket>,
    stream: Stream,
    recipients: Vec<Recipient>,
    /// Oldest first.
    pending: VecDeque<Pending>,
    challenge_key: RandomState,
    challenges: u64,
    /// A late hello retry from a dropped subscriber gets a reset instead of
    /// rejoining.
    dropped: Vec<(SocketAddr, u32, Instant)>,
    send_buffer: Vec<u8>,
    ctrl: Vec<u8>,
    batch: SendBatch,
    // entries here correspond to `SendBatch` order
    staged: Vec<Staged>,
    unsubscribed: Vec<(Token, SocketAddr)>,
    /// Dropped subscribers' tokens, retired once the poll's events are out.
    retired: Vec<Token>,
    telemetry: Telemetry,
    next_tick: Instant,
    next_rotation: Instant,
}

fn on_connect_msg(config: &UdpGroupConfigV2) -> &[u8] {
    config.on_connect_msg.as_deref().unwrap_or_default()
}

impl PublisherState {
    pub(super) fn new(config: UdpGroupConfigV2, group: Group) -> Self {
        let udp = config.udp;
        crate::udp_v2::validate_group(&config, HELLO_ACK_SIZE);
        Self {
            ctrl: vec![0; HELLO_ACK_SIZE + on_connect_msg(&config).len()],
            stream: Stream::new(&udp, wire::PACKET, new_session(0)),
            batch: SendBatch::new(udp.max_datagram_size),
            telemetry: Telemetry::new(config.telemetry, config.name),
            config,
            group,
            listeners: Vec::new(),
            recipients: Vec::new(),
            pending: VecDeque::new(),
            challenge_key: RandomState::new(),
            challenges: 0,
            dropped: Vec::new(),
            send_buffer: Vec::with_capacity(32 * 1024),
            staged: Vec::new(),
            unsubscribed: Vec::new(),
            retired: Vec::new(),
            next_tick: Instant::ZERO,
            next_rotation: Instant::ZERO,
        }
    }

    pub(super) fn close_all(&mut self, registry: &Registry) {
        for listener in &mut self.listeners {
            listener.close(registry);
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.listeners.is_empty()
    }

    pub(super) fn listen(
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
                self.listeners.push(socket);
                Ok(token)
            }
            Err(err) => {
                tokens.retire(token);
                Err(err)
            }
        }
    }

    fn release(&mut self) {
        let base = self
            .recipients
            .iter()
            .map(Recipient::base)
            .chain(self.pending.iter().map(|p| p.join))
            .min()
            .unwrap_or_else(|| self.stream.next());
        if base > self.stream.base() {
            self.stream.release_to(base);
        }
    }

    /// Dropping a pending subscriber is silent: it was never `Accepted`.
    fn retain_pending(&mut self, keep: impl FnMut(&Pending) -> bool) {
        let len = self.pending.len();
        self.pending.retain(keep);
        if self.pending.len() != len {
            self.release();
        }
    }

    fn remove_recipient(&mut self, i: usize) {
        let recipient = self.recipients.swap_remove(i);
        self.retired.push(recipient.token);
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
        // Newer messages must not join a packet it still gets.
        self.stream.seal();
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
        self.retain_pending(|p| p.listener != token);
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
        self.recipients.iter().any(|r| !r.is_draining()) || !self.pending.is_empty()
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
        if !self.pending.is_empty() {
            self.retain_pending(|p| p.join >= min_base);
        }
        let recipients = self.recipients.iter().filter(|r| !r.is_draining()).count();
        // Also, enqueued with no one holding it, it would never be released.
        if recipients == 0 && self.pending.is_empty() {
            self.send_buffer.clear();
            return 0;
        }
        self.stream.enqueue(&mut self.send_buffer);
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
                let recipient = &mut self.recipients[i];
                let until = recipient.close_at.unwrap_or(u64::MAX);
                let packets =
                    recipient.out.stage(&mut self.stream, &mut self.batch, probes, until, now);
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
        // A pump stages one recipient's packets after another on one socket,
        // so in a burst the last recipient waits for all the others'. Nearest
        // tier first keeps a colocated subscriber from queueing behind far
        // ones, whose own path dwarfs the wait. Rotating before the stable
        // sort moves the first recipient to the back of its tier, so peers
        // in a tier take turns going first.
        if now >= self.next_rotation && !self.recipients.is_empty() {
            self.next_rotation = now + Duration::from_secs(1);
            self.recipients.rotate_left(1);
        }
        if !self.recipients.is_sorted_by_key(Recipient::tier) {
            self.recipients.sort_by_key(Recipient::tier);
        }
        if !self.pending.is_empty() {
            self.tick_pending(now);
        }
        self.ping(now);
        for k in 0..self.listeners.len() {
            self.pump(registry, k, now, true);
        }
    }

    fn ping(&mut self, now: Instant) {
        let heartbeat = self.config.udp.heartbeat_interval;
        let ping = wire::ping(self.stream.session(), now.0);
        for recipient in &mut self.recipients {
            if now.saturating_sub(recipient.last_ping) < heartbeat {
                continue;
            }
            let Some(listener) = self.listeners.iter().find(|l| l.token == recipient.listener)
            else {
                continue;
            };
            recipient.last_ping = now;
            send_datagram(&listener.udp, recipient.addr, &ping);
        }
    }

    /// Resends each challenge at the ping interval: the subscriber stops
    /// sending hellos once the hello ack arrives, so a lost challenge or echo
    /// is only recovered from here.
    fn tick_pending(&mut self, now: Instant) {
        let peer_timeout = self.config.peer_timeout;
        self.retain_pending(|p| now.saturating_sub(p.created) < peer_timeout);
        let heartbeat = self.config.udp.heartbeat_interval;
        let session = self.stream.session();
        for p in &mut self.pending {
            if now.saturating_sub(p.sent_at) < heartbeat {
                continue;
            }
            let Some(listener) = self.listeners.iter().find(|l| l.token == p.listener) else {
                continue;
            };
            p.sent_at = now;
            send_datagram(&listener.udp, p.addr, &wire::ping(session, p.challenge));
        }
    }

    fn send_reset(&self, k: usize, to: SocketAddr, subscriber: u32) {
        let mut buf = [0; RESET_SIZE];
        wire::reset(&mut buf, subscriber);
        send_datagram(&self.listeners[k].udp, to, &buf);
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
        let listener = self.listeners[k].token;
        let index = self.recipients.iter().position(|r| r.listener == listener && r.addr == from);
        match datagram {
            Datagram::Ack { publisher, subscriber, ack_next, echo_ts, bitmap } => {
                let verified = index.filter(|&i| {
                    publisher == self.stream.session() && self.recipients[i].session == subscriber
                });
                let i = if let Some(i) = verified {
                    i
                } else {
                    let session = self.stream.session();
                    let Some(j) = self.pending.iter().position(|p| {
                        p.listener == listener &&
                            p.addr == from &&
                            p.session == subscriber &&
                            p.challenge == echo_ts &&
                            publisher == session
                    }) else {
                        // A pending subscriber acks before its challenge
                        // arrives, and a reset would make it start over.
                        if !self.pending.iter().any(|p| p.listener == listener && p.addr == from) {
                            self.send_reset(k, from, subscriber);
                        }
                        return;
                    };
                    let Some(i) = self.promote(j, now, tokens, dcache, deliver) else {
                        self.send_reset(k, from, subscriber);
                        return;
                    };
                    i
                };
                let recipient = &mut self.recipients[i];
                recipient.last_recv = now;
                if (1..CHALLENGE_BIT).contains(&echo_ts) {
                    recipient.sample_ping_rtt(Instant(echo_ts), now, &mut self.telemetry);
                }
                let held_oldest = recipient.base() == self.stream.base();
                recipient.out.on_ack(ack_next, bitmap, now, &mut self.telemetry);
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
                if let Some(i) = index &&
                    self.recipients[i].session == subscriber
                {
                    // A retry: the hello ack was lost or is still on its way.
                    self.recipients[i].last_recv = now;
                    let join = self.recipients[i].join;
                    self.send_hello_ack(k, from, subscriber, join);
                    return;
                }
                self.on_hello(k, from, subscriber, msg, now, tokens);
            }
            Datagram::Packet(_) |
            Datagram::HelloAck { .. } |
            Datagram::Reset { .. } |
            Datagram::Ping { .. } => {}
        }
    }

    /// A hello from an address not yet proven to receive: it is answered with
    /// the hello ack and a challenge ping, whose echo `promote`s it.
    fn on_hello(
        &mut self,
        k: usize,
        from: SocketAddr,
        subscriber: u32,
        msg: &[u8],
        now: Instant,
        tokens: &Tokens,
    ) {
        let listener = self.listeners[k].token;
        if let Some(p) = self
            .pending
            .iter_mut()
            .find(|p| p.listener == listener && p.addr == from && p.session == subscriber)
        {
            p.sent_at = now;
            let (join, challenge) = (p.join, p.challenge);
            self.send_challenge(k, from, subscriber, join, challenge);
            return;
        }
        // Any sender can open sessions, so running out refuses one instead
        // of panicking. Refused now, before the hello ack tells it it joined.
        if !tokens.can_allocate() {
            warn!(addr = %from, "network token range exhausted, subscriber refused");
            return;
        }
        let evicted =
            if self.pending.len() >= MAX_PENDING { self.pending.pop_front() } else { None };
        let release = evicted.is_some();
        let mut buf = evicted.map(|p| p.msg).unwrap_or_default();
        buf.clear();
        buf.extend_from_slice(msg);
        // The open packet may already be on its way to others.
        self.stream.seal();
        let join = self.stream.next();
        self.challenges += 1;
        let challenge = self.challenge_key.hash_one(self.challenges) | CHALLENGE_BIT;
        self.pending.push_back(Pending {
            listener,
            addr: from,
            session: subscriber,
            join,
            challenge,
            sent_at: now,
            created: now,
            msg: buf,
        });
        if release {
            self.release();
        }
        self.send_challenge(k, from, subscriber, join, challenge);
    }

    /// Its address is proven: it joins at the `join` its hello ack named and
    /// takes over from an older session at that address.
    fn promote<F>(
        &mut self,
        j: usize,
        now: Instant,
        tokens: &mut Tokens,
        dcache: Option<&DCache>,
        deliver: &mut F,
    ) -> Option<usize>
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let group = self.group;
        let p = self.pending.remove(j)?;
        let Some(token) = tokens.try_allocate(group) else {
            warn!(addr = %p.addr, "network token range exhausted, subscriber refused");
            self.release();
            return None;
        };
        let mut recipient = Recipient {
            addr: p.addr,
            token,
            listener: p.listener,
            session: p.session,
            join: p.join,
            last_recv: now,
            last_ping: now,
            close_at: None,
            out: Outbound::new(p.addr, p.join, &self.config.udp, PeerId::new(token, p.addr)),
        };
        // The echo of a resent challenge may answer any copy of it.
        if p.sent_at == p.created {
            recipient.sample_ping_rtt(p.sent_at, now, &mut self.telemetry);
        }
        let old = self.recipients.iter().position(|r| r.listener == p.listener && r.addr == p.addr);
        // Pushed before the old one goes, so the release that follows keeps
        // the stream from `join`.
        self.recipients.push(recipient);
        let mut i = self.recipients.len() - 1;
        if let Some(old) = old {
            let old_token = self.recipients[old].token;
            self.remove_recipient(old);
            i = old;
            deliver(Event::Disconnected { group, token: old_token, peer_addr: p.addr });
        }
        info!(addr = %p.addr, "udp subscriber joined");
        deliver(Event::Accepted { group, token, peer_addr: p.addr });
        deliver_on_connect(group, token, &p.msg, dcache, deliver);
        Some(i)
    }

    fn send_challenge(
        &mut self,
        k: usize,
        to: SocketAddr,
        subscriber: u32,
        join: u64,
        challenge: u64,
    ) {
        self.send_hello_ack(k, to, subscriber, join);
        send_datagram(&self.listeners[k].udp, to, &wire::ping(self.stream.session(), challenge));
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
        tokens: &mut Tokens,
        dcache: Option<&DCache>,
        deliver: &mut F,
    ) where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let Some(k) = self.listeners.iter().position(|s| s.token == event.token()) else { return };
        let now = Instant::now();
        if event.is_readable() {
            let fd = self.listeners[k].fd();
            recv_all(fd, recv, |bytes, from| {
                self.on_datagram(k, bytes, from, now, tokens, dcache, deliver);
            });
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
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let had = !self.unsubscribed.is_empty();
        let group = self.group;
        for (token, peer_addr) in self.unsubscribed.drain(..) {
            deliver(Event::Disconnected { group, token, peer_addr });
        }
        had
    }

    pub(super) fn pre_poll<F>(&mut self, registry: &Registry, deliver: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let work = self.drain_unsubscribed(deliver);
        let now = Instant::now();
        if now >= self.next_tick {
            self.next_tick = now + self.tick_interval();
            self.tick(registry, now);
        }
        work
    }

    pub(super) fn post_poll<F>(&mut self, tokens: &mut Tokens, deliver: &mut F) -> bool
    where
        F: for<'a> FnMut(Event<RxPayload<'a>>),
    {
        let work = self.drain_unsubscribed(deliver);
        for token in self.retired.drain(..) {
            tokens.retire(token);
        }
        work
    }
}
