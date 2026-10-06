//! One remote endpoint: its session with us, what we owe it and what it
//! owes us. The manager owns the sockets and passes them in.

use std::{io, net::SocketAddr};

use flux_communication::Timer;
use flux_timing::{Duration, Instant, Nanos};
use flux_utils::DCache;
use mio::{Token, net::UdpSocket};
use tracing::{debug, warn};

use super::{
    inbound::Inbound,
    outbound::{Full, MessageStore, Outbound},
    sys::{SendBatch, SockAddr},
    wire::{self, ACK_HEADER, HELLO_ACK_SIZE, HELLO_SIZE, Packet, RESET_SIZE, Record, Records},
};
use crate::{
    NetworkTelemetry,
    network::{RxPayload, UdpGroupConfig},
};

const WARN_INTERVAL_SECS: u64 = 5;

/// Result of a socket write. Errors other than `WouldBlock` are logged and
/// treated as sent: the retransmit path tries again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SendOutcome {
    Done,
    WouldBlock,
}

#[inline]
fn send_datagram(socket: &UdpSocket, addr: SocketAddr, bytes: &[u8]) -> SendOutcome {
    match socket.send_to(bytes, addr) {
        Ok(_) => SendOutcome::Done,
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => SendOutcome::WouldBlock,
        Err(e) => {
            debug!(?e, %addr, "udp send failed");
            SendOutcome::Done
        }
    }
}

fn telemetry_label(group_name: &str, token: Token, peer: SocketAddr) -> String {
    format!("{group_name}_{}_{peer}", token.0)
}

fn latency_timer(telemetry: NetworkTelemetry, label: &str) -> Option<Timer> {
    let NetworkTelemetry::Enabled { app_name } = telemetry else { return None };
    Some(Timer::new(app_name, format!("udp_latency_{label}")))
}

/// Why a packet was not taken in, leaving its sequence unacked so the
/// sender's backlog policy surfaces the mismatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refused {
    Malformed,
    TooLarge,
    ExceedsDcache,
}

// Session flags are independent; packing them would only obscure the state.
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct Peer {
    pub(crate) addr: SocketAddr,
    pub(crate) token: Token,
    /// Socket this peer sends and receives on: its own token for outbound
    /// peers, the listener's for accepted ones.
    pub(crate) socket_token: Token,
    pub(crate) native_addr: SockAddr,
    max_message_size: usize,
    reliable: bool,
    heartbeat_interval: Duration,
    max_rto: Duration,
    connected: bool,
    local_session: u32,
    remote_session: Option<u32>,
    pub(crate) outbound: Outbound,
    inbound: Inbound,
    last_recv: Instant,
    last_send: Instant,
    /// When the next hello goes out while disconnected.
    hello_due: Instant,
    hello_backoff: u8,
    ack_due: bool,
    /// Drop the session once every packet is acked; sends are refused.
    close_when_drained: bool,
    latency: Option<Timer>,
    dropped_full: u64,
    last_warn: Instant,
    /// Control datagram staging: the largest ack.
    ctrl: Vec<u8>,
}

impl Peer {
    pub(crate) fn new(
        addr: SocketAddr,
        token: Token,
        socket_token: Token,
        local_session: u32,
        group: &UdpGroupConfig,
    ) -> Self {
        let now = Instant::now();
        let udp = group.udp;
        let label = telemetry_label(group.name, token, addr);
        let inbound = Inbound::new(udp.recv_window, udp.max_datagram_size, !udp.reliable);
        Self {
            addr,
            token,
            socket_token,
            native_addr: SockAddr::new(addr),
            max_message_size: udp.max_message_size,
            reliable: udp.reliable,
            heartbeat_interval: udp.heartbeat_interval,
            max_rto: udp.max_rto,
            connected: false,
            local_session,
            remote_session: None,
            outbound: Outbound::new(group, &label),
            ctrl: vec![0; ACK_HEADER + inbound.bitmap_capacity()],
            inbound,
            last_recv: now,
            last_send: now,
            hello_due: Instant::ZERO,
            hello_backoff: 0,
            ack_due: false,
            close_when_drained: false,
            latency: latency_timer(group.telemetry, &label),
            dropped_full: 0,
            last_warn: Instant::ZERO,
        }
    }

    #[inline]
    pub(crate) fn is_connected(&self) -> bool {
        self.connected
    }

    /// Outbound peers own their socket under their own token.
    #[inline]
    pub(crate) fn is_outbound(&self) -> bool {
        self.socket_token == self.token
    }

    #[inline]
    pub(crate) fn take_ack_due(&mut self) -> bool {
        std::mem::take(&mut self.ack_due)
    }

    /// Whether a graceful close was requested and is still pending.
    #[inline]
    pub(crate) fn is_draining(&self) -> bool {
        self.close_when_drained
    }

    /// Requests a close once every queued packet is acked. Returns whether
    /// that is already the case.
    pub(crate) fn close_when_drained(&mut self) -> bool {
        self.close_when_drained = true;
        self.drained()
    }

    /// Whether a requested graceful close can happen now.
    #[inline]
    pub(crate) fn drained(&self) -> bool {
        self.close_when_drained && self.outbound.inflight() == 0
    }

    /// Queues the message in store `slot`. On `Full::Window` the peer is
    /// unusable and must be dropped.
    pub(crate) fn enqueue(
        &mut self,
        store: &mut MessageStore,
        slot: u32,
        now: Instant,
    ) -> Result<(), Full> {
        let result = self.outbound.enqueue(store, slot);
        match result {
            Err(Full::TooLarge) => {
                let len = store.bytes(slot).len();
                warn!(%self.addr, len, max = self.max_message_size, "udp message too large");
            }
            Err(Full::Window) => {
                self.dropped_full += 1;
                if now.saturating_sub(self.last_warn) >= Duration::from_secs(WARN_INTERVAL_SECS) {
                    warn!(
                        %self.addr,
                        dropped = self.dropped_full,
                        inflight = self.outbound.inflight(),
                        "udp send window full"
                    );
                    self.last_warn = now;
                }
            }
            Ok(()) => {}
        }
        result
    }

    /// Puts due packets into `batch`; see [`Outbound::stage`]. Nothing goes
    /// out before the session is up.
    pub(crate) fn stage(
        &mut self,
        store: &MessageStore,
        batch: &mut SendBatch,
        probes: u64,
        now: Instant,
    ) -> usize {
        if !self.connected {
            return 0;
        }
        self.outbound.stage(store, batch, &self.native_addr, self.local_session, probes, now)
    }

    /// Records what the kernel took of the last staging.
    pub(crate) fn commit(&mut self, store: &mut MessageStore, accepted: usize, now: Instant) {
        if accepted != 0 {
            self.last_send = now;
        }
        self.outbound.commit(accepted, now);
        if !self.reliable {
            self.outbound.forget_sent(store);
        }
    }

    /// Drops the session and adopts `new_session` so the remote sees a fresh
    /// peer. Queued packets are kept for the next session unless
    /// `drop_backlog`; hello retries restart on the next tick.
    pub(crate) fn mark_disconnected(
        &mut self,
        drop_backlog: bool,
        new_session: u32,
        store: &mut MessageStore,
    ) {
        self.connected = false;
        self.close_when_drained = false;
        self.remote_session = None;
        self.local_session = new_session;
        if drop_backlog {
            self.outbound.clear(store);
        } else {
            self.outbound.rewind();
        }
        self.hello_due = Instant::ZERO;
        self.hello_backoff = 0;
    }

    /// Tells the sender of a datagram under `their_session` that the listener
    /// holds no peer for it.
    pub(crate) fn send_reset(socket: &UdpSocket, addr: SocketAddr, their_session: u32) {
        let mut buf = [0; RESET_SIZE];
        wire::reset(&mut buf, their_session);
        send_datagram(socket, addr, &buf);
    }

    /// Also schedules the retry, backing off per attempt up to `max_rto`.
    pub(crate) fn send_hello(&mut self, socket: &UdpSocket, now: Instant) -> SendOutcome {
        let rto = self.outbound.rto().current();
        let interval = (rto.0 << self.hello_backoff.min(6)).min(self.max_rto.0);
        self.hello_due = now + Duration(interval);
        self.hello_backoff = self.hello_backoff.saturating_add(1);
        let mut buf = [0; HELLO_SIZE];
        wire::hello(&mut buf, self.local_session, self.outbound.base());
        self.last_send = now;
        send_datagram(socket, self.addr, &buf)
    }

    pub(crate) fn send_ack(&mut self, socket: &UdpSocket, now: Instant) -> SendOutcome {
        let n_bits = self.inbound.ack_bits();
        let header: &mut [u8; ACK_HEADER] = (&mut self.ctrl[..ACK_HEADER]).try_into().unwrap();
        wire::ack_header(header, self.local_session, self.inbound.ack_next(), n_bits as u16);
        let n = self.inbound.write_bitmap(n_bits, &mut self.ctrl[ACK_HEADER..]);
        self.ack_due = false;
        self.last_send = now;
        send_datagram(socket, self.addr, &self.ctrl[..ACK_HEADER + n])
    }

    /// Accepted side. Establishes the session and replies with a hello ack.
    /// `false` when the hello carries a different session than the one we
    /// hold: the remote restarted and this peer must be replaced.
    pub(crate) fn on_hello(
        &mut self,
        session: u32,
        base: u64,
        socket: &UdpSocket,
        now: Instant,
    ) -> bool {
        match self.remote_session {
            Some(s) if s != session => return false,
            Some(_) => {}
            None => {
                self.remote_session = Some(session);
                self.inbound.reset(base);
                self.connected = true;
            }
        }
        self.last_recv = now;
        let mut buf = [0; HELLO_ACK_SIZE];
        wire::hello_ack(&mut buf, self.local_session, session, self.outbound.base());
        self.last_send = now;
        send_datagram(socket, self.addr, &buf);
        true
    }

    /// Initiating side. `true` when this completes a handshake; `false` if
    /// already connected or the ack answers an earlier attempt.
    pub(crate) fn on_hello_ack(
        &mut self,
        session: u32,
        their_session: u32,
        base: u64,
        now: Instant,
    ) -> bool {
        if self.connected || their_session != self.local_session {
            return false;
        }
        self.last_recv = now;
        self.remote_session = Some(session);
        self.inbound.reset(base);
        self.connected = true;
        self.hello_backoff = 0;
        true
    }

    /// A reset naming our session, or a hello ack naming it under a
    /// different remote session, means the listener lost our peer.
    #[inline]
    pub(crate) fn lost_by_remote(&self, their_session: u32, remote_session: Option<u32>) -> bool {
        self.connected &&
            their_session == self.local_session &&
            remote_session.is_none_or(|s| self.remote_session != Some(s))
    }

    /// Datagrams from any other session are stale: a restarted remote shows
    /// up through a hello or a reset, never through data.
    #[inline]
    fn current_session(&self, session: u32) -> bool {
        self.connected && self.remote_session == Some(session)
    }

    /// Takes in a packet, delivering every message it completes. Validation
    /// comes before the sequence is committed: a bad packet must not consume
    /// the sequence a good retry will carry.
    pub(crate) fn on_packet<F>(
        &mut self,
        packet: Packet<'_>,
        dcache: Option<&DCache>,
        now: Instant,
        deliver: &mut F,
    ) where
        F: FnMut(RxPayload<'_>, Nanos),
    {
        let Packet { session, seq, send_ts, records } = packet;
        if !self.current_session(session) {
            return;
        }
        self.last_recv = now;
        if let Err(why) = self.validate(records.clone(), dcache) {
            if why == Refused::ExceedsDcache {
                warn!(%self.addr, "udp message exceeds dcache capacity, not acked");
            } else {
                debug!(%self.addr, ?why, seq, "udp packet refused");
            }
            return;
        }
        if self.reliable {
            self.ack_due = true;
        }
        if !self.inbound.accept(seq) {
            return;
        }
        for record in records {
            match record {
                Record::Whole(bytes) => self.deliver(bytes, Nanos(send_ts), dcache, deliver),
                Record::Fragment { message, offset, total, bytes } => {
                    if let Some(done) =
                        self.inbound.fragment(seq, message, offset, total, bytes, send_ts)
                    {
                        self.deliver(done.bytes(), Nanos(done.send_ts()), dcache, deliver);
                        self.inbound.recycle(done);
                    }
                }
            }
        }
    }

    /// Checks every record of a packet before any is taken in. A packet
    /// cut short in transit shows up here as a record count mismatch.
    fn validate(&self, records: Records<'_>, dcache: Option<&DCache>) -> Result<(), Refused> {
        let mut left = records.len();
        for record in records {
            left -= 1;
            let len = match record {
                Record::Whole(bytes) => bytes.len(),
                Record::Fragment { total, .. } => total as usize,
            };
            if len > self.max_message_size {
                return Err(Refused::TooLarge);
            }
            if dcache.is_some_and(|dc| len > dc.capacity()) {
                return Err(Refused::ExceedsDcache);
            }
        }
        if left != 0 { Err(Refused::Malformed) } else { Ok(()) }
    }

    #[inline]
    fn deliver<F>(&mut self, bytes: &[u8], send_ts: Nanos, dcache: Option<&DCache>, deliver: &mut F)
    where
        F: FnMut(RxPayload<'_>, Nanos),
    {
        if let Some(t) = &mut self.latency {
            t.emit_latency_from_nanos(send_ts, Nanos::now());
        }
        match dcache {
            None => deliver(RxPayload::Raw(bytes), send_ts),
            Some(dc) => match dc.write(bytes.len(), |buf| buf.copy_from_slice(bytes)) {
                Ok(dref) => deliver(RxPayload::DCache(dref), send_ts),
                Err(e) => warn!("dcache write failed: {e}"),
            },
        }
    }

    /// Takes in an ack. Unreliable: the ack is a liveness signal only, its
    /// contents are not trusted to drive any send.
    pub(crate) fn on_ack(
        &mut self,
        session: u32,
        ack_next: u64,
        bits: u16,
        bitmap: &[u8],
        store: &mut MessageStore,
        now: Instant,
    ) {
        if !self.current_session(session) {
            return;
        }
        self.last_recv = now;
        if self.reliable {
            self.outbound.on_ack(ack_next, u64::from(bits), bitmap, store, now);
        }
    }

    /// Periodic work: hello retries while disconnected; heartbeats while
    /// connected. `Err(())` means the peer timed out. Retransmit probes are
    /// granted through [`Self::probe_budget`] and sent with the next flush.
    pub(crate) fn tick(
        &mut self,
        socket: &UdpSocket,
        now: Instant,
        peer_timeout: Duration,
    ) -> Result<SendOutcome, ()> {
        if !self.connected {
            if now >= self.hello_due {
                return Ok(self.send_hello(socket, now));
            }
            return Ok(SendOutcome::Done);
        }
        if now.saturating_sub(self.last_recv) >= peer_timeout {
            return Err(());
        }
        if self.ack_due || now.saturating_sub(self.last_send) >= self.heartbeat_interval {
            return Ok(self.send_ack(socket, now));
        }
        Ok(SendOutcome::Done)
    }

    /// Oldest unacked packets a timeout probe may resend now.
    #[inline]
    pub(crate) fn probe_budget(&mut self, now: Instant) -> u64 {
        if self.connected && self.reliable { self.outbound.probe_budget(now) } else { 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::udp::{
        UdpConfig,
        wire::{self, FRAGMENT_HEADER, PACKET_HEADER, WHOLE_HEADER},
    };

    const SESSION: u32 = 7;

    fn peer(socket: &UdpSocket) -> Peer {
        let group = UdpGroupConfig {
            udp: UdpConfig { send_window: 64, recv_window: 64, ..UdpConfig::lan() },
            ..Default::default()
        };
        let mut peer = Peer::new(socket.local_addr().unwrap(), Token(1), Token(1), 1, &group);
        assert!(peer.on_hello_ack(SESSION, 1, 10, Instant::now()));
        peer
    }

    /// A record to encode: the fragment's `(message, offset, total)`, or
    /// `None` for a whole message, and its bytes.
    type Piece<'a> = (Option<(u32, u32, u32)>, &'a [u8]);

    fn packet(seq: u64, records: &[Piece<'_>]) -> Vec<u8> {
        let mut header = [0; PACKET_HEADER];
        wire::packet_header(&mut header, SESSION, seq, seq * 100, records.len() as u16);
        let mut bytes = header.to_vec();
        for (fragment, payload) in records {
            match fragment {
                None => {
                    let mut h = [0; WHOLE_HEADER];
                    wire::whole_header(&mut h, payload.len());
                    bytes.extend_from_slice(&h);
                }
                Some((message, offset, total)) => {
                    let mut h = [0; FRAGMENT_HEADER];
                    wire::fragment_header(&mut h, payload.len(), *message, *offset, *total);
                    bytes.extend_from_slice(&h);
                }
            }
            bytes.extend_from_slice(payload);
        }
        bytes
    }

    fn feed(peer: &mut Peer, bytes: &[u8], out: &mut Vec<(Vec<u8>, u64)>) {
        let Some(wire::Datagram::Packet(packet)) = wire::decode(bytes) else {
            panic!("not a packet")
        };
        peer.on_packet(packet, None, Instant::now(), &mut |payload, ts| {
            let RxPayload::Raw(bytes) = payload else { panic!() };
            out.push((bytes.to_vec(), ts.0));
        });
    }

    #[test]
    fn fragments_arrive_in_any_order_and_only_count_once() {
        let socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let mut peer = peer(&socket);
        let message: Vec<u8> = (0..2500u32).map(|i| i as u8).collect();
        let a = packet(10, &[(None, b"first"), (Some((3, 0, 2500)), &message[..1000])]);
        let b = packet(11, &[(Some((3, 1000, 2500)), &message[1000..2000])]);
        let c = packet(12, &[(Some((3, 2000, 2500)), &message[2000..]), (None, b"last")]);
        let mut out = Vec::new();
        feed(&mut peer, &c, &mut out);
        feed(&mut peer, &b, &mut out);
        feed(&mut peer, &c, &mut out);
        assert_eq!(out, [(b"last".to_vec(), 1200)]);
        feed(&mut peer, &a, &mut out);
        assert_eq!(out.len(), 3);
        assert_eq!(out[1], (b"first".to_vec(), 1000));
        assert_eq!(out[2], (message, 1000));
        for again in [&a, &b, &c] {
            feed(&mut peer, again, &mut out);
        }
        assert_eq!(out.len(), 3);
        assert_eq!(peer.inbound.ack_next(), 13);
    }

    #[test]
    fn a_lost_packet_is_a_hole_until_its_copy_arrives() {
        let socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let mut peer = peer(&socket);
        let lost = packet(10, &[(Some((1, 0, 6)), b"abc")]);
        let after = packet(11, &[(Some((1, 3, 6)), b"def"), (None, b"solo")]);
        let mut out = Vec::new();
        feed(&mut peer, &after, &mut out);
        assert_eq!(out, [(b"solo".to_vec(), 1100)]);
        assert_eq!(peer.inbound.ack_next(), 10);
        assert_eq!(peer.inbound.ack_bits(), 1);
        feed(&mut peer, &lost, &mut out);
        assert_eq!(out[1], (b"abcdef".to_vec(), 1000));
        assert_eq!(peer.inbound.ack_next(), 12);
    }

    #[test]
    fn packets_of_another_session_are_ignored() {
        let socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let mut peer = peer(&socket);
        let mut stale = packet(10, &[(None, b"old")]);
        stale[3..7].copy_from_slice(&(SESSION + 1).to_le_bytes());
        let mut out = Vec::new();
        feed(&mut peer, &stale, &mut out);
        assert!(out.is_empty());
        feed(&mut peer, &packet(10, &[(None, b"new")]), &mut out);
        assert_eq!(out, [(b"new".to_vec(), 1000)]);
    }

    #[test]
    fn a_cut_short_packet_leaves_its_sequence_unacked() {
        let socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let mut peer = peer(&socket);
        let mut bytes = packet(10, &[(None, b"one"), (None, b"two")]);
        bytes.truncate(bytes.len() - 1);
        let mut out = Vec::new();
        feed(&mut peer, &bytes, &mut out);
        assert!(out.is_empty());
        assert_eq!(peer.inbound.ack_next(), 10);
    }
}
