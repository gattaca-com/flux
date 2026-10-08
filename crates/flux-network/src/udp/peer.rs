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
    outbound::{MessageStore, Outbound, WindowFull},
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

    #[inline]
    pub(crate) fn is_draining(&self) -> bool {
        self.close_when_drained
    }

    /// Requests a close; returns whether it can happen already.
    pub(crate) fn close_when_drained(&mut self) -> bool {
        self.close_when_drained = true;
        self.drained()
    }

    #[inline]
    pub(crate) fn drained(&self) -> bool {
        self.close_when_drained && self.outbound.inflight() == 0
    }

    /// Queues the stream in store `slot`. On [`WindowFull`] the peer is
    /// unusable and must be dropped.
    pub(crate) fn enqueue(
        &mut self,
        store: &mut MessageStore,
        slot: u32,
        now: Instant,
    ) -> Result<(), WindowFull> {
        let result = self.outbound.enqueue(store, slot);
        if result.is_err() {
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
        let (session, seq, send_ts) = (packet.session, packet.seq, packet.send_ts);
        if !self.current_session(session) {
            return;
        }
        self.last_recv = now;
        let checked =
            packet.split().ok_or(Refused::Malformed).and_then(|(continuation, records)| {
                self.validate(records.clone(), dcache).map(|()| (continuation, records))
            });
        let (continuation, records) = match checked {
            Ok(split) => split,
            Err(Refused::ExceedsDcache) => {
                warn!(%self.addr, "udp message exceeds dcache capacity, not acked");
                return;
            }
            Err(why) => {
                debug!(%self.addr, ?why, seq, "udp packet refused");
                return;
            }
        };
        if self.reliable {
            self.ack_due = true;
        }
        if !self.inbound.accept(seq) {
            return;
        }
        let latency = &mut self.latency;
        self.inbound.packet(seq, continuation, records, send_ts, &mut |bytes, ts| {
            deliver_message(latency, bytes, Nanos(ts), dcache, deliver);
        });
    }

    /// Checks every message starting in a packet before any byte is taken
    /// in.
    fn validate(&self, mut records: Records<'_>, dcache: Option<&DCache>) -> Result<(), Refused> {
        for record in records.by_ref() {
            let len = match record {
                Record::Whole(bytes) => bytes.len(),
                Record::Head { total, .. } => total,
            };
            if len > self.max_message_size {
                return Err(Refused::TooLarge);
            }
            if dcache.is_some_and(|dc| len > dc.capacity()) {
                return Err(Refused::ExceedsDcache);
            }
        }
        if records.ok() { Ok(()) } else { Err(Refused::Malformed) }
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

#[inline]
fn deliver_message<F>(
    latency: &mut Option<Timer>,
    bytes: &[u8],
    send_ts: Nanos,
    dcache: Option<&DCache>,
    deliver: &mut F,
) where
    F: FnMut(RxPayload<'_>, Nanos),
{
    if let Some(t) = latency {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::udp::{
        UdpConfig,
        wire::{self, LONG_HEADER, PACKET_HEADER},
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

    /// A packet of `continuation` then `messages`, the last cut to `head`
    /// bytes if given.
    fn packet(seq: u64, continuation: &[u8], messages: &[&[u8]], head: Option<usize>) -> Vec<u8> {
        let mut header = [0; PACKET_HEADER];
        let start = if messages.is_empty() { 0 } else { continuation.len() as u16 + 1 };
        wire::packet_header(&mut header, SESSION, seq, seq * 100, start);
        let mut bytes = header.to_vec();
        bytes.extend_from_slice(continuation);
        for (i, m) in messages.iter().enumerate() {
            let mut h = [0; LONG_HEADER];
            let n = wire::message_header(&mut h, m.len());
            bytes.extend_from_slice(&h[..n]);
            let cut = if i + 1 == messages.len() { head.unwrap_or(m.len()) } else { m.len() };
            bytes.extend_from_slice(&m[..cut]);
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
        let a = packet(10, b"", &[b"first", &message], Some(1000));
        let b = packet(11, &message[1000..2000], &[], None);
        let c = packet(12, &message[2000..], &[b"last"], None);
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
        let lost = packet(10, b"", &[b"abcdef"], Some(3));
        let after = packet(11, b"def", &[b"solo"], None);
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
        let mut stale = packet(10, b"", &[b"old"], None);
        stale[3..7].copy_from_slice(&(SESSION + 1).to_le_bytes());
        let mut out = Vec::new();
        feed(&mut peer, &stale, &mut out);
        assert!(out.is_empty());
        feed(&mut peer, &packet(10, b"", &[b"new"], None), &mut out);
        assert_eq!(out, [(b"new".to_vec(), 1000)]);
    }

    #[test]
    fn a_packet_cut_at_a_header_leaves_its_sequence_unacked() {
        let socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let mut peer = peer(&socket);
        let mut bytes = packet(10, b"", &[b"one", b"two"], None);
        bytes.truncate(bytes.len() - 3);
        let mut out = Vec::new();
        feed(&mut peer, &bytes, &mut out);
        assert!(out.is_empty());
        assert_eq!(peer.inbound.ack_next(), 10);
        // Cut inside a message it reads as a head, which only the next
        // packet can refute.
        bytes.truncate(bytes.len() - 1);
        feed(&mut peer, &bytes, &mut out);
        assert_eq!(out, [(b"one".to_vec(), 1000)]);
        assert_eq!(peer.inbound.ack_next(), 11);
    }
}
