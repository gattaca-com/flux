use std::net::SocketAddr;

use flux_timing::{Duration, Instant, Nanos};
use mio::{Token, net::UdpSocket};
use tracing::debug;

use super::wire::{self, ACK_HEADER, RESET_SIZE};
use crate::{
    network::UdpGroupConfigV2,
    udp_v2::{
        SendOutcome,
        inbound::Inbound,
        new_session,
        outbound::Outbound,
        send_datagram,
        stream::Stream,
        telemetry::{PeerId, Telemetry},
        wire::Packet,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Role {
    /// Dialed out from a socket of its own; reconnects when lost.
    Initiator,
    /// Accepted on a listener; dropped when lost.
    Acceptor,
}

/// Both ends of a connection: a stream out, a window in.
pub(super) struct Peer {
    pub(super) addr: SocketAddr,
    pub(super) token: Token,
    /// The socket it sends from: its own for an initiator, else the listener's.
    pub(super) socket: Token,
    pub(super) role: Role,
    pub(super) connected: bool,
    /// Ours; the peer names it in everything it sends.
    pub(super) session: u32,
    /// Theirs, once connected.
    pub(super) remote_session: u32,
    pub(super) stream: Stream,
    pub(super) out: Outbound,
    inbound: Inbound,
    reliable: bool,
    heartbeat_interval: Duration,
    initial_rto: Duration,
    max_rto: Duration,
    pub(super) last_recv: Instant,
    last_send: Instant,
    pub(super) ack_due: bool,
    connect_due: Instant,
    connect_backoff: u8,
    panic_after_unanswered_handshake: Option<Duration>,
    connect_unanswered: Duration,
    pub(super) close_at: Option<u64>,
    id: PeerId,
}

impl Peer {
    pub(super) fn new(
        addr: SocketAddr,
        token: Token,
        socket: Token,
        role: Role,
        session: u32,
        config: &UdpGroupConfigV2,
        now: Instant,
    ) -> Self {
        let udp = config.udp;
        let id = PeerId::new(token, addr);
        Self {
            addr,
            token,
            socket,
            role,
            connected: false,
            session,
            remote_session: 0,
            stream: Stream::new(&udp, wire::PACKET, 0),
            out: Outbound::new(addr, 0, &udp, id),
            inbound: Inbound::new(
                udp.recv_window,
                udp.max_datagram_size,
                ACK_HEADER,
                udp.max_message_size,
                !udp.reliable,
            ),
            reliable: udp.reliable,
            heartbeat_interval: udp.heartbeat_interval,
            initial_rto: udp.initial_rto,
            max_rto: udp.max_rto,
            last_recv: now,
            last_send: now,
            ack_due: false,
            connect_due: Instant::ZERO,
            connect_backoff: 0,
            panic_after_unanswered_handshake: udp.panic_after_unanswered_handshake,
            connect_unanswered: Duration::ZERO,
            close_at: None,
            id,
        }
    }

    /// Both streams start at seq 0.
    pub(super) fn establish(&mut self, remote_session: u32, now: Instant) {
        self.connected = true;
        self.remote_session = remote_session;
        self.stream.reset(remote_session);
        self.out.reset(0);
        self.inbound.reset(0);
        self.last_recv = now;
        self.ack_due = false;
        self.connect_backoff = 0;
        self.connect_unanswered = Duration::ZERO;
        self.close_at = None;
    }

    /// An initiator's next attempt, under a new session.
    pub(super) fn restart(&mut self) {
        self.connected = false;
        self.session = new_session(self.token.0);
        self.stream.reset(0);
        self.connect_due = Instant::ZERO;
        self.connect_backoff = 0;
        self.connect_unanswered = Duration::ZERO;
        self.ack_due = false;
        self.close_at = None;
    }

    #[inline]
    pub(super) fn drained(&self) -> bool {
        self.close_at.is_some_and(|end| self.out.base() >= end)
    }

    /// Releases what the receiver acked.
    #[inline]
    pub(super) fn release(&mut self) {
        let base = self.out.base();
        if base > self.stream.base() {
            self.stream.release_to(base);
        }
    }

    pub(super) fn send_hello(
        &mut self,
        socket: &UdpSocket,
        ctrl: &mut [u8],
        msg: &[u8],
        now: Instant,
    ) -> SendOutcome {
        let n = wire::hello(ctrl, self.session, msg);
        self.last_send = now;
        let outcome = send_datagram(socket, self.addr, &ctrl[..n]);
        // Not sent: retried on the next tick.
        if outcome == SendOutcome::Done {
            let interval = (self.initial_rto.0 << self.connect_backoff.min(6)).min(self.max_rto.0);
            self.connect_due = now + Duration(interval);
            self.connect_backoff = self.connect_backoff.saturating_add(1);
        }
        outcome
    }

    pub(super) fn send_hello_ack(
        &self,
        socket: &UdpSocket,
        ctrl: &mut [u8],
        msg: &[u8],
    ) -> SendOutcome {
        let n = wire::hello_ack(ctrl, self.remote_session, self.session, msg);
        send_datagram(socket, self.addr, &ctrl[..n])
    }

    pub(super) fn on_packet(
        &mut self,
        packet: Packet<'_>,
        telemetry: &mut Telemetry,
        deliver: &mut dyn FnMut(&[u8], Nanos),
    ) {
        let received = self.inbound.receive(packet, &mut |bytes, send_ts| {
            telemetry.latency(self.id, Nanos(send_ts));
            deliver(bytes, Nanos(send_ts));
        });
        match received {
            Ok(()) => self.ack_due |= self.reliable,
            Err(why) => debug!(%self.addr, ?why, seq = packet.seq, "udp packet refused"),
        }
    }

    pub(super) fn send_ack(
        &mut self,
        socket: &UdpSocket,
        ctrl: &mut [u8],
        now: Instant,
    ) -> SendOutcome {
        let n_bits = self.inbound.ack_bits();
        let header: &mut [u8; ACK_HEADER] = (&mut ctrl[..ACK_HEADER]).try_into().unwrap();
        wire::ack_header(header, self.remote_session, self.inbound.ack_next());
        let n = self.inbound.write_bitmap(n_bits, &mut ctrl[ACK_HEADER..]);
        self.last_send = now;
        let outcome = send_datagram(socket, self.addr, &ctrl[..ACK_HEADER + n]);
        self.ack_due = outcome == SendOutcome::WouldBlock;
        outcome
    }

    /// Hello retries, due acks and heartbeats.
    pub(super) fn tick(
        &mut self,
        socket: &UdpSocket,
        ctrl: &mut [u8],
        msg: &[u8],
        now: Instant,
    ) -> SendOutcome {
        if !self.connected {
            if self.role == Role::Initiator && now >= self.connect_due {
                if let Some(limit) = self.panic_after_unanswered_handshake {
                    self.connect_unanswered += now.saturating_sub(self.last_send).min(self.max_rto);
                    assert!(
                        self.connect_unanswered < limit,
                        "udp handshake with {} unanswered for {}",
                        self.addr,
                        self.connect_unanswered
                    );
                }
                return self.send_hello(socket, ctrl, msg, now);
            }
            return SendOutcome::Done;
        }
        if self.ack_due || now.saturating_sub(self.last_send) >= self.heartbeat_interval {
            return self.send_ack(socket, ctrl, now);
        }
        SendOutcome::Done
    }
}

pub(super) fn send_reset(socket: &UdpSocket, to: SocketAddr, session: u32) {
    let mut buf = [0; RESET_SIZE];
    wire::reset(&mut buf, session);
    send_datagram(socket, to, &buf);
}
