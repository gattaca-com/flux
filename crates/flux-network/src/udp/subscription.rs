use std::net::SocketAddr;

use flux_communication::Timer;
use flux_timing::{Duration, Instant, Nanos};
use flux_utils::DCache;
use mio::{Token, net::UdpSocket};
use tracing::{debug, warn};

use super::{
    SendOutcome,
    inbound::Inbound,
    new_session, send_datagram,
    wire::{self, ACK_HEADER, HELLO_SIZE, MESSAGE_HEADER, Packet, Record, Records},
};
use crate::{
    NetworkTelemetry,
    network::{RxPayload, UdpGroupConfig},
};

fn latency_timer(telemetry: NetworkTelemetry, label: &str) -> Option<Timer> {
    let NetworkTelemetry::Enabled { app_name } = telemetry else { return None };
    Some(Timer::new(app_name, format!("udp_latency_{label}")))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refused {
    Malformed,
    TooLarge,
    ExceedsDcache,
}

pub(crate) struct Subscription {
    pub(crate) addr: SocketAddr,
    /// Also the token of the socket it owns.
    pub(crate) token: Token,
    max_message_size: usize,
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
    pub(crate) fn new(addr: SocketAddr, token: Token, config: &UdpGroupConfig) -> Self {
        let now = Instant::now();
        let udp = config.udp;
        let inbound = Inbound::new(
            udp.recv_window,
            udp.max_datagram_size,
            udp.max_message_size,
            !udp.reliable,
        );
        let on_connect_len = config.on_connect_msg.as_ref().map_or(0, Vec::len);
        let ctrl_len = (ACK_HEADER + inbound.bitmap_capacity()).max(HELLO_SIZE + on_connect_len);
        let label = format!("{}_{}_{addr}", config.name, token.0);
        Self {
            addr,
            token,
            max_message_size: udp.max_message_size,
            reliable: udp.reliable,
            heartbeat_interval: udp.heartbeat_interval,
            initial_rto: udp.initial_rto,
            max_rto: udp.max_rto,
            connected: false,
            session: new_session(token.0),
            publisher: None,
            inbound,
            last_send: now,
            hello_due: Instant::ZERO,
            hello_backoff: 0,
            ack_due: false,
            latency: latency_timer(config.telemetry, &label),
            ctrl: vec![0; ctrl_len],
        }
    }

    #[inline]
    pub(crate) fn is_connected(&self) -> bool {
        self.connected
    }

    #[inline]
    pub(crate) fn take_ack_due(&mut self) -> bool {
        std::mem::take(&mut self.ack_due)
    }

    pub(crate) fn reset(&mut self) {
        self.connected = false;
        self.publisher = None;
        self.session = new_session(self.token.0);
        self.hello_due = Instant::ZERO;
        self.hello_backoff = 0;
        self.ack_due = false;
    }

    pub(crate) fn send_hello(
        &mut self,
        socket: &UdpSocket,
        msg: &[u8],
        now: Instant,
    ) -> SendOutcome {
        let interval = (self.initial_rto.0 << self.hello_backoff.min(6)).min(self.max_rto.0);
        self.hello_due = now + Duration(interval);
        self.hello_backoff = self.hello_backoff.saturating_add(1);
        let n = wire::hello(&mut self.ctrl, self.session, msg);
        self.last_send = now;
        send_datagram(socket, self.addr, &self.ctrl[..n])
    }

    pub(crate) fn on_hello_ack(&mut self, publisher: u32, subscriber: u32, join: u64) -> bool {
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
    pub(crate) fn lost(&self, subscriber: u32, publisher: Option<u32>) -> bool {
        self.connected &&
            subscriber == self.session &&
            publisher.is_none_or(|s| self.publisher != Some(s))
    }

    #[inline]
    fn current(&self, publisher: u32) -> bool {
        self.connected && self.publisher == Some(publisher)
    }

    /// Validates before accepting: a bad packet must not consume the
    /// sequence a good retry will carry.
    pub(crate) fn on_packet<F>(
        &mut self,
        packet: Packet<'_>,
        dcache: Option<&DCache>,
        deliver: &mut F,
    ) where
        F: FnMut(RxPayload<'_>, Nanos),
    {
        let (seq, send_ts) = (packet.seq, packet.send_ts);
        if !self.current(packet.session) {
            return;
        }
        let checked =
            packet.split().ok_or(Refused::Malformed).and_then(|(continuation, records)| {
                self.validate(seq, continuation, records.clone(), dcache)
                    .map(|()| (continuation, records))
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

    /// Includes a length whose header is cut between this packet and one
    /// already in.
    fn validate(
        &self,
        seq: u64,
        continuation: &[u8],
        records: Records<'_>,
        dcache: Option<&DCache>,
    ) -> Result<(), Refused> {
        let check = |len: usize| {
            if len > self.max_message_size {
                return Err(Refused::TooLarge);
            }
            if dcache.is_some_and(|dc| len > dc.capacity()) {
                return Err(Refused::ExceedsDcache);
            }
            Ok(())
        };
        if let Some(len) = self.inbound.waiting_len(seq, continuation) {
            check(len)?;
        }
        for record in records {
            let len = match record {
                Record::Whole(bytes) => Some(bytes.len()),
                Record::Head(bytes) => wire::record_len(bytes)
                    .checked_sub(MESSAGE_HEADER)
                    .or_else(|| self.inbound.stashed_len(seq, bytes)),
            };
            if let Some(len) = len {
                check(len)?;
            }
        }
        Ok(())
    }

    pub(crate) fn send_ack(&mut self, socket: &UdpSocket, now: Instant) -> SendOutcome {
        let Some(publisher) = self.publisher else { return SendOutcome::Done };
        let n_bits = self.inbound.ack_bits();
        let header: &mut [u8; ACK_HEADER] = (&mut self.ctrl[..ACK_HEADER]).try_into().unwrap();
        wire::ack_header(header, publisher, self.session, self.inbound.ack_next());
        let n = self.inbound.write_bitmap(n_bits, &mut self.ctrl[ACK_HEADER..]);
        self.ack_due = false;
        self.last_send = now;
        send_datagram(socket, self.addr, &self.ctrl[..ACK_HEADER + n])
    }

    pub(crate) fn tick(&mut self, socket: &UdpSocket, msg: &[u8], now: Instant) -> SendOutcome {
        if !self.connected {
            if now >= self.hello_due {
                return self.send_hello(socket, msg, now);
            }
            return SendOutcome::Done;
        }
        if self.ack_due || now.saturating_sub(self.last_send) >= self.heartbeat_interval {
            return self.send_ack(socket, now);
        }
        SendOutcome::Done
    }
}

#[inline]
pub(crate) fn deliver_message<F>(
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
        wire::{self, PACKET_HEADER},
    };

    const SESSION: u32 = 7;

    fn subscription() -> Subscription {
        let group = UdpGroupConfig {
            udp: UdpConfig { send_window: 64, recv_window: 64, ..UdpConfig::lan() },
            ..Default::default()
        };
        let mut sub = Subscription::new("127.0.0.1:1".parse().unwrap(), Token(1), &group);
        let session = sub.session;
        assert!(sub.on_hello_ack(SESSION, session, 10));
        sub
    }

    /// The last message is cut to `head` bytes if given.
    fn packet(seq: u64, continuation: &[u8], messages: &[&[u8]], head: Option<usize>) -> Vec<u8> {
        let mut header = [0; PACKET_HEADER];
        let start = if messages.is_empty() { 0 } else { continuation.len() as u16 + 1 };
        wire::packet_header(&mut header, SESSION, seq, seq * 100, start);
        let mut bytes = header.to_vec();
        bytes.extend_from_slice(continuation);
        for (i, m) in messages.iter().enumerate() {
            bytes.extend_from_slice(&(m.len() as u32).to_le_bytes());
            let cut = if i + 1 == messages.len() { head.unwrap_or(m.len()) } else { m.len() };
            bytes.extend_from_slice(&m[..cut]);
        }
        bytes
    }

    fn feed(sub: &mut Subscription, bytes: &[u8], out: &mut Vec<(Vec<u8>, u64)>) {
        let Some(wire::Datagram::Packet(packet)) = wire::decode(bytes) else {
            panic!("not a packet")
        };
        sub.on_packet(packet, None, &mut |payload, ts| {
            let RxPayload::Raw(bytes) = payload else { panic!() };
            out.push((bytes.to_vec(), ts.0));
        });
    }

    #[test]
    fn fragments_arrive_in_any_order_and_only_count_once() {
        let mut sub = subscription();
        let message: Vec<u8> = (0..2500u32).map(|i| i as u8).collect();
        let a = packet(10, b"", &[b"first", &message], Some(1000));
        let b = packet(11, &message[1000..2000], &[], None);
        let c = packet(12, &message[2000..], &[b"last"], None);
        let mut out = Vec::new();
        feed(&mut sub, &c, &mut out);
        feed(&mut sub, &b, &mut out);
        feed(&mut sub, &c, &mut out);
        assert_eq!(out, [(b"last".to_vec(), 1200)]);
        feed(&mut sub, &a, &mut out);
        assert_eq!(out.len(), 3);
        assert_eq!(out[1], (b"first".to_vec(), 1000));
        assert_eq!(out[2], (message, 1000));
        for again in [&a, &b, &c] {
            feed(&mut sub, again, &mut out);
        }
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn packets_of_another_session_are_ignored() {
        let mut sub = subscription();
        let mut stale = packet(10, b"", &[b"old"], None);
        stale[3..7].copy_from_slice(&(SESSION + 1).to_le_bytes());
        let mut out = Vec::new();
        feed(&mut sub, &stale, &mut out);
        assert!(out.is_empty());
        feed(&mut sub, &packet(10, b"", &[b"new"], None), &mut out);
        assert_eq!(out, [(b"new".to_vec(), 1000)]);
    }

    fn raw(seq: u64, start: u16, payload: &[u8]) -> Vec<u8> {
        let mut header = [0; PACKET_HEADER];
        wire::packet_header(&mut header, SESSION, seq, seq * 100, start);
        [&header[..], payload].concat()
    }

    #[test]
    fn a_message_cut_in_or_right_after_its_header_completes_in_any_order() {
        let mut sub = subscription();
        let stream = [&6u32.to_le_bytes()[..], b"abcdef"].concat();
        let mut out = Vec::new();
        let mut seq = 10;
        for cut in 1..=MESSAGE_HEADER {
            for head_first in [true, false] {
                let head = raw(seq, 1, &stream[..cut]);
                let tail = raw(seq + 1, 0, &stream[cut..]);
                let order = if head_first { [&head, &tail] } else { [&tail, &head] };
                for bytes in order {
                    feed(&mut sub, bytes, &mut out);
                }
                assert_eq!(out, [(b"abcdef".to_vec(), seq * 100)]);
                out.clear();
                seq += 2;
            }
        }
        // A cut header naming too much.
        let header = u32::MAX.to_le_bytes();
        feed(&mut sub, &raw(seq, 1, &header[..2]), &mut out);
        feed(&mut sub, &raw(seq + 1, 0, &[&header[2..], b"x"].concat()), &mut out);
        assert!(out.is_empty());
    }
}
