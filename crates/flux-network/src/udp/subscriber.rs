use std::{collections::VecDeque, net::SocketAddr};

use flux_communication::queue::{Producer, Queue, QueueType};
use flux_timing::{Duration, Instant, Nanos};
use flux_utils::directories::{local_share_dir, shmem_dir_queues_with_base};
use mio::Token;

use super::{
    stream::Stream,
    sys::{SendBatch, SockAddr},
};
use crate::{NetworkTelemetry, network::UdpGroupConfig};

const MAX_BACKOFF_SHIFT: u8 = 6;
/// Most packets one timeout probe resends.
const MAX_RECOVER: u64 = 64;
/// One flow does not reorder this far, while the time-based reorder window
/// grows with queueing delay.
const FAST_RETRANSMIT_GAP: u64 = 64;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct UdpSend {
    pub sent_at: Instant,
    pub bytes: u32,
    pub datagrams: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct UdpRetransmit {
    pub sent_at: Instant,
    pub bytes: u32,
    pub retries: u8,
}

fn telemetry_producer<T: Copy>(
    telemetry: NetworkTelemetry,
    kind: &str,
    label: &str,
) -> Option<Producer<T>> {
    const QUEUE_SIZE: usize = 2usize.pow(13);
    let NetworkTelemetry::Enabled { app_name } = telemetry else { return None };
    let dir = shmem_dir_queues_with_base(local_share_dir(), app_name);
    let _ = std::fs::create_dir_all(&dir);
    let file = dir.join(format!("udp_{kind}_{label}"));
    Some(Producer::from(Queue::create_or_open_shared(file, QUEUE_SIZE, QueueType::MPMC)))
}

/// RFC 6298 estimator in clock ticks.
pub(crate) struct Rto {
    srtt: Option<u64>,
    rttvar: u64,
    value: u64,
    min: u64,
    max: u64,
}

impl Rto {
    fn new(initial: Duration, min: Duration, max: Duration) -> Self {
        Self { srtt: None, rttvar: 0, value: initial.0.clamp(min.0, max.0), min: min.0, max: max.0 }
    }

    fn sample(&mut self, rtt: Duration) {
        let r = rtt.0;
        match self.srtt {
            None => {
                self.srtt = Some(r);
                self.rttvar = r / 2;
            }
            Some(srtt) => {
                self.rttvar = self.rttvar - self.rttvar / 4 + srtt.abs_diff(r) / 4;
                self.srtt = Some(srtt - srtt / 8 + r / 8);
            }
        }
        let srtt = self.srtt.unwrap_or(r);
        self.value = srtt.saturating_add(4 * self.rttvar).clamp(self.min, self.max);
    }

    #[inline]
    fn current(&self) -> Duration {
        Duration(self.value)
    }

    #[inline]
    fn reorder_window(&self) -> Duration {
        Duration(self.srtt.unwrap_or(self.value / 2) / 4)
    }
}

#[derive(Clone, Copy)]
struct Resent {
    seq: u64,
    retries: u8,
    sent_at: Instant,
}

pub(crate) struct Subscriber {
    pub(crate) addr: SocketAddr,
    native_addr: SockAddr,
    pub(crate) token: Token,
    pub(crate) socket_token: Token,
    pub(crate) session: u32,
    pub(crate) join: u64,
    reliable: bool,
    base: u64,
    next_send: u64,
    acked: Box<[u64]>,
    mask: u64,
    /// Unacked sequences below it are holes; above it, silence proves
    /// nothing, they may be queued behind a slow link.
    highest_acked: Option<u64>,
    /// When an ack last acked something new.
    last_ack_at: Instant,
    recover: u64,
    last_scan: Instant,
    rto: Rto,
    /// Runs of consecutive sequences first sent together: (first seq, when).
    first_sends: VecDeque<(u64, Instant)>,
    /// Sorted by `seq`.
    resent: Vec<Resent>,
    staged: Vec<u64>,
    pub(crate) last_recv: Instant,
    close_at: Option<u64>,
    sends: Option<Producer<UdpSend>>,
    retransmits: Option<Producer<UdpRetransmit>>,
}

impl Subscriber {
    pub(crate) fn new(
        addr: SocketAddr,
        token: Token,
        socket_token: Token,
        session: u32,
        join: u64,
        config: &UdpGroupConfig,
        now: Instant,
    ) -> Self {
        let udp = config.udp;
        let label = format!("{}_{}_{addr}", config.name, token.0);
        Self {
            addr,
            native_addr: SockAddr::new(addr),
            token,
            socket_token,
            session,
            join,
            reliable: udp.reliable,
            base: join,
            next_send: join,
            acked: vec![0; udp.send_window.div_ceil(u64::BITS as usize)].into_boxed_slice(),
            mask: udp.send_window as u64 - 1,
            highest_acked: None,
            last_ack_at: Instant::ZERO,
            recover: 1,
            last_scan: Instant::ZERO,
            rto: Rto::new(udp.initial_rto, udp.min_rto, udp.max_rto),
            first_sends: VecDeque::new(),
            resent: Vec::new(),
            staged: Vec::new(),
            last_recv: now,
            close_at: None,
            sends: telemetry_producer(config.telemetry, "sends", &label),
            retransmits: telemetry_producer(config.telemetry, "retransmits", &label),
        }
    }

    #[inline]
    pub(crate) fn base(&self) -> u64 {
        self.base
    }

    #[inline]
    pub(crate) fn is_draining(&self) -> bool {
        self.close_at.is_some()
    }

    pub(crate) fn close_when_drained(&mut self, end: u64) -> bool {
        self.close_at = Some(end);
        self.drained()
    }

    #[inline]
    pub(crate) fn drained(&self) -> bool {
        self.close_at.is_some_and(|end| self.base >= end)
    }

    #[inline]
    fn is_acked(&self, seq: u64) -> bool {
        let i = seq & self.mask;
        self.acked[(i >> 6) as usize] & (1 << (i & 63)) != 0
    }

    #[inline]
    fn set_acked(&mut self, seq: u64, v: bool) {
        let i = seq & self.mask;
        let bit = 1 << (i & 63);
        let word = &mut self.acked[(i >> 6) as usize];
        if v {
            *word |= bit;
        } else {
            *word &= !bit;
        }
    }

    #[inline]
    fn resent(&self, seq: u64) -> Option<&Resent> {
        self.resent.binary_search_by_key(&seq, |r| r.seq).ok().map(|i| &self.resent[i])
    }

    #[inline]
    fn retries(&self, seq: u64) -> u8 {
        self.resent(seq).map_or(0, |r| r.retries)
    }

    fn sent_at(&self, seq: u64) -> Instant {
        if let Some(r) = self.resent(seq) {
            return r.sent_at;
        }
        let run = self.first_sends.partition_point(|&(first, _)| first <= seq);
        run.checked_sub(1).map_or(Instant::ZERO, |i| self.first_sends[i].1)
    }

    fn prune(&mut self) {
        while self.first_sends.get(1).is_some_and(|&(first, _)| first <= self.base) {
            self.first_sends.pop_front();
        }
        let below = self.resent.partition_point(|r| r.seq < self.base);
        self.resent.drain(..below);
    }

    pub(crate) fn on_ack(&mut self, ack_next: u64, bitmap: &[u8], now: Instant) {
        self.last_recv = now;
        if !self.reliable || ack_next > self.next_send {
            return;
        }
        let mut newest = None;
        let mut progressed = false;
        while self.base < ack_next {
            if !self.is_acked(self.base) {
                progressed = true;
                if self.retries(self.base) == 0 {
                    newest = Some(self.base);
                }
            }
            self.set_acked(self.base, false);
            self.base += 1;
        }
        let mut highest = ack_next.checked_sub(1).filter(|&h| h + 1 > self.base);
        'words: for (w, chunk) in bitmap.chunks(8).enumerate() {
            let mut word = [0; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            let mut bits = u64::from_le_bytes(word);
            while bits != 0 {
                let i = bits.trailing_zeros() as u64;
                bits &= bits - 1;
                let seq = ack_next + 1 + 64 * w as u64 + i;
                if seq >= self.next_send {
                    break 'words;
                }
                // A stale ack may name sequences whose ring slots now hold
                // newer packets.
                if seq < self.base {
                    continue;
                }
                highest = Some(seq);
                if !self.is_acked(seq) {
                    progressed = true;
                    if self.retries(seq) == 0 {
                        newest = Some(seq);
                    }
                    self.set_acked(seq, true);
                }
            }
        }
        if progressed {
            self.last_ack_at = now;
        }
        // Only acks of first sends prove the path flows again.
        if newest.is_some() {
            self.recover = 1;
        }
        if let Some(h) = highest {
            self.highest_acked = Some(self.highest_acked.map_or(h, |old| old.max(h)));
        }
        if let Some(seq) = newest {
            self.rto.sample(now.saturating_sub(self.sent_at(seq)));
        }
        self.prune();
    }

    #[inline]
    fn hole_scan_end(&self) -> Option<u64> {
        self.highest_acked.map(|h| h.min(self.next_send)).filter(|&end| end > self.base)
    }

    /// A retransmit waits its backed-off RTO so its own ack can arrive
    /// through whatever queue delayed the original.
    #[inline]
    fn is_hole(&self, seq: u64, now: Instant) -> bool {
        if self.is_acked(seq) {
            return false;
        }
        let age = now.saturating_sub(self.sent_at(seq)).0;
        match self.retries(seq) {
            0 => {
                age >= self.rto.reorder_window().0 ||
                    self.highest_acked
                        .is_some_and(|h| h.saturating_sub(seq) >= FAST_RETRANSMIT_GAP)
            }
            retries => age >= self.rto.current().0 << retries.min(MAX_BACKOFF_SHIFT),
        }
    }

    /// Silence only ever probes a few packets, so a burst queued behind a
    /// slow link is not sent twice.
    pub(crate) fn probe_budget(&mut self, now: Instant) -> u64 {
        let rto = self.rto.current();
        if !self.reliable ||
            self.base == self.next_send ||
            now.saturating_sub(self.last_scan) < rto / 2_u32
        {
            return 0;
        }
        self.last_scan = now;
        let backoff =
            (rto.0 << self.retries(self.base).min(MAX_BACKOFF_SHIFT)).min(self.rto.max).max(rto.0);
        if now.saturating_sub(self.last_ack_at.max(self.sent_at(self.base))).0 < backoff {
            return 0;
        }
        let probes = self.recover;
        self.recover = (self.recover * 2).min(MAX_RECOVER);
        probes
    }

    pub(crate) fn stage(
        &mut self,
        stream: &mut Stream,
        batch: &mut SendBatch,
        mut probes: u64,
        now: Instant,
    ) -> usize {
        debug_assert!(self.staged.is_empty());
        let send_ts = Nanos::now();
        let hole_end = self.hole_scan_end().unwrap_or(self.base);
        // Ends before `next_send`, so nothing is staged twice.
        let end = if probes != 0 { self.next_send } else { hole_end };
        let mut seq = self.base;
        while seq < end && !batch.is_full() {
            let hole = seq < hole_end && self.is_hole(seq, now);
            let probe = !hole && probes != 0 && !self.is_acked(seq);
            if hole || probe {
                stream.put(batch, &self.native_addr, seq, send_ts);
                self.staged.push(seq);
                probes -= u64::from(probe);
            }
            seq += 1;
        }
        let mut seq = self.next_send;
        while seq < stream.next() && !batch.is_full() {
            stream.put(batch, &self.native_addr, seq, send_ts);
            self.staged.push(seq);
            seq += 1;
        }
        self.staged.len()
    }

    pub(crate) fn commit(&mut self, stream: &Stream, accepted: usize, now: Instant) {
        for &seq in &self.staged[..accepted.min(self.staged.len())] {
            let bytes = u32::from(stream.packet_bytes(seq));
            if seq >= self.next_send {
                self.next_send = seq + 1;
                if self.first_sends.back().is_none_or(|&(_, at)| at != now) {
                    self.first_sends.push_back((seq, now));
                }
                if let Some(sends) = &mut self.sends {
                    sends.produce(&UdpSend { sent_at: now, bytes, datagrams: 1 });
                }
                continue;
            }
            let retries = match self.resent.binary_search_by_key(&seq, |r| r.seq) {
                Ok(i) => {
                    let r = &mut self.resent[i];
                    r.retries = r.retries.saturating_add(1);
                    r.sent_at = now;
                    r.retries
                }
                Err(i) => {
                    self.resent.insert(i, Resent { seq, retries: 1, sent_at: now });
                    1
                }
            };
            if let Some(retransmits) = &mut self.retransmits {
                retransmits.produce(&UdpRetransmit { sent_at: now, bytes, retries });
            }
        }
        self.staged.clear();
        // Unreliable: handed to the kernel is as good as acked.
        if !self.reliable {
            self.base = self.next_send;
            self.prune();
        }
    }
}
