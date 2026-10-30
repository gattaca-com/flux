use std::{collections::VecDeque, net::SocketAddr};

use flux_timing::{Duration, Instant, Nanos};

use super::{
    UdpConfig,
    stream::Stream,
    sys::{SendBatch, SockAddr},
    telemetry::{PeerId, Telemetry, UdpRetransmit, UdpSend},
};

const MAX_BACKOFF_SHIFT: u8 = 6;
/// Most packets one timeout probe resends.
const MAX_RECOVER: u64 = 64;
/// One flow does not reorder this far, while the time-based reorder window
/// grows with queueing delay.
const FAST_RETRANSMIT_GAP: u64 = 64;

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

/// One receiver's progress through a stream: what it acked, what to resend.
pub(crate) struct Outbound {
    to: SockAddr,
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
    peer: PeerId,
}

impl Outbound {
    pub(crate) fn new(to: SocketAddr, first: u64, udp: &UdpConfig, peer: PeerId) -> Self {
        Self {
            to: SockAddr::new(to),
            reliable: udp.reliable,
            base: first,
            next_send: first,
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
            peer,
        }
    }

    /// Starts over at `first`, keeping the RTT estimate of the path.
    pub(crate) fn reset(&mut self, first: u64) {
        self.base = first;
        self.next_send = first;
        self.acked.fill(0);
        self.highest_acked = None;
        self.last_ack_at = Instant::ZERO;
        self.recover = 1;
        self.last_scan = Instant::ZERO;
        self.first_sends.clear();
        self.resent.clear();
        self.staged.clear();
    }

    #[inline]
    pub(crate) fn base(&self) -> u64 {
        self.base
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
        let mut highest = None;
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

    /// First sends stop at `until`.
    pub(crate) fn stage(
        &mut self,
        stream: &mut Stream,
        batch: &mut SendBatch,
        mut probes: u64,
        until: u64,
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
                stream.put(batch, &self.to, seq, send_ts);
                self.staged.push(seq);
                probes -= u64::from(probe);
            }
            seq += 1;
        }
        let mut seq = self.next_send;
        while seq < until.min(stream.next()) && !batch.is_full() {
            stream.put(batch, &self.to, seq, send_ts);
            self.staged.push(seq);
            seq += 1;
        }
        self.staged.len()
    }

    pub(crate) fn commit(
        &mut self,
        stream: &Stream,
        accepted: usize,
        now: Instant,
        telemetry: &mut Telemetry,
    ) {
        for &seq in &self.staged[..accepted.min(self.staged.len())] {
            let bytes = u32::from(stream.packet_bytes(seq));
            if seq >= self.next_send {
                self.next_send = seq + 1;
                if self.first_sends.back().is_none_or(|&(_, at)| at != now) {
                    self.first_sends.push_back((seq, now));
                }
                telemetry.send(&UdpSend { peer: self.peer, sent_at: now, bytes, datagrams: 1 });
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
            telemetry.retransmit(&UdpRetransmit { peer: self.peer, sent_at: now, bytes, retries });
        }
        self.staged.clear();
        // Unreliable: handed to the kernel is as good as acked.
        if !self.reliable {
            self.base = self.next_send;
            self.prune();
        }
    }
}
