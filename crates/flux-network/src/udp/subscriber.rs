//! What a publisher knows of one subscriber: where it joined the stream,
//! what it has acked, and how to recover what it lost. The packets
//! themselves are the [`Stream`]'s; a retransmit resends their bytes to this
//! subscriber alone.

use std::{collections::VecDeque, net::SocketAddr};

use flux_communication::queue::{Producer, Queue, QueueType};
use flux_timing::{Duration, Instant, Nanos};
use flux_utils::directories::{local_share_dir, shmem_dir_queues_with_base};
use mio::Token;

use super::{
    stream::{MessageStore, Stream},
    sys::{SendBatch, SockAddr},
};
use crate::{NetworkTelemetry, network::UdpGroupConfig};

/// Retransmit backoff saturates at `rto << MAX_BACKOFF_SHIFT` (and `max_rto`).
const MAX_BACKOFF_SHIFT: u8 = 6;
/// Most packets one timeout probe resends.
const MAX_RECOVER: u64 = 64;
/// A first send this many packets below the highest ack is lost, not
/// reordered: one flow does not reorder that far, while the reorder window
/// grows with queueing delay.
const FAST_RETRANSMIT_GAP: u64 = 64;

/// Telemetry record of a packet's first send.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct UdpSend {
    pub sent_at: Instant,
    pub bytes: u32,
    pub datagrams: u32,
}

/// Telemetry record of a packet sent again.
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

    /// How long an unacked packet below the highest ack may lag before it
    /// counts as lost rather than reordered: a quarter RTT.
    #[inline]
    fn reorder_window(&self) -> Duration {
        Duration(self.srtt.unwrap_or(self.value / 2) / 4)
    }
}

/// A packet sent to this subscriber more than once.
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
    /// The listener it subscribed on.
    pub(crate) socket_token: Token,
    /// The subscriber's session, named in its acks.
    pub(crate) session: u32,
    /// Where it joined the stream; repeated in every hello ack.
    pub(crate) join: u64,
    reliable: bool,
    /// Oldest packet it still needs: unacked, or unsent when unreliable.
    base: u64,
    /// First packet never handed to the kernel for it.
    next_send: u64,
    acked: Box<[u64]>,
    mask: u64,
    /// Highest sequence it has ever acked. Unacked sequences below it are
    /// holes it has moved past; above it, silence proves nothing (they may
    /// be queued behind a slow link).
    highest_acked: Option<u64>,
    /// When an ack last acked something new.
    last_ack_at: Instant,
    /// Packets the next timeout probe resends; doubles per consecutive probe
    /// round and resets when acks flow again, like slow start after an RTO.
    recover: u64,
    last_scan: Instant,
    rto: Rto,
    /// First sends as runs of consecutive sequences that went out together:
    /// the first sequence of each run and when.
    first_sends: VecDeque<(u64, Instant)>,
    /// Retransmitted packets, by sequence.
    resent: Vec<Resent>,
    /// Sequences put in the current batch, in order, until `commit`.
    staged: Vec<u64>,
    pub(crate) last_recv: Instant,
    /// Dropped once everything below this is acked.
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

    /// Oldest packet it still needs; the stream holds nothing it can drop
    /// below the lowest of these.
    #[inline]
    pub(crate) fn base(&self) -> u64 {
        self.base
    }

    #[inline]
    pub(crate) fn is_draining(&self) -> bool {
        self.close_at.is_some()
    }

    /// Asks to drop it once it has acked everything below `end`; returns
    /// whether that already holds.
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

    /// When `seq`, already sent, last went out to it.
    fn sent_at(&self, seq: u64) -> Instant {
        if let Some(r) = self.resent(seq) {
            return r.sent_at;
        }
        let run = self.first_sends.partition_point(|&(first, _)| first <= seq);
        run.checked_sub(1).map_or(Instant::ZERO, |i| self.first_sends[i].1)
    }

    /// Forgets send times below `base`.
    fn prune(&mut self) {
        while self.first_sends.get(1).is_some_and(|&(first, _)| first <= self.base) {
            self.first_sends.pop_front();
        }
        let below = self.resent.partition_point(|r| r.seq < self.base);
        self.resent.drain(..below);
    }

    /// Applies an ack: everything below `ack_next` plus the bitmap words for
    /// `n_bits` sequences above it. Unreliable: liveness only.
    pub(crate) fn on_ack(&mut self, ack_next: u64, n_bits: u64, words: &[u8], now: Instant) {
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
        'words: for (w, chunk) in words.chunks_exact(8).enumerate() {
            let mut bits = u64::from_le_bytes(chunk.try_into().unwrap());
            while bits != 0 {
                let i = bits.trailing_zeros() as u64;
                bits &= bits - 1;
                let seq = ack_next + 1 + 64 * w as u64 + i;
                if seq >= ack_next + 1 + n_bits || seq >= self.next_send {
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
        // Acks of first sends mean the path is flowing again; acks of probes
        // alone keep the ramp going.
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

    /// Sequences below the highest acked one are candidates for holes.
    #[inline]
    fn hole_scan_end(&self) -> Option<u64> {
        self.highest_acked.map(|h| h.min(self.next_send)).filter(|&end| end > self.base)
    }

    /// An unacked sequence it has moved past. A first send counts as lost
    /// after a reordering window, or at once when it is
    /// [`FAST_RETRANSMIT_GAP`] packets beyond it; a retransmit only after its
    /// backed-off RTO so its own ack has time to arrive through whatever
    /// queue delayed the original.
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

    /// Timer-driven recovery budget, granted at most every `rto / 2`: after
    /// a full RTO of ack silence, the oldest `recover` unacked packets are
    /// probed. Silence never triggers more than that, so a burst queued
    /// behind a slow link is not blasted twice; each probe round doubles
    /// `recover`.
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

    /// Puts packets into `batch` until it is full or nothing is due: holes
    /// it has moved past, then `probes` oldest unacked packets, then
    /// whatever it was never sent. [`Self::commit`] records what the kernel
    /// took; nothing may be staged for it again before that.
    pub(crate) fn stage(
        &mut self,
        stream: &mut Stream,
        store: &MessageStore,
        batch: &mut SendBatch,
        mut probes: u64,
        now: Instant,
    ) -> usize {
        debug_assert!(self.staged.is_empty());
        let send_ts = Nanos::now();
        let hole_end = self.hole_scan_end().unwrap_or(self.base);
        // Probes may reach anything in flight; holes only lie below the
        // highest ack. Either way the range ends before `next_send`, so
        // nothing is staged twice.
        let end = if probes != 0 { self.next_send } else { hole_end };
        let mut seq = self.base;
        while seq < end && !batch.is_full() {
            let hole = seq < hole_end && self.is_hole(seq, now);
            let probe = !hole && probes != 0 && !self.is_acked(seq);
            if hole || probe {
                stream.put(store, batch, &self.native_addr, seq, send_ts);
                self.staged.push(seq);
                probes -= u64::from(probe);
            }
            seq += 1;
        }
        let mut seq = self.next_send;
        while seq < stream.next() && !batch.is_full() {
            stream.put(store, batch, &self.native_addr, seq, send_ts);
            self.staged.push(seq);
            seq += 1;
        }
        self.staged.len()
    }

    /// Records that the kernel accepted the first `accepted` staged packets
    /// and forgets the rest of the staging, which stays due.
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
