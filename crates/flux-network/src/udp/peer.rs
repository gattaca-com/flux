//! Per-peer reliability state. One [`UdpPeer`] per remote address; the
//! connector owns the sockets and passes them in.

use std::{collections::VecDeque, io, net::SocketAddr, os::fd::AsRawFd};

use flux_communication::Timer;
use flux_timing::{Duration, Instant, Nanos};
use flux_utils::{DCache, DCacheRef};
use mio::{Token, net::UdpSocket};
use tracing::{debug, warn};

use super::{
    UdpConfig,
    sys::{BATCH, SendBatch, SockAddr},
    wire::{HEADER_SIZE, Header, Kind, fragment_count, write_session},
};

/// Retransmit backoff saturates at `rto << MAX_BACKOFF_SHIFT` (and `max_rto`).
const MAX_BACKOFF_SHIFT: u8 = 6;
/// Most datagrams one timeout probe resends.
const MAX_RECOVER: u64 = 64;
const WARN_INTERVAL_SECS: u64 = 5;

pub(crate) enum RxPayload<'a> {
    Raw(&'a [u8]),
    DCache(DCacheRef),
}

/// Result of a socket write. Errors other than `WouldBlock` are logged and
/// treated as sent: the RTO path retries them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SendOutcome {
    Done,
    WouldBlock,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PushOutcome {
    Queued,
    /// The message does not fit in the send window.
    WindowFull,
    /// Larger than `max_message_size`; dropped, the peer is unaffected.
    TooLarge,
}

/// Sends a filled batch of `n` datagrams. Returns how many the kernel took;
/// errors other than `WouldBlock` count as sent so the RTO path retries them.
#[inline]
pub(crate) fn send_batch(batch: &mut SendBatch, fd: i32, n: usize) -> usize {
    match batch.send(fd) {
        Ok(k) => k,
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
        Err(e) => {
            debug!(?e, "udp send failed");
            n
        }
    }
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

/// RFC 6298 estimator in TSC ticks.
struct Rto {
    srtt: Option<u64>,
    rttvar: u64,
    value: u64,
    min: u64,
    max: u64,
}

impl Rto {
    fn new(config: &UdpConfig) -> Self {
        let min = config.min_rto.0;
        let max = config.max_rto.0;
        Self { srtt: None, rttvar: 0, value: config.initial_rto.0.clamp(min, max), min, max }
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

    /// How long an unacked datagram below the highest ack may lag before it
    /// counts as lost rather than reordered: a quarter RTT.
    #[inline]
    fn reorder_window(&self) -> Duration {
        Duration(self.srtt.unwrap_or(self.value / 2) / 4)
    }
}

/// Serialised messages shared by every peer that still has fragments of
/// them in flight. A broadcast is stored once and referenced per peer.
pub(crate) struct MsgStore {
    entries: Vec<Entry>,
    /// Entries with no references; their buffers are reused by `insert`.
    free: Vec<u32>,
}

struct Entry {
    bytes: Vec<u8>,
    refs: u32,
}

impl MsgStore {
    pub(crate) fn new() -> Self {
        Self { entries: Vec::new(), free: Vec::new() }
    }

    /// Takes `payload`'s buffer into the store, leaving a recycled buffer in
    /// its place. The caller holds one reference and must [`Self::release`]
    /// it once every peer has taken its own.
    pub(crate) fn insert(&mut self, payload: &mut Vec<u8>) -> u32 {
        if let Some(slot) = self.free.pop() {
            let e = &mut self.entries[slot as usize];
            std::mem::swap(&mut e.bytes, payload);
            payload.clear();
            e.refs = 1;
            slot
        } else {
            self.entries.push(Entry { bytes: std::mem::take(payload), refs: 1 });
            (self.entries.len() - 1) as u32
        }
    }

    #[inline]
    fn add_ref(&mut self, slot: u32) {
        self.entries[slot as usize].refs += 1;
    }

    /// Drops one reference; the last frees the entry for reuse.
    pub(crate) fn release(&mut self, slot: u32) {
        let e = &mut self.entries[slot as usize];
        e.refs -= 1;
        if e.refs == 0 {
            self.free.push(slot);
        }
    }

    #[inline]
    pub(crate) fn bytes(&self, slot: u32) -> &[u8] {
        &self.entries[slot as usize].bytes
    }
}

/// One datagram: its header and where its payload lives in the store.
struct Fragment {
    header: [u8; HEADER_SIZE],
    slot: u32,
    offset: u32,
    len: u16,
    sent_at: Instant,
    retries: u8,
}

/// A message this peer holds a store reference on.
struct MsgRef {
    slot: u32,
    first_seq: u64,
    last_seq: u64,
}

/// What an ack told the sender.
struct Acked {
    blocked: bool,
    /// Round trip of the newest datagram acked for the first time that was
    /// never retransmitted (Karn's rule).
    rtt: Option<Duration>,
}

#[inline]
fn payload_of<'a>(store: &'a MsgStore, f: &Fragment) -> &'a [u8] {
    &store.bytes(f.slot)[f.offset as usize..][..usize::from(f.len)]
}

/// Send-side entry in a manager batch: which peer staged which sequence, so
/// the accepted prefix can be marked sent afterwards.
#[derive(Clone, Copy)]
pub(crate) struct Staged {
    pub(crate) peer: usize,
    pub(crate) seq: u64,
}

/// Ring of fragment descriptors from `base` (oldest unacked) to `next` (next
/// to allocate). `[base, next_send)` has been handed to the kernel at least
/// once; `[next_send, next)` is queued behind a full socket buffer. Message
/// bytes live in the [`MsgStore`]; references are released in order as
/// `base` passes them. Slots stay reserved back to the first fragment of the
/// oldest retained message, so a reconnect can replay whole messages.
struct TxWindow {
    slots: Vec<Fragment>,
    acked: Vec<u64>,
    mask: u64,
    base: u64,
    next_send: u64,
    next: u64,
    messages: VecDeque<MsgRef>,
    /// Highest sequence the receiver has ever acked. Unacked sequences below
    /// it are holes the receiver has moved past; above it, silence proves
    /// nothing (they may be queued behind a slow link).
    highest_acked: Option<u64>,
    /// When an ack last acked something new.
    last_ack_at: Instant,
    /// Datagrams the next timeout probe resends; doubles per consecutive probe
    /// round and resets when acks flow again, like slow start after an RTO.
    recover: u64,
    last_scan: Instant,
    exceeded_since: Option<Instant>,
}

impl TxWindow {
    fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || Fragment {
            header: [0; HEADER_SIZE],
            slot: 0,
            offset: 0,
            len: 0,
            sent_at: Instant::ZERO,
            retries: 0,
        });
        Self {
            slots,
            acked: vec![0; capacity.div_ceil(u64::BITS as usize)],
            mask: capacity as u64 - 1,
            base: 0,
            next_send: 0,
            next: 0,
            messages: VecDeque::new(),
            highest_acked: None,
            last_ack_at: Instant::ZERO,
            recover: 1,
            last_scan: Instant::ZERO,
            exceeded_since: None,
        }
    }

    /// Unacked datagrams.
    #[inline]
    fn inflight(&self) -> u64 {
        self.next - self.base
    }

    /// First sequence whose slot is still needed.
    #[inline]
    fn retained_start(&self) -> u64 {
        self.messages.front().map_or(self.next, |m| m.first_seq)
    }

    #[inline]
    fn free(&self) -> u64 {
        self.mask + 1 - (self.next - self.retained_start())
    }

    #[inline]
    fn is_acked(&self, seq: u64) -> bool {
        let i = seq & self.mask;
        self.acked[(i / 64) as usize] & (1 << (i % 64)) != 0
    }

    #[inline]
    fn set_acked(&mut self, seq: u64, v: bool) {
        let i = seq & self.mask;
        let bit = 1 << (i % 64);
        let word = &mut self.acked[(i / 64) as usize];
        if v {
            *word |= bit;
        } else {
            *word &= !bit;
        }
    }

    /// Stages the message in store `slot` as fragments and takes a reference
    /// on it. `false` if the window cannot hold it.
    fn push(
        &mut self,
        session: u32,
        stride: usize,
        store: &mut MsgStore,
        slot: u32,
        ts: Nanos,
    ) -> bool {
        let payload = store.bytes(slot);
        let total = fragment_count(payload.len(), stride);
        if total as u64 > self.free() {
            return false;
        }
        let len = payload.len() as u32;
        let first_seq = self.next;
        for (index, chunk) in payload.chunks(stride).enumerate() {
            let seq = self.next;
            let f = &mut self.slots[(seq & self.mask) as usize];
            Header { kind: Kind::Data, session, seq, len, index: index as u16, send_ts: ts.0 }
                .encode(&mut f.header);
            f.slot = slot;
            f.offset = (index * stride) as u32;
            f.len = chunk.len() as u16;
            f.retries = 0;
            self.next += 1;
        }
        self.messages.push_back(MsgRef { slot, first_seq, last_seq: self.next - 1 });
        store.add_ref(slot);
        true
    }

    /// Puts fragment `seq` into `batch`. The header keeps its original send
    /// timestamp: receive-side latency includes recovery time.
    #[inline]
    fn stage(&self, seq: u64, store: &MsgStore, to: &SockAddr, batch: &mut SendBatch) {
        let f = &self.slots[(seq & self.mask) as usize];
        batch.push(&f.header, payload_of(store, f), to);
    }

    /// Records that the kernel accepted `seq`.
    #[inline]
    fn mark_sent(&mut self, seq: u64, retransmit: bool, now: Instant) {
        let f = &mut self.slots[(seq & self.mask) as usize];
        f.sent_at = now;
        if retransmit {
            f.retries = f.retries.saturating_add(1);
        } else if seq == self.next_send {
            self.next_send += 1;
        }
    }

    /// Resends every `seq` in `range` for which `pred` holds, in batches.
    /// Returns how many went out and whether the socket blocked before the
    /// end.
    #[allow(clippy::too_many_arguments)]
    fn send_where(
        &mut self,
        range: std::ops::Range<u64>,
        mut pred: impl FnMut(&Self, u64) -> bool,
        store: &MsgStore,
        fd: i32,
        to: &SockAddr,
        batch: &mut SendBatch,
        now: Instant,
    ) -> (u64, bool) {
        let mut pushed = [0u64; BATCH];
        let mut n = 0;
        let mut sent = 0;
        for seq in range {
            if !pred(self, seq) {
                continue;
            }
            self.stage(seq, store, to, batch);
            pushed[n] = seq;
            n += 1;
            if n == BATCH {
                let accepted = send_batch(batch, fd, n);
                for &seq in &pushed[..accepted] {
                    self.mark_sent(seq, true, now);
                }
                sent += accepted as u64;
                if accepted < n {
                    return (sent, true);
                }
                n = 0;
            }
        }
        if n != 0 {
            let accepted = send_batch(batch, fd, n);
            for &seq in &pushed[..accepted] {
                self.mark_sent(seq, true, now);
            }
            sent += accepted as u64;
            if accepted < n {
                return (sent, true);
            }
        }
        (sent, false)
    }

    /// Releases store references whose last fragment is below `base`.
    fn release_messages(&mut self, store: &mut MsgStore) {
        while self.messages.front().is_some_and(|m| m.last_seq < self.base) {
            store.release(self.messages.pop_front().unwrap().slot);
        }
    }

    /// Releases every store reference this window holds.
    fn release_all(&mut self, store: &mut MsgStore) {
        for m in self.messages.drain(..) {
            store.release(m.slot);
        }
    }

    /// Applies an ack: everything below `ack_next` plus the bitmap words for
    /// `n_bits` sequences above it. Holes the ack reveals are resent.
    #[allow(clippy::too_many_arguments)]
    fn on_ack(
        &mut self,
        ack_next: u64,
        n_bits: u64,
        words: &[u8],
        rto: Duration,
        reorder: Duration,
        store: &mut MsgStore,
        fd: i32,
        to: &SockAddr,
        batch: &mut SendBatch,
        now: Instant,
    ) -> Acked {
        let mut acked = Acked { blocked: false, rtt: None };
        if ack_next > self.next_send {
            return acked;
        }
        let mut newest = None;
        let mut progressed = false;
        while self.base < ack_next {
            if !self.is_acked(self.base) {
                progressed = true;
                if self.slots[(self.base & self.mask) as usize].retries == 0 {
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
                // newer datagrams.
                if seq < self.base {
                    continue;
                }
                highest = Some(seq);
                if !self.is_acked(seq) {
                    progressed = true;
                    if self.slots[(seq & self.mask) as usize].retries == 0 {
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
            let slot = &self.slots[(seq & self.mask) as usize];
            acked.rtt = Some(now.saturating_sub(slot.sent_at));
        }
        self.release_messages(store);

        if let Some(end) = self.hole_scan_end() {
            let pred = |tx: &Self, seq: u64| tx.is_hole(seq, rto, reorder, now);
            let (_, blocked) = self.send_where(self.base..end, pred, store, fd, to, batch, now);
            acked.blocked = blocked;
        }
        acked
    }

    /// Sequences below the highest acked one are candidates for holes.
    #[inline]
    fn hole_scan_end(&self) -> Option<u64> {
        self.highest_acked.map(|h| h.min(self.next_send)).filter(|&end| end > self.base)
    }

    /// An unacked sequence the receiver has moved past. A first send counts
    /// as lost after a reordering window, a retransmit only after its
    /// backed-off RTO so its own ack has time to arrive through whatever
    /// queue delayed the original.
    #[inline]
    fn is_hole(&self, seq: u64, rto: Duration, reorder: Duration, now: Instant) -> bool {
        if self.is_acked(seq) {
            return false;
        }
        let slot = &self.slots[(seq & self.mask) as usize];
        let age = now.saturating_sub(slot.sent_at).0;
        if slot.retries == 0 {
            age >= reorder.0
        } else {
            age >= rto.0 << slot.retries.min(MAX_BACKOFF_SHIFT)
        }
    }

    /// Timer-driven recovery, scanned at most every `rto / 2`. Resends holes
    /// whose earlier resend was blocked or whose ack was lost, and after a
    /// full RTO of ack silence probes the oldest `recover` unacked datagrams.
    /// Silence never triggers more than that, so a burst queued behind a slow
    /// link is not blasted twice; each probe round doubles `recover`.
    #[allow(clippy::too_many_arguments)]
    fn retransmit_due(
        &mut self,
        rto: Duration,
        max_rto: Duration,
        reorder: Duration,
        store: &MsgStore,
        fd: i32,
        to: &SockAddr,
        batch: &mut SendBatch,
        now: Instant,
    ) -> (u64, bool) {
        if self.base == self.next_send || now.saturating_sub(self.last_scan) < rto / 2_u32 {
            return (0, false);
        }
        self.last_scan = now;
        let oldest = &self.slots[(self.base & self.mask) as usize];
        let backoff = (rto.0 << oldest.retries.min(MAX_BACKOFF_SHIFT)).min(max_rto.0).max(rto.0);
        let probing = now.saturating_sub(self.last_ack_at.max(oldest.sent_at)).0 >= backoff;
        let hole_end = self.hole_scan_end().unwrap_or(self.base);
        let mut probes = if probing { self.recover } else { 0 };
        if probing {
            self.recover = (self.recover * 2).min(MAX_RECOVER);
        }
        let pred = |tx: &Self, seq: u64| {
            if seq < hole_end && tx.is_hole(seq, rto, reorder, now) {
                return true;
            }
            if probes != 0 && !tx.is_acked(seq) {
                probes -= 1;
                return true;
            }
            false
        };
        self.send_where(self.base..self.next_send, pred, store, fd, to, batch, now)
    }

    /// Restarts every retained message from its first fragment under
    /// `session`: the new remote holds none of the old acks, and its
    /// reassembly needs whole messages.
    fn rewind(&mut self, session: u32) {
        self.acked.fill(0);
        self.base = self.retained_start();
        self.next_send = self.base;
        for seq in self.base..self.next {
            write_session(&mut self.slots[(seq & self.mask) as usize].header, session);
        }
        self.highest_acked = None;
        self.recover = 1;
        self.exceeded_since = None;
    }

    fn clear(&mut self, store: &mut MsgStore) {
        self.acked.fill(0);
        self.base = self.next;
        self.next_send = self.next;
        self.highest_acked = None;
        self.recover = 1;
        self.release_all(store);
        self.exceeded_since = None;
    }
}

/// A multi-fragment message being reassembled in owned memory (a dcache
/// reservation could be lapped while fragments are in flight). Recycled
/// whole; stale bytes are overwritten before delivery so nothing is zeroed.
struct Partial {
    first_seq: u64,
    len: usize,
    /// Kept at its largest past length; the message is `buf[..len]`.
    buf: Vec<u8>,
    remaining: usize,
    send_ts: u64,
}

/// Receive side: sequence tracking above the ack point plus in-progress
/// multi-fragment messages. A partial always straddles the ack point (its
/// missing fragment is unacked), so the window bounds how many can exist.
struct RxWindow {
    ack_next: u64,
    /// Highest sequence accepted; the ack bitmap runs from `ack_next + 1` to
    /// here.
    highest: u64,
    bits: Vec<u64>,
    mask: u64,
    capacity: u64,
    /// Most bits one ack can carry.
    max_bits: u64,
    partials: Vec<Partial>,
    spare: Vec<Partial>,
}

impl RxWindow {
    fn new(capacity: usize, max_bits: u64) -> Self {
        Self {
            ack_next: 0,
            highest: 0,
            bits: vec![0; capacity.div_ceil(u64::BITS as usize)],
            mask: capacity as u64 - 1,
            capacity: capacity as u64,
            max_bits: max_bits.min(capacity as u64 - 1),
            partials: Vec::new(),
            spare: Vec::new(),
        }
    }

    fn reset(&mut self, first_seq: u64) {
        self.ack_next = first_seq;
        self.highest = first_seq;
        self.bits.fill(0);
        self.spare.append(&mut self.partials);
    }

    #[inline]
    fn bit(&self, seq: u64) -> bool {
        let i = seq & self.mask;
        self.bits[(i / 64) as usize] & (1 << (i % 64)) != 0
    }

    #[inline]
    fn set_bit(&mut self, seq: u64, v: bool) {
        let i = seq & self.mask;
        let bit = 1 << (i % 64);
        let word = &mut self.bits[(i / 64) as usize];
        if v {
            *word |= bit;
        } else {
            *word &= !bit;
        }
    }

    /// Records `seq`; `false` for duplicates and out-of-window sequences.
    fn accept(&mut self, seq: u64) -> bool {
        if seq < self.ack_next || seq >= self.ack_next + self.capacity || self.bit(seq) {
            return false;
        }
        self.set_bit(seq, true);
        self.highest = self.highest.max(seq);
        while self.bit(self.ack_next) {
            self.set_bit(self.ack_next, false);
            self.ack_next += 1;
        }
        true
    }

    /// Bits an ack should carry: one per sequence in `ack_next + 1 ..=
    /// highest`, capped.
    #[inline]
    fn ack_bits(&self) -> u64 {
        self.highest.saturating_sub(self.ack_next).min(self.max_bits)
    }

    /// Writes `n_bits` of the ring starting at `ack_next + 1` into `out` as
    /// little-endian words. Bit `i` set: `ack_next + 1 + i` has arrived.
    fn write_bitmap(&self, n_bits: u64, out: &mut [u8]) -> usize {
        let words = n_bits.div_ceil(64) as usize;
        let ring_words = self.bits.len();
        for w in 0..words {
            let idx = (self.ack_next + 1 + 64 * w as u64) & self.mask;
            let (word, shift) = ((idx / 64) as usize, idx % 64);
            let mut v = self.bits[word] >> shift;
            if shift != 0 {
                v |= self.bits[(word + 1) % ring_words] << (64 - shift);
            }
            let valid = (n_bits - 64 * w as u64).min(64);
            if valid < 64 {
                v &= (1 << valid) - 1;
            }
            out[w * 8..w * 8 + 8].copy_from_slice(&v.to_le_bytes());
        }
        words * 8
    }
}

pub(crate) struct UdpPeer {
    pub(crate) addr: SocketAddr,
    pub(crate) token: Token,
    /// Socket this peer sends and receives on: its own token for outbound
    /// peers, the listener's for accepted ones.
    pub(crate) socket_token: Token,
    native_addr: SockAddr,
    connected: bool,
    ever_connected: bool,
    local_session: u32,
    remote_session: Option<u32>,
    stride: usize,
    max_message_size: usize,
    heartbeat: Duration,
    max_rto: Duration,
    tx: TxWindow,
    rx: RxWindow,
    rto: Rto,
    last_recv: Instant,
    last_send: Instant,
    hello_sent_at: Instant,
    hello_backoff: u8,
    ack_due: bool,
    latency: Option<Timer>,
    dropped_full: u64,
    last_warn: Instant,
    /// Control datagram staging: header plus the largest ack bitmap.
    ctrl: Vec<u8>,
}

impl UdpPeer {
    pub(crate) fn new(
        addr: SocketAddr,
        token: Token,
        socket_token: Token,
        local_session: u32,
        config: &UdpConfig,
        latency: Option<Timer>,
    ) -> Self {
        let now = Instant::now();
        // Whole 64-bit words of bitmap that fit in one datagram.
        let max_bits = (config.stride() / 8 * 64) as u64;
        let rx = RxWindow::new(config.recv_window, max_bits);
        Self {
            addr,
            token,
            socket_token,
            native_addr: SockAddr::new(addr),
            connected: false,
            ever_connected: false,
            local_session,
            remote_session: None,
            stride: config.stride(),
            max_message_size: config.max_message_size,
            heartbeat: config.heartbeat_interval,
            max_rto: config.max_rto,
            tx: TxWindow::new(config.send_window),
            ctrl: vec![0; HEADER_SIZE + rx.max_bits.div_ceil(64) as usize * 8],
            rx,
            rto: Rto::new(config),
            last_recv: now,
            last_send: now,
            hello_sent_at: Instant::ZERO,
            hello_backoff: 0,
            ack_due: false,
            latency,
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

    /// Sequences queued but not yet handed to the kernel.
    #[inline]
    pub(crate) fn unsent(&self) -> std::ops::Range<u64> {
        if self.connected { self.tx.next_send..self.tx.next } else { 0..0 }
    }

    #[inline]
    pub(crate) fn stage(&self, seq: u64, store: &MsgStore, batch: &mut SendBatch) {
        self.tx.stage(seq, store, &self.native_addr, batch);
    }

    #[inline]
    pub(crate) fn mark_sent(&mut self, seq: u64, now: Instant) {
        self.tx.mark_sent(seq, false, now);
        self.last_send = now;
    }

    /// Releases every store reference before the peer is dropped.
    pub(crate) fn release_all(&mut self, store: &mut MsgStore) {
        self.tx.release_all(store);
    }

    /// Drops the session and adopts `new_session` so the remote sees a fresh
    /// peer. Queued datagrams are kept for the next session unless
    /// `drop_backlog`; hello retries restart on the next tick.
    pub(crate) fn mark_disconnected(
        &mut self,
        drop_backlog: bool,
        new_session: u32,
        store: &mut MsgStore,
    ) {
        self.connected = false;
        self.remote_session = None;
        self.local_session = new_session;
        if drop_backlog {
            self.tx.clear(store);
        } else {
            self.tx.rewind(new_session);
        }
        self.hello_sent_at = Instant::ZERO;
        self.hello_backoff = 0;
    }

    /// Tells the sender of a datagram under `their_session` that the listener
    /// holds no peer for it.
    pub(crate) fn send_reset(socket: &UdpSocket, addr: SocketAddr, their_session: u32) {
        let mut buf = [0; HEADER_SIZE];
        Header { kind: Kind::Reset, session: 0, seq: 0, len: their_session, index: 0, send_ts: 0 }
            .encode(&mut buf);
        send_datagram(socket, addr, &buf);
    }

    /// A reset naming our current session means the remote holds no peer for
    /// this connection: renegotiate. Resets for earlier sessions are stale.
    #[inline]
    pub(crate) fn on_reset(&self, header: &Header) -> bool {
        self.connected && header.len == self.local_session
    }

    /// `len` carries the echoed remote session in a hello ack.
    fn send_control(
        &mut self,
        socket: &UdpSocket,
        kind: Kind,
        seq: u64,
        len: u32,
        now: Instant,
    ) -> SendOutcome {
        Header { kind, session: self.local_session, seq, len, index: 0, send_ts: 0 }
            .encode(&mut self.ctrl);
        self.last_send = now;
        send_datagram(socket, self.addr, &self.ctrl[..HEADER_SIZE])
    }

    pub(crate) fn send_hello(&mut self, socket: &UdpSocket, now: Instant) -> SendOutcome {
        self.hello_sent_at = now;
        self.send_control(socket, Kind::Hello, self.tx.base, 0, now)
    }

    /// Ack: `seq` is the cumulative point, `len` the bitmap bit count, the
    /// payload the bitmap words.
    pub(crate) fn send_ack(&mut self, socket: &UdpSocket, now: Instant) -> SendOutcome {
        let n_bits = self.rx.ack_bits();
        Header {
            kind: Kind::Ack,
            session: self.local_session,
            seq: self.rx.ack_next,
            len: n_bits as u32,
            index: 0,
            send_ts: 0,
        }
        .encode(&mut self.ctrl);
        let n = self.rx.write_bitmap(n_bits, &mut self.ctrl[HEADER_SIZE..]);
        self.ack_due = false;
        self.last_send = now;
        send_datagram(socket, self.addr, &self.ctrl[..HEADER_SIZE + n])
    }

    /// Stages the message in store `slot`.
    pub(crate) fn push_message(
        &mut self,
        store: &mut MsgStore,
        slot: u32,
        ts: Nanos,
        now: Instant,
    ) -> PushOutcome {
        let len = store.bytes(slot).len();
        if len > self.max_message_size {
            warn!(%self.addr, len, max = self.max_message_size, "udp message too large");
            return PushOutcome::TooLarge;
        }
        if self.tx.push(self.local_session, self.stride, store, slot, ts) {
            return PushOutcome::Queued;
        }
        self.dropped_full += 1;
        if now.saturating_sub(self.last_warn) >= Duration::from_secs(WARN_INTERVAL_SECS) {
            warn!(
                %self.addr,
                dropped = self.dropped_full,
                inflight = self.tx.inflight(),
                "udp send window full"
            );
            self.last_warn = now;
        }
        PushOutcome::WindowFull
    }

    /// Mirrors the TCP backlog rule on the in-flight datagram count.
    pub(crate) fn backlog_exceeded(&mut self, max_backlog: Option<(usize, Duration)>) -> bool {
        let Some((max, timeout)) = max_backlog else { return false };
        if self.tx.inflight() <= max as u64 {
            self.tx.exceeded_since = None;
            return false;
        }
        let now = Instant::now();
        let since = self.tx.exceeded_since.get_or_insert(now);
        now.saturating_sub(*since) >= timeout
    }

    /// Accepted side. Establishes the session and replies with a hello ack.
    /// `false` when the hello carries a different session than the one we
    /// hold: the remote restarted and this peer must be replaced.
    pub(crate) fn on_hello(&mut self, header: &Header, socket: &UdpSocket, now: Instant) -> bool {
        match self.remote_session {
            Some(s) if s != header.session => return false,
            Some(_) => {}
            None => {
                self.remote_session = Some(header.session);
                self.rx.reset(header.seq);
                self.connected = true;
                self.ever_connected = true;
            }
        }
        self.last_recv = now;
        self.send_control(socket, Kind::HelloAck, self.tx.base, header.session, now);
        true
    }

    /// Initiating side. `Some(was_connected_before)` when this completes a
    /// handshake; `None` if already connected or the ack answers an earlier
    /// attempt.
    pub(crate) fn on_hello_ack(&mut self, header: &Header, now: Instant) -> Option<bool> {
        if self.connected || header.len != self.local_session {
            return None;
        }
        self.last_recv = now;
        self.remote_session = Some(header.session);
        self.rx.reset(header.seq);
        self.connected = true;
        let was = std::mem::replace(&mut self.ever_connected, true);
        self.hello_backoff = 0;
        Some(was)
    }

    /// Datagrams from any other session are stale: a restarted remote shows
    /// up through a hello or a reset, never through data.
    #[inline]
    fn current_session(&self, header: &Header) -> bool {
        self.connected && self.remote_session == Some(header.session)
    }

    /// Ingests a data fragment, delivering any message it completes.
    pub(crate) fn on_data<F>(
        &mut self,
        header: &Header,
        payload: &[u8],
        dcache: Option<&DCache>,
        now: Instant,
        deliver: &mut F,
    ) where
        F: FnMut(RxPayload<'_>, Nanos),
    {
        if !self.current_session(header) {
            return;
        }
        self.last_recv = now;
        let len = header.len as usize;
        let index = usize::from(header.index);
        let offset = index * self.stride;
        let total = fragment_count(len, self.stride);
        // Validate before the sequence is committed: a bad datagram must not
        // consume the sequence a good retry will carry.
        if len == 0 ||
            len > self.max_message_size ||
            index >= total ||
            header.seq < index as u64 ||
            payload.len() != self.stride.min(len - offset)
        {
            debug!(%self.addr, len, index, got = payload.len(), "udp fragment header invalid");
            return;
        }
        // Never acked, so the sender's backlog policy surfaces the mismatch.
        if dcache.is_some_and(|dc| len > dc.capacity()) {
            warn!(%self.addr, len, "udp message exceeds dcache capacity, not acked");
            return;
        }
        self.ack_due = true;
        if !self.rx.accept(header.seq) {
            return;
        }
        let send_ts = Nanos(header.send_ts);

        if total == 1 {
            match dcache {
                None => self.deliver(RxPayload::Raw(payload), send_ts, deliver),
                Some(dc) => match dc.write(len, |buf| buf.copy_from_slice(payload)) {
                    Ok(dref) => self.deliver(RxPayload::DCache(dref), send_ts, deliver),
                    Err(e) => warn!("dcache write failed: {e}"),
                },
            }
            return;
        }

        let first_seq = header.seq - index as u64;
        let pos = self.partial_index(first_seq, len, total, header.send_ts);
        let partial = &mut self.rx.partials[pos];
        if partial.len != len {
            debug!(%self.addr, "udp fragment disagrees on message length");
            return;
        }
        // No per-fragment dedup needed: the window rejects duplicate sequences
        // and a fragment index maps to exactly one sequence of its message.
        partial.remaining -= 1;
        partial.buf[offset..offset + payload.len()].copy_from_slice(payload);
        if partial.remaining != 0 {
            return;
        }

        let done = self.rx.partials.swap_remove(pos);
        let send_ts = Nanos(done.send_ts);
        let bytes = &done.buf[..len];
        match dcache {
            None => self.deliver(RxPayload::Raw(bytes), send_ts, deliver),
            Some(dc) => match dc.write(len, |buf| buf.copy_from_slice(bytes)) {
                Ok(dref) => self.deliver(RxPayload::DCache(dref), send_ts, deliver),
                Err(e) => warn!("dcache write failed: {e}"),
            },
        }
        self.rx.spare.push(done);
    }

    /// Finds or creates the partial for `first_seq`.
    fn partial_index(&mut self, first_seq: u64, len: usize, total: usize, send_ts: u64) -> usize {
        if let Some(pos) = self.rx.partials.iter().position(|p| p.first_seq == first_seq) {
            return pos;
        }
        let mut partial = self.rx.spare.pop().unwrap_or(Partial {
            first_seq: 0,
            len: 0,
            buf: Vec::new(),
            remaining: 0,
            send_ts: 0,
        });
        if partial.buf.len() < len {
            partial.buf.resize(len, 0);
        }
        partial.first_seq = first_seq;
        partial.len = len;
        partial.remaining = total;
        partial.send_ts = send_ts;
        self.rx.partials.push(partial);
        self.rx.partials.len() - 1
    }

    #[inline]
    fn deliver<F>(&mut self, payload: RxPayload<'_>, send_ts: Nanos, deliver: &mut F)
    where
        F: FnMut(RxPayload<'_>, Nanos),
    {
        if let Some(t) = &mut self.latency {
            t.emit_latency_from_nanos(send_ts, Nanos::now());
        }
        deliver(payload, send_ts);
    }

    /// Ingests an ack; reports whether the socket blocked while resending.
    pub(crate) fn on_ack(
        &mut self,
        header: &Header,
        payload: &[u8],
        store: &mut MsgStore,
        socket: &UdpSocket,
        batch: &mut SendBatch,
        now: Instant,
    ) -> SendOutcome {
        if !self.current_session(header) {
            return SendOutcome::Done;
        }
        self.last_recv = now;
        let acked = self.tx.on_ack(
            header.seq,
            u64::from(header.len),
            payload,
            self.rto.current(),
            self.rto.reorder_window(),
            store,
            socket.as_raw_fd(),
            &self.native_addr,
            batch,
            now,
        );
        if let Some(rtt) = acked.rtt {
            self.rto.sample(rtt);
        }
        if acked.blocked { SendOutcome::WouldBlock } else { SendOutcome::Done }
    }

    /// Periodic work: hello retries while disconnected; retransmits and
    /// heartbeats while connected. `Err(())` means the peer timed out.
    pub(crate) fn tick(
        &mut self,
        store: &MsgStore,
        socket: &UdpSocket,
        batch: &mut SendBatch,
        now: Instant,
        peer_timeout: Duration,
    ) -> Result<SendOutcome, ()> {
        if !self.connected {
            let interval = Duration(
                (self.rto.current().0 << self.hello_backoff.min(MAX_BACKOFF_SHIFT))
                    .min(self.max_rto.0),
            );
            if now.saturating_sub(self.hello_sent_at) >= interval {
                self.hello_backoff = self.hello_backoff.saturating_add(1);
                return Ok(self.send_hello(socket, now));
            }
            return Ok(SendOutcome::Done);
        }
        if now.saturating_sub(self.last_recv) >= peer_timeout {
            return Err(());
        }
        let (resent, blocked) = self.tx.retransmit_due(
            self.rto.current(),
            self.max_rto,
            self.rto.reorder_window(),
            store,
            socket.as_raw_fd(),
            &self.native_addr,
            batch,
            now,
        );
        if resent != 0 {
            self.last_send = now;
        }
        if blocked {
            return Ok(SendOutcome::WouldBlock);
        }
        if self.ack_due || now.saturating_sub(self.last_send) >= self.heartbeat {
            return Ok(self.send_ack(socket, now));
        }
        Ok(SendOutcome::Done)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> UdpConfig {
        UdpConfig {
            send_window: 64,
            recv_window: 64,
            max_message_size: 64 * 1173,
            ..UdpConfig::lan()
        }
    }

    fn sock() -> (UdpSocket, SockAddr) {
        let s = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let to = SockAddr::new(s.local_addr().unwrap());
        (s, to)
    }

    /// Stores a payload and pushes it, dropping the owner reference.
    fn push(tx: &mut TxWindow, store: &mut MsgStore, stride: usize, payload: &[u8]) -> bool {
        let slot = store.insert(&mut payload.to_vec());
        let ok = tx.push(1, stride, store, slot, Nanos(1));
        store.release(slot);
        ok
    }

    /// Marks `range` as sent for the first time, as the manager's flush does.
    fn send_all(tx: &mut TxWindow, range: std::ops::Range<u64>) {
        let now = Instant::now();
        for seq in range {
            tx.mark_sent(seq, false, now);
        }
    }

    #[test]
    fn rto_tracks_samples_within_clamps() {
        let c = cfg();
        let mut rto = Rto::new(&c);
        assert_eq!(rto.current(), c.initial_rto);
        rto.sample(Duration::from_micros(10));
        assert_eq!(rto.current(), c.min_rto);
        for _ in 0..50 {
            rto.sample(Duration::from_millis(200));
        }
        assert_eq!(rto.current(), c.max_rto);
    }

    #[test]
    fn tx_window_capacity_and_ack() {
        let config = cfg();
        let stride = config.stride();
        let mut store = MsgStore::new();
        let mut tx = TxWindow::new(config.send_window);
        assert!(push(&mut tx, &mut store, stride, &vec![0; stride * 3]));
        assert!(push(&mut tx, &mut store, stride, &vec![7; stride * 5]));
        assert_eq!(tx.free(), 56);
        assert!(!push(&mut tx, &mut store, stride, &vec![0; stride * 57]));
        assert_eq!(store.free.len(), 1, "unqueued payload recycled");
        assert_eq!(tx.messages.len(), 2);
        assert_eq!(payload_of(&store, &tx.slots[4]), &vec![7; stride][..]);

        let (s, to) = sock();
        let mut batch = SendBatch::new();
        let now = Instant::now();
        let fd = s.as_raw_fd();
        send_all(&mut tx, 0..8);
        assert_eq!(tx.next_send, 8);
        // Ack 0..3 cumulatively, 5 and 7 selectively: 3, 4, 6 are holes.
        let bits = 0b0000_1010_u64.to_le_bytes();
        let zero = Duration::ZERO;
        let acked = tx.on_ack(3, 8, &bits, zero, zero, &mut store, fd, &to, &mut batch, now);
        assert!(!acked.blocked);
        assert!(acked.rtt.is_some());
        assert_eq!(tx.base, 3);
        assert_eq!(tx.messages.len(), 1, "first message released");
        assert_eq!(store.free.len(), 2);
        assert_eq!(tx.free(), 59, "slots of the unfinished message stay reserved");
        assert!(tx.is_acked(5) && tx.is_acked(7) && !tx.is_acked(4));
        let acked = tx.on_ack(8, 0, &[], zero, zero, &mut store, fd, &to, &mut batch, now);
        assert!(acked.rtt.is_none(), "holes were retransmitted, no clean sample");
        assert_eq!(tx.free(), 64);
        assert!(tx.messages.is_empty());
        assert_eq!(store.free.len(), 3);
    }

    #[test]
    fn stale_ack_cannot_touch_reused_slots() {
        let config = cfg();
        let stride = config.stride();
        let mut store = MsgStore::new();
        let mut tx = TxWindow::new(config.send_window);
        let (s, to) = sock();
        let fd = s.as_raw_fd();
        let mut batch = SendBatch::new();
        let now = Instant::now();
        let zero = Duration::ZERO;
        for _ in 0..64 {
            assert!(push(&mut tx, &mut store, stride, &[1]));
        }
        send_all(&mut tx, 0..64);
        tx.on_ack(64, 0, &[], zero, zero, &mut store, fd, &to, &mut batch, now);
        assert_eq!(tx.base, 64);
        for _ in 0..8 {
            assert!(push(&mut tx, &mut store, stride, &[2]));
        }
        send_all(&mut tx, 64..72);
        // Old ack: cumulative 1, selective bit for seq 2, which aliases seq 66.
        let bits = 0b1_u64.to_le_bytes();
        tx.on_ack(1, 8, &bits, zero, zero, &mut store, fd, &to, &mut batch, now);
        assert_eq!(tx.base, 64);
        assert!(!tx.is_acked(66));
    }

    #[test]
    fn rewind_replays_whole_messages_under_new_session() {
        let config = cfg();
        let stride = config.stride();
        let mut store = MsgStore::new();
        let mut tx = TxWindow::new(config.send_window);
        let (s, to) = sock();
        let fd = s.as_raw_fd();
        let mut batch = SendBatch::new();
        let now = Instant::now();
        let zero = Duration::ZERO;
        assert!(push(&mut tx, &mut store, stride, &vec![0; stride * 4]));
        send_all(&mut tx, 0..4);
        tx.on_ack(2, 0, &[], zero, zero, &mut store, fd, &to, &mut batch, now);
        assert_eq!(tx.base, 2);
        tx.rewind(0xabcd);
        assert_eq!((tx.base, tx.next_send), (0, 0), "restarts from the first fragment");
        for seq in 0..4 {
            assert_eq!(Header::decode(&tx.slots[seq].header).unwrap().session, 0xabcd);
        }
    }

    #[test]
    fn rx_window_accepts_once_and_reports_bitmap() {
        let mut rx = RxWindow::new(64, 64);
        rx.reset(10);
        assert_eq!(rx.ack_bits(), 0);
        assert!(rx.accept(11));
        assert!(!rx.accept(11));
        assert!(rx.accept(13));
        assert_eq!(rx.ack_next, 10);
        assert_eq!(rx.ack_bits(), 3);
        let mut out = [0u8; 8];
        assert_eq!(rx.write_bitmap(rx.ack_bits(), &mut out), 8);
        assert_eq!(u64::from_le_bytes(out), 0b101);
        assert!(rx.accept(10));
        assert_eq!(rx.ack_next, 12);
        assert!(!rx.accept(9));
        assert!(!rx.accept(12 + 64));
        assert!(rx.accept(12 + 63));
    }

    #[test]
    fn bitmap_spans_ring_words() {
        let mut rx = RxWindow::new(256, 1024);
        rx.reset(100);
        for seq in (101..250).step_by(3) {
            assert!(rx.accept(seq));
        }
        let n = rx.ack_bits();
        assert_eq!(n, 248 - 100);
        let mut out = [0u8; 32];
        assert_eq!(rx.write_bitmap(n, &mut out), 24);
        for i in 0..n {
            let word = u64::from_le_bytes(out[(i / 64 * 8) as usize..][..8].try_into().unwrap());
            let seq = 101 + i;
            assert_eq!(word >> (i % 64) & 1 == 1, (seq - 101) % 3 == 0, "seq {seq}");
        }
    }
}
