//! The sending half of a peer. A packet is byte-identical on every send, so
//! the receiver dedups by sequence; its bytes live in the shared
//! [`MessageStore`] and are held until the packet is acked.

use std::collections::VecDeque;

use flux_communication::queue::{Producer, Queue, QueueType};
use flux_timing::{Duration, Instant, Nanos};
use flux_utils::directories::{local_share_dir, shmem_dir_queues_with_base};

use super::{
    sys::{SendBatch, SockAddr},
    wire::{self, LONG_HEADER, PACKET_HEADER, PADDING},
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
/// Most streams one packet carries spans of; it closes short at this many,
/// which bounds the slice ring at this multiple of the packet window.
const MAX_SLICES_PER_PACKET: u16 = 4;

/// Message bytes shared by every peer with packets of them in flight, so a
/// broadcast is stored once and referenced per peer.
pub(crate) struct MessageStore {
    entries: Vec<Entry>,
    free: Vec<u32>,
}

struct Entry {
    bytes: Vec<u8>,
    refs: u32,
}

impl MessageStore {
    pub(crate) fn new() -> Self {
        Self { entries: Vec::new(), free: Vec::new() }
    }

    /// Takes `payload`'s buffer, leaving a recycled one in its place. The
    /// caller holds one reference and must [`Self::release`] it.
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
    pub(crate) fn current(&self) -> Duration {
        Duration(self.value)
    }

    /// How long an unacked packet below the highest ack may lag before it
    /// counts as lost rather than reordered: a quarter RTT.
    #[inline]
    fn reorder_window(&self) -> Duration {
        Duration(self.srtt.unwrap_or(self.value / 2) / 4)
    }
}

struct Message {
    slot: u32,
    first_packet: u64,
    last_packet: u64,
}

#[derive(Clone, Copy, Default)]
struct Slice {
    slot: u32,
    offset: u32,
    len: u16,
    /// 1 + offset within the span of the first record header; 0 without one.
    start: u16,
}

struct Records<'a> {
    stream: &'a [u8],
    start: usize,
    header: usize,
    body: usize,
}

impl<'a> Records<'a> {
    fn new(stream: &'a [u8]) -> Self {
        let (header, body) = wire::record_len(stream);
        Self { stream, start: 0, header, body }
    }

    #[inline]
    fn end(&self) -> usize {
        self.start + self.header + self.body
    }

    fn next(&mut self) {
        self.start = self.end();
        (self.header, self.body) = wire::record_len(&self.stream[self.start..]);
    }

    /// Where to end a span starting at `pos` that may run to `want`: never
    /// inside a record header or right after one, so such an end moves back
    /// to the header. Also the first record start inside the span.
    fn cut(&mut self, pos: usize, want: usize) -> (usize, Option<usize>) {
        while self.end() <= pos && self.end() < self.stream.len() {
            self.next();
        }
        let mut first = None;
        let mut end = want;
        loop {
            let (rs, re) = (self.start, self.end());
            if rs >= pos && rs < end {
                first.get_or_insert(rs);
            }
            let past_header = rs + self.header + usize::from(self.body > 0);
            if end > rs && end < past_header {
                end = rs;
                if first == Some(rs) {
                    first = None;
                }
                break;
            }
            if end <= re || re >= self.stream.len() {
                break;
            }
            self.next();
        }
        (end, first)
    }
}

#[derive(Clone, Copy, Default)]
struct Packet {
    /// Index of its first slice in the slice ring.
    records: u32,
    count: u16,
    /// Size on the wire, padding included.
    bytes: u16,
    /// 1 + payload offset of the first message header; 0 without one.
    start: u16,
    pad: u8,
    retries: u8,
    /// Staged into a batch whose result is not in yet.
    staged: bool,
    send_ts: Nanos,
    sent_at: Instant,
}

/// No room in the send window for the stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WindowFull;

/// What an ack told the sender.
pub(crate) struct Acked {
    /// Round trip of the newest packet acked for the first time that was
    /// never retransmitted (Karn's rule).
    pub(crate) rtt: Option<Duration>,
}

pub(crate) struct Outbound {
    datagram_size: usize,
    packets: Box<[Packet]>,
    acked: Box<[u64]>,
    mask: u64,
    /// Oldest unacked packet.
    base: u64,
    /// First packet never handed to the kernel.
    next_send: u64,
    /// Next packet to build. The last built one stays open for more records
    /// until it is full or handed over.
    next: u64,
    open: bool,
    slices: Box<[Slice]>,
    slice_mask: u32,
    slice_next: u32,
    messages: VecDeque<Message>,
    /// Highest sequence the receiver has ever acked. Unacked sequences below
    /// it are holes the receiver has moved past; above it, silence proves
    /// nothing (they may be queued behind a slow link).
    highest_acked: Option<u64>,
    /// When an ack last acked something new.
    last_ack_at: Instant,
    /// Packets the next timeout probe resends; doubles per consecutive probe
    /// round and resets when acks flow again, like slow start after an RTO.
    recover: u64,
    last_scan: Instant,
    exceeded_since: Option<Instant>,
    /// Sequences put in the current batch, in order, until `commit`.
    staged: Vec<u64>,
    rto: Rto,
    sends: Option<Producer<UdpSend>>,
    retransmits: Option<Producer<UdpRetransmit>>,
}

impl Outbound {
    pub(crate) fn new(config: &UdpGroupConfig, label: &str) -> Self {
        let udp = config.udp;
        let capacity = udp.send_window;
        let slice_capacity = capacity * usize::from(MAX_SLICES_PER_PACKET);
        Self {
            datagram_size: udp.max_datagram_size,
            packets: vec![Packet::default(); capacity].into_boxed_slice(),
            acked: vec![0; capacity.div_ceil(u64::BITS as usize)].into_boxed_slice(),
            mask: capacity as u64 - 1,
            base: 0,
            next_send: 0,
            next: 0,
            open: false,
            slices: vec![Slice::default(); slice_capacity].into_boxed_slice(),
            slice_mask: slice_capacity as u32 - 1,
            slice_next: 0,
            messages: VecDeque::new(),
            highest_acked: None,
            last_ack_at: Instant::ZERO,
            recover: 1,
            last_scan: Instant::ZERO,
            exceeded_since: None,
            staged: Vec::new(),
            rto: Rto::new(udp.initial_rto, udp.min_rto, udp.max_rto),
            sends: telemetry_producer(config.telemetry, "sends", label),
            retransmits: telemetry_producer(config.telemetry, "retransmits", label),
        }
    }

    #[inline]
    pub(crate) fn rto(&self) -> &Rto {
        &self.rto
    }

    #[inline]
    pub(crate) fn base(&self) -> u64 {
        self.base
    }

    #[inline]
    pub(crate) fn inflight(&self) -> u64 {
        self.next - self.base
    }

    #[inline]
    fn packet(&self, seq: u64) -> &Packet {
        &self.packets[(seq & self.mask) as usize]
    }

    #[inline]
    fn packet_mut(&mut self, seq: u64) -> &mut Packet {
        &mut self.packets[(seq & self.mask) as usize]
    }

    /// First packet whose slot is still needed: a reconnect replays whole
    /// messages, so slots stay reserved back to the oldest retained one.
    #[inline]
    fn retained_start(&self) -> u64 {
        self.messages.front().map_or(self.base, |m| m.first_packet)
    }

    #[inline]
    fn free_packets(&self) -> u64 {
        self.mask + 1 - (self.next - self.retained_start())
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

    pub(crate) fn oldest_retained<'a>(&self, store: &'a MessageStore) -> Option<&'a [u8]> {
        self.messages.front().map(|m| store.bytes(m.slot))
    }

    fn open_packet(&mut self) -> u64 {
        let seq = self.next;
        self.next += 1;
        *self.packet_mut(seq) =
            Packet { records: self.slice_next, bytes: PACKET_HEADER as u16, ..Packet::default() };
        self.open = true;
        seq
    }

    fn push_slice(&mut self, seq: u64, slice: Slice) {
        self.slices[(self.slice_next & self.slice_mask) as usize] = slice;
        self.slice_next = self.slice_next.wrapping_add(1);
        let packet = self.packet_mut(seq);
        if slice.start != 0 && packet.start == 0 {
            packet.start = packet.bytes - PACKET_HEADER as u16 + slice.start;
        }
        packet.count += 1;
        packet.bytes += slice.len;
    }

    /// Queues the stream in store `slot`, cutting it into packets behind
    /// whatever is already queued, and takes a reference on it.
    pub(crate) fn enqueue(
        &mut self,
        store: &mut MessageStore,
        slot: u32,
    ) -> Result<(), WindowFull> {
        let stream = store.bytes(slot);
        let len = stream.len();
        // A sliver in the open packet, full packets, a sliver at the end;
        // a cut moved back before a header wastes at most a header.
        let room = self.datagram_size - PACKET_HEADER - LONG_HEADER;
        let packets = (len / room + 2) as u64;
        if packets > self.free_packets() {
            return Err(WindowFull);
        }
        let mut records = Records::new(stream);
        let mut pos = 0usize;
        let mut first_packet = None;
        loop {
            let seq = if self.open { self.next - 1 } else { self.open_packet() };
            let space = self.datagram_size - usize::from(self.packet(seq).bytes);
            let (end, start) = records.cut(pos, (pos + space).min(len));
            if end == pos {
                // Not even a header and a byte fit: pad the packet full so
                // it still segments with its neighbours.
                let packet = self.packet_mut(seq);
                packet.pad = space as u8;
                packet.bytes += space as u16;
            } else {
                let start = start.map_or(0, |s| (s - pos + 1) as u16);
                let slice = Slice { slot, offset: pos as u32, len: (end - pos) as u16, start };
                self.push_slice(seq, slice);
                first_packet.get_or_insert(seq);
                pos = end;
            }
            let packet = self.packet(seq);
            if usize::from(packet.bytes) == self.datagram_size ||
                packet.count == MAX_SLICES_PER_PACKET
            {
                self.open = false;
            }
            if pos == len {
                break;
            }
        }
        store.add_ref(slot);
        self.messages.push_back(Message {
            slot,
            first_packet: first_packet.unwrap(),
            last_packet: self.next - 1,
        });
        Ok(())
    }

    /// Mirrors the TCP backlog rule on the in-flight packet count.
    pub(crate) fn backlog_exceeded(
        &mut self,
        max_backlog: Option<(usize, Duration)>,
        now: Instant,
    ) -> bool {
        let Some((max, timeout)) = max_backlog else { return false };
        if self.inflight() <= max as u64 {
            self.exceeded_since = None;
            return false;
        }
        let since = self.exceeded_since.get_or_insert(now);
        now.saturating_sub(*since) >= timeout
    }

    fn release_messages(&mut self, store: &mut MessageStore) {
        while self.messages.front().is_some_and(|m| m.last_packet < self.base) {
            store.release(self.messages.pop_front().unwrap().slot);
        }
    }

    pub(crate) fn release_all(&mut self, store: &mut MessageStore) {
        for m in self.messages.drain(..) {
            store.release(m.slot);
        }
    }

    /// Drops every stream none of whose bytes has been handed to the
    /// kernel, keeping the ones already partly on the wire. Returns how many.
    pub(crate) fn clear_unsent(&mut self, store: &mut MessageStore) -> usize {
        let mut dropped = 0;
        while let Some(m) = self.messages.back() &&
            m.first_packet >= self.next_send
        {
            let m = self.messages.pop_back().unwrap();
            store.release(m.slot);
            // Rebuild the packet it started in from the slices before its
            // own: a kept message may still end there.
            let seq = m.first_packet;
            let packet = *self.packet(seq);
            let mask = self.slice_mask;
            let at = |i: u16| ((packet.records + u32::from(i)) & mask) as usize;
            let kept = (0..packet.count).take_while(|&i| self.slices[at(i)].slot != m.slot).count();
            self.slice_next = packet.records;
            self.next = seq;
            self.open = false;
            if kept != 0 {
                self.open_packet();
                for i in 0..kept as u16 {
                    let slice = self.slices[at(i)];
                    self.push_slice(seq, slice);
                }
            }
            dropped += 1;
        }
        dropped
    }

    /// Restarts every retained message from its first packet: the new
    /// remote holds none of the old acks.
    pub(crate) fn rewind(&mut self) {
        self.acked.fill(0);
        self.base = self.retained_start();
        self.next_send = self.base;
        for seq in self.base..self.next {
            self.packet_mut(seq).retries = 0;
        }
        self.highest_acked = None;
        self.recover = 1;
        self.exceeded_since = None;
    }

    /// Forgets everything queued or in flight.
    pub(crate) fn clear(&mut self, store: &mut MessageStore) {
        self.acked.fill(0);
        self.base = self.next;
        self.next_send = self.next;
        self.open = false;
        self.highest_acked = None;
        self.recover = 1;
        self.release_all(store);
        self.exceeded_since = None;
    }

    /// Applies an ack: everything below `ack_next` plus the bitmap words for
    /// `n_bits` sequences above it.
    pub(crate) fn on_ack(
        &mut self,
        ack_next: u64,
        n_bits: u64,
        words: &[u8],
        store: &mut MessageStore,
        now: Instant,
    ) -> Acked {
        let mut acked = Acked { rtt: None };
        if ack_next > self.next_send {
            return acked;
        }
        let mut newest = None;
        let mut progressed = false;
        while self.base < ack_next {
            if !self.is_acked(self.base) {
                progressed = true;
                if self.packet(self.base).retries == 0 {
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
                    if self.packet(seq).retries == 0 {
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
            let rtt = now.saturating_sub(self.packet(seq).sent_at);
            self.rto.sample(rtt);
            acked.rtt = Some(rtt);
        }
        self.release_messages(store);
        acked
    }

    /// Sequences below the highest acked one are candidates for holes.
    #[inline]
    fn hole_scan_end(&self) -> Option<u64> {
        self.highest_acked.map(|h| h.min(self.next_send)).filter(|&end| end > self.base)
    }

    /// An unacked sequence the receiver has moved past. A first send counts
    /// as lost after a reordering window, or at once when the receiver is
    /// [`FAST_RETRANSMIT_GAP`] packets beyond it; a retransmit only after its
    /// backed-off RTO so its own ack has time to arrive through whatever
    /// queue delayed the original.
    #[inline]
    fn is_hole(&self, seq: u64, now: Instant) -> bool {
        if self.is_acked(seq) {
            return false;
        }
        let packet = self.packet(seq);
        if packet.staged {
            return false;
        }
        let age = now.saturating_sub(packet.sent_at).0;
        if packet.retries == 0 {
            age >= self.rto.reorder_window().0 ||
                self.highest_acked.is_some_and(|h| h.saturating_sub(seq) >= FAST_RETRANSMIT_GAP)
        } else {
            age >= self.rto.current().0 << packet.retries.min(MAX_BACKOFF_SHIFT)
        }
    }

    /// Timer-driven recovery budget, granted at most every `rto / 2`: after
    /// a full RTO of ack silence, the oldest `recover` unacked packets are
    /// probed. Silence never triggers more than that, so a burst queued
    /// behind a slow link is not blasted twice; each probe round doubles
    /// `recover`.
    pub(crate) fn probe_budget(&mut self, now: Instant) -> u64 {
        let rto = self.rto.current();
        if self.base == self.next_send || now.saturating_sub(self.last_scan) < rto / 2_u32 {
            return 0;
        }
        self.last_scan = now;
        let oldest = self.packet(self.base);
        let backoff = (rto.0 << oldest.retries.min(MAX_BACKOFF_SHIFT)).min(self.rto.max).max(rto.0);
        if now.saturating_sub(self.last_ack_at.max(oldest.sent_at)).0 < backoff {
            return 0;
        }
        let probes = self.recover;
        self.recover = (self.recover * 2).min(MAX_RECOVER);
        probes
    }

    /// Puts packets into `batch` until it is full or nothing is due: holes
    /// the receiver has moved past, then `probes` oldest unacked packets,
    /// then whatever was never sent. [`Self::commit`] records what the
    /// kernel took.
    pub(crate) fn stage(
        &mut self,
        store: &MessageStore,
        batch: &mut SendBatch,
        to: &SockAddr,
        session: u32,
        mut probes: u64,
        now: Instant,
    ) -> usize {
        let before = self.staged.len();
        let send_ts = Nanos::now();
        let hole_end = self.hole_scan_end().unwrap_or(self.base);
        // Probes may reach anything in flight; holes only lie below the
        // highest ack.
        let end = if probes != 0 { self.next_send } else { hole_end };
        let mut seq = self.base;
        while seq < end && !batch.is_full() {
            let hole = seq < hole_end && self.is_hole(seq, now);
            let probe = !hole && probes != 0 && !self.is_acked(seq) && !self.packet(seq).staged;
            if hole || probe {
                self.put(store, batch, to, session, seq, send_ts);
                probes -= u64::from(probe);
            }
            seq += 1;
        }
        let mut seq = self.next_send;
        while seq < self.next && !batch.is_full() {
            if !self.packet(seq).staged {
                self.put(store, batch, to, session, seq, send_ts);
            }
            seq += 1;
        }
        self.staged.len() - before
    }

    /// Builds packet `seq` into the batch; `send_ts` stamps a first send.
    fn put(
        &mut self,
        store: &MessageStore,
        batch: &mut SendBatch,
        to: &SockAddr,
        session: u32,
        seq: u64,
        send_ts: Nanos,
    ) {
        let first_send = seq >= self.next_send;
        let packet = self.packet_mut(seq);
        if first_send {
            packet.send_ts = send_ts;
        }
        packet.staged = true;
        let packet = *packet;
        batch.open(to);
        let mut header = [0; PACKET_HEADER];
        wire::packet_header(&mut header, session, seq, packet.send_ts.0, packet.start);
        batch.copy(&header);
        for i in 0..packet.count {
            let slice = self.slices[((packet.records + u32::from(i)) & self.slice_mask) as usize];
            let bytes = store.bytes(slice.slot);
            batch.payload(&bytes[slice.offset as usize..][..usize::from(slice.len)]);
        }
        if packet.pad != 0 {
            batch.copy(&PADDING[..usize::from(packet.pad)]);
        }
        batch.close();
        self.staged.push(seq);
        // Once handed over, the open packet takes no more records.
        if seq + 1 == self.next {
            self.open = false;
        }
    }

    /// Records that the kernel accepted the first `accepted` staged packets
    /// and forgets the rest of the staging, which stays due.
    pub(crate) fn commit(&mut self, accepted: usize, now: Instant) {
        for i in 0..self.staged.len() {
            let seq = self.staged[i];
            let first_send = seq >= self.next_send;
            let packet = self.packet_mut(seq);
            packet.staged = false;
            if i >= accepted {
                continue;
            }
            packet.sent_at = now;
            if first_send {
                let bytes = u32::from(packet.bytes);
                self.next_send = seq + 1;
                if let Some(sends) = &mut self.sends {
                    sends.produce(&UdpSend { sent_at: now, bytes, datagrams: 1 });
                }
            } else {
                packet.retries = packet.retries.saturating_add(1);
                let (bytes, retries) = (u32::from(packet.bytes), packet.retries);
                if let Some(retransmits) = &mut self.retransmits {
                    retransmits.produce(&UdpRetransmit { sent_at: now, bytes, retries });
                }
            }
        }
        self.staged.clear();
    }

    /// Unreliable: handed to the kernel is as good as acked.
    pub(crate) fn forget_sent(&mut self, store: &mut MessageStore) {
        self.base = self.next_send;
        self.release_messages(store);
    }
}
