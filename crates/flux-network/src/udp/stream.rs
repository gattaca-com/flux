//! The publisher's stream: messages cut into packets once and sent byte for
//! byte to every subscriber. A packet's bytes live in the shared
//! [`MessageStore`] until every subscriber is done with it.

use std::collections::VecDeque;

use flux_timing::Nanos;

use super::{
    UdpConfig,
    sys::{SendBatch, SockAddr},
    wire::{self, PACKET_HEADER},
};

/// Most streams one packet carries spans of; it closes short at this many,
/// which bounds the slice ring at this multiple of the packet window.
const MAX_SLICES_PER_PACKET: u16 = 4;

/// Message bytes the stream holds until every packet of them is released.
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
    fn bytes(&self, slot: u32) -> &[u8] {
        &self.entries[slot as usize].bytes
    }
}

struct Message {
    slot: u32,
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

/// Record starts of a stream, walked forward once.
struct Records<'a> {
    stream: &'a [u8],
    start: usize,
}

impl<'a> Records<'a> {
    fn new(stream: &'a [u8]) -> Self {
        Self { stream, start: 0 }
    }

    /// The first record start in `pos..end`; calls must not move backwards.
    fn first(&mut self, pos: usize, end: usize) -> Option<usize> {
        while self.start < pos {
            let len = self.stream.get(self.start..).map_or(0, wire::record_len);
            if len == 0 {
                return None;
            }
            self.start += len;
        }
        (self.start < end).then_some(self.start)
    }
}

#[derive(Clone, Copy, Default)]
struct Packet {
    /// Index of its first slice in the slice ring.
    records: u32,
    count: u16,
    /// Size on the wire.
    bytes: u16,
    /// 1 + payload offset of the first message header; 0 without one.
    start: u16,
    /// Stamped when the packet is first staged for anyone, so every copy
    /// carries the same bytes.
    send_ts: Nanos,
}

pub(crate) struct Stream {
    session: u32,
    datagram_size: usize,
    packets: Box<[Packet]>,
    mask: u64,
    /// Oldest packet still held.
    base: u64,
    /// Next packet to build. The last built one stays open for more records
    /// until it is full, first staged, or a subscriber joins.
    next: u64,
    open: bool,
    slices: Box<[Slice]>,
    slice_mask: u32,
    slice_next: u32,
    messages: VecDeque<Message>,
}

impl Stream {
    pub(crate) fn new(udp: &UdpConfig, session: u32) -> Self {
        let capacity = udp.send_window;
        let slice_capacity = capacity * usize::from(MAX_SLICES_PER_PACKET);
        Self {
            session,
            datagram_size: udp.max_datagram_size,
            packets: vec![Packet::default(); capacity].into_boxed_slice(),
            mask: capacity as u64 - 1,
            base: 0,
            next: 0,
            open: false,
            slices: vec![Slice::default(); slice_capacity].into_boxed_slice(),
            slice_mask: slice_capacity as u32 - 1,
            slice_next: 0,
            messages: VecDeque::new(),
        }
    }

    #[inline]
    pub(crate) fn session(&self) -> u32 {
        self.session
    }

    #[inline]
    pub(crate) fn base(&self) -> u64 {
        self.base
    }

    /// The sequence the next packet built gets.
    #[inline]
    pub(crate) fn next(&self) -> u64 {
        self.next
    }

    #[inline]
    fn packet(&self, seq: u64) -> &Packet {
        &self.packets[(seq & self.mask) as usize]
    }

    #[inline]
    fn packet_mut(&mut self, seq: u64) -> &mut Packet {
        &mut self.packets[(seq & self.mask) as usize]
    }

    #[inline]
    pub(crate) fn packet_bytes(&self, seq: u64) -> u16 {
        self.packet(seq).bytes
    }

    /// Whether a stream of `len` bytes fits the window as it stands: a
    /// sliver in the open packet, full packets, a sliver at the end.
    pub(crate) fn fits(&self, len: usize) -> bool {
        let room = self.datagram_size - PACKET_HEADER;
        (len / room + 2) as u64 <= self.mask + 1 - (self.next - self.base)
    }

    /// Ends the open packet, so the next record starts a new one. A joining
    /// subscriber starts at [`Self::next`] after this.
    #[inline]
    pub(crate) fn seal(&mut self) {
        self.open = false;
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

    /// Cuts the stream in `slot` into packets; the caller has checked that
    /// it [`Self::fits`].
    pub(crate) fn enqueue(&mut self, store: &mut MessageStore, slot: u32) {
        let stream = store.bytes(slot);
        let len = stream.len();
        debug_assert!(self.fits(len));
        let mut records = Records::new(stream);
        let mut pos = 0usize;
        loop {
            let seq = if self.open { self.next - 1 } else { self.open_packet() };
            let space = self.datagram_size - usize::from(self.packet(seq).bytes);
            let end = (pos + space).min(len);
            let start = records.first(pos, end).map_or(0, |s| (s - pos + 1) as u16);
            let slice = Slice { slot, offset: pos as u32, len: (end - pos) as u16, start };
            self.push_slice(seq, slice);
            pos = end;
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
        self.messages.push_back(Message { slot, last_packet: self.next - 1 });
    }

    /// Builds packet `seq` into the batch, to `to`. `send_ts` stamps it if
    /// this is its first staging for anyone.
    pub(crate) fn put(
        &mut self,
        store: &MessageStore,
        batch: &mut SendBatch,
        to: &SockAddr,
        seq: u64,
        send_ts: Nanos,
    ) {
        let packet = self.packet_mut(seq);
        if packet.send_ts.0 == 0 {
            packet.send_ts = send_ts;
        }
        let packet = *packet;
        batch.open(to);
        let mut header = [0; PACKET_HEADER];
        wire::packet_header(&mut header, self.session, seq, packet.send_ts.0, packet.start);
        batch.copy(&header);
        for i in 0..packet.count {
            let slice = self.slices[((packet.records + u32::from(i)) & self.slice_mask) as usize];
            let bytes = store.bytes(slice.slot);
            batch.payload(&bytes[slice.offset as usize..][..usize::from(slice.len)]);
        }
        batch.close();
        // Once on its way to anyone, a packet takes no more records.
        if seq + 1 == self.next {
            self.open = false;
        }
    }

    /// Drops every packet below `base`, which must not move backwards or
    /// past [`Self::next`].
    pub(crate) fn release_to(&mut self, base: u64, store: &mut MessageStore) {
        debug_assert!(self.base <= base && base <= self.next);
        self.base = base;
        while self.messages.front().is_some_and(|m| m.last_packet < base) {
            store.release(self.messages.pop_front().unwrap().slot);
        }
    }
}
