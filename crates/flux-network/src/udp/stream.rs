use std::collections::VecDeque;

use flux_timing::Nanos;

use super::{
    UdpConfig,
    sys::{SendBatch, SockAddr},
    wire::{self, PACKET_HEADER},
};

/// Bounds the slice ring at this multiple of the packet window.
const MAX_SLICES_PER_PACKET: u16 = 4;

struct Message {
    slot: u32,
    last_packet: u64,
}

#[derive(Clone, Copy, Default)]
struct Slice {
    slot: u32,
    offset: u32,
    len: u16,
    /// 1 + offset of the first record header in the span; 0 without one.
    start: u16,
}

struct Records<'a> {
    stream: &'a [u8],
    start: usize,
}

impl<'a> Records<'a> {
    fn new(stream: &'a [u8]) -> Self {
        Self { stream, start: 0 }
    }

    /// Calls must not move backwards.
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
    first_slice: u32,
    count: u16,
    bytes: u16,
    start: u16,
    /// Stamped at the first staging, so every copy carries the same bytes.
    send_ts: Nanos,
}

pub(crate) struct Stream {
    session: u32,
    datagram_size: usize,
    packets: Box<[Packet]>,
    mask: u64,
    base: u64,
    next: u64,
    /// Packet `next - 1` still takes records.
    open: bool,
    slices: Box<[Slice]>,
    slice_mask: u32,
    slice_next: u32,
    messages: VecDeque<Message>,
    buffers: Vec<Vec<u8>>,
    free: Vec<u32>,
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
            buffers: Vec::new(),
            free: Vec::new(),
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

    /// +2: a sliver in the open packet and one at the end.
    pub(crate) fn fits(&self, len: usize) -> bool {
        let room = self.datagram_size - PACKET_HEADER;
        (len / room + 2) as u64 <= self.mask + 1 - (self.next - self.base)
    }

    #[inline]
    pub(crate) fn seal(&mut self) {
        self.open = false;
    }

    fn open_packet(&mut self) -> u64 {
        let seq = self.next;
        self.next += 1;
        *self.packet_mut(seq) = Packet {
            first_slice: self.slice_next,
            bytes: PACKET_HEADER as u16,
            ..Packet::default()
        };
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

    /// Takes `payload`'s buffer, leaving a recycled one in its place.
    pub(crate) fn enqueue(&mut self, payload: &mut Vec<u8>) {
        let slot = self.free.pop().unwrap_or_else(|| {
            self.buffers.push(Vec::new());
            (self.buffers.len() - 1) as u32
        });
        let len = payload.len();
        debug_assert!(self.fits(len));
        let mut records = Records::new(payload);
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
        std::mem::swap(&mut self.buffers[slot as usize], payload);
        payload.clear();
        self.messages.push_back(Message { slot, last_packet: self.next - 1 });
    }

    pub(crate) fn put(&mut self, batch: &mut SendBatch, to: &SockAddr, seq: u64, send_ts: Nanos) {
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
            let slice =
                self.slices[((packet.first_slice + u32::from(i)) & self.slice_mask) as usize];
            let bytes = &self.buffers[slice.slot as usize];
            batch.payload(&bytes[slice.offset as usize..][..usize::from(slice.len)]);
        }
        batch.close();
        // Its bytes must stay identical for every subscriber.
        if seq + 1 == self.next {
            self.open = false;
        }
    }

    pub(crate) fn release_to(&mut self, base: u64) {
        debug_assert!(self.base <= base && base <= self.next);
        self.base = base;
        while self.messages.front().is_some_and(|m| m.last_packet < base) {
            self.free.push(self.messages.pop_front().unwrap().slot);
        }
    }
}
