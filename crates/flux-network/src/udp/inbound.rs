//! The receiving half of a peer: packet tracking and reassembly of messages
//! cut across packets, which complete whatever order the packets arrive in.

use super::wire::{ACK_HEADER, Record, Records};

/// A message cut across packets, waiting on packet `expect` to continue it.
struct Partial {
    total: usize,
    received: usize,
    /// Grown to its largest `total` and reused; the message is `buf[..total]`.
    buf: Vec<u8>,
    expect: u64,
    send_ts: u64,
}

/// The leading bytes of a packet that arrived before the message they
/// continue was known, kept until its head packet comes.
struct Stash {
    seq: Option<u64>,
    buf: Vec<u8>,
}

/// Packet sequence tracking above the ack point plus in-progress messages.
///
/// A packet is accepted once, so its bytes join a message exactly once. A
/// partial always waits on a packet at or above the ack point, and a stash
/// belongs to one, so the window bounds both.
pub(crate) struct Inbound {
    ack_next: u64,
    /// Highest sequence accepted; the ack bitmap runs from `ack_next + 1` to
    /// here.
    highest: u64,
    bits: Box<[u64]>,
    mask: u64,
    capacity: u64,
    /// Most bits one ack datagram can carry.
    max_bits: u64,
    /// Unreliable mode: the window follows the highest sequence instead of
    /// waiting at a hole that no retransmit will ever fill.
    slide: bool,
    /// One slot per sequence of the window.
    stash: Box<[Stash]>,
    partials: Vec<Partial>,
    spare: Vec<Partial>,
}

impl Inbound {
    pub(crate) fn new(capacity: usize, datagram_size: usize, slide: bool) -> Self {
        let max_bits = ((datagram_size - ACK_HEADER) / 8 * 64) as u64;
        Self {
            ack_next: 0,
            highest: 0,
            bits: vec![0; capacity.div_ceil(u64::BITS as usize)].into_boxed_slice(),
            mask: capacity as u64 - 1,
            capacity: capacity as u64,
            max_bits: max_bits.min(capacity as u64 - 1),
            slide,
            stash: (0..capacity).map(|_| Stash { seq: None, buf: Vec::new() }).collect(),
            partials: Vec::new(),
            spare: Vec::new(),
        }
    }

    /// Starts over at `first`: a new session on the other side.
    pub(crate) fn reset(&mut self, first: u64) {
        self.ack_next = first;
        self.highest = first;
        self.bits.fill(0);
        self.spare.append(&mut self.partials);
        for s in &mut self.stash {
            s.seq = None;
        }
    }

    #[inline]
    pub(crate) fn ack_next(&self) -> u64 {
        self.ack_next
    }

    #[inline]
    fn bit(&self, seq: u64) -> bool {
        let i = seq & self.mask;
        self.bits[(i >> 6) as usize] & (1 << (i & 63)) != 0
    }

    #[inline]
    fn set_bit(&mut self, seq: u64, v: bool) {
        let i = seq & self.mask;
        let bit = 1 << (i & 63);
        let word = &mut self.bits[(i >> 6) as usize];
        if v {
            *word |= bit;
        } else {
            *word &= !bit;
        }
    }

    /// Records packet `seq`; `false` for duplicates and sequences outside
    /// the window, whose bytes must not be taken in.
    pub(crate) fn accept(&mut self, seq: u64) -> bool {
        if seq < self.ack_next {
            return false;
        }
        if seq >= self.ack_next + self.capacity {
            if !self.slide {
                return false;
            }
            self.slide_to(seq + 1 - self.capacity);
        }
        if self.bit(seq) {
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

    /// Moves the ack point past holes. Everything below it is lost for good,
    /// so a message waiting on a packet there is dropped.
    fn slide_to(&mut self, ack_next: u64) {
        if ack_next - self.ack_next >= self.capacity {
            self.bits.fill(0);
        } else {
            for seq in self.ack_next..ack_next {
                self.set_bit(seq, false);
            }
        }
        self.ack_next = ack_next;
        let mut i = 0;
        while i < self.partials.len() {
            if self.partials[i].expect < ack_next {
                let dead = self.partials.swap_remove(i);
                self.spare.push(dead);
            } else {
                i += 1;
            }
        }
    }

    /// Takes in accepted packet `seq`: `continuation` finishes or extends
    /// the message cut before it, `records` are the messages starting in it.
    /// Every completed message goes to `deliver` with its first packet's
    /// `send_ts`.
    pub(crate) fn packet(
        &mut self,
        seq: u64,
        continuation: &[u8],
        records: Records<'_>,
        send_ts: u64,
        deliver: &mut dyn FnMut(&[u8], u64),
    ) {
        match self.partials.iter().position(|p| p.expect == seq) {
            Some(i) => {
                if !self.feed(i, continuation, deliver) {
                    self.drain(i, deliver);
                }
            }
            None if !continuation.is_empty() => {
                let stash = &mut self.stash[(seq & self.mask) as usize];
                stash.seq = Some(seq);
                stash.buf.clear();
                stash.buf.extend_from_slice(continuation);
            }
            None => {}
        }
        for record in records {
            match record {
                Record::Whole(bytes) => deliver(bytes, send_ts),
                Record::Head { total, bytes } => {
                    let i = self.start(total, bytes, seq + 1, send_ts);
                    self.drain(i, deliver);
                }
            }
        }
    }

    /// Appends the packet partial `i` waits for. `true` once the partial is
    /// gone: delivered, or dropped because the packet started a message
    /// where it should have continued this one.
    fn feed(&mut self, i: usize, bytes: &[u8], deliver: &mut dyn FnMut(&[u8], u64)) -> bool {
        let p = &mut self.partials[i];
        if bytes.is_empty() {
            let dead = self.partials.swap_remove(i);
            self.spare.push(dead);
            return true;
        }
        let n = (p.total - p.received).min(bytes.len());
        p.buf[p.received..p.received + n].copy_from_slice(&bytes[..n]);
        p.received += n;
        if p.received < p.total {
            p.expect += 1;
            return false;
        }
        let done = self.partials.swap_remove(i);
        deliver(&done.buf[..done.total], done.send_ts);
        self.spare.push(done);
        true
    }

    /// Feeds partial `i` the stashed packets that arrived before the one it
    /// was waiting for.
    fn drain(&mut self, i: usize, deliver: &mut dyn FnMut(&[u8], u64)) {
        loop {
            let seq = self.partials[i].expect;
            let slot = (seq & self.mask) as usize;
            if self.stash[slot].seq != Some(seq) {
                return;
            }
            self.stash[slot].seq = None;
            let buf = std::mem::take(&mut self.stash[slot].buf);
            let gone = self.feed(i, &buf, deliver);
            self.stash[slot].buf = buf;
            if gone {
                return;
            }
        }
    }

    fn start(&mut self, total: usize, bytes: &[u8], expect: u64, send_ts: u64) -> usize {
        let mut partial = self.spare.pop().unwrap_or(Partial {
            total: 0,
            received: 0,
            buf: Vec::new(),
            expect: 0,
            send_ts: 0,
        });
        if partial.buf.len() < total {
            partial.buf.resize(total, 0);
        }
        partial.buf[..bytes.len()].copy_from_slice(bytes);
        partial.total = total;
        partial.received = bytes.len();
        partial.expect = expect;
        partial.send_ts = send_ts;
        self.partials.push(partial);
        self.partials.len() - 1
    }

    /// Bits an ack should carry: one per sequence in `ack_next + 1 ..=
    /// highest`, capped.
    #[inline]
    pub(crate) fn ack_bits(&self) -> u64 {
        self.highest.saturating_sub(self.ack_next).min(self.max_bits)
    }

    /// Writes `n_bits` of the ring starting at `ack_next + 1` into `out` as
    /// little-endian words. Bit `i` set: `ack_next + 1 + i` has arrived.
    pub(crate) fn write_bitmap(&self, n_bits: u64, out: &mut [u8]) -> usize {
        let words = n_bits.div_ceil(64) as usize;
        let ring_words = self.bits.len();
        for w in 0..words {
            let idx = (self.ack_next + 1 + 64 * w as u64) & self.mask;
            let (word, shift) = ((idx >> 6) as usize, idx & 63);
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

    pub(crate) fn bitmap_capacity(&self) -> usize {
        self.max_bits.div_ceil(64) as usize * 8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::udp::wire::{self, LONG_HEADER, PACKET_HEADER};

    /// Feeds a packet of `continuation` then `messages`, the last cut to
    /// `head` bytes if given, collecting deliveries as `(bytes, send_ts)`.
    fn feed(
        rx: &mut Inbound,
        seq: u64,
        continuation: &[u8],
        messages: &[&[u8]],
        head: Option<usize>,
        out: &mut Vec<(Vec<u8>, u64)>,
    ) {
        let mut bytes = [0; PACKET_HEADER].to_vec();
        let start = if messages.is_empty() { 0 } else { continuation.len() as u16 + 1 };
        wire::packet_header(bytes.as_mut_slice().try_into().unwrap(), 1, seq, seq * 10, start);
        bytes.extend_from_slice(continuation);
        for (i, m) in messages.iter().enumerate() {
            let mut h = [0; LONG_HEADER];
            let n = wire::message_header(&mut h, m.len());
            bytes.extend_from_slice(&h[..n]);
            let cut = if i + 1 == messages.len() { head.unwrap_or(m.len()) } else { m.len() };
            bytes.extend_from_slice(&m[..cut]);
        }
        let Some(wire::Datagram::Packet(p)) = wire::decode(&bytes) else { panic!() };
        let (continuation, records) = p.split().unwrap();
        assert!(rx.accept(seq));
        rx.packet(seq, continuation, records, p.send_ts, &mut |b, ts| out.push((b.to_vec(), ts)));
    }

    #[test]
    fn accepts_once_and_reports_bitmap() {
        let mut rx = Inbound::new(64, 1200, false);
        rx.reset(10);
        assert!(rx.accept(10));
        assert!(!rx.accept(10));
        assert!(rx.accept(12));
        assert!(rx.accept(13));
        assert!(!rx.accept(9));
        assert!(!rx.accept(11 + 64));
        assert_eq!(rx.ack_next(), 11);
        assert_eq!(rx.ack_bits(), 2);
        let mut out = [0; 8];
        assert_eq!(rx.write_bitmap(2, &mut out), 8);
        assert_eq!(u64::from_le_bytes(out), 0b11);
        assert!(rx.accept(11));
        assert_eq!(rx.ack_next(), 14);
    }

    #[test]
    fn a_cut_message_completes_in_any_packet_order() {
        let mut rx = Inbound::new(64, 1200, false);
        rx.reset(0);
        let mut out = Vec::new();
        // "abc" is cut after "a", runs through packet 4 and ends in 5.
        feed(&mut rx, 5, b"c", &[b"next"], None, &mut out);
        feed(&mut rx, 4, b"b", &[], None, &mut out);
        assert_eq!(out, [(b"next".to_vec(), 50)]);
        feed(&mut rx, 3, b"", &[b"first", b"abc"], Some(1), &mut out);
        assert_eq!(out[1..], [(b"first".to_vec(), 30), (b"abc".to_vec(), 30)]);
        // In order, with the tail and a new head in the same packet.
        feed(&mut rx, 6, b"", &[b"xyz"], Some(2), &mut out);
        feed(&mut rx, 7, b"z", &[b"pq"], Some(1), &mut out);
        feed(&mut rx, 8, b"q", &[], None, &mut out);
        assert_eq!(out[3..], [(b"xyz".to_vec(), 60), (b"pq".to_vec(), 70)]);
    }

    #[test]
    fn a_packet_that_does_not_continue_drops_the_cut_message() {
        let mut rx = Inbound::new(64, 1200, false);
        rx.reset(0);
        let mut out = Vec::new();
        feed(&mut rx, 1, b"", &[b"abc"], Some(1), &mut out);
        feed(&mut rx, 2, b"", &[b"other"], None, &mut out);
        assert_eq!(out, [(b"other".to_vec(), 20)]);
    }
}
