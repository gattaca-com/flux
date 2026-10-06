//! The receiving half of a peer: which packets arrived, and messages put
//! back together from fragments. Delivery is the caller's business; a
//! completed message comes back by value.

use super::wire::{ACK_HEADER, FRAGMENT_HEADER, PACKET_HEADER};

/// A message being reassembled in owned memory. Recycled whole; stale bytes
/// are overwritten before delivery so nothing is zeroed.
pub(crate) struct Partial {
    message: u32,
    total: u32,
    received: u32,
    /// Kept at its largest past length; the message is `buf[..total]`.
    buf: Vec<u8>,
    /// Lowest packet sequence a fragment arrived in.
    first_packet: u64,
    send_ts: u64,
}

impl Partial {
    #[inline]
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.buf[..self.total as usize]
    }

    #[inline]
    pub(crate) fn send_ts(&self) -> u64 {
        self.send_ts
    }
}

/// Packet sequence tracking above the ack point plus in-progress messages.
///
/// A packet is accepted once; its records are then processed exactly once,
/// so fragments of one message never overlap and a message is complete when
/// its received bytes reach its length. An incomplete message always waits
/// on a packet at or above the ack point, so the window bounds how many can
/// exist.
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
    /// Packets a message's fragments can span: a sliver in the first, full
    /// packets, a sliver in the last.
    fragment_room: u64,
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
            fragment_room: (datagram_size - PACKET_HEADER - FRAGMENT_HEADER) as u64,
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
    /// the window, whose records must not be processed.
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
    /// so a message that could still have had a fragment there is dropped.
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
            let p = &self.partials[i];
            let span = (u64::from(p.total) - 1) / self.fragment_room + 2;
            if p.first_packet + span <= ack_next {
                let dead = self.partials.swap_remove(i);
                self.spare.push(dead);
            } else {
                i += 1;
            }
        }
    }

    /// Takes in a fragment of packet `seq`. Returns the message once it is
    /// complete; hand it back with [`Self::recycle`] after delivery.
    pub(crate) fn fragment(
        &mut self,
        seq: u64,
        message: u32,
        offset: u32,
        total: u32,
        bytes: &[u8],
        send_ts: u64,
    ) -> Option<Partial> {
        let pos = self
            .partials
            .iter()
            .position(|p| p.message == message)
            .unwrap_or_else(|| self.start(message, total, seq, send_ts));
        let partial = &mut self.partials[pos];
        if partial.total != total {
            return None;
        }
        let start = offset as usize;
        partial.buf[start..start + bytes.len()].copy_from_slice(bytes);
        partial.received += bytes.len() as u32;
        partial.first_packet = partial.first_packet.min(seq);
        if offset == 0 {
            partial.send_ts = send_ts;
        }
        (partial.received == total).then(|| self.partials.swap_remove(pos))
    }

    fn start(&mut self, message: u32, total: u32, seq: u64, send_ts: u64) -> usize {
        let mut partial = self.spare.pop().unwrap_or(Partial {
            message: 0,
            total: 0,
            received: 0,
            buf: Vec::new(),
            first_packet: 0,
            send_ts: 0,
        });
        if partial.buf.len() < total as usize {
            partial.buf.resize(total as usize, 0);
        }
        partial.message = message;
        partial.total = total;
        partial.received = 0;
        partial.first_packet = seq;
        partial.send_ts = send_ts;
        self.partials.push(partial);
        self.partials.len() - 1
    }

    pub(crate) fn recycle(&mut self, partial: Partial) {
        self.spare.push(partial);
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

    /// Largest ack bitmap in bytes.
    pub(crate) fn bitmap_capacity(&self) -> usize {
        self.max_bits.div_ceil(64) as usize * 8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn fragments_complete_in_any_order() {
        let mut rx = Inbound::new(64, 1200, false);
        rx.reset(0);
        assert!(rx.fragment(5, 1, 3, 6, b"def", 50).is_none());
        assert!(rx.fragment(7, 2, 0, 2, b"z", 70).is_none());
        let done = rx.fragment(4, 1, 0, 6, b"abc", 40).unwrap();
        assert_eq!(done.bytes(), b"abcdef");
        assert_eq!(done.send_ts(), 40);
        rx.recycle(done);
        let done = rx.fragment(8, 2, 1, 2, b"y", 80).unwrap();
        assert_eq!(done.bytes(), b"zy");
        assert_eq!(rx.partials.len(), 0);
    }

    #[test]
    fn sliding_drops_messages_that_can_no_longer_complete() {
        let mut rx = Inbound::new(64, 1200, true);
        rx.reset(0);
        assert!(rx.fragment(3, 9, 0, 2000, &[0; 1000], 0).is_none());
        assert!(rx.accept(3));
        assert_eq!(rx.partials.len(), 1);
        // Message 9 spans at most 3 packets from 3; the window slides past.
        assert!(rx.accept(3 + 64 + 10));
        assert_eq!(rx.partials.len(), 0);
    }
}
