use super::wire::{self, MESSAGE_HEADER, Packet, Record, Records};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refused {
    Malformed,
    TooLarge,
}

struct Partial {
    /// May itself be cut; `total` is known once all of it is in.
    header: [u8; MESSAGE_HEADER],
    header_len: usize,
    total: usize,
    received: usize,
    /// Reused; the message is `buf[..total]`.
    buf: Vec<u8>,
    expect: u64,
    send_ts: u64,
}

impl Partial {
    /// `Some(true)` once complete, `None` when the header names more than
    /// `max_len`.
    fn append(&mut self, mut bytes: &[u8], max_len: usize) -> Option<bool> {
        if self.header_len < MESSAGE_HEADER {
            let n = (MESSAGE_HEADER - self.header_len).min(bytes.len());
            self.header[self.header_len..][..n].copy_from_slice(&bytes[..n]);
            self.header_len += n;
            bytes = &bytes[n..];
            if self.header_len < MESSAGE_HEADER {
                return Some(false);
            }
            self.total = u32::from_le_bytes(self.header) as usize;
            if self.total > max_len {
                return None;
            }
            if self.buf.len() < self.total {
                self.buf.resize(self.total, 0);
            }
        }
        let n = (self.total - self.received).min(bytes.len());
        self.buf[self.received..][..n].copy_from_slice(&bytes[..n]);
        self.received += n;
        Some(self.received == self.total)
    }
}

fn joined_len(head: &[u8], rest: &[u8]) -> Option<usize> {
    let mut header = [0; MESSAGE_HEADER];
    header[..head.len()].copy_from_slice(head);
    header[head.len()..].copy_from_slice(rest.get(..MESSAGE_HEADER - head.len())?);
    Some(u32::from_le_bytes(header) as usize)
}

/// The leading bytes of a packet that arrived before the packet starting
/// the message they continue.
struct Stash {
    seq: Option<u64>,
    buf: Vec<u8>,
}

/// A partial always waits on a packet at or above the ack point, and a stash
/// belongs to one, so the window bounds both.
pub(crate) struct Inbound {
    ack_next: u64,
    highest: u64,
    bits: Box<[u64]>,
    mask: u64,
    capacity: u64,
    max_bits: u64,
    /// A header cut over more than two packets is only checked here.
    max_len: usize,
    /// Unreliable: no retransmit will fill a hole.
    slide: bool,
    stash: Box<[Stash]>,
    partials: Vec<Partial>,
    spare: Vec<Partial>,
}

impl Inbound {
    /// `ack_header` bytes precede the bitmap in an ack of `datagram_size`.
    pub(crate) fn new(
        capacity: usize,
        datagram_size: usize,
        ack_header: usize,
        max_len: usize,
        slide: bool,
    ) -> Self {
        let max_bits = ((datagram_size - ack_header) * 8) as u64;
        Self {
            ack_next: 0,
            highest: 0,
            bits: vec![0; capacity.div_ceil(u64::BITS as usize)].into_boxed_slice(),
            mask: capacity as u64 - 1,
            capacity: capacity as u64,
            max_bits: max_bits.min(capacity as u64 - 1),
            max_len,
            slide,
            stash: (0..capacity).map(|_| Stash { seq: None, buf: Vec::new() }).collect(),
            partials: Vec::new(),
            spare: Vec::new(),
        }
    }

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

    /// `false`: its bytes must not be taken in.
    fn accept(&mut self, seq: u64) -> bool {
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

    /// Validates before accepting: a bad packet must not consume the
    /// sequence a good retry will carry. `Ok` for a duplicate too, whose
    /// sender still waits for an ack.
    pub(crate) fn receive(
        &mut self,
        packet: Packet<'_>,
        deliver: &mut dyn FnMut(&[u8], u64),
    ) -> Result<(), Refused> {
        let seq = packet.seq;
        // Out of reach of a real stream; keeps `seq + 1` and the window
        // arithmetic from overflowing on a peer's say-so.
        if seq > u64::MAX / 2 {
            return Err(Refused::Malformed);
        }
        let (continuation, records) = packet.split().ok_or(Refused::Malformed)?;
        self.validate(seq, continuation, records.clone())?;
        // TODO: emit duplicates (below `ack_next` or already in) to the
        // group's telemetry. Against the sender's retransmits they tell
        // spurious resends (RTO or reorder window too tight) from real loss.
        if self.accept(seq) {
            self.packet(seq, continuation, records, packet.send_ts, deliver);
        }
        Ok(())
    }

    /// Includes a length whose header is cut between this packet and one
    /// already in.
    fn validate(&self, seq: u64, continuation: &[u8], records: Records<'_>) -> Result<(), Refused> {
        let check = |len: usize| if len > self.max_len { Err(Refused::TooLarge) } else { Ok(()) };
        if let Some(len) = self.waiting_len(seq, continuation) {
            check(len)?;
        }
        for record in records {
            let len = match record {
                Record::Whole(bytes) => Some(bytes.len()),
                Record::Head(bytes) => wire::record_len(bytes)
                    .checked_sub(MESSAGE_HEADER)
                    .or_else(|| self.stashed_len(seq, bytes)),
            };
            if let Some(len) = len {
                check(len)?;
            }
        }
        Ok(())
    }

    fn packet(
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
                Record::Head(bytes) => {
                    if let Some(i) = self.start(bytes, seq + 1, send_ts) {
                        self.drain(i, deliver);
                    }
                }
            }
        }
    }

    /// `true` once the partial is gone, delivered or dropped.
    fn feed(&mut self, i: usize, bytes: &[u8], deliver: &mut dyn FnMut(&[u8], u64)) -> bool {
        let p = &mut self.partials[i];
        let complete = if bytes.is_empty() { None } else { p.append(bytes, self.max_len) };
        if complete == Some(false) {
            p.expect += 1;
            return false;
        }
        let done = self.partials.swap_remove(i);
        if complete.is_some() {
            deliver(&done.buf[..done.total], done.send_ts);
        }
        self.spare.push(done);
        true
    }

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

    fn start(&mut self, head: &[u8], expect: u64, send_ts: u64) -> Option<usize> {
        let mut partial = self.spare.pop().unwrap_or(Partial {
            header: [0; MESSAGE_HEADER],
            header_len: 0,
            total: 0,
            received: 0,
            buf: Vec::new(),
            expect: 0,
            send_ts: 0,
        });
        partial.header_len = 0;
        partial.total = 0;
        partial.received = 0;
        partial.expect = expect;
        partial.send_ts = send_ts;
        if partial.append(head, self.max_len) != Some(false) {
            self.spare.push(partial);
            return None;
        }
        self.partials.push(partial);
        Some(self.partials.len() - 1)
    }

    /// The length of a cut header that `continuation` of packet `seq`
    /// completes.
    fn waiting_len(&self, seq: u64, continuation: &[u8]) -> Option<usize> {
        let p = self.partials.iter().find(|p| p.expect == seq && p.header_len < MESSAGE_HEADER)?;
        joined_len(&p.header[..p.header_len], continuation)
    }

    /// The length of a header cut into `head` that the stashed packet
    /// `seq + 1` completes.
    fn stashed_len(&self, seq: u64, head: &[u8]) -> Option<usize> {
        let stash = &self.stash[((seq + 1) & self.mask) as usize];
        if stash.seq != Some(seq + 1) {
            return None;
        }
        joined_len(head, &stash.buf)
    }

    #[inline]
    pub(crate) fn ack_bits(&self) -> u64 {
        self.highest.saturating_sub(self.ack_next).min(self.max_bits)
    }

    /// Bit `i` set: `ack_next + 1 + i` has arrived.
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
            let n = (n_bits.div_ceil(8) as usize - w * 8).min(8);
            out[w * 8..][..n].copy_from_slice(&v.to_le_bytes()[..n]);
        }
        n_bits.div_ceil(8) as usize
    }

    pub(crate) fn bitmap_capacity(&self) -> usize {
        self.max_bits.div_ceil(8) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::udp_v2::wire::{decode_packet, test_packet};

    fn inbound() -> Inbound {
        let mut inbound = Inbound::new(64, 1200, 19, 16 << 20, false);
        inbound.reset(10);
        inbound
    }

    /// The last message is cut to `head` bytes if given.
    fn packet(seq: u64, continuation: &[u8], messages: &[&[u8]], head: Option<usize>) -> Vec<u8> {
        let start = if messages.is_empty() { 0 } else { continuation.len() as u16 + 1 };
        let mut payload = continuation.to_vec();
        for (i, m) in messages.iter().enumerate() {
            payload.extend_from_slice(&(m.len() as u32).to_le_bytes());
            let cut = if i + 1 == messages.len() { head.unwrap_or(m.len()) } else { m.len() };
            payload.extend_from_slice(&m[..cut]);
        }
        test_packet(7, seq, seq * 100, start, &payload)
    }

    fn feed(inbound: &mut Inbound, bytes: &[u8], out: &mut Vec<(Vec<u8>, u64)>) {
        let _ = inbound.receive(decode_packet(bytes).unwrap(), &mut |bytes, ts| {
            out.push((bytes.to_vec(), ts));
        });
    }

    #[test]
    fn fragments_arrive_in_any_order_and_only_count_once() {
        let mut inbound = inbound();
        let message: Vec<u8> = (0..2500u32).map(|i| i as u8).collect();
        let a = packet(10, b"", &[b"first", &message], Some(1000));
        let b = packet(11, &message[1000..2000], &[], None);
        let c = packet(12, &message[2000..], &[b"last"], None);
        let mut out = Vec::new();
        feed(&mut inbound, &c, &mut out);
        feed(&mut inbound, &b, &mut out);
        feed(&mut inbound, &c, &mut out);
        assert_eq!(out, [(b"last".to_vec(), 1200)]);
        feed(&mut inbound, &a, &mut out);
        assert_eq!(out.len(), 3);
        assert_eq!(out[1], (b"first".to_vec(), 1000));
        assert_eq!(out[2], (message, 1000));
        for again in [&a, &b, &c] {
            feed(&mut inbound, again, &mut out);
        }
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn a_message_cut_in_or_right_after_its_header_completes_in_any_order() {
        let mut inbound = inbound();
        let stream = [&6u32.to_le_bytes()[..], b"abcdef"].concat();
        let mut out = Vec::new();
        let mut seq = 10;
        for cut in 1..=MESSAGE_HEADER {
            for head_first in [true, false] {
                let head = test_packet(7, seq, seq * 100, 1, &stream[..cut]);
                let tail = test_packet(7, seq + 1, (seq + 1) * 100, 0, &stream[cut..]);
                let order = if head_first { [&head, &tail] } else { [&tail, &head] };
                for bytes in order {
                    feed(&mut inbound, bytes, &mut out);
                }
                assert_eq!(out, [(b"abcdef".to_vec(), seq * 100)]);
                out.clear();
                seq += 2;
            }
        }
        // A cut header naming too much.
        let header = u32::MAX.to_le_bytes();
        feed(&mut inbound, &test_packet(7, seq, 0, 1, &header[..2]), &mut out);
        feed(
            &mut inbound,
            &test_packet(7, seq + 1, 0, 0, &[&header[2..], b"x"].concat()),
            &mut out,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn a_sequence_out_of_reach_is_refused() {
        let mut inbound = Inbound::new(64, 1200, 19, 16 << 20, true);
        inbound.reset(10);
        let mut out = Vec::new();
        let bytes = test_packet(7, u64::MAX, 0, 1, &[&1u32.to_le_bytes()[..], b"x"].concat());
        let refused = inbound.receive(decode_packet(&bytes).unwrap(), &mut |b, ts| {
            out.push((b.to_vec(), ts));
        });
        assert!(matches!(refused, Err(Refused::Malformed)));
        assert!(out.is_empty());
        assert_eq!(inbound.ack_next(), 10);
    }
}
