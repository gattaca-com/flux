//! Datagram encodings. Each datagram decodes on its own, in any order.
//!
//! Every datagram starts with the same prefix:
//!
//! ```text
//! [0..2]  magic "FX"; anything else is not ours and is dropped unread
//! [2]     version << 4 | kind
//! [3..7]  session of the sender; 0 in a reset, which has none
//! ```
//!
//! A packet (kind 1) carries a slice of the sender's message stream:
//!
//! ```text
//! [7..15]  seq       per packet; the unit of acks and retransmission
//! [15..23] send_ts   sender wall clock when the packet first went out
//! [23..25] start     1 + offset of the first message header in the payload,
//!                    0 when no message starts in this packet
//! ```
//!
//! The payload is the rest of the message cut at the end of the previous
//! packet, then messages, each its length followed by its bytes; the last
//! may be cut at the packet end and continue in the next. A length is 2
//! bytes below 32768, else 4 with the top bit set:
//!
//! ```text
//! short:  [len u16, top bit clear]
//! long:   [len u32 big-endian halves: (len >> 16) | 0x8000 u16][len & 0xffff u16]
//! ```
//!
//! A header is never cut and is always followed by at least one byte of its
//! message in the same packet, so trailing bytes too few for that are
//! padding, written as 0xff.
//!
//! An ack (kind 2) carries the cumulative point `ack_next` and a bitmap of
//! `bits` sequences above it: bit `i` set means `ack_next + 1 + i` arrived.
//! A hello (kind 3) announces the sender's first unacked packet sequence; a
//! hello ack (kind 4) echoes the session it answers and announces its own
//! base; a reset (kind 5) names the session a listener holds no peer for.

pub(crate) const MAGIC: [u8; 2] = *b"FX";
pub(crate) const VERSION: u8 = 2;
/// Largest UDP payload over IPv4.
pub(crate) const MAX_DATAGRAM_SIZE: usize = 65_507;

const PREFIX: usize = 7;
pub(crate) const PACKET_HEADER: usize = PREFIX + 8 + 8 + 2;
pub(crate) const SHORT_HEADER: usize = 2;
pub(crate) const LONG_HEADER: usize = 4;
/// Longest message with a short header.
const MAX_SHORT: usize = (1 << 15) - 1;
pub(crate) const PADDING: [u8; LONG_HEADER] = [0xff; LONG_HEADER];
pub(crate) const ACK_HEADER: usize = PREFIX + 8 + 2;
pub(crate) const HELLO_SIZE: usize = PREFIX + 8;
pub(crate) const HELLO_ACK_SIZE: usize = PREFIX + 4 + 8;
pub(crate) const RESET_SIZE: usize = PREFIX + 4;
const LONG: u16 = 1 << 15;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Kind {
    Packet = 1,
    Ack = 2,
    Hello = 3,
    HelloAck = 4,
    Reset = 5,
}

/// One received datagram, borrowed from the receive buffer.
#[derive(Debug)]
pub(crate) enum Datagram<'a> {
    Packet(Packet<'a>),
    Ack { session: u32, ack_next: u64, bits: u16, bitmap: &'a [u8] },
    Hello { session: u32, base: u64 },
    HelloAck { session: u32, their_session: u32, base: u64 },
    Reset { their_session: u32 },
}

/// A received packet.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Packet<'a> {
    pub(crate) session: u32,
    pub(crate) seq: u64,
    pub(crate) send_ts: u64,
    start: u16,
    payload: &'a [u8],
}

impl<'a> Packet<'a> {
    /// Bytes continuing the message cut at the end of the previous packet,
    /// and the messages starting here. `None` when `start` points past the
    /// payload.
    pub(crate) fn split(&self) -> Option<(&'a [u8], Records<'a>)> {
        let at = match self.start {
            0 => self.payload.len(),
            s => usize::from(s) - 1,
        };
        let (continuation, rest) = self.payload.split_at_checked(at)?;
        Some((continuation, Records { bytes: rest, ok: true }))
    }
}

/// The messages starting in a packet, decoded one at a time. Stops at the
/// padding, or at a header nothing follows, which [`Self::ok`] reports.
#[derive(Clone, Debug)]
pub(crate) struct Records<'a> {
    bytes: &'a [u8],
    ok: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Record<'a> {
    Whole(&'a [u8]),
    /// The first `bytes` of a message of `total` bytes; the rest follows in
    /// the next packets.
    Head {
        total: usize,
        bytes: &'a [u8],
    },
}

impl Records<'_> {
    /// Whether everything decoded so far was well formed.
    #[inline]
    pub(crate) fn ok(&self) -> bool {
        self.ok
    }
}

impl<'a> Iterator for Records<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Record<'a>> {
        let v = u16::from_le_bytes(self.bytes.get(..2)?.try_into().unwrap());
        let (header, len) = if v & LONG == 0 {
            (SHORT_HEADER, usize::from(v))
        } else {
            if self.bytes.len() < LONG_HEADER + 1 {
                return None;
            }
            let low = u16::from_le_bytes(self.bytes[2..4].try_into().unwrap());
            (LONG_HEADER, usize::from(v & !LONG) << 16 | usize::from(low))
        };
        let rest = &self.bytes[header..];
        if rest.len() >= len {
            self.bytes = &rest[len..];
            return Some(Record::Whole(&rest[..len]));
        }
        self.bytes = &[];
        if rest.is_empty() {
            self.ok = false;
            return None;
        }
        Some(Record::Head { total: len, bytes: rest })
    }
}

#[inline]
fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

#[inline]
fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap())
}

/// `None` for anything that is not a datagram of this version.
pub(crate) fn decode(bytes: &[u8]) -> Option<Datagram<'_>> {
    if bytes.len() < PREFIX || bytes[0..2] != MAGIC || bytes[2] >> 4 != VERSION {
        return None;
    }
    let session = u32_at(bytes, 3);
    let body = &bytes[PREFIX..];
    match bytes[2] & 0x0f {
        1 if bytes.len() >= PACKET_HEADER => Some(Datagram::Packet(Packet {
            session,
            seq: u64_at(body, 0),
            send_ts: u64_at(body, 8),
            start: u16::from_le_bytes(body[16..18].try_into().unwrap()),
            payload: &body[18..],
        })),
        2 if bytes.len() >= ACK_HEADER => {
            let bits = u16::from_le_bytes(body[8..10].try_into().unwrap());
            let bitmap = &body[10..];
            (bitmap.len() >= usize::from(bits).div_ceil(64) * 8).then_some(Datagram::Ack {
                session,
                ack_next: u64_at(body, 0),
                bits,
                bitmap,
            })
        }
        3 if bytes.len() >= HELLO_SIZE => Some(Datagram::Hello { session, base: u64_at(body, 0) }),
        4 if bytes.len() >= HELLO_ACK_SIZE => Some(Datagram::HelloAck {
            session,
            their_session: u32_at(body, 0),
            base: u64_at(body, 4),
        }),
        5 if bytes.len() >= RESET_SIZE => Some(Datagram::Reset { their_session: u32_at(body, 0) }),
        _ => None,
    }
}

fn prefix(buf: &mut [u8], kind: Kind, session: u32) {
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = (VERSION << 4) | kind as u8;
    buf[3..7].copy_from_slice(&session.to_le_bytes());
}

pub(crate) fn packet_header(
    buf: &mut [u8; PACKET_HEADER],
    session: u32,
    seq: u64,
    send_ts: u64,
    start: u16,
) {
    prefix(buf, Kind::Packet, session);
    buf[7..15].copy_from_slice(&seq.to_le_bytes());
    buf[15..23].copy_from_slice(&send_ts.to_le_bytes());
    buf[23..25].copy_from_slice(&start.to_le_bytes());
}

/// Header and body length of the record at the start of `bytes`; `(0, 0)`
/// past the end of a stream.
#[inline]
pub(crate) fn record_len(bytes: &[u8]) -> (usize, usize) {
    if bytes.len() < SHORT_HEADER {
        return (0, 0);
    }
    let v = u16::from_le_bytes(bytes[..2].try_into().unwrap());
    if v & LONG == 0 {
        (SHORT_HEADER, usize::from(v))
    } else {
        let low = u16::from_le_bytes(bytes[2..4].try_into().unwrap());
        (LONG_HEADER, usize::from(v & !LONG) << 16 | usize::from(low))
    }
}

/// Writes the header of a message of `len` bytes; returns its size.
pub(crate) fn message_header(buf: &mut [u8; LONG_HEADER], len: usize) -> usize {
    if len <= MAX_SHORT {
        buf[0..2].copy_from_slice(&(len as u16).to_le_bytes());
        SHORT_HEADER
    } else {
        debug_assert!(len >> 16 <= usize::from(!LONG));
        buf[0..2].copy_from_slice(&((len >> 16) as u16 | LONG).to_le_bytes());
        buf[2..4].copy_from_slice(&(len as u16).to_le_bytes());
        LONG_HEADER
    }
}

/// Writes an ack header; the caller appends `bits.div_ceil(64) * 8` bitmap
/// bytes.
pub(crate) fn ack_header(buf: &mut [u8; ACK_HEADER], session: u32, ack_next: u64, bits: u16) {
    prefix(buf, Kind::Ack, session);
    buf[7..15].copy_from_slice(&ack_next.to_le_bytes());
    buf[15..17].copy_from_slice(&bits.to_le_bytes());
}

pub(crate) fn hello(buf: &mut [u8; HELLO_SIZE], session: u32, base: u64) {
    prefix(buf, Kind::Hello, session);
    buf[7..15].copy_from_slice(&base.to_le_bytes());
}

pub(crate) fn hello_ack(
    buf: &mut [u8; HELLO_ACK_SIZE],
    session: u32,
    their_session: u32,
    base: u64,
) {
    prefix(buf, Kind::HelloAck, session);
    buf[7..11].copy_from_slice(&their_session.to_le_bytes());
    buf[11..19].copy_from_slice(&base.to_le_bytes());
}

pub(crate) fn reset(buf: &mut [u8; RESET_SIZE], their_session: u32) {
    prefix(buf, Kind::Reset, 0);
    buf[7..11].copy_from_slice(&their_session.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(seq: u64, start: u16, payload: &[u8]) -> Vec<u8> {
        let mut header = [0; PACKET_HEADER];
        packet_header(&mut header, 7, seq, 99, start);
        let mut bytes = header.to_vec();
        bytes.extend_from_slice(payload);
        bytes
    }

    fn message(bytes: &[u8]) -> Vec<u8> {
        let mut h = [0; LONG_HEADER];
        let n = message_header(&mut h, bytes.len());
        let mut out = h[..n].to_vec();
        out.extend_from_slice(bytes);
        out
    }

    #[test]
    fn packet_round_trip() {
        let mut payload = b"tail".to_vec();
        payload.extend(message(b"abc"));
        payload.extend(message(b""));
        let long = vec![7u8; 40_000];
        let mut h = [0; LONG_HEADER];
        assert_eq!(message_header(&mut h, long.len()), LONG_HEADER);
        payload.extend_from_slice(&h);
        payload.extend_from_slice(&long[..2]);
        let bytes = packet(42, 5, &payload);
        let Some(Datagram::Packet(p)) = decode(&bytes) else { panic!() };
        assert_eq!((p.session, p.seq, p.send_ts), (7, 42, 99));
        let (continuation, mut records) = p.split().unwrap();
        assert_eq!(continuation, b"tail");
        assert_eq!(records.next(), Some(Record::Whole(b"abc")));
        assert_eq!(records.next(), Some(Record::Whole(b"")));
        assert_eq!(records.next(), Some(Record::Head { total: 40_000, bytes: &long[..2] }));
        assert_eq!(records.next(), None);
        assert!(records.ok());
        let mut payload = message(b"ok");
        payload.extend_from_slice(&PADDING[..3]);
        let bytes = packet(43, 1, &payload);
        let Some(Datagram::Packet(p)) = decode(&bytes) else { panic!() };
        let (_, mut records) = p.split().unwrap();
        assert_eq!(records.next(), Some(Record::Whole(b"ok")));
        assert_eq!(records.next(), None);
        assert!(records.ok());
    }

    #[test]
    fn a_header_nothing_follows_is_malformed() {
        let mut payload = message(b"ok");
        payload.extend_from_slice(&9u16.to_le_bytes());
        let bytes = packet(1, 1, &payload);
        let Some(Datagram::Packet(p)) = decode(&bytes) else { panic!() };
        let (_, mut records) = p.split().unwrap();
        assert_eq!(records.next(), Some(Record::Whole(b"ok")));
        assert_eq!(records.next(), None);
        assert!(!records.ok());
        let bytes = packet(1, 9, b"short");
        let Some(Datagram::Packet(p)) = decode(&bytes) else { panic!() };
        assert!(p.split().is_none());
        let bytes = packet(1, 0, b"all tail");
        let Some(Datagram::Packet(p)) = decode(&bytes) else { panic!() };
        let (continuation, mut records) = p.split().unwrap();
        assert_eq!(continuation, b"all tail");
        assert_eq!(records.next(), None);
    }

    #[test]
    fn control_round_trips() {
        let mut buf = [0; HELLO_SIZE];
        hello(&mut buf, 3, 9);
        assert!(matches!(decode(&buf), Some(Datagram::Hello { session: 3, base: 9 })));
        let mut buf = [0; HELLO_ACK_SIZE];
        hello_ack(&mut buf, 4, 3, 11);
        assert!(matches!(
            decode(&buf),
            Some(Datagram::HelloAck { session: 4, their_session: 3, base: 11 })
        ));
        let mut buf = [0; RESET_SIZE];
        reset(&mut buf, 3);
        assert!(matches!(decode(&buf), Some(Datagram::Reset { their_session: 3 })));
        let mut header = [0; ACK_HEADER];
        ack_header(&mut header, 5, 100, 70);
        let mut ack = header.to_vec();
        ack.extend_from_slice(&[0; 16]);
        let Some(Datagram::Ack { session: 5, ack_next: 100, bits: 70, bitmap }) = decode(&ack)
        else {
            panic!()
        };
        assert_eq!(bitmap.len(), 16);
        ack.truncate(ACK_HEADER + 8);
        assert!(decode(&ack).is_none());
        assert!(decode(b"FX\x30junkjunkjunk").is_none());
    }
}
