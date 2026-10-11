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
//! may be cut at any byte, its length included, and continue in the next.
//! Every message starts with a four-byte little-endian `u32` payload length.
//!
//! An ack (kind 2) carries the cumulative point `ack_next` and a bitmap of
//! `bits` sequences above it: bit `i` set means `ack_next + 1 + i` arrived.
//! A hello (kind 3) announces the sender's first unacked packet sequence; a
//! hello ack (kind 4) echoes the session it answers and announces its own
//! base. Both carry the sender's on-connect message, if any, as the rest of
//! the datagram. A reset (kind 5) names the session a listener holds no
//! peer for.

pub(crate) const MAGIC: [u8; 2] = *b"FX";
pub(crate) const VERSION: u8 = 2;
/// Largest UDP payload over IPv4.
pub(crate) const MAX_DATAGRAM_SIZE: usize = 65_507;

const PREFIX: usize = 7;
pub(crate) const PACKET_HEADER: usize = PREFIX + 8 + 8 + 2;
pub(crate) const MESSAGE_HEADER: usize = 4;
pub(crate) const ACK_HEADER: usize = PREFIX + 8 + 2;
pub(crate) const HELLO_SIZE: usize = PREFIX + 8;
pub(crate) const HELLO_ACK_SIZE: usize = PREFIX + 4 + 8;
pub(crate) const RESET_SIZE: usize = PREFIX + 4;

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
    Ack {
        session: u32,
        ack_next: u64,
        bits: u16,
        bitmap: &'a [u8],
    },
    /// `msg` is the on-connect message; empty without one.
    Hello {
        session: u32,
        base: u64,
        msg: &'a [u8],
    },
    HelloAck {
        session: u32,
        their_session: u32,
        base: u64,
        msg: &'a [u8],
    },
    Reset {
        their_session: u32,
    },
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
        Some((continuation, Records { bytes: rest }))
    }
}

/// The messages starting in a packet, decoded one at a time.
#[derive(Clone, Debug)]
pub(crate) struct Records<'a> {
    bytes: &'a [u8],
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Record<'a> {
    Whole(&'a [u8]),
    /// The leading bytes of a message, header first, possibly not all of
    /// the header; the rest follows in the next packets.
    Head(&'a [u8]),
}

impl<'a> Iterator for Records<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Record<'a>> {
        if self.bytes.is_empty() {
            return None;
        }
        let record = self.bytes;
        let len = record_len(record);
        if len != 0 && len <= record.len() {
            self.bytes = &record[len..];
            return Some(Record::Whole(&record[MESSAGE_HEADER..len]));
        }
        self.bytes = &[];
        Some(Record::Head(record))
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
        3 if bytes.len() >= HELLO_SIZE => {
            Some(Datagram::Hello { session, base: u64_at(body, 0), msg: &body[8..] })
        }
        4 if bytes.len() >= HELLO_ACK_SIZE => Some(Datagram::HelloAck {
            session,
            their_session: u32_at(body, 0),
            base: u64_at(body, 4),
            msg: &body[12..],
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

/// Total record length, including its header; zero when `bytes` holds less
/// than a header.
#[inline]
pub(crate) fn record_len(bytes: &[u8]) -> usize {
    if bytes.len() < MESSAGE_HEADER {
        return 0;
    }
    MESSAGE_HEADER + u32_at(bytes, 0) as usize
}

/// Writes an ack header; the caller appends `bits.div_ceil(64) * 8` bitmap
/// bytes.
pub(crate) fn ack_header(buf: &mut [u8; ACK_HEADER], session: u32, ack_next: u64, bits: u16) {
    prefix(buf, Kind::Ack, session);
    buf[7..15].copy_from_slice(&ack_next.to_le_bytes());
    buf[15..17].copy_from_slice(&bits.to_le_bytes());
}

/// Writes a hello carrying `msg` to the front of `buf`; returns its length.
pub(crate) fn hello(buf: &mut [u8], session: u32, base: u64, msg: &[u8]) -> usize {
    prefix(buf, Kind::Hello, session);
    buf[7..15].copy_from_slice(&base.to_le_bytes());
    buf[HELLO_SIZE..][..msg.len()].copy_from_slice(msg);
    HELLO_SIZE + msg.len()
}

/// Writes a hello ack carrying `msg` to the front of `buf`; returns its
/// length.
pub(crate) fn hello_ack(
    buf: &mut [u8],
    session: u32,
    their_session: u32,
    base: u64,
    msg: &[u8],
) -> usize {
    prefix(buf, Kind::HelloAck, session);
    buf[7..11].copy_from_slice(&their_session.to_le_bytes());
    buf[11..19].copy_from_slice(&base.to_le_bytes());
    buf[HELLO_ACK_SIZE..][..msg.len()].copy_from_slice(msg);
    HELLO_ACK_SIZE + msg.len()
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
        let mut out = (bytes.len() as u32).to_le_bytes().to_vec();
        out.extend_from_slice(bytes);
        out
    }

    #[test]
    fn packet_round_trip() {
        let mut payload = b"tail".to_vec();
        payload.extend(message(b"abc"));
        payload.extend(message(b""));
        payload.extend_from_slice(&40_000u32.to_le_bytes());
        payload.extend_from_slice(&[7; 2]);
        let bytes = packet(42, 5, &payload);
        let Some(Datagram::Packet(p)) = decode(&bytes) else { panic!() };
        assert_eq!((p.session, p.seq, p.send_ts), (7, 42, 99));
        let (continuation, mut records) = p.split().unwrap();
        assert_eq!(continuation, b"tail");
        assert_eq!(records.next(), Some(Record::Whole(b"abc")));
        assert_eq!(records.next(), Some(Record::Whole(b"")));
        assert_eq!(records.next(), Some(Record::Head(&payload[15..])));
        assert_eq!(records.next(), None);
    }

    #[test]
    fn start_past_the_payload_is_malformed() {
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
        let mut buf = [0; 64];
        let n = hello(&mut buf, 3, 9, b"hi");
        assert!(matches!(
            decode(&buf[..n]),
            Some(Datagram::Hello { session: 3, base: 9, msg: b"hi" })
        ));
        let n = hello(&mut buf, 3, 9, b"");
        assert!(matches!(decode(&buf[..n]), Some(Datagram::Hello { msg: b"", .. })));
        let n = hello_ack(&mut buf, 4, 3, 11, b"yo");
        assert!(matches!(
            decode(&buf[..n]),
            Some(Datagram::HelloAck { session: 4, their_session: 3, base: 11, msg: b"yo" })
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
