//! Datagram encodings. Nothing spans two datagrams: each one decodes on its
//! own, in any order.
//!
//! Every datagram starts with the same prefix:
//!
//! ```text
//! [0..2]  magic "FX"; anything else is not ours and is dropped unread
//! [2]     version << 4 | kind
//! [3..7]  session of the sender; 0 in a reset, which has none
//! ```
//!
//! A packet (kind 1) carries records of one session:
//!
//! ```text
//! [7..15]  seq       per packet; the unit of acks and retransmission
//! [15..23] send_ts   sender wall clock when the packet first went out
//! [23..25] count     records that follow
//! ```
//!
//! A record is a whole message or a fragment of one, each prefixed by its
//! length; a fragment also names the message and its byte range:
//!
//! ```text
//! whole:     [len u16, top bit clear][len bytes]
//! fragment:  [len u16, top bit set][message u32][offset u32][total u32][len bytes]
//! ```
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
pub(crate) const WHOLE_HEADER: usize = 2;
pub(crate) const FRAGMENT_HEADER: usize = 2 + 4 + 4 + 4;
/// Longest record payload: the length field keeps one bit for the kind.
pub(crate) const MAX_RECORD: usize = (1 << 15) - 1;
pub(crate) const ACK_HEADER: usize = PREFIX + 8 + 2;
pub(crate) const HELLO_SIZE: usize = PREFIX + 8;
pub(crate) const HELLO_ACK_SIZE: usize = PREFIX + 4 + 8;
pub(crate) const RESET_SIZE: usize = PREFIX + 4;
const FRAGMENT: u16 = 1 << 15;

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

/// A received packet of records.
#[derive(Clone, Debug)]
pub(crate) struct Packet<'a> {
    pub(crate) session: u32,
    pub(crate) seq: u64,
    pub(crate) send_ts: u64,
    pub(crate) records: Records<'a>,
}

/// The records of a packet, decoded one at a time; stops early at a
/// malformed one.
#[derive(Clone, Debug)]
pub(crate) struct Records<'a> {
    left: u16,
    bytes: &'a [u8],
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Record<'a> {
    Whole(&'a [u8]),
    Fragment { message: u32, offset: u32, total: u32, bytes: &'a [u8] },
}

impl Records<'_> {
    /// Records the packet announces, decoded or not.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        usize::from(self.left)
    }
}

impl<'a> Iterator for Records<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Record<'a>> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let len = u16::from_le_bytes(self.bytes.get(..2)?.try_into().unwrap());
        let (header, payload) = if len & FRAGMENT == 0 {
            (WHOLE_HEADER, usize::from(len))
        } else {
            (FRAGMENT_HEADER, usize::from(len & !FRAGMENT))
        };
        let end = header + payload;
        let bytes = self.bytes.get(..end)?;
        self.bytes = &self.bytes[end..];
        if len & FRAGMENT == 0 {
            return Some(Record::Whole(&bytes[WHOLE_HEADER..]));
        }
        let message = u32_at(bytes, 2);
        let offset = u32_at(bytes, 6);
        let total = u32_at(bytes, 10);
        // A fragment that could not be part of its message is malformed.
        if payload == 0 || offset.checked_add(payload as u32)? > total {
            self.left = 0;
            return None;
        }
        Some(Record::Fragment { message, offset, total, bytes: &bytes[FRAGMENT_HEADER..] })
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
            records: Records {
                left: u16::from_le_bytes(body[16..18].try_into().unwrap()),
                bytes: &body[18..],
            },
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
    count: u16,
) {
    prefix(buf, Kind::Packet, session);
    buf[7..15].copy_from_slice(&seq.to_le_bytes());
    buf[15..23].copy_from_slice(&send_ts.to_le_bytes());
    buf[23..25].copy_from_slice(&count.to_le_bytes());
}

pub(crate) fn whole_header(buf: &mut [u8; WHOLE_HEADER], len: usize) {
    debug_assert!(len <= MAX_RECORD);
    buf.copy_from_slice(&(len as u16).to_le_bytes());
}

pub(crate) fn fragment_header(
    buf: &mut [u8; FRAGMENT_HEADER],
    len: usize,
    message: u32,
    offset: u32,
    total: u32,
) {
    debug_assert!(len <= MAX_RECORD);
    buf[0..2].copy_from_slice(&(len as u16 | FRAGMENT).to_le_bytes());
    buf[2..6].copy_from_slice(&message.to_le_bytes());
    buf[6..10].copy_from_slice(&offset.to_le_bytes());
    buf[10..14].copy_from_slice(&total.to_le_bytes());
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

    #[test]
    fn packet_round_trip() {
        let mut header = [0; PACKET_HEADER];
        packet_header(&mut header, 7, 42, 99, 3);
        let mut bytes = header.to_vec();
        let mut whole = [0; WHOLE_HEADER];
        whole_header(&mut whole, 3);
        bytes.extend_from_slice(&whole);
        bytes.extend_from_slice(b"abc");
        let mut fragment = [0; FRAGMENT_HEADER];
        fragment_header(&mut fragment, 2, 5, 10, 12);
        bytes.extend_from_slice(&fragment);
        bytes.extend_from_slice(b"xy");
        whole_header(&mut whole, 0);
        bytes.extend_from_slice(&whole);
        let Some(Datagram::Packet(Packet { session: 7, seq: 42, send_ts: 99, records })) =
            decode(&bytes)
        else {
            panic!("not a packet");
        };
        let records: Vec<Record<'_>> = records.collect();
        assert_eq!(records, [
            Record::Whole(b"abc"),
            Record::Fragment { message: 5, offset: 10, total: 12, bytes: b"xy" },
            Record::Whole(b""),
        ]);
    }

    #[test]
    fn truncated_records_stop_cleanly() {
        let mut header = [0; PACKET_HEADER];
        packet_header(&mut header, 1, 1, 0, 2);
        let mut bytes = header.to_vec();
        let mut whole = [0; WHOLE_HEADER];
        whole_header(&mut whole, 2);
        bytes.extend_from_slice(&whole);
        bytes.extend_from_slice(b"ok");
        whole_header(&mut whole, 9);
        bytes.extend_from_slice(&whole);
        bytes.extend_from_slice(b"short");
        let Some(Datagram::Packet(Packet { records, .. })) = decode(&bytes) else { panic!() };
        assert_eq!(records.collect::<Vec<_>>(), [Record::Whole(b"ok")]);
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
