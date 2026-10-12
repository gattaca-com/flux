//! ```text
//! every datagram
//! [0..2]    magic       "FX"
//! [2]       version << 4 | kind
//!
//! packet (1)
//! [3..7]    publisher   session
//! [7..15]   seq
//! [15..23]  send_ts     first transmission of this packet
//! [23..25]  start       1 + payload offset of the first message header; 0 if none
//! [25..]    payload     a span of the stream, cut at any byte
//!
//! ack (2)
//! [3..7]    publisher   session
//! [7..11]   subscriber  session
//! [11..19]  ack_next    everything below arrived
//! [19..]    bitmap      bit i (LSB first) set: ack_next + 1 + i arrived
//!
//! hello (3)
//! [3..7]    subscriber  session
//! [7..]     on-connect message
//!
//! hello ack (4)
//! [3..7]    publisher   session
//! [7..11]   subscriber  session
//! [11..19]  join        first seq the subscriber gets
//! [19..]    on-connect message
//!
//! reset (5)
//! [3..7]    subscriber  session
//!
//! message, in the stream
//! [len u32 LE][len bytes]
//! ```

pub(crate) const MAGIC: [u8; 2] = *b"FX";
pub(crate) const VERSION: u8 = 2;
/// Largest UDP payload over IPv4.
pub(crate) const MAX_DATAGRAM_SIZE: usize = 65_507;

const PREFIX: usize = 3;
pub(crate) const PACKET_HEADER: usize = PREFIX + 4 + 8 + 8 + 2;
pub(crate) const MESSAGE_HEADER: usize = 4;
pub(crate) const ACK_HEADER: usize = PREFIX + 4 + 4 + 8;
pub(crate) const HELLO_SIZE: usize = PREFIX + 4;
pub(crate) const HELLO_ACK_SIZE: usize = PREFIX + 4 + 4 + 8;
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

#[derive(Clone, Copy, Debug)]
pub(crate) enum Datagram<'a> {
    Packet(Packet<'a>),
    Ack { publisher: u32, subscriber: u32, ack_next: u64, bitmap: &'a [u8] },
    Hello { subscriber: u32, msg: &'a [u8] },
    HelloAck { publisher: u32, subscriber: u32, join: u64, msg: &'a [u8] },
    Reset { subscriber: u32 },
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Packet<'a> {
    pub(crate) publisher: u32,
    pub(crate) seq: u64,
    pub(crate) send_ts: u64,
    start: u16,
    payload: &'a [u8],
}

impl<'a> Packet<'a> {
    pub(crate) fn split(&self) -> Option<(&'a [u8], Records<'a>)> {
        let at = match self.start {
            0 => self.payload.len(),
            s => usize::from(s) - 1,
        };
        let (continuation, rest) = self.payload.split_at_checked(at)?;
        Some((continuation, Records { bytes: rest }))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Records<'a> {
    bytes: &'a [u8],
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Record<'a> {
    Whole(&'a [u8]),
    /// Cut at the packet end, possibly inside its header.
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

pub(crate) fn decode(bytes: &[u8]) -> Option<Datagram<'_>> {
    if bytes.len() < PREFIX + 4 || bytes[0..2] != MAGIC || bytes[2] >> 4 != VERSION {
        return None;
    }
    match bytes[2] & 0x0f {
        1 if bytes.len() >= PACKET_HEADER => Some(Datagram::Packet(Packet {
            publisher: u32_at(bytes, 3),
            seq: u64_at(bytes, 7),
            send_ts: u64_at(bytes, 15),
            start: u16::from_le_bytes(bytes[23..25].try_into().unwrap()),
            payload: &bytes[PACKET_HEADER..],
        })),
        2 if bytes.len() >= ACK_HEADER => Some(Datagram::Ack {
            publisher: u32_at(bytes, 3),
            subscriber: u32_at(bytes, 7),
            ack_next: u64_at(bytes, 11),
            bitmap: &bytes[ACK_HEADER..],
        }),
        3 => Some(Datagram::Hello { subscriber: u32_at(bytes, 3), msg: &bytes[HELLO_SIZE..] }),
        4 if bytes.len() >= HELLO_ACK_SIZE => Some(Datagram::HelloAck {
            publisher: u32_at(bytes, 3),
            subscriber: u32_at(bytes, 7),
            join: u64_at(bytes, 11),
            msg: &bytes[HELLO_ACK_SIZE..],
        }),
        5 => Some(Datagram::Reset { subscriber: u32_at(bytes, 3) }),
        _ => None,
    }
}

fn prefix(buf: &mut [u8], kind: Kind) {
    buf[0..2].copy_from_slice(&MAGIC);
    buf[2] = (VERSION << 4) | kind as u8;
}

pub(crate) fn packet_header(
    buf: &mut [u8; PACKET_HEADER],
    publisher: u32,
    seq: u64,
    send_ts: u64,
    start: u16,
) {
    prefix(buf, Kind::Packet);
    buf[3..7].copy_from_slice(&publisher.to_le_bytes());
    buf[7..15].copy_from_slice(&seq.to_le_bytes());
    buf[15..23].copy_from_slice(&send_ts.to_le_bytes());
    buf[23..25].copy_from_slice(&start.to_le_bytes());
}

#[inline]
pub(crate) fn record_len(bytes: &[u8]) -> usize {
    if bytes.len() < MESSAGE_HEADER {
        return 0;
    }
    MESSAGE_HEADER + u32_at(bytes, 0) as usize
}

/// The caller appends the bitmap.
pub(crate) fn ack_header(
    buf: &mut [u8; ACK_HEADER],
    publisher: u32,
    subscriber: u32,
    ack_next: u64,
) {
    prefix(buf, Kind::Ack);
    buf[3..7].copy_from_slice(&publisher.to_le_bytes());
    buf[7..11].copy_from_slice(&subscriber.to_le_bytes());
    buf[11..19].copy_from_slice(&ack_next.to_le_bytes());
}

pub(crate) fn hello(buf: &mut [u8], subscriber: u32, msg: &[u8]) -> usize {
    prefix(buf, Kind::Hello);
    buf[3..7].copy_from_slice(&subscriber.to_le_bytes());
    buf[HELLO_SIZE..][..msg.len()].copy_from_slice(msg);
    HELLO_SIZE + msg.len()
}

pub(crate) fn hello_ack(
    buf: &mut [u8],
    publisher: u32,
    subscriber: u32,
    join: u64,
    msg: &[u8],
) -> usize {
    prefix(buf, Kind::HelloAck);
    buf[3..7].copy_from_slice(&publisher.to_le_bytes());
    buf[7..11].copy_from_slice(&subscriber.to_le_bytes());
    buf[11..19].copy_from_slice(&join.to_le_bytes());
    buf[HELLO_ACK_SIZE..][..msg.len()].copy_from_slice(msg);
    HELLO_ACK_SIZE + msg.len()
}

pub(crate) fn reset(buf: &mut [u8; RESET_SIZE], subscriber: u32) {
    prefix(buf, Kind::Reset);
    buf[3..7].copy_from_slice(&subscriber.to_le_bytes());
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
}
