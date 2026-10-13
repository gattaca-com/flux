//! ```text
//! every datagram
//! [0..2]    magic       "FP"
//! [2]       version << 4 | kind
//!
//! hello (1)
//! [3..7]    subscriber  session
//! [7..]     on-connect message
//!
//! hello ack (2)
//! [3..7]    publisher   session
//! [7..11]   subscriber  session
//! [11..19]  join        first seq the subscriber gets
//! [19..]    on-connect message
//!
//! packet (3)
//! [3..7]    publisher   session
//! [7..15]   seq
//! [15..23]  send_ts     first transmission of this packet
//! [23..25]  start       1 + payload offset of the first message header; 0 if none
//! [25..]    payload     a span of the stream, cut at any byte
//!
//! ack (4)
//! [3..7]    publisher   session
//! [7..11]   subscriber  session
//! [11..19]  ack_next    everything below arrived
//! [19..]    bitmap      bit i (LSB first) set: ack_next + 1 + i arrived
//!
//! reset (5)
//! [3..7]    subscriber  session
//! ```

use crate::udp_v2::wire::{self, PREFIX, Packet, u32_at, u64_at};

const MAGIC: [u8; 2] = *b"FP";

pub(crate) const PACKET: [u8; PREFIX] = wire::prefix(MAGIC, Kind::Packet as u8);
pub(crate) const ACK_HEADER: usize = PREFIX + 4 + 4 + 8;
pub(crate) const HELLO_SIZE: usize = PREFIX + 4;
pub(crate) const HELLO_ACK_SIZE: usize = PREFIX + 4 + 4 + 8;
pub(crate) const RESET_SIZE: usize = PREFIX + 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Kind {
    Hello = 1,
    HelloAck = 2,
    Packet = 3,
    Ack = 4,
    Reset = 5,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Datagram<'a> {
    Hello { subscriber: u32, msg: &'a [u8] },
    HelloAck { publisher: u32, subscriber: u32, join: u64, msg: &'a [u8] },
    Packet(Packet<'a>),
    Ack { publisher: u32, subscriber: u32, ack_next: u64, bitmap: &'a [u8] },
    Reset { subscriber: u32 },
}

pub(crate) fn decode(bytes: &[u8]) -> Option<Datagram<'_>> {
    match wire::kind(bytes, MAGIC)? {
        1 => Some(Datagram::Hello { subscriber: u32_at(bytes, 3), msg: &bytes[HELLO_SIZE..] }),
        2 if bytes.len() >= HELLO_ACK_SIZE => Some(Datagram::HelloAck {
            publisher: u32_at(bytes, 3),
            subscriber: u32_at(bytes, 7),
            join: u64_at(bytes, 11),
            msg: &bytes[HELLO_ACK_SIZE..],
        }),
        3 => wire::decode_packet(bytes).map(Datagram::Packet),
        4 if bytes.len() >= ACK_HEADER => Some(Datagram::Ack {
            publisher: u32_at(bytes, 3),
            subscriber: u32_at(bytes, 7),
            ack_next: u64_at(bytes, 11),
            bitmap: &bytes[ACK_HEADER..],
        }),
        5 => Some(Datagram::Reset { subscriber: u32_at(bytes, 3) }),
        _ => None,
    }
}

fn prefix(buf: &mut [u8], kind: Kind) {
    buf[..PREFIX].copy_from_slice(&wire::prefix(MAGIC, kind as u8));
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
