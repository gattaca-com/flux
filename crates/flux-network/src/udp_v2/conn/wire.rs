//! Each side picks its own session, and every datagram but a hello names
//! the receiver's, so a lookup is one field. A reset names a session its
//! sender doesn't know: its own as the peer believed it, or the initiator's
//! from a refused hello.
//!
//! ```text
//! every datagram
//! [0..2]    magic       "FC"
//! [2]       version << 4 | kind
//!
//! hello (1)
//! [3..7]    initiator   session
//! [7..]     on-connect message
//!
//! hello ack (2)
//! [3..7]    initiator   session
//! [7..11]   acceptor    session
//! [11..]    on-connect message
//!
//! packet (3)
//! [3..7]    receiver    session
//! [7..15]   seq
//! [15..23]  send_ts     first transmission of this packet
//! [23..25]  start       1 + payload offset of the first message header; 0 if none
//! [25..]    payload     a span of the stream, cut at any byte
//!
//! ack (4)
//! [3..7]    receiver    session
//! [7..15]   ack_next    everything below arrived
//! [15..]    bitmap      bit i (LSB first) set: ack_next + 1 + i arrived
//!
//! reset (5)
//! [3..7]    unknown     session
//! ```

use crate::udp_v2::wire::{self, PREFIX, Packet, u32_at, u64_at};

const MAGIC: [u8; 2] = *b"FC";

pub(crate) const PACKET: [u8; PREFIX] = wire::prefix(MAGIC, Kind::Packet as u8);
pub(crate) const HELLO_SIZE: usize = PREFIX + 4;
pub(crate) const HELLO_ACK_SIZE: usize = PREFIX + 4 + 4;
pub(crate) const ACK_HEADER: usize = PREFIX + 4 + 8;
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
    Hello { initiator: u32, msg: &'a [u8] },
    HelloAck { initiator: u32, acceptor: u32, msg: &'a [u8] },
    Packet(Packet<'a>),
    Ack { receiver: u32, ack_next: u64, bitmap: &'a [u8] },
    Reset { session: u32 },
}

pub(crate) fn decode(bytes: &[u8]) -> Option<Datagram<'_>> {
    match wire::kind(bytes, MAGIC)? {
        1 => Some(Datagram::Hello { initiator: u32_at(bytes, 3), msg: &bytes[HELLO_SIZE..] }),
        2 if bytes.len() >= HELLO_ACK_SIZE => Some(Datagram::HelloAck {
            initiator: u32_at(bytes, 3),
            acceptor: u32_at(bytes, 7),
            msg: &bytes[HELLO_ACK_SIZE..],
        }),
        3 => wire::decode_packet(bytes).map(Datagram::Packet),
        4 if bytes.len() >= ACK_HEADER => Some(Datagram::Ack {
            receiver: u32_at(bytes, 3),
            ack_next: u64_at(bytes, 7),
            bitmap: &bytes[ACK_HEADER..],
        }),
        5 => Some(Datagram::Reset { session: u32_at(bytes, 3) }),
        _ => None,
    }
}

fn prefix(buf: &mut [u8], kind: Kind) {
    buf[..PREFIX].copy_from_slice(&wire::prefix(MAGIC, kind as u8));
}

pub(crate) fn hello(buf: &mut [u8], initiator: u32, msg: &[u8]) -> usize {
    prefix(buf, Kind::Hello);
    buf[3..7].copy_from_slice(&initiator.to_le_bytes());
    buf[HELLO_SIZE..][..msg.len()].copy_from_slice(msg);
    HELLO_SIZE + msg.len()
}

pub(crate) fn hello_ack(buf: &mut [u8], initiator: u32, acceptor: u32, msg: &[u8]) -> usize {
    prefix(buf, Kind::HelloAck);
    buf[3..7].copy_from_slice(&initiator.to_le_bytes());
    buf[7..11].copy_from_slice(&acceptor.to_le_bytes());
    buf[HELLO_ACK_SIZE..][..msg.len()].copy_from_slice(msg);
    HELLO_ACK_SIZE + msg.len()
}

/// The caller appends the bitmap.
pub(crate) fn ack_header(buf: &mut [u8; ACK_HEADER], receiver: u32, ack_next: u64) {
    prefix(buf, Kind::Ack);
    buf[3..7].copy_from_slice(&receiver.to_le_bytes());
    buf[7..15].copy_from_slice(&ack_next.to_le_bytes());
}

pub(crate) fn reset(buf: &mut [u8; RESET_SIZE], session: u32) {
    prefix(buf, Kind::Reset);
    buf[3..7].copy_from_slice(&session.to_le_bytes());
}
