//! Datagram layout.
//!
//! ```text
//! [0..2]   magic        "FX"; anything else is not ours and is dropped unread
//! [2]      version << 4 | kind
//! [3..7]   session      random per connection attempt
//! [7..15]  seq          per-datagram sequence (Data) or ack point (Ack)
//! [15..19] len          message length (Data) or bitmap bit count (Ack)
//! [19..21] index        fragment index within the message (Data)
//! [21..29] send_ts      sender wall clock, for receive-side latency telemetry
//! ```
//!
//! An Ack's payload is its bitmap: bit `i` set means `seq + 1 + i` arrived.
//!
//! A message is split into `ceil(len / stride)` fragments carrying
//! consecutive sequence numbers, so the message is identified by the sequence
//! of its first fragment: `seq - index`. Datagrams from any other session are
//! dropped.

pub(crate) const MAGIC: [u8; 2] = *b"FX";
pub(crate) const VERSION: u8 = 1;
pub(crate) const HEADER_SIZE: usize = 29;
/// Largest UDP payload over IPv4.
pub(crate) const MAX_DATAGRAM_SIZE: usize = 65_507;
/// Fragment index is a `u16`.
pub(crate) const MAX_FRAGMENTS: usize = 1 << 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Kind {
    Data = 1,
    Ack = 2,
    Hello = 3,
    HelloAck = 4,
    /// From a listener to a sender it holds no session for: renegotiate.
    Reset = 5,
}

impl Kind {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Data),
            2 => Some(Self::Ack),
            3 => Some(Self::Hello),
            4 => Some(Self::HelloAck),
            5 => Some(Self::Reset),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) kind: Kind,
    pub(crate) session: u32,
    pub(crate) seq: u64,
    pub(crate) len: u32,
    pub(crate) index: u16,
    pub(crate) send_ts: u64,
}

impl Header {
    #[inline]
    pub(crate) fn encode(&self, buf: &mut [u8]) {
        buf[0..2].copy_from_slice(&MAGIC);
        buf[2] = (VERSION << 4) | self.kind as u8;
        buf[3..7].copy_from_slice(&self.session.to_le_bytes());
        buf[7..15].copy_from_slice(&self.seq.to_le_bytes());
        buf[15..19].copy_from_slice(&self.len.to_le_bytes());
        buf[19..21].copy_from_slice(&self.index.to_le_bytes());
        buf[21..29].copy_from_slice(&self.send_ts.to_le_bytes());
    }

    /// `None` for short, foreign, unknown-kind, or other-version datagrams.
    #[inline]
    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_SIZE || bytes[0..2] != MAGIC || bytes[2] >> 4 != VERSION {
            return None;
        }
        Some(Self {
            kind: Kind::from_u8(bytes[2] & 0x0f)?,
            session: u32::from_le_bytes(bytes[3..7].try_into().unwrap()),
            seq: u64::from_le_bytes(bytes[7..15].try_into().unwrap()),
            len: u32::from_le_bytes(bytes[15..19].try_into().unwrap()),
            index: u16::from_le_bytes(bytes[19..21].try_into().unwrap()),
            send_ts: u64::from_le_bytes(bytes[21..29].try_into().unwrap()),
        })
    }
}

/// Rewrite only the session of an encoded header (replay after reconnect).
#[inline]
pub(crate) fn write_session(buf: &mut [u8], session: u32) {
    buf[3..7].copy_from_slice(&session.to_le_bytes());
}

/// Fragments per message; an empty message is never sent.
#[inline]
pub(crate) fn fragment_count(len: usize, stride: usize) -> usize {
    len.div_ceil(stride)
}
