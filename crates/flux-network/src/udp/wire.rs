//! Datagram layout shared by every UDP peer. Internal links only, so the
//! header carries exactly what the protocol needs:
//!
//! ```text
//! [0]      version << 4 | kind
//! [1..5]   session      random per socket, changes on restart
//! [5..13]  seq          per-datagram sequence (Data) or ack point (Ack)
//! [13..17] len          message length (Data) or bitmap bit count (Ack)
//! [17..19] index        fragment index within the message (Data)
//! [19..27] send_ts      sender wall clock, for receive-side latency telemetry
//! ```
//!
//! An Ack's payload is its bitmap: bit `i` set means `seq + 1 + i` arrived.
//!
//! A message is split into `ceil(len / stride)` fragments carrying
//! consecutive sequence numbers, so the message is identified by the sequence
//! of its first fragment: `seq - index`. Anything with a foreign session is
//! dropped, which is what keeps stray datagrams out.

pub(crate) const VERSION: u8 = 1;
pub(crate) const HEADER_SIZE: usize = 27;
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
        buf[0] = (VERSION << 4) | self.kind as u8;
        buf[1..5].copy_from_slice(&self.session.to_le_bytes());
        buf[5..13].copy_from_slice(&self.seq.to_le_bytes());
        buf[13..17].copy_from_slice(&self.len.to_le_bytes());
        buf[17..19].copy_from_slice(&self.index.to_le_bytes());
        buf[19..27].copy_from_slice(&self.send_ts.to_le_bytes());
    }

    /// `None` for short, unknown-kind, or other-version datagrams.
    #[inline]
    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_SIZE || bytes[0] >> 4 != VERSION {
            return None;
        }
        Some(Self {
            kind: Kind::from_u8(bytes[0] & 0x0f)?,
            session: u32::from_le_bytes(bytes[1..5].try_into().unwrap()),
            seq: u64::from_le_bytes(bytes[5..13].try_into().unwrap()),
            len: u32::from_le_bytes(bytes[13..17].try_into().unwrap()),
            index: u16::from_le_bytes(bytes[17..19].try_into().unwrap()),
            send_ts: u64::from_le_bytes(bytes[19..27].try_into().unwrap()),
        })
    }
}

/// Rewrite only the send timestamp of an encoded header (retransmits).
#[inline]
pub(crate) fn write_send_ts(buf: &mut [u8], ts: u64) {
    buf[19..27].copy_from_slice(&ts.to_le_bytes());
}

/// Rewrite only the session of an encoded header (replay after reconnect).
#[inline]
pub(crate) fn write_session(buf: &mut [u8], session: u32) {
    buf[1..5].copy_from_slice(&session.to_le_bytes());
}

/// Fragments per message; an empty message is never sent.
#[inline]
pub(crate) fn fragment_count(len: usize, stride: usize) -> usize {
    len.div_ceil(stride)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let h = Header {
            kind: Kind::Data,
            session: 0xdead_beef,
            seq: u64::MAX - 7,
            len: 4096,
            index: 3,
            send_ts: 123_456_789,
        };
        let mut buf = [0u8; HEADER_SIZE];
        h.encode(&mut buf);
        assert_eq!(Header::decode(&buf), Some(h));
        write_send_ts(&mut buf, 5);
        assert_eq!(Header::decode(&buf).unwrap().send_ts, 5);
    }

    #[test]
    fn rejects_foreign_and_short() {
        let mut buf = [0u8; HEADER_SIZE];
        Header { kind: Kind::Ack, session: 1, seq: 2, len: 0, index: 0, send_ts: 0 }
            .encode(&mut buf);
        assert!(Header::decode(&buf[..HEADER_SIZE - 1]).is_none());
        buf[0] = ((VERSION + 1) << 4) | Kind::Ack as u8;
        assert!(Header::decode(&buf).is_none());
        buf[0] = (VERSION << 4) | 9;
        assert!(Header::decode(&buf).is_none());
    }

    #[test]
    fn fragment_counts() {
        assert_eq!(fragment_count(1, 1168), 1);
        assert_eq!(fragment_count(1168, 1168), 1);
        assert_eq!(fragment_count(1169, 1168), 2);
    }
}
