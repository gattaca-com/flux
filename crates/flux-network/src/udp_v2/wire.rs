//! What both protocols carry the same way: the prefix, the packet body and
//! the messages packed into the stream. Each protocol lays out its own
//! datagrams under its own magic.
//!
//! ```text
//! every datagram
//! [0..2]    magic       one per protocol
//! [2]       version << 4 | kind
//!
//! packet body
//! [3..7]    session
//! [7..15]   seq
//! [15..23]  send_ts     first transmission of this packet
//! [23..25]  start       1 + payload offset of the first message header; 0 if none
//! [25..]    payload     a span of the stream, cut at any byte
//!
//! message, in the stream
//! [len u32 LE][len bytes]
//! ```

pub(crate) const VERSION: u8 = 2;
/// Largest UDP payload over IPv4.
pub(crate) const MAX_DATAGRAM_SIZE: usize = 65_507;

pub(crate) const PREFIX: usize = 3;
pub(crate) const PACKET_HEADER: usize = PREFIX + 4 + 8 + 8 + 2;
pub(crate) const MESSAGE_HEADER: usize = 4;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Packet<'a> {
    pub(crate) session: u32,
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
pub(crate) fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

#[inline]
pub(crate) fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap())
}

#[inline]
pub(crate) const fn prefix(magic: [u8; 2], kind: u8) -> [u8; PREFIX] {
    [magic[0], magic[1], (VERSION << 4) | kind]
}

/// The kind of a datagram under `magic`, with room for a session after it.
#[inline]
pub(crate) fn kind(bytes: &[u8], magic: [u8; 2]) -> Option<u8> {
    (bytes.len() >= PREFIX + 4 && bytes[0..2] == magic && bytes[2] >> 4 == VERSION)
        .then_some(bytes[2] & 0x0f)
}

pub(crate) fn decode_packet(bytes: &[u8]) -> Option<Packet<'_>> {
    (bytes.len() >= PACKET_HEADER).then(|| Packet {
        session: u32_at(bytes, 3),
        seq: u64_at(bytes, 7),
        send_ts: u64_at(bytes, 15),
        start: u16::from_le_bytes(bytes[23..25].try_into().unwrap()),
        payload: &bytes[PACKET_HEADER..],
    })
}

pub(crate) fn packet_header(
    buf: &mut [u8; PACKET_HEADER],
    prefix: [u8; PREFIX],
    session: u32,
    seq: u64,
    send_ts: u64,
    start: u16,
) {
    buf[..PREFIX].copy_from_slice(&prefix);
    buf[3..7].copy_from_slice(&session.to_le_bytes());
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

#[cfg(test)]
pub(crate) fn test_packet(
    session: u32,
    seq: u64,
    send_ts: u64,
    start: u16,
    payload: &[u8],
) -> Vec<u8> {
    let mut header = [0; PACKET_HEADER];
    packet_header(&mut header, prefix(*b"XX", 1), session, seq, send_ts, start);
    [&header[..], payload].concat()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_past_the_payload_is_malformed() {
        let bytes = test_packet(7, 1, 99, 9, b"short");
        assert!(decode_packet(&bytes).unwrap().split().is_none());
        let bytes = test_packet(7, 1, 99, 0, b"all tail");
        let (continuation, mut records) = decode_packet(&bytes).unwrap().split().unwrap();
        assert_eq!(continuation, b"all tail");
        assert_eq!(records.next(), None);
    }
}
