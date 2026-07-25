use std::net::SocketAddr;

use thiserror::Error;

/// Magic bytes at the start of every UDP datagram.
pub const UDP_MAGIC: [u8; 3] = *b"FLX";
/// Reliable UDP wire version implemented by this module.
pub const UDP_VERSION: u8 = 1;
/// Size of the fixed version 1 UDP header.
pub const UDP_HEADER_SIZE: usize = 24;
/// Default maximum UDP payload for an IPv4 publisher, including the Flux
/// header.
pub const DEFAULT_IPV4_MAX_DATAGRAM_SIZE: usize = 1400;
/// Default maximum UDP payload for an IPv6 publisher, including the Flux
/// header.
pub const DEFAULT_IPV6_MAX_DATAGRAM_SIZE: usize = 1200;
/// Largest portable UDP payload supported by the encoder (IPv4 maximum).
pub const MAX_DATAGRAM_SIZE: usize = 65_507;

/// Selects the family-specific default from the publisher address.
pub fn default_max_datagram_size_for(publisher_addr: SocketAddr) -> usize {
    if publisher_addr.is_ipv4() {
        DEFAULT_IPV4_MAX_DATAGRAM_SIZE
    } else {
        DEFAULT_IPV6_MAX_DATAGRAM_SIZE
    }
}

/// Fixed, self-describing header carried by every UDP fragment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FragmentHeader {
    pub(crate) session_id: u32,
    pub(crate) seq: u64,
    pub(crate) len: u32,
    pub(crate) offset: u32,
}

impl FragmentHeader {
    pub(crate) fn encode(self, buf: &mut [u8; UDP_HEADER_SIZE]) {
        buf[..3].copy_from_slice(&UDP_MAGIC);
        buf[3] = UDP_VERSION;
        buf[4..8].copy_from_slice(&self.session_id.to_le_bytes());
        buf[8..16].copy_from_slice(&self.seq.to_le_bytes());
        buf[16..20].copy_from_slice(&self.len.to_le_bytes());
        buf[20..24].copy_from_slice(&self.offset.to_le_bytes());
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DatagramError> {
        if bytes.len() < UDP_HEADER_SIZE {
            return Err(DatagramError::Truncated { actual: bytes.len() });
        }
        if bytes[..3] != UDP_MAGIC {
            return Err(DatagramError::InvalidMagic);
        }
        if bytes[3] != UDP_VERSION {
            return Err(DatagramError::UnsupportedVersion(bytes[3]));
        }

        Ok(Self {
            session_id: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            seq: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            len: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            offset: u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
        })
    }
}

/// A validated borrowed UDP fragment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Fragment<'a> {
    pub(crate) header: FragmentHeader,
    pub(crate) index: usize,
    pub(crate) payload: &'a [u8],
}

impl<'a> Fragment<'a> {
    pub(crate) fn decode(
        datagram: &'a [u8],
        expected_session: u32,
        fragment_payload_size: usize,
    ) -> Result<Self, DatagramError> {
        let header = FragmentHeader::decode(datagram)?;

        if header.session_id != expected_session {
            return Err(DatagramError::UnexpectedSession {
                expected: expected_session,
                actual: header.session_id,
            });
        }

        let payload = &datagram[UDP_HEADER_SIZE..];
        if payload.is_empty() {
            if header.len != 0 || header.offset != 0 {
                return Err(DatagramError::EmptyFragment);
            }
            return Ok(Fragment { header, index: 0, payload });
        }

        if header.offset >= header.len {
            return Err(DatagramError::FragmentOutOfBounds {
                offset: header.offset,
                payload_length: payload.len(),
                message_length: header.len,
            });
        }
        if !(header.offset as usize).is_multiple_of(fragment_payload_size) {
            return Err(DatagramError::MisalignedFragment {
                offset: header.offset,
                fragment_payload_size,
            });
        }

        let remaining = (header.len - header.offset) as usize;
        let expected_length = remaining.min(fragment_payload_size);
        if payload.len() != expected_length {
            return Err(DatagramError::InvalidFragmentLength {
                offset: header.offset,
                actual: payload.len(),
                expected: expected_length,
                message_length: header.len,
            });
        }

        Ok(Fragment { header, index: header.offset as usize / fragment_payload_size, payload })
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum DatagramError {
    #[error("UDP datagram is truncated ({actual} bytes)")]
    Truncated { actual: usize },
    #[error("UDP datagram has invalid magic")]
    InvalidMagic,
    #[error("unsupported UDP protocol version {0}")]
    UnsupportedVersion(u8),
    #[error("UDP datagram session {actual} does not match authoritative session {expected}")]
    UnexpectedSession { expected: u32, actual: u32 },
    #[error("non-empty UDP message fragment has no payload")]
    EmptyFragment,
    #[error(
        "UDP fragment at offset {offset} with length {payload_length} is outside message length {message_length}"
    )]
    FragmentOutOfBounds { offset: u32, payload_length: usize, message_length: u32 },
    #[error(
        "UDP fragment offset {offset} is not aligned to fragment payload size {fragment_payload_size}"
    )]
    MisalignedFragment { offset: u32, fragment_payload_size: usize },
    #[error(
        "UDP fragment at offset {offset} has length {actual}, expected {expected} for message length {message_length}"
    )]
    InvalidFragmentLength { offset: u32, actual: usize, expected: usize, message_length: u32 },
}

pub(crate) fn encode_fragments<F>(
    max_datagram_size: usize,
    session_id: u32,
    seq: u64,
    message: &[u8],
    mut emit: F,
) -> bool
where
    F: FnMut(FragmentHeader, &[u8]) -> bool,
{
    let len = message.len() as u32;
    if message.is_empty() {
        let header = FragmentHeader { session_id, seq, len, offset: 0 };
        return emit(header, message);
    }

    let fragment_payload_size = max_datagram_size - UDP_HEADER_SIZE;
    for (index, payload) in message.chunks(fragment_payload_size).enumerate() {
        let offset = (index * fragment_payload_size) as u32;
        let header = FragmentHeader { session_id, seq, len, offset };
        if !emit(header, payload) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    const SESSION: u32 = 0x1234_5678;
    const FRAGMENT_PAYLOAD_SIZE: usize = 4;

    fn datagram(header: FragmentHeader, payload: &[u8]) -> Vec<u8> {
        let mut buf = [0; UDP_HEADER_SIZE];
        header.encode(&mut buf);
        let mut bytes = buf.to_vec();
        bytes.extend_from_slice(payload);
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Fragment<'_>, DatagramError> {
        Fragment::decode(bytes, SESSION, FRAGMENT_PAYLOAD_SIZE)
    }

    #[test]
    fn fragment_roundtrip() {
        let header = FragmentHeader { session_id: SESSION, seq: 42, len: 10, offset: 4 };
        let bytes = datagram(header, b"4567");
        let fragment = decode(&bytes).unwrap();
        assert_eq!(fragment.header, header);
        assert_eq!(fragment.index, 1);
        assert_eq!(fragment.payload, b"4567");
    }

    #[test]
    fn tail_fragment_roundtrip() {
        let header = FragmentHeader { session_id: SESSION, seq: 42, len: 10, offset: 8 };
        let bytes = datagram(header, b"89");
        let fragment = decode(&bytes).unwrap();
        assert_eq!(fragment.header, header);
        assert_eq!(fragment.index, 2);
        assert_eq!(fragment.payload, b"89");
    }

    #[test]
    fn empty_message_roundtrip() {
        let header = FragmentHeader { session_id: SESSION, seq: 7, len: 0, offset: 0 };
        let bytes = datagram(header, b"");
        let fragment = decode(&bytes).unwrap();
        assert_eq!(fragment.header, header);
        assert_eq!(fragment.index, 0);
        assert!(fragment.payload.is_empty());
    }

    #[test]
    fn encode_fragments_roundtrip() {
        let message = b"0123456789a"; // fragments of 4, 4 and 3 bytes
        let mut reassembled = vec![0; message.len()];
        let mut fragments = 0;
        let encoded_all = encode_fragments(
            UDP_HEADER_SIZE + FRAGMENT_PAYLOAD_SIZE,
            SESSION,
            9,
            message,
            |header, payload| {
                let bytes = datagram(header, payload);
                let fragment = decode(&bytes).unwrap();
                assert_eq!(fragment.header.seq, 9);
                assert_eq!(fragment.header.len, message.len() as u32);
                let offset = fragment.index * FRAGMENT_PAYLOAD_SIZE;
                reassembled[offset..offset + payload.len()].copy_from_slice(fragment.payload);
                fragments += 1;
                true
            },
        );
        assert!(encoded_all);
        assert_eq!(fragments, 3);
        assert_eq!(&reassembled[..], &message[..]);
    }

    #[test]
    fn decode_rejects_invalid_datagrams() {
        let valid =
            datagram(FragmentHeader { session_id: SESSION, seq: 1, len: 4, offset: 0 }, b"abcd");

        assert_eq!(
            decode(&valid[..UDP_HEADER_SIZE - 1]),
            Err(DatagramError::Truncated { actual: UDP_HEADER_SIZE - 1 })
        );

        let mut bad_magic = valid.clone();
        bad_magic[0] = b'X';
        assert_eq!(decode(&bad_magic), Err(DatagramError::InvalidMagic));

        let mut bad_version = valid.clone();
        bad_version[3] = UDP_VERSION + 1;
        assert_eq!(decode(&bad_version), Err(DatagramError::UnsupportedVersion(UDP_VERSION + 1)));

        assert_eq!(
            Fragment::decode(&valid, SESSION + 1, FRAGMENT_PAYLOAD_SIZE),
            Err(DatagramError::UnexpectedSession { expected: SESSION + 1, actual: SESSION })
        );

        let past_end =
            datagram(FragmentHeader { session_id: SESSION, seq: 1, len: 4, offset: 4 }, b"x");
        assert_eq!(
            decode(&past_end),
            Err(DatagramError::FragmentOutOfBounds {
                offset: 4,
                payload_length: 1,
                message_length: 4
            })
        );

        let misaligned =
            datagram(FragmentHeader { session_id: SESSION, seq: 1, len: 10, offset: 2 }, b"abcd");
        assert_eq!(
            decode(&misaligned),
            Err(DatagramError::MisalignedFragment {
                offset: 2,
                fragment_payload_size: FRAGMENT_PAYLOAD_SIZE
            })
        );

        let short_non_tail =
            datagram(FragmentHeader { session_id: SESSION, seq: 1, len: 10, offset: 4 }, b"abc");
        assert_eq!(
            decode(&short_non_tail),
            Err(DatagramError::InvalidFragmentLength {
                offset: 4,
                actual: 3,
                expected: 4,
                message_length: 10
            })
        );

        let missing_payload =
            datagram(FragmentHeader { session_id: SESSION, seq: 1, len: 4, offset: 0 }, b"");
        assert_eq!(decode(&missing_payload), Err(DatagramError::EmptyFragment));
    }
}
