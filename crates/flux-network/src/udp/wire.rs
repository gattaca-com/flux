pub const UDP_MAGIC: [u8; 3] = *b"FLX";

/// Bump on upgrade
pub const UDP_VERSION: u8 = 1;

/// `magic(3) + version(1) + session_id(4) + seq(8) + len(4) + offset(4) +
/// send_ns(8)`
pub const UDP_HEADER_SIZE: usize = 32;

pub const SUBSCRIBE: [u8; 8] = [UDP_MAGIC[0], UDP_MAGIC[1], UDP_MAGIC[2], UDP_VERSION, 1, 0, 0, 0];

/// Fits the 1280-byte IPv6 minimum MTU, so no per-family choice.
pub const DEFAULT_MAX_DATAGRAM_SIZE: usize = 1200;

pub const MAX_DATAGRAM_SIZE: usize = 65_507;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FragmentHeader {
    /// Random per process. Changes per restart
    pub session_id: u32,
    pub seq: u64,
    pub len: u32,
    pub offset: u32,
    pub send_ns: u64,
}

impl FragmentHeader {
    pub fn encode(self, buf: &mut [u8; UDP_HEADER_SIZE]) {
        buf[..3].copy_from_slice(&UDP_MAGIC);
        buf[3] = UDP_VERSION;
        buf[4..8].copy_from_slice(&self.session_id.to_le_bytes());
        buf[8..16].copy_from_slice(&self.seq.to_le_bytes());
        buf[16..20].copy_from_slice(&self.len.to_le_bytes());
        buf[20..24].copy_from_slice(&self.offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.send_ns.to_le_bytes());
    }

    /// `None` if short, foreign, or another version.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < UDP_HEADER_SIZE || bytes[..3] != UDP_MAGIC || bytes[3] != UDP_VERSION {
            return None;
        }
        Some(Self {
            session_id: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            seq: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            len: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            offset: u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
            send_ns: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Fragment<'a> {
    pub header: FragmentHeader,
    pub index: usize,
    pub payload: &'a [u8],
}

impl<'a> Fragment<'a> {
    /// `fragment_payload_size` must match the publisher's.
    pub fn decode(bytes: &'a [u8], fragment_payload_size: usize) -> Option<Self> {
        let header = FragmentHeader::decode(bytes)?;
        let payload = &bytes[UDP_HEADER_SIZE..];
        let (offset, len) = (header.offset as usize, header.len as usize);
        // catch short datagrams
        if offset > len ||
            !offset.is_multiple_of(fragment_payload_size) ||
            payload.len() != (len - offset).min(fragment_payload_size)
        {
            return None;
        }
        Some(Self { header, index: offset / fragment_payload_size, payload })
    }
}

/// Calls `emit` per datagram, reusing `buf`. An empty message still sends one,
/// so its sequence number is accounted for.
pub fn encode_fragments<F>(
    max_datagram_size: usize,
    session_id: u32,
    seq: u64,
    send_ns: u64,
    message: &[u8],
    buf: &mut Vec<u8>,
    mut emit: F,
) where
    F: FnMut(&[u8]),
{
    let fragment_payload_size = max_datagram_size - UDP_HEADER_SIZE;
    let len = message.len() as u32;
    let mut header = [0u8; UDP_HEADER_SIZE];

    let mut send = |offset: u32, payload: &[u8]| {
        FragmentHeader { session_id, seq, len, offset, send_ns }.encode(&mut header);
        buf.clear();
        buf.extend_from_slice(&header);
        buf.extend_from_slice(payload);
        emit(buf);
    };

    if message.is_empty() {
        send(0, &[]);
        return;
    }
    for (index, payload) in message.chunks(fragment_payload_size).enumerate() {
        send((index * fragment_payload_size) as u32, payload);
    }
}

/// An empty message is one datagram.
pub fn fragment_count(len: usize, fragment_payload_size: usize) -> usize {
    len.div_ceil(fragment_payload_size).max(1)
}
