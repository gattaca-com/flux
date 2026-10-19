//! WebSocket (RFC 6455) framing for upgraded HTTP connections.
//!
//! [`HttpNetwork`](crate::http::HttpNetwork) uses it for the handshake keys,
//! frame headers and the frame parser. No extension is negotiated, so frames
//! are never compressed.

/// A message, or a frame to send. Received pings are delivered and answered
/// once per read, with the last one's payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WsKind {
    Text,
    Binary,
    Ping,
    Pong,
}

pub(crate) const OP_CONTINUATION: u8 = 0x0;
pub(crate) const OP_TEXT: u8 = 0x1;
pub(crate) const OP_BINARY: u8 = 0x2;
pub(crate) const OP_CLOSE: u8 = 0x8;
pub(crate) const OP_PING: u8 = 0x9;
pub(crate) const OP_PONG: u8 = 0xA;

pub(crate) const CLOSE_PROTOCOL_ERROR: u16 = 1002;
pub(crate) const CLOSE_INVALID_DATA: u16 = 1007;
pub(crate) const CLOSE_TOO_BIG: u16 = 1009;

/// Largest frame header: 2 bytes, a 64-bit length and a mask.
pub(crate) const MAX_HEADER: usize = 14;
/// Largest control frame payload.
pub(crate) const MAX_CONTROL: usize = 125;

const GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

impl WsKind {
    pub(crate) fn opcode(self) -> u8 {
        match self {
            Self::Text => OP_TEXT,
            Self::Binary => OP_BINARY,
            Self::Ping => OP_PING,
            Self::Pong => OP_PONG,
        }
    }
}

/// A connection speaking WebSocket, or a client handshake waiting for its
/// `101`.
pub(crate) struct Ws {
    /// The `Sec-WebSocket-Accept` a client handshake waits for; `None` once
    /// open.
    pub(crate) expect: Option<[u8; 28]>,
    /// Kind of the data message being reassembled from fragments.
    pub(crate) fragmented: Option<WsKind>,
    pub(crate) message: Vec<u8>,
    /// A close frame went out: nothing more is sent or delivered.
    pub(crate) closing: bool,
    /// Client mask source.
    pub(crate) rng: u64,
}

impl Ws {
    pub(crate) fn open(seed: u64) -> Self {
        Self { expect: None, fragmented: None, message: Vec::new(), closing: false, rng: seed | 1 }
    }

    pub(crate) fn is_open(&self) -> bool {
        self.expect.is_none() && !self.closing
    }

    pub(crate) fn next_random(&mut self) -> u64 {
        // xorshift64*: masks need only be unpredictable to intermediaries, not
        // cryptographic.
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        self.rng.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

/// One frame's header, parsed from the front of a buffer.
pub(crate) struct Frame {
    pub(crate) fin: bool,
    pub(crate) opcode: u8,
    pub(crate) mask: Option<[u8; 4]>,
    pub(crate) header_len: usize,
    pub(crate) payload_len: usize,
}

/// `Ok(None)` until the whole header is buffered; `Err` for a header no peer
/// may send.
pub(crate) fn parse_header(buf: &[u8]) -> Result<Option<Frame>, ()> {
    let [b0, b1, ..] = *buf else { return Ok(None) };
    if b0 & 0x70 != 0 {
        return Err(())
    }
    let (fin, opcode) = (b0 & 0x80 != 0, b0 & 0x0F);
    let (len, mut at) = match b1 & 0x7F {
        126 => match buf.get(2..4) {
            Some(bytes) => (u64::from(u16::from_be_bytes([bytes[0], bytes[1]])), 4),
            None => return Ok(None),
        },
        127 => match buf.get(2..10) {
            Some(bytes) => (u64::from_be_bytes(bytes.try_into().unwrap()), 10),
            None => return Ok(None),
        },
        len => (u64::from(len), 2),
    };
    let control = opcode & 0x08 != 0;
    let known =
        matches!(opcode, OP_CONTINUATION | OP_TEXT | OP_BINARY | OP_CLOSE | OP_PING | OP_PONG);
    if !known || len >> 63 != 0 || (control && (!fin || len > MAX_CONTROL as u64)) {
        return Err(())
    }
    let mask = if b1 & 0x80 != 0 {
        let Some(bytes) = buf.get(at..at + 4) else { return Ok(None) };
        at += 4;
        Some(bytes.try_into().unwrap())
    } else {
        None
    };
    let payload_len = usize::try_from(len).map_err(|_| ())?;
    Ok(Some(Frame { fin, opcode, mask, header_len: at, payload_len }))
}

/// Writes a final frame's header into `out`; returns its length.
pub(crate) fn write_header(
    out: &mut [u8; MAX_HEADER],
    opcode: u8,
    len: usize,
    mask: Option<[u8; 4]>,
) -> usize {
    out[0] = 0x80 | opcode;
    let mask_bit = if mask.is_some() { 0x80 } else { 0 };
    let mut at = if len < 126 {
        out[1] = mask_bit | len as u8;
        2
    } else if let Ok(len) = u16::try_from(len) {
        out[1] = mask_bit | 126;
        out[2..4].copy_from_slice(&len.to_be_bytes());
        4
    } else {
        out[1] = mask_bit | 127;
        out[2..10].copy_from_slice(&(len as u64).to_be_bytes());
        10
    };
    if let Some(mask) = mask {
        out[at..at + 4].copy_from_slice(&mask);
        at += 4;
    }
    at
}

/// XORs `data` with the repeating `mask`, eight bytes at a time.
pub(crate) fn apply_mask(data: &mut [u8], mask: [u8; 4]) {
    let wide = u64::from_ne_bytes([
        mask[0], mask[1], mask[2], mask[3], mask[0], mask[1], mask[2], mask[3],
    ]);
    let mut chunks = data.chunks_exact_mut(8);
    for chunk in &mut chunks {
        let value = u64::from_ne_bytes(chunk.try_into().unwrap()) ^ wide;
        chunk.copy_from_slice(&value.to_ne_bytes());
    }
    for (i, byte) in chunks.into_remainder().iter_mut().enumerate() {
        *byte ^= mask[i % 4];
    }
}

/// `Sec-WebSocket-Accept` for a `Sec-WebSocket-Key`.
pub(crate) fn accept_key(key: &[u8]) -> [u8; 28] {
    let mut sha = Sha1::default();
    sha.update(key);
    sha.update(GUID);
    let mut out = [0; 28];
    base64(&sha.finish(), &mut out);
    out
}

/// A fresh `Sec-WebSocket-Key`: 16 random bytes, base64.
pub(crate) fn client_key(ws: &mut Ws) -> [u8; 24] {
    let mut nonce = [0; 16];
    nonce[..8].copy_from_slice(&ws.next_random().to_le_bytes());
    nonce[8..].copy_from_slice(&ws.next_random().to_le_bytes());
    let mut out = [0; 24];
    base64(&nonce, &mut out);
    out
}

/// Standard base64 with padding; `out` holds exactly `4 * ceil(input / 3)`
/// bytes.
fn base64(input: &[u8], out: &mut [u8]) {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for (chunk, slot) in input.chunks(3).zip(out.chunks_mut(4)) {
        let n = chunk.iter().enumerate().fold(0u32, |n, (i, b)| n | u32::from(*b) << (16 - 8 * i));
        for (i, byte) in slot.iter_mut().enumerate() {
            *byte = if i <= chunk.len() { TABLE[(n >> (18 - 6 * i)) as usize & 63] } else { b'=' };
        }
    }
}

/// SHA-1, only for the handshake digest.
struct Sha1 {
    state: [u32; 5],
    block: [u8; 64],
    filled: usize,
    total: u64,
}

impl Default for Sha1 {
    fn default() -> Self {
        Self {
            state: [0x6745_2301, 0xEFCD_AB89, 0x98BA_DCFE, 0x1032_5476, 0xC3D2_E1F0],
            block: [0; 64],
            filled: 0,
            total: 0,
        }
    }
}

impl Sha1 {
    fn update(&mut self, mut data: &[u8]) {
        self.total += data.len() as u64;
        while !data.is_empty() {
            let take = (64 - self.filled).min(data.len());
            self.block[self.filled..self.filled + take].copy_from_slice(&data[..take]);
            self.filled += take;
            data = &data[take..];
            if self.filled == 64 {
                self.compress();
                self.filled = 0;
            }
        }
    }

    fn finish(mut self) -> [u8; 20] {
        let bits = self.total * 8;
        self.update(&[0x80]);
        while self.filled != 56 {
            self.update(&[0]);
        }
        self.update(&bits.to_be_bytes());
        let mut out = [0; 20];
        for (word, slot) in self.state.iter().zip(out.chunks_mut(4)) {
            slot.copy_from_slice(&word.to_be_bytes());
        }
        out
    }

    // The names are the standard's.
    #[allow(clippy::many_single_char_names)]
    fn compress(&mut self) {
        let mut w = [0u32; 80];
        for (i, word) in self.block.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes(word.try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = self.state;
        for (i, word) in w.iter().enumerate() {
            let (f, k) = match i {
                0..20 => ((b & c) | (!b & d), 0x5A82_7999),
                20..40 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..60 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let t = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*word);
            (e, d, c, b, a) = (d, c, b.rotate_left(30), a, t);
        }
        for (state, value) in self.state.iter_mut().zip([a, b, c, d, e]) {
            *state = state.wrapping_add(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc_6455_accept_key() {
        assert_eq!(&accept_key(b"dGhlIHNhbXBsZSBub25jZQ=="), b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }
}
