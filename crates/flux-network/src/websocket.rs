//! WebSocket (RFC 6455) on [`HttpNetwork`](crate::http::HttpNetwork)
//! connections. No extension is negotiated.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use mio::Token;
use sha1::{Digest as _, Sha1};

use crate::{NetworkCore, http::HttpEvent};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WsKind {
    Text,
    Binary,
    Ping,
    Pong,
}

const OP_CONTINUATION: u8 = 0x0;
const OP_TEXT: u8 = 0x1;
const OP_BINARY: u8 = 0x2;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xA;

const CLOSE_PROTOCOL_ERROR: u16 = 1002;
const CLOSE_INVALID_DATA: u16 = 1007;
const CLOSE_TOO_BIG: u16 = 1009;

/// Two bytes, a 64-bit length and a mask.
const MAX_HEADER: usize = 14;
const MAX_CONTROL: usize = 125;
const KEEP_BUF_BYTES: usize = 1024 * 1024;

impl WsKind {
    fn opcode(self) -> u8 {
        match self {
            Self::Text => OP_TEXT,
            Self::Binary => OP_BINARY,
            Self::Ping => OP_PING,
            Self::Pong => OP_PONG,
        }
    }
}

/// An upgraded connection, or a client handshake awaiting its `101`.
pub(crate) struct Ws {
    /// The `Sec-WebSocket-Accept` a client handshake waits for.
    pub(crate) expect: Option<[u8; 28]>,
    pub(crate) max: usize,
    /// Clients mask what they send; servers expect masked frames.
    client: bool,
    /// The data message being reassembled from fragments.
    fragmented: Option<WsKind>,
    message: Vec<u8>,
    /// A close frame went out: nothing more is sent or delivered.
    closing: bool,
}

impl Ws {
    pub(crate) fn server(max: usize) -> Self {
        Self::new(false, max)
    }

    /// A client handshake and the `Sec-WebSocket-Key` to send with it.
    pub(crate) fn client(max: usize) -> (Self, [u8; 24]) {
        let mut ws = Self::new(true, max);
        let mut key = [0; 24];
        let _ = STANDARD.encode_slice(rand::random::<[u8; 16]>(), &mut key);
        ws.expect = Some(accept_key(&key));
        (ws, key)
    }

    fn new(client: bool, max: usize) -> Self {
        Self { expect: None, max, client, fragmented: None, message: Vec::new(), closing: false }
    }

    pub(crate) fn is_open(&self) -> bool {
        self.expect.is_none() && !self.closing
    }

    pub(crate) fn send(
        &self,
        net: &mut NetworkCore,
        token: Token,
        kind: WsKind,
        payload: &[u8],
    ) -> bool {
        if matches!(kind, WsKind::Ping | WsKind::Pong) && payload.len() > MAX_CONTROL {
            return false
        }
        self.is_open() && self.send_frame(net, token, kind.opcode(), payload)
    }

    /// Sends a close frame and drops the connection once it is written;
    /// `false` if it was already closing.
    pub(crate) fn close(&mut self, net: &mut NetworkCore, token: Token, code: u16) -> bool {
        self.close_with(net, token, &code.to_be_bytes())
    }

    fn close_with(&mut self, net: &mut NetworkCore, token: Token, payload: &[u8]) -> bool {
        if self.closing {
            return false
        }
        self.send_frame(net, token, OP_CLOSE, payload);
        self.closing = true;
        net.disconnect_when_drained(token);
        true
    }

    fn send_frame(&self, net: &mut NetworkCore, token: Token, opcode: u8, payload: &[u8]) -> bool {
        let mut head = [0; MAX_HEADER];
        if !self.client {
            let len = write_header(&mut head, opcode, payload.len(), None);
            return net.send_parts(token, &head[..len], payload)
        }
        // RFC 6455 §5.3: masks come from a strong source of entropy.
        let mask = rand::random();
        let len = write_header(&mut head, opcode, payload.len(), Some(mask));
        net.send_with(token, |out| {
            out.extend_from_slice(&head[..len]);
            let start = out.len();
            out.extend_from_slice(payload);
            apply_mask(&mut out.as_mut_slice()[start..], mask);
        })
    }

    /// Delivers and drains the messages in `buf`. Pings get one pong per drive,
    /// with the last payload.
    pub(crate) fn parse<F>(
        &mut self,
        net: &mut NetworkCore,
        token: Token,
        buf: &mut Vec<u8>,
        handler: &mut F,
    ) where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        let mut at = 0;
        let (mut pong, mut pong_len) = ([0; MAX_CONTROL], None);
        while !self.closing {
            let frame = match parse_header(&buf[at..]) {
                Ok(Some(frame)) if frame.mask.is_some() != self.client => frame,
                Ok(None) => break,
                _ => {
                    self.close(net, token, CLOSE_PROTOCOL_ERROR);
                    break
                }
            };
            let held = if frame.opcode == OP_CONTINUATION { self.message.len() } else { 0 };
            if frame.opcode < OP_CLOSE && frame.len > self.max.saturating_sub(held) {
                self.close(net, token, CLOSE_TOO_BIG);
                break
            }
            let start = at + frame.header_len;
            let Some(end) = start.checked_add(frame.len) else {
                self.close(net, token, CLOSE_TOO_BIG);
                break
            };
            if buf.len() < end {
                break
            }
            at = end;
            let payload = &mut buf[start..end];
            if let Some(mask) = frame.mask {
                apply_mask(payload, mask);
            }
            let kind = match frame.opcode {
                OP_PING => {
                    pong[..payload.len()].copy_from_slice(payload);
                    pong_len = Some(payload.len());
                    handler(HttpEvent::WsMessage { token, kind: WsKind::Ping, payload });
                    continue
                }
                OP_PONG => {
                    handler(HttpEvent::WsMessage { token, kind: WsKind::Pong, payload });
                    continue
                }
                OP_CLOSE if payload.len() == 1 => {
                    self.close(net, token, CLOSE_PROTOCOL_ERROR);
                    break
                }
                OP_CLOSE => {
                    self.close_with(net, token, payload.get(..2).unwrap_or_default());
                    handler(HttpEvent::WsClose { token, payload });
                    break
                }
                OP_TEXT => Some(WsKind::Text),
                OP_BINARY => Some(WsKind::Binary),
                _ => None,
            };
            let kind = match (kind, self.fragmented) {
                // Reject orphan continuations and interleaved data messages.
                (Some(_), Some(_)) | (None, None) => {
                    self.close(net, token, CLOSE_PROTOCOL_ERROR);
                    break
                }
                (Some(kind), None) if frame.fin => {
                    if kind == WsKind::Text && std::str::from_utf8(payload).is_err() {
                        self.close(net, token, CLOSE_INVALID_DATA);
                        break
                    }
                    handler(HttpEvent::WsMessage { token, kind, payload });
                    continue
                }
                (Some(kind), None) => {
                    self.fragmented = Some(kind);
                    self.message.clear();
                    self.message.extend_from_slice(payload);
                    continue
                }
                (None, Some(kind)) => {
                    self.message.extend_from_slice(payload);
                    if !frame.fin {
                        continue
                    }
                    self.fragmented = None;
                    kind
                }
            };
            if kind == WsKind::Text && std::str::from_utf8(&self.message).is_err() {
                self.close(net, token, CLOSE_INVALID_DATA);
                break
            }
            handler(HttpEvent::WsMessage { token, kind, payload: &self.message });
        }
        if self.closing {
            buf.clear();
        } else {
            if let Some(len) = pong_len {
                self.send_frame(net, token, OP_PONG, &pong[..len]);
            }
            buf.drain(..at);
        }
        if self.fragmented.is_none() {
            let held = self.message.len();
            self.message.clear();
            release(&mut self.message, held);
        }
    }
}

/// Frees oversized empty buffers after a small drive to avoid threshold churn.
pub(crate) fn release(buf: &mut Vec<u8>, held: usize) {
    if buf.is_empty() && held <= KEEP_BUF_BYTES / 2 && buf.capacity() > KEEP_BUF_BYTES {
        *buf = Vec::new();
    }
}

struct Frame {
    fin: bool,
    opcode: u8,
    mask: Option<[u8; 4]>,
    header_len: usize,
    len: usize,
}

/// `Ok(None)` until the header is buffered; `Err` for one no peer may send.
fn parse_header(buf: &[u8]) -> Result<Option<Frame>, ()> {
    let [b0, b1, ..] = *buf else { return Ok(None) };
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
    if b0 & 0x70 != 0 || !known || len >> 63 != 0 || (control && (!fin || len > MAX_CONTROL as u64))
    {
        return Err(())
    }
    let mask = if b1 & 0x80 != 0 {
        let Some(bytes) = buf.get(at..at + 4) else { return Ok(None) };
        at += 4;
        Some(bytes.try_into().unwrap())
    } else {
        None
    };
    let len = usize::try_from(len).map_err(|_| ())?;
    Ok(Some(Frame { fin, opcode, mask, header_len: at, len }))
}

/// A final frame's header; returns its length.
fn write_header(
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

fn apply_mask(data: &mut [u8], mask: [u8; 4]) {
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
    let digest = Sha1::new()
        .chain_update(key)
        .chain_update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
        .finalize();
    let mut out = [0; 28];
    let _ = STANDARD.encode_slice(digest, &mut out);
    out
}
