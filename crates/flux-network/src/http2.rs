//! HTTP/2 frames and a synchronous server connection over caller-owned buffers.
//!
//! [`ServerConnection`] handles the HTTP/2 preface, HPACK, stream state, and
//! flow control. It performs no I/O. The low-level [`Decoder`] returns borrowed
//! frames.

mod connection;
#[allow(clippy::all, clippy::pedantic, clippy::nursery, dead_code)]
#[rustfmt::skip]
pub(crate) mod hpack;

use std::fmt;

pub use connection::{
    CLIENT_PREFACE, Event, Fields, Http2Config, RequestHead, SendError, ServerConnection,
};

pub const FRAME_HEADER_LEN: usize = 9;
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 16_384;
pub const MAX_FRAME_SIZE: u32 = 0x00ff_ffff;

pub mod kind {
    pub const DATA: u8 = 0;
    pub const HEADERS: u8 = 1;
    pub const PRIORITY: u8 = 2;
    pub const RST_STREAM: u8 = 3;
    pub const SETTINGS: u8 = 4;
    pub const PUSH_PROMISE: u8 = 5;
    pub const PING: u8 = 6;
    pub const GOAWAY: u8 = 7;
    pub const WINDOW_UPDATE: u8 = 8;
    pub const CONTINUATION: u8 = 9;
}

pub mod flags {
    pub const END_STREAM: u8 = 0x01;
    pub const ACK: u8 = 0x01;
    pub const END_HEADERS: u8 = 0x04;
    pub const PADDED: u8 = 0x08;
    pub const PRIORITY: u8 = 0x20;
}

/// All decode errors terminate the connection. Discriminants are GOAWAY codes.
/// Stream errors are escalated as permitted by RFC 9113 section 5.4.1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum Error {
    Protocol = 1,
    FlowControl = 3,
    StreamClosed = 5,
    FrameSize = 6,
    Compression = 9,
    LimitExceeded = 11,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Protocol => "invalid HTTP/2 frame or frame sequence",
            Self::FlowControl => "invalid HTTP/2 flow-control window",
            Self::StreamClosed => "HTTP/2 stream is closed",
            Self::Compression => "invalid HTTP/2 header compression",
            Self::FrameSize => "invalid HTTP/2 frame size",
            Self::LimitExceeded => "HTTP/2 resource limit exceeded",
        })
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Priority {
    pub dependency: u32,
    pub exclusive: bool,
    pub weight: u16,
}

/// Validated settings, in wire order. Unknown identifiers remain available.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Settings<'a>(&'a [u8]);

impl Settings<'_> {
    pub fn iter(&self) -> impl Iterator<Item = (u16, u32)> + '_ {
        self.0.chunks_exact(6).map(|s| (u16::from_be_bytes([s[0], s[1]]), read_u32(&s[2..])))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frame<'a> {
    pub stream_id: u32,
    pub payload: Payload<'a>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Payload<'a> {
    Data {
        data: &'a [u8],
        end_stream: bool,
        /// Includes the pad-length byte and padding, as required for flow
        /// control.
        flow_controlled_len: u32,
    },
    Headers {
        fragment: &'a [u8],
        end_headers: bool,
        end_stream: bool,
        priority: Option<Priority>,
    },
    Priority(Priority),
    Reset {
        error_code: u32,
    },
    Settings {
        ack: bool,
        settings: Settings<'a>,
    },
    PushPromise {
        promised_stream_id: u32,
        fragment: &'a [u8],
        end_headers: bool,
    },
    Ping {
        ack: bool,
        opaque: [u8; 8],
    },
    GoAway {
        last_stream_id: u32,
        error_code: u32,
        debug_data: &'a [u8],
    },
    WindowUpdate {
        increment: u32,
    },
    Continuation {
        fragment: &'a [u8],
        end_headers: bool,
    },
    Unknown {
        kind: u8,
        flags: u8,
        data: &'a [u8],
    },
}

/// One receive direction. Stores only limits and unfinished header-block state.
pub struct Decoder {
    max_frame_size: u32,
    max_header_block_size: usize,
    continuation: Option<(u32, usize)>,
}

impl Decoder {
    /// `max_frame_size` must be in 16384..=16777215. The header limit bounds
    /// compressed bytes across `HEADERS`/`PUSH_PROMISE` and all CONTINUATION
    /// frames. The HPACK layer must separately limit decoded headers.
    pub fn new(max_frame_size: u32, max_header_block_size: usize) -> Result<Self, Error> {
        check_max_frame_size(max_frame_size)?;
        Ok(Self { max_frame_size, max_header_block_size, continuation: None })
    }

    /// Returns one borrowed frame and its wire length, or `None` for incomplete
    /// input. Incomplete input does not change state. Errors require
    /// closing the connection. Invalid lengths and continuation order are
    /// rejected from the nine-byte header.
    pub fn decode<'a>(&mut self, input: &'a [u8]) -> Result<Option<(Frame<'a>, usize)>, Error> {
        if input.len() < FRAME_HEADER_LEN {
            return Ok(None);
        }
        let length = u32::from_be_bytes([0, input[0], input[1], input[2]]);
        let kind = input[3];
        let flags = input[4];
        let stream_id = read_u32(&input[5..]) & 0x7fff_ffff;
        validate_header(kind, flags, stream_id, length, self.max_frame_size)?;
        match self.continuation {
            Some((id, _)) if kind != kind::CONTINUATION || stream_id != id => {
                return Err(Error::Protocol);
            }
            None if kind == kind::CONTINUATION => return Err(Error::Protocol),
            _ => {}
        }
        let consumed = FRAME_HEADER_LEN + length as usize;
        if input.len() < consumed {
            return Ok(None);
        }
        let payload = parse_payload(kind, flags, stream_id, &input[FRAME_HEADER_LEN..consumed])?;
        match payload {
            Payload::Headers { fragment, end_headers, .. } |
            Payload::PushPromise { fragment, end_headers, .. } |
            Payload::Continuation { fragment, end_headers } => {
                let previous = self.continuation.map_or(0, |(_, bytes)| bytes);
                let total = previous.checked_add(fragment.len()).ok_or(Error::LimitExceeded)?;
                if total > self.max_header_block_size {
                    return Err(Error::LimitExceeded);
                }
                self.continuation = if end_headers { None } else { Some((stream_id, total)) };
            }
            _ => {}
        }
        Ok(Some((Frame { stream_id, payload }, consumed)))
    }
}

/// Appends one frame after validating its shape and the peer's maximum frame
/// size.
///
/// The caller supplies wire-format payload bytes and owns outbound
/// stream/order checks. Errors leave `output` unchanged. This function does not
/// split large payloads.
pub fn encode_frame(
    kind: u8,
    flags: u8,
    stream_id: u32,
    payload: &[u8],
    max_frame_size: u32,
    output: &mut Vec<u8>,
) -> Result<(), Error> {
    check_max_frame_size(max_frame_size)?;
    if stream_id > 0x7fff_ffff {
        return Err(Error::Protocol);
    }
    let length = u32::try_from(payload.len()).map_err(|_| Error::FrameSize)?;
    validate_header(kind, flags, stream_id, length, max_frame_size)?;
    parse_payload(kind, flags, stream_id, payload)?;
    output.reserve(FRAME_HEADER_LEN + payload.len());
    output.extend_from_slice(&length.to_be_bytes()[1..]);
    output.extend_from_slice(&[kind, flags]);
    output.extend_from_slice(&stream_id.to_be_bytes());
    output.extend_from_slice(payload);
    Ok(())
}

fn check_max_frame_size(size: u32) -> Result<(), Error> {
    if (DEFAULT_MAX_FRAME_SIZE..=MAX_FRAME_SIZE).contains(&size) {
        Ok(())
    } else {
        Err(Error::Protocol)
    }
}

fn validate_header(kind: u8, flags: u8, id: u32, len: u32, max: u32) -> Result<(), Error> {
    if len > max {
        return Err(Error::FrameSize);
    }
    match kind {
        kind::DATA |
        kind::HEADERS |
        kind::PRIORITY |
        kind::RST_STREAM |
        kind::PUSH_PROMISE |
        kind::CONTINUATION
            if id == 0 =>
        {
            return Err(Error::Protocol);
        }
        kind::SETTINGS | kind::PING | kind::GOAWAY if id != 0 => return Err(Error::Protocol),
        _ => {}
    }
    let padded = u32::from(flags & flags::PADDED != 0);
    let valid_size = match kind {
        kind::DATA => len >= padded,
        kind::HEADERS => len >= padded + if flags & flags::PRIORITY != 0 { 5 } else { 0 },
        kind::PRIORITY => len == 5,
        kind::RST_STREAM | kind::WINDOW_UPDATE => len == 4,
        kind::SETTINGS => len.is_multiple_of(6) && (flags & flags::ACK == 0 || len == 0),
        kind::PUSH_PROMISE => len >= padded + 4,
        kind::PING => len == 8,
        kind::GOAWAY => len >= 8,
        _ => true,
    };
    if valid_size { Ok(()) } else { Err(Error::FrameSize) }
}

// Header validation precedes payload parsing, so fixed fields are present here.
fn parse_payload(kind: u8, flags: u8, id: u32, data: &[u8]) -> Result<Payload<'_>, Error> {
    let end_stream = flags & flags::END_STREAM != 0;
    let end_headers = flags & flags::END_HEADERS != 0;
    let ack = flags & flags::ACK != 0;
    Ok(match kind {
        kind::DATA => Payload::Data {
            data: unpad(data, flags)?,
            end_stream,
            flow_controlled_len: data.len() as u32,
        },
        kind::HEADERS => {
            let mut fragment = unpad(data, flags)?;
            let priority = if flags & flags::PRIORITY != 0 {
                if fragment.len() < 5 {
                    return Err(Error::FrameSize);
                }
                let priority = parse_priority(fragment, id)?;
                fragment = &fragment[5..];
                Some(priority)
            } else {
                None
            };
            Payload::Headers { fragment, end_headers, end_stream, priority }
        }
        kind::PRIORITY => Payload::Priority(parse_priority(data, id)?),
        kind::RST_STREAM => Payload::Reset { error_code: read_u32(data) },
        kind::SETTINGS => {
            let settings = Settings(data);
            for (setting, value) in settings.iter() {
                match setting {
                    2 if value > 1 => return Err(Error::Protocol),
                    4 if value > 0x7fff_ffff => return Err(Error::FlowControl),
                    5 => check_max_frame_size(value)?,
                    _ => {}
                }
            }
            Payload::Settings { ack, settings }
        }
        kind::PUSH_PROMISE => {
            let data = unpad(data, flags)?;
            if data.len() < 4 {
                return Err(Error::FrameSize);
            }
            let promised_stream_id = read_u32(data) & 0x7fff_ffff;
            if promised_stream_id == 0 || !promised_stream_id.is_multiple_of(2) {
                return Err(Error::Protocol);
            }
            Payload::PushPromise { promised_stream_id, fragment: &data[4..], end_headers }
        }
        kind::PING => Payload::Ping { ack, opaque: data.try_into().unwrap() },
        kind::GOAWAY => Payload::GoAway {
            last_stream_id: read_u32(data) & 0x7fff_ffff,
            error_code: read_u32(&data[4..]),
            debug_data: &data[8..],
        },
        kind::WINDOW_UPDATE => {
            let increment = read_u32(data) & 0x7fff_ffff;
            if increment == 0 {
                return Err(Error::Protocol);
            }
            Payload::WindowUpdate { increment }
        }
        kind::CONTINUATION => Payload::Continuation { fragment: data, end_headers },
        _ => Payload::Unknown { kind, flags, data },
    })
}

fn unpad(data: &[u8], flags: u8) -> Result<&[u8], Error> {
    if flags & flags::PADDED == 0 {
        return Ok(data);
    }
    let (&padding, rest) = data.split_first().ok_or(Error::FrameSize)?;
    let end = rest.len().checked_sub(usize::from(padding)).ok_or(Error::Protocol)?;
    Ok(&rest[..end])
}

fn parse_priority(data: &[u8], id: u32) -> Result<Priority, Error> {
    let dependency = read_u32(data) & 0x7fff_ffff;
    if dependency == id {
        return Err(Error::Protocol);
    }
    Ok(Priority { dependency, exclusive: data[0] & 0x80 != 0, weight: u16::from(data[4]) + 1 })
}

fn read_u32(data: &[u8]) -> u32 {
    u32::from_be_bytes(data[..4].try_into().unwrap())
}
