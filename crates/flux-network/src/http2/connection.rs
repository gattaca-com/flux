//! Server-side HTTP/2 over caller-owned transport buffers.

use std::{io::Cursor, ops::ControlFlow};

use bytes::BytesMut;
use flux_timing::IngestionTime;
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use rustc_hash::FxHashMap;

use super::{
    DEFAULT_MAX_FRAME_SIZE, Decoder, Error, Frame, Payload, encode_frame, flags, hpack, kind,
};

pub const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const INITIAL_WINDOW: i64 = 65_535;
const MAX_WINDOW: i64 = 0x7fff_ffff;
const MAX_HEADER_FIELDS: usize = 256;

#[derive(Clone, Copy, Debug)]
pub struct Http2Config {
    pub max_concurrent_streams: u32,
    /// Decoded bytes including 32 bytes per field. At most 256 fields are
    /// accepted.
    pub max_header_list_size: u32,
    pub max_header_block_size: usize,
    pub max_send_buffer: usize,
    /// Receive window advertised for the connection and each stream; how much
    /// a client may send before waiting for credit.
    pub recv_window: u32,
}

impl Default for Http2Config {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 128,
            max_header_list_size: 16 * 1024,
            max_header_block_size: 64 * 1024,
            max_send_buffer: 256 * 1024,
            recv_window: 1 << 20,
        }
    }
}

/// A validated request head, borrowed from the connection's decode buffer for
/// the duration of the event.
#[derive(Clone, Copy, Debug)]
pub struct RequestHead<'a> {
    pub method: &'a Method,
    pub scheme: Option<&'a [u8]>,
    pub authority: Option<&'a [u8]>,
    pub path: Option<&'a [u8]>,
    pub fields: Fields<'a>,
}

/// Regular header fields of one block in wire order, borrowed without copies.
/// Names are lowercase.
#[derive(Clone, Copy, Debug)]
pub struct Fields<'a>(&'a [hpack::Header]);

impl<'a> Fields<'a> {
    pub fn iter(&self) -> impl Iterator<Item = (&'a str, &'a [u8])> + 'a {
        self.0.iter().filter_map(|header| match header {
            hpack::Header::Field { name, value } => Some((name.as_str(), value.as_bytes())),
            _ => None,
        })
    }

    /// The first value of `name`, which must be lowercase.
    pub fn get(&self, name: &str) -> Option<&'a [u8]> {
        self.get_all(name).next()
    }

    pub fn get_all<'n>(&self, name: &'n str) -> impl Iterator<Item = &'a [u8]> + 'n
    where
        'a: 'n,
    {
        self.iter().filter(move |(n, _)| *n == name).map(|(_, value)| value)
    }
}

#[derive(Debug)]
pub enum Event<'a> {
    Request {
        stream_id: u32,
        head: RequestHead<'a>,
        end_stream: bool,
        /// When the input holding the first byte of the request's HEADERS
        /// frame was received.
        received_at: IngestionTime,
    },
    Data {
        stream_id: u32,
        data: &'a [u8],
        end_stream: bool,
    },
    Trailers {
        stream_id: u32,
        fields: Fields<'a>,
    },
    Reset {
        stream_id: u32,
        error_code: u32,
    },
    /// Capacity might now be available. A stream identifier of zero applies to
    /// all streams.
    Writable {
        stream_id: u32,
    },
    GoAway {
        last_stream_id: u32,
        error_code: u32,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendError {
    Closed,
    InvalidHeaders,
    InvalidState,
    TooLarge,
    WouldBlock,
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HTTP/2 send failed: {self:?}")
    }
}

impl std::error::Error for SendError {}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SendState {
    AwaitingHeaders,
    Body,
    Closed,
}

struct Stream {
    recv_open: bool,
    send_state: SendState,
    recv_window: i64,
    send_window: i64,
    pending_recv: u32,
    request_length: Option<u64>,
    received: u64,
    response_length: Option<u64>,
    sent: u64,
    method: Method,
    body_forbidden: bool,
}

struct HeaderBlock {
    stream_id: u32,
    end_stream: bool,
    bytes: BytesMut,
    /// When the input holding its HEADERS frame was received.
    received_at: IngestionTime,
}

/// An HTTP/2 server connection without sockets, tasks, or an executor.
///
/// Write `output()` to the transport and call `consume_output()` after
/// successful writes. Retain unconsumed input from `receive()`. Release DATA
/// credit only when the application has consumed it. The caller owns transport
/// and timeout policy. HTTP/1 upgrades, server push, and extended CONNECT are
/// not enabled.
pub struct ServerConnection {
    config: Http2Config,
    decoder: Decoder,
    hpack_decoder: hpack::Decoder,
    hpack_encoder: hpack::Encoder,
    headers: Option<HeaderBlock>,
    /// Header block storage, reused once decoded fields release it.
    block_buffer: BytesMut,
    decoded_headers: Vec<hpack::Header>,
    streams: FxHashMap<u32, Stream>,
    preface_received: usize,
    peer_settings: bool,
    settings_ack_pending: bool,
    last_stream_id: u32,
    peer_frame_size: u32,
    peer_header_limit: u32,
    peer_initial_window: i64,
    send_window: i64,
    recv_window: i64,
    /// Connection credit consumed but not yet announced.
    unreturned: u32,
    output: Vec<u8>,
    output_offset: usize,
    goaway_last: Option<u32>,
    failed: bool,
}

impl ServerConnection {
    pub fn new(config: Http2Config) -> Result<Self, Error> {
        if config.max_send_buffer < 64 ||
            config.max_header_list_size == 0 ||
            !(INITIAL_WINDOW..=MAX_WINDOW).contains(&i64::from(config.recv_window))
        {
            return Err(Error::Protocol);
        }
        let mut connection = Self {
            config,
            decoder: Decoder::new(DEFAULT_MAX_FRAME_SIZE, config.max_header_block_size)?,
            hpack_decoder: hpack::Decoder::new(4096),
            hpack_encoder: hpack::Encoder::new(4096, 0),
            headers: None,
            block_buffer: BytesMut::new(),
            decoded_headers: Vec::new(),
            streams: FxHashMap::default(),
            preface_received: 0,
            peer_settings: false,
            settings_ack_pending: true,
            last_stream_id: 0,
            peer_frame_size: DEFAULT_MAX_FRAME_SIZE,
            peer_header_limit: u32::MAX,
            peer_initial_window: INITIAL_WINDOW,
            send_window: INITIAL_WINDOW,
            recv_window: i64::from(config.recv_window),
            unreturned: 0,
            output: Vec::new(),
            output_offset: 0,
            goaway_last: None,
            failed: false,
        };
        let mut settings = Vec::with_capacity(18);
        for (id, value) in [
            (3u16, config.max_concurrent_streams),
            (4, config.recv_window),
            (6, config.max_header_list_size),
        ] {
            settings.extend_from_slice(&id.to_be_bytes());
            settings.extend_from_slice(&value.to_be_bytes());
        }
        connection.queue(kind::SETTINGS, 0, 0, &settings)?;
        // The connection window starts at the protocol default; raise it.
        let raise = config.recv_window - INITIAL_WINDOW as u32;
        if raise != 0 {
            connection.queue(kind::WINDOW_UPDATE, 0, 0, &raise.to_be_bytes())?;
        }
        Ok(connection)
    }

    pub fn output(&self) -> &[u8] {
        &self.output[self.output_offset..]
    }

    /// Advance by bytes accepted by the transport. Panics if `bytes` exceeds
    /// output.
    pub fn consume_output(&mut self, bytes: usize) {
        assert!(bytes <= self.output().len());
        self.output_offset += bytes;
        if self.output_offset == self.output.len() {
            self.output.clear();
            self.output_offset = 0;
        }
    }

    pub fn is_draining(&self) -> bool {
        self.goaway_last.is_some() || self.failed
    }

    /// Process complete frames and return the number of input bytes consumed.
    /// `received_at` is when the first byte of `input` arrived; requests
    /// report it for their HEADERS frame.
    ///
    /// DATA borrows input for the callback. `release_capacity` counts DATA
    /// bytes, not padding. Control output is bounded; exhaustion terminates
    /// the connection. Errors queue GOAWAY when room remains. Flush output,
    /// then close the transport.
    pub fn receive(
        &mut self,
        input: &[u8],
        received_at: IngestionTime,
        mut handler: impl FnMut(Event<'_>),
    ) -> Result<usize, Error> {
        if self.failed {
            return Err(Error::Protocol);
        }
        let result = self.receive_inner(input, received_at, &mut handler);
        if let Err(error) = result {
            self.failed = true;
            let mut payload = [0; 8];
            payload[..4]
                .copy_from_slice(&self.goaway_last.unwrap_or(self.last_stream_id).to_be_bytes());
            payload[4..].copy_from_slice(&(error as u32).to_be_bytes());
            let _ = self.queue(kind::GOAWAY, 0, 0, &payload);
        }
        result
    }

    fn receive_inner(
        &mut self,
        input: &[u8],
        received_at: IngestionTime,
        handler: &mut impl FnMut(Event<'_>),
    ) -> Result<usize, Error> {
        let prefix = (CLIENT_PREFACE.len() - self.preface_received).min(input.len());
        if input[..prefix] != CLIENT_PREFACE[self.preface_received..self.preface_received + prefix]
        {
            return Err(Error::Protocol);
        }
        self.preface_received += prefix;
        let mut consumed = prefix;
        if self.preface_received != CLIENT_PREFACE.len() {
            return Ok(consumed);
        }
        while let Some((frame, length)) = self.decoder.decode(&input[consumed..])? {
            if !self.peer_settings {
                if !matches!(frame.payload, Payload::Settings { ack: false, .. }) {
                    return Err(Error::Protocol);
                }
                self.peer_settings = true;
            }
            self.on_frame(frame, received_at, handler)?;
            consumed += length;
        }
        Ok(consumed)
    }

    fn on_frame(
        &mut self,
        frame: Frame<'_>,
        received_at: IngestionTime,
        handler: &mut impl FnMut(Event<'_>),
    ) -> Result<(), Error> {
        let id = frame.stream_id;
        match frame.payload {
            Payload::Settings { ack: true, .. } => {
                if !self.settings_ack_pending {
                    return Err(Error::Protocol);
                }
                self.settings_ack_pending = false;
            }
            Payload::Settings { ack: false, settings } => {
                for (setting, value) in settings.iter() {
                    match setting {
                        1 => self.hpack_encoder.update_max_size(value as usize),
                        4 => {
                            let delta = i64::from(value) - self.peer_initial_window;
                            for stream in self.streams.values_mut() {
                                stream.send_window += delta;
                                if stream.send_window > MAX_WINDOW {
                                    return Err(Error::FlowControl);
                                }
                            }
                            self.peer_initial_window = i64::from(value);
                        }
                        5 => self.peer_frame_size = value,
                        6 => self.peer_header_limit = value,
                        _ => {}
                    }
                }
                self.queue(kind::SETTINGS, flags::ACK, 0, &[])?;
                handler(Event::Writable { stream_id: 0 });
            }
            Payload::Headers { fragment, end_headers, end_stream, .. } => {
                if id.is_multiple_of(2) {
                    return Err(Error::Protocol);
                }
                let mut bytes = std::mem::take(&mut self.block_buffer);
                bytes.clear();
                bytes.extend_from_slice(fragment);
                self.headers = Some(HeaderBlock { stream_id: id, end_stream, bytes, received_at });
                if end_headers {
                    self.finish_headers(handler)?;
                }
            }
            Payload::Continuation { fragment, end_headers } => {
                self.headers.as_mut().ok_or(Error::Protocol)?.bytes.extend_from_slice(fragment);
                if end_headers {
                    self.finish_headers(handler)?;
                }
            }
            Payload::Data { data, end_stream, flow_controlled_len } => {
                self.receive_data(id, data, end_stream, flow_controlled_len, handler)?;
            }
            Payload::WindowUpdate { increment } => {
                let window = if id == 0 {
                    &mut self.send_window
                } else if let Some(stream) = self.streams.get_mut(&id) {
                    &mut stream.send_window
                } else if id > self.last_stream_id || id.is_multiple_of(2) {
                    return Err(Error::Protocol);
                } else {
                    return Ok(());
                };
                *window += i64::from(increment);
                if *window > MAX_WINDOW {
                    return Err(Error::FlowControl);
                }
                handler(Event::Writable { stream_id: id });
            }
            Payload::Reset { error_code } => {
                if id > self.last_stream_id || id.is_multiple_of(2) {
                    return Err(Error::Protocol);
                }
                if self.remove_stream(id)? {
                    handler(Event::Reset { stream_id: id, error_code });
                }
            }
            Payload::Ping { ack: false, opaque } => {
                self.queue(kind::PING, flags::ACK, 0, &opaque)?;
            }
            Payload::GoAway { last_stream_id, error_code, .. } => {
                self.go_away().map_err(|_| Error::LimitExceeded)?;
                handler(Event::GoAway { last_stream_id, error_code });
            }
            Payload::PushPromise { .. } => return Err(Error::Protocol),
            Payload::Priority(_) | Payload::Ping { ack: true, .. } | Payload::Unknown { .. } => {}
        }
        Ok(())
    }

    fn finish_headers(&mut self, handler: &mut impl FnMut(Event<'_>)) -> Result<(), Error> {
        let mut block = self.headers.take().ok_or(Error::Protocol)?;
        self.decoded_headers.clear();
        let headers = &mut self.decoded_headers;
        let mut size = 0;
        let limit = self.config.max_header_list_size as usize;
        let mut oversized = false;
        self.hpack_decoder
            .decode(&mut Cursor::new(&mut block.bytes), |header| {
                size += header.len();
                if size > limit || headers.len() == MAX_HEADER_FIELDS {
                    oversized = true;
                    return ControlFlow::Break(());
                }
                headers.push(header);
                ControlFlow::Continue(())
            })
            .map_err(|_| Error::Compression)?;
        self.block_buffer = block.bytes;
        if oversized {
            return Err(Error::LimitExceeded);
        }
        let result =
            self.dispatch_headers(block.stream_id, block.end_stream, block.received_at, handler);
        // Releases the block's bytes for reuse by the next header block.
        self.decoded_headers.clear();
        result
    }

    fn dispatch_headers(
        &mut self,
        id: u32,
        end_stream: bool,
        received_at: IngestionTime,
        handler: &mut impl FnMut(Event<'_>),
    ) -> Result<(), Error> {
        let headers = &self.decoded_headers;
        if let Some(stream) = self.streams.get_mut(&id) {
            if !stream.recv_open || !end_stream {
                return Err(Error::Protocol);
            }
            let fields = regular_headers(headers)?;
            if fields.get(header::CONTENT_LENGTH.as_str()).is_some() {
                return Err(Error::Protocol);
            }
            if stream.request_length.is_some_and(|length| length != stream.received) {
                return Err(Error::Protocol);
            }
            stream.recv_open = false;
            handler(Event::Trailers { stream_id: id, fields });
            self.retire(id);
        } else if id <= self.last_stream_id {
            // Closed streams still update HPACK state before their headers are discarded.
            self.queue(kind::RST_STREAM, 0, id, &5u32.to_be_bytes())?;
        } else {
            self.last_stream_id = id;
            if self.goaway_last.is_some() ||
                self.streams.len() >= self.config.max_concurrent_streams as usize
            {
                self.queue(kind::RST_STREAM, 0, id, &7u32.to_be_bytes())?;
                return Ok(());
            }
            let head = request_head(headers)?;
            let request_length =
                content_length(head.fields.get_all(header::CONTENT_LENGTH.as_str()))?;
            if end_stream && request_length.is_some_and(|len| len != 0) {
                return Err(Error::Protocol);
            }
            self.streams.insert(id, Stream {
                recv_open: !end_stream,
                send_state: SendState::AwaitingHeaders,
                recv_window: i64::from(self.config.recv_window),
                send_window: self.peer_initial_window,
                pending_recv: 0,
                request_length,
                received: 0,
                response_length: None,
                sent: 0,
                method: head.method.clone(),
                body_forbidden: false,
            });
            handler(Event::Request { stream_id: id, head, end_stream, received_at });
        }
        Ok(())
    }

    fn receive_data(
        &mut self,
        id: u32,
        data: &[u8],
        end_stream: bool,
        flow_len: u32,
        handler: &mut impl FnMut(Event<'_>),
    ) -> Result<(), Error> {
        self.recv_window -= i64::from(flow_len);
        if self.recv_window < 0 {
            return Err(Error::FlowControl);
        }
        let Some(stream) = self.streams.get_mut(&id) else {
            if id > self.last_stream_id || id.is_multiple_of(2) {
                return Err(Error::Protocol);
            }
            self.return_connection_credit(flow_len)?;
            return self.queue(kind::RST_STREAM, 0, id, &5u32.to_be_bytes());
        };
        if !stream.recv_open {
            return Err(Error::StreamClosed);
        }
        stream.recv_window -= i64::from(flow_len);
        if stream.recv_window < 0 {
            return Err(Error::FlowControl);
        }
        stream.received = stream.received.checked_add(data.len() as u64).ok_or(Error::Protocol)?;
        if stream
            .request_length
            .is_some_and(|len| stream.received > len || (end_stream && stream.received != len))
        {
            return Err(Error::Protocol);
        }
        stream.pending_recv += data.len() as u32;
        let padding = flow_len - data.len() as u32;
        stream.recv_window += i64::from(padding);
        stream.recv_open = !end_stream;
        if padding != 0 {
            self.queue(kind::WINDOW_UPDATE, 0, id, &padding.to_be_bytes())?;
            self.return_connection_credit(padding)?;
        }
        handler(Event::Data { stream_id: id, data, end_stream });
        self.retire(id);
        Ok(())
    }

    /// Return credit after consuming DATA. A reset discards all remaining
    /// credit.
    pub fn release_capacity(&mut self, id: u32, bytes: u32) -> Result<(), SendError> {
        if self.failed {
            return Err(SendError::Closed);
        }
        let stream = self.streams.get(&id).ok_or(SendError::Closed)?;
        if bytes > stream.pending_recv {
            return Err(SendError::InvalidState);
        }
        if bytes == 0 {
            return Ok(());
        }
        self.reserve_output(26)?;
        let stream = self.streams.get_mut(&id).unwrap();
        stream.pending_recv -= bytes;
        stream.recv_window += i64::from(bytes);
        // A peer that ended its side sends no more on this stream.
        if stream.recv_open {
            self.queue(kind::WINDOW_UPDATE, 0, id, &bytes.to_be_bytes())
                .map_err(|_| SendError::WouldBlock)?;
        }
        self.return_connection_credit(bytes).map_err(|_| SendError::WouldBlock)?;
        self.retire(id);
        Ok(())
    }

    pub fn send_headers(
        &mut self,
        id: u32,
        status: StatusCode,
        fields: &HeaderMap,
        end_stream: bool,
    ) -> Result<(), SendError> {
        let stream = self.send_stream(id)?;
        if stream.send_state != SendState::AwaitingHeaders ||
            status == StatusCode::SWITCHING_PROTOCOLS ||
            (status.is_informational() && end_stream)
        {
            return Err(SendError::InvalidState);
        }
        validate_fields(map_fields(fields)).map_err(|_| SendError::InvalidHeaders)?;
        let mut length = content_length(
            fields.get_all(header::CONTENT_LENGTH).iter().map(HeaderValue::as_bytes),
        )
        .map_err(|_| SendError::InvalidHeaders)?;
        let body_forbidden = (stream.method == Method::HEAD) ||
            status == StatusCode::NO_CONTENT ||
            status == StatusCode::NOT_MODIFIED;
        if (status.is_informational() ||
            status == StatusCode::NO_CONTENT ||
            ((stream.method == Method::CONNECT) && status.is_success())) &&
            length.is_some()
        {
            return Err(SendError::InvalidHeaders);
        }
        if body_forbidden || ((stream.method == Method::CONNECT) && status.is_success()) {
            length = None;
        }
        if end_stream && length.is_some_and(|length| length != 0) {
            return Err(SendError::InvalidState);
        }
        self.write_headers(id, Some(status), fields, end_stream)?;
        let stream = self.streams.get_mut(&id).unwrap();
        if !status.is_informational() {
            stream.send_state = SendState::Body;
            stream.response_length = length;
            stream.body_forbidden = body_forbidden;
        }
        if end_stream {
            stream.send_state = SendState::Closed;
        }
        self.retire(id);
        Ok(())
    }

    pub fn send_trailers(&mut self, id: u32, fields: &HeaderMap) -> Result<(), SendError> {
        let stream = self.send_stream(id)?;
        if stream.send_state != SendState::Body ||
            stream.response_length.is_some_and(|length| length != stream.sent)
        {
            return Err(SendError::InvalidState);
        }
        validate_fields(map_fields(fields)).map_err(|_| SendError::InvalidHeaders)?;
        if fields.contains_key(header::CONTENT_LENGTH) {
            return Err(SendError::InvalidHeaders);
        }
        self.write_headers(id, None, fields, true)?;
        self.streams.get_mut(&id).unwrap().send_state = SendState::Closed;
        self.retire(id);
        Ok(())
    }

    /// Queue at most one DATA frame. Retry the remaining suffix after capacity
    /// returns. `end_stream` is emitted only when the complete supplied
    /// slice fits.
    pub fn send_data(
        &mut self,
        id: u32,
        data: &[u8],
        end_stream: bool,
    ) -> Result<usize, SendError> {
        self.send_data_prefixed(id, &[], data, end_stream)
    }

    /// [`Self::send_data`] for `prefix` followed by `data`, written as one
    /// frame without joining them first. Returns the bytes taken from both.
    pub fn send_data_prefixed(
        &mut self,
        id: u32,
        prefix: &[u8],
        data: &[u8],
        end_stream: bool,
    ) -> Result<usize, SendError> {
        let total = prefix.len() + data.len();
        let stream = self.send_stream(id)?;
        if stream.send_state != SendState::Body || (stream.body_forbidden && total != 0) {
            return Err(SendError::InvalidState);
        }
        let available = self.send_window.min(stream.send_window).max(0) as usize;
        let room = self.config.max_send_buffer.saturating_sub(self.output().len());
        if room < 9 {
            return Err(SendError::WouldBlock);
        }
        let length = total.min(available).min(self.peer_frame_size as usize).min(room - 9);
        if total != 0 && length == 0 {
            return Err(SendError::WouldBlock);
        }
        let finish = end_stream && length == total;
        let sent = stream.sent.checked_add(length as u64).ok_or(SendError::TooLarge)?;
        if stream
            .response_length
            .is_some_and(|expected| sent > expected || (finish && sent != expected))
        {
            return Err(SendError::InvalidState);
        }
        self.reserve_output(9 + length)?;
        let head = length.min(prefix.len());
        self.output.extend_from_slice(&(length as u32).to_be_bytes()[1..]);
        self.output.extend_from_slice(&[kind::DATA, if finish { flags::END_STREAM } else { 0 }]);
        self.output.extend_from_slice(&id.to_be_bytes());
        self.output.extend_from_slice(&prefix[..head]);
        self.output.extend_from_slice(&data[..length - head]);
        self.send_window -= length as i64;
        let stream = self.streams.get_mut(&id).unwrap();
        stream.send_window -= length as i64;
        stream.sent = sent;
        if finish {
            stream.send_state = SendState::Closed;
        }
        self.retire(id);
        Ok(length)
    }

    pub fn reset(&mut self, id: u32, error_code: u32) -> Result<(), SendError> {
        if self.failed || !self.streams.contains_key(&id) {
            return Err(SendError::Closed);
        }
        self.reserve_output(26)?;
        self.queue(kind::RST_STREAM, 0, id, &error_code.to_be_bytes())
            .map_err(|_| SendError::WouldBlock)?;
        self.remove_stream(id).map_err(|_| SendError::WouldBlock)?;
        Ok(())
    }

    /// Stop accepting new streams while existing streams finish.
    pub fn go_away(&mut self) -> Result<(), SendError> {
        if self.failed {
            return Err(SendError::Closed);
        }
        if self.goaway_last.is_some() {
            return Ok(());
        }
        self.reserve_output(17)?;
        let mut payload = [0; 8];
        payload[..4].copy_from_slice(&self.last_stream_id.to_be_bytes());
        self.queue(kind::GOAWAY, 0, 0, &payload).map_err(|_| SendError::WouldBlock)?;
        self.goaway_last = Some(self.last_stream_id);
        Ok(())
    }

    fn write_headers(
        &mut self,
        id: u32,
        status: Option<StatusCode>,
        fields: &HeaderMap,
        end_stream: bool,
    ) -> Result<(), SendError> {
        let size = fields.iter().try_fold(
            if status.is_some() { 42usize } else { 0 },
            |sum, (name, value)| {
                sum.checked_add(name.as_str().len() + value.len() + 32).ok_or(SendError::TooLarge)
            },
        )?;
        if size > self.config.max_header_list_size as usize ||
            size > self.peer_header_limit as usize
        {
            return Err(SendError::TooLarge);
        }
        // Reserve before mutating HPACK state; failed sends must not desynchronize the
        // peer.
        let bound =
            size.checked_mul(4).and_then(|n| n.checked_add(32)).ok_or(SendError::TooLarge)?;
        self.reserve_output(bound + 9 * bound.div_ceil(self.peer_frame_size as usize))?;
        let headers = status.map(hpack::Header::Status).into_iter().chain(fields.iter().map(
            |(name, value)| hpack::Header::Field { name: Some(name.clone()), value: value.clone() },
        ));
        let mut block = self.hpack_encoder.take_scratch();
        block.clear();
        self.hpack_encoder.encode(headers, &mut block);
        let mut offset = 0;
        loop {
            let end = (offset + self.peer_frame_size as usize).min(block.len());
            let mut frame_flags = if end == block.len() { flags::END_HEADERS } else { 0 };
            if offset == 0 && end_stream {
                frame_flags |= flags::END_STREAM;
            }
            self.queue(
                if offset == 0 { kind::HEADERS } else { kind::CONTINUATION },
                frame_flags,
                id,
                &block[offset..end],
            )
            .map_err(|_| SendError::WouldBlock)?;
            if end == block.len() {
                break;
            }
            offset = end;
        }
        self.hpack_encoder.return_scratch(block);
        Ok(())
    }

    fn send_stream(&self, id: u32) -> Result<&Stream, SendError> {
        if self.failed {
            return Err(SendError::Closed);
        }
        self.streams
            .get(&id)
            .filter(|stream| stream.send_state != SendState::Closed)
            .ok_or(SendError::Closed)
    }

    fn reserve_output(&mut self, bytes: usize) -> Result<(), SendError> {
        if bytes > self.config.max_send_buffer {
            return Err(SendError::TooLarge);
        }
        if self.output().len() > self.config.max_send_buffer - bytes {
            return Err(SendError::WouldBlock);
        }
        if self.output.len() > self.config.max_send_buffer - bytes {
            self.output.copy_within(self.output_offset.., 0);
            self.output.truncate(self.output.len() - self.output_offset);
            self.output_offset = 0;
        }
        self.output.reserve(bytes);
        Ok(())
    }

    fn queue(&mut self, kind: u8, flags: u8, id: u32, payload: &[u8]) -> Result<(), Error> {
        self.reserve_output(9 + payload.len()).map_err(|_| Error::LimitExceeded)?;
        encode_frame(kind, flags, id, payload, self.peer_frame_size, &mut self.output)
    }

    /// Announces connection credit once half the receive window is owed, so
    /// the peer's window never falls below half.
    fn return_connection_credit(&mut self, bytes: u32) -> Result<(), Error> {
        self.unreturned += bytes;
        if self.unreturned >= self.config.recv_window / 2 {
            self.queue(kind::WINDOW_UPDATE, 0, 0, &self.unreturned.to_be_bytes())?;
            self.recv_window += i64::from(self.unreturned);
            self.unreturned = 0;
        }
        Ok(())
    }

    fn remove_stream(&mut self, id: u32) -> Result<bool, Error> {
        if let Some(stream) = self.streams.remove(&id) {
            self.return_connection_credit(stream.pending_recv)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn retire(&mut self, id: u32) {
        if self.streams.get(&id).is_some_and(|stream| {
            !stream.recv_open && stream.send_state == SendState::Closed && stream.pending_recv == 0
        }) {
            self.streams.remove(&id);
        }
    }
}

fn request_head(headers: &[hpack::Header]) -> Result<RequestHead<'_>, Error> {
    let mut method = None;
    let mut scheme = None;
    let mut authority = None;
    let mut path = None;
    // Pseudo-headers precede regular fields.
    let first_field = headers
        .iter()
        .position(|header| matches!(header, hpack::Header::Field { .. }))
        .unwrap_or(headers.len());
    for header in &headers[..first_field] {
        match header {
            hpack::Header::Method(value) if method.is_none() => method = Some(value),
            hpack::Header::Scheme(value) if scheme.is_none() => scheme = Some(value),
            hpack::Header::Authority(value) if authority.is_none() => authority = Some(value),
            hpack::Header::Path(value) if path.is_none() => path = Some(value),
            _ => return Err(Error::Protocol),
        }
    }
    let fields = regular_headers(&headers[first_field..])?;
    let method = method.ok_or(Error::Protocol)?;
    let empty = |value: Option<&hpack::BytesStr>| value.is_none_or(|v| v.as_bytes().is_empty());
    if method == Method::CONNECT {
        if scheme.is_some() || path.is_some() || empty(authority) {
            return Err(Error::Protocol);
        }
    } else if empty(scheme) || empty(path) {
        return Err(Error::Protocol);
    }
    if let Some(value) = scheme {
        value.as_str().parse::<http::uri::Scheme>().map_err(|_| Error::Protocol)?;
    }
    // Parsing a `Bytes` clone validates without copying.
    if let Some(value) = authority {
        if value.as_bytes().contains(&b'@') {
            return Err(Error::Protocol);
        }
        let parsed = http::uri::Authority::from_maybe_shared(value.to_bytes())
            .map_err(|_| Error::Protocol)?;
        if method == Method::CONNECT && parsed.port_u16().is_none() {
            return Err(Error::Protocol);
        }
    }
    if let Some(value) = path {
        let bytes = value.as_bytes();
        if bytes.contains(&b'#') {
            return Err(Error::Protocol);
        }
        if bytes.first() != Some(&b'/') && !(method == Method::OPTIONS && bytes == b"*") {
            return Err(Error::Protocol);
        }
        http::uri::PathAndQuery::from_maybe_shared(value.to_bytes())
            .map_err(|_| Error::Protocol)?;
    }
    Ok(RequestHead {
        method,
        scheme: scheme.map(hpack::BytesStr::as_bytes),
        authority: authority.map(hpack::BytesStr::as_bytes),
        path: path.map(hpack::BytesStr::as_bytes),
        fields,
    })
}

fn regular_headers(headers: &[hpack::Header]) -> Result<Fields<'_>, Error> {
    if !headers.iter().all(|header| matches!(header, hpack::Header::Field { .. })) {
        return Err(Error::Protocol);
    }
    let fields = Fields(headers);
    validate_fields(fields.iter())?;
    Ok(fields)
}

fn validate_fields<'a>(fields: impl Iterator<Item = (&'a str, &'a [u8])>) -> Result<(), Error> {
    for (name, value) in fields {
        if matches!(
            name,
            "connection" | "proxy-connection" | "keep-alive" | "transfer-encoding" | "upgrade"
        ) || (name == "te" && !value.eq_ignore_ascii_case(b"trailers")) ||
            value.first().is_some_and(|c| matches!(c, b' ' | b'\t')) ||
            value.last().is_some_and(|c| matches!(c, b' ' | b'\t')) ||
            value.iter().any(|c| matches!(c, 0 | b'\r' | b'\n'))
        {
            return Err(Error::Protocol);
        }
    }
    Ok(())
}

fn map_fields(fields: &HeaderMap) -> impl Iterator<Item = (&str, &[u8])> {
    fields.iter().map(|(name, value)| (name.as_str(), value.as_bytes()))
}

fn content_length<'a>(values: impl Iterator<Item = &'a [u8]>) -> Result<Option<u64>, Error> {
    let mut length = None;
    for value in values {
        if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
            return Err(Error::Protocol);
        }
        let parsed = std::str::from_utf8(value)
            .map_err(|_| Error::Protocol)?
            .parse()
            .map_err(|_| Error::Protocol)?;
        if length.is_some_and(|old| old != parsed) {
            return Err(Error::Protocol);
        }
        length = Some(parsed);
    }
    Ok(length)
}
