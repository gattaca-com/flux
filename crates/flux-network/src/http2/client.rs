//! Client-side HTTP/2 over caller-owned transport buffers.

use std::{io::Cursor, ops::ControlFlow};

use bytes::BytesMut;
use http::{HeaderMap, Method, StatusCode};
use rustc_hash::FxHashMap;

use super::{
    CLIENT_PREFACE, DEFAULT_MAX_FRAME_SIZE, Decoder, Error, Fields, Frame, Http2Config, Payload,
    SendError,
    connection::{
        INITIAL_WINDOW, MAX_HEADER_FIELDS, MAX_WINDOW, content_length, map_fields, regular_headers,
        validate_fields,
    },
    encode_frame, flags, hpack, kind,
};

const MAX_STREAM_ID: u32 = 0x7fff_ffff;
const REFUSED_STREAM: u32 = 7;

/// A request's header block, encoded once and sent by reference.
///
/// Every field is a literal that never enters the peer's dynamic table, so a
/// block is valid on every stream of every connection and sending it costs
/// one copy.
#[derive(Clone, Debug)]
pub struct RequestHeaders {
    block: Vec<u8>,
    /// Decoded size, as `SETTINGS_MAX_HEADER_LIST_SIZE` counts it.
    size: usize,
    /// Responses to HEAD carry no content whatever their `content-length`.
    head: bool,
}

impl RequestHeaders {
    /// `fields` must be lowercase and free of connection-specific headers.
    pub fn new(
        method: Method,
        scheme: &str,
        authority: &str,
        path: &str,
        fields: &HeaderMap,
    ) -> Result<Self, SendError> {
        validate_fields(map_fields(fields)).map_err(|_| SendError::InvalidHeaders)?;
        let head = method == Method::HEAD;
        let headers = [
            hpack::Header::Method(method),
            hpack::Header::Scheme(hpack::BytesStr::from(scheme)),
            hpack::Header::Authority(hpack::BytesStr::from(authority)),
            hpack::Header::Path(hpack::BytesStr::from(path)),
        ];
        let size = headers.iter().map(hpack::Header::len).sum::<usize>() +
            fields.iter().map(|(name, value)| hpack::header::len(name, value)).sum::<usize>();
        let headers = headers.into_iter().map(hpack::Header::into);
        let fields = fields.iter().map(|(name, value)| hpack::Header::Field {
            name: Some(name.clone()),
            value: value.clone(),
        });
        let mut block = BytesMut::new();
        // No table: nothing is indexed.
        hpack::Encoder::new(0, 0).encode(headers.chain(fields), &mut block);
        Ok(Self { block: block.to_vec(), size, head })
    }
}

#[derive(Debug)]
pub enum ClientEvent<'a> {
    /// The final response headers. Informational responses are skipped.
    Response {
        stream_id: u32,
        status: StatusCode,
        fields: Fields<'a>,
        end_stream: bool,
    },
    /// Response body bytes. Their flow-control credit is returned after the
    /// callback, so copy what must outlive it.
    Data {
        stream_id: u32,
        data: &'a [u8],
        end_stream: bool,
    },
    Trailers {
        stream_id: u32,
        fields: Fields<'a>,
    },
    /// The stream ended without a complete response: the server reset it, or
    /// refused it in GOAWAY with `REFUSED_STREAM`.
    Reset {
        stream_id: u32,
        error_code: u32,
    },
    /// Capacity might now be available. A stream identifier of zero applies to
    /// all streams.
    Writable {
        stream_id: u32,
    },
    /// No new streams are accepted. Streams above `last_stream_id` were
    /// reset; the others may still complete.
    GoAway {
        last_stream_id: u32,
        error_code: u32,
    },
    /// The acknowledgement of a [`ClientConnection::ping`].
    Pong {
        opaque: [u8; 8],
    },
}

struct Stream {
    send_open: bool,
    recv_open: bool,
    /// The final response headers arrived.
    responded: bool,
    /// The response's content length: declared, or zero when it carries none.
    expected: Option<u64>,
    received: u64,
    send_window: i64,
    recv_window: i64,
}

struct HeaderBlock {
    stream_id: u32,
    end_stream: bool,
    bytes: BytesMut,
}

/// An HTTP/2 client connection without sockets, tasks, or an executor.
///
/// Write `output()` to the transport, starting with the preface `new` queues,
/// and call `consume_output()` after successful writes. Retain unconsumed
/// input from `receive()`. Response DATA credit is returned as it is
/// delivered. Server push is disabled.
// Independent connection flags, not states of one value.
#[allow(clippy::struct_excessive_bools)]
pub struct ClientConnection {
    config: Http2Config,
    decoder: Decoder,
    hpack_decoder: hpack::Decoder,
    headers: Option<HeaderBlock>,
    /// Header block storage, reused once decoded fields release it.
    block_buffer: BytesMut,
    decoded_headers: Vec<hpack::Header>,
    streams: FxHashMap<u32, Stream>,
    next_stream_id: u32,
    peer_settings: bool,
    settings_ack_pending: bool,
    peer_max_streams: u32,
    peer_frame_size: u32,
    peer_header_limit: u32,
    peer_initial_window: i64,
    send_window: i64,
    recv_window: i64,
    /// Connection credit consumed but not yet announced.
    unreturned: u32,
    output: Vec<u8>,
    output_offset: usize,
    goaway: bool,
    failed: bool,
}

impl ClientConnection {
    /// `max_concurrent_streams` is unused: servers open no streams.
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
            headers: None,
            block_buffer: BytesMut::new(),
            decoded_headers: Vec::new(),
            streams: FxHashMap::default(),
            next_stream_id: 1,
            peer_settings: false,
            settings_ack_pending: true,
            peer_max_streams: u32::MAX,
            peer_frame_size: DEFAULT_MAX_FRAME_SIZE,
            peer_header_limit: u32::MAX,
            peer_initial_window: INITIAL_WINDOW,
            send_window: INITIAL_WINDOW,
            recv_window: i64::from(config.recv_window),
            unreturned: 0,
            output: CLIENT_PREFACE.to_vec(),
            output_offset: 0,
            goaway: false,
            failed: false,
        };
        let mut settings = Vec::with_capacity(18);
        for (id, value) in [(2u16, 0), (4, config.recv_window), (6, config.max_header_list_size)] {
            settings.extend_from_slice(&id.to_be_bytes());
            settings.extend_from_slice(&value.to_be_bytes());
        }
        connection.queue(kind::SETTINGS, 0, 0, &settings)?;
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

    /// No new streams can open: the server sent GOAWAY, stream ids ran out, or
    /// the connection failed. Reconnect once open streams finish.
    pub fn is_draining(&self) -> bool {
        self.goaway || self.failed || self.next_stream_id > MAX_STREAM_ID
    }

    /// Streams that have not finished.
    pub fn open_streams(&self) -> usize {
        self.streams.len()
    }

    /// Process complete frames and return the number of input bytes consumed.
    ///
    /// Errors queue GOAWAY when room remains. Flush output, then close the
    /// transport.
    pub fn receive(
        &mut self,
        input: &[u8],
        mut handler: impl FnMut(ClientEvent<'_>),
    ) -> Result<usize, Error> {
        if self.failed {
            return Err(Error::Protocol);
        }
        let result = self.receive_inner(input, &mut handler);
        if let Err(error) = result {
            self.failed = true;
            let mut payload = [0; 8];
            payload[4..].copy_from_slice(&(error as u32).to_be_bytes());
            let _ = self.queue(kind::GOAWAY, 0, 0, &payload);
        }
        result
    }

    fn receive_inner(
        &mut self,
        input: &[u8],
        handler: &mut impl FnMut(ClientEvent<'_>),
    ) -> Result<usize, Error> {
        let mut consumed = 0;
        while let Some((frame, length)) = self.decoder.decode(&input[consumed..])? {
            if !self.peer_settings {
                if !matches!(frame.payload, Payload::Settings { ack: false, .. }) {
                    return Err(Error::Protocol);
                }
                self.peer_settings = true;
            }
            self.on_frame(frame, handler)?;
            consumed += length;
        }
        Ok(consumed)
    }

    /// Whether `id` names a stream this client has not opened yet.
    const fn idle(&self, id: u32) -> bool {
        id >= self.next_stream_id || id.is_multiple_of(2)
    }

    fn on_frame(
        &mut self,
        frame: Frame<'_>,
        handler: &mut impl FnMut(ClientEvent<'_>),
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
                        3 => self.peer_max_streams = value,
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
                handler(ClientEvent::Writable { stream_id: 0 });
            }
            Payload::Headers { fragment, end_headers, end_stream, .. } => {
                if self.idle(id) {
                    return Err(Error::Protocol);
                }
                let mut bytes = std::mem::take(&mut self.block_buffer);
                bytes.clear();
                bytes.extend_from_slice(fragment);
                self.headers = Some(HeaderBlock { stream_id: id, end_stream, bytes });
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
                } else if self.idle(id) {
                    return Err(Error::Protocol);
                } else {
                    return Ok(());
                };
                *window += i64::from(increment);
                if *window > MAX_WINDOW {
                    return Err(Error::FlowControl);
                }
                handler(ClientEvent::Writable { stream_id: id });
            }
            Payload::Reset { error_code } => {
                if self.idle(id) {
                    return Err(Error::Protocol);
                }
                if self.streams.remove(&id).is_some() {
                    handler(ClientEvent::Reset { stream_id: id, error_code });
                }
            }
            Payload::Ping { ack: false, opaque } => {
                self.queue(kind::PING, flags::ACK, 0, &opaque)?;
            }
            Payload::Ping { ack: true, opaque } => handler(ClientEvent::Pong { opaque }),
            Payload::GoAway { last_stream_id, error_code, .. } => {
                self.goaway = true;
                // Rare, so the refused ids are collected without reuse.
                let refused: Vec<u32> =
                    self.streams.keys().copied().filter(|&s| s > last_stream_id).collect();
                for stream_id in refused {
                    self.streams.remove(&stream_id);
                    handler(ClientEvent::Reset { stream_id, error_code: REFUSED_STREAM });
                }
                handler(ClientEvent::GoAway { last_stream_id, error_code });
            }
            Payload::PushPromise { .. } => return Err(Error::Protocol),
            Payload::Priority(_) | Payload::Unknown { .. } => {}
        }
        Ok(())
    }

    fn finish_headers(&mut self, handler: &mut impl FnMut(ClientEvent<'_>)) -> Result<(), Error> {
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
        let result = self.dispatch_headers(block.stream_id, block.end_stream, handler);
        // Releases the block's bytes for reuse by the next header block.
        self.decoded_headers.clear();
        result
    }

    fn dispatch_headers(
        &mut self,
        id: u32,
        end_stream: bool,
        handler: &mut impl FnMut(ClientEvent<'_>),
    ) -> Result<(), Error> {
        // Closed streams still update HPACK state before their headers are
        // discarded.
        let Some(stream) = self.streams.get_mut(&id) else { return Ok(()) };
        if !stream.recv_open {
            return Err(Error::StreamClosed);
        }
        let headers = &self.decoded_headers;
        if stream.responded {
            let fields = regular_headers(headers)?;
            if !end_stream ||
                fields.get(http::header::CONTENT_LENGTH.as_str()).is_some() ||
                stream.expected.is_some_and(|length| length != stream.received)
            {
                return Err(Error::Protocol);
            }
            stream.recv_open = false;
            handler(ClientEvent::Trailers { stream_id: id, fields });
        } else {
            let Some((hpack::Header::Status(status), rest)) = headers.split_first() else {
                return Err(Error::Protocol);
            };
            let fields = regular_headers(rest)?;
            if status.is_informational() {
                if end_stream || *status == StatusCode::SWITCHING_PROTOCOLS {
                    return Err(Error::Protocol);
                }
                return Ok(());
            }
            // Set to zero already for HEAD.
            if stream.expected.is_none() {
                stream.expected =
                    if *status == StatusCode::NO_CONTENT || *status == StatusCode::NOT_MODIFIED {
                        Some(0)
                    } else {
                        content_length(fields.get_all(http::header::CONTENT_LENGTH.as_str()))?
                    };
            }
            if end_stream && stream.expected.is_some_and(|length| length != 0) {
                return Err(Error::Protocol);
            }
            stream.responded = true;
            stream.recv_open = !end_stream;
            handler(ClientEvent::Response { stream_id: id, status: *status, fields, end_stream });
        }
        self.retire(id)
    }

    fn receive_data(
        &mut self,
        id: u32,
        data: &[u8],
        end_stream: bool,
        flow_len: u32,
        handler: &mut impl FnMut(ClientEvent<'_>),
    ) -> Result<(), Error> {
        self.recv_window -= i64::from(flow_len);
        if self.recv_window < 0 {
            return Err(Error::FlowControl);
        }
        self.return_connection_credit(flow_len)?;
        let Some(stream) = self.streams.get_mut(&id) else {
            return if self.idle(id) { Err(Error::Protocol) } else { Ok(()) };
        };
        if !stream.recv_open || !stream.responded {
            return Err(Error::Protocol);
        }
        stream.recv_window -= i64::from(flow_len);
        if stream.recv_window < 0 {
            return Err(Error::FlowControl);
        }
        stream.received = stream.received.checked_add(data.len() as u64).ok_or(Error::Protocol)?;
        if stream.expected.is_some_and(|length| {
            stream.received > length || (end_stream && stream.received != length)
        }) {
            return Err(Error::Protocol);
        }
        stream.recv_open = !end_stream;
        // Announce stream credit once half the window is owed.
        let owed = i64::from(self.config.recv_window) - stream.recv_window;
        let update =
            (stream.recv_open && owed >= i64::from(self.config.recv_window / 2)).then(|| {
                stream.recv_window += owed;
                owed as u32
            });
        handler(ClientEvent::Data { stream_id: id, data, end_stream });
        if let Some(increment) = update {
            self.queue(kind::WINDOW_UPDATE, 0, id, &increment.to_be_bytes())?;
        }
        self.retire(id)
    }

    /// Opens a stream with `headers` and returns its id. `WouldBlock` while
    /// the server's stream limit or the send buffer is full; `Closed` once the
    /// connection is draining.
    pub fn send_headers(
        &mut self,
        headers: &RequestHeaders,
        end_stream: bool,
    ) -> Result<u32, SendError> {
        if self.is_draining() {
            return Err(SendError::Closed);
        }
        if headers.size > self.config.max_header_list_size as usize ||
            headers.size > self.peer_header_limit as usize
        {
            return Err(SendError::TooLarge);
        }
        if self.streams.len() >= self.peer_max_streams as usize {
            return Err(SendError::WouldBlock);
        }
        let block = &headers.block;
        let frame = self.peer_frame_size as usize;
        self.reserve_output(block.len() + 9 * block.len().div_ceil(frame).max(1))?;
        let id = self.next_stream_id;
        let mut offset = 0;
        loop {
            let end = (offset + frame).min(block.len());
            let mut frame_flags = if end == block.len() { flags::END_HEADERS } else { 0 };
            if offset == 0 && end_stream {
                frame_flags |= flags::END_STREAM;
            }
            let frame_kind = if offset == 0 { kind::HEADERS } else { kind::CONTINUATION };
            self.queue(frame_kind, frame_flags, id, &block[offset..end])
                .map_err(|_| SendError::WouldBlock)?;
            if end == block.len() {
                break;
            }
            offset = end;
        }
        self.next_stream_id += 2;
        self.streams.insert(id, Stream {
            send_open: !end_stream,
            recv_open: true,
            responded: false,
            expected: headers.head.then_some(0),
            received: 0,
            send_window: self.peer_initial_window,
            recv_window: i64::from(self.config.recv_window),
        });
        Ok(id)
    }

    /// Queue at most one DATA frame. Retry the remaining suffix after capacity
    /// returns. `end_stream` is emitted only when the complete supplied slice
    /// fits.
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
        if self.failed {
            return Err(SendError::Closed);
        }
        let stream = self.streams.get(&id).filter(|s| s.send_open).ok_or(SendError::Closed)?;
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
        stream.send_open = !finish;
        // Never fails: removal returns no credit.
        let _ = self.retire(id);
        Ok(length)
    }

    /// Abandons a stream; its later frames are discarded.
    pub fn reset(&mut self, id: u32, error_code: u32) -> Result<(), SendError> {
        if self.failed || !self.streams.contains_key(&id) {
            return Err(SendError::Closed);
        }
        self.reserve_output(13)?;
        self.queue(kind::RST_STREAM, 0, id, &error_code.to_be_bytes())
            .map_err(|_| SendError::WouldBlock)?;
        self.streams.remove(&id);
        Ok(())
    }

    /// Queues a PING; its acknowledgement arrives as [`ClientEvent::Pong`].
    pub fn ping(&mut self, opaque: [u8; 8]) -> Result<(), SendError> {
        if self.failed {
            return Err(SendError::Closed);
        }
        self.reserve_output(17)?;
        self.queue(kind::PING, 0, 0, &opaque).map_err(|_| SendError::WouldBlock)
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

    /// Removes a finished stream. A response that ends while the request is
    /// still being sent resets the request with `NO_ERROR` (RFC 9113 8.1).
    fn retire(&mut self, id: u32) -> Result<(), Error> {
        let Some(stream) = self.streams.get(&id) else { return Ok(()) };
        if stream.recv_open {
            return Ok(());
        }
        let send_open = stream.send_open;
        self.streams.remove(&id);
        if send_open {
            self.queue(kind::RST_STREAM, 0, id, &0u32.to_be_bytes())?;
        }
        Ok(())
    }
}
