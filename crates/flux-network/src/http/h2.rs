//! HTTP/2 on accepted connections. Each request is buffered whole and
//! delivered like an HTTP/1 request, then answered by its stream id.

use flux_timing::IngestionTime;
use http::{HeaderMap, StatusCode};
use mio::Token;

use super::{HttpRequest, STACK_HEADERS, header_slots};
use crate::{
    NetworkCore,
    http2::{Error, Event, Http2Config, SendError, ServerConnection},
};

/// `RST_STREAM` code ending a stream whose response is complete while its
/// client is still sending.
const NO_ERROR: u32 = 0;
const INTERNAL_ERROR: u32 = 2;
/// Recycled streams drop buffers that grew past this.
const SPARE_CAPACITY: usize = 16 * 1024;
/// Unsent bytes TCP holds per connection: half the group backlog cap's
/// margin over the request limits, so TLS record overhead fits too.
const TCP_BACKLOG: usize = super::TLS_MARGIN_BYTES / 2;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Receiving,
    /// Awaiting [`super::HttpNetwork::respond_to`].
    Delivered,
    /// Headers sent; `out[offset..]` waits for flow-control credit.
    Sending,
}

#[derive(Default)]
pub(super) struct Stream {
    id: u32,
    phase: Phase,
    /// Method, path and authority, then each field's name and value, each
    /// ended by a zero byte, which HTTP/2 forbids in all of them.
    head: Vec<u8>,
    fields: usize,
    body: Vec<u8>,
    out: Vec<u8>,
    offset: usize,
    /// DATA bytes not yet returned to the client's window.
    credit: u32,
    recv_open: bool,
    /// Answered with this status instead of being delivered.
    reject: Option<u16>,
}

impl Stream {
    fn is_head_request(&self) -> bool {
        self.head.starts_with(b"HEAD\0")
    }
}

pub(super) struct Limits {
    pub(super) headers: usize,
    pub(super) head_bytes: usize,
    pub(super) body_bytes: usize,
}

pub(super) struct Http2Conn {
    h2: ServerConnection,
    streams: Vec<Stream>,
    failed: bool,
}

impl Http2Conn {
    pub(super) fn new(config: Http2Config) -> Self {
        let h2 = ServerConnection::new(config).expect("validated HTTP/2 configuration");
        Self { h2, streams: Vec::new(), failed: false }
    }

    /// Takes in complete frames and returns the bytes consumed. A protocol
    /// error queues GOAWAY and drops every stream.
    pub(super) fn receive(
        &mut self,
        input: &[u8],
        limits: &Limits,
        spare: &mut Vec<Stream>,
    ) -> Result<usize, Error> {
        let Self { h2, streams, .. } = self;
        let received = h2.receive(input, IngestionTime::default(), |event| match event {
            Event::Request { stream_id, head, end_stream, .. } => {
                let mut stream = spare.pop().unwrap_or_default();
                stream.id = stream_id;
                stream.recv_open = !end_stream;
                let path = head.path.unwrap_or_default();
                for part in
                    [head.method.as_str().as_bytes(), path, head.authority.unwrap_or_default()]
                {
                    push(&mut stream.head, part);
                }
                for (name, value) in head.fields.iter() {
                    push(&mut stream.head, name.as_bytes());
                    push(&mut stream.head, value);
                    stream.fields += 1;
                }
                if std::str::from_utf8(path).is_err() {
                    stream.reject = Some(400);
                } else if stream.fields > limits.headers || stream.head.len() > limits.head_bytes {
                    stream.reject = Some(431);
                }
                streams.push(stream);
            }
            Event::Data { stream_id, data, end_stream } => {
                if let Some(stream) = streams.iter_mut().find(|stream| stream.id == stream_id) {
                    stream.credit += data.len() as u32;
                    stream.recv_open = !end_stream;
                    if stream.phase == Phase::Receiving && stream.reject.is_none() {
                        if data.len() > limits.body_bytes - stream.body.len() {
                            stream.reject = Some(413);
                        } else {
                            stream.body.extend_from_slice(data);
                        }
                    }
                }
            }
            Event::Trailers { stream_id, .. } => {
                if let Some(stream) = streams.iter_mut().find(|stream| stream.id == stream_id) {
                    stream.recv_open = false;
                }
            }
            Event::Reset { stream_id, .. } => {
                if let Some(at) = streams.iter().position(|stream| stream.id == stream_id) {
                    recycle(streams.swap_remove(at), spare);
                }
            }
            Event::Writable { .. } | Event::GoAway { .. } => {}
        });
        if received.is_err() {
            self.failed = true;
            for stream in self.streams.drain(..) {
                recycle(stream, spare);
            }
        }
        received
    }

    /// Answers rejected streams, then hands every complete request to `emit`.
    pub(super) fn deliver(
        &mut self,
        spare: &mut Vec<Stream>,
        mut emit: impl FnMut(HttpRequest<'_>),
    ) {
        let mut at = 0;
        while at < self.streams.len() {
            if self.streams[at].phase == Phase::Receiving &&
                let Some(status) = self.streams[at].reject
            {
                let status = StatusCode::from_u16(status).expect("valid status");
                if !self.answer(at, status, &HeaderMap::new(), &[], spare) {
                    continue
                }
            }
            at += 1;
        }
        for stream in &mut self.streams {
            if stream.phase != Phase::Receiving || stream.recv_open {
                continue
            }
            stream.phase = Phase::Delivered;
            let mut parts = stream.head.split(|byte| *byte == 0);
            let mut text = || std::str::from_utf8(parts.next().unwrap_or_default()).unwrap_or("");
            let (method, path) = (text(), text());
            let authority = parts.next().unwrap_or_default();
            let (mut stack_slots, mut heap_slots) =
                ([httparse::EMPTY_HEADER; STACK_HEADERS], Vec::new());
            let headers = header_slots(&mut stack_slots, &mut heap_slots, stream.fields + 1);
            let mut len = 0;
            let mut host = false;
            while let (Some(name), Some(value)) = (parts.next(), parts.next()) {
                let name = std::str::from_utf8(name).unwrap_or("");
                host |= name == "host";
                headers[len] = httparse::Header { name, value };
                len += 1;
            }
            // `:authority` stands in for `Host`.
            if !host && !authority.is_empty() {
                headers[len] = httparse::Header { name: "host", value: authority };
                len += 1;
            }
            emit(HttpRequest {
                method,
                path,
                version: 2,
                headers: &headers[..len],
                body: &stream.body,
                stream: stream.id,
            });
        }
    }

    pub(super) fn respond(
        &mut self,
        id: u32,
        status: StatusCode,
        fields: &HeaderMap,
        body: &[u8],
        spare: &mut Vec<Stream>,
    ) -> bool {
        let Some(at) = self
            .streams
            .iter()
            .position(|stream| stream.id == id && stream.phase == Phase::Delivered)
        else {
            return false
        };
        let answered = self.answer(at, status, fields, body, spare);
        self.pump(spare);
        answered
    }

    /// Sends what flow control allows and queues the rest for [`Self::pump`].
    /// False if the stream had to be reset; it is gone then.
    fn answer(
        &mut self,
        at: usize,
        status: StatusCode,
        fields: &HeaderMap,
        body: &[u8],
        spare: &mut Vec<Stream>,
    ) -> bool {
        let stream = &mut self.streams[at];
        let bodyless = stream.is_head_request() || matches!(status.as_u16(), 204 | 304);
        let end = bodyless || body.is_empty();
        let mut sent = self.h2.send_headers(stream.id, status, fields, end).map(|()| 0);
        if sent.is_ok() && !end {
            sent = match self.h2.send_data(stream.id, body, true) {
                Err(SendError::WouldBlock) => Ok(0),
                sent => sent,
            };
        }
        let Ok(sent) = sent else {
            let _ = self.h2.reset(stream.id, INTERNAL_ERROR);
            recycle(self.streams.swap_remove(at), spare);
            return false
        };
        stream.phase = Phase::Sending;
        if !end {
            stream.out.extend_from_slice(&body[sent..]);
        }
        true
    }

    /// Returns DATA credit and sends queued response bytes; recycles streams
    /// whose response is complete.
    pub(super) fn pump(&mut self, spare: &mut Vec<Stream>) {
        let mut at = 0;
        while at < self.streams.len() {
            let stream = &mut self.streams[at];
            if stream.credit != 0 &&
                self.h2.release_capacity(stream.id, stream.credit) != Err(SendError::WouldBlock)
            {
                stream.credit = 0;
            }
            let mut done = stream.phase == Phase::Sending;
            while done && stream.offset < stream.out.len() {
                match self.h2.send_data(stream.id, &stream.out[stream.offset..], true) {
                    Ok(sent) => stream.offset += sent,
                    Err(SendError::WouldBlock) => done = false,
                    Err(_) => {
                        let _ = self.h2.reset(stream.id, INTERNAL_ERROR);
                        stream.offset = stream.out.len();
                    }
                }
            }
            if done && stream.credit == 0 {
                if stream.recv_open {
                    let _ = self.h2.reset(stream.id, NO_ERROR);
                }
                recycle(self.streams.swap_remove(at), spare);
            } else {
                at += 1;
            }
        }
    }

    pub(super) fn go_away(&mut self) {
        let _ = self.h2.go_away();
    }

    /// Writes queued frames to `token` up to [`TCP_BACKLOG`] unsent.
    pub(super) fn flush(&mut self, net: &mut NetworkCore, token: Token) {
        let Some(queued) = net.pending_write_bytes(token) else { return };
        let output = self.h2.output();
        let len = output.len().min(TCP_BACKLOG.saturating_sub(queued));
        if len != 0 && net.send_with(token, |out| out.extend_from_slice(&output[..len])) {
            self.h2.consume_output(len);
        }
    }

    /// Frames, response bytes or DATA credit wait to go out.
    pub(super) fn has_output(&self) -> bool {
        !self.h2.output().is_empty() ||
            self.streams.iter().any(|stream| {
                stream.credit != 0 ||
                    (stream.phase == Phase::Sending && stream.offset < stream.out.len())
            })
    }

    /// No request awaits its response.
    pub(super) fn is_idle(&self) -> bool {
        !self.streams.iter().any(|stream| stream.phase == Phase::Delivered)
    }

    /// Every frame is written and the connection should close.
    pub(super) fn finished(&self) -> bool {
        self.h2.output().is_empty() &&
            (self.failed || (self.h2.is_draining() && self.streams.is_empty()))
    }
}

fn push(head: &mut Vec<u8>, part: &[u8]) {
    head.extend_from_slice(part);
    head.push(0);
}

fn recycle(mut stream: Stream, spare: &mut Vec<Stream>) {
    if spare.len() >= super::SPARE_HEADS {
        return
    }
    for buf in [&mut stream.head, &mut stream.body, &mut stream.out] {
        buf.clear();
        buf.shrink_to(SPARE_CAPACITY);
    }
    let Stream { head, body, out, .. } = stream;
    spare.push(Stream { head, body, out, ..Stream::default() });
}
