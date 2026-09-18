//! Poll-driven `ClickHouse` client speaking the native TCP protocol on a
//! caller-owned TCP network, sharing one poll with the tile's other traffic.
//!
//! Requests queue inside the client; [`ClickHouse::connect`] opens the pool,
//! [`ClickHouse::on_event`], called from the network's event handler, tracks
//! the pooled connections, and [`ClickHouse::drive`] sends queued requests on
//! idle ones and delivers one outcome per id. A full queue refuses the new
//! request instead of failing queued ones; queued requests wait out pool
//! outages.
//!
//! Rows are handed over as `RowBinary` and transposed into a native block once
//! the server has named the target columns, so the server's types, not the
//! Rust ones, decide the wire layout.

mod native;
pub mod rowbinary;

use std::{
    collections::VecDeque,
    net::SocketAddr,
    time::{Duration, Instant},
};

use flux_network::{
    Token,
    tcp::{Framing, TcpEvent, TcpGroup, TcpGroupConfig, TcpNetworkCore},
};
use native::{BlockResume, Cursor, push_block, put_string, put_uvarint, read_compressed};
pub use native::{Column, Output};
use serde::Serialize;

const CLIENT_NAME: &str = "flux";
const CLIENT_VERSION_MAJOR: u64 = 1;
const CLIENT_VERSION_MINOR: u64 = 1;
/// Protocol revision this client speaks, and the oldest server it talks to:
/// settings are serialized as strings from here on, so no older server is
/// supported. `ClickHouse` 20.1 and newer are above it.
const CLIENT_REVISION: u64 = 54_429;
const DEFAULT_MAX_OUTPUT_BYTES: usize = 16 << 20;
const DEFAULT_MAX_QUEUED_BYTES: usize = 256 << 20;
/// The consumed receive prefix is compacted once it reaches this, so no byte
/// is moved more than once per block of progress.
const RX_COMPACT_AT: usize = 64 * 1024;

const CLIENT_HELLO: u64 = 0;
const CLIENT_QUERY: u64 = 1;
const CLIENT_DATA: u64 = 2;
const SERVER_HELLO: u64 = 0;
const SERVER_DATA: u64 = 1;
const SERVER_EXCEPTION: u64 = 2;
const SERVER_PROGRESS: u64 = 3;
const SERVER_PONG: u64 = 4;
const SERVER_END_OF_STREAM: u64 = 5;
const SERVER_PROFILE_INFO: u64 = 6;
const SERVER_TOTALS: u64 = 7;
const SERVER_EXTREMES: u64 = 8;
const SERVER_LOG: u64 = 10;
const SERVER_TABLE_COLUMNS: u64 = 11;
const STATE_COMPLETE: u64 = 2;
const COMPRESS_DISABLE: u64 = 0;
const COMPRESS_ENABLE: u64 = 1;
const QUERY_KIND_INITIAL: u8 = 1;
const INTERFACE_TCP: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryId(u64);

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// The server refused or aborted the request.
    Server { code: i32, name: String, message: String },
    /// Lost before the end of the stream; the server may or may not have run
    /// the request.
    Disconnected,
    /// The server's reply exceeded `max_output_bytes`.
    TooLarge,
    /// A message arrived that fits no legal state.
    Protocol(&'static str),
    /// A dispatched request was still running after `with_request_timeout`.
    Timeout,
}

type Outcome = (QueryId, Result<Output, Error>);

struct Request {
    id: QueryId,
    sql: String,
    data: Option<Vec<u8>>,
}

impl Request {
    fn queued_len(&self) -> usize {
        self.sql.len() + self.data.as_ref().map_or(0, Vec::len)
    }
}

enum Packet {
    Hello {
        revision: u64,
    },
    Data {
        block: Output,
        /// The decoded bytes this packet counts against the output budget.
        bytes: usize,
    },
    Exception {
        code: i32,
        name: String,
        message: String,
    },
    EndOfStream,
    /// A packet this client reads past: progress, profile info, and the like.
    Ignored,
}

impl Packet {
    /// Reads one packet, returning how many bytes it spanned. `Ok(None)` means
    /// the buffer holds only part of a packet.
    fn read(
        rx: &[u8],
        compressed: bool,
        budget: usize,
        resume: &mut Option<BlockResume>,
    ) -> Result<Option<(usize, Self)>, Error> {
        let mut cursor = Cursor::new(rx);
        if let Some(packet) = Self::parse(&mut cursor, compressed, budget, resume) {
            resume.take();
            return Ok(Some((rx.len() - cursor.remaining(), packet)))
        }
        if cursor.too_large {
            return Err(Error::TooLarge)
        }
        cursor.invalid.map_or(Ok(None), |reason| Err(Error::Protocol(reason)))
    }

    fn parse(
        cursor: &mut Cursor<'_>,
        compressed: bool,
        budget: usize,
        resume: &mut Option<BlockResume>,
    ) -> Option<Self> {
        match cursor.uvarint()? {
            SERVER_HELLO => {
                cursor.string()?;
                cursor.uvarint()?;
                cursor.uvarint()?;
                let revision = cursor.uvarint()?;
                cursor.string()?;
                cursor.string()?;
                cursor.uvarint()?;
                Some(Self::Hello { revision })
            }
            SERVER_DATA | SERVER_TOTALS | SERVER_EXTREMES | SERVER_LOG => {
                cursor.string()?;
                let (block, bytes) = if compressed {
                    let raw = read_compressed(cursor, budget)?;
                    let bytes = raw.len();
                    let mut inner = Cursor::new(&raw);
                    let Some(block) = Output::read_resume(&mut inner, &mut None) else {
                        cursor.invalid = inner.invalid.or(Some("compressed block holds no block"));
                        return None
                    };
                    (block, bytes)
                } else {
                    let before = cursor.remaining();
                    let block = Output::read_resume(cursor, resume)?;
                    (block, before - cursor.remaining())
                };
                Some(Self::Data { block, bytes })
            }
            SERVER_EXCEPTION => {
                let code = cursor.i32()?;
                let name = cursor.owned_string()?;
                let message = cursor.owned_string()?;
                cursor.string()?;
                while cursor.u8()? == 1 {
                    cursor.i32()?;
                    cursor.string()?;
                    cursor.string()?;
                    cursor.string()?;
                }
                Some(Self::Exception { code, name, message })
            }
            SERVER_PROGRESS => {
                for _ in 0..5 {
                    cursor.uvarint()?;
                }
                Some(Self::Ignored)
            }
            SERVER_PROFILE_INFO => {
                cursor.uvarint()?;
                cursor.uvarint()?;
                cursor.uvarint()?;
                cursor.u8()?;
                cursor.uvarint()?;
                cursor.u8()?;
                Some(Self::Ignored)
            }
            SERVER_TABLE_COLUMNS => {
                cursor.string()?;
                cursor.string()?;
                Some(Self::Ignored)
            }
            SERVER_PONG => Some(Self::Ignored),
            SERVER_END_OF_STREAM => Some(Self::EndOfStream),
            _ => cursor.fail("unknown server packet"),
        }
    }
}

struct InFlight {
    id: QueryId,
    /// The insert body, held until the server's header block names the target
    /// columns; `None` once it has been written to the outbox.
    data: Option<Vec<u8>>,
    output: Output,
    bytes: usize,
    compressed: bool,
    started: Instant,
}

enum State {
    Connecting,
    Hello,
    Ready,
    Busy(InFlight),
    Dead,
}

struct Conn {
    token: Token,
    state: State,
    rx: Vec<u8>,
    rx_start: usize,
    data_resume: Option<BlockResume>,
    outbox: Vec<u8>,
}

impl Conn {
    fn reset(&mut self, state: State) {
        self.state = state;
        self.rx.clear();
        self.rx_start = 0;
        self.data_resume = None;
        self.outbox.clear();
    }

    fn in_flight(&self) -> Option<QueryId> {
        match &self.state {
            State::Busy(flight) => Some(flight.id),
            _ => None,
        }
    }

    fn fatal(&mut self, error: Error) -> Option<Outcome> {
        let id = self.in_flight();
        self.state = State::Dead;
        id.map(|id| (id, Err(error)))
    }

    /// Applies one packet, yielding this request's outcome when it finished.
    fn on_packet(&mut self, packet: Packet, max_output: usize) -> Option<Outcome> {
        match (&mut self.state, packet) {
            (State::Hello, Packet::Hello { revision }) => {
                if revision < CLIENT_REVISION {
                    return self.fatal(Error::Protocol("server speaks an unsupported revision"))
                }
                self.state = State::Ready;
                None
            }
            (State::Busy(flight), Packet::Data { block, bytes }) => {
                flight.bytes += bytes;
                if flight.bytes > max_output {
                    return self.fatal(Error::TooLarge)
                }
                if let Some(data) = flight.data.take() {
                    let compressed = flight.compressed;
                    let mut raw = Vec::with_capacity(data.len() + 64);
                    if let Err(reason) = Output::write_rowbinary(&block.columns, &data, &mut raw) {
                        return self.fatal(Error::Protocol(reason))
                    }
                    let mut packet = Vec::with_capacity(raw.len() + 64);
                    put_uvarint(&mut packet, CLIENT_DATA);
                    put_string(&mut packet, b"");
                    push_block(&mut packet, &raw, compressed);
                    put_uvarint(&mut packet, CLIENT_DATA);
                    put_string(&mut packet, b"");
                    raw.clear();
                    Output::write_empty(&mut raw);
                    push_block(&mut packet, &raw, compressed);
                    self.outbox.extend_from_slice(&packet);
                    return None
                }
                if flight.output.columns.is_empty() {
                    flight.output.columns = block.columns;
                }
                flight.output.rows.extend(block.rows);
                None
            }
            (State::Busy(flight), Packet::EndOfStream) => {
                let id = flight.id;
                let output = std::mem::take(&mut flight.output);
                self.state = State::Ready;
                Some((id, Ok(output)))
            }
            // The server keeps the connection after an exception, but only it
            // knows how much of an interrupted insert it consumed, so the
            // connection is dropped and redialled instead.
            (State::Busy(_), Packet::Exception { code, name, message }) => {
                self.fatal(Error::Server { code, name, message })
            }
            (State::Busy(_) | State::Hello, Packet::Ignored) => None,
            (State::Hello, _) => self.fatal(Error::Protocol("packet arrived before the handshake")),
            _ => self.fatal(Error::Protocol("packet arrived with no query in flight")),
        }
    }
}

pub struct ClickHouse {
    addr: SocketAddr,
    user: String,
    password: String,
    database: String,
    settings: Vec<(String, String)>,
    connections: usize,
    max_output_bytes: usize,
    max_queued_bytes: usize,
    compression: bool,
    request_timeout: Option<Duration>,
    group: Option<TcpGroup>,
    conns: Vec<Conn>,
    queue: VecDeque<Request>,
    queued_bytes: usize,
    outcomes: Vec<Outcome>,
    next_id: u64,
}

impl ClickHouse {
    pub fn new(addr: SocketAddr, connections: usize) -> Self {
        assert!(connections > 0, "connections must be nonzero");
        Self {
            addr,
            user: "default".to_owned(),
            password: String::new(),
            database: String::new(),
            settings: Vec::new(),
            connections,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            max_queued_bytes: DEFAULT_MAX_QUEUED_BYTES,
            compression: false,
            request_timeout: None,
            group: None,
            conns: Vec::new(),
            queue: VecDeque::new(),
            queued_bytes: 0,
            outcomes: Vec::new(),
            next_id: 0,
        }
    }

    pub fn with_credentials(mut self, user: &str, password: &str) -> Self {
        assert!(self.group.is_none(), "configure before connect");
        user.clone_into(&mut self.user);
        password.clone_into(&mut self.password);
        self
    }

    pub fn with_database(mut self, database: &str) -> Self {
        assert!(self.group.is_none(), "configure before connect");
        database.clone_into(&mut self.database);
        self
    }

    /// Sets a query setting such as `max_execution_time`, sent with every
    /// request.
    pub fn with_setting(mut self, name: &str, value: &str) -> Self {
        assert!(self.group.is_none(), "configure before connect");
        match self.settings.iter_mut().find(|(n, _)| n == name) {
            Some((_, v)) => value.clone_into(v),
            None => self.settings.push((name.to_owned(), value.to_owned())),
        }
        self
    }

    pub fn with_max_output_bytes(mut self, max_output_bytes: usize) -> Self {
        assert!(max_output_bytes > 0, "max_output_bytes must be nonzero");
        self.max_output_bytes = max_output_bytes;
        self
    }

    /// Bound on queued body bytes; a request that would exceed it is refused,
    /// so accepted work is never dropped.
    pub fn with_max_queued_bytes(mut self, max_queued_bytes: usize) -> Self {
        self.max_queued_bytes = max_queued_bytes;
        self
    }

    /// Compresses blocks both ways with `LZ4`, trading CPU for wire bytes.
    /// Off unless enabled.
    pub fn with_compression(mut self) -> Self {
        assert!(self.group.is_none(), "configure before connect");
        self.compression = true;
        self
    }

    /// Fails a dispatched request still running after `timeout`, dropping its
    /// connection; queued requests still wait out pool outages.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        assert!(self.group.is_none(), "configure before connect");
        assert!(!timeout.is_zero(), "timeout must be nonzero");
        self.request_timeout = Some(timeout);
        self
    }

    /// Queues `sql`, or `None` when the queue is full.
    pub fn query(&mut self, sql: &str) -> Option<QueryId> {
        if self.full_for(sql.len()) {
            return None
        }
        Some(self.enqueue(sql.to_owned(), None))
    }

    /// Queues `RowBinary` `body` as the rows of an `INSERT INTO table (columns)
    /// VALUES` statement, handing it back when the queue is full.
    pub fn insert(&mut self, sql: &str, body: Vec<u8>) -> Result<QueryId, Vec<u8>> {
        if self.full_for(sql.len() + body.len()) {
            return Err(body)
        }
        Ok(self.enqueue(sql.to_owned(), Some(body)))
    }

    /// Encodes `rows` as `RowBinary` and queues them for `table`, naming the
    /// columns after the first row's fields. A full queue hands the encoded
    /// body back for a later [`ClickHouse::insert`]. Panics on an empty batch
    /// or an unencodable row; a row shaped unlike the first is refused by the
    /// server.
    pub fn insert_rows<T: Serialize>(
        &mut self,
        table: &str,
        rows: &[T],
    ) -> Result<QueryId, Vec<u8>> {
        let sql = rowbinary::insert_statement(table, &rows[0]).expect("RowBinary row");
        let mut body = Vec::new();
        for row in rows {
            rowbinary::encode(&mut body, row).expect("RowBinary row");
        }
        self.insert(&sql, body)
    }

    fn enqueue(&mut self, sql: String, data: Option<Vec<u8>>) -> QueryId {
        let id = QueryId(self.next_id);
        self.next_id += 1;
        let request = Request { id, sql, data };
        self.queued_bytes += request.queued_len();
        self.queue.push_back(request);
        id
    }

    fn full_for(&self, len: usize) -> bool {
        self.queued_bytes + len > self.max_queued_bytes
    }

    fn pop_queued(&mut self) -> Option<Request> {
        let request = self.queue.pop_front()?;
        self.queued_bytes -= request.queued_len();
        Some(request)
    }

    fn hello_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, CLIENT_HELLO);
        put_string(&mut out, CLIENT_NAME.as_bytes());
        put_uvarint(&mut out, CLIENT_VERSION_MAJOR);
        put_uvarint(&mut out, CLIENT_VERSION_MINOR);
        put_uvarint(&mut out, CLIENT_REVISION);
        put_string(&mut out, self.database.as_bytes());
        put_string(&mut out, self.user.as_bytes());
        put_string(&mut out, self.password.as_bytes());
        out
    }

    fn query_bytes(&self, sql: &str) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, CLIENT_QUERY);
        put_string(&mut out, b"");
        out.push(QUERY_KIND_INITIAL);
        put_string(&mut out, b"");
        put_string(&mut out, b"");
        put_string(&mut out, b"[::ffff:127.0.0.1]:0");
        out.push(INTERFACE_TCP);
        put_string(&mut out, b"");
        put_string(&mut out, CLIENT_NAME.as_bytes());
        put_string(&mut out, CLIENT_NAME.as_bytes());
        put_uvarint(&mut out, CLIENT_VERSION_MAJOR);
        put_uvarint(&mut out, CLIENT_VERSION_MINOR);
        put_uvarint(&mut out, CLIENT_REVISION);
        put_string(&mut out, b"");
        put_uvarint(&mut out, 0);
        for (name, value) in &self.settings {
            put_string(&mut out, name.as_bytes());
            out.push(0);
            put_string(&mut out, value.as_bytes());
        }
        put_string(&mut out, b"");
        put_uvarint(&mut out, STATE_COMPLETE);
        put_uvarint(&mut out, if self.compression { COMPRESS_ENABLE } else { COMPRESS_DISABLE });
        put_string(&mut out, sql.as_bytes());
        put_uvarint(&mut out, CLIENT_DATA);
        put_string(&mut out, b"");
        let mut empty = Vec::new();
        Output::write_empty(&mut empty);
        push_block(&mut out, &empty, self.compression);
        out
    }

    /// Opens the pool; the builders must have run, and [`ClickHouse::drive`]
    /// never opens anything itself.
    pub fn connect(&mut self, net: &mut TcpNetworkCore) {
        assert!(self.group.is_none(), "connect once");
        let group = net.add_group(TcpGroupConfig {
            name: "clickhouse",
            framing: Framing::Raw,
            max_frame_size: usize::MAX,
            on_connect_msg: Some(self.hello_bytes()),
            ..Default::default()
        });
        self.group = Some(group);
        for _ in 0..self.connections {
            let token = net.connect(group, self.addr);
            self.conns.push(Conn {
                token,
                state: State::Connecting,
                rx: Vec::new(),
                rx_start: 0,
                data_resume: None,
                outbox: Vec::new(),
            });
        }
    }

    /// Sends queued requests on idle connections, then delivers each finished
    /// request's outcome to `handler` exactly once.
    pub fn drive<F>(&mut self, net: &mut TcpNetworkCore, mut handler: F)
    where
        F: FnMut(QueryId, Result<Output, Error>),
    {
        assert!(self.group.is_some(), "connect before drive");
        if let Some(timeout) = self.request_timeout {
            let now = Instant::now();
            for index in 0..self.conns.len() {
                let expired = matches!(
                    &self.conns[index].state,
                    State::Busy(flight) if now.duration_since(flight.started) >= timeout
                );
                if expired {
                    if let Some(outcome) = self.conns[index].fatal(Error::Timeout) {
                        self.outcomes.push(outcome);
                    }
                }
            }
        }
        for conn in &mut self.conns {
            if matches!(conn.state, State::Dead) {
                net.disconnect(conn.token);
            } else if !conn.outbox.is_empty() &&
                net.send_with(conn.token, |buf| buf.extend_from_slice(&conn.outbox))
            {
                conn.outbox.clear();
            }
        }
        for index in 0..self.conns.len() {
            if !matches!(self.conns[index].state, State::Ready) {
                continue;
            }
            let Some(request) = self.pop_queued() else { break };
            let bytes = self.query_bytes(&request.sql);
            let token = self.conns[index].token;
            if !net.send_with(token, |buf| buf.extend_from_slice(&bytes)) {
                self.queued_bytes += request.queued_len();
                self.queue.push_front(request);
                continue;
            }
            self.conns[index].state = State::Busy(InFlight {
                id: request.id,
                data: request.data,
                output: Output::default(),
                bytes: 0,
                compressed: self.compression,
                started: Instant::now(),
            });
        }
        for (id, outcome) in self.outcomes.drain(..) {
            handler(id, outcome);
        }
    }

    /// Removes every pooled endpoint; queued and in-flight requests are
    /// dropped without outcomes.
    pub fn close(self, net: &mut TcpNetworkCore) {
        for conn in &self.conns {
            net.remove(conn.token);
        }
    }

    fn conn_index(&self, group: TcpGroup, token: Token) -> Option<usize> {
        if self.group != Some(group) {
            return None;
        }
        self.conns.iter().position(|conn| conn.token == token)
    }

    /// Returns whether the event belonged to this client. Outcomes queue
    /// inside and are delivered by [`ClickHouse::drive`].
    pub fn on_event(&mut self, event: &TcpEvent<'_>) -> bool {
        match *event {
            TcpEvent::Accepted { .. } => false,
            TcpEvent::Connected { group, token, .. } |
            TcpEvent::Disconnected { group, token, .. } => {
                let Some(index) = self.conn_index(group, token) else { return false };
                let conn = &mut self.conns[index];
                if let Some(id) = conn.in_flight() {
                    self.outcomes.push((id, Err(Error::Disconnected)));
                }
                conn.reset(if matches!(event, TcpEvent::Connected { .. }) {
                    State::Hello
                } else {
                    State::Connecting
                });
                true
            }
            TcpEvent::Message { group, token, payload, .. } => {
                let Some(index) = self.conn_index(group, token) else { return false };
                let max_output_bytes = self.max_output_bytes;
                let conn = &mut self.conns[index];
                if matches!(conn.state, State::Dead) {
                    return true;
                }
                conn.rx.extend_from_slice(payload);
                let compressed = matches!(&conn.state, State::Busy(flight) if flight.compressed);
                let mut outcome = None;
                if conn.rx.len() - conn.rx_start > max_output_bytes {
                    outcome = conn.fatal(Error::TooLarge);
                    conn.rx.clear();
                    conn.rx_start = 0;
                } else {
                    let mut rx = std::mem::take(&mut conn.rx);
                    let mut start = std::mem::take(&mut conn.rx_start);
                    while outcome.is_none() && !matches!(conn.state, State::Dead) {
                        // Native packets carry no length, so a partial packet
                        // is attempted again once more bytes land; a block
                        // resumes after its decoded columns instead of
                        // re-decoding them.
                        match Packet::read(
                            &rx[start..],
                            compressed,
                            max_output_bytes,
                            &mut conn.data_resume,
                        ) {
                            Ok(Some((size, packet))) => {
                                start += size;
                                outcome = conn.on_packet(packet, max_output_bytes);
                            }
                            Ok(None) => break,
                            Err(error) => outcome = conn.fatal(error),
                        }
                    }
                    if !matches!(conn.state, State::Dead) {
                        if start == rx.len() {
                            rx.clear();
                            start = 0;
                        } else if start >= RX_COMPACT_AT {
                            rx.drain(..start);
                            start = 0;
                        }
                        conn.rx = rx;
                        conn.rx_start = start;
                    }
                }
                if let Some(outcome) = outcome {
                    self.outcomes.push(outcome);
                }
                true
            }
        }
    }
}
