//! Poll-driven `Mongo` client over a caller-owned TCP network, sharing
//! one poll with the tile's other traffic.
//!
//! Commands queue inside the client; [`Mongo::connect`] opens the pool,
//! [`Mongo::on_event`] tracks the pooled connections from the network's
//! event handler, and [`Mongo::drive`] sends queued commands on idle ones
//! and delivers one outcome per id. A full queue refuses the new command
//! instead of failing queued ones; queued commands wait out pool outages.
//!
//! The wire protocol is `OP_MSG` only. Each connection first sends
//! `{hello: 1}` against `admin` and, when credentials are configured,
//! completes a SCRAM-SHA-256 exchange with `saslStart` / `saslContinue`
//! against the auth-source database. A failed startup kills the connection
//! without an outcome; the pool redials and queued commands wait.
//!
//! [`Mongo::find`] returns the server's raw reply; read `cursor.firstBatch`
//! from it and page with `getMore` through [`Mongo::run_command`].

mod scram;

use std::{collections::VecDeque, net::SocketAddr};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bson::{Bson, Document, doc};
use flux_network::{
    Token,
    tcp::{Framing, TcpEvent, TcpGroup, TcpGroupConfig, TcpNetworkCore},
};
use rand::Rng as _;
use serde::Serialize;
use tracing::warn;

const OP_MSG: i32 = 2013;
const HEADER_LEN: usize = 16;
const CHECKSUM_PRESENT: u32 = 1;
const SCRAM_MECHANISM: &str = "SCRAM-SHA-256";
const NONCE_ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const DEFAULT_AUTH_SOURCE: &str = "admin";
const DEFAULT_MAX_OUTPUT_BYTES: usize = 16 << 20;
const DEFAULT_MAX_QUEUED_BYTES: usize = 256 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandId(u64);

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Server {
        code: Option<i32>,
        code_name: Option<String>,
        message: String,
    },
    /// Lost before a reply; the server may or may not have run the command.
    Disconnected,
    TooLarge,
    Protocol(&'static str),
}

type Outcome = (CommandId, Result<Document, Error>);

struct Queued {
    id: CommandId,
    request_id: i32,
    bytes: Vec<u8>,
}

struct InFlight {
    id: CommandId,
    request_id: i32,
}

enum Startup {
    Hello { request_id: i32 },
    SaslStart { request_id: i32 },
    SaslContinue { request_id: i32, conversation: i32 },
}

enum State {
    Connecting,
    Startup(Startup),
    Ready,
    Busy(InFlight),
    Dead,
}

struct Conn {
    token: Token,
    state: State,
    rx: Vec<u8>,
    outbox: Vec<u8>,
    auth: Option<scram::Exchange>,
}

impl Conn {
    fn reset(&mut self, state: State) {
        self.state = state;
        self.rx.clear();
        self.outbox.clear();
        self.auth = None;
    }

    fn in_flight(&self) -> Option<CommandId> {
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
}

pub struct Mongo {
    addr: SocketAddr,
    credentials: Option<(String, String)>,
    auth_source: String,
    connections: usize,
    max_output_bytes: usize,
    max_queued_bytes: usize,
    group: Option<TcpGroup>,
    conns: Vec<Conn>,
    queue: VecDeque<Queued>,
    queued_bytes: usize,
    outcomes: Vec<Outcome>,
    next_id: u64,
    next_request_id: i32,
}

impl Mongo {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            credentials: None,
            auth_source: DEFAULT_AUTH_SOURCE.to_owned(),
            connections: 1,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            max_queued_bytes: DEFAULT_MAX_QUEUED_BYTES,
            group: None,
            conns: Vec::new(),
            queue: VecDeque::new(),
            queued_bytes: 0,
            outcomes: Vec::new(),
            next_id: 0,
            next_request_id: rand::rng().random_range(1..=i32::MAX),
        }
    }

    pub fn with_credentials(mut self, user: &str, password: &str) -> Self {
        assert!(self.group.is_none(), "configure before connect");
        self.credentials = Some((user.to_owned(), password.to_owned()));
        self
    }

    pub fn with_auth_source(mut self, auth_source: &str) -> Self {
        assert!(self.group.is_none(), "configure before connect");
        auth_source.clone_into(&mut self.auth_source);
        self
    }

    pub fn with_connections(mut self, connections: usize) -> Self {
        assert!(connections > 0 && self.group.is_none(), "nonzero, before connect");
        self.connections = connections;
        self
    }

    pub fn with_max_output_bytes(mut self, max_output_bytes: usize) -> Self {
        assert!(max_output_bytes > 0, "max_output_bytes must be nonzero");
        self.max_output_bytes = max_output_bytes;
        self
    }

    /// Bound on queued body bytes; a command that would exceed it is
    /// refused, so accepted work is never dropped.
    pub fn with_max_queued_bytes(mut self, max_queued_bytes: usize) -> Self {
        self.max_queued_bytes = max_queued_bytes;
        self
    }

    /// Queues `cmd` against `db`, appending `$db` when absent, or `None`
    /// when the queue is full.
    pub fn run_command(&mut self, db: &str, cmd: &Document) -> Option<CommandId> {
        let mut body = cmd.clone();
        if !body.contains_key("$db") {
            body.insert("$db", db);
        }
        self.queue_body(&body)
    }

    pub fn insert<T: Serialize>(&mut self, db: &str, coll: &str, rows: &[T]) -> Option<CommandId> {
        assert!(!rows.is_empty(), "insert needs at least one document");
        let documents: Vec<Bson> = rows
            .iter()
            .map(|row| bson::to_document(row).map(Bson::Document).expect("insert row"))
            .collect();
        self.queue_body(&doc! {
            "insert": coll,
            "documents": documents,
            "$db": db,
        })
    }

    /// Queues a `find` of `filter` in `db.coll`. The outcome is the server's
    /// raw reply; page it with `getMore` through [`Mongo::run_command`].
    pub fn find(&mut self, db: &str, coll: &str, filter: Document) -> Option<CommandId> {
        self.queue_body(&doc! {
            "find": coll,
            "filter": filter,
            "$db": db,
        })
    }

    fn queue_body(&mut self, body: &Document) -> Option<CommandId> {
        let request_id = Self::alloc_request_id(&mut self.next_request_id);
        let bytes = Self::encode_msg(request_id, body);
        if self.full_for(bytes.len()) {
            return None;
        }
        Some(self.enqueue(request_id, bytes))
    }

    fn enqueue(&mut self, request_id: i32, bytes: Vec<u8>) -> CommandId {
        let id = CommandId(self.next_id);
        self.next_id += 1;
        self.queued_bytes += bytes.len();
        self.queue.push_back(Queued { id, request_id, bytes });
        id
    }

    fn full_for(&self, len: usize) -> bool {
        self.queued_bytes + len > self.max_queued_bytes
    }

    fn pop_queued(&mut self) -> Option<Queued> {
        let request = self.queue.pop_front()?;
        self.queued_bytes -= request.bytes.len();
        Some(request)
    }

    fn alloc_request_id(next: &mut i32) -> i32 {
        let id = *next;
        *next = next.wrapping_add(1);
        if *next == 0 {
            *next = 1;
        }
        id
    }

    fn encode_msg(request_id: i32, body: &Document) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&0i32.to_le_bytes());
        out.extend_from_slice(&request_id.to_le_bytes());
        out.extend_from_slice(&0i32.to_le_bytes());
        out.extend_from_slice(&OP_MSG.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.push(0);
        body.to_writer(&mut out).expect("command document encodes as BSON");
        let len = out.len() as i32;
        out[0..4].copy_from_slice(&len.to_le_bytes());
        out
    }

    pub fn connect(&mut self, net: &mut TcpNetworkCore) {
        assert!(self.group.is_none(), "connect once");
        let group = net.add_group(TcpGroupConfig {
            name: "mongo",
            framing: Framing::Raw,
            max_frame_size: usize::MAX,
            ..Default::default()
        });
        self.group = Some(group);
        for _ in 0..self.connections {
            let token = net.connect(group, self.addr);
            self.conns.push(Conn {
                token,
                state: State::Connecting,
                rx: Vec::new(),
                outbox: Vec::new(),
                auth: None,
            });
        }
    }

    pub fn drive<F>(&mut self, net: &mut TcpNetworkCore, mut handler: F)
    where
        F: FnMut(CommandId, Result<Document, Error>),
    {
        assert!(self.group.is_some(), "connect before drive");
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
            let token = self.conns[index].token;
            if !net.send_with(token, |buf| buf.extend_from_slice(&request.bytes)) {
                self.queued_bytes += request.bytes.len();
                self.queue.push_front(request);
                continue;
            }
            self.conns[index].state =
                State::Busy(InFlight { id: request.id, request_id: request.request_id });
        }
        for (id, outcome) in self.outcomes.drain(..) {
            handler(id, outcome);
        }
    }

    /// Removes every pooled endpoint; queued and in-flight commands are
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
    /// inside and are delivered by [`Mongo::drive`].
    pub fn on_event(&mut self, event: &TcpEvent) -> bool {
        match *event {
            TcpEvent::Accepted { .. } => false,
            TcpEvent::Connected { group, token, .. } |
            TcpEvent::Disconnected { group, token, .. } => {
                let Some(index) = self.conn_index(group, token) else { return false };
                if let Some(id) = self.conns[index].in_flight() {
                    self.outcomes.push((id, Err(Error::Disconnected)));
                }
                if matches!(event, TcpEvent::Connected { .. }) {
                    let request_id = Self::alloc_request_id(&mut self.next_request_id);
                    let hello = doc! { "hello": 1, "$db": "admin" };
                    let conn = &mut self.conns[index];
                    conn.reset(State::Startup(Startup::Hello { request_id }));
                    conn.outbox.extend_from_slice(&Self::encode_msg(request_id, &hello));
                } else {
                    self.conns[index].reset(State::Connecting);
                }
                true
            }
            TcpEvent::Message { group, token, payload, .. } => {
                let Some(index) = self.conn_index(group, token) else { return false };
                let Self {
                    conns,
                    outcomes,
                    credentials,
                    auth_source,
                    max_output_bytes,
                    next_request_id,
                    ..
                } = self;
                let max_output_bytes = *max_output_bytes;
                let conn = &mut conns[index];
                if matches!(conn.state, State::Dead) {
                    return true;
                }
                conn.rx.extend_from_slice(payload);
                let mut outcome = None;
                if conn.rx.len() > max_output_bytes {
                    outcome = conn.fatal(Error::TooLarge);
                    conn.rx.clear();
                } else {
                    let mut rx = std::mem::take(&mut conn.rx);
                    let mut consumed = 0;
                    while outcome.is_none() && !matches!(conn.state, State::Dead) {
                        if rx.len() - consumed < HEADER_LEN {
                            break;
                        }
                        let total = i32::from_le_bytes([
                            rx[consumed],
                            rx[consumed + 1],
                            rx[consumed + 2],
                            rx[consumed + 3],
                        ]);
                        if total < HEADER_LEN as i32 {
                            outcome = conn.fatal(Error::Protocol("message length below header"));
                            break;
                        }
                        let total = total as usize;
                        if rx.len() - consumed < total {
                            break;
                        }
                        let frame = &rx[consumed..consumed + total];
                        consumed += total;
                        let login =
                            credentials.as_ref().map(|login| (login.0.as_str(), login.1.as_str()));
                        outcome = conn
                            .state
                            .on_frame(
                                &mut conn.outbox,
                                &mut conn.auth,
                                login,
                                auth_source,
                                &mut *next_request_id,
                                frame,
                            )
                            .unwrap_or_else(|error| conn.fatal(error));
                    }
                    if !matches!(conn.state, State::Dead) {
                        rx.drain(..consumed);
                        conn.rx = rx;
                    }
                }
                if let Some(outcome) = outcome {
                    outcomes.push(outcome);
                }
                true
            }
        }
    }

    fn parse_reply(frame: &[u8]) -> Result<(i32, Document), Error> {
        let opcode = i32::from_le_bytes([frame[12], frame[13], frame[14], frame[15]]);
        if opcode != OP_MSG {
            return Err(Error::Protocol("unexpected opcode"));
        }
        let response_to = i32::from_le_bytes([frame[8], frame[9], frame[10], frame[11]]);
        let flags = u32::from_le_bytes([frame[16], frame[17], frame[18], frame[19]]);
        let mut sections = &frame[HEADER_LEN + 4..];
        if flags & CHECKSUM_PRESENT != 0 {
            if sections.len() < 4 {
                return Err(Error::Protocol("truncated checksum"));
            }
            sections = &sections[..sections.len() - 4];
        }
        if sections.first() != Some(&0) {
            return Err(Error::Protocol("reply misses a body section"));
        }
        let body = &sections[1..];
        if body.len() < 4 {
            return Err(Error::Protocol("truncated reply document"));
        }
        let doc_len = i32::from_le_bytes([body[0], body[1], body[2], body[3]]);
        if doc_len < 5 || doc_len as usize > body.len() {
            return Err(Error::Protocol("truncated reply document"));
        }
        if body.len() != doc_len as usize {
            return Err(Error::Protocol("trailing bytes in reply"));
        }
        Document::from_reader(&body[..doc_len as usize])
            .map(|reply| (response_to, reply))
            .map_err(|_| Error::Protocol("invalid BSON in reply"))
    }

    fn is_ok(reply: &Document) -> bool {
        match reply.get("ok") {
            Some(Bson::Double(value)) => *value != 0.0,
            Some(Bson::Int32(value)) => *value != 0,
            Some(Bson::Int64(value)) => *value != 0,
            Some(Bson::Boolean(value)) => *value,
            _ => false,
        }
    }

    fn server_error(reply: &Document) -> Error {
        Error::Server {
            code: reply.get_i32("code").ok(),
            code_name: reply.get_str("codeName").ok().map(str::to_owned),
            message: reply.get_str("errmsg").unwrap_or("unknown server error").to_owned(),
        }
    }
}

impl State {
    fn on_frame(
        &mut self,
        outbox: &mut Vec<u8>,
        auth: &mut Option<scram::Exchange>,
        credentials: Option<(&str, &str)>,
        auth_source: &str,
        next_request_id: &mut i32,
        frame: &[u8],
    ) -> Result<Option<Outcome>, Error> {
        match self {
            Self::Connecting => Err(Error::Protocol("message arrived before connect completed")),
            Self::Dead => Ok(None),
            Self::Ready => Err(Error::Protocol("reply arrived with no command in flight")),
            Self::Startup(startup) => {
                if startup.on_frame(
                    outbox,
                    auth,
                    credentials,
                    auth_source,
                    next_request_id,
                    frame,
                )? {
                    *self = Self::Ready;
                }
                Ok(None)
            }
            Self::Busy(flight) => {
                let (response_to, reply) = Mongo::parse_reply(frame)?;
                if response_to != flight.request_id {
                    return Err(Error::Protocol("responseTo mismatch"));
                }
                let result =
                    if Mongo::is_ok(&reply) { Ok(reply) } else { Err(Mongo::server_error(&reply)) };
                let id = flight.id;
                *self = Self::Ready;
                Ok(Some((id, result)))
            }
        }
    }
}

impl Startup {
    fn on_frame(
        &mut self,
        outbox: &mut Vec<u8>,
        auth: &mut Option<scram::Exchange>,
        credentials: Option<(&str, &str)>,
        auth_source: &str,
        next_request_id: &mut i32,
        frame: &[u8],
    ) -> Result<bool, Error> {
        let next = match self {
            Self::Hello { request_id } => Self::hello(
                *request_id,
                outbox,
                auth,
                credentials,
                auth_source,
                next_request_id,
                frame,
            )?,
            Self::SaslStart { request_id } => Self::sasl_start(
                *request_id,
                outbox,
                auth,
                credentials,
                auth_source,
                next_request_id,
                frame,
            )?,
            Self::SaslContinue { request_id, conversation } => {
                Self::sasl_continue(*request_id, *conversation, auth, frame)?
            }
        };
        next.map_or(Ok(true), |state| {
            *self = state;
            Ok(false)
        })
    }

    fn hello(
        request_id: i32,
        outbox: &mut Vec<u8>,
        auth: &mut Option<scram::Exchange>,
        credentials: Option<(&str, &str)>,
        auth_source: &str,
        next_request_id: &mut i32,
        frame: &[u8],
    ) -> Result<Option<Self>, Error> {
        let (response_to, reply) = Mongo::parse_reply(frame)?;
        if response_to != request_id {
            return Err(Error::Protocol("responseTo mismatch"));
        }
        if !Mongo::is_ok(&reply) {
            warn!(reply = ?reply, "mongo rejected hello");
            return Err(Mongo::server_error(&reply));
        }
        let Some((user, _)) = credentials else { return Ok(None) };
        let cnonce: String = {
            let mut rng = rand::rng();
            (0..24)
                .map(|_| NONCE_ALPHABET[rng.random_range(0..NONCE_ALPHABET.len())] as char)
                .collect()
        };
        let (exchange, first) = scram::Exchange::start(user, &cnonce);
        let id = Mongo::alloc_request_id(next_request_id);
        let command = doc! {
            "saslStart": 1,
            "mechanism": SCRAM_MECHANISM,
            "payload": STANDARD.encode(&first),
            "autoAuthorize": 1,
            "options": doc! { "skipEmptyExchange": true },
            "$db": auth_source,
        };
        outbox.extend_from_slice(&Mongo::encode_msg(id, &command));
        *auth = Some(exchange);
        Ok(Some(Self::SaslStart { request_id: id }))
    }

    fn sasl_start(
        request_id: i32,
        outbox: &mut Vec<u8>,
        auth: &mut Option<scram::Exchange>,
        credentials: Option<(&str, &str)>,
        auth_source: &str,
        next_request_id: &mut i32,
        frame: &[u8],
    ) -> Result<Option<Self>, Error> {
        let (response_to, reply) = Mongo::parse_reply(frame)?;
        if response_to != request_id {
            return Err(Error::Protocol("responseTo mismatch"));
        }
        if !Mongo::is_ok(&reply) {
            warn!(reply = ?reply, "mongo rejected saslStart");
            return Err(Mongo::server_error(&reply));
        }
        let conversation = reply.get_i32("conversationId").map_err(|_| {
            warn!(reply = ?reply, "saslStart reply misses conversationId");
            Error::Protocol("saslStart reply misses conversationId")
        })?;
        let payload = reply.get_str("payload").map_err(|_| {
            warn!(reply = ?reply, "saslStart reply misses payload");
            Error::Protocol("saslStart reply misses payload")
        })?;
        let server_first = STANDARD
            .decode(payload)
            .map_err(|_| Error::Protocol("saslStart payload is not base64"))?;
        let Some((_, password)) = credentials else {
            return Err(Error::Protocol("server demanded authentication"));
        };
        let Some(mut exchange) = auth.take() else {
            return Err(Error::Protocol("saslStart arrived without an exchange"));
        };
        let response = exchange.client_final(password, &server_first).map_err(|message| {
            warn!(%message, "SCRAM exchange failed");
            Error::Protocol(message)
        })?;
        let id = Mongo::alloc_request_id(next_request_id);
        let command = doc! {
            "saslContinue": 1,
            "conversationId": conversation,
            "payload": STANDARD.encode(&response),
            "$db": auth_source,
        };
        outbox.extend_from_slice(&Mongo::encode_msg(id, &command));
        *auth = Some(exchange);
        Ok(Some(Self::SaslContinue { request_id: id, conversation }))
    }

    fn sasl_continue(
        request_id: i32,
        conversation: i32,
        auth: &mut Option<scram::Exchange>,
        frame: &[u8],
    ) -> Result<Option<Self>, Error> {
        let (response_to, reply) = Mongo::parse_reply(frame)?;
        if response_to != request_id {
            return Err(Error::Protocol("responseTo mismatch"));
        }
        if reply.get_i32("conversationId").ok() != Some(conversation) {
            return Err(Error::Protocol("conversationId mismatch"));
        }
        if !Mongo::is_ok(&reply) {
            warn!(reply = ?reply, "mongo rejected saslContinue");
            return Err(Mongo::server_error(&reply));
        }
        let done = reply.get_bool("done").map_err(|_| {
            warn!(reply = ?reply, "saslContinue reply misses done");
            Error::Protocol("saslContinue reply misses done")
        })?;
        if !done {
            return Err(Error::Protocol("saslContinue reply is not done"));
        }
        let payload = reply.get_str("payload").map_err(|_| {
            warn!(reply = ?reply, "saslContinue reply misses payload");
            Error::Protocol("saslContinue reply misses payload")
        })?;
        let server_final = STANDARD
            .decode(payload)
            .map_err(|_| Error::Protocol("saslContinue payload is not base64"))?;
        let Some(exchange) = auth.as_ref() else {
            return Err(Error::Protocol("saslContinue arrived without an exchange"));
        };
        exchange.verify_server_final(&server_final).map_err(|message| {
            warn!(%message, "SCRAM exchange failed");
            Error::Protocol(message)
        })?;
        auth.take();
        Ok(None)
    }
}
