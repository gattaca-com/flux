//! Poll-driven `Postgres` client over a caller-owned [`TcpNetwork`].
//!
//! `query` and `copy` take an idle pooled connection and return a
//! [`QueryId`], or `None` when none is idle: nothing is queued, retry after
//! the next poll. Call [`Postgres::on_event`] from the network's `poll_with`
//! handler and [`Postgres::flush`] after polling.
//!
//! Authentication covers trust, cleartext, MD5, and SCRAM-SHA-256; anything
//! else fails the connection without an outcome. A connection lost
//! mid-request reports [`Error::Disconnected`] and reconnects on its own.

pub mod copybinary;
mod scram;

use std::net::SocketAddr;

use flux_network::{
    Token,
    tcp::{Framing, TcpEvent, TcpGroup, TcpGroupConfig, TcpNetwork},
};
use md5::Digest as _;
use rand::Rng as _;
use tracing::warn;

const PROTOCOL_VERSION: i32 = 196_608;
const SCRAM_MECHANISM: &[u8] = b"SCRAM-SHA-256";
const NONCE_ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const SEND_CHUNK_BYTES: usize = 8 << 20;
const DEFAULT_MAX_OUTPUT_BYTES: usize = 16 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueryId(u64);

#[derive(Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub oid: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Output {
    pub tag: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// The server refused or aborted the request: SQLSTATE code and message.
    Server { code: String, message: String },
    /// Lost before a response; the server may or may not have run the request.
    Disconnected,
    /// The server asked for an authentication method this client does not
    /// speak.
    UnsupportedAuth(String),
    /// The server's reply exceeded `max_output_bytes`.
    TooLarge,
    /// A message arrived that fits no legal state.
    Protocol(&'static str),
}

type Outcome = (QueryId, Result<Output, Error>);

struct ServerError {
    code: String,
    message: String,
}

struct QueryOut {
    id: QueryId,
    columns: Vec<Column>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    tag: Option<String>,
    error: Option<ServerError>,
    bytes: usize,
}

struct CopyOut {
    id: QueryId,
    data: Vec<u8>,
    sent: bool,
    tag: Option<String>,
    error: Option<ServerError>,
}

enum Startup {
    AwaitAuth,
    Sasl(scram::Exchange),
    AwaitReady,
}

enum State {
    Connecting,
    Startup(Startup),
    Ready,
    Query(QueryOut),
    Copy(CopyOut),
    Dead,
}

struct Conn {
    token: Token,
    state: State,
    rx: Vec<u8>,
    outbox: Vec<u8>,
    out_sent: usize,
}

impl Conn {
    fn reset(&mut self, state: State) {
        self.state = state;
        self.rx.clear();
        self.outbox.clear();
        self.out_sent = 0;
    }

    fn take_in_flight(&self) -> Option<QueryId> {
        match &self.state {
            State::Query(query) => Some(query.id),
            State::Copy(copy) => Some(copy.id),
            _ => None,
        }
    }

    fn fatal(&mut self, error: Error) -> Option<Outcome> {
        let id = self.take_in_flight();
        self.state = State::Dead;
        id.map(|id| (id, Err(error)))
    }

    fn dispatch(
        &mut self,
        user: &str,
        password: &str,
        max_output: usize,
        tag: u8,
        body: &[u8],
    ) -> Option<Outcome> {
        if matches!(tag, b'A' | b'K' | b'N' | b'S') {
            if matches!(self.state, State::Connecting) {
                return self.fatal(Error::Protocol("message arrived before connect completed"));
            }
            return None;
        }
        match Postgres::dispatch_inner(
            &mut self.state,
            &mut self.outbox,
            user,
            password,
            max_output,
            tag,
            body,
        ) {
            Ok(outcome) => outcome,
            Err(error) => self.fatal(error),
        }
    }
}

#[derive(Clone, Copy)]
struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn u8(&mut self) -> Option<u8> {
        let (first, rest) = self.0.split_first()?;
        self.0 = rest;
        Some(*first)
    }

    fn i16(&mut self) -> Option<i16> {
        let bytes = self.bytes(2)?;
        Some(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn i32(&mut self) -> Option<i32> {
        let bytes = self.bytes(4)?;
        Some(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.0.len() < len {
            return None;
        }
        let (head, rest) = self.0.split_at(len);
        self.0 = rest;
        Some(head)
    }

    fn cstring(&mut self) -> Option<&'a [u8]> {
        let end = self.0.iter().position(|byte| *byte == 0)?;
        let value = &self.0[..end];
        self.0 = &self.0[end + 1..];
        Some(value)
    }

    fn rest(self) -> &'a [u8] {
        self.0
    }
}

pub struct Postgres {
    addr: SocketAddr,
    user: String,
    password: String,
    database: String,
    params: Vec<(String, String)>,
    connections: usize,
    max_output_bytes: usize,
    group: Option<TcpGroup>,
    conns: Vec<Conn>,
    next_id: u64,
}

impl Postgres {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            user: "postgres".to_owned(),
            password: String::new(),
            database: String::new(),
            params: Vec::new(),
            connections: 1,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            group: None,
            conns: Vec::new(),
            next_id: 0,
        }
    }

    pub fn with_credentials(mut self, user: &str, password: &str) -> Self {
        user.clone_into(&mut self.user);
        password.clone_into(&mut self.password);
        self
    }

    pub fn with_database(mut self, database: &str) -> Self {
        database.clone_into(&mut self.database);
        self
    }

    /// Sets a startup parameter such as `application_name`.
    pub fn with_parameter(mut self, name: &str, value: &str) -> Self {
        match self.params.iter_mut().find(|(n, _)| n == name) {
            Some((_, v)) => value.clone_into(v),
            None => self.params.push((name.to_owned(), value.to_owned())),
        }
        self
    }

    pub fn with_connections(mut self, connections: usize) -> Self {
        assert!(connections > 0 && self.conns.is_empty(), "nonzero, before the first send");
        self.connections = connections;
        self
    }

    pub fn with_max_output_bytes(mut self, max_output_bytes: usize) -> Self {
        assert!(max_output_bytes > 0, "max_output_bytes must be nonzero");
        self.max_output_bytes = max_output_bytes;
        self
    }

    /// Runs `sql` as one simple-protocol statement.
    pub fn query(&mut self, net: &mut TcpNetwork, sql: &str) -> Option<QueryId> {
        let index = self.ready_conn(net)?;
        let id = self.send_request(net, index, sql, None)?;
        Some(id)
    }

    /// Streams `data` as the body of a `COPY ... FROM STDIN` statement built
    /// by [`copybinary::copy_statement`]. The body is copied; keep your own
    /// for retries after [`Error::Disconnected`].
    pub fn copy(&mut self, net: &mut TcpNetwork, sql: &str, data: &[u8]) -> Option<QueryId> {
        let index = self.ready_conn(net)?;
        let id = self.send_request(net, index, sql, Some(data))?;
        Some(id)
    }

    fn ready_conn(&mut self, net: &mut TcpNetwork) -> Option<usize> {
        if self.conns.is_empty() {
            let group = *self.group.get_or_insert_with(|| {
                net.add_group(TcpGroupConfig {
                    name: "postgres",
                    framing: Framing::Raw,
                    ..Default::default()
                })
            });
            for _ in 0..self.connections {
                let token = net.connect(group, self.addr);
                self.conns.push(Conn {
                    token,
                    state: State::Connecting,
                    rx: Vec::new(),
                    outbox: Vec::new(),
                    out_sent: 0,
                });
            }
        }
        self.conns.iter().position(|conn| matches!(conn.state, State::Ready))
    }

    fn send_request(
        &mut self,
        net: &mut TcpNetwork,
        index: usize,
        sql: &str,
        data: Option<&[u8]>,
    ) -> Option<QueryId> {
        let prev = self.conns[index].outbox.len();
        Self::queue_cstring(&mut self.conns[index].outbox, b'Q', sql.as_bytes());
        if !self.send_pending(net, index) {
            let conn = &mut self.conns[index];
            conn.outbox.truncate(prev);
            conn.out_sent = conn.out_sent.min(prev);
            return None;
        }
        let id = QueryId(self.next_id);
        self.next_id += 1;
        self.conns[index].state = data.map_or_else(
            || {
                State::Query(QueryOut {
                    id,
                    columns: Vec::new(),
                    rows: Vec::new(),
                    tag: None,
                    error: None,
                    bytes: 0,
                })
            },
            |data| {
                State::Copy(CopyOut {
                    id,
                    data: data.to_vec(),
                    sent: false,
                    tag: None,
                    error: None,
                })
            },
        );
        Some(id)
    }

    fn send_pending(&mut self, net: &mut TcpNetwork, index: usize) -> bool {
        let token = self.conns[index].token;
        loop {
            let conn = &self.conns[index];
            if conn.out_sent >= conn.outbox.len() {
                break;
            }
            let start = conn.out_sent;
            let end = (start + SEND_CHUNK_BYTES).min(conn.outbox.len());
            let outbox = &self.conns[index].outbox;
            if !net.send_with(token, |buf| buf.extend_from_slice(&outbox[start..end])) {
                return false;
            }
            self.conns[index].out_sent = end;
        }
        let conn = &mut self.conns[index];
        conn.outbox.clear();
        conn.out_sent = 0;
        true
    }

    /// Sends queued handshake and COPY bytes, which cannot be sent from inside
    /// the poll handler; call after every poll.
    pub fn flush(&mut self, net: &mut TcpNetwork) {
        for index in 0..self.conns.len() {
            if matches!(self.conns[index].state, State::Dead) {
                net.disconnect(self.conns[index].token);
            } else {
                self.send_pending(net, index);
            }
        }
    }

    fn conn_index(&self, group: TcpGroup, token: Token) -> Option<usize> {
        if self.group != Some(group) {
            return None;
        }
        self.conns.iter().position(|conn| conn.token == token)
    }

    /// Returns whether the event belonged to this client; a completed
    /// request's outcome is delivered to `handler` exactly once.
    pub fn on_event<F>(&mut self, event: &TcpEvent, handler: F) -> bool
    where
        F: FnOnce(QueryId, Result<Output, Error>),
    {
        match *event {
            TcpEvent::Accepted { .. } => false,
            TcpEvent::Connected { group, token, .. } => {
                let Some(index) = self.conn_index(group, token) else { return false };
                let Self { conns, user, database, params, .. } = self;
                let conn = &mut conns[index];
                let stale = conn.take_in_flight();
                conn.reset(State::Startup(Startup::AwaitAuth));
                Self::queue_startup(&mut conn.outbox, user, database, params);
                if let Some(id) = stale {
                    handler(id, Err(Error::Disconnected));
                }
                true
            }
            TcpEvent::Disconnected { group, token, .. } => {
                let Some(index) = self.conn_index(group, token) else { return false };
                let conn = &mut self.conns[index];
                let in_flight = conn.take_in_flight();
                conn.reset(State::Connecting);
                if let Some(id) = in_flight {
                    handler(id, Err(Error::Disconnected));
                }
                true
            }
            TcpEvent::Message { group, token, payload, .. } => {
                let Some(index) = self.conn_index(group, token) else { return false };
                let Self { conns, user, password, max_output_bytes, .. } = self;
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
                        if rx.len() - consumed < 5 {
                            break;
                        }
                        let len = u32::from_be_bytes([
                            rx[consumed + 1],
                            rx[consumed + 2],
                            rx[consumed + 3],
                            rx[consumed + 4],
                        ]) as usize;
                        if len < 4 {
                            outcome = conn.fatal(Error::Protocol("message length below 4"));
                            break;
                        }
                        if rx.len() - consumed < 1 + len {
                            break;
                        }
                        let tag = rx[consumed];
                        let body = &rx[consumed + 5..consumed + 1 + len];
                        consumed += 1 + len;
                        outcome = conn.dispatch(user, password, max_output_bytes, tag, body);
                    }
                    if !matches!(conn.state, State::Dead) {
                        rx.drain(..consumed);
                        conn.rx = rx;
                    }
                }
                if let Some((id, result)) = outcome {
                    handler(id, result);
                }
                true
            }
        }
    }

    fn dispatch_inner(
        state: &mut State,
        outbox: &mut Vec<u8>,
        user: &str,
        password: &str,
        max_output: usize,
        tag: u8,
        body: &[u8],
    ) -> Result<Option<Outcome>, Error> {
        match state {
            State::Connecting => Err(Error::Protocol("message arrived before connect completed")),
            State::Dead => Ok(None),
            State::Ready => match tag {
                b'Z' => Ok(None),
                b'E' => {
                    let error = Self::parse_error(body);
                    warn!(code = %error.code, message = %error.message, "postgres errored while idle");
                    Err(Error::Protocol("server error with no query in flight"))
                }
                _ => Err(Error::Protocol("message arrived with no query in flight")),
            },
            State::Startup(startup) => {
                if Self::startup(startup, outbox, user, password, tag, body)? {
                    *state = State::Ready;
                }
                Ok(None)
            }
            State::Query(query) => match Self::query_msg(query, max_output, tag, body)? {
                Some(result) => {
                    let id = query.id;
                    *state = State::Ready;
                    Ok(Some((id, result)))
                }
                None => Ok(None),
            },
            State::Copy(copy) => match Self::copy_msg(copy, outbox, tag, body)? {
                Some(result) => {
                    let id = copy.id;
                    *state = State::Ready;
                    Ok(Some((id, result)))
                }
                None => Ok(None),
            },
        }
    }

    fn startup(
        startup: &mut Startup,
        outbox: &mut Vec<u8>,
        user: &str,
        password: &str,
        tag: u8,
        body: &[u8],
    ) -> Result<bool, Error> {
        match tag {
            b'R' => Self::auth(startup, outbox, user, password, body),
            b'E' => {
                let error = Self::parse_error(body);
                warn!(code = %error.code, message = %error.message, "postgres rejected the connection");
                Err(Error::Server { code: error.code, message: error.message })
            }
            b'Z' => match startup {
                Startup::AwaitReady => Ok(true),
                _ => Err(Error::Protocol("ready arrived before authentication completed")),
            },
            b'T' | b'D' | b'C' | b'I' => {
                Err(Error::Protocol("query results arrived during startup"))
            }
            b'G' | b'H' | b'W' | b'c' | b'd' => {
                Err(Error::Protocol("COPY message arrived during startup"))
            }
            _ => Ok(false),
        }
    }

    fn auth(
        startup: &mut Startup,
        outbox: &mut Vec<u8>,
        user: &str,
        password: &str,
        body: &[u8],
    ) -> Result<bool, Error> {
        let mut cursor = Cursor(body);
        let method = cursor.i32().ok_or(Error::Protocol("truncated authentication message"))?;
        match startup {
            Startup::AwaitAuth => match method {
                0 => {
                    *startup = Startup::AwaitReady;
                    Ok(false)
                }
                3 => {
                    Self::queue_cstring(outbox, b'p', password.as_bytes());
                    Ok(false)
                }
                5 => {
                    let salt = cursor.bytes(4).ok_or(Error::Protocol("truncated MD5 salt"))?;
                    Self::queue_cstring(
                        outbox,
                        b'p',
                        Self::md5_password(password, user, salt).as_bytes(),
                    );
                    Ok(false)
                }
                10 => {
                    let mut mechanisms = Vec::new();
                    let mut scram = false;
                    while let Some(name) = cursor.cstring() {
                        if name.is_empty() {
                            break;
                        }
                        scram |= name == SCRAM_MECHANISM;
                        mechanisms.push(String::from_utf8_lossy(name).into_owned());
                    }
                    if !scram {
                        warn!(
                            mechanisms = mechanisms.join(","),
                            "postgres offered no supported SASL mechanism"
                        );
                        return Err(Error::UnsupportedAuth(format!(
                            "SASL({})",
                            mechanisms.join(",")
                        )));
                    }
                    let cnonce: String = {
                        let mut rng = rand::rng();
                        (0..24)
                            .map(|_| {
                                NONCE_ALPHABET[rng.random_range(0..NONCE_ALPHABET.len())] as char
                            })
                            .collect()
                    };
                    let (exchange, response) = scram::Exchange::start(user, &cnonce);
                    Self::queue_raw(outbox, b'p', &response);
                    *startup = Startup::Sasl(exchange);
                    Ok(false)
                }
                _ => {
                    let name = match method {
                        2 => "KerberosV5".to_owned(),
                        6 => "SCM credential".to_owned(),
                        7 => "GSS".to_owned(),
                        8 => "GSS continue".to_owned(),
                        9 => "SSPI".to_owned(),
                        _ => format!("authentication type {method}"),
                    };
                    warn!(method, "postgres asked for an unsupported authentication method");
                    Err(Error::UnsupportedAuth(name))
                }
            },
            Startup::Sasl(exchange) => match method {
                11 => {
                    let response =
                        exchange.client_final(password, cursor.rest()).map_err(|message| {
                            warn!(%message, "SCRAM exchange failed");
                            Error::Protocol(message)
                        })?;
                    Self::queue_raw(outbox, b'p', &response);
                    Ok(false)
                }
                12 => {
                    exchange.verify_server_final(cursor.rest()).map_err(|message| {
                        warn!(%message, "SCRAM exchange failed");
                        Error::Protocol(message)
                    })?;
                    *startup = Startup::AwaitReady;
                    Ok(false)
                }
                _ => Err(Error::Protocol("authentication message out of sequence")),
            },
            Startup::AwaitReady => match method {
                0 => Ok(false),
                _ => Err(Error::Protocol("authentication message out of sequence")),
            },
        }
    }

    fn md5_password(password: &str, user: &str, salt: &[u8]) -> String {
        let inner = format!("{:x}", md5::Md5::digest(format!("{password}{user}")));
        let mut outer = md5::Md5::new();
        outer.update(inner.as_bytes());
        outer.update(salt);
        format!("md5{:x}", outer.finalize())
    }

    fn query_msg(
        query: &mut QueryOut,
        max_output: usize,
        tag: u8,
        body: &[u8],
    ) -> Result<Option<Result<Output, Error>>, Error> {
        match tag {
            b'T' => {
                let columns = Self::parse_columns(body)?;
                query.bytes += body.len();
                if query.bytes > max_output {
                    return Err(Error::TooLarge);
                }
                if query.tag.is_some() {
                    query.columns.clear();
                    query.rows.clear();
                    query.tag = None;
                }
                query.columns = columns;
                Ok(None)
            }
            b'D' => {
                let row = Self::parse_row(body)?;
                query.bytes += body.len();
                if query.bytes > max_output {
                    return Err(Error::TooLarge);
                }
                query.rows.push(row);
                Ok(None)
            }
            b'C' => {
                query.tag = Some(Self::command_tag(body)?);
                Ok(None)
            }
            b'I' => {
                if query.tag.is_none() {
                    query.tag = Some(String::new());
                }
                Ok(None)
            }
            b'E' => {
                query.error = Some(Self::parse_error(body));
                Ok(None)
            }
            b'Z' => {
                let result = match query.error.take() {
                    Some(error) => Err(Error::Server { code: error.code, message: error.message }),
                    None => Ok(Output {
                        tag: query.tag.take().unwrap_or_default(),
                        columns: std::mem::take(&mut query.columns),
                        rows: std::mem::take(&mut query.rows),
                    }),
                };
                Ok(Some(result))
            }
            b'R' => Err(Error::Protocol("authentication message outside startup")),
            b'G' => Err(Error::Protocol("COPY FROM STDIN needs copy(), not query()")),
            b'H' | b'W' | b'c' | b'd' => Err(Error::Protocol("COPY TO STDOUT is not supported")),
            _ => Ok(None),
        }
    }

    fn copy_msg(
        copy: &mut CopyOut,
        outbox: &mut Vec<u8>,
        tag: u8,
        body: &[u8],
    ) -> Result<Option<Result<Output, Error>>, Error> {
        match tag {
            b'G' => {
                if copy.sent {
                    return Err(Error::Protocol("second COPY FROM STDIN in one copy()"));
                }
                copy.sent = true;
                Self::queue_raw(outbox, b'd', &copy.data);
                Self::queue_raw(outbox, b'c', &[]);
                Ok(None)
            }
            b'C' => {
                copy.tag = Some(Self::command_tag(body)?);
                Ok(None)
            }
            b'E' => {
                copy.error = Some(Self::parse_error(body));
                Ok(None)
            }
            b'Z' => {
                let result = match copy.error.take() {
                    Some(error) => Err(Error::Server { code: error.code, message: error.message }),
                    None => Ok(Output {
                        tag: copy.tag.take().unwrap_or_default(),
                        columns: Vec::new(),
                        rows: Vec::new(),
                    }),
                };
                Ok(Some(result))
            }
            b'R' => Err(Error::Protocol("authentication message outside startup")),
            b'T' | b'D' | b'I' => Err(Error::Protocol("query results arrived in COPY")),
            b'H' | b'W' | b'c' | b'd' => Err(Error::Protocol("COPY TO STDOUT is not supported")),
            _ => Ok(None),
        }
    }

    fn parse_columns(body: &[u8]) -> Result<Vec<Column>, Error> {
        let mut cursor = Cursor(body);
        let count = cursor.i16().ok_or(Error::Protocol("truncated row description"))?;
        if count < 0 {
            return Err(Error::Protocol("negative row description count"));
        }
        let mut columns = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let name = cursor.cstring().ok_or(Error::Protocol("truncated row description"))?;
            cursor.i32().ok_or(Error::Protocol("truncated row description"))?;
            cursor.i16().ok_or(Error::Protocol("truncated row description"))?;
            let oid = cursor.i32().ok_or(Error::Protocol("truncated row description"))? as u32;
            cursor.i16().ok_or(Error::Protocol("truncated row description"))?;
            cursor.i32().ok_or(Error::Protocol("truncated row description"))?;
            cursor.i16().ok_or(Error::Protocol("truncated row description"))?;
            columns.push(Column { name: String::from_utf8_lossy(name).into_owned(), oid });
        }
        Ok(columns)
    }

    fn parse_row(body: &[u8]) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let mut cursor = Cursor(body);
        let count = cursor.i16().ok_or(Error::Protocol("truncated data row"))?;
        if count < 0 {
            return Err(Error::Protocol("negative data row count"));
        }
        let mut row = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let len = cursor.i32().ok_or(Error::Protocol("truncated data row"))?;
            if len < 0 {
                row.push(None);
            } else {
                let field =
                    cursor.bytes(len as usize).ok_or(Error::Protocol("truncated data row"))?;
                row.push(Some(field.to_vec()));
            }
        }
        Ok(row)
    }

    fn command_tag(body: &[u8]) -> Result<String, Error> {
        let mut cursor = Cursor(body);
        let tag = cursor.cstring().ok_or(Error::Protocol("truncated command tag"))?;
        Ok(String::from_utf8_lossy(tag).into_owned())
    }

    fn parse_error(body: &[u8]) -> ServerError {
        let mut cursor = Cursor(body);
        let mut code = String::new();
        let mut message = String::new();
        while let Some(field) = cursor.u8() {
            if field == 0 {
                break;
            }
            let value = cursor.cstring().unwrap_or_default();
            match field {
                b'C' => code = String::from_utf8_lossy(value).into_owned(),
                b'M' => message = String::from_utf8_lossy(value).into_owned(),
                _ => {}
            }
        }
        ServerError { code, message }
    }

    fn queue_raw(outbox: &mut Vec<u8>, tag: u8, body: &[u8]) {
        outbox.push(tag);
        outbox.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        outbox.extend_from_slice(body);
    }

    fn queue_cstring(outbox: &mut Vec<u8>, tag: u8, value: &[u8]) {
        outbox.push(tag);
        outbox.extend_from_slice(&((value.len() + 5) as u32).to_be_bytes());
        outbox.extend_from_slice(value);
        outbox.push(0);
    }

    fn queue_startup(
        outbox: &mut Vec<u8>,
        user: &str,
        database: &str,
        params: &[(String, String)],
    ) {
        let mut len = 4 + 4 + "user".len() + 1 + user.len() + 1 + 1;
        if !database.is_empty() {
            len += "database".len() + 1 + database.len() + 1;
        }
        for (name, value) in params {
            len += name.len() + 1 + value.len() + 1;
        }
        outbox.extend_from_slice(&(len as u32).to_be_bytes());
        outbox.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        Self::queue_param(outbox, "user", user);
        if !database.is_empty() {
            Self::queue_param(outbox, "database", database);
        }
        for (name, value) in params {
            Self::queue_param(outbox, name, value);
        }
        outbox.push(0);
    }

    fn queue_param(outbox: &mut Vec<u8>, name: &str, value: &str) {
        outbox.extend_from_slice(name.as_bytes());
        outbox.push(0);
        outbox.extend_from_slice(value.as_bytes());
        outbox.push(0);
    }
}
