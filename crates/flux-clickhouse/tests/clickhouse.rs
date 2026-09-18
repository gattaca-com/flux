use std::{
    collections::HashMap,
    net::Ipv4Addr,
    thread,
    time::{Duration, Instant},
};

use flux_clickhouse::{ClickHouse, Column, Error, Output, QueryId};
use flux_network::{
    Token,
    tcp::{Framing, TcpEvent, TcpGroupConfig, TcpNetwork},
};
use serde::Serialize;

#[derive(Serialize)]
struct Row {
    name: String,
    count: u64,
    maybe: Option<u64>,
    tags: Vec<u8>,
    pair: (u32, String),
    attrs: HashMap<String, u64>,
    price: i64,
    flag: String,
}

const COLUMNS: [(&str, &str); 8] = [
    ("name", "String"),
    ("count", "UInt64"),
    ("maybe", "Nullable(UInt64)"),
    ("tags", "Array(UInt8)"),
    ("pair", "Tuple(UInt32, String)"),
    ("attrs", "Map(String, UInt64)"),
    ("price", "Decimal(10, 2)"),
    ("flag", "LowCardinality(String)"),
];

fn uvarint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn string(out: &mut Vec<u8>, bytes: &[u8]) {
    uvarint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// A block: info fields, the column count and row count, then each column's
/// name, type, and payload.
fn block(columns: &[(&str, &str)], rows: u64, payloads: &[Vec<u8>]) -> Vec<u8> {
    let mut out = vec![1, 0, 2, 255, 255, 255, 255, 0];
    uvarint(&mut out, columns.len() as u64);
    uvarint(&mut out, rows);
    for ((name, type_name), payload) in columns.iter().zip(payloads) {
        string(&mut out, name.as_bytes());
        string(&mut out, type_name.as_bytes());
        out.extend_from_slice(payload);
    }
    out
}

/// The same envelope the client speaks, derived independently: checksum over
/// the header and payload, then the header, then the `LZ4` payload.
fn compress_block(raw: &[u8]) -> Vec<u8> {
    let payload = lz4_flex::block::compress(raw);
    let mut envelope = Vec::with_capacity(9 + payload.len());
    envelope.push(0x82);
    envelope.extend_from_slice(&((9 + payload.len()) as u32).to_le_bytes());
    envelope.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    envelope.extend_from_slice(&payload);
    let hash = cityhash_rs::cityhash_102_128(&envelope);
    let mut out = Vec::with_capacity(16 + envelope.len());
    out.extend_from_slice(&((hash >> 64) as u64).to_le_bytes());
    out.extend_from_slice(&(hash as u64).to_le_bytes());
    out.extend_from_slice(&envelope);
    out
}

fn server_hello() -> Vec<u8> {
    let mut out = vec![0];
    string(&mut out, b"fake");
    uvarint(&mut out, 1);
    uvarint(&mut out, 1);
    uvarint(&mut out, 54_429);
    string(&mut out, b"UTC");
    string(&mut out, b"fake");
    uvarint(&mut out, 0);
    out
}

fn exception(code: i32, message: &str) -> Vec<u8> {
    let mut out = vec![2];
    out.extend_from_slice(&code.to_le_bytes());
    string(&mut out, b"DB::Exception");
    string(&mut out, message.as_bytes());
    string(&mut out, b"");
    out.push(0);
    out
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ClientMsg {
    Hello { database: String, user: String, password: String },
    Query(String, bool),
    Data(Vec<u8>),
}

/// Reader mirroring the client's encoding, which is all this fake server has
/// to understand.
struct Reader<'a> {
    input: &'a [u8],
    at: usize,
    compressed: bool,
}

impl<'a> Reader<'a> {
    fn u8(&mut self) -> Option<u8> {
        let byte = *self.input.get(self.at)?;
        self.at += 1;
        Some(byte)
    }

    fn uvarint(&mut self) -> Option<u64> {
        let mut value = 0;
        for shift in (0..64).step_by(7) {
            let byte = self.u8()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Some(value)
            }
        }
        None
    }

    fn bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(len)?;
        let slice = self.input.get(self.at..end)?;
        self.at = end;
        Some(slice)
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    fn string(&mut self) -> Option<&'a [u8]> {
        let len = self.uvarint()? as usize;
        self.bytes(len)
    }

    fn text(&mut self) -> Option<String> {
        Some(String::from_utf8_lossy(self.string()?).into_owned())
    }

    fn hello(&mut self) -> Option<ClientMsg> {
        self.string()?;
        self.uvarint()?;
        self.uvarint()?;
        self.uvarint()?;
        Some(ClientMsg::Hello {
            database: self.text()?,
            user: self.text()?,
            password: self.text()?,
        })
    }

    fn query(&mut self) -> Option<ClientMsg> {
        self.string()?;
        self.u8()?;
        for _ in 0..3 {
            self.string()?;
        }
        self.u8()?;
        for _ in 0..3 {
            self.string()?;
        }
        for _ in 0..3 {
            self.uvarint()?;
        }
        self.string()?;
        self.uvarint()?;
        while !self.string()?.is_empty() {
            self.u8()?;
            self.string()?;
        }
        self.uvarint()?;
        let compressed = self.uvarint()? != 0;
        let sql = self.text()?;
        Some(ClientMsg::Query(sql, compressed))
    }

    /// Reads one envelope the client sent, verifying its checksum.
    fn envelope(&mut self) -> Option<Vec<u8>> {
        let wire = self.bytes(16)?;
        assert_eq!(self.u8()?, 0x82);
        let compressed = u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()) as usize;
        let original = u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()) as usize;
        let payload = self.bytes(compressed.checked_sub(9).unwrap())?;
        let mut envelope = Vec::with_capacity(compressed);
        envelope.push(0x82);
        envelope.extend_from_slice(&(compressed as u32).to_le_bytes());
        envelope.extend_from_slice(&(original as u32).to_le_bytes());
        envelope.extend_from_slice(payload);
        let hash = cityhash_rs::cityhash_102_128(&envelope);
        let mut expected = [0; 16];
        expected[..8].copy_from_slice(&((hash >> 64) as u64).to_le_bytes());
        expected[8..].copy_from_slice(&(hash as u64).to_le_bytes());
        assert_eq!(wire, expected);
        Some(lz4_flex::block::decompress(payload, original).unwrap())
    }

    /// Reads a block, returning its bytes; the types are the ones this server
    /// named in its own header block.
    fn block(&mut self) -> Option<Vec<u8>> {
        if self.compressed {
            let raw = self.envelope()?;
            let mut inner = Reader { input: &raw, at: 0, compressed: false };
            inner.raw_block()?;
            assert_eq!(inner.at, raw.len());
            Some(raw)
        } else {
            let start = self.at;
            self.raw_block()?;
            Some(self.input[start..self.at].to_vec())
        }
    }

    fn raw_block(&mut self) -> Option<()> {
        loop {
            match self.uvarint()? {
                0 => break,
                1 => self.bytes(1)?,
                2 => self.bytes(4)?,
                other => panic!("unknown block info field {other}"),
            };
        }
        let columns = self.uvarint()?;
        let rows = self.uvarint()? as usize;
        for _ in 0..columns {
            self.string()?;
            let type_name = self.text()?;
            self.column(&type_name, rows)?;
        }
        Some(())
    }

    fn column(&mut self, type_name: &str, rows: usize) -> Option<()> {
        match type_name {
            "String" => {
                for _ in 0..rows {
                    self.string()?;
                }
            }
            "UInt64" | "Decimal(10, 2)" => {
                self.bytes(8 * rows)?;
            }
            "Nullable(UInt64)" => {
                self.bytes(rows)?;
                self.bytes(8 * rows)?;
            }
            "Array(UInt8)" => {
                let offsets = self.bytes(8 * rows)?;
                let total = offsets
                    .rchunks(8)
                    .next()
                    .map_or(0, |end| u64::from_le_bytes(end.try_into().unwrap()) as usize);
                self.bytes(total)?;
            }
            "Tuple(UInt32, String)" => {
                self.bytes(4 * rows)?;
                for _ in 0..rows {
                    self.string()?;
                }
            }
            "Map(String, UInt64)" => {
                let offsets = self.bytes(8 * rows)?;
                let total = offsets
                    .rchunks(8)
                    .next()
                    .map_or(0, |end| u64::from_le_bytes(end.try_into().unwrap()) as usize);
                for _ in 0..total {
                    self.string()?;
                }
                self.bytes(8 * total)?;
            }
            "LowCardinality(String)" => {
                if rows == 0 {
                    return Some(());
                }
                assert_eq!(self.u64()?, 1);
                let width = match self.u64()? & 0xff {
                    0 => 1,
                    1 => 2,
                    2 => 4,
                    3 => 8,
                    other => panic!("unknown low cardinality key width {other}"),
                };
                let dictionary = self.u64()? as usize;
                for _ in 0..dictionary {
                    self.string()?;
                }
                let keys = self.u64()? as usize;
                assert_eq!(keys, rows);
                self.bytes(keys * width)?;
            }
            other => panic!("the fake server cannot read {other}"),
        }
        Some(())
    }
}

struct ServerConn {
    token: Token,
    input: Vec<u8>,
    greeted: bool,
    compressed: bool,
}

struct FakeServer {
    conns: Vec<ServerConn>,
    compressed: bool,
}

impl FakeServer {
    fn data(&self, body: &[u8]) -> Vec<u8> {
        let mut out = vec![1, 0];
        if self.compressed {
            out.extend_from_slice(&compress_block(body));
        } else {
            out.extend_from_slice(body);
        }
        out
    }

    /// Splits everything the client sent into whole messages.
    fn push(&mut self, token: Token, payload: &[u8]) -> Vec<ClientMsg> {
        let conn = self.conns.iter_mut().find(|conn| conn.token == token).unwrap();
        conn.input.extend_from_slice(payload);
        let mut msgs = Vec::new();
        let mut reader = Reader { input: &conn.input, at: 0, compressed: conn.compressed };
        let mut consumed = 0;
        loop {
            let msg = match reader.uvarint() {
                Some(0) if !conn.greeted => reader.hello(),
                Some(1) => reader.query(),
                Some(2) => reader.string().and_then(|_| reader.block()).map(ClientMsg::Data),
                Some(other) => panic!("unknown client packet {other}"),
                None => None,
            };
            let Some(msg) = msg else { break };
            if let ClientMsg::Query(_, compressed) = &msg {
                conn.compressed = *compressed;
                reader.compressed = *compressed;
            }
            conn.greeted = true;
            consumed = reader.at;
            msgs.push(msg);
        }
        conn.input.drain(..consumed);
        msgs
    }
}

fn listen() -> (TcpNetwork, std::net::SocketAddr) {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    (TcpNetwork::default(), addr)
}

#[test]
#[allow(clippy::too_many_lines)]
fn inserts_queries_and_errors_over_one_poll() {
    let (mut net, addr) = listen();
    let server_group = net.add_group(TcpGroupConfig {
        name: "fake-clickhouse",
        framing: Framing::Raw,
        ..Default::default()
    });
    net.listen(server_group, addr).unwrap();
    let mut server = FakeServer { conns: Vec::new(), compressed: true };
    let mut ch = ClickHouse::new(addr, 2)
        .with_credentials("w", "s")
        .with_database("db")
        .with_setting("max_execution_time", "5")
        .with_max_queued_bytes(4096)
        .with_compression();

    ch.connect(&mut net);
    let row = Row {
        name: "ab".to_owned(),
        count: 5,
        maybe: Some(7),
        tags: vec![9, 8],
        pair: (3, "xy".to_owned()),
        attrs: HashMap::from([("k".to_owned(), 11)]),
        price: 12_345,
        flag: "low".to_owned(),
    };
    let insert = ch.insert_rows("t", &[row]).unwrap();
    let select = ch.query("SELECT one, note, pair, attrs, price, flag FROM t").unwrap();
    let bad = ch.query("SELEC").unwrap();
    assert_eq!(ch.query(&"x".repeat(4096)), None);

    let mut seen = Vec::new();
    let mut outcomes: Vec<(QueryId, Result<Output, Error>)> = Vec::new();
    let mut recovered = None;
    let mut inserting = None;
    let hello = ClientMsg::Hello {
        database: "db".to_owned(),
        user: "w".to_owned(),
        password: "s".to_owned(),
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    // Both pooled connections greet, and the one the exception killed greets
    // again once it has reconnected.
    while Instant::now() < deadline &&
        (outcomes.len() < 4 || seen.iter().filter(|msg| **msg == hello).count() < 3)
    {
        let mut replies = Vec::new();
        net.poll_with(|event| {
            if ch.on_event(&event) {
                return;
            }
            match event {
                TcpEvent::Accepted { group, token, .. } if group == server_group => {
                    server.conns.push(ServerConn {
                        token,
                        input: Vec::new(),
                        greeted: false,
                        compressed: false,
                    });
                }
                TcpEvent::Message { group, token, payload, .. } if group == server_group => {
                    for msg in server.push(token, payload) {
                        let reply = match &msg {
                            ClientMsg::Hello { .. } => server_hello(),
                            // An insert is answered with the header block that
                            // names the target columns; its data follows.
                            ClientMsg::Query(sql, _) if sql.starts_with("INSERT") => {
                                inserting = Some(token);
                                server.data(&block(&COLUMNS, 0, &vec![Vec::new(); 8]))
                            }
                            ClientMsg::Query(sql, _)
                                if sql.starts_with("SELEC ") || sql == "SELEC" =>
                            {
                                exception(62, "Syntax error")
                            }
                            ClientMsg::Query(_, _) => {
                                let mut note = vec![1, 0, 0];
                                string(&mut note, b"hi");
                                let mut pair = Vec::new();
                                pair.extend_from_slice(&10u32.to_le_bytes());
                                pair.extend_from_slice(&20u32.to_le_bytes());
                                string(&mut pair, b"a");
                                string(&mut pair, b"bb");
                                let mut attrs = Vec::new();
                                attrs.extend_from_slice(&1u64.to_le_bytes());
                                attrs.extend_from_slice(&3u64.to_le_bytes());
                                string(&mut attrs, b"a");
                                string(&mut attrs, b"b");
                                string(&mut attrs, b"c");
                                attrs.extend_from_slice(&1u64.to_le_bytes());
                                attrs.extend_from_slice(&2u64.to_le_bytes());
                                attrs.extend_from_slice(&3u64.to_le_bytes());
                                let mut flag = Vec::new();
                                flag.extend_from_slice(&1u64.to_le_bytes());
                                flag.extend_from_slice(&0u64.to_le_bytes());
                                flag.extend_from_slice(&2u64.to_le_bytes());
                                string(&mut flag, b"x");
                                string(&mut flag, b"y");
                                flag.extend_from_slice(&2u64.to_le_bytes());
                                flag.extend_from_slice(&[0, 1]);
                                let mut reply = server.data(&block(
                                    &[
                                        ("one", "UInt64"),
                                        ("note", "Nullable(String)"),
                                        ("pair", "Tuple(UInt32, String)"),
                                        ("attrs", "Map(String, UInt64)"),
                                        ("price", "Decimal(10, 2)"),
                                        ("flag", "LowCardinality(String)"),
                                    ],
                                    2,
                                    &[
                                        [1u64.to_le_bytes(), 2u64.to_le_bytes()].concat(),
                                        note,
                                        pair,
                                        attrs,
                                        [100i64.to_le_bytes(), (-5i64).to_le_bytes()].concat(),
                                        flag,
                                    ],
                                ));
                                reply.push(5);
                                reply
                            }
                            // The empty block that terminates the insert data;
                            // the one ending external tables looks the same.
                            ClientMsg::Data(body)
                                if body.len() == 10 && inserting == Some(token) =>
                            {
                                inserting = None;
                                vec![5]
                            }
                            ClientMsg::Data(_) => Vec::new(),
                        };
                        seen.push(msg);
                        replies.push((token, reply));
                    }
                }
                _ => {}
            }
        });
        ch.drive(&mut net, |id, result| outcomes.push((id, result)));
        for (token, reply) in replies {
            if !reply.is_empty() {
                net.send_with(token, |buf| buf.extend_from_slice(&reply));
            }
        }
        if outcomes.len() == 3 && recovered.is_none() {
            recovered = ch.query("SELECT one, note, pair, attrs, price, flag FROM t");
        }
        thread::sleep(Duration::from_millis(1));
    }

    outcomes.sort_by_key(|(id, _)| *id);
    let mut pair0 = 10u32.to_le_bytes().to_vec();
    pair0.extend_from_slice(&[1, b'a']);
    let mut pair1 = 20u32.to_le_bytes().to_vec();
    pair1.extend_from_slice(&[2, b'b', b'b']);
    let mut attrs0 = vec![1, 1, b'a'];
    attrs0.extend_from_slice(&1u64.to_le_bytes());
    let mut attrs1 = vec![2, 1, b'b'];
    attrs1.extend_from_slice(&2u64.to_le_bytes());
    attrs1.extend_from_slice(&[1, b'c']);
    attrs1.extend_from_slice(&3u64.to_le_bytes());
    let rows = vec![
        vec![
            Some(1u64.to_le_bytes().to_vec()),
            None,
            Some(pair0),
            Some(attrs0),
            Some(100i64.to_le_bytes().to_vec()),
            Some(b"x".to_vec()),
        ],
        vec![
            Some(2u64.to_le_bytes().to_vec()),
            Some(b"hi".to_vec()),
            Some(pair1),
            Some(attrs1),
            Some((-5i64).to_le_bytes().to_vec()),
            Some(b"y".to_vec()),
        ],
    ];
    let selected = Output {
        columns: vec![
            Column { name: "one".to_owned(), type_name: "UInt64".to_owned() },
            Column { name: "note".to_owned(), type_name: "Nullable(String)".to_owned() },
            Column { name: "pair".to_owned(), type_name: "Tuple(UInt32, String)".to_owned() },
            Column { name: "attrs".to_owned(), type_name: "Map(String, UInt64)".to_owned() },
            Column { name: "price".to_owned(), type_name: "Decimal(10, 2)".to_owned() },
            Column { name: "flag".to_owned(), type_name: "LowCardinality(String)".to_owned() },
        ],
        rows: rows.clone(),
    };
    assert_eq!(outcomes[0], (insert, Ok(Output::default())));
    assert_eq!(outcomes[1].0, select);
    assert_eq!(outcomes[1].1.as_ref().unwrap().rows, rows);
    assert_eq!(
        outcomes[2],
        (
            bad,
            Err(Error::Server {
                code: 62,
                name: "DB::Exception".to_owned(),
                message: "Syntax error".to_owned(),
            })
        )
    );
    // The connection dropped by the exception comes back and serves again.
    assert_eq!(outcomes[3], (recovered.unwrap(), Ok(selected)));

    assert_eq!(seen.iter().filter(|msg| **msg == hello).count(), 3);
    assert!(
        seen.iter().any(|msg| matches!(msg, ClientMsg::Query(sql, true)
            if sql == "INSERT INTO t (name, count, maybe, tags, pair, attrs, price, flag) VALUES"))
    );
    let mut payloads = vec![Vec::new(); 8];
    string(&mut payloads[0], b"ab");
    payloads[1].extend_from_slice(&5u64.to_le_bytes());
    payloads[2].push(0);
    payloads[2].extend_from_slice(&7u64.to_le_bytes());
    payloads[3].extend_from_slice(&2u64.to_le_bytes());
    payloads[3].extend_from_slice(&[9, 8]);
    payloads[4].extend_from_slice(&3u32.to_le_bytes());
    string(&mut payloads[4], b"xy");
    payloads[5].extend_from_slice(&1u64.to_le_bytes());
    string(&mut payloads[5], b"k");
    payloads[5].extend_from_slice(&11u64.to_le_bytes());
    payloads[6].extend_from_slice(&12_345i64.to_le_bytes());
    payloads[7].extend_from_slice(&1u64.to_le_bytes());
    payloads[7].extend_from_slice(&0x600u64.to_le_bytes());
    payloads[7].extend_from_slice(&1u64.to_le_bytes());
    string(&mut payloads[7], b"low");
    payloads[7].extend_from_slice(&1u64.to_le_bytes());
    payloads[7].push(0);
    assert!(seen.contains(&ClientMsg::Data(block(&COLUMNS, 1, &payloads))));
}

#[test]
fn timeouts_budgets_and_recovery() {
    let (mut net, addr) = listen();
    let server_group = net.add_group(TcpGroupConfig {
        name: "fake-clickhouse",
        framing: Framing::Raw,
        ..Default::default()
    });
    net.listen(server_group, addr).unwrap();
    let mut server = FakeServer { conns: Vec::new(), compressed: true };
    let mut ch = ClickHouse::new(addr, 1)
        .with_compression()
        .with_request_timeout(Duration::from_millis(200));

    ch.connect(&mut net);
    let big = ch.query("SELECT big").unwrap();

    let mut outcomes: Vec<(QueryId, Result<Output, Error>)> = Vec::new();
    let mut sent = 1;
    let hello = ClientMsg::Hello {
        database: String::new(),
        user: "default".to_owned(),
        password: String::new(),
    };
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    // One connection, three requests in turn; each failure drops it, so three
    // hellos land in the end.
    while Instant::now() < deadline &&
        (outcomes.len() < 3 || seen.iter().filter(|msg| **msg == hello).count() < 3)
    {
        let mut replies = Vec::new();
        net.poll_with(|event| {
            if ch.on_event(&event) {
                return;
            }
            match event {
                TcpEvent::Accepted { group, token, .. } if group == server_group => {
                    server.conns.push(ServerConn {
                        token,
                        input: Vec::new(),
                        greeted: false,
                        compressed: false,
                    });
                }
                TcpEvent::Message { group, token, payload, .. } if group == server_group => {
                    for msg in server.push(token, payload) {
                        let reply = match &msg {
                            ClientMsg::Hello { .. } => server_hello(),
                            // A block declaring a gigabyte: refused before a
                            // byte of it is allocated.
                            ClientMsg::Query(sql, _) if sql == "SELECT big" => {
                                let mut reply = vec![1, 0];
                                reply.extend_from_slice(&[0; 16]);
                                reply.push(0x82);
                                reply.extend_from_slice(&9u32.to_le_bytes());
                                reply.extend_from_slice(&0x4000_0000u32.to_le_bytes());
                                reply
                            }
                            // Never answered: the request times out instead.
                            ClientMsg::Query(sql, _) if sql == "SELECT slow" => Vec::new(),
                            ClientMsg::Query(_, _) => {
                                let mut reply = server.data(&block(&[], 0, &[]));
                                reply.push(5);
                                reply
                            }
                            ClientMsg::Data(_) => Vec::new(),
                        };
                        seen.push(msg);
                        replies.push((token, reply));
                    }
                }
                _ => {}
            }
        });
        ch.drive(&mut net, |id, result| outcomes.push((id, result)));
        for (token, reply) in replies {
            if !reply.is_empty() {
                net.send_with(token, |buf| buf.extend_from_slice(&reply));
            }
        }
        if outcomes.len() == sent && sent < 3 {
            sent += 1;
            ch.query(if sent == 2 { "SELECT slow" } else { "SELECT ok" });
        }
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(outcomes.len(), 3);
    assert_eq!(outcomes[0], (big, Err(Error::TooLarge)));
    assert_eq!(outcomes[1].1, Err(Error::Timeout));
    assert_eq!(outcomes[2].1, Ok(Output::default()));
    assert_eq!(seen.iter().filter(|msg| **msg == hello).count(), 3);
}
