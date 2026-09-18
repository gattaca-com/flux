use std::{
    net::Ipv4Addr,
    thread,
    time::{Duration, Instant},
};

use flux_clickhouse::{ClickHouse, Error, Output, QueryId};
use flux_network::{
    Token,
    tcp::{Framing, TcpEvent, TcpGroupConfig, TcpNetwork},
};
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
