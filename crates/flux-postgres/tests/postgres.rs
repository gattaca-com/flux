use std::{
    collections::VecDeque,
    net::Ipv4Addr,
    thread,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use flux_clickhouse::ClickHouse;
use flux_network::{
    Token,
    http::{HttpEvent, HttpNetwork},
    tcp::{Framing, TcpEvent, TcpGroup, TcpGroupConfig, TcpNetwork},
};
use flux_postgres::{Error, Output, Postgres, QueryId, copybinary};
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::{Digest, Sha256};

fn srv(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut msg = vec![tag];
    msg.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    msg.extend_from_slice(body);
    msg
}

fn auth(method: i32, extra: &[u8]) -> Vec<u8> {
    let mut body = method.to_be_bytes().to_vec();
    body.extend_from_slice(extra);
    srv(b'R', &body)
}

fn ready() -> Vec<u8> {
    srv(b'Z', b"I")
}

fn param(name: &str, value: &str) -> Vec<u8> {
    let mut body = Vec::from(name.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    srv(b'S', &body)
}

fn row_desc(columns: &[(&str, u32)]) -> Vec<u8> {
    let mut body = (columns.len() as u16).to_be_bytes().to_vec();
    for (name, oid) in columns {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&(*oid as i32).to_be_bytes());
        body.extend_from_slice(&4i16.to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
    }
    srv(b'T', &body)
}

fn data_row(fields: &[Option<&[u8]>]) -> Vec<u8> {
    let mut body = (fields.len() as u16).to_be_bytes().to_vec();
    for field in fields {
        match field {
            Some(bytes) => {
                body.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                body.extend_from_slice(bytes);
            }
            None => body.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    srv(b'D', &body)
}

fn complete(tag: &str) -> Vec<u8> {
    let mut body = Vec::from(tag.as_bytes());
    body.push(0);
    srv(b'C', &body)
}

fn error_msg(code: &str, message: &str) -> Vec<u8> {
    let mut body = vec![b'S'];
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(code.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);
    srv(b'E', &body)
}

fn copy_in() -> Vec<u8> {
    srv(b'G', &[1, 0, 5, 1, 0, 1, 0, 1, 0, 1, 0])
}

#[derive(Clone, Debug)]
enum ClientMsg {
    Startup(Vec<(String, String)>),
    Query(String),
    Password(Vec<u8>),
    CopyData(Vec<u8>),
    CopyDone,
}

struct ServerConn {
    token: Token,
    input: Vec<u8>,
    started: bool,
    outbox: VecDeque<u8>,
}

struct FakeServer {
    conns: Vec<ServerConn>,
    bytes_per_tick: usize,
}

impl FakeServer {
    fn paced(bytes_per_tick: usize) -> Self {
        Self { conns: Vec::new(), bytes_per_tick }
    }

    fn push(&mut self, token: Token, payload: &[u8]) -> Vec<ClientMsg> {
        let conn = self.conns.iter_mut().find(|conn| conn.token == token).unwrap();
        conn.input.extend_from_slice(payload);
        let mut msgs = Vec::new();
        let mut consumed = 0;
        if !conn.started {
            if conn.input.len() < 4 {
                return msgs;
            }
            let len = u32::from_be_bytes(conn.input[0..4].try_into().unwrap()) as usize;
            if conn.input.len() < len {
                return msgs;
            }
            let mut params = Vec::new();
            let mut parts = conn.input[8..len].split(|byte| *byte == 0);
            while let (Some(name), Some(value)) = (parts.next(), parts.next()) {
                if name.is_empty() {
                    break;
                }
                params.push((
                    String::from_utf8_lossy(name).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                ));
            }
            msgs.push(ClientMsg::Startup(params));
            consumed = len;
            conn.started = true;
        }
        while conn.input.len() - consumed >= 5 {
            let tag = conn.input[consumed];
            let len = u32::from_be_bytes(conn.input[consumed + 1..consumed + 5].try_into().unwrap())
                as usize;
            if len < 4 || conn.input.len() - consumed < 1 + len {
                break;
            }
            let body = &conn.input[consumed + 5..consumed + 1 + len];
            match tag {
                b'Q' => msgs.push(ClientMsg::Query(
                    String::from_utf8_lossy(&body[..body.len() - 1]).into_owned(),
                )),
                b'p' => msgs.push(ClientMsg::Password(body.to_vec())),
                b'd' => msgs.push(ClientMsg::CopyData(body.to_vec())),
                b'c' => msgs.push(ClientMsg::CopyDone),
                _ => panic!("unexpected client message {tag}"),
            }
            consumed += 1 + len;
        }
        conn.input.drain(..consumed);
        msgs
    }

    fn reply(&mut self, token: Token, bytes: &[u8]) {
        let conn = self.conns.iter_mut().find(|conn| conn.token == token).unwrap();
        conn.outbox.extend(bytes);
    }

    fn drain(&mut self) -> Vec<(Token, Vec<u8>)> {
        let mut replies = Vec::new();
        for conn in &mut self.conns {
            let take = conn.outbox.len().min(self.bytes_per_tick);
            if take > 0 {
                replies.push((conn.token, conn.outbox.drain(..take).collect()));
            }
        }
        replies
    }
}

fn tick(
    net: &mut TcpNetwork,
    pg: &mut Postgres,
    server_group: TcpGroup,
    server: &mut FakeServer,
    outcomes: &mut Vec<(QueryId, Result<Output, Error>)>,
    seen: &mut Vec<ClientMsg>,
    mut on_msg: impl FnMut(&mut FakeServer, Token, ClientMsg) -> bool,
) {
    let mut drop = Vec::new();
    net.poll_with(|event| {
        if pg.on_event(&event) {
            return;
        }
        match event {
            TcpEvent::Accepted { group, token, .. } if group == server_group => {
                server.conns.push(ServerConn {
                    token,
                    input: Vec::new(),
                    started: false,
                    outbox: VecDeque::new(),
                });
            }
            TcpEvent::Message { group, token, payload, .. } if group == server_group => {
                for msg in server.push(token, payload) {
                    seen.push(msg.clone());
                    if on_msg(server, token, msg) {
                        drop.push(token);
                    }
                }
            }
            _ => {}
        }
    });
    pg.drive(net, |id, result| outcomes.push((id, result)));
    for (token, bytes) in server.drain() {
        net.send_with(token, |buf| buf.extend_from_slice(&bytes));
    }
    for token in drop {
        net.disconnect(token);
    }
}

fn free_addr() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn setup() -> (TcpNetwork, TcpGroup, std::net::SocketAddr) {
    let addr = free_addr();
    let mut net = TcpNetwork::default();
    let group = net.add_group(TcpGroupConfig {
        name: "fake-pg",
        framing: Framing::Raw,
        ..Default::default()
    });
    net.listen(group, addr).unwrap();
    (net, group, addr)
}

fn outcome_of(
    outcomes: &[(QueryId, Result<Output, Error>)],
    id: QueryId,
) -> &(QueryId, Result<Output, Error>) {
    outcomes.iter().find(|(i, _)| *i == id).unwrap()
}

#[derive(Serialize)]
struct Row {
    a: i32,
    b: String,
    c: Option<u64>,
    d: bool,
    e: f64,
}

#[derive(Serialize)]
struct TextRow {
    name: &'static str,
    amount: Option<i64>,
}

/// `COPY TEXT` as `Postgres` reads it: tab-separated, `\N` for NULL,
/// control characters backslash-escaped.
const TEXT_BODY: &str = "a\\tb\t-1\nc\t\\N\n";

fn hello() -> Vec<u8> {
    let mut reply = auth(0, &[]);
    reply.extend_from_slice(&param("client_encoding", "UTF8"));
    reply.extend_from_slice(&param("server_version", "18.6"));
    reply.extend_from_slice(&srv(b'K', &[0, 0, 0, 7, 0, 0, 0, 9]));
    reply.extend_from_slice(&srv(b'N', &[b'S', b'W', 0, 0]));
    reply.extend_from_slice(&ready());
    reply
}

fn one_row() -> Vec<u8> {
    let mut reply = row_desc(&[("one", 23)]);
    reply.extend_from_slice(&data_row(&[Some(&1i32.to_be_bytes())]));
    reply.extend_from_slice(&complete("SELECT 1"));
    reply.extend_from_slice(&ready());
    reply
}

fn binary_batch(rows: &[Row]) -> Vec<u8> {
    let mut body = Vec::new();
    copybinary::header(&mut body);
    for row in rows {
        copybinary::encode(&mut body, row).unwrap();
    }
    copybinary::trailer(&mut body);
    body
}

#[test]
fn query_copy_and_error_byte_at_a_time() {
    let (mut net, server_group, addr) = setup();
    let mut pg = Postgres::new(addr).with_database("db").with_connections(2);
    pg.connect(&mut net);
    let mut server = FakeServer::paced(1);
    let mut outcomes = Vec::new();
    let mut seen = Vec::new();
    let mut copies: Vec<(String, Vec<u8>)> = Vec::new();
    let mut open: Vec<(Token, String, Vec<u8>)> = Vec::new();

    let rows = [Row { a: 1, b: "x".to_owned(), c: Some(2), d: false, e: 0.5 }, Row {
        a: 1,
        b: "x".to_owned(),
        c: Some(2),
        d: false,
        e: 0.5,
    }];

    let text_rows =
        [TextRow { name: "a\tb", amount: Some(-1) }, TextRow { name: "c", amount: None }];

    let one = pg.query("SELECT 1").unwrap();
    let copy = pg.copy_rows("t", &rows).unwrap();
    let text = pg.copy_text_rows("t", &text_rows).unwrap();
    let bad = pg.query("SELEC").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && outcomes.len() < 4 {
        tick(
            &mut net,
            &mut pg,
            server_group,
            &mut server,
            &mut outcomes,
            &mut seen,
            |server, token, msg| {
                match msg {
                    ClientMsg::Startup(_) => server.reply(token, &hello()),
                    ClientMsg::Query(sql) if sql == "SELECT 1" => {
                        server.reply(token, &one_row());
                    }
                    ClientMsg::Query(sql) if sql == "SELEC" => {
                        let mut reply = error_msg("42601", "syntax error at end of input");
                        reply.extend_from_slice(&ready());
                        server.reply(token, &reply);
                    }
                    ClientMsg::Query(sql) => {
                        assert!(sql.starts_with("COPY t "), "unexpected query {sql}");
                        open.push((token, sql, Vec::new()));
                        server.reply(token, &copy_in());
                    }
                    ClientMsg::CopyData(data) => {
                        let open = open.iter_mut().find(|(other, ..)| *other == token).unwrap();
                        open.2.extend_from_slice(&data);
                    }
                    ClientMsg::CopyDone => {
                        let index = open.iter().position(|(other, ..)| *other == token).unwrap();
                        let (_, sql, body) = open.swap_remove(index);
                        copies.push((sql, body));
                        let mut reply = complete("COPY 2");
                        reply.extend_from_slice(&ready());
                        server.reply(token, &reply);
                    }
                    ClientMsg::Password(_) => panic!("no password expected"),
                }
                false
            },
        );
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(outcomes.len(), 4);
    let (_, result) = outcome_of(&outcomes, one);
    let output = result.as_ref().unwrap();
    assert_eq!(output.tag, "SELECT 1");
    assert_eq!(output.columns.len(), 1);
    assert_eq!(output.columns[0].name, "one");
    assert_eq!(output.columns[0].oid, 23);
    assert_eq!(output.rows, vec![vec![Some(1i32.to_be_bytes().to_vec())]]);
    let (_, result) = outcome_of(&outcomes, bad);
    assert_eq!(result.as_ref().unwrap_err(), &Error::Server {
        code: "42601".to_owned(),
        message: "syntax error at end of input".to_owned()
    });
    let body_of = |prefix: &str| {
        let (_, body) = copies.iter().find(|(sql, _)| sql.starts_with(prefix)).unwrap();
        body.clone()
    };
    assert_eq!(body_of("COPY t (\"a\""), binary_batch(&rows));
    assert_eq!(String::from_utf8(body_of("COPY t (\"name\"")).unwrap(), TEXT_BODY);
    for id in [copy, text] {
        let (_, result) = outcome_of(&outcomes, id);
        assert_eq!(result.as_ref().unwrap().tag, "COPY 2");
    }
    let startups: Vec<_> = seen
        .iter()
        .filter_map(|msg| match msg {
            ClientMsg::Startup(p) => Some(p),
            _ => None,
        })
        .collect();
    assert_eq!(startups.len(), 2);
    for params in startups {
        assert!(params.contains(&("user".to_owned(), "postgres".to_owned())));
        assert!(params.contains(&("database".to_owned(), "db".to_owned())));
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn scram_server_check(transcript: &[Vec<u8>], salt: &[u8], client_final: &[u8]) -> Vec<u8> {
    let client_final = String::from_utf8_lossy(client_final).into_owned();
    let without_proof = client_final.split(",p=").next().unwrap().to_owned();
    let proof = STANDARD.decode(client_final.split(",p=").nth(1).unwrap()).unwrap();
    let mut salted_input = salt.to_vec();
    salted_input.extend_from_slice(&1u32.to_be_bytes());
    let mut salted = hmac_sha256(b"pencil", &salted_input);
    let mut prev = salted;
    for _ in 1..4096 {
        prev = hmac_sha256(b"pencil", &prev);
        for (acc, byte) in salted.iter_mut().zip(prev) {
            *acc ^= byte;
        }
    }
    let auth_message = format!(
        "{},{},{}",
        String::from_utf8_lossy(&transcript[0]),
        String::from_utf8_lossy(&transcript[1]),
        without_proof
    );
    let client_key = hmac_sha256(&salted, b"Client Key");
    let signature = hmac_sha256(&Sha256::digest(client_key), auth_message.as_bytes());
    let expected: Vec<u8> = client_key.iter().zip(signature).map(|(key, sig)| key ^ sig).collect();
    assert_eq!(proof, expected);
    let server_sig = hmac_sha256(&hmac_sha256(&salted, b"Server Key"), auth_message.as_bytes());
    format!("v={}", STANDARD.encode(server_sig)).into_bytes()
}

#[test]
fn scram_login_then_disconnect_recovers() {
    let (mut net, server_group, addr) = setup();
    let mut pg = Postgres::new(addr).with_credentials("user", "pencil");
    pg.connect(&mut net);
    let mut server = FakeServer::paced(usize::MAX);
    let mut outcomes = Vec::new();
    let mut seen = Vec::new();
    let salt = b"fixed-salt-16byte";
    let mut transcript = Vec::new();
    let mut dropped = false;
    let first = pg.query("SELECT 1").unwrap();
    let mut second = None;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && outcomes.len() < 2 {
        tick(
            &mut net,
            &mut pg,
            server_group,
            &mut server,
            &mut outcomes,
            &mut seen,
            |server, token, msg| {
                match msg {
                    ClientMsg::Startup(_) => {
                        let mut body = Vec::from(&b"SCRAM-SHA-256\0"[..]);
                        body.push(0);
                        server.reply(token, &auth(10, &body));
                    }
                    ClientMsg::Password(message) => {
                        if transcript.is_empty() {
                            assert!(message.starts_with(b"SCRAM-SHA-256\0"));
                            let len =
                                u32::from_be_bytes(message[14..18].try_into().unwrap()) as usize;
                            let first = &message[18..18 + len];
                            let cnonce = first.strip_prefix(b"n,,n=user,r=").unwrap();
                            let server_first = format!(
                                "r={},s={},i=4096",
                                String::from_utf8_lossy(cnonce).into_owned() + "SERVER123",
                                STANDARD.encode(salt),
                            );
                            transcript.push(first[3..].to_vec());
                            transcript.push(server_first.as_bytes().to_vec());
                            server.reply(token, &auth(11, server_first.as_bytes()));
                        } else {
                            let server_final = scram_server_check(&transcript, salt, &message);
                            transcript.clear();
                            let mut reply = auth(12, &server_final);
                            reply.extend_from_slice(&auth(0, &[]));
                            reply.extend_from_slice(&ready());
                            server.reply(token, &reply);
                        }
                    }
                    ClientMsg::Query(_) if !dropped => {
                        dropped = true;
                        return true;
                    }
                    ClientMsg::Query(_) => {
                        let mut reply = complete("SELECT 1");
                        reply.extend_from_slice(&ready());
                        server.reply(token, &reply);
                    }
                    msg => panic!("unexpected {msg:?}"),
                }
                false
            },
        );
        if dropped && second.is_none() {
            second = pg.query("SELECT 1");
        }
        thread::sleep(Duration::from_millis(1));
    }
    let (_, result) = outcome_of(&outcomes, first);
    assert_eq!(result.as_ref().unwrap_err(), &Error::Disconnected);
    let (_, result) = outcome_of(&outcomes, second.unwrap());
    assert_eq!(result.as_ref().unwrap().tag, "SELECT 1");
}

/// The acceptance case for the shared core: one poll loop carries a
/// `Postgres` client and a `ClickHouse` client, and both report their own
/// outcome.
#[test]
fn one_poll_serves_postgres_and_clickhouse() {
    let (mut net, server_group, pg_addr) = setup();
    let http_addr = free_addr();
    let mut http = HttpNetwork::default();
    http.listen(&mut net, http_addr).unwrap();
    let ch = ClickHouse::new(&mut http, &mut net, http_addr, 1).with_database("telemetry");
    let mut pg = Postgres::new(pg_addr).with_database("telemetry");
    pg.connect(&mut net);

    let rows = [Row { a: 7, b: "shared".to_owned(), c: Some(1), d: true, e: 2.5 }];
    let copy = pg.copy_rows("fills", &rows).unwrap();
    let insert = ch.insert_rows(&mut http, "fills", &rows).unwrap();

    let mut server = FakeServer::paced(usize::MAX);
    let mut pg_outcome = None;
    let mut ch_outcome = None;
    let mut copied = Vec::new();
    let mut inserted = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && (pg_outcome.is_none() || ch_outcome.is_none()) {
        net.poll_with(|event| {
            if http.on_event(&event) || pg.on_event(&event) {
                return;
            }
            match event {
                TcpEvent::Accepted { group, token, .. } if group == server_group => {
                    server.conns.push(ServerConn {
                        token,
                        input: Vec::new(),
                        started: false,
                        outbox: VecDeque::new(),
                    });
                }
                TcpEvent::Message { group, token, payload, .. } if group == server_group => {
                    for msg in server.push(token, payload) {
                        match msg {
                            ClientMsg::Startup(_) => server.reply(token, &hello()),
                            ClientMsg::Query(_) => server.reply(token, &copy_in()),
                            ClientMsg::CopyData(data) => copied.extend_from_slice(&data),
                            ClientMsg::CopyDone => {
                                let mut reply = complete("COPY 1");
                                reply.extend_from_slice(&ready());
                                server.reply(token, &reply);
                            }
                            ClientMsg::Password(_) => panic!("no password expected"),
                        }
                    }
                }
                _ => {}
            }
        });

        let mut requests = Vec::new();
        http.drive(&mut net, |event| {
            if let Some((id, result)) = ch.outcome(&event) {
                assert_eq!(id, insert);
                ch_outcome = Some(result.map(<[u8]>::to_vec).map_err(|_| ()));
            } else if let HttpEvent::Request { token, request } = event {
                inserted.push(request.body.to_vec());
                requests.push(token);
            }
        });
        for token in requests {
            http.respond(&mut net, token, 200, &[], b"");
        }
        pg.drive(&mut net, |id, result| {
            assert_eq!(id, copy);
            pg_outcome = Some(result.map(|output| output.tag).map_err(|_| ()));
        });
        for (token, bytes) in server.drain() {
            net.send_with(token, |buf| buf.extend_from_slice(&bytes));
        }
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(pg_outcome, Some(Ok("COPY 1".to_owned())));
    assert_eq!(ch_outcome, Some(Ok(Vec::new())));
    assert_eq!(copied, binary_batch(&rows));
    assert_eq!(inserted.len(), 1);
    assert!(!inserted[0].is_empty());
}
