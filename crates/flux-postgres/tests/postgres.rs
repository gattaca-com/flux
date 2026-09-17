use std::{
    collections::VecDeque,
    net::Ipv4Addr,
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Token,
    tcp::{Framing, TcpEvent, TcpGroup, TcpGroupConfig, TcpNetwork},
};
use flux_postgres::{Error, Output, Postgres, QueryId, copybinary};
use serde::Serialize;

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
    Password,
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
                b'p' => msgs.push(ClientMsg::Password),
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
    mut on_msg: impl FnMut(&mut FakeServer, Token, ClientMsg),
) {
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
                    on_msg(server, token, msg);
                }
            }
            _ => {}
        }
    });
    pg.drive(net, |id, result| outcomes.push((id, result)));
    for (token, bytes) in server.drain() {
        net.send_with(token, |buf| buf.extend_from_slice(&bytes));
    }
}

fn setup() -> (TcpNetwork, TcpGroup, std::net::SocketAddr) {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
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

#[test]
fn query_copy_and_error_byte_at_a_time() {
    let (mut net, server_group, addr) = setup();
    let mut pg = Postgres::new(addr).with_database("db").with_connections(2);
    let mut server = FakeServer::paced(1);
    let mut outcomes = Vec::new();
    let mut seen = Vec::new();
    let mut copied = Vec::new();

    let rows = [Row { a: 1, b: "x".to_owned(), c: Some(2), d: false, e: 0.5 }, Row {
        a: 1,
        b: "x".to_owned(),
        c: Some(2),
        d: false,
        e: 0.5,
    }];
    let mut body = Vec::new();
    copybinary::header(&mut body);
    for row in &rows {
        copybinary::encode(&mut body, row).unwrap();
    }
    copybinary::trailer(&mut body);

    let one = pg.query("SELECT 1").unwrap();
    let copy = pg.copy_rows("t", &rows).unwrap();
    let bad = pg.query("SELEC").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && outcomes.len() < 3 {
        tick(
            &mut net,
            &mut pg,
            server_group,
            &mut server,
            &mut outcomes,
            &mut seen,
            |server, token, msg| match msg {
                ClientMsg::Startup(_) => {
                    let mut reply = auth(0, &[]);
                    reply.extend_from_slice(&param("client_encoding", "UTF8"));
                    reply.extend_from_slice(&param("server_version", "18.6"));
                    reply.extend_from_slice(&srv(b'K', &[0, 0, 0, 7, 0, 0, 0, 9]));
                    reply.extend_from_slice(&srv(b'N', &[b'S', b'W', 0, 0]));
                    reply.extend_from_slice(&ready());
                    server.reply(token, &reply);
                }
                ClientMsg::Query(sql) if sql == "SELECT 1" => {
                    let mut reply = row_desc(&[("one", 23)]);
                    reply.extend_from_slice(&data_row(&[Some(&1i32.to_be_bytes())]));
                    reply.extend_from_slice(&complete("SELECT 1"));
                    reply.extend_from_slice(&ready());
                    server.reply(token, &reply);
                }
                ClientMsg::Query(sql) if sql == "SELEC" => {
                    let mut reply = error_msg("42601", "syntax error at end of input");
                    reply.extend_from_slice(&ready());
                    server.reply(token, &reply);
                }
                ClientMsg::Query(sql) => {
                    assert!(sql.starts_with("COPY t "), "unexpected query {sql}");
                    server.reply(token, &copy_in());
                }
                ClientMsg::CopyData(data) => copied.extend_from_slice(&data),
                ClientMsg::CopyDone => {
                    let mut reply = complete("COPY 2");
                    reply.extend_from_slice(&ready());
                    server.reply(token, &reply);
                }
                ClientMsg::Password => panic!("no password expected"),
            },
        );
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(outcomes.len(), 3);
    let (_, result) = outcome_of(&outcomes, one);
    let output = result.as_ref().unwrap();
    assert_eq!(output.tag, "SELECT 1");
    assert_eq!(output.columns.len(), 1);
    assert_eq!(output.columns[0].name, "one");
    assert_eq!(output.columns[0].oid, 23);
    assert_eq!(output.rows, vec![vec![Some(1i32.to_be_bytes().to_vec())]]);
    let (_, result) = outcome_of(&outcomes, copy);
    assert_eq!(result.as_ref().unwrap().tag, "COPY 2");
    let (_, result) = outcome_of(&outcomes, bad);
    assert_eq!(result.as_ref().unwrap_err(), &Error::Server {
        code: "42601".to_owned(),
        message: "syntax error at end of input".to_owned()
    });
    assert_eq!(copied, body);
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
