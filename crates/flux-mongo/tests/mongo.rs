use std::{
    collections::VecDeque,
    net::Ipv4Addr,
    thread,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bson::{Bson, Document, doc};
use flux_mongo::{CommandId, Error, Mongo};
use flux_network::{
    Token,
    tcp::{Framing, TcpEvent, TcpGroup, TcpGroupConfig, TcpNetwork},
};
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::{Digest, Sha256};

const OP_MSG: i32 = 2013;

#[derive(Serialize)]
struct DocRow {
    a: i32,
    b: String,
}

#[derive(Clone, Debug)]
enum ServerMsg {
    Hello { request_id: i32 },
    SaslStart { request_id: i32, db: String, payload: Vec<u8> },
    SaslContinue { request_id: i32, conversation: i32, db: String, payload: Vec<u8> },
    Command { request_id: i32, doc: Document },
}

struct ServerConn {
    token: Token,
    input: Vec<u8>,
    outbox: VecDeque<u8>,
}

struct FakeServer {
    conns: Vec<ServerConn>,
    bytes_per_tick: usize,
    next_id: i32,
}

impl FakeServer {
    fn paced(bytes_per_tick: usize) -> Self {
        Self { conns: Vec::new(), bytes_per_tick, next_id: 1 }
    }

    fn push(&mut self, token: Token, payload: &[u8]) -> Vec<ServerMsg> {
        let conn = self.conns.iter_mut().find(|conn| conn.token == token).unwrap();
        conn.input.extend_from_slice(payload);
        let mut msgs = Vec::new();
        let mut consumed = 0;
        while conn.input.len() - consumed >= 16 {
            let total =
                i32::from_le_bytes(conn.input[consumed..consumed + 4].try_into().unwrap()) as usize;
            assert!(total >= 16, "client message length below header");
            if conn.input.len() - consumed < total {
                break;
            }
            let frame = &conn.input[consumed..consumed + total];
            let opcode = i32::from_le_bytes(frame[12..16].try_into().unwrap());
            assert_eq!(opcode, OP_MSG, "client must speak OP_MSG");
            let request_id = i32::from_le_bytes(frame[4..8].try_into().unwrap());
            assert_ne!(request_id, 0, "client requestID must be nonzero");
            assert_eq!(frame[20], 0, "client must send kind-0 sections");
            let doc = Document::from_reader(&frame[21..]).unwrap();
            if doc.contains_key("hello") {
                msgs.push(ServerMsg::Hello { request_id });
            } else if doc.contains_key("saslStart") {
                msgs.push(ServerMsg::SaslStart {
                    request_id,
                    db: doc.get_str("$db").unwrap().to_owned(),
                    payload: STANDARD.decode(doc.get_str("payload").unwrap()).unwrap(),
                });
            } else if doc.contains_key("saslContinue") {
                msgs.push(ServerMsg::SaslContinue {
                    request_id,
                    conversation: doc.get_i32("conversationId").unwrap(),
                    db: doc.get_str("$db").unwrap().to_owned(),
                    payload: STANDARD.decode(doc.get_str("payload").unwrap()).unwrap(),
                });
            } else {
                msgs.push(ServerMsg::Command { request_id, doc });
            }
            consumed += total;
        }
        conn.input.drain(..consumed);
        msgs
    }

    fn reply(&mut self, token: Token, response_to: i32, reply: &Document) {
        let payload = bson::to_vec(reply).unwrap();
        let mut msg = Vec::with_capacity(16 + 5 + payload.len());
        msg.extend_from_slice(&((16 + 5 + payload.len()) as i32).to_le_bytes());
        msg.extend_from_slice(&self.next_id.to_le_bytes());
        msg.extend_from_slice(&response_to.to_le_bytes());
        msg.extend_from_slice(&OP_MSG.to_le_bytes());
        msg.extend_from_slice(&0u32.to_le_bytes());
        msg.push(0);
        msg.extend_from_slice(&payload);
        self.next_id += 1;
        let conn = self.conns.iter_mut().find(|conn| conn.token == token).unwrap();
        conn.outbox.extend(msg);
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
    mongo: &mut Mongo,
    server_group: TcpGroup,
    server: &mut FakeServer,
    outcomes: &mut Vec<(CommandId, Result<Document, Error>)>,
    seen: &mut Vec<ServerMsg>,
    mut on_msg: impl FnMut(&mut FakeServer, Token, ServerMsg) -> bool,
) {
    let mut drop = Vec::new();
    net.poll_with(|event| {
        if mongo.on_event(&event) {
            return;
        }
        match event {
            TcpEvent::Accepted { group, token, .. } if group == server_group => {
                server.conns.push(ServerConn { token, input: Vec::new(), outbox: VecDeque::new() });
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
    mongo.drive(net, |id, result| outcomes.push((id, result)));
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
        name: "fake-mongo",
        framing: Framing::Raw,
        ..Default::default()
    });
    net.listen(group, addr).unwrap();
    (net, group, addr)
}

fn outcome_of(
    outcomes: &[(CommandId, Result<Document, Error>)],
    id: CommandId,
) -> &(CommandId, Result<Document, Error>) {
    outcomes.iter().find(|(other, _)| *other == id).unwrap()
}

fn rows() -> [DocRow; 2] {
    [DocRow { a: 1, b: "x".to_owned() }, DocRow { a: 2, b: "y".to_owned() }]
}

#[test]
#[allow(clippy::too_many_lines)]
fn commands_round_trip_byte_at_a_time() {
    let (mut net, server_group, addr) = setup();
    let mut mongo = Mongo::new(addr);
    mongo.connect(&mut net);
    let mut server = FakeServer::paced(1);
    let mut outcomes = Vec::new();
    let mut seen = Vec::new();

    let mut tiny = Mongo::new(addr).with_max_queued_bytes(1);
    assert_eq!(tiny.run_command("db", &doc! { "ping": 1 }), None);

    let batch = rows();
    let insert = mongo.insert("db", "things", &batch).unwrap();
    let find = mongo.find("db", "things", doc! { "a": 1 }).unwrap();
    let bogus = mongo.run_command("db", &doc! { "bogus": 1 }).unwrap();

    let first_batch = vec![doc! { "a": 1, "b": "x" }, doc! { "a": 2, "b": "y" }];
    let mut inserted = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && outcomes.len() < 3 {
        tick(
            &mut net,
            &mut mongo,
            server_group,
            &mut server,
            &mut outcomes,
            &mut seen,
            |server, token, msg| {
                match msg {
                    ServerMsg::Hello { request_id } => {
                        server.reply(token, request_id, &doc! { "ok": 1 });
                    }
                    ServerMsg::Command { request_id, doc } if doc.contains_key("insert") => {
                        assert_eq!(doc.get_str("$db"), Ok("db"));
                        inserted = doc.get_array("documents").unwrap().clone();
                        server.reply(token, request_id, &doc! { "ok": 1, "n": 2 });
                    }
                    ServerMsg::Command { request_id, doc } if doc.contains_key("find") => {
                        assert_eq!(doc.get_document("filter").unwrap(), &doc! { "a": 1 });
                        server.reply(token, request_id, &doc! {
                            "ok": 1.0,
                            "cursor": doc! {
                                "id": 0i64,
                                "ns": "db.things",
                                "firstBatch": first_batch.clone(),
                            },
                        });
                    }
                    ServerMsg::Command { request_id, doc } => {
                        assert!(doc.contains_key("bogus"));
                        assert_eq!(doc.get_str("$db"), Ok("db"));
                        server.reply(token, request_id, &doc! {
                            "ok": 0.0,
                            "code": 59,
                            "codeName": "CommandNotFound",
                            "errmsg": "no such command: 'bogus'",
                        });
                    }
                    ServerMsg::SaslStart { .. } | ServerMsg::SaslContinue { .. } => {
                        panic!("no authentication expected")
                    }
                }
                false
            },
        );
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(outcomes.len(), 3);
    let (_, result) = outcome_of(&outcomes, insert);
    assert_eq!(result.as_ref().unwrap(), &doc! { "ok": 1, "n": 2 });
    let (_, result) = outcome_of(&outcomes, find);
    assert_eq!(result.as_ref().unwrap(), &doc! {
        "ok": 1.0,
        "cursor": doc! {
            "id": 0i64,
            "ns": "db.things",
            "firstBatch": first_batch,
        },
    });
    let (_, result) = outcome_of(&outcomes, bogus);
    assert_eq!(result.as_ref().unwrap_err(), &Error::Server {
        code: Some(59),
        code_name: Some("CommandNotFound".to_owned()),
        message: "no such command: 'bogus'".to_owned(),
    });
    let expected: Vec<Bson> =
        batch.iter().map(|row| Bson::Document(bson::to_document(row).unwrap())).collect();
    assert_eq!(inserted, expected);
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn scram_server_check(transcript: &[Vec<u8>], salt: &[u8], client_final: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(client_final).into_owned();
    let (without_proof, proof_b64) = text.split_once(",p=").unwrap();
    let proof = STANDARD.decode(proof_b64).unwrap();
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
fn scram_auth_then_insert() {
    let (mut net, server_group, addr) = setup();
    let mut mongo = Mongo::new(addr).with_credentials("user", "pencil").with_auth_source("creds");
    mongo.connect(&mut net);
    let mut server = FakeServer::paced(1);
    let mut outcomes = Vec::new();
    let mut seen = Vec::new();
    let salt = b"fixed-salt-16byte";
    let mut transcript = Vec::new();

    let batch = rows();
    let insert = mongo.insert("db", "things", &batch).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && outcomes.is_empty() {
        tick(
            &mut net,
            &mut mongo,
            server_group,
            &mut server,
            &mut outcomes,
            &mut seen,
            |server, token, msg| {
                match msg {
                    ServerMsg::Hello { request_id } => {
                        server.reply(token, request_id, &doc! { "ok": 1 });
                    }
                    ServerMsg::SaslStart { request_id, db, payload } => {
                        assert_eq!(db, "creds");
                        let first = String::from_utf8(payload).unwrap();
                        let cnonce = first.strip_prefix("n,,n=user,r=").unwrap().to_owned();
                        let server_first =
                            format!("r={cnonce}SERVER123,s={},i=4096", STANDARD.encode(salt));
                        transcript.push(first.as_bytes()[3..].to_vec());
                        transcript.push(server_first.as_bytes().to_vec());
                        server.reply(token, request_id, &doc! {
                            "ok": 1,
                            "conversationId": 7,
                            "payload": STANDARD.encode(&server_first),
                            "done": false,
                        });
                    }
                    ServerMsg::SaslContinue { request_id, conversation, db, payload } => {
                        assert_eq!((conversation, db.as_str()), (7, "creds"));
                        let server_final = scram_server_check(&transcript, salt, &payload);
                        transcript.clear();
                        server.reply(token, request_id, &doc! {
                            "ok": 1,
                            "conversationId": 7,
                            "payload": STANDARD.encode(&server_final),
                            "done": true,
                        });
                    }
                    ServerMsg::Command { request_id, doc } => {
                        assert!(doc.contains_key("insert"));
                        server.reply(token, request_id, &doc! { "ok": 1, "n": 2 });
                    }
                }
                false
            },
        );
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(outcomes.len(), 1);
    let (_, result) = outcome_of(&outcomes, insert);
    assert_eq!(result.as_ref().unwrap(), &doc! { "ok": 1, "n": 2 });
    let dbs: Vec<_> = seen
        .iter()
        .filter_map(|msg| match msg {
            ServerMsg::SaslStart { db, .. } | ServerMsg::SaslContinue { db, .. } => {
                Some(db.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(dbs, ["creds", "creds"]);
}

#[test]
fn disconnect_mid_request_reports_disconnected() {
    let (mut net, server_group, addr) = setup();
    let mut mongo = Mongo::new(addr);
    mongo.connect(&mut net);
    let mut server = FakeServer::paced(1);
    let mut outcomes = Vec::new();
    let mut seen = Vec::new();
    let mut dropped = false;

    let batch = rows();
    let insert = mongo.insert("db", "things", &batch).unwrap();
    let mut retry = None;
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && outcomes.len() < 2 {
        tick(
            &mut net,
            &mut mongo,
            server_group,
            &mut server,
            &mut outcomes,
            &mut seen,
            |server, token, msg| {
                match msg {
                    ServerMsg::Hello { request_id } => {
                        server.reply(token, request_id, &doc! { "ok": 1 });
                    }
                    ServerMsg::Command { .. } if !dropped => {
                        dropped = true;
                        return true;
                    }
                    ServerMsg::Command { request_id, .. } => {
                        server.reply(token, request_id, &doc! { "ok": 1, "n": 1 });
                    }
                    ServerMsg::SaslStart { .. } | ServerMsg::SaslContinue { .. } => {
                        panic!("no authentication expected")
                    }
                }
                false
            },
        );
        if dropped && retry.is_none() {
            retry = mongo.find("db", "things", doc! {}).unwrap().into();
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(outcomes.len(), 2);
    let (_, result) = outcome_of(&outcomes, insert);
    assert_eq!(result.as_ref().unwrap_err(), &Error::Disconnected);
    let (_, result) = outcome_of(&outcomes, retry.unwrap());
    assert_eq!(result.as_ref().unwrap(), &doc! { "ok": 1, "n": 1 });
}
