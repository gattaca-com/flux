use std::{
    net::Ipv4Addr,
    thread,
    time::{Duration, Instant},
};

use flux_clickhouse::{ClickHouse, Error, HttpResponse, QueryId};
use flux_network::http::{HttpEvent, HttpNetwork};

const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq)]
enum Outcome {
    Ok(Vec<u8>),
    Server { status: u16, code: Option<u32>, message: Vec<u8> },
    Disconnected,
}

fn record(result: &Result<HttpResponse<'_>, Error<'_>>) -> Outcome {
    match result {
        Ok(response) => Outcome::Ok(response.body.to_vec()),
        Err(Error::Server { status, code, message }) => {
            Outcome::Server { status: *status, code: *code, message: message.to_vec() }
        }
        Err(Error::Disconnected) => Outcome::Disconnected,
    }
}

struct Request {
    path: String,
    user: Vec<u8>,
    key: Vec<u8>,
    body: Vec<u8>,
}

/// One `HttpNetwork` plays both sides: it listens as a fake `ClickHouse` and
/// hosts the client's pool, so the client must share the caller's network and
/// hand back every event that is not its own.
struct Harness {
    http: HttpNetwork,
    ch: ClickHouse,
    requests: Vec<Request>,
    outcomes: Vec<(QueryId, Outcome)>,
}

impl Harness {
    fn start(configure: impl FnOnce(ClickHouse) -> ClickHouse) -> Self {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let mut http = HttpNetwork::default();
        http.listen(addr).unwrap();
        let ch = configure(ClickHouse::new(addr));
        Self { http, ch, requests: Vec::new(), outcomes: Vec::new() }
    }
    fn tick(&mut self) {
        let Self { http, ch, requests, outcomes } = self;
        let mut replies = Vec::new();
        http.poll_with(|event| {
            if ch.on_event(&event, |id, result| outcomes.push((id, record(&result)))) {
                return
            }
            if let HttpEvent::Request { token, request } = event {
                requests.push(Request {
                    path: request.path.to_owned(),
                    user: request.header("x-clickhouse-user").unwrap_or_default().to_vec(),
                    key: request.header("x-clickhouse-key").unwrap_or_default().to_vec(),
                    body: request.body.to_vec(),
                });
                replies.push((token, request.path.to_owned(), request.body.to_vec()));
            }
        });
        for (token, path, body) in replies {
            if path.starts_with("/?query=INSERT") {
                assert!(http.respond(token, 200, &[], b""));
            } else if body == b"SELECT 42" {
                assert!(http.respond(token, 200, &[], b"42\n"));
            } else if body == b"SELECT sleep(1)" {
                assert!(http.disconnect(token));
            } else {
                let headers = [("X-ClickHouse-Exception-Code", "62")];
                assert!(http.respond(
                    token,
                    400,
                    &headers,
                    b"Code: 62. DB::Exception: Syntax error"
                ));
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
    fn outcome(&self, id: Option<QueryId>) -> Outcome {
        self.outcomes.iter().find(|(i, _)| Some(*i) == id).map(|(_, o)| o.clone()).unwrap()
    }
}

#[test]
fn pooled_requests_share_the_network_and_map_to_their_outcomes() {
    let mut h = Harness::start(|ch| {
        ch.with_credentials("writer", "secret")
            .with_database("telemetry")
            .with_setting("async_insert", "1")
            .with_connections(2)
    });
    let rows = b"\x01\x00\n\xff\x00";
    let (mut insert, mut select, mut bad) = (None, None, None);
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && h.outcomes.len() < 3 {
        h.tick();
        if insert.is_none() {
            insert = h.ch.insert(&mut h.http, "INSERT INTO t FORMAT RowBinary", rows);
        }
        if select.is_none() {
            select = h.ch.query(&mut h.http, "SELECT 42");
        }
        if bad.is_none() {
            bad = h.ch.query(&mut h.http, "SELEC");
        }
    }
    assert_eq!(h.outcome(insert), Outcome::Ok(Vec::new()));
    assert_eq!(h.outcome(select), Outcome::Ok(b"42\n".to_vec()));
    assert_eq!(h.outcome(bad), Outcome::Server {
        status: 400,
        code: Some(62),
        message: b"Code: 62. DB::Exception: Syntax error".to_vec(),
    });

    let insert_request = h.requests.iter().find(|r| r.path.starts_with("/?query=")).unwrap();
    assert_eq!(
        insert_request.path,
        "/?query=INSERT%20INTO%20t%20FORMAT%20RowBinary&wait_end_of_query=1&database=telemetry&async_insert=1"
    );
    assert_eq!(insert_request.body, rows);
    assert_eq!(insert_request.user, b"writer");
    assert_eq!(insert_request.key, b"secret");
    let select_request = h.requests.iter().find(|r| r.body == b"SELECT 42").unwrap();
    assert_eq!(select_request.path, "/?wait_end_of_query=1&database=telemetry&async_insert=1");
}

#[test]
fn dropped_connection_fails_in_flight_query_then_reconnects() {
    let mut h = Harness::start(|ch| ch);
    let (mut dropped, mut retry) = (None, None);
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && h.outcomes.len() < 2 {
        h.tick();
        if dropped.is_none() {
            dropped = h.ch.query(&mut h.http, "SELECT sleep(1)");
        } else if h.outcomes.len() == 1 && retry.is_none() {
            retry = h.ch.query(&mut h.http, "SELECT 42");
        }
    }
    assert_eq!(h.outcomes, [
        (dropped.unwrap(), Outcome::Disconnected),
        (retry.unwrap(), Outcome::Ok(b"42\n".to_vec()))
    ]);
}

/// `RowBinary` rows for a `(id UInt64, value UInt32)` table with `value == id`.
fn rows(n: u64) -> Vec<u8> {
    let mut rows = Vec::with_capacity(n as usize * 12);
    for i in 0..n {
        rows.extend_from_slice(&i.to_le_bytes());
        rows.extend_from_slice(&(i as u32).to_le_bytes());
    }
    rows
}

/// Runs `steps` one at a time against `CLICKHOUSE_ADDR` (default
/// `127.0.0.1:8123`); a step with data is an insert, otherwise a query.
fn live_run(http: &mut HttpNetwork, steps: &[(&str, Option<&[u8]>)]) -> Vec<Outcome> {
    let addr = std::env::var("CLICKHOUSE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8123".to_owned())
        .parse()
        .unwrap();
    let mut ch = ClickHouse::new(addr);
    let mut outcomes = Vec::new();
    let mut pending = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && outcomes.len() < steps.len() {
        http.poll_with(|event| {
            assert!(ch.on_event(&event, |id, result| {
                assert_eq!(Some(id), pending);
                pending = None;
                outcomes.push(record(&result));
            }));
        });
        if pending.is_none() && outcomes.len() < steps.len() {
            pending = match steps[outcomes.len()] {
                (sql, Some(data)) => ch.insert(http, sql, data),
                (sql, None) => ch.query(http, sql),
            };
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(outcomes.len(), steps.len(), "{outcomes:?}");
    outcomes
}

#[test]
#[ignore = "needs a ClickHouse server; set CLICKHOUSE_ADDR (default 127.0.0.1:8123)"]
fn live_server_roundtrip() {
    let table = format!("flux_clickhouse_test_{}", std::process::id());
    let rows = rows(4);
    let outcomes = live_run(&mut HttpNetwork::default(), &[
        (&format!("CREATE TABLE {table} (id UInt64, value UInt32) ENGINE = Memory"), None),
        (&format!("INSERT INTO {table} FORMAT RowBinary"), Some(&rows)),
        (&format!("SELECT sum(id), sum(value) FROM {table} FORMAT TabSeparated"), None),
        ("SELEC", None),
        (&format!("DROP TABLE {table}"), None),
    ]);
    assert_eq!(outcomes[0], Outcome::Ok(Vec::new()));
    assert_eq!(outcomes[1], Outcome::Ok(Vec::new()));
    assert_eq!(outcomes[2], Outcome::Ok(b"6\t6\n".to_vec()));
    assert!(matches!(&outcomes[3], Outcome::Server { code: Some(62), .. }), "{:?}", outcomes[3]);
    assert_eq!(outcomes[4], Outcome::Ok(Vec::new()));
}

#[test]
#[ignore = "needs a ClickHouse server; set CLICKHOUSE_ADDR (default 127.0.0.1:8123)"]
fn live_large_insert_lands_whole() {
    let table = format!("flux_clickhouse_large_{}", std::process::id());
    let n = 5_000_000;
    let rows = rows(n);
    let mut http = HttpNetwork::default().with_max_body_bytes(2 * rows.len());
    let outcomes = live_run(&mut http, &[
        (
            &format!(
                "CREATE TABLE {table} (id UInt64, value UInt32) ENGINE = MergeTree ORDER BY id"
            ),
            None,
        ),
        (&format!("INSERT INTO {table} FORMAT RowBinary"), Some(&rows)),
        (&format!("SELECT count(), sum(value) FROM {table} FORMAT TabSeparated"), None),
        (&format!("DROP TABLE {table}"), None),
    ]);
    assert_eq!(outcomes[1], Outcome::Ok(Vec::new()));
    let sum: u64 = (0..n).sum();
    assert_eq!(outcomes[2], Outcome::Ok(format!("{n}\t{sum}\n").into_bytes()));
}
