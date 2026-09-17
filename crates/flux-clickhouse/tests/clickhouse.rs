use std::{
    net::Ipv4Addr,
    thread,
    time::{Duration, Instant},
};

use flux_clickhouse::{ClickHouse, Error};
use flux_network::http::{HttpEvent, HttpNetwork};
use serde::Serialize;

#[derive(Serialize)]
struct Row {
    name: String,
    maybe: Option<u64>,
    count: u64,
    wide: u128,
    small: u8,
    flag: bool,
    ratio: f64,
    tags: Vec<u8>,
    note: Option<String>,
}

// ClickHouse 26.3: SELECT 'ab', toNullable(toUInt64(7)), toUInt64(2),
// toUInt128(2), toUInt8(3), true, 1.5, [toUInt8(9), toUInt8(8)], CAST(NULL,
// 'Nullable(String)') FORMAT RowBinary
const ROW: &[u8] = &[
    2, b'a', b'b', 0, 7, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 0, 0, 0, 0xf8, 0x3f, 2, 9, 8, 1,
];

#[test]
fn insert_survives_a_dropped_connection_and_errors_map() {
    let row = Row {
        name: "ab".to_owned(),
        maybe: Some(7),
        count: 2,
        wide: 2,
        small: 3,
        flag: true,
        ratio: 1.5,
        tags: vec![9, 8],
        note: None,
    };
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    // One network is both the fake server and the client's pool.
    let mut http = HttpNetwork::default();
    http.listen(addr).unwrap();
    let mut ch =
        ClickHouse::new(addr).with_credentials("w", "s").with_database("db").with_connections(2);
    let insert = ch.insert_rows("t", &[row]).unwrap();
    let bad = ch.query("SELEC");
    let mut inserts = Vec::new();
    let mut outcomes = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && outcomes.len() < 2 {
        let mut replies = Vec::new();
        http.poll_with(|event| {
            if ch.on_event(&event) {
                return
            }
            if let HttpEvent::Request { token, request } = event {
                if request.path.starts_with("/?query=") {
                    inserts.push((
                        request.path.to_owned(),
                        request.header("x-clickhouse-key").unwrap().to_vec(),
                        request.body.to_vec(),
                    ));
                }
                replies.push((token, request.path.starts_with("/?query=")));
            }
        });
        for (token, is_insert) in replies {
            if !is_insert {
                http.respond(token, 400, &[("X-ClickHouse-Exception-Code", "62")], b"Code: 62");
            } else if inserts.len() == 1 {
                // The first attempt is cut off; the client must resend it.
                http.disconnect(token);
            } else {
                http.respond(token, 200, &[], b"");
            }
        }
        ch.drive(&mut http, |id, outcome| outcomes.push((id, outcome)));
        thread::sleep(Duration::from_millis(1));
    }
    outcomes.sort_by_key(|(id, _)| *id);
    assert_eq!(outcomes, [
        (insert, Ok(Vec::new())),
        (bad, Err(Error::Server { status: 400, code: Some(62), message: b"Code: 62".to_vec() })),
    ]);
    assert_eq!(inserts.len(), 2);
    for (path, key, body) in &inserts {
        assert_eq!(
            path,
            "/?query=INSERT%20INTO%20t%20%28name%2C%20maybe%2C%20count%2C%20wide%2C%20small%2C%20flag%2C%20ratio%2C%20tags%2C%20note%29%20FORMAT%20RowBinary&wait_end_of_query=1&database=db"
        );
        assert_eq!((key.as_slice(), body.as_slice()), (&b"s"[..], ROW));
    }
}
