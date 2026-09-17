use std::{
    net::Ipv4Addr,
    thread,
    time::{Duration, Instant},
};

use flux_clickhouse::{ClickHouse, Error, QueryId, rowbinary};
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
fn insert_and_error_through_one_network() {
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
    let sql = rowbinary::insert_statement("t", &row).unwrap();
    let mut body = Vec::new();
    rowbinary::encode(&mut body, &row).unwrap();
    assert_eq!(body, ROW);

    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    // One network is both the fake server and the client's pool.
    let mut http = HttpNetwork::default();
    http.listen(addr).unwrap();
    let mut ch =
        ClickHouse::new(addr).with_credentials("w", "s").with_database("db").with_connections(2);
    let (mut insert, mut bad) = (None, None);
    let mut requests = Vec::new();
    let mut outcomes = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && outcomes.len() < 2 {
        let mut replies = Vec::new();
        http.poll_with(|event| {
            if ch.on_event(&event, |id, result| {
                outcomes.push((id, match result {
                    Ok(response) => Ok(response.body.to_vec()),
                    Err(Error::Server { status, code, message }) => {
                        Err((status, code, message.to_vec()))
                    }
                    Err(Error::Disconnected) => panic!("disconnected"),
                }));
            }) {
                return
            }
            if let HttpEvent::Request { token, request } = event {
                requests.push((
                    request.path.to_owned(),
                    request.header("x-clickhouse-key").unwrap().to_vec(),
                    request.body.to_vec(),
                ));
                replies.push((token, request.path.starts_with("/?query=")));
            }
        });
        for (token, is_insert) in replies {
            if is_insert {
                http.respond(token, 200, &[], b"");
            } else {
                http.respond(token, 400, &[("X-ClickHouse-Exception-Code", "62")], b"Code: 62");
            }
        }
        if insert.is_none() {
            insert = ch.insert(&mut http, &sql, &body);
        }
        if bad.is_none() {
            bad = ch.query(&mut http, "SELEC");
        }
        thread::sleep(Duration::from_millis(1));
    }
    let outcome =
        |id: Option<QueryId>| outcomes.iter().find(|(i, _)| Some(*i) == id).unwrap().1.clone();
    assert_eq!(outcome(insert), Ok(Vec::new()));
    assert_eq!(outcome(bad), Err((400, Some(62), b"Code: 62".to_vec())));
    let (path, key, sent) = requests.iter().find(|(p, ..)| p.starts_with("/?query=")).unwrap();
    assert_eq!(
        path,
        "/?query=INSERT%20INTO%20t%20%28name%2C%20maybe%2C%20count%2C%20wide%2C%20small%2C%20flag%2C%20ratio%2C%20tags%2C%20note%29%20FORMAT%20RowBinary&wait_end_of_query=1&database=db"
    );
    assert_eq!((key.as_slice(), sent), (&b"s"[..], &body));
}
