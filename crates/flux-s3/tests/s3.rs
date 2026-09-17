use std::{
    net::Ipv4Addr,
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    http::{HttpEvent, HttpNetwork},
    tcp::TcpNetwork,
};
use flux_s3::{Error, S3};

const NOT_FOUND: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>nope</Message></Error>"#;
const LIST: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Contents><Key>a/b</Key></Contents></ListBucketResult>"#;

#[test]
fn objects_round_trip_put_is_retried_and_errors_map() {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    // One network is both the fake server and the client's pool.
    let mut net = TcpNetwork::default();
    let mut http = HttpNetwork::default();
    http.listen(&mut net, addr).unwrap();
    let s3 = S3::new(&mut http, &mut net, addr, 2)
        .with_credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    let put = s3.put_object(&mut http, "bucket", "some key+a/b", b"hello".to_vec()).unwrap();
    let get = s3.get_object(&mut http, "bucket", "key").unwrap();
    let missing = s3.get_object(&mut http, "bucket", "missing").unwrap();
    let delete = s3.delete_object(&mut http, "bucket", "key").unwrap();
    let list = s3.list_objects(&mut http, "bucket", Some("a/b")).unwrap();
    let mut requests = Vec::new();
    let mut outcomes = Vec::new();
    let mut puts = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && outcomes.len() < 5 {
        let mut replies = Vec::new();
        net.poll_with(|event| {
            http.on_event(&event);
        });
        http.drive(&mut net, |event| {
            if let Some((id, result)) = s3.outcome(&event) {
                outcomes.push((id, match result {
                    Ok(body) => Ok(body.to_vec()),
                    Err(Error::Server { status, code, message }) => {
                        Err((status, code.map(str::to_owned), message.to_vec()))
                    }
                    Err(err) => panic!("{err:?}"),
                }));
            } else if let HttpEvent::Request { token, request } = event {
                if request.method == "PUT" {
                    puts += 1;
                }
                requests.push((
                    request.method.to_owned(),
                    request.path.to_owned(),
                    request.header("authorization").unwrap().to_vec(),
                    request.header("x-amz-date").unwrap().to_vec(),
                    request.body.to_vec(),
                ));
                replies.push((token, request.method.to_owned(), request.path.to_owned(), puts));
            }
        });
        for (token, method, path, attempt) in replies {
            if method == "PUT" && attempt == 1 {
                // The first attempt is cut off; the pool must resend it.
                http.disconnect(&mut net, token);
            } else if path == "/bucket/missing" {
                http.respond(&mut net, token, 404, &[], NOT_FOUND);
            } else if method == "GET" && path == "/bucket/key" {
                http.respond(&mut net, token, 200, &[], b"hello");
            } else if method == "DELETE" {
                http.respond(&mut net, token, 204, &[], b"");
            } else if path.starts_with("/bucket?") {
                http.respond(&mut net, token, 200, &[], LIST);
            } else {
                http.respond(&mut net, token, 200, &[], b"");
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
    outcomes.sort_by_key(|(id, _)| *id);
    assert_eq!(outcomes, [
        (put, Ok(Vec::new())),
        (get, Ok(b"hello".to_vec())),
        (missing, Err((404, Some("NoSuchKey".to_owned()), NOT_FOUND.to_vec()))),
        (delete, Ok(Vec::new())),
        (list, Ok(LIST.to_vec())),
    ]);
    assert_eq!(requests.len(), 6);
    for (_, _, authorization, date, _) in &requests {
        assert!(authorization.starts_with(b"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/"));
        assert_eq!(date.len(), 16);
        assert_eq!(date[15], b'Z');
    }
    let puts: Vec<_> = requests.iter().filter(|request| request.0 == "PUT").collect();
    assert_eq!(puts.len(), 2);
    assert_eq!(puts[0].1, "/bucket/some%20key%2Ba/b");
    assert_eq!((&puts[0].2, &puts[0].4), (&puts[1].2, &puts[1].4));
    assert!(requests.iter().any(|request| request.1 == "/bucket?list-type=2&prefix=a/b"));
}
