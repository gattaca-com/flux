use std::{
    net::Ipv4Addr,
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Network,
    http::{HttpEvent, HttpNetwork},
};
use flux_s3::{Error, S3, upload_id};

const NOT_FOUND: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>nope</Message></Error>"#;
const INITIATED: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>up/1</UploadId></InitiateMultipartUploadResult>"#;
const LATE_ERROR: &[u8] = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n<Error><Code>InternalError</Code><Message>retry</Message></Error>";
const LIST: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Contents><Key>a/b</Key></Contents></ListBucketResult>"#;

#[test]
fn objects_round_trip_put_is_retried_and_errors_map() {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    // One poll carries the fake server and the client, each with its own
    // HTTP layer.
    let mut net = Network::default();
    let mut server = HttpNetwork::default();
    server.listen(&mut net, addr).unwrap();
    let mut s3 = S3::new(addr, 2)
        .with_credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    s3.connect(&mut net);
    let put = s3.put_object(&mut net, "bucket", "some key+a/b", b"hello").unwrap();
    let get = s3.get_object(&mut net, "bucket", "key").unwrap();
    let missing = s3.get_object(&mut net, "bucket", "missing").unwrap();
    let delete = s3.delete_object(&mut net, "bucket", "key").unwrap();
    let list = s3.list_objects(&mut net, "bucket", Some("a/b"), Some("t/0")).unwrap();
    let mut requests = Vec::new();
    let mut outcomes = Vec::new();
    let mut puts = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && outcomes.len() < 5 {
        let mut replies = Vec::new();
        net.poll_with(|event| {
            if !s3.on_event(&event) {
                server.on_event(&event);
            }
        });
        s3.drive(&mut net, |id, result| {
            outcomes.push((id, match result {
                Ok(response) => Ok(response.body.to_vec()),
                Err(Error::Server { status, code, message }) => {
                    Err((status, code.map(str::to_owned), message.to_vec()))
                }
                Err(err) => panic!("{err:?}"),
            }));
        });
        server.drive(&mut net, |event| {
            if let HttpEvent::Request { token, request } = event {
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
                server.disconnect(&mut net, token);
            } else if path == "/bucket/missing" {
                server.respond(&mut net, token, 404, &[], NOT_FOUND);
            } else if method == "GET" && path == "/bucket/key" {
                server.respond(&mut net, token, 200, &[], b"hello");
            } else if method == "DELETE" {
                server.respond(&mut net, token, 204, &[], b"");
            } else if path.starts_with("/bucket?") {
                server.respond(&mut net, token, 200, &[], LIST);
            } else {
                server.respond(&mut net, token, 200, &[], b"");
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
    assert!(
        requests
            .iter()
            .any(|request| request.1 == "/bucket?continuation-token=t%2F0&list-type=2&prefix=a%2Fb")
    );
}

#[test]
fn multipart_upload_carries_ids_etags_tokens_and_late_errors() {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let mut net = Network::default();
    let mut server = HttpNetwork::default();
    server.listen(&mut net, addr).unwrap();
    let mut s3 = S3::new(addr, 1);
    s3.set_credentials("ASIAEXAMPLE", "secret", Some("token+/="));
    s3.connect(&mut net);

    let create = s3.create_multipart_upload(&mut net, "bucket", "key").unwrap();
    let mut requests = Vec::new();
    let mut outcomes = Vec::new();
    let mut step = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && outcomes.len() < 4 {
        let mut replies = Vec::new();
        net.poll_with(|event| {
            if !s3.on_event(&event) {
                server.on_event(&event);
            }
        });
        let mut finished = Vec::new();
        s3.drive(&mut net, |id, result| {
            finished.push((id, match result {
                Ok(response) => {
                    Ok((response.body.to_vec(), response.header("etag").map(<[u8]>::to_vec)))
                }
                Err(Error::Server { status, code, .. }) => Err((status, code.map(str::to_owned))),
                Err(err) => panic!("{err:?}"),
            }));
        });
        for (id, outcome) in finished {
            if id == create {
                let (body, _) = outcome.clone().unwrap();
                let upload = upload_id(&body).unwrap();
                s3.upload_part(&mut net, "bucket", "key", upload, 1, b"part").unwrap();
            } else if step == 1 {
                let (_, etag) = outcome.clone().unwrap();
                let etag = String::from_utf8(etag.unwrap()).unwrap();
                s3.complete_multipart_upload(&mut net, "bucket", "key", "up/1", [(
                    1,
                    etag.as_str(),
                )])
                .unwrap();
            } else if step == 2 {
                s3.abort_multipart_upload(&mut net, "bucket", "key", "up/1").unwrap();
            }
            step += 1;
            outcomes.push(outcome);
        }
        server.drive(&mut net, |event| {
            if let HttpEvent::Request { token, request } = event {
                requests.push((
                    request.method.to_owned(),
                    request.path.to_owned(),
                    request.header("authorization").unwrap().to_vec(),
                    request.header("x-amz-security-token").unwrap().to_vec(),
                    request.body.to_vec(),
                ));
                replies.push((token, request.method.to_owned()));
            }
        });
        for (token, method) in replies {
            match (method.as_str(), requests.len()) {
                ("POST", 1) => server.respond(&mut net, token, 200, &[], INITIATED),
                ("PUT", _) => server.respond(&mut net, token, 200, &[("ETag", "\"e1\"")], b""),
                ("POST", _) => server.respond(&mut net, token, 200, &[], LATE_ERROR),
                _ => server.respond(&mut net, token, 204, &[], b""),
            };
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(outcomes, [
        Ok((INITIATED.to_vec(), None)),
        Ok((Vec::new(), Some(b"\"e1\"".to_vec()))),
        Err((200, Some("InternalError".to_owned()))),
        Ok((Vec::new(), None)),
    ]);
    let paths: Vec<_> = requests.iter().map(|r| (r.0.as_str(), r.1.as_str())).collect();
    assert_eq!(paths, [
        ("POST", "/bucket/key?uploads="),
        ("PUT", "/bucket/key?partNumber=1&uploadId=up%2F1"),
        ("POST", "/bucket/key?uploadId=up%2F1"),
        ("DELETE", "/bucket/key?uploadId=up%2F1"),
    ]);
    assert_eq!(
        requests[2].4,
        b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"e1\"</ETag></Part></CompleteMultipartUpload>"
    );
    for (_, _, authorization, token, _) in &requests {
        assert_eq!(token, b"token+/=");
        let authorization = String::from_utf8(authorization.clone()).unwrap();
        assert!(
            authorization.contains(
                "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token,"
            ),
            "{authorization}"
        );
    }
}
