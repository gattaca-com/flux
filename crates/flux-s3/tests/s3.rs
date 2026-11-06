use std::{
    net::Ipv4Addr,
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Network,
    http::{HttpEvent, HttpNetwork},
};
use flux_s3::{Error, S3, ServerSideEncryption, UploadOptions};

const NOT_FOUND: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>nope</Message></Error>"#;
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
fn upload_options_are_signed_for_put_and_multipart_and_survive_retry() {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let mut net = Network::default();
    let mut server = HttpNetwork::default();
    server.listen(&mut net, addr).unwrap();
    let mut s3 = S3::new(addr, 1);
    s3.set_credentials("access", "secret", Some("session"));
    s3.connect(&mut net);
    let options = UploadOptions {
        storage_class: Some("INTELLIGENT_TIERING"),
        content_type: Some("application/octet-stream"),
        tags: &[("data set", "a/b+&=雪"), ("empty", "")],
        encryption: Some(ServerSideEncryption::Kms {
            key_id: Some("arn:aws:kms:us-east-1:123456789012:key/example"),
            bucket_key_enabled: Some(true),
        }),
    };
    s3.put_object_with_options(&mut net, "bucket", "put", b"hello", &options).unwrap();
    s3.create_multipart_upload_with_options(&mut net, "bucket", "multipart", &options).unwrap();
    s3.put_object_with_options(&mut net, "bucket", "sse-s3", b"hello", &UploadOptions {
        encryption: Some(ServerSideEncryption::S3),
        ..UploadOptions::default()
    })
    .unwrap();
    let kms_defaults = UploadOptions {
        encryption: Some(ServerSideEncryption::Kms {
            key_id: None,
            bucket_key_enabled: Some(false),
        }),
        ..UploadOptions::default()
    };
    s3.create_multipart_upload_with_options(&mut net, "bucket", "kms-default-key", &kms_defaults)
        .unwrap();
    s3.put_object_with_options(&mut net, "bucket", "defaults", b"hello", &UploadOptions::default())
        .unwrap();
    s3.create_multipart_upload(&mut net, "bucket", "defaults").unwrap();
    let mut requests = Vec::new();
    let mut completed = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && completed < 6 {
        net.poll_with(|event| {
            if !s3.on_event(&event) {
                server.on_event(&event);
            }
        });
        s3.drive(&mut net, |_, result| {
            result.unwrap();
            completed += 1;
        });
        let mut replies = Vec::new();
        server.drive(&mut net, |event| {
            if let HttpEvent::Request { token, request } = event {
                let authorization =
                    std::str::from_utf8(request.header("authorization").unwrap()).unwrap();
                let signed = authorization
                    .split("SignedHeaders=")
                    .nth(1)
                    .unwrap()
                    .split(',')
                    .next()
                    .unwrap();
                let mut headers: Vec<_> = request
                    .headers
                    .iter()
                    .filter(|header| header.name != "Authorization")
                    .map(|header| (header.name.to_ascii_lowercase(), header.value.to_vec()))
                    .collect();
                headers.sort();
                let option_headers: Vec<_> = headers
                    .iter()
                    .filter(|(name, _)| name == "content-type" || name.starts_with("x-amz-"))
                    .map(|(name, _)| name.as_str())
                    .collect();
                let mut signed_headers: Vec<_> = signed.split(';').collect();
                signed_headers.retain(|name| *name != "host");
                assert_eq!(signed_headers, option_headers);
                assert_eq!(request.header("x-amz-security-token"), Some(b"session".as_slice()));
                requests.push((request.method.to_owned(), request.path.to_owned(), headers));
                replies.push((token, requests.len() == 1));
            }
        });
        for (token, disconnect) in replies {
            if disconnect {
                server.disconnect(&mut net, token);
            } else {
                server.respond(&mut net, token, 200, &[], b"");
            }
        }
        thread::yield_now();
    }
    assert_eq!(completed, 6);
    assert_eq!(requests.len(), 7);
    assert_eq!(requests[0], requests[1]);
    for (method, path, headers) in &requests {
        assert_upload_options(method, path, headers);
    }
}

fn assert_upload_options(method: &str, path: &str, headers: &[(String, Vec<u8>)]) {
    let expected = [
        ("content-type", "application/octet-stream"),
        ("x-amz-server-side-encryption", "aws:kms"),
        (
            "x-amz-server-side-encryption-aws-kms-key-id",
            "arn:aws:kms:us-east-1:123456789012:key/example",
        ),
        ("x-amz-server-side-encryption-bucket-key-enabled", "true"),
        ("x-amz-storage-class", "INTELLIGENT_TIERING"),
        ("x-amz-tagging", "data%20set=a%2Fb%2B%26%3D%E9%9B%AA&empty="),
    ];
    let options: Vec<_> = headers
        .iter()
        .filter(|(name, _)| {
            name == "content-type" ||
                name == "x-amz-storage-class" ||
                name == "x-amz-tagging" ||
                name.starts_with("x-amz-server-side-encryption")
        })
        .map(|(name, value)| (name.as_str(), std::str::from_utf8(value).unwrap()))
        .collect();
    match path {
        "/bucket/put" | "/bucket/multipart?uploads=" => {
            assert_eq!(method, if path == "/bucket/put" { "PUT" } else { "POST" });
            assert_eq!(options, expected);
        }
        "/bucket/sse-s3" => assert_eq!(options, [("x-amz-server-side-encryption", "AES256")]),
        "/bucket/kms-default-key?uploads=" => assert_eq!(options, [
            ("x-amz-server-side-encryption", "aws:kms"),
            ("x-amz-server-side-encryption-bucket-key-enabled", "false"),
        ]),
        "/bucket/defaults" | "/bucket/defaults?uploads=" => assert!(options.is_empty()),
        other => panic!("unexpected path {other}"),
    }
}
