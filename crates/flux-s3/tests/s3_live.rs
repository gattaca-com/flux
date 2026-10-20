//! Signed round-trip against a real S3 endpoint.
//!
//! Ignored by default so CI stays hermetic; nothing else can validate the
//! signer end to end, because a bad signature is only ever rejected by a
//! server holding the secret. Point it at `MinIO`, `SeaweedFS`, or a real
//! bucket by setting `S3_ENDPOINT` (host:port), `S3_ACCESS_KEY`,
//! `S3_SECRET_KEY`, and `S3_BUCKET`, then run the test with `--ignored`.
//! Set `S3_TLS=1` for an HTTPS endpoint, `S3_REGION` when it is not
//! `us-east-1`, `S3_SESSION_TOKEN` with temporary credentials, and
//! `S3_PREFIX` to keep the test's objects under a prefix such as `test/`.

use std::{
    net::{SocketAddr, ToSocketAddrs},
    thread,
    time::{Duration, Instant},
};

use flux_network::Network;
use flux_s3::{Error, RequestId, S3, upload_id};

const TIMEOUT: Duration = Duration::from_secs(30);
/// Exercises segment encoding: a space, a '+', and a '/' separator.
const KEY: &str = "flux-s3 probe/nested key+1.bin";
/// Exercises query encoding: the '/' must reach the server as `%2F`.
const PREFIX: &str = "flux-s3 probe/";

fn var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for this test"))
}

/// Drives the network until `id` completes, returning its body and `ETag`.
fn settle(
    net: &mut Network,
    s3: &mut S3,
    id: RequestId,
) -> Result<(Vec<u8>, Option<String>), String> {
    let mut outcome = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && outcome.is_none() {
        net.poll_with(|event| {
            s3.on_event(&event);
        });
        s3.drive(net, |done, result| {
            assert_eq!(done, id);
            outcome = Some(match result {
                Ok(response) => Ok((
                    response.body.to_vec(),
                    response.header("etag").map(|etag| String::from_utf8_lossy(etag).into_owned()),
                )),
                Err(Error::Server { status, code, message }) => {
                    Err(format!("{status} {code:?}: {}", String::from_utf8_lossy(message)))
                }
                Err(other) => Err(format!("{other:?}")),
            });
        });
        thread::sleep(Duration::from_millis(1));
    }
    outcome.expect("the request never completed")
}

fn ok(net: &mut Network, s3: &mut S3, id: RequestId) -> (Vec<u8>, Option<String>) {
    settle(net, s3, id).unwrap_or_else(|error| panic!("{error}"))
}

#[test]
#[ignore = "needs an S3 endpoint; see the module docs"]
fn signed_round_trip() {
    let endpoint = var("S3_ENDPOINT");
    let bucket = var("S3_BUCKET");
    let tls = std::env::var("S3_TLS").is_ok_and(|value| value == "1");
    let region = std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_owned());
    let addr: SocketAddr = endpoint
        .to_socket_addrs()
        .expect("S3_ENDPOINT must be host:port")
        .find(SocketAddr::is_ipv4)
        .expect("S3_ENDPOINT resolved to no IPv4 address");

    let mut net = Network::default();
    let mut s3 = if tls {
        let host = endpoint.rsplit_once(':').map_or(endpoint.as_str(), |(host, _)| host);
        S3::new_tls(addr, host, 1)
    } else {
        S3::new(addr, 1)
    }
    .with_region(&region)
    .with_http(|http| http.with_max_body_bytes(8 << 20));
    let session_token = std::env::var("S3_SESSION_TOKEN").ok();
    s3.set_credentials(&var("S3_ACCESS_KEY"), &var("S3_SECRET_KEY"), session_token.as_deref());
    s3.connect(&mut net);
    let prefix = std::env::var("S3_PREFIX").unwrap_or_default();
    let key = format!("{prefix}{KEY}");

    let body: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let id = s3.put_object(&mut net, &bucket, &key, &body).unwrap();
    ok(&mut net, &mut s3, id);

    let id = s3.get_object(&mut net, &bucket, &key).unwrap();
    assert_eq!(ok(&mut net, &mut s3, id).0, body);

    let id = s3.list_objects(&mut net, &bucket, Some(&format!("{prefix}{PREFIX}")), None).unwrap();
    let listing = String::from_utf8(ok(&mut net, &mut s3, id).0).unwrap();
    assert!(listing.contains("nested key+1.bin"), "{listing}");

    let id = s3.delete_object(&mut net, &bucket, &key).unwrap();
    ok(&mut net, &mut s3, id);

    let id = s3.get_object(&mut net, &bucket, &key).unwrap();
    let error = settle(&mut net, &mut s3, id).expect_err("the deleted object is still readable");
    assert!(error.starts_with("404 "), "{error}");

    // Parts below 5 MiB are refused, so the first is exactly the minimum.
    let first: Vec<u8> = (0..5 << 20).map(|i| (i % 253) as u8).collect();
    let id = s3.create_multipart_upload(&mut net, &bucket, &key).unwrap();
    let initiated = ok(&mut net, &mut s3, id).0;
    let upload = upload_id(&initiated).unwrap().to_owned();
    let mut etags = Vec::new();
    for (part_number, part) in [(1, first.as_slice()), (2, body.as_slice())] {
        let id = s3.upload_part(&mut net, &bucket, &key, &upload, part_number, part).unwrap();
        etags.push((part_number, ok(&mut net, &mut s3, id).1.expect("a part has an ETag")));
    }
    let parts = etags.iter().map(|(part_number, etag)| (*part_number, etag.as_str()));
    let id = s3.complete_multipart_upload(&mut net, &bucket, &key, &upload, parts).unwrap();
    ok(&mut net, &mut s3, id);

    let id = s3.head_object(&mut net, &bucket, &key).unwrap();
    assert!(ok(&mut net, &mut s3, id).1.is_some_and(|etag| etag.ends_with("-2\"")));
    let id = s3.delete_object(&mut net, &bucket, &key).unwrap();
    ok(&mut net, &mut s3, id);

    let id = s3.create_multipart_upload(&mut net, &bucket, &key).unwrap();
    let initiated = ok(&mut net, &mut s3, id).0;
    let upload = upload_id(&initiated).unwrap().to_owned();
    let id = s3.upload_part(&mut net, &bucket, &key, &upload, 1, &body).unwrap();
    ok(&mut net, &mut s3, id);
    let id = s3.abort_multipart_upload(&mut net, &bucket, &key, &upload).unwrap();
    ok(&mut net, &mut s3, id);
    let id = s3.head_object(&mut net, &bucket, &key).unwrap();
    let error = settle(&mut net, &mut s3, id).expect_err("an aborted upload left an object");
    assert!(error.starts_with("404 "), "{error}");
}
