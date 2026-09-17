//! Signed round-trip against a real S3 endpoint.
//!
//! Ignored by default so CI stays hermetic; nothing else can validate the
//! signer end to end, because a bad signature is only ever rejected by a
//! server holding the secret. Point it at `MinIO`, `SeaweedFS`, or a real
//! bucket by setting `S3_ENDPOINT` (host:port), `S3_ACCESS_KEY`,
//! `S3_SECRET_KEY`, and `S3_BUCKET`, then run the test with `--ignored`.
//! Set `S3_TLS=1` for an HTTPS endpoint and `S3_REGION` when it is not
//! `us-east-1`.

use std::{
    net::{SocketAddr, ToSocketAddrs},
    thread,
    time::{Duration, Instant},
};

use flux_network::{http::HttpNetwork, tcp::TcpNetwork};
use flux_s3::{Error, RequestId, S3};

const TIMEOUT: Duration = Duration::from_secs(30);
/// Exercises segment encoding: a space, a '+', and a '/' separator.
const KEY: &str = "flux-s3 probe/nested key+1.bin";
/// Exercises query encoding: the '/' must reach the server as `%2F`.
const PREFIX: &str = "flux-s3 probe/";

fn var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for this test"))
}

/// Drives the network until `id` completes, returning its body.
fn settle(
    net: &mut TcpNetwork,
    http: &mut HttpNetwork,
    s3: &S3,
    id: RequestId,
) -> Result<Vec<u8>, String> {
    let mut outcome = None;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && outcome.is_none() {
        net.poll_with(|event| {
            http.on_event(&event);
        });
        http.drive(net, |event| {
            if let Some((done, result)) = s3.outcome(&event) {
                assert_eq!(done, id);
                outcome = Some(match result {
                    Ok(body) => Ok(body.to_vec()),
                    Err(Error::Server { status, code, message }) => {
                        Err(format!("{status} {code:?}: {}", String::from_utf8_lossy(message)))
                    }
                    Err(other) => Err(format!("{other:?}")),
                });
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    outcome.expect("the request never completed")
}

fn ok(net: &mut TcpNetwork, http: &mut HttpNetwork, s3: &S3, id: RequestId) -> Vec<u8> {
    settle(net, http, s3, id).unwrap_or_else(|error| panic!("{error}"))
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

    let mut net = TcpNetwork::default();
    let mut http = HttpNetwork::default().with_max_body_bytes(8 << 20);
    let s3 = if tls {
        let host = endpoint.rsplit_once(':').map_or(endpoint.as_str(), |(host, _)| host);
        S3::new_tls(&mut http, &mut net, addr, host, 1)
    } else {
        S3::new(&mut http, &mut net, addr, 1)
    }
    .with_region(&region)
    .with_credentials(&var("S3_ACCESS_KEY"), &var("S3_SECRET_KEY"));

    let body: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let id = s3.put_object(&mut http, &bucket, KEY, body.clone()).unwrap();
    ok(&mut net, &mut http, &s3, id);

    let id = s3.get_object(&mut http, &bucket, KEY).unwrap();
    assert_eq!(ok(&mut net, &mut http, &s3, id), body);

    let id = s3.list_objects(&mut http, &bucket, Some(PREFIX), None).unwrap();
    let listing = String::from_utf8(ok(&mut net, &mut http, &s3, id)).unwrap();
    assert!(listing.contains("nested key+1.bin"), "{listing}");

    let id = s3.delete_object(&mut http, &bucket, KEY).unwrap();
    ok(&mut net, &mut http, &s3, id);

    let id = s3.get_object(&mut http, &bucket, KEY).unwrap();
    let error =
        settle(&mut net, &mut http, &s3, id).expect_err("the deleted object is still readable");
    assert!(error.starts_with("404 "), "{error}");
}
