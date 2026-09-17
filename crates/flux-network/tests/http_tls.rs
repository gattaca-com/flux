//! End-to-end TLS behaviour for outbound HTTP pools.

use std::{
    io::{Read, Write},
    net::Ipv4Addr,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    http::{HttpEvent, HttpNetwork},
    tcp::TcpNetwork,
    tls::rustls::{
        self, ClientConfig, RootCertStore, ServerConfig, ServerConnection,
        pki_types::PrivatePkcs8KeyDer,
    },
};

const TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn tls_pool_round_trips_big_bodies() {
    fn body() -> Vec<u8> {
        (0..48 * 1024).map(|i| (i % 251) as u8).collect()
    }
    let key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert_der = key.cert.der().clone();
    let server_config = Arc::new(
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert_der.clone()],
                PrivatePkcs8KeyDer::from(key.signing_key.serialize_der()).into(),
            )
            .unwrap(),
    );
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    thread::spawn(move || {
        let (sock, _) = listener.accept().unwrap();
        let mut sock = sock;
        let mut conn = ServerConnection::new(server_config).unwrap();
        let mut tls = rustls::Stream::new(&mut conn, &mut sock);
        let expected = body();
        for _ in 0..2 {
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                tls.read_exact(&mut byte).unwrap();
                head.push(byte[0]);
            }
            write!(tls, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", expected.len()).unwrap();
            tls.write_all(&expected).unwrap();
            tls.flush().unwrap();
        }
        let _ = done_rx.recv();
    });

    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(vec![cert_der]);
    let client_config = Arc::new(
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let mut net = TcpNetwork::default();
    let mut http =
        HttpNetwork::default().with_max_body_bytes(64 * 1024).with_tls_config(client_config);
    let pool = http.pool_tls(&mut net, addr, "localhost", 1);
    let first = http.send(pool, "GET", "/", &[("Host", "localhost")], Vec::new(), 0).unwrap();
    let second = http.send(pool, "GET", "/", &[("Host", "localhost")], Vec::new(), 0).unwrap();
    let expected = body();
    let mut bodies = Vec::new();
    let mut disconnects = 0;
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && bodies.len() < 2 {
        net.poll_with(|event| {
            http.on_event(&event);
        });
        http.drive(&mut net, |event| match event {
            HttpEvent::Response { id: Some(id), response, .. } => {
                assert!(id == first || id == second);
                bodies.push(response.body.to_vec());
            }
            HttpEvent::Disconnected { .. } => disconnects += 1,
            HttpEvent::Failed { reason, .. } => panic!("failed: {reason:?}"),
            _ => {}
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(bodies, [expected.clone(), expected]);
    assert_eq!(disconnects, 0);
    drop(done_tx);
}
