//! End-to-end TLS behaviour for HTTP pools and listeners.

use std::{
    io::{Read, Write},
    net::Ipv4Addr,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    Network,
    http::{HttpEvent, HttpNetwork},
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
    let mut net = Network::default();
    let mut http =
        HttpNetwork::default().with_max_body_bytes(64 * 1024).with_tls_config(client_config);
    let pool = http.pool_tls(&mut net, addr, "localhost", 1);
    let first = http.send(&mut net, pool, "GET", "/", &[("Host", "localhost")], &[], 0).unwrap();
    let second = http.send(&mut net, pool, "GET", "/", &[("Host", "localhost")], &[], 0).unwrap();
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

/// Server config for a fresh self-signed `localhost` cert, and a client
/// trusting only it.
fn tls_pair() -> (ServerConfig, ClientConfig) {
    let key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert_der = key.cert.der().clone();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut server = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert_der.clone()],
            PrivatePkcs8KeyDer::from(key.signing_key.serialize_der()).into(),
        )
        .unwrap();
    server.alpn_protocols = vec![b"http/1.1".to_vec()];
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(vec![cert_der]);
    let mut client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![b"http/1.1".to_vec()];
    (server, client)
}

/// Sends one keep-alive request per path, each after a message on `go`;
/// returns the responses. The server echoes the path as the body.
fn tls_client(
    addr: std::net::SocketAddr,
    config: ClientConfig,
    paths: &'static [&'static str],
    go: std::sync::mpsc::Receiver<()>,
) -> thread::JoinHandle<Vec<Vec<u8>>> {
    thread::spawn(move || {
        let mut sock = std::net::TcpStream::connect(addr).unwrap();
        sock.set_read_timeout(Some(TIMEOUT)).unwrap();
        let mut conn =
            rustls::ClientConnection::new(Arc::new(config), "localhost".try_into().unwrap())
                .unwrap();
        let mut tls = rustls::Stream::new(&mut conn, &mut sock);
        let mut responses = Vec::new();
        for path in paths {
            go.recv().unwrap();
            write!(tls, "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\nbody")
                .unwrap();
            tls.flush().unwrap();
            let mut response = Vec::new();
            let mut byte = [0u8; 1];
            while !response.ends_with(path.as_bytes()) {
                tls.read_exact(&mut byte).unwrap();
                response.push(byte[0]);
            }
            responses.push(response);
        }
        responses
    })
}

#[test]
fn tls_listener_rotates_config_without_dropping_connections() {
    let (old_server, old_client) = tls_pair();
    // The rotated listener configures no ALPN, so a client that sends none
    // is served.
    let (mut new_server, mut new_client) = tls_pair();
    new_server.alpn_protocols.clear();
    new_client.alpn_protocols.clear();
    let addr = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap();
    let mut net = Network::default();
    let mut http = HttpNetwork::default();
    let mut h2 = old_server.clone();
    h2.alpn_protocols = vec![b"h2".to_vec()];
    assert!(http.listen_tls(&mut net, addr, Arc::new(h2)).is_err());
    let listener = http.listen_tls(&mut net, addr, Arc::new(old_server)).unwrap();

    let deadline = Instant::now() + TIMEOUT;
    let mut served = 0;
    let mut serve = |net: &mut Network, http: &mut HttpNetwork, until: usize| {
        while Instant::now() < deadline && served < until {
            let mut replies = Vec::new();
            net.poll_with(|event| {
                http.on_event(&event);
            });
            http.drive(net, |event| {
                if let HttpEvent::Request { token, request } = event {
                    assert_eq!(request.body, b"body");
                    replies.push((token, request.path.to_owned()));
                }
            });
            for (token, path) in replies {
                assert!(http.respond(net, token, 200, &[], path.as_bytes()));
                served += 1;
            }
        }
        assert_eq!(served, until);
    };

    let (old_go, go) = std::sync::mpsc::channel();
    let old = tls_client(addr, old_client, &["/one", "/two"], go);
    old_go.send(()).unwrap();
    serve(&mut net, &mut http, 1);
    http.set_tls_config(&mut net, listener, Arc::new(new_server)).unwrap();
    old_go.send(()).unwrap();
    let (new_go, go) = std::sync::mpsc::channel();
    let new = tls_client(addr, new_client, &["/three"], go);
    new_go.send(()).unwrap();
    serve(&mut net, &mut http, 3);
    while !(old.is_finished() && new.is_finished()) && Instant::now() < deadline {
        net.poll_with(|event| {
            http.on_event(&event);
        });
        http.drive(&mut net, |_| {});
    }
    for response in old.join().unwrap().into_iter().chain(new.join().unwrap()) {
        assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    }
}
