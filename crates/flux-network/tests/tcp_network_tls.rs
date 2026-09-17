//! Transport-level TLS behaviour: the handshake gates the lifecycle, and
//! framing happens inside the session.

use std::{
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use flux_network::{
    tcp::{Framing, TcpEvent, TcpGroupConfig, TcpNetwork},
    tls::{
        Session,
        rustls::{
            self, ClientConfig, RootCertStore, ServerConfig, ServerConnection,
            pki_types::PrivatePkcs8KeyDer,
        },
    },
};

const TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_DELAY: Duration = Duration::from_millis(250);
const HELLO: &[u8] = b"on-connect";

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// A length-prefixed frame as the transport writes it.
fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Reads one length-prefixed frame's payload.
fn read_frame(tls: &mut impl Read) -> Vec<u8> {
    let mut header = [0u8; 12];
    tls.read_exact(&mut header).unwrap();
    let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; len];
    tls.read_exact(&mut payload).unwrap();
    payload
}

#[test]
fn connected_waits_for_the_handshake_and_frames_travel_encrypted() {
    let key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert = key.cert.der().clone();
    let server_config = Arc::new(
        ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.clone()],
                PrivatePkcs8KeyDer::from(key.signing_key.serialize_der()).into(),
            )
            .unwrap(),
    );
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let (echo_tx, echo_rx) = mpsc::channel();
    thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        // The socket is connected well before the handshake even starts, so a
        // client that announced on connect would announce during this window.
        thread::sleep(HANDSHAKE_DELAY);
        let mut conn = ServerConnection::new(server_config).unwrap();
        conn.complete_io(&mut socket).unwrap();
        let mut tls = rustls::Stream::new(&mut conn, &mut socket);
        echo_tx.send(read_frame(&mut tls)).unwrap();
        echo_tx.send(read_frame(&mut tls)).unwrap();
        tls.write_all(&frame(b"pong")).unwrap();
        tls.flush().unwrap();
        // Holds the connection open until the test drops the receiver.
        while echo_tx.send(Vec::new()).is_ok() {
            thread::sleep(Duration::from_millis(5));
        }
    });

    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(vec![cert]);
    let client_config = Arc::new(
        ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let mut network = TcpNetwork::default();
    let group = network.add_group(TcpGroupConfig {
        name: "tls-client",
        framing: Framing::LengthPrefixed,
        on_connect_msg: Some(HELLO.to_vec()),
        ..TcpGroupConfig::default()
    });
    let token =
        network.connect_tls(group, addr, Session::new("localhost").with_config(client_config));

    let mut connected = 0;
    let mut received = Vec::new();
    let poll = |network: &mut TcpNetwork, connected: &mut usize, received: &mut Vec<Vec<u8>>| {
        network.poll_with(|event| match event {
            TcpEvent::Connected { token: got, .. } => {
                assert_eq!(got, token);
                *connected += 1;
            }
            TcpEvent::Message { payload, .. } => received.push(payload.to_vec()),
            TcpEvent::Disconnected { .. } => panic!("the connection dropped"),
            TcpEvent::Accepted { .. } => {}
        });
        thread::sleep(Duration::from_millis(1));
    };

    // Nothing may be announced while the peer has not started the handshake.
    let quiet = Instant::now() + HANDSHAKE_DELAY;
    while Instant::now() < quiet {
        poll(&mut network, &mut connected, &mut received);
        assert_eq!(connected, 0, "Connected arrived before the handshake completed");
    }

    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline && received.is_empty() {
        poll(&mut network, &mut connected, &mut received);
        if connected == 1 {
            // A send accepted right after Connected must reach the peer.
            assert!(network.send_with(token, |buf| buf.extend_from_slice(b"ping")));
            connected += 1;
        }
    }

    assert_eq!(connected, 2, "the handshake never completed");
    assert_eq!(received, [b"pong".to_vec()]);
    // The server saw both payloads decrypted and correctly framed.
    assert_eq!(echo_rx.recv().unwrap(), HELLO);
    assert_eq!(echo_rx.recv().unwrap(), b"ping");
    drop(echo_rx);
}

#[test]
fn a_peer_that_never_speaks_tls_is_redialled_silently() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let counter = accepts.clone();
    let (keep_tx, keep_rx) = mpsc::channel();
    thread::spawn(move || {
        for socket in listener.incoming() {
            // Accepts and then stays silent: never a byte of TLS.
            keep_tx.send(socket.unwrap()).unwrap();
            counter.fetch_add(1, Ordering::Relaxed);
        }
    });

    let mut network = TcpNetwork::default();
    let group = network.add_group(TcpGroupConfig {
        name: "tls-stalled",
        handshake_timeout: flux_timing::Duration::from_millis(200),
        reconnect_interval: flux_timing::Duration::from_millis(100),
        ..TcpGroupConfig::default()
    });
    let _token = network.connect_tls(group, addr, Session::new("localhost"));

    let mut lifecycle = 0;
    let deadline = Instant::now() + Duration::from_millis(1200);
    while Instant::now() < deadline {
        network.poll_with(|event| match event {
            TcpEvent::Connected { .. } | TcpEvent::Disconnected { .. } => lifecycle += 1,
            _ => {}
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(lifecycle, 0, "a handshake that never completed was announced");
    assert!(accepts.load(Ordering::Relaxed) >= 2, "the stalled handshake was never redialled");
    drop(keep_rx);
}
