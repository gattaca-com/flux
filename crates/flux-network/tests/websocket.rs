use std::{
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpStream},
    time::{Duration, Instant},
};

use flux_network::{
    Network, Token,
    http::{HttpEvent, HttpNetwork},
    websocket::WsKind,
};

const TIMEOUT: Duration = Duration::from_secs(10);
const MAX_MESSAGE: usize = 256 * 1024;

fn unused_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

#[derive(Default)]
struct Seen {
    upgrades: Vec<(Token, Vec<u8>)>,
    messages: Vec<(Token, WsKind, Vec<u8>)>,
    opened: Vec<Token>,
    closes: Vec<Vec<u8>>,
    pings: usize,
    disconnected: Vec<Token>,
}

fn poll(net: &mut Network, http: &mut HttpNetwork, seen: &mut Seen) {
    net.poll_with(|event| {
        http.on_event(&event);
    });
    http.drive(net, |event| match event {
        HttpEvent::Request { token, request } => {
            assert!(request.is_websocket_upgrade());
            // Strict servers refuse a bodiless GET that declares a length.
            assert!(request.header("content-length").is_none());
            seen.upgrades.push((token, request.header("sec-websocket-key").unwrap().to_vec()));
        }
        HttpEvent::WsOpen { token } => seen.opened.push(token),
        HttpEvent::WsMessage { token, kind, payload } => {
            seen.messages.push((token, kind, payload.to_vec()));
        }
        HttpEvent::WsClose { payload, .. } => seen.closes.push(payload.to_vec()),
        HttpEvent::Disconnected { token } => seen.disconnected.push(token),
        _ => {}
    });
}

fn serve(net: &mut Network, http: &mut HttpNetwork, seen: &mut Seen) {
    poll(net, http, seen);
    for (token, key) in seen.upgrades.drain(..) {
        assert!(http.accept_websocket(net, token, &key));
    }
    for (token, kind, payload) in &seen.messages {
        seen.pings += usize::from(*kind == WsKind::Ping);
        if matches!(kind, WsKind::Text | WsKind::Binary) {
            assert!(http.ws_send(net, *token, *kind, payload));
        }
    }
    seen.messages.clear();
}

fn server() -> (Network, HttpNetwork, SocketAddr) {
    let addr = unused_addr();
    let mut net = Network::default();
    let mut http = HttpNetwork::default().with_websockets(MAX_MESSAGE);
    http.listen(&mut net, addr).unwrap();
    (net, http, addr)
}

#[test]
fn client_and_server_exchange_messages() {
    let (mut server_net, mut server_http, addr) = server();
    let mut server_seen = Seen::default();
    let mut net = Network::default();
    let mut http = HttpNetwork::default().with_websockets(MAX_MESSAGE);
    let token = http.connect(&mut net, addr);
    let mut seen = Seen::default();
    let large: Vec<u8> = (0..100_000u32).map(|i| i as u8).collect();
    let (mut handshake, mut sent, mut closed) = (false, false, false);
    let deadline = Instant::now() + TIMEOUT;
    while seen.disconnected.is_empty() || server_seen.disconnected.is_empty() {
        assert!(Instant::now() < deadline, "timed out");
        serve(&mut server_net, &mut server_http, &mut server_seen);
        net.poll_with(|event| {
            if let flux_network::NetworkEvent::Connected { token: connected, .. } = event {
                handshake |= connected == token;
            }
            http.on_event(&event);
        });
        http.drive(&mut net, |event| match event {
            HttpEvent::WsOpen { token } => seen.opened.push(token),
            HttpEvent::WsMessage { token, kind, payload } => {
                seen.messages.push((token, kind, payload.to_vec()));
            }
            HttpEvent::Disconnected { token } => seen.disconnected.push(token),
            _ => {}
        });
        if handshake {
            assert!(http.websocket(&mut net, token, "/feed", &[
                ("Host", "localhost"),
                ("Authorization", "key")
            ]));
            handshake = false;
        }
        if !seen.opened.is_empty() && !sent {
            assert!(http.ws_send(&mut net, token, WsKind::Text, b"hello"));
            assert!(http.ws_send(&mut net, token, WsKind::Binary, &large));
            assert!(http.ws_send(&mut net, token, WsKind::Binary, &large[..1000]));
            assert!(http.ws_send(&mut net, token, WsKind::Ping, b"beat"));
            assert!(
                !http.ws_send(&mut net, token, WsKind::Ping, &[0; 126]),
                "control frames carry at most 125 bytes"
            );
            sent = true;
        }
        if seen.messages.len() == 4 && !closed {
            for message in [
                (token, WsKind::Text, b"hello".to_vec()),
                (token, WsKind::Binary, large.clone()),
                (token, WsKind::Binary, large[..1000].to_vec()),
                (token, WsKind::Pong, b"beat".to_vec()),
            ] {
                assert!(seen.messages.contains(&message), "missing {:?}", message.1);
            }
            assert!(http.ws_close(&mut net, token, 1000));
            assert!(
                !http.ws_send(&mut net, token, WsKind::Text, b"late"),
                "nothing is sent after a close"
            );
            closed = true;
        }
    }
    assert_eq!(seen.opened, [token]);
    assert_eq!(server_seen.closes, [1000u16.to_be_bytes().to_vec()]);
    assert_eq!(server_seen.pings, 1);
    http.remove(&mut net, token);
}

fn frame(fin: bool, opcode: u8, payload: &[u8], mask: Option<[u8; 4]>) -> Vec<u8> {
    let mut out = vec![u8::from(fin) << 7 | opcode];
    let mask_bit = if mask.is_some() { 0x80 } else { 0 };
    match payload.len() {
        len @ 0..126 => out.push(mask_bit | len as u8),
        len @ 126..65536 => {
            out.push(mask_bit | 126);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        }
        len => {
            out.push(mask_bit | 127);
            out.extend_from_slice(&(len as u64).to_be_bytes());
        }
    }
    match mask {
        Some(mask) => {
            out.extend_from_slice(&mask);
            out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
        }
        None => out.extend_from_slice(payload),
    }
    out
}

fn raw_client(
    net: &mut Network,
    http: &mut HttpNetwork,
    seen: &mut Seen,
    addr: SocketAddr,
) -> TcpStream {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_nonblocking(true).unwrap();
    stream
        .write_all(
            b"GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\n\
              Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
        )
        .unwrap();
    let expected =
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
                     Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
    assert_eq!(read_until(net, http, seen, &mut stream, expected.len()), expected);
    stream
}

fn read_until(
    net: &mut Network,
    http: &mut HttpNetwork,
    seen: &mut Seen,
    stream: &mut TcpStream,
    len: usize,
) -> Vec<u8> {
    let (mut out, mut buf) = (Vec::new(), [0; 4096]);
    let deadline = Instant::now() + TIMEOUT;
    while out.len() < len {
        assert!(Instant::now() < deadline, "timed out with {out:?}");
        serve(net, http, seen);
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => panic!("{e}"),
        }
    }
    out
}

#[test]
fn server_reassembles_fragments_and_closes_on_violations() {
    let (mut net, mut http, addr) = server();
    let mut seen = Seen::default();
    let mask = Some([1, 2, 3, 4]);

    // A text message in three fragments, pings between them: one pong, with
    // the last payload.
    let mut stream = raw_client(&mut net, &mut http, &mut seen, addr);
    let mut bytes = frame(false, 0x1, "héllo ".as_bytes(), mask);
    bytes.extend(frame(true, 0x9, b"a", mask));
    bytes.extend(frame(true, 0x9, b"p", mask));
    bytes.extend(frame(false, 0x0, b"fragmented ", mask));
    bytes.extend(frame(true, 0x0, b"world", mask));
    stream.write_all(&bytes).unwrap();
    let pong = frame(true, 0xA, b"p", None);
    let echo = frame(true, 0x1, "héllo fragmented world".as_bytes(), None);
    let got = read_until(&mut net, &mut http, &mut seen, &mut stream, pong.len() + echo.len());
    assert_eq!(got, [pong, echo].concat());

    for (bytes, code) in [
        (frame(true, 0x1, b"unmasked", None), 1002u16),
        (frame(true, 0x1, &[0xFF, 0xFE], mask), 1007),
        (frame(true, 0x2, &vec![0; MAX_MESSAGE + 1], mask), 1009),
        (frame(false, 0x9, b"", mask), 1002),
        (frame(true, 0x8, &[3], mask), 1002),
        (vec![0x82, 0xFF, 0x80, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4], 1002),
    ] {
        let mut stream = raw_client(&mut net, &mut http, &mut seen, addr);
        stream.write_all(&bytes).unwrap();
        let close = frame(true, 0x8, &code.to_be_bytes(), None);
        assert_eq!(read_until(&mut net, &mut http, &mut seen, &mut stream, usize::MAX), close);
    }
}

#[test]
fn reads_a_continuous_writer_a_poll_at_a_time() {
    let addr = unused_addr();
    let mut net = Network::default();
    let mut http = HttpNetwork::default()
        .with_max_body_bytes(512)
        .with_websockets(4096)
        .with_socket_buf_size(4 << 20);
    http.listen(&mut net, addr).unwrap();
    let mut seen = Seen::default();
    let mut stream = raw_client(&mut net, &mut http, &mut seen, addr);
    let message = frame(true, 0x2, &[7; 1000], Some([1, 2, 3, 4]));
    let (burst, bursts) = (message.repeat(1024), 16);
    stream.set_nonblocking(false).unwrap();
    let (written, first_written) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        for _ in 0..bursts {
            stream.write_all(&burst).unwrap();
            let _ = written.send(());
        }
        stream
    });
    // With a burst already waiting, a poll still reads only a full-size
    // message, head included, plus the 32KB read crossing it.
    first_written.recv_timeout(TIMEOUT).unwrap();
    let max_per_poll = (16 * 1024 + 4096 + 32 * 1024) / message.len() + 1;
    let (mut received, deadline) = (0, Instant::now() + TIMEOUT);
    while received < 1024 * bursts {
        assert!(Instant::now() < deadline, "timed out after {received} messages");
        poll(&mut net, &mut http, &mut seen);
        assert!(seen.messages.len() <= max_per_poll, "{} in one poll", seen.messages.len());
        assert!(seen.messages.iter().all(|(_, _, payload)| payload == &[7; 1000]));
        received += seen.messages.len();
        seen.messages.clear();
    }
    assert!(seen.disconnected.is_empty());
    drop(writer.join().unwrap());
}
