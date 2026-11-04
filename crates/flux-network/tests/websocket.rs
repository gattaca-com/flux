use std::{
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpStream},
    time::{Duration, Instant},
};

use flux_network::{
    Network, Token,
    websocket::{WsEvent, WsKind, WsNetwork, is_upgrade},
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
    requests: Vec<(Token, String)>,
    opened: Vec<Token>,
    messages: Vec<(Token, WsKind, Vec<u8>)>,
    closes: Vec<Vec<u8>>,
    disconnected: Vec<Token>,
}

fn poll(net: &mut Network, ws: &mut WsNetwork, seen: &mut Seen) {
    net.poll_with(|event| {
        ws.on_event(&event);
    });
    ws.drive(net, |event| match event {
        WsEvent::Request { token, request, .. } => {
            assert!(is_upgrade(&request));
            seen.requests.push((token, request.path.to_owned()));
        }
        WsEvent::Open { token } => seen.opened.push(token),
        WsEvent::Message { token, kind, payload } => {
            seen.messages.push((token, kind, payload.to_vec()));
        }
        WsEvent::Close { payload, .. } => seen.closes.push(payload.to_vec()),
        WsEvent::Disconnected { token } => seen.disconnected.push(token),
    });
}

/// Rejects requests for `/refused`, accepts the rest and echoes data messages.
fn serve(net: &mut Network, ws: &mut WsNetwork, seen: &mut Seen) {
    poll(net, ws, seen);
    for (token, path) in seen.requests.drain(..) {
        if path == "/refused" {
            assert!(ws.reject(net, token, 404, &[], b""));
        } else {
            assert!(ws.accept(net, token));
        }
    }
    for (token, kind, payload) in seen.messages.drain(..) {
        if matches!(kind, WsKind::Text | WsKind::Binary) {
            assert!(ws.send(net, token, kind, &payload));
        }
    }
}

fn server() -> (Network, WsNetwork, SocketAddr) {
    let addr = unused_addr();
    let mut net = Network::default();
    let mut ws = WsNetwork::default().with_max_message_bytes(MAX_MESSAGE);
    ws.listen(&mut net, addr).unwrap();
    (net, ws, addr)
}

#[test]
fn client_redials_and_exchanges_messages() {
    let (mut server_net, mut server_ws, addr) = server();
    let mut server_seen = Seen::default();
    let mut net = Network::default();
    let mut ws = WsNetwork::default()
        .with_max_message_bytes(MAX_MESSAGE)
        .with_reconnect_interval(flux_timing::Duration::from_millis(10));
    let refused = ws.connect(&mut net, addr, "/refused", &[]);
    let token = ws.connect(&mut net, addr, "/feed", &[("Authorization", "key")]);
    let mut seen = Seen::default();
    let large: Vec<u8> = (0..100_000u32).map(|i| i as u8).collect();
    let (mut sent, mut closed) = (false, false);
    let deadline = Instant::now() + TIMEOUT;
    while server_seen.closes.is_empty() || !seen.disconnected.contains(&refused) {
        assert!(Instant::now() < deadline, "timed out");
        serve(&mut server_net, &mut server_ws, &mut server_seen);
        poll(&mut net, &mut ws, &mut seen);
        if seen.opened.contains(&token) && !sent {
            assert!(ws.send(&mut net, token, WsKind::Text, b"hello"));
            assert!(ws.send(&mut net, token, WsKind::Binary, &large));
            assert!(ws.send(&mut net, token, WsKind::Binary, &large[..1000]));
            assert!(ws.send(&mut net, token, WsKind::Ping, b"beat"));
            assert!(!ws.send(&mut net, token, WsKind::Ping, &[0; 126]));
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
            assert!(ws.send_close(&mut net, token, 1000));
            assert!(!ws.send(&mut net, token, WsKind::Text, b"late"));
            closed = true;
        }
    }
    assert!(!seen.opened.contains(&refused), "a refused handshake does not open");
    assert_eq!(server_seen.closes, [1000u16.to_be_bytes().to_vec()]);

    // The closed endpoint redials and opens again.
    seen.opened.clear();
    assert!(ws.remove(&mut net, refused));
    while !seen.opened.contains(&token) {
        assert!(Instant::now() < deadline, "timed out redialling");
        serve(&mut server_net, &mut server_ws, &mut server_seen);
        poll(&mut net, &mut ws, &mut seen);
    }
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
    ws: &mut WsNetwork,
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
    assert_eq!(read_until(net, ws, seen, &mut stream, expected.len()), expected);
    stream
}

fn read_until(
    net: &mut Network,
    ws: &mut WsNetwork,
    seen: &mut Seen,
    stream: &mut TcpStream,
    len: usize,
) -> Vec<u8> {
    let (mut out, mut buf) = (Vec::new(), [0; 4096]);
    let deadline = Instant::now() + TIMEOUT;
    while out.len() < len {
        assert!(Instant::now() < deadline, "timed out with {out:?}");
        serve(net, ws, seen);
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
    let (mut net, mut ws, addr) = server();
    let mut seen = Seen::default();
    let mask = Some([1, 2, 3, 4]);

    // A text message in three fragments, pings between them: one pong, with
    // the last payload.
    let mut stream = raw_client(&mut net, &mut ws, &mut seen, addr);
    let mut bytes = frame(false, 0x1, "héllo ".as_bytes(), mask);
    bytes.extend(frame(true, 0x9, b"a", mask));
    bytes.extend(frame(true, 0x9, b"p", mask));
    bytes.extend(frame(false, 0x0, b"fragmented ", mask));
    bytes.extend(frame(true, 0x0, b"world", mask));
    stream.write_all(&bytes).unwrap();
    let pong = frame(true, 0xA, b"p", None);
    let echo = frame(true, 0x1, "héllo fragmented world".as_bytes(), None);
    let got = read_until(&mut net, &mut ws, &mut seen, &mut stream, pong.len() + echo.len());
    assert_eq!(got, [pong, echo].concat());

    // Oversized frames are refused from their 14-byte headers alone.
    let oversized = |opcode, len| frame(true, opcode, &vec![0; len], mask)[..14].to_vec();
    for (bytes, code) in [
        (frame(true, 0x1, b"unmasked", None), 1002u16),
        (frame(true, 0x1, &[0xFF, 0xFE], mask), 1007),
        (oversized(0x2, MAX_MESSAGE + 1), 1009),
        ([frame(false, 0x2, b"!", mask), oversized(0x0, MAX_MESSAGE)].concat(), 1009),
        (frame(false, 0x9, b"", mask), 1002),
        (frame(true, 0x0, b"orphan", mask), 1002),
        (frame(true, 0x8, &[3], mask), 1002),
        (frame(true, 0x8, &1005u16.to_be_bytes(), mask), 1002),
        (frame(true, 0x8, &[0x03, 0xE8, 0xFF], mask), 1007),
        (vec![0x82, 0xFF, 0x80, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4], 1002),
    ] {
        let mut stream = raw_client(&mut net, &mut ws, &mut seen, addr);
        stream.write_all(&bytes).unwrap();
        let close = frame(true, 0x8, &code.to_be_bytes(), None);
        assert_eq!(read_until(&mut net, &mut ws, &mut seen, &mut stream, usize::MAX), close);
    }
}

#[test]
fn reads_a_continuous_writer_a_poll_at_a_time() {
    let addr = unused_addr();
    let mut net = Network::default();
    let mut ws = WsNetwork::default().with_max_head_bytes(1024).with_max_message_bytes(4096);
    ws.listen(&mut net, addr).unwrap();
    let mut seen = Seen::default();
    let mut stream = raw_client(&mut net, &mut ws, &mut seen, addr);
    let message = frame(true, 0x2, &[7; 1000], Some([1, 2, 3, 4]));
    let (burst, bursts) = (message.repeat(1024), 16);
    stream.set_nonblocking(false).unwrap();
    let writer = std::thread::spawn(move || {
        for _ in 0..bursts {
            stream.write_all(&burst).unwrap();
        }
        stream
    });
    // A poll reads a message and head's worth plus the 32 KiB read crossing it.
    let max_per_poll = (1024 + 4096 + 32 * 1024) / message.len() + 1;
    let (mut received, deadline) = (0, Instant::now() + TIMEOUT);
    while received < 1024 * bursts {
        assert!(Instant::now() < deadline, "timed out after {received} messages");
        poll(&mut net, &mut ws, &mut seen);
        assert!(seen.messages.len() <= max_per_poll, "{} in one poll", seen.messages.len());
        assert!(seen.messages.iter().all(|(_, _, payload)| payload == &[7; 1000]));
        received += seen.messages.len();
        seen.messages.clear();
    }
    assert!(seen.disconnected.is_empty());
    drop(writer.join().unwrap());
}
