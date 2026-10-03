//! End-to-end calls against a real socket, polled on one thread.

use std::{
    io::{Cursor, ErrorKind, Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    ops::ControlFlow,
    time::{Duration, Instant},
};

use bytes::{BufMut, BytesMut};
use flux_timing::Nanos;

use super::{Code, GrpcConfig, GrpcServer, MessageDecoder, Request, Response, Route, Status};
use crate::{
    Network, PayloadBuf,
    http2::{self, Decoder, Payload, encode_frame, flags, hpack, kind},
};

const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, Eq)]
enum Got {
    /// Decoded fields, `:status` first, and whether the frame ended the stream.
    Headers(Vec<(String, String)>, bool),
    Data(Vec<u8>),
    Reset(u32),
    GoAway,
    Closed,
}

impl Got {
    fn field(&self, name: &str) -> Option<&str> {
        let Self::Headers(fields, _) = self else { return None };
        fields.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str())
    }
}

trait Transport {
    fn send(&mut self, bytes: &[u8]);
    /// Appends whatever plaintext is available; false at end of stream.
    fn recv(&mut self, out: &mut Vec<u8>) -> bool;
}

impl Transport for TcpStream {
    fn send(&mut self, bytes: &[u8]) {
        self.set_nonblocking(false).unwrap();
        self.write_all(bytes).unwrap();
        self.set_nonblocking(true).unwrap();
    }

    fn recv(&mut self, out: &mut Vec<u8>) -> bool {
        let mut buf = [0; 16384];
        loop {
            match self.read(&mut buf) {
                Ok(0) => return false,
                Ok(n) => out.extend_from_slice(&buf[..n]),
                Err(e) if e.kind() == ErrorKind::WouldBlock => return true,
                Err(_) => return false,
            }
        }
    }
}

struct Client<T> {
    transport: T,
    decoder: Decoder,
    hpack: hpack::Decoder,
    input: Vec<u8>,
    block: BytesMut,
    got: Vec<(u32, Got)>,
}

/// A gRPC length-prefixed message.
fn framed(payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0];
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn frame(out: &mut Vec<u8>, ty: u8, fl: u8, id: u32, data: &[u8]) {
    encode_frame(ty, fl, id, data, http2::DEFAULT_MAX_FRAME_SIZE, out).unwrap();
}

fn literal(block: &mut Vec<u8>, name: &str, value: &str) {
    block.extend_from_slice(&[0, name.len() as u8]);
    block.extend_from_slice(name.as_bytes());
    block.push(value.len() as u8);
    block.extend_from_slice(value.as_bytes());
}

/// HEADERS for `path` (POST, http), then `message` if any. The last frame ends
/// the request.
fn request(id: u32, path: &str, content_type: &str, message: Option<&[u8]>) -> Vec<u8> {
    let mut block = vec![0x83, 0x86];
    literal(&mut block, ":path", path);
    literal(&mut block, ":authority", "localhost");
    literal(&mut block, "content-type", content_type);
    literal(&mut block, "te", "trailers");
    literal(&mut block, "authorization", "Bearer token");
    let mut wire = Vec::new();
    let end = if message.is_none() { flags::END_STREAM } else { 0 };
    frame(&mut wire, kind::HEADERS, flags::END_HEADERS | end, id, &block);
    if let Some(message) = message {
        frame(&mut wire, kind::DATA, flags::END_STREAM, id, &framed(message));
    }
    wire
}

impl<T: Transport> Client<T> {
    /// Sends the preface with the given initial stream window.
    fn new(mut transport: T, window: u32) -> Self {
        let mut wire = http2::CLIENT_PREFACE.to_vec();
        let mut settings = vec![0, 4];
        settings.extend_from_slice(&window.to_be_bytes());
        frame(&mut wire, kind::SETTINGS, 0, 0, &settings);
        frame(&mut wire, kind::WINDOW_UPDATE, 0, 0, &(1u32 << 30).to_be_bytes());
        transport.send(&wire);
        Self {
            transport,
            decoder: Decoder::new(http2::DEFAULT_MAX_FRAME_SIZE, 65536).unwrap(),
            hpack: hpack::Decoder::new(4096),
            input: Vec::new(),
            block: BytesMut::new(),
            got: Vec::new(),
        }
    }

    /// Polls the server and reads frames until `done` holds for what arrived.
    fn run(
        &mut self,
        harness: &mut Harness,
        mut route: impl Route,
        done: impl Fn(&[(u32, Got)]) -> bool,
    ) {
        let end = Instant::now() + TIMEOUT;
        while !done(&self.got) {
            assert!(Instant::now() < end, "timed out; got {:?}", self.got);
            harness.server.poll(&mut harness.net, &mut route);
            if !self.transport.recv(&mut self.input) &&
                self.got.last().is_none_or(|(_, got)| *got != Got::Closed)
            {
                self.got.push((0, Got::Closed));
            }
            self.parse();
        }
    }

    fn parse(&mut self) {
        let mut consumed = 0;
        while let Some((f, n)) = self.decoder.decode(&self.input[consumed..]).unwrap() {
            consumed += n;
            let got = match f.payload {
                Payload::Data { data, .. } => Got::Data(data.to_vec()),
                Payload::Headers { fragment, end_stream, end_headers, .. } => {
                    assert!(end_headers, "test client expects single-frame header blocks");
                    self.block.extend_from_slice(fragment);
                    let mut fields = Vec::new();
                    self.hpack
                        .decode(&mut Cursor::new(&mut self.block), |header| {
                            fields.push(match header {
                                hpack::Header::Status(status) => {
                                    (":status".to_owned(), status.as_str().to_owned())
                                }
                                hpack::Header::Field { name, value } => {
                                    (name.as_str().to_owned(), value.to_str().unwrap().to_owned())
                                }
                                other => panic!("unexpected response header {other:?}"),
                            });
                            ControlFlow::Continue(())
                        })
                        .unwrap();
                    self.block.clear();
                    Got::Headers(fields, end_stream)
                }
                Payload::Reset { error_code } => Got::Reset(error_code),
                Payload::GoAway { .. } => Got::GoAway,
                _ => continue,
            };
            self.got.push((f.stream_id, got));
        }
        self.input.drain(..consumed);
    }

    /// Everything received on stream `id`, in order.
    fn stream(&self, id: u32) -> Vec<&Got> {
        self.got.iter().filter(|(s, _)| *s == id).map(|(_, got)| got).collect()
    }
}

/// True once stream `id` has received a frame ending it.
fn ended(id: u32) -> impl Fn(&[(u32, Got)]) -> bool {
    move |got| {
        got.iter().any(|(s, g)| *s == id && matches!(g, Got::Headers(_, true) | Got::Reset(_)))
    }
}

struct Harness {
    net: Network,
    server: GrpcServer,
    addr: SocketAddr,
}

impl Harness {
    fn new(config: GrpcConfig) -> Self {
        let addr = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap();
        let mut net = Network::default();
        let mut server = GrpcServer::new(&mut net, config).unwrap();
        server.listen(&mut net, addr).unwrap();
        Self { net, server, addr }
    }

    fn connect(&self, window: u32) -> Client<TcpStream> {
        let socket = TcpStream::connect(self.addr).unwrap();
        socket.set_nodelay(true).unwrap();
        socket.set_nonblocking(true).unwrap();
        Client::new(socket, window)
    }
}

fn echo(request: &Request<'_>, reply: &mut PayloadBuf<'_>) -> Response {
    match request.path() {
        b"/test.Echo/Unary" => {
            assert_eq!(request.header("authorization"), Some(&b"Bearer token"[..]));
            assert!(!request.tls());
            assert!(request.peer().ip().is_loopback());
            // Through `BufMut`, as Prost's `encode` writes.
            reply.put_slice(request.message());
            Response::Message
        }
        _ => Status::new(Code::Unimplemented, "unknown method").into(),
    }
}

#[test]
fn decoder_borrows_whole_messages_and_joins_fragments() {
    let message = framed(b"hello");
    let mut seen = Vec::new();
    let mut decoder = MessageDecoder::new(16);
    decoder.receive(&message, false, |m| seen.push(m.to_vec())).unwrap();
    for byte in &message {
        decoder.receive(&[*byte], false, |m| seen.push(m.to_vec())).unwrap();
    }
    assert_eq!(seen, [b"hello", b"hello"]);
    assert!(decoder.receive(&message[..3], true, |_| {}).is_err());
    assert_eq!(
        MessageDecoder::new(4).receive(&message, false, |_| {}).unwrap_err().code,
        Code::ResourceExhausted
    );
}

#[test]
fn status_message_is_percent_encoded() {
    let mut trailers = http::HeaderMap::new();
    let mut scratch = bytes::BytesMut::new();
    Status::new(Code::Internal, "a b%").write_trailers(&mut trailers, &mut scratch);
    assert_eq!(trailers["grpc-status"], "13");
    assert_eq!(trailers["grpc-message"], "a%20b%25");
    trailers.clear();
    Status::new(Code::Unauthenticated, "plain").write_trailers(&mut trailers, &mut scratch);
    assert_eq!(trailers["grpc-status"], "16");
    assert_eq!(trailers["grpc-message"], "plain");
}

#[test]
fn unary_calls_reply_and_reject() {
    let mut harness = Harness::new(GrpcConfig::default());
    let mut client = harness.connect(65535);
    let mut wire = request(1, "/test.Echo/Unary", "application/grpc", Some(b"hi"));
    wire.extend(request(3, "/test.Echo/Missing", "application/grpc", Some(b"")));
    wire.extend(request(5, "/test.Echo/Unary", "text/plain", Some(b"")));
    wire.extend(request(7, "/test.Echo/Unary", "application/grpc", None));
    // Byte-at-a-time input exercises retained partial frames.
    for byte in wire {
        client.transport.send(&[byte]);
        harness.server.poll(&mut harness.net, echo);
    }
    client.run(&mut harness, echo, |got| [1, 3, 5, 7].into_iter().all(|id| ended(id)(got)));

    let unary = client.stream(1);
    assert_eq!(unary[0].field(":status"), Some("200"));
    assert_eq!(unary[0].field("content-type"), Some("application/grpc"));
    assert_eq!(unary[1], &Got::Data(framed(b"hi")));
    assert_eq!(unary[2].field("grpc-status"), Some("0"));
    assert_eq!(unary.len(), 3);

    let [missing] = client.stream(3)[..] else { panic!("trailers-only response expected") };
    assert_eq!(missing.field("grpc-status"), Some("12"));
    assert_eq!(missing.field("grpc-message"), Some("unknown%20method"));
    assert_eq!(missing.field("content-type"), Some("application/grpc"));

    assert_eq!(client.stream(5)[0].field(":status"), Some("415"));
    assert_eq!(client.stream(7)[0].field("grpc-status"), Some("13"));
}

#[test]
fn received_at_is_when_the_headers_frame_started_arriving() {
    let mut harness = Harness::new(GrpcConfig::default());
    let mut client = harness.connect(65535);
    let wire = request(1, "/test.Echo/Unary", "application/grpc", Some(b"hi"));
    let start = Nanos::now();
    // The HEADERS frame's first bytes, read on their own.
    client.transport.send(&wire[..4]);
    let gap = Duration::from_millis(20);
    let end = Instant::now() + gap;
    while Instant::now() < end {
        harness.server.poll(&mut harness.net, echo);
    }
    let rest_sent = Nanos::now();
    client.transport.send(&wire[4..]);
    let mut timing = None;
    client.run(
        &mut harness,
        |request: &Request<'_>, reply: &mut PayloadBuf<'_>| {
            timing = Some((request.received_at(), request.receive_duration()));
            echo(request, reply)
        },
        ended(1),
    );
    let (received_at, receive_duration) = timing.unwrap();
    let received_at = received_at.real();
    assert!(
        start <= received_at && received_at < rest_sent,
        "{start:?} {received_at:?} {rest_sent:?}"
    );
    assert!(receive_duration.0 >= gap.as_nanos() as u64, "{receive_duration:?}");
}

#[test]
fn unsendable_status_resets_the_stream() {
    let mut harness = Harness::new(GrpcConfig::default());
    let mut client = harness.connect(65535);
    client.transport.send(&request(1, "/test.Echo/Huge", "application/grpc", Some(b"")));
    // Larger than the header list limit, so HTTP/2 refuses the trailers.
    let huge = |_: &Request<'_>, _: &mut PayloadBuf<'_>| -> Response {
        Status::new(Code::Internal, "x".repeat(20_000)).into()
    };
    client.run(&mut harness, huge, ended(1));
    assert_eq!(client.stream(1).last(), Some(&&Got::Reset(2)), "the client is not left waiting");
}

#[test]
fn streams_fan_out_finish_and_close() {
    let mut harness = Harness::new(GrpcConfig::default());
    let mut clients = [harness.connect(65535), harness.connect(65535)];
    let mut streams = Vec::new();
    let mut subscribe = |request: &Request<'_>, _: &mut PayloadBuf<'_>| {
        streams.push(request.stream());
        Response::Stream
    };
    for client in &mut clients {
        client.transport.send(&request(1, "/test.Feed/Subscribe", "application/grpc", Some(b"")));
        client.transport.send(&request(3, "/test.Feed/Subscribe", "application/grpc", Some(b"")));
    }
    for client in &mut clients {
        client.run(&mut harness, &mut subscribe, |got| got.len() >= 2);
    }
    assert_eq!(streams.len(), 4);

    for n in 0..3u8 {
        for &stream in &streams {
            harness.server.send(stream, &[n; 100]).unwrap();
        }
    }
    // Larger than one DATA frame: the fast path writes part, the queue the rest.
    let large = vec![7; 40_000];
    harness.server.send(streams[0], &large).unwrap();
    harness.server.finish(streams[0], &Status::ok()).unwrap();
    assert_eq!(harness.server.send(streams[0], b"late"), Err(super::Closed));
    let never = |_: &Request<'_>, _: &mut PayloadBuf<'_>| -> Response { unreachable!() };
    clients[0].run(&mut harness, never, ended(1));
    let first = clients[0].stream(1);
    let mut body = Vec::new();
    for got in &first[1..first.len() - 1] {
        let Got::Data(data) = got else { panic!("unexpected {got:?}") };
        body.extend_from_slice(data);
    }
    let mut expected: Vec<u8> = (0..3u8).flat_map(|n| framed(&[n; 100])).collect();
    expected.extend_from_slice(&framed(&large));
    assert_eq!(body, expected);
    assert!(first.len() > 5, "the large message spans several frames");
    assert_eq!(first.last().unwrap().field("grpc-status"), Some("0"));

    // A peer reset and a disconnect both close their streams.
    let [mut first, second] = clients;
    let mut reset = Vec::new();
    frame(&mut reset, kind::RST_STREAM, 0, 3, &8u32.to_be_bytes());
    first.transport.send(&reset);
    drop(second);
    let end = Instant::now() + TIMEOUT;
    while streams[1..].iter().any(|&stream| harness.server.send(stream, b"x").is_ok()) {
        assert!(Instant::now() < end, "reset and disconnect did not close streams");
        harness.server.poll(&mut harness.net, never);
    }
}

#[test]
fn lagging_stream_finishes_with_resource_exhausted() {
    let mut harness = Harness::new(GrpcConfig { max_queued_bytes: 30, ..GrpcConfig::default() });
    // A zero stream window lets headers through but no messages.
    let mut client = harness.connect(0);
    let mut stream = None;
    client.transport.send(&request(1, "/test.Feed/Subscribe", "application/grpc", Some(b"")));
    client.run(
        &mut harness,
        |request, _| {
            stream = Some(request.stream());
            Response::Stream
        },
        |got| !got.is_empty(),
    );
    let stream = stream.unwrap();
    // 11 framed bytes each: two fit in 30, the third lags.
    harness.server.send(stream, b"queued").unwrap();
    harness.server.send(stream, b"queued").unwrap();
    assert_eq!(harness.server.send(stream, b"queued"), Err(super::Closed));
    client.run(&mut harness, |_, _| unreachable!(), ended(1));
    let got = client.stream(1);
    assert_eq!(got.len(), 2, "queued messages are dropped: {got:?}");
    assert_eq!(got[1].field("grpc-status"), Some("8"));
}

#[test]
fn lag_keeps_a_partly_written_message_whole() {
    // Unsent bytes count: 3 + 11 fit in 20, 3 + 11 + 11 do not.
    let mut harness = Harness::new(GrpcConfig { max_queued_bytes: 20, ..GrpcConfig::default() });
    // An 8-byte window splits the first 11-byte message.
    let mut client = harness.connect(8);
    let mut stream = None;
    client.transport.send(&request(1, "/test.Feed/Subscribe", "application/grpc", Some(b"")));
    client.run(
        &mut harness,
        |request, _| {
            stream = Some(request.stream());
            Response::Stream
        },
        |got| !got.is_empty(),
    );
    let stream = stream.unwrap();
    harness.server.send(stream, b"first!").unwrap();
    harness.server.send(stream, b"second").unwrap();
    client.run(&mut harness, |_, _| unreachable!(), |got| got.len() >= 2);
    assert_eq!(harness.server.send(stream, b"third!"), Err(super::Closed));
    let mut update = Vec::new();
    frame(&mut update, kind::WINDOW_UPDATE, 0, 1, &1000u32.to_be_bytes());
    client.transport.send(&update);
    client.run(&mut harness, |_, _| unreachable!(), ended(1));
    let got = client.stream(1);
    let body: Vec<u8> = got[1..got.len() - 1]
        .iter()
        .flat_map(|got| match got {
            Got::Data(data) => data.clone(),
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(body, framed(b"first!"), "only the split message is completed");
    assert_eq!(got.last().unwrap().field("grpc-status"), Some("8"));
}

#[test]
fn closed_connections_return_calls_to_the_shared_pool() {
    let mut harness = Harness::new(GrpcConfig { pooled_calls: 4, ..GrpcConfig::default() });
    let mut client = harness.connect(65535);
    client.transport.send(&request(1, "/test.Feed/Subscribe", "application/grpc", Some(b"")));
    client.transport.send(&request(3, "/test.Feed/Subscribe", "application/grpc", Some(b"")));
    client.run(&mut harness, |_, _| Response::Stream, |got| got.len() >= 2);
    assert_eq!(harness.server.pooled(), 2, "open streams hold pooled calls");
    drop(client);
    let end = Instant::now() + TIMEOUT;
    while harness.server.pooled() != 4 {
        assert!(Instant::now() < end, "disconnect never returned the calls");
        harness
            .server
            .poll(&mut harness.net, |_: &Request<'_>, _: &mut PayloadBuf<'_>| -> Response {
                unreachable!()
            });
    }
}

#[test]
fn output_behind_a_full_socket_keeps_flowing_without_events() {
    let config = GrpcConfig { max_queued_bytes: 64 << 20, ..GrpcConfig::default() };
    let mut harness = Harness::new(config);
    let mut client = harness.connect(1 << 30);
    let mut stream = None;
    client.transport.send(&request(1, "/test.Feed/Subscribe", "application/grpc", Some(b"")));
    client.run(
        &mut harness,
        |request, _| {
            stream = Some(request.stream());
            Response::Stream
        },
        |got| !got.is_empty(),
    );
    // 16 MiB while the client does not read: the kernel buffers and the TCP
    // backlog fill, so output waits with no network event to wake it.
    let message = vec![9; 256 * 1024];
    for _ in 0..64 {
        harness.server.send(stream.unwrap(), &message).unwrap();
    }
    let never = |_: &Request<'_>, _: &mut PayloadBuf<'_>| -> Response { unreachable!() };
    for _ in 0..100 {
        harness.server.poll(&mut harness.net, never);
    }
    // Reading alone must drain it; the client sends nothing back.
    let expected = 64 * (message.len() + 5);
    client.run(&mut harness, never, |got| {
        got.iter()
            .map(|(_, got)| if let Got::Data(data) = got { data.len() } else { 0 })
            .sum::<usize>() >=
            expected
    });
}

#[test]
fn streams_waiting_on_the_peer_window_are_not_polled() {
    let mut harness = Harness::new(GrpcConfig::default());
    let mut client = harness.connect(0);
    let mut stream = None;
    client.transport.send(&request(1, "/test.Feed/Subscribe", "application/grpc", Some(b"")));
    client.run(
        &mut harness,
        |request, _| {
            stream = Some(request.stream());
            Response::Stream
        },
        |got| !got.is_empty(),
    );
    harness.server.send(stream.unwrap(), b"waiting").unwrap();
    let never = |_: &Request<'_>, _: &mut PayloadBuf<'_>| -> Response { unreachable!() };
    for _ in 0..10 {
        harness.server.poll(&mut harness.net, never);
    }
    assert_eq!(harness.server.dirty(), 0, "a window-blocked stream is not retried each iteration");
    // The peer's window update is an event, which resumes the stream.
    let mut update = Vec::new();
    frame(&mut update, kind::WINDOW_UPDATE, 0, 1, &1000u32.to_be_bytes());
    client.transport.send(&update);
    client.run(&mut harness, never, |got| got.iter().any(|(_, got)| matches!(got, Got::Data(_))));
}

#[test]
fn idle_connections_are_not_driven() {
    let mut harness = Harness::new(GrpcConfig::default());
    let mut clients = [harness.connect(65535), harness.connect(65535), harness.connect(65535)];
    for client in &mut clients {
        client.transport.send(&request(1, "/test.Feed/Subscribe", "application/grpc", Some(b"")));
        client.run(&mut harness, |_, _| Response::Stream, |got| !got.is_empty());
    }
    let never = |_: &Request<'_>, _: &mut PayloadBuf<'_>| -> Response { unreachable!() };
    for _ in 0..10 {
        harness.server.poll(&mut harness.net, never);
    }
    assert_eq!(harness.server.dirty(), 0, "three idle connections stay off the dirty list");
}

#[test]
fn shutdown_finishes_streams_and_closes_connections() {
    let mut harness = Harness::new(GrpcConfig::default());
    let mut client = harness.connect(65535);
    client.transport.send(&request(1, "/test.Feed/Subscribe", "application/grpc", Some(b"")));
    client.run(&mut harness, |_, _| Response::Stream, |got| !got.is_empty());
    harness.server.shutdown();
    client.run(
        &mut harness,
        |_, _| unreachable!(),
        |got| got.last().is_some_and(|(_, got)| *got == Got::Closed),
    );
    assert!(client.got.iter().any(|(_, got)| *got == Got::GoAway));
    assert_eq!(client.stream(1).last().unwrap().field("grpc-status"), Some("14"));
    assert!(harness.server.is_idle());
}

#[cfg(feature = "tls")]
mod tls {
    use std::sync::Arc;

    use rustls::{ClientConnection, pki_types::PrivatePkcs8KeyDer};

    use super::*;
    use crate::tls::{ClientConfig, ServerConfig};

    struct Tls {
        socket: TcpStream,
        conn: ClientConnection,
    }

    impl Tls {
        fn flush(&mut self) {
            while self.conn.wants_write() {
                match self.conn.write_tls(&mut self.socket) {
                    Ok(_) => {}
                    Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                    Err(e) => panic!("{e}"),
                }
            }
        }
    }

    impl Transport for Tls {
        fn send(&mut self, bytes: &[u8]) {
            self.conn.writer().write_all(bytes).unwrap();
            self.flush();
        }

        fn recv(&mut self, out: &mut Vec<u8>) -> bool {
            self.flush();
            loop {
                match self.conn.read_tls(&mut self.socket) {
                    Ok(0) => return false,
                    Ok(_) => {
                        self.conn.process_new_packets().unwrap();
                        let _ = self.conn.reader().read_to_end(out);
                        self.flush();
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => return true,
                    Err(_) => return false,
                }
            }
        }
    }

    #[test]
    fn tls_listener_marks_calls() {
        let key = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = key.cert.der().clone();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut server_config = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.clone()],
                PrivatePkcs8KeyDer::from(key.signing_key.serialize_der()).into(),
            )
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert).unwrap();
        let mut client_config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client_config.alpn_protocols = vec![b"h2".to_vec()];

        let mut harness = Harness::new(GrpcConfig::default());
        let addr = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap();
        assert!(
            harness
                .server
                .listen_tls(&mut harness.net, addr, Arc::new(server_config.clone()))
                .is_err(),
            "TLS without h2 ALPN is refused"
        );
        server_config.alpn_protocols = vec![b"h2".to_vec()];
        harness.server.listen_tls(&mut harness.net, addr, Arc::new(server_config)).unwrap();

        let socket = TcpStream::connect(addr).unwrap();
        socket.set_nonblocking(true).unwrap();
        let conn = ClientConnection::new(Arc::new(client_config), "localhost".try_into().unwrap())
            .unwrap();
        let mut client = Client::new(Tls { socket, conn }, 65535);
        client.transport.send(&request(1, "/test.Echo/Tls", "application/grpc", Some(b"x")));
        client.run(
            &mut harness,
            |request, reply| {
                assert!(request.tls());
                reply.extend_from_slice(request.message());
                Response::Message
            },
            ended(1),
        );
        assert_eq!(client.stream(1).last().unwrap().field("grpc-status"), Some("0"));
    }
}
