//! tonic client against the flux server, plaintext and TLS, in one process.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut, Bytes};
use flux_network::{
    Network, PayloadBuf,
    grpc::{Code as FluxCode, GrpcConfig, GrpcServer, Response, Status as FluxStatus, Stream},
    tls::{ServerConfig, rustls},
};
use http::uri::PathAndQuery;
use tonic::{
    Code, Request, Status,
    client::Grpc,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};

/// Counts allocations a thread makes while `COUNTING` is set.
struct Counting;
thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static COUNT: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: forwards to `System`, only counting calls.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            COUNT.with(|count| count.set(count.get() + 1));
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            COUNT.with(|count| count.set(count.get() + 1));
        }
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Messages are raw protobuf bytes on both sides.
#[derive(Clone, Copy, Default)]
struct Raw;

impl Codec for Raw {
    type Decode = Bytes;
    type Decoder = Self;
    type Encode = Bytes;
    type Encoder = Self;

    fn encoder(&mut self) -> Self {
        Self
    }

    fn decoder(&mut self) -> Self {
        Self
    }
}

impl Encoder for Raw {
    type Error = Status;
    type Item = Bytes;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put(item);
        Ok(())
    }
}

impl Decoder for Raw {
    type Error = Status;
    type Item = Bytes;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(src.copy_to_bytes(src.remaining())))
    }
}

enum Work {
    /// Send `n` messages of `size` bytes, then finish with `status`.
    Send { n: usize, size: usize, status: FluxStatus },
    /// Send until the client goes away.
    Forever,
}

struct Flux {
    plain: SocketAddr,
    tls: SocketAddr,
    ca: String,
    /// Forever streams the server saw close.
    closed: Arc<AtomicUsize>,
    /// Allocations the server thread made inside `poll`.
    allocations: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for Flux {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}

fn free_addr() -> SocketAddr {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap()
}

fn route(
    request: &flux_network::grpc::Request<'_>,
    reply: &mut PayloadBuf<'_>,
    work: &mut Vec<(Stream, Work)>,
) -> Response {
    let stream = |work: &mut Vec<_>, job| {
        work.push((request.stream(), job));
        Response::Stream
    };
    match request.path() {
        b"/test.Echo/Unary" => {
            if request.header("authorization") != Some(b"Bearer ok") {
                return FluxStatus::new(FluxCode::Unauthenticated, "bad token").into();
            }
            reply.extend_from_slice(request.message());
            Response::Message
        }
        b"/test.Echo/Peer" => {
            reply.extend_from_slice(if request.tls() { b"tls" } else { b"plain" });
            Response::Message
        }
        b"/test.Feed/Count" => {
            let n = usize::from(request.message()[0]);
            stream(work, Work::Send { n, size: 100, status: FluxStatus::ok() })
        }
        b"/test.Feed/Large" => {
            stream(work, Work::Send { n: 3, size: 200_000, status: FluxStatus::ok() })
        }
        b"/test.Feed/Fail" => stream(work, Work::Send {
            n: 1,
            size: 10,
            status: FluxStatus::new(FluxCode::InvalidArgument, "bad subscription"),
        }),
        b"/test.Feed/Forever" => stream(work, Work::Forever),
        _ => FluxStatus::new(FluxCode::Unimplemented, "unknown method").into(),
    }
}

fn start() -> Flux {
    let key = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let ca = key.cert.pem();
    let mut tls =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![key.cert.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(key.signing_key.serialize_der()).into(),
            )
            .unwrap();
    tls.alpn_protocols = vec![b"h2".to_vec()];

    let (plain, tls_addr) = (free_addr(), free_addr());
    let stop = Arc::new(AtomicBool::new(false));
    let closed = Arc::new(AtomicUsize::new(0));
    let allocations = Arc::new(AtomicUsize::new(0));
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let thread = thread::spawn({
        let (stop, closed, allocations) = (stop.clone(), closed.clone(), allocations.clone());
        move || {
            let mut net = Network::default();
            let mut server = GrpcServer::new(&mut net, GrpcConfig::default()).unwrap();
            server.listen(&mut net, plain).unwrap();
            server.listen_tls(&mut net, tls_addr, Arc::new(tls)).unwrap();
            ready_tx.send(()).unwrap();
            let mut work = Vec::new();
            let mut forever = Vec::new();
            let tick = vec![1; 64];
            while !stop.load(Ordering::Relaxed) {
                COUNTING.with(|on| on.set(true));
                server.poll(&mut net, |request, reply| route(request, reply, &mut work));
                COUNTING.with(|on| on.set(false));
                allocations.store(COUNT.with(Cell::get), Ordering::Relaxed);
                while let Some((stream, job)) = work.pop() {
                    match job {
                        Work::Send { n, size, status } => {
                            for i in 0..n {
                                server.send(stream, &vec![i as u8; size]).unwrap();
                            }
                            server.finish(stream, &status).unwrap();
                        }
                        Work::Forever => forever.push(stream),
                    }
                }
                forever.retain(|&stream| {
                    let open = server.send(stream, &tick).is_ok();
                    if !open {
                        closed.fetch_add(1, Ordering::Relaxed);
                    }
                    open
                });
                thread::sleep(Duration::from_micros(50));
            }
        }
    });
    ready_rx.recv().unwrap();
    Flux { plain, tls: tls_addr, ca, closed, allocations, stop, thread: Some(thread) }
}

async fn plain(flux: &Flux) -> Grpc<Channel> {
    let channel =
        Endpoint::from_shared(format!("http://{}", flux.plain)).unwrap().connect().await.unwrap();
    Grpc::new(channel)
}

async fn tls(flux: &Flux) -> Grpc<Channel> {
    let config = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&flux.ca))
        .domain_name("localhost");
    let channel = Endpoint::from_shared(format!("https://{}", flux.tls))
        .unwrap()
        .tls_config(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    Grpc::new(channel)
}

async fn unary(
    client: &mut Grpc<Channel>,
    path: &'static str,
    body: Bytes,
) -> Result<Bytes, Status> {
    client.ready().await.unwrap();
    let mut request = Request::new(body);
    request.metadata_mut().insert("authorization", "Bearer ok".parse().unwrap());
    client
        .unary(request, PathAndQuery::from_static(path), Raw)
        .await
        .map(tonic::Response::into_inner)
}

async fn collect(
    client: &mut Grpc<Channel>,
    path: &'static str,
    body: Bytes,
) -> (Vec<Bytes>, Result<(), Status>) {
    client.ready().await.unwrap();
    let mut stream = match client
        .server_streaming(Request::new(body), PathAndQuery::from_static(path), Raw)
        .await
    {
        Ok(response) => response.into_inner(),
        Err(status) => return (Vec::new(), Err(status)),
    };
    let mut messages = Vec::new();
    loop {
        match stream.message().await {
            Ok(Some(message)) => messages.push(message),
            Ok(None) => return (messages, Ok(())),
            Err(status) => return (messages, Err(status)),
        }
    }
}

#[tokio::test]
async fn unary_calls_and_statuses() {
    let flux = start();
    for mut client in [plain(&flux).await, tls(&flux).await] {
        assert_eq!(
            unary(&mut client, "/test.Echo/Unary", Bytes::from_static(b"hi")).await.unwrap(),
            "hi"
        );
        assert_eq!(unary(&mut client, "/test.Echo/Unary", Bytes::new()).await.unwrap(), "");
        let missing = unary(&mut client, "/test.Echo/Missing", Bytes::new()).await.unwrap_err();
        assert_eq!((missing.code(), missing.message()), (Code::Unimplemented, "unknown method"));

        // Larger than a frame and the initial window: exercises flow control.
        let large = Bytes::from(vec![9; 1 << 20]);
        assert_eq!(unary(&mut client, "/test.Echo/Unary", large.clone()).await.unwrap(), large);

        client.ready().await.unwrap();
        let denied = client
            .unary(Request::new(Bytes::new()), PathAndQuery::from_static("/test.Echo/Unary"), Raw)
            .await
            .unwrap_err();
        assert_eq!(denied.code(), Code::Unauthenticated);
    }
    let mut plain = plain(&flux).await;
    let mut tls = tls(&flux).await;
    assert_eq!(unary(&mut plain, "/test.Echo/Peer", Bytes::new()).await.unwrap(), "plain");
    assert_eq!(unary(&mut tls, "/test.Echo/Peer", Bytes::new()).await.unwrap(), "tls");
}

#[tokio::test]
async fn concurrent_calls_share_one_connection() {
    let flux = start();
    let client = plain(&flux).await;
    let calls = (0..200u32).map(|i| {
        let mut client = client.clone();
        tokio::spawn(async move {
            let body = Bytes::from(i.to_le_bytes().to_vec());
            assert_eq!(unary(&mut client, "/test.Echo/Unary", body.clone()).await.unwrap(), body);
        })
    });
    for call in calls.collect::<Vec<_>>() {
        call.await.unwrap();
    }
}

#[tokio::test]
async fn server_streams() {
    let flux = start();
    for mut client in [plain(&flux).await, tls(&flux).await] {
        let (messages, end) =
            collect(&mut client, "/test.Feed/Count", Bytes::from_static(&[50])).await;
        end.unwrap();
        assert_eq!(messages.len(), 50);
        assert!(messages.iter().enumerate().all(|(i, m)| m[..] == [i as u8; 100]));

        let (messages, end) = collect(&mut client, "/test.Feed/Large", Bytes::new()).await;
        end.unwrap();
        assert_eq!(messages.iter().map(Bytes::len).collect::<Vec<_>>(), [200_000; 3]);

        let (messages, end) = collect(&mut client, "/test.Feed/Fail", Bytes::new()).await;
        assert_eq!(messages.len(), 1, "messages before the error status arrive");
        let status = end.unwrap_err();
        assert_eq!((status.code(), status.message()), (Code::InvalidArgument, "bad subscription"));
    }
}

#[tokio::test]
async fn client_cancel_closes_the_stream() {
    let flux = start();
    let mut client = plain(&flux).await;
    client.ready().await.unwrap();
    let mut stream = client
        .server_streaming(
            Request::new(Bytes::new()),
            PathAndQuery::from_static("/test.Feed/Forever"),
            Raw,
        )
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stream.message().await.unwrap().unwrap().len(), 64);
    drop(stream);
    let end = Instant::now() + Duration::from_secs(10);
    while flux.closed.load(Ordering::Relaxed) == 0 {
        assert!(Instant::now() < end, "server never saw the cancel");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // The connection stays usable after the reset.
    assert_eq!(
        unary(&mut client, "/test.Echo/Unary", Bytes::from_static(b"ok")).await.unwrap(),
        "ok"
    );
}

#[tokio::test]
async fn warm_unary_calls_allocate_nothing_on_the_server() {
    let flux = start();
    let mut client = plain(&flux).await;
    let body = Bytes::from_static(b"hi");
    // Warm-up fills HPACK tables and reusable buffers on both sides.
    for _ in 0..20 {
        unary(&mut client, "/test.Echo/Unary", body.clone()).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    let before = flux.allocations.load(Ordering::Relaxed);
    for _ in 0..100 {
        unary(&mut client, "/test.Echo/Unary", body.clone()).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    let allocations = flux.allocations.load(Ordering::Relaxed) - before;
    eprintln!("server allocations for 100 tonic unary calls: {allocations}");
    assert_eq!(allocations, 0);
}
