//! flux `GrpcServer` against tonic over loopback, driven by the same tonic
//! client. Each server runs on one pinned core; the client runs on others.
//!
//! - unary latency: one call in flight, 4 KiB request, 16-byte reply.
//! - unary throughput: many calls in flight on 1 and 8 connections.
//! - fan-out: a timestamped 1 KiB message published to N subscribers at rising
//!   rates, until deliveries fall behind the offered rate.
//! - allocations made by the server thread, per call or per delivery.
//!
//! tonic runs on a single-thread runtime. Its fan-out is measured twice:
//! "idiomatic" broadcasts an owned `Vec<u8>` that each subscriber clones and
//! encodes; "shared" broadcasts `Bytes`, so tonic only frames it.
//!
//! Run: `cargo bench -p flux-network --bench grpc_tonic -- [unary|fanout]`.
//! Cores: `BENCH_SERVER_CPU` (default 2), `BENCH_CLIENT_CPUS` (default 4-15).

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    convert::Infallible,
    future::{Future, Ready, ready},
    marker::PhantomData,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    pin::Pin,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
    task::{Context, Poll},
    thread,
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut, Bytes};
use flux_network::{
    Network,
    grpc::{GrpcConfig, GrpcServer, Response},
};
use hdrhistogram::Histogram;
use http::uri::PathAndQuery;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, wrappers::BroadcastStream};
use tonic::{
    Request, Status,
    body::Body,
    client::Grpc,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    server::{Grpc as ServerGrpc, NamedService, ServerStreamingService, UnaryService},
    transport::{Channel, Endpoint, Server},
};

const UNARY: &str = "/bench.Bench/Unary";
const SUBSCRIBE: &str = "/bench.Bench/Subscribe";
const REQUEST_BYTES: usize = 4096;
const REPLY: [u8; 16] = [7; 16];
const MESSAGE_BYTES: usize = 1024;

// Allocations made by the server thread only.
struct Counting;
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
thread_local!(static COUNTING: Cell<bool> = const { Cell::new(false) });

// SAFETY: forwards to `System`, only counting calls.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            ALLOCATIONS.fetch_add(1, Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            ALLOCATIONS.fetch_add(1, Relaxed);
        }
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

fn now_ns() -> u64 {
    EPOCH.elapsed().as_nanos() as u64
}

/// Raw bytes in both directions, so neither server pays for protobuf.
struct Raw<T>(PhantomData<fn() -> T>);

impl<T> Default for Raw<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: AsRef<[u8]> + Send + 'static> Codec for Raw<T> {
    type Decode = Bytes;
    type Decoder = Self;
    type Encode = T;
    type Encoder = Self;

    fn encoder(&mut self) -> Self {
        Self::default()
    }

    fn decoder(&mut self) -> Self {
        Self::default()
    }
}

impl<T: AsRef<[u8]>> Encoder for Raw<T> {
    type Error = Status;
    type Item = T;

    fn encode(&mut self, item: T, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put_slice(item.as_ref());
        Ok(())
    }
}

impl<T> Decoder for Raw<T> {
    type Error = Status;
    type Item = Bytes;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(src.copy_to_bytes(src.remaining())))
    }
}

/// A broadcast message for tonic: `Vec<u8>` is cloned per subscriber,
/// `Bytes` is shared.
trait Payload: AsRef<[u8]> + Clone + Send + Sync + 'static {
    fn make(timestamp: u64) -> Self;
}

impl Payload for Vec<u8> {
    fn make(timestamp: u64) -> Self {
        let mut message = vec![9; MESSAGE_BYTES];
        message[..8].copy_from_slice(&timestamp.to_le_bytes());
        message
    }
}

impl Payload for Bytes {
    fn make(timestamp: u64) -> Self {
        Vec::make(timestamp).into()
    }
}

#[derive(Clone)]
struct Bench<P> {
    tx: broadcast::Sender<P>,
}

impl<P> NamedService for Bench<P> {
    const NAME: &'static str = "bench.Bench";
}

struct Echo;

impl UnaryService<Bytes> for Echo {
    type Future = Ready<Result<tonic::Response<Bytes>, Status>>;
    type Response = Bytes;

    fn call(&mut self, _: Request<Bytes>) -> Self::Future {
        ready(Ok(tonic::Response::new(Bytes::from_static(&REPLY))))
    }
}

struct Subscribe<P>(broadcast::Sender<P>);

impl<P: Payload> ServerStreamingService<Bytes> for Subscribe<P> {
    type Future = Ready<Result<tonic::Response<Self::ResponseStream>, Status>>;
    type Response = P;
    type ResponseStream = Pin<Box<dyn Stream<Item = Result<P, Status>> + Send>>;

    fn call(&mut self, _: Request<Bytes>) -> Self::Future {
        let stream = BroadcastStream::new(self.0.subscribe())
            .map(|message| message.map_err(|_| Status::resource_exhausted("lagged")));
        ready(Ok(tonic::Response::new(Box::pin(stream))))
    }
}

impl<P: Payload> tonic::codegen::Service<http::Request<Body>> for Bench<P> {
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<http::Response<Body>, Infallible>> + Send>>;
    type Response = http::Response<Body>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let tx = self.tx.clone();
        Box::pin(async move {
            Ok(if request.uri().path() == UNARY {
                ServerGrpc::new(Raw::<Bytes>::default()).unary(Echo, request).await
            } else {
                ServerGrpc::new(Raw::<P>::default()).server_streaming(Subscribe(tx), request).await
            })
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Flux,
    TonicShared,
    TonicIdiomatic,
}

impl Kind {
    const fn name(self) -> &'static str {
        match self {
            Self::Flux => "flux",
            Self::TonicShared => "tonic (shared Bytes)",
            Self::TonicIdiomatic => "tonic (idiomatic)",
        }
    }
}

struct Running {
    addr: SocketAddr,
    /// Messages published per millisecond; 0 stops publishing.
    per_ms: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.stop.store(true, Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}

fn start(kind: Kind, cpu: core_affinity::CoreId) -> Running {
    let addr = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap();
    let per_ms = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let thread = thread::spawn({
        let (per_ms, stop) = (per_ms.clone(), stop.clone());
        move || {
            core_affinity::set_for_current(cpu);
            COUNTING.set(true);
            match kind {
                Kind::Flux => serve_flux(addr, &per_ms, &stop, &ready_tx),
                Kind::TonicShared => serve_tonic::<Bytes>(addr, per_ms, stop, ready_tx),
                Kind::TonicIdiomatic => serve_tonic::<Vec<u8>>(addr, per_ms, stop, ready_tx),
            }
        }
    });
    ready_rx.recv().unwrap();
    thread::sleep(Duration::from_millis(100));
    Running { addr, per_ms, stop, thread: Some(thread) }
}

fn serve_flux(
    addr: SocketAddr,
    per_ms: &AtomicU64,
    stop: &AtomicBool,
    ready: &std::sync::mpsc::Sender<()>,
) {
    let mut net = Network::default();
    let mut server = GrpcServer::new(&mut net, GrpcConfig::default()).unwrap();
    server.listen(&mut net, addr).unwrap();
    ready.send(()).unwrap();
    let mut subscribers = Vec::new();
    let mut message = vec![9; MESSAGE_BYTES];
    let mut next_tick = Instant::now();
    while !stop.load(Relaxed) {
        server.poll(&mut net, |request, reply| {
            if request.path() == UNARY.as_bytes() {
                reply.extend_from_slice(&REPLY);
                Response::Message
            } else {
                subscribers.push(request.stream());
                Response::Stream
            }
        });
        let rate = per_ms.load(Relaxed);
        let now = Instant::now();
        if rate == 0 {
            next_tick = now;
            continue;
        }
        while next_tick <= now {
            next_tick += Duration::from_millis(1);
            for _ in 0..rate {
                message[..8].copy_from_slice(&now_ns().to_le_bytes());
                let mut i = 0;
                while i < subscribers.len() {
                    if server.send(subscribers[i], &message).is_ok() {
                        i += 1;
                    } else {
                        subscribers.swap_remove(i);
                    }
                }
            }
        }
    }
}

fn serve_tonic<P: Payload>(
    addr: SocketAddr,
    per_ms: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    ready: std::sync::mpsc::Sender<()>,
) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        let (tx, _) = broadcast::channel::<P>(100_000);
        let publisher = tx.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(1));
            loop {
                tick.tick().await;
                for _ in 0..per_ms.load(Relaxed) {
                    let _ = publisher.send(P::make(now_ns()));
                }
            }
        });
        ready.send(()).unwrap();
        Server::builder()
            .add_service(Bench { tx })
            .serve_with_shutdown(addr, async move {
                while !stop.load(Relaxed) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
    });
}

fn client_runtime(cpus: &[core_affinity::CoreId]) -> tokio::runtime::Runtime {
    let cpus = cpus.to_vec();
    let next = AtomicU64::new(0);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cpus.len())
        .on_thread_start(move || {
            let i = next.fetch_add(1, Relaxed) as usize;
            core_affinity::set_for_current(cpus[i % cpus.len()]);
        })
        .enable_all()
        .build()
        .unwrap()
}

async fn connect(addr: SocketAddr) -> Grpc<Channel> {
    let channel = Endpoint::from_shared(format!("http://{addr}")).unwrap().connect().await.unwrap();
    Grpc::new(channel)
}

async fn call(client: &mut Grpc<Channel>, body: Bytes) {
    client.ready().await.unwrap();
    let path = PathAndQuery::from_static(UNARY);
    client.unary(Request::new(body), path, Raw::<Bytes>::default()).await.unwrap();
}

fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap()
}

fn us(nanos: u64) -> String {
    format!("{:.1}", nanos as f64 / 1000.0)
}

async fn unary_latency(addr: SocketAddr) -> Histogram<u64> {
    let mut client = connect(addr).await;
    let body = Bytes::from(vec![1; REQUEST_BYTES]);
    for _ in 0..2_000 {
        call(&mut client, body.clone()).await;
    }
    let mut latency = histogram();
    for _ in 0..20_000 {
        let start = Instant::now();
        call(&mut client, body.clone()).await;
        latency.record(start.elapsed().as_nanos() as u64).unwrap();
    }
    latency
}

/// Calls per second and server allocations per call, with `in_flight` calls
/// per connection.
async fn unary_throughput(addr: SocketAddr, connections: usize, in_flight: usize) -> (f64, f64) {
    let warmup = Duration::from_secs(1);
    let measure = Duration::from_secs(3);
    let start = Instant::now() + warmup;
    let end = start + measure;
    let calls = Arc::new(AtomicU64::new(0));
    let body = Bytes::from(vec![1; REQUEST_BYTES]);
    let mut tasks = Vec::new();
    for _ in 0..connections {
        let client = connect(addr).await;
        for _ in 0..in_flight {
            let (mut client, calls, body) = (client.clone(), calls.clone(), body.clone());
            tasks.push(tokio::spawn(async move {
                while Instant::now() < end {
                    call(&mut client, body.clone()).await;
                    let now = Instant::now();
                    if now >= start && now < end {
                        calls.fetch_add(1, Relaxed);
                    }
                }
            }));
        }
    }
    tokio::time::sleep_until(start.into()).await;
    let before = ALLOCATIONS.load(Relaxed);
    tokio::time::sleep_until(end.into()).await;
    let allocations = ALLOCATIONS.load(Relaxed) - before;
    for task in tasks {
        task.await.unwrap();
    }
    let calls = calls.load(Relaxed) as f64;
    (calls / measure.as_secs_f64(), allocations as f64 / calls)
}

struct FanoutPoint {
    offered: f64,
    delivered: f64,
    p50: u64,
    p99: u64,
    allocations: f64,
    /// "ok"; "behind" when under 95% is delivered or p99 exceeds 10 ms;
    /// "lagged" when the server ended subscribers that fell behind.
    result: &'static str,
}

/// Subscribes `subscribers` streams over `connections`, then measures
/// deliveries at rising publish rates until they fall behind.
async fn fanout(server: &Running, subscribers: usize, connections: usize) -> Vec<FanoutPoint> {
    let measuring = Arc::new(AtomicBool::new(false));
    let delivered = Arc::new(AtomicU64::new(0));
    let ended = Arc::new(AtomicU64::new(0));
    let latency = Arc::new(Mutex::new(histogram()));
    let mut clients = Vec::new();
    for _ in 0..connections {
        clients.push(connect(server.addr).await);
    }
    let mut tasks = Vec::new();
    for i in 0..subscribers {
        let mut client = clients[i % connections].clone();
        let (measuring, delivered, ended) = (measuring.clone(), delivered.clone(), ended.clone());
        let latency = latency.clone();
        tasks.push(tokio::spawn(async move {
            client.ready().await.unwrap();
            let path = PathAndQuery::from_static(SUBSCRIBE);
            let mut stream = client
                .server_streaming(Request::new(Bytes::new()), path, Raw::<Bytes>::default())
                .await
                .unwrap()
                .into_inner();
            let mut local = histogram();
            while let Ok(Some(message)) = stream.message().await {
                if measuring.load(Relaxed) {
                    let sent = u64::from_le_bytes(message[..8].try_into().unwrap());
                    local.record(now_ns().saturating_sub(sent).max(1)).unwrap();
                    delivered.fetch_add(1, Relaxed);
                } else if !local.is_empty() {
                    latency.lock().unwrap().add(&local).unwrap();
                    local.reset();
                }
            }
            ended.fetch_add(1, Relaxed);
        }));
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut points = Vec::new();
    for rate in (0..12).map(|step| 1u64 << step) {
        server.per_ms.store(rate, Relaxed);
        tokio::time::sleep(Duration::from_millis(500)).await;
        delivered.store(0, Relaxed);
        latency.lock().unwrap().reset();
        let before = ALLOCATIONS.load(Relaxed);
        measuring.store(true, Relaxed);
        let window = Duration::from_secs(2);
        tokio::time::sleep(window).await;
        measuring.store(false, Relaxed);
        let allocations = ALLOCATIONS.load(Relaxed) - before;
        // Let every stream fold its samples in.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let count = delivered.load(Relaxed) as f64;
        let offered = (rate * 1000) as f64 * subscribers as f64;
        let delivered_per_s = count / window.as_secs_f64();
        let (p50, p99) = {
            let latency = latency.lock().unwrap();
            (latency.value_at_quantile(0.5), latency.value_at_quantile(0.99))
        };
        let result = if ended.load(Relaxed) != 0 {
            "lagged"
        } else if delivered_per_s < 0.95 * offered || p99 > 10_000_000 {
            "behind"
        } else {
            "ok"
        };
        points.push(FanoutPoint {
            offered,
            delivered: delivered_per_s,
            p50,
            p99,
            allocations: if count == 0.0 { f64::NAN } else { allocations as f64 / count },
            result,
        });
        if result != "ok" {
            break;
        }
    }
    server.per_ms.store(0, Relaxed);
    for task in tasks {
        task.abort();
    }
    points
}

fn parse_cpus(spec: &str) -> Vec<core_affinity::CoreId> {
    spec.split(',')
        .flat_map(|part| match part.split_once('-') {
            Some((a, b)) => (a.parse().unwrap()..=b.parse().unwrap()).collect::<Vec<usize>>(),
            None => vec![part.parse().unwrap()],
        })
        .map(|id| core_affinity::CoreId { id })
        .collect()
}

fn main() {
    let filter = std::env::args().skip(1).find(|arg| !arg.starts_with('-'));
    let run = |name: &str| filter.as_deref().is_none_or(|filter| filter == name);
    let server_cpu =
        parse_cpus(&std::env::var("BENCH_SERVER_CPU").unwrap_or_else(|_| "2".into()))[0];
    let client_cpus =
        parse_cpus(&std::env::var("BENCH_CLIENT_CPUS").unwrap_or_else(|_| "4-15".into()));
    println!(
        "server core {}, client cores {:?}",
        server_cpu.id,
        client_cpus.iter().map(|c| c.id).collect::<Vec<_>>()
    );

    if run("unary") {
        println!("\n## Unary: 4 KiB request, 16-byte reply\n");
        println!(
            "| server | p50 µs | p99 µs | p99.9 µs | 1 conn × 32 calls/s | 8 conns × 32 calls/s | allocs/call |"
        );
        println!("| --- | ---: | ---: | ---: | ---: | ---: | ---: |");
        for kind in [Kind::Flux, Kind::TonicShared] {
            let server = start(kind, server_cpu);
            let runtime = client_runtime(&client_cpus);
            let latency = runtime.block_on(unary_latency(server.addr));
            let (one, _) = runtime.block_on(unary_throughput(server.addr, 1, 32));
            let (eight, allocations) = runtime.block_on(unary_throughput(server.addr, 8, 32));
            println!(
                "| {} | {} | {} | {} | {:.0} | {:.0} | {:.1} |",
                if kind == Kind::Flux { "flux" } else { "tonic" },
                us(latency.value_at_quantile(0.5)),
                us(latency.value_at_quantile(0.99)),
                us(latency.value_at_quantile(0.999)),
                one,
                eight,
                allocations,
            );
            drop(runtime);
        }
    }

    if run("fanout") {
        for (subscribers, connections) in [(1, 1), (64, 8), (512, 8)] {
            println!(
                "\n## Fan-out: 1 KiB to {subscribers} subscribers on {connections} connections\n"
            );
            println!("| server | offered/s | delivered/s | p50 µs | p99 µs | allocs/delivery | |");
            println!("| --- | ---: | ---: | ---: | ---: | ---: | --- |");
            for kind in [Kind::Flux, Kind::TonicShared, Kind::TonicIdiomatic] {
                let server = start(kind, server_cpu);
                let runtime = client_runtime(&client_cpus);
                for point in runtime.block_on(fanout(&server, subscribers, connections)) {
                    if point.delivered == 0.0 {
                        let name = kind.name();
                        println!(
                            "| {name} | {:.0} | – | – | – | – | {} |",
                            point.offered, point.result
                        );
                        continue;
                    }
                    println!(
                        "| {} | {:.0} | {:.0} | {} | {} | {:.2} | {} |",
                        kind.name(),
                        point.offered,
                        point.delivered,
                        us(point.p50),
                        us(point.p99),
                        point.allocations,
                        point.result,
                    );
                }
                drop(runtime);
            }
        }
    }
}
