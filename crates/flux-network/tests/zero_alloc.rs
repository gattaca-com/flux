//! The HTTP hot path allocates nothing once its connections are warm.
//!
//! Every allocation this thread makes is counted, and both directions are
//! measured over a keep-alive connection of their own: a peer's request
//! answered through [`flux_network::http::Responder::respond_with`], and the
//! service's own request answered by a peer. Everything the measured window
//! touches was sized by the warm-up — the connection buffers, the header
//! ranges, the body scratch, the send buffer, and the fixed array the peers
//! read into.
//!
//! One thread runs all of it. The peers are plain nonblocking sockets driven
//! between iterations of the network, so nothing else runs while the count is
//! open, and nothing the peers themselves do grows a buffer. The count is
//! per-thread besides, which keeps the test harness's own thread out of it.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    io::{self, Read, Write},
    net::{Ipv4Addr, TcpListener, TcpStream},
    time::{Duration, Instant},
};

use flux_network::{
    Token,
    http::{HttpConfig, HttpEvent, HttpService},
    stream::{ConnectionGroupConfig, Endpoint, Framing, StreamNetwork},
};

thread_local! {
    // `const` init keeps the counter off the lazy TLS path, so bumping it
    // from inside the allocator cannot reenter the allocator.
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

/// Counts allocation events on the calling thread, and allocates as the
/// system does.
///
/// flux-profiler's `CountingAllocator` tallies bytes rather than events and
/// keeps its counters to the profiler, so the tally this test asserts on is
/// its own.
struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn count() {
    ALLOCATIONS.with(|events| events.set(events.get() + 1));
}

/// Allocation events this thread has made.
fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

/// Round trips run before anything is counted: enough for every buffer on
/// both paths to reach the size it keeps.
const WARM_UP: u64 = 16;

/// Round trips run under the count, in each direction.
const ROUND_TRIPS: u64 = 1000;

const TIMEOUT: Duration = Duration::from_secs(60);

const REQUEST_BODY: &[u8] = br#"{"jsonrpc":"2.0","method":"engine_newPayloadV3","id":1}"#;
const RESPONSE_BODY: &[u8] = br#"{"jsonrpc":"2.0","result":{"status":"VALID"},"id":1}"#;
const JSON: &[(&str, &str)] = &[("content-type", "application/json")];

/// One network with one service that both serves and asks, and the two plain
/// sockets on the other end of each.
struct Harness {
    net: StreamNetwork,
    service: HttpService,
    /// The client the service serves, and what the two of them exchange.
    client: TcpStream,
    client_request: Vec<u8>,
    client_response: Vec<u8>,
    /// The endpoint the service asks, and what the two of them exchange.
    upstream: TcpStream,
    upstream_token: Token,
    upstream_request: Vec<u8>,
    upstream_response: Vec<u8>,
    /// Where the peers read, so that reading grows nothing.
    buf: [u8; 8192],
    served: u64,
    answered: u64,
}

impl Harness {
    /// A service listening for `client` and connected to `upstream`, with both
    /// connections established.
    fn build(deadline: Instant) -> Self {
        let probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let serve_addr = probe.local_addr().unwrap();
        drop(probe);
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();

        let mut net = StreamNetwork::default();
        let group = net.add_group(ConnectionGroupConfig {
            name: "zero-alloc",
            framing: Framing::Raw,
            max_frame_size: usize::MAX,
            backlog_warn_bytes: None,
            ..ConnectionGroupConfig::default()
        });
        let mut service = HttpService::new(&mut net, group, HttpConfig::default());
        service.listen(&mut net, Endpoint::Tcp(serve_addr)).unwrap();
        let upstream_token = service.connect(&mut net, Endpoint::Tcp(upstream_addr));

        let client = TcpStream::connect(serve_addr).unwrap();
        client.set_nonblocking(true).unwrap();

        let mut client_request = Vec::new();
        write!(client_request, "POST /rpc HTTP/1.1\r\nHost: peer\r\n").unwrap();
        write!(client_request, "content-type: application/json\r\n").unwrap();
        write!(client_request, "Content-Length: {}\r\n\r\n", REQUEST_BODY.len()).unwrap();
        client_request.extend_from_slice(REQUEST_BODY);

        let mut client_response = Vec::new();
        write!(client_response, "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n").unwrap();
        write!(client_response, "Content-Length: {}\r\n", RESPONSE_BODY.len()).unwrap();
        write!(client_response, "Connection: keep-alive\r\n\r\n").unwrap();
        client_response.extend_from_slice(RESPONSE_BODY);

        // The service supplies the Host itself: an endpoint with no header of
        // its own names the address it dials.
        let mut upstream_request = Vec::new();
        write!(upstream_request, "POST /rpc HTTP/1.1\r\ncontent-type: application/json\r\n")
            .unwrap();
        write!(upstream_request, "Host: {upstream_addr}\r\n").unwrap();
        write!(upstream_request, "Content-Length: {}\r\n\r\n", REQUEST_BODY.len()).unwrap();
        upstream_request.extend_from_slice(REQUEST_BODY);

        let mut upstream_response = Vec::new();
        write!(upstream_response, "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n").unwrap();
        write!(upstream_response, "Content-Length: {}\r\n\r\n", RESPONSE_BODY.len()).unwrap();
        upstream_response.extend_from_slice(RESPONSE_BODY);

        let mut harness = Self {
            net,
            service,
            client,
            client_request,
            client_response,
            upstream: TcpStream::connect(upstream_addr).unwrap(),
            upstream_token,
            upstream_request,
            upstream_response,
            buf: [0; 8192],
            served: 0,
            answered: 0,
        };
        // That placeholder connection is dropped for the one the service
        // itself dialled, which is the connection every request rides.
        harness.upstream = loop {
            assert!(Instant::now() < deadline, "the service never reached its endpoint");
            harness.pump();
            if let Ok((stream, _)) = listener.accept() {
                stream.set_nonblocking(true).unwrap();
                break stream;
            }
        };
        harness
    }

    /// One iteration of the network, and every event it left to pull.
    fn pump(&mut self) {
        let Self { net, service, served, answered, .. } = self;
        net.drive(Some(Duration::ZERO.into()), &mut [service.as_service()], |_| {});
        while let Some(event) = service.next_event(net) {
            match event {
                HttpEvent::Request { responder, .. } => {
                    assert!(responder.respond_with(200, JSON, |out| {
                        out.extend_from_slice(RESPONSE_BODY);
                    }));
                    *served += 1;
                }
                HttpEvent::Response { response, .. } => {
                    assert_eq!(response.status, 200);
                    assert_eq!(response.body, RESPONSE_BODY);
                    *answered += 1;
                }
                _ => {}
            }
        }
    }

    /// One inbound round trip: the client asks, the service answers, and the
    /// client reads the whole answer back.
    fn serve_one(&mut self, deadline: Instant) {
        let sent = self.client.write(&self.client_request).unwrap();
        assert_eq!(sent, self.client_request.len(), "the request outgrew the socket buffer");
        let mut read = 0;
        while read < self.client_response.len() {
            assert!(Instant::now() < deadline, "the inbound round trip stalled");
            self.pump();
            read += drain(&mut self.client, &mut self.buf, &self.client_response[read..]);
        }
    }

    /// One outbound round trip: the service asks, the upstream answers, and
    /// the service reports the response.
    fn ask_one(&mut self, deadline: Instant) {
        let Self { net, service, upstream_token, .. } = self;
        assert!(service.request(net, *upstream_token, "POST", "/rpc", JSON, REQUEST_BODY));
        let mut read = 0;
        while read < self.upstream_request.len() {
            assert!(Instant::now() < deadline, "the outbound request stalled");
            self.pump();
            read += drain(&mut self.upstream, &mut self.buf, &self.upstream_request[read..]);
        }
        let sent = self.upstream.write(&self.upstream_response).unwrap();
        assert_eq!(sent, self.upstream_response.len(), "the answer outgrew the socket buffer");
        let answered = self.answered + 1;
        while self.answered < answered {
            assert!(Instant::now() < deadline, "the outbound answer stalled");
            self.pump();
        }
    }
}

/// Reads what has arrived against what must arrive next, and reports how
/// much of it that was.
fn drain(stream: &mut TcpStream, buf: &mut [u8; 8192], expected: &[u8]) -> usize {
    match stream.read(buf) {
        Ok(0) => panic!("the peer closed the connection"),
        Ok(read) => {
            assert!(read <= expected.len(), "more arrived than the message holds");
            assert_eq!(&buf[..read], &expected[..read], "the bytes on the wire changed");
            read
        }
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => 0,
        Err(err) => panic!("the peer could not read: {err}"),
    }
}

#[test]
fn a_warm_http_connection_allocates_nothing() {
    let deadline = Instant::now() + TIMEOUT;
    let mut harness = Harness::build(deadline);
    for _ in 0..WARM_UP {
        harness.serve_one(deadline);
        harness.ask_one(deadline);
    }

    let before = allocations();
    for _ in 0..ROUND_TRIPS {
        harness.serve_one(deadline);
    }
    let inbound = allocations() - before;

    let before = allocations();
    for _ in 0..ROUND_TRIPS {
        harness.ask_one(deadline);
    }
    let outbound = allocations() - before;

    assert_eq!(harness.served, WARM_UP + ROUND_TRIPS, "every request reached the service");
    assert_eq!(harness.answered, WARM_UP + ROUND_TRIPS, "every response reached the service");
    assert_eq!(
        inbound, 0,
        "serving {ROUND_TRIPS} requests allocated {inbound} times, and must allocate none"
    );
    assert_eq!(
        outbound, 0,
        "making {ROUND_TRIPS} requests allocated {outbound} times, and must allocate none"
    );
}
