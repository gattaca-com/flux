//! Warm unary calls and stream sends to a client that keeps up allocate
//! nothing on the server thread. Its own binary: the allocator is process-wide.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    io::{Read, Write},
    net::{Ipv4Addr, TcpListener, TcpStream},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use flux_network::{
    Network,
    grpc::{Code, GrpcConfig, GrpcServer, Response, Status},
    http2::{self, encode_frame, flags, kind},
};

struct Counting;
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
thread_local!(static COUNTING: Cell<bool> = const { Cell::new(false) });

// SAFETY: forwards to `System`, only counting calls.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn frame(out: &mut Vec<u8>, ty: u8, fl: u8, id: u32, data: &[u8]) {
    encode_frame(ty, fl, id, data, http2::DEFAULT_MAX_FRAME_SIZE, out).unwrap();
}

fn request(id: u32, path: &str, out: &mut Vec<u8>) {
    let mut block = vec![0x83, 0x86];
    for (name, value) in [
        (":path", path),
        (":authority", "localhost"),
        ("content-type", "application/grpc"),
        ("te", "trailers"),
        ("authorization", "Bearer token"),
    ] {
        block.extend_from_slice(&[0, name.len() as u8]);
        block.extend_from_slice(name.as_bytes());
        block.push(value.len() as u8);
        block.extend_from_slice(value.as_bytes());
    }
    frame(out, kind::HEADERS, flags::END_HEADERS, id, &block);
    frame(out, kind::DATA, flags::END_STREAM, id, &[0, 0, 0, 0, 2, 1, 2]);
}

/// Counts server allocations while it answers `rounds` of `batch` unary
/// calls in flight together.
fn unary_allocations(
    server: &mut GrpcServer,
    net: &mut Network,
    client: &mut TcpStream,
    ids: &mut u32,
    rounds: usize,
    batch: usize,
    path: &str,
) -> usize {
    let mut wire = Vec::new();
    let mut sink = vec![0; 1 << 16];
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..rounds {
        wire.clear();
        for _ in 0..batch {
            request(*ids, path, &mut wire);
            *ids += 2;
        }
        client.set_nonblocking(false).unwrap();
        client.write_all(&wire).unwrap();
        client.set_nonblocking(true).unwrap();
        let mut answered = 0;
        let end = Instant::now() + Duration::from_secs(10);
        while answered < batch {
            assert!(Instant::now() < end, "unary call never answered");
            COUNTING.with(|on| on.set(true));
            server.poll(net, |request, reply| {
                assert_eq!(request.header("authorization"), Some(&b"Bearer token"[..]));
                answered += 1;
                match request.path() {
                    b"/test.Echo/Large" => reply.resize(100_000, 7),
                    // Fixed messages: one used in place, one percent-encoded.
                    b"/test.Echo/Missing" => {
                        return Status::new(Code::Unimplemented, "unknown").into()
                    }
                    b"/test.Echo/Denied" => {
                        return Status::new(Code::Unauthenticated, "invalid bearer token").into();
                    }
                    _ => reply.extend_from_slice(request.message()),
                }
                Response::Message
            });
            COUNTING.with(|on| on.set(false));
        }
        // Drain the reply so the server can flush the next one.
        let end = Instant::now() + Duration::from_millis(5);
        while Instant::now() < end {
            COUNTING.with(|on| on.set(true));
            server.poll(net, |_, _| unreachable!());
            COUNTING.with(|on| on.set(false));
            while client.read(&mut sink).is_ok_and(|n| n > 0) {}
        }
    }
    ALLOCATIONS.load(Ordering::Relaxed) - before
}

#[test]
fn warm_unary_calls_do_not_allocate() {
    let addr = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap();
    let mut net = Network::default();
    let mut server =
        GrpcServer::new(&mut net, GrpcConfig { pooled_calls: 16, ..GrpcConfig::default() })
            .unwrap();
    server.listen(&mut net, addr).unwrap();
    let mut client = TcpStream::connect(addr).unwrap();
    // Large windows: a client that keeps granting credit, so replies never
    // wait on flow control.
    let mut wire = http2::CLIENT_PREFACE.to_vec();
    frame(&mut wire, kind::SETTINGS, 0, 0, &[0, 4, 0x40, 0, 0, 0]);
    frame(&mut wire, kind::WINDOW_UPDATE, 0, 0, &(1u32 << 30).to_be_bytes());
    client.write_all(&wire).unwrap();
    client.set_nonblocking(true).unwrap();
    let mut ids = 1;
    let unary = "/test.Echo/Unary";
    // Up to `pooled_calls` (16) in flight at once.
    for batch in [1, 16] {
        unary_allocations(&mut server, &mut net, &mut client, &mut ids, 20, batch, unary);
        let warm =
            unary_allocations(&mut server, &mut net, &mut client, &mut ids, 50, batch, unary);
        assert_eq!(warm, 0, "allocations across 50 warm rounds of {batch} unary calls");
    }
    // A large reply grows a pooled buffer, which shrinks back on reuse.
    let large = "/test.Echo/Large";
    assert!(unary_allocations(&mut server, &mut net, &mut client, &mut ids, 1, 1, large) > 0);
    let warm = unary_allocations(&mut server, &mut net, &mut client, &mut ids, 50, 16, unary);
    assert_eq!(warm, 0, "allocations after a large reply");
    // Error statuses reuse the call's trailer storage.
    for path in ["/test.Echo/Missing", "/test.Echo/Denied"] {
        unary_allocations(&mut server, &mut net, &mut client, &mut ids, 20, 16, path);
        let warm = unary_allocations(&mut server, &mut net, &mut client, &mut ids, 50, 16, path);
        assert_eq!(warm, 0, "allocations across error statuses on {path}");
    }
    // Calls beyond the pool are allocated and freed each time.
    let burst = unary_allocations(&mut server, &mut net, &mut client, &mut ids, 5, 32, unary);
    assert!(burst > 0, "calls beyond `pooled_calls` allocate");
}

#[test]
fn stream_sends_do_not_allocate() {
    let addr = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap();
    let mut net = Network::default();
    let mut server = GrpcServer::new(&mut net, GrpcConfig::default()).unwrap();
    server.listen(&mut net, addr).unwrap();
    let mut client = TcpStream::connect(addr).unwrap();

    // Large windows: a client that keeps granting credit.
    let mut wire = http2::CLIENT_PREFACE.to_vec();
    frame(&mut wire, kind::SETTINGS, 0, 0, &[0, 4, 0x40, 0, 0, 0]);
    frame(&mut wire, kind::WINDOW_UPDATE, 0, 0, &(1u32 << 30).to_be_bytes());
    let mut block = vec![0x83, 0x86];
    for (name, value) in [
        (":path", "/test.Feed/Subscribe"),
        (":authority", "localhost"),
        ("content-type", "application/grpc"),
        ("te", "trailers"),
    ] {
        block.extend_from_slice(&[0, name.len() as u8]);
        block.extend_from_slice(name.as_bytes());
        block.push(value.len() as u8);
        block.extend_from_slice(value.as_bytes());
    }
    frame(&mut wire, kind::HEADERS, flags::END_HEADERS, 1, &block);
    frame(&mut wire, kind::DATA, flags::END_STREAM, 1, &[0; 5]);
    client.write_all(&wire).unwrap();
    client.set_nonblocking(true).unwrap();

    let mut stream = None;
    let mut sink = vec![0; 1 << 20];
    let end = Instant::now() + Duration::from_secs(10);
    while stream.is_none() {
        assert!(Instant::now() < end, "subscription never arrived");
        server.poll(&mut net, |request, _| {
            stream = Some(request.stream());
            Response::Stream
        });
    }
    let stream = stream.unwrap();
    for round in 0..2 {
        let before = ALLOCATIONS.load(Ordering::Relaxed);
        for _ in 0..1000 {
            COUNTING.with(|on| on.set(true));
            server.send(stream, &[7; 100]).unwrap();
            server.poll(&mut net, |_, _| unreachable!());
            COUNTING.with(|on| on.set(false));
            while client.read(&mut sink).is_ok_and(|n| n > 0) {}
        }
        // The first round may grow buffers once.
        if round == 1 {
            assert_eq!(ALLOCATIONS.load(Ordering::Relaxed) - before, 0);
        }
    }
}
