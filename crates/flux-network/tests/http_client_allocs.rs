//! Warm pooled client requests allocate nothing on the client thread.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    io::{Read, Write},
    net::Ipv4Addr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use flux_network::{Network, http::HttpNetwork};

struct Counting;
thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.with(|a| a.set(a.get() + 1));
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.with(|a| a.set(a.get() + 1));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}
#[global_allocator]
static GLOBAL: Counting = Counting;

fn serve(listener: &std::net::TcpListener, response: &'static [u8], stop: &AtomicBool) {
    let (mut stream, _) = listener.accept().unwrap();
    stream.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
    let mut buf = [0u8; 4096];
    let mut pending = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        let Ok(n) = stream.read(&mut buf) else { continue };
        if n == 0 {
            return
        }
        pending.extend_from_slice(&buf[..n]);
        while let Some(end) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
            pending.drain(..end + 4);
            stream.write_all(response).unwrap();
        }
    }
}

fn client_allocs(response: &'static [u8]) -> u64 {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let server = {
        let stop = stop.clone();
        thread::spawn(move || serve(&listener, response, &stop))
    };
    let mut net = Network::default();
    let mut http = HttpNetwork::default();
    let pool = http.pool(&mut net, addr, 1);
    let mut counted = 0;
    for i in 0..200 {
        let before = ALLOCS.with(Cell::get);
        http.send(pool, "GET", "/path", &[("X-Key", "value")], Vec::new(), 0).unwrap();
        let mut body_ok = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while !body_ok && Instant::now() < deadline {
            net.poll_with(|e| {
                http.on_event(&e);
            });
            http.drive(&mut net, |e| {
                if let Some((_, Ok(response))) = e.outcome(pool) {
                    body_ok = response.body == b"hello";
                }
            });
        }
        assert!(body_ok, "request {i} failed");
        if i >= 100 {
            counted += ALLOCS.with(Cell::get) - before;
        }
    }
    stop.store(true, Ordering::Relaxed);
    server.join().unwrap();
    counted
}

#[test]
fn warm_pooled_requests_do_not_allocate() {
    assert_eq!(client_allocs(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"), 0);
}

#[test]
fn warm_chunked_responses_do_not_allocate() {
    let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
    assert_eq!(client_allocs(response), 0);
}
