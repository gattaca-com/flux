//! Client-side cost of pooled HTTP requests on a warm loopback connection.
//!
//! The server runs on its own thread and network. The client thread times its
//! own calls and counts its own allocations: `send` queues a request, the
//! next `drive` writes it, and the drive that delivers the response parses it.
//! An idle drive is the per-poll cost with a request in flight.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Instant,
};

use flux_network::{
    Network,
    http::{HttpEvent, HttpNetwork},
};

const REQUESTS: usize = 20_000;
const WARMUP: usize = 1_000;
const PATH: &str = "/eth/v1/builder/header/12345678/0x5e9c609604f72c601d9e87ef0b8f60287ea23b27b18d828fc6d3974bc0082db5/0xab71f5bd739a8af01086d52cdf5cb73824a4ee0fd67e6a35bd4fa43c5d4b220ac29688c38b091d74e38624c540077bb7";

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

fn allocs() -> u64 {
    ALLOCS.with(Cell::get)
}

fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.local_addr().unwrap()
}

fn serve(addr: SocketAddr, body: &'static [u8], stop: &AtomicBool) {
    let mut net = Network::default();
    let mut http = HttpNetwork::default();
    http.listen(&mut net, addr).unwrap();
    let mut pending = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        net.poll_with(|e| {
            http.on_event(&e);
        });
        http.drive(&mut net, |e| {
            if let HttpEvent::Request { token, .. } = e {
                pending.push(token);
            }
        });
        for &token in &pending {
            http.respond(&mut net, token, 200, &[("Content-Type", "application/json")], body);
        }
        pending.clear();
    }
}

fn pct(v: &mut [u64], p: f64) -> u64 {
    v.sort_unstable();
    v[((v.len() - 1) as f64 * p) as usize]
}

fn run(name: &str, body: &'static [u8]) {
    let addr = free_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let server = {
        let stop = stop.clone();
        thread::spawn(move || serve(addr, body, &stop))
    };

    let mut net = Network::default();
    let mut http = HttpNetwork::default();
    let pool = http.pool(&mut net, addr, 1);
    let headers = [("Host", "relay.example"), ("X-Api-Key", "0123456789abcdef0123456789abcdef")];

    let mut send = Vec::with_capacity(REQUESTS);
    let mut dispatch = Vec::with_capacity(REQUESTS);
    let mut respond = Vec::with_capacity(REQUESTS);
    let mut idle = Vec::with_capacity(REQUESTS * 64);
    let (mut send_allocs, mut drive_allocs) = (0u64, 0u64);
    for i in 0..REQUESTS + WARMUP {
        let a0 = allocs();
        let t = Instant::now();
        http.send(pool, "GET", PATH, &headers, Vec::new(), 0).unwrap();
        let send_ns = t.elapsed().as_nanos() as u64;
        let a1 = allocs();

        let mut dispatched = None;
        let mut done = None;
        while done.is_none() {
            let t = Instant::now();
            net.poll_with(|e| {
                http.on_event(&e);
            });
            let mut got = false;
            http.drive(&mut net, |e| {
                if e.outcome(pool).is_some() {
                    got = true;
                }
            });
            let ns = t.elapsed().as_nanos() as u64;
            if got {
                done = Some(ns);
            } else if dispatched.is_none() {
                dispatched = Some(ns);
            } else if i >= WARMUP && idle.len() < idle.capacity() {
                idle.push(ns);
            }
        }
        if i >= WARMUP {
            send.push(send_ns);
            dispatch.push(dispatched.unwrap_or(0));
            respond.push(done.unwrap());
            send_allocs += a1 - a0;
            drive_allocs += allocs() - a1;
        }
    }
    stop.store(true, Ordering::Relaxed);
    server.join().unwrap();

    println!(
        "{name:>10}  send p50 {:>5} p99 {:>6} | dispatch p50 {:>6} p99 {:>6} | response p50 {:>6} p99 {:>6} | idle drive p50 {:>4} | allocs/req send {:.2} drive {:.2}",
        pct(&mut send, 0.5),
        pct(&mut send, 0.99),
        pct(&mut dispatch, 0.5),
        pct(&mut dispatch, 0.99),
        pct(&mut respond, 0.5),
        pct(&mut respond, 0.99),
        pct(&mut idle, 0.5),
        send_allocs as f64 / REQUESTS as f64,
        drive_allocs as f64 / REQUESTS as f64,
    );
}

fn main() {
    static JSON: [u8; 1200] = [b'x'; 1200];
    run("empty", b"");
    run("1.2k body", &JSON);
}
