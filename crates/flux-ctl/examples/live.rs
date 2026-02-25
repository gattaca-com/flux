//! Live demo: simulates a running application that continuously produces
//! messages on shared-memory queues.
//!
//! Run in one terminal:
//!   cargo run --example live -p flux-ctl
//!
//! Then observe in another terminal:
//!   cargo run -p flux-ctl -- watch
//!
//! Flags:
//!   --no-cleanup       Leave shmem segments intact on exit
//!   --reattach         Reattach to existing segments from a previous run
//!   --workers <N>      Fork N child worker processes that attach to the same
//!                      segments (default: 0, i.e. single-process mode)
//!   --poison           After 3s, crash a child process mid-write to poison
//!                      a seqlock slot (simulates a real production crash)
//!
//! Press Ctrl-C to stop (propagates to children).

use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use flux_communication::queue::{Consumer, Producer, Queue, QueueType};
use flux_communication::registry::ShmemRegistry;
use flux_communication::{shmem_queue_with_base_dir, ShmemData};
use flux_utils::directories::{local_share_dir, shmem_dir_queues_with_base};
use shared_memory::ShmemConf;

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
struct Quote {
    bid: u64,
    ask: u64,
    bid_size: u32,
    ask_size: u32,
    sequence: u64,
}

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
struct Trade {
    price: u64,
    quantity: u32,
    _pad: u32,
    trade_id: u64,
}

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
struct OrderEvent {
    order_id: u64,
    filled_qty: u32,
    remaining: u32,
    status: u64,
}

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
struct RiskMetrics {
    exposure: u64,
    pnl: i64,
    open_orders: u32,
    _pad: u32,
}

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
struct EngineState {
    uptime_secs: u64,
    msgs_processed: u64,
    errors: u64,
}

struct Flags {
    no_cleanup: bool,
    reattach: bool,
    workers: usize,
    poison: bool,
    is_worker: bool,
    is_poison_crash: bool,
}

fn parse_flags() -> Flags {
    let args: Vec<String> = std::env::args().collect();
    let workers = args
        .windows(2)
        .find(|w| w[0] == "--workers")
        .and_then(|w| w[1].parse().ok())
        .unwrap_or(0);
    Flags {
        no_cleanup: args.iter().any(|a| a == "--no-cleanup"),
        reattach: args.iter().any(|a| a == "--reattach"),
        workers,
        poison: args.iter().any(|a| a == "--poison"),
        is_worker: args.iter().any(|a| a == "--_worker"),
        is_poison_crash: args.iter().any(|a| a == "--_poison_crash"),
    }
}

fn main() {
    let flags = parse_flags();

    if flags.is_poison_crash {
        run_poison_crash();
    } else if flags.is_worker {
        run_worker();
    } else {
        run_primary(flags);
    }
}

fn run_primary(flags: Flags) {
    let base_dir = local_share_dir();
    let stop = Arc::new(AtomicBool::new(false));

    {
        let stop = stop.clone();
        ctrlc::set_handler(move || {
            stop.store(true, Ordering::Relaxed);
        })
        .expect("set Ctrl-C handler");
    }

    let mode = if flags.reattach { "reattach" } else { "new session" };
    let cleanup = if flags.no_cleanup { "no cleanup" } else { "cleanup on exit" };

    println!("╔══════════════════════════════════════════════════╗");
    println!("║  flux-ctl live demo                              ║");
    println!("║                                                  ║");
    println!("║  Simulating two apps writing shmem queues.       ║");
    println!("║  Open another terminal and run:                  ║");
    println!("║                                                  ║");
    println!("║    cargo run -p flux-ctl -- watch                ║");
    println!("║                                                  ║");
    println!("║  Press Ctrl-C to stop.                           ║");
    println!("╚══════════════════════════════════════════════════╝");
    println!("  mode: {mode}, {cleanup}, workers: {}\n", flags.workers);

    if !flags.reattach {
        let registry = ShmemRegistry::open_or_create(&base_dir);
        registry.cleanup_app("market-data");
        registry.cleanup_app("order-engine");
    }

    let quote_q: Queue<Quote> =
        shmem_queue_with_base_dir(&base_dir, "market-data", 4096, QueueType::SPMC);
    let trade_q: Queue<Trade> =
        shmem_queue_with_base_dir(&base_dir, "market-data", 1024, QueueType::SPMC);
    let _risk: ShmemData<RiskMetrics> =
        ShmemData::open_or_init_with_base_dir(&base_dir, "market-data", RiskMetrics::default)
            .unwrap();

    let order_q: Queue<OrderEvent> =
        shmem_queue_with_base_dir(&base_dir, "order-engine", 2048, QueueType::MPMC);
    let _state: ShmemData<EngineState> =
        ShmemData::open_or_init_with_base_dir(&base_dir, "order-engine", EngineState::default)
            .unwrap();

    // Spawn child worker processes that attach to the same segments
    let mut children: Vec<std::process::Child> = Vec::new();
    let exe = std::env::current_exe().expect("resolve own exe path");
    for i in 0..flags.workers {
        let child = Command::new(&exe)
            .arg("--_worker")
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn worker {i}: {e}"));
        println!("  spawned worker {i} (pid {})", child.id());
        children.push(child);
    }

    let mut handles = Vec::new();

    // Quotes: ~1000 msgs/sec
    {
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut p = Producer::from(quote_q);
            let mut seq = 0u64;
            while !stop.load(Ordering::Relaxed) {
                p.produce(&Quote {
                    bid: 42000 + (seq % 100),
                    ask: 42001 + (seq % 100),
                    bid_size: 10 + (seq % 50) as u32,
                    ask_size: 10 + (seq % 50) as u32,
                    sequence: seq,
                });
                seq += 1;
                thread::sleep(Duration::from_millis(1));
            }
            println!("  quotes: sent {seq} messages");
        }));
    }

    // Trades: ~100 msgs/sec
    {
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut p = Producer::from(trade_q);
            let mut id = 0u64;
            while !stop.load(Ordering::Relaxed) {
                p.produce(&Trade {
                    price: 42000 + (id % 200),
                    quantity: 1 + (id % 10) as u32,
                    _pad: 0,
                    trade_id: id,
                });
                id += 1;
                thread::sleep(Duration::from_millis(10));
            }
            println!("  trades: sent {id} messages");
        }));
    }

    // Orders: ~10 msgs/sec
    {
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut p = Producer::from(order_q);
            let mut id = 0u64;
            while !stop.load(Ordering::Relaxed) {
                p.produce(&OrderEvent {
                    order_id: 10000 + id,
                    filled_qty: (id % 100) as u32,
                    remaining: (100 - id % 100) as u32,
                    status: if id % 5 == 0 { 2 } else { 1 },
                });
                id += 1;
                thread::sleep(Duration::from_millis(100));
            }
            println!("  orders: sent {id} messages");
        }));
    }

    // --poison: after 3s, spawn a child that opens the trade queue shmem,
    // sets a seqlock slot's version to odd (simulating a mid-write crash),
    // then aborts. The TUI will show ☠ poisoned on the Trade segment.
    if flags.poison {
        let exe = std::env::current_exe().expect("resolve own exe path");
        let stop2 = stop.clone();
        thread::spawn(move || {
            for _ in 0..30 {
                if stop2.load(Ordering::Relaxed) {
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
            println!("\n  💉 --poison: spawning child to crash mid-write on Trade queue...");
            let mut child = Command::new(&exe)
                .arg("--_poison_crash")
                .spawn()
                .expect("spawn poison child");
            let status = child.wait().expect("wait poison child");
            println!("  💉 poison child exited: {status} — Trade queue should now show ☠ poisoned");
        });
    }

    while !stop.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }

    println!("\nStopping producers...");
    for h in handles {
        h.join().unwrap();
    }

    // Wait for worker children (Ctrl-C already propagated via process group)
    for (i, mut child) in children.into_iter().enumerate() {
        match child.wait() {
            Ok(status) => println!("  worker {i} exited: {status}"),
            Err(e) => eprintln!("  worker {i} wait error: {e}"),
        }
    }

    if flags.no_cleanup {
        println!("Skipping cleanup (--no-cleanup). Segments remain in shared memory.");
    } else {
        println!("Cleaning up shmem...");
        let registry = ShmemRegistry::open_or_create(&base_dir);
        registry.cleanup_app("market-data");
        registry.cleanup_app("order-engine");
        println!("Done.");
    }
}

/// Worker process: attaches to the same segments as the primary and consumes
/// messages in a loop until killed (Ctrl-C propagates from the parent's
/// process group).
fn run_worker() {
    let base_dir = local_share_dir();
    let stop = Arc::new(AtomicBool::new(false));

    {
        let stop = stop.clone();
        ctrlc::set_handler(move || {
            stop.store(true, Ordering::Relaxed);
        })
        .expect("set Ctrl-C handler");
    }

    let pid = std::process::id();
    eprintln!("  [worker {pid}] attaching to segments...");

    // Attach to existing queues (open-or-create reattaches; registration
    // records this PID against the same flink)
    let quote_q: Queue<Quote> =
        shmem_queue_with_base_dir(&base_dir, "market-data", 4096, QueueType::SPMC);
    let trade_q: Queue<Trade> =
        shmem_queue_with_base_dir(&base_dir, "market-data", 1024, QueueType::SPMC);
    let _risk: ShmemData<RiskMetrics> =
        ShmemData::open_or_init_with_base_dir(&base_dir, "market-data", RiskMetrics::default)
            .unwrap();

    let order_q: Queue<OrderEvent> =
        shmem_queue_with_base_dir(&base_dir, "order-engine", 2048, QueueType::MPMC);
    let _state: ShmemData<EngineState> =
        ShmemData::open_or_init_with_base_dir(&base_dir, "order-engine", EngineState::default)
            .unwrap();

    let mut handles = Vec::new();

    // Consumer on quotes
    {
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut c = Consumer::from(quote_q);
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if c.consume(|_| {}) {
                    n += 1;
                } else {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            eprintln!("  [worker {pid}] quotes consumed: {n}");
        }));
    }

    // Consumer on trades
    {
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut c = Consumer::from(trade_q);
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if c.consume(|_| {}) {
                    n += 1;
                } else {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            eprintln!("  [worker {pid}] trades consumed: {n}");
        }));
    }

    // Consumer on orders
    {
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut c = Consumer::from(order_q);
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if c.consume(|_| {}) {
                    n += 1;
                } else {
                    thread::sleep(Duration::from_millis(10));
                }
            }
            eprintln!("  [worker {pid}] orders consumed: {n}");
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    eprintln!("  [worker {pid}] exiting");
}

/// Simulates a process crashing mid-write on the Trade queue.
///
/// Opens the shmem backing the Trade queue, finds the seqlock buffer,
/// sets a slot's version to odd (exactly what `Seqlock::write` does as its
/// first step via `fetch_add(1)`), then aborts without completing the write.
fn run_poison_crash() {
    let base_dir = local_share_dir();
    let flink = shmem_dir_queues_with_base(&base_dir, "market-data").join("Trade");
    let pid = std::process::id();

    eprintln!("  [poison {pid}] opening Trade queue at {}", flink.display());

    let shmem = ShmemConf::new()
        .flink(&flink)
        .open()
        .expect("open Trade queue shmem");

    let base = shmem.as_ptr();

    // QueueHeader is repr(C, align(64)) → 64 bytes, buffer follows.
    // Each slot is elsize bytes with AtomicU64 version at offset 0.
    const HEADER_SIZE: usize = 64;
    let elsize = unsafe { &*(base as *const flux_communication::queue::QueueHeader) }.elsize;

    // Pick slot 3 (arbitrary, away from the active write head)
    let slot = 3;
    let version_ptr = unsafe { base.add(HEADER_SIZE + slot * elsize) } as *const AtomicU64;
    let version = unsafe { &*version_ptr };

    // Do exactly what Seqlock::write does first: fetch_add(1) → version becomes odd
    let v = version.fetch_add(1, Ordering::Release);
    eprintln!("  [poison {pid}] slot {slot} version {v} → {} (odd = write in progress)", v + 1);
    eprintln!("  [poison {pid}] aborting — write will never complete ☠");

    // Abort without completing the write. The slot stays at odd version forever.
    std::process::abort();
}
