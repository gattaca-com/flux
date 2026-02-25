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
//!   --no-cleanup   Leave shmem segments intact on exit
//!   --reattach     Reattach to existing segments from a previous run
//!
//! Press Ctrl-C to stop.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use flux_communication::queue::{Producer, Queue, QueueType};
use flux_communication::registry::ShmemRegistry;
use flux_communication::{shmem_queue_with_base_dir, ShmemData};
use flux_utils::directories::local_share_dir;

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
}

fn parse_flags() -> Flags {
    let args: Vec<String> = std::env::args().collect();
    Flags {
        no_cleanup: args.iter().any(|a| a == "--no-cleanup"),
        reattach: args.iter().any(|a| a == "--reattach"),
    }
}

fn main() {
    let flags = parse_flags();
    let base_dir = local_share_dir();
    let stop = Arc::new(AtomicBool::new(false));

    {
        let stop = stop.clone();
        ctrlc::set_handler(move || {
            stop.store(true, Ordering::Relaxed);
        })
        .expect("set Ctrl-C handler");
    }

    let mode = if flags.reattach {
        "reattach"
    } else {
        "new session"
    };
    let cleanup = if flags.no_cleanup {
        "no cleanup"
    } else {
        "cleanup on exit"
    };

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
    println!("  mode: {mode}, {cleanup}\n");

    // shmem_queue_with_base_dir and ShmemData::open_or_init both use
    // open-or-create semantics, so they naturally reattach to existing
    // segments when --reattach is used. For a fresh start, clean first.
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

    let mut handles = Vec::new();

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

    while !stop.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }

    println!("\nStopping producers...");
    for h in handles {
        h.join().unwrap();
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
