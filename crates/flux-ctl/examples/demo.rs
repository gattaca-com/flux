//! Demo: create some shmem queues and data, then launch flux-ctl watch.
//!
//! Run with:
//!   cargo run --example demo -p flux-ctl

use flux_communication::{
    ShmemData,
    queue::{Producer, Queue, QueueType},
    shmem_queue_with_base_dir,
};
use flux_ctl::discovery::scan_base_dir;
use flux_utils::directories::local_share_dir;

/// Example message types
#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
struct PriceUpdate {
    price: u64,
    volume: u64,
    timestamp: u64,
}

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
struct OrderFill {
    order_id: u64,
    filled_qty: u64,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct AppConfig {
    max_orders: u64,
    tick_rate_ms: u64,
    version: u64,
}

fn main() {
    let base_dir = local_share_dir();
    let app_name = "flux-demo";

    println!("Base dir: {}", base_dir.display());
    println!("Shmem dir: {}\n", base_dir.display());

    // Create some queues (auto-registered)
    println!("Creating shmem queues...");
    let price_q: Queue<PriceUpdate> =
        shmem_queue_with_base_dir(&base_dir, app_name, 1024, QueueType::SPMC);
    let order_q: Queue<OrderFill> =
        shmem_queue_with_base_dir(&base_dir, app_name, 256, QueueType::MPMC);

    // Create shmem data (auto-registered)
    println!("Creating shmem data...");
    let _config: ShmemData<AppConfig> =
        ShmemData::open_or_init_with_base_dir(&base_dir, app_name, AppConfig::default).unwrap();

    // Write some messages into the queues
    let mut price_producer = Producer::from(price_q);
    let mut order_producer = Producer::from(order_q);

    for i in 0..50 {
        price_producer.produce(&PriceUpdate {
            price: 42000 + i,
            volume: 100 * (i + 1),
            timestamp: i,
        });
    }
    for i in 0..10 {
        order_producer.produce(&OrderFill { order_id: 1000 + i, filled_qty: 5 * (i + 1) });
    }
    println!("Wrote 50 PriceUpdates and 10 OrderFills\n");

    // Discover segments via filesystem scan
    let entries = scan_base_dir(&base_dir);
    println!("Discovered {} entries:", entries.len());
    for entry in &entries {
        println!(
            "  {} {:>14} {:>16}  elem={}B  cap={}",
            entry.kind, entry.app_name, entry.type_name, entry.elem_size, entry.capacity,
        );
    }

    println!("\n─── Now run these commands ───");
    println!("  cargo run -p flux-ctl -- list --verbose");
    println!("  cargo run -p flux-ctl -- inspect {app_name}");
    println!("  cargo run -p flux-ctl -- watch");
    println!("  cargo run -p flux-ctl -- clean");
}
