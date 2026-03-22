//! Smoke test: spawns a mini flux app with multiple tiles that emit
//! `TileMetrics`, so you can observe them in the flux-ctl Tiles tab.
//!
//! Terminal 1 — run the app:
//!   cargo run --example tile_smoke -p flux-ctl
//!
//! Terminal 2 — open flux-ctl and press → or 2 to switch to the Tiles tab:
//!   cargo run -p flux-ctl -- --base-dir /tmp/flux-tile-smoke watch
//!
//! Press Ctrl-C in terminal 1 to stop.

use std::sync::{
    atomic::Ordering,
    Arc,
};

use flux::{
    communication::{ShmemData, cleanup_shmem},
    persistence::Persistable,
    spine::{SpineAdapter, SpineQueue},
    spine_derive::from_spine,
    tile::{Tile, TileConfig, TileInfo, attach_tile},
};
use flux_timing::Duration;
use serde::{Deserialize, Serialize};

// ── Message types ───────────────────────────────────────────────────────────

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
#[repr(C)]
struct Quote {
    bid: u64,
    ask: u64,
    seq: u64,
}

impl Persistable for Quote {
    const PERSIST_DIR: &'static str = "quote";
}

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
#[repr(C)]
struct Signal {
    value: f64,
    _pad: u64,
}

impl Persistable for Signal {
    const PERSIST_DIR: &'static str = "signal";
}

// ── Spine ───────────────────────────────────────────────────────────────────

#[from_spine("smoke-app")]
#[derive(Debug)]
struct SmokeSpine {
    pub tile_info: ShmemData<TileInfo>,
    #[queue(size(2usize.pow(14)))]
    pub quotes: SpineQueue<Quote>,
    #[queue(size(2usize.pow(14)))]
    pub signals: SpineQueue<Signal>,
}

// ── Tiles ───────────────────────────────────────────────────────────────────

/// Hot producer: blasts quotes as fast as possible → high utilisation.
#[derive(Clone, Copy, Default)]
struct QuoteIngester {
    seq: u64,
}

impl Tile<SmokeSpine> for QuoteIngester {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<SmokeSpine>) {
        self.seq += 1;
        adapter.produce(Quote {
            bid: 42000 + (self.seq % 100),
            ask: 42001 + (self.seq % 100),
            seq: self.seq,
        });
    }
}

/// Medium load: reads quotes, emits a signal every N quotes.
#[derive(Clone, Copy, Default)]
struct SignalGen {
    count: u64,
}

impl Tile<SmokeSpine> for SignalGen {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<SmokeSpine>) {
        adapter.consume(|_q: Quote, _| {
            self.count += 1;
        });
        if self.count >= 128 {
            adapter.produce(Signal { value: self.count as f64 * 0.01, _pad: 0 });
            self.count = 0;
        }
    }
}

/// Light consumer: drains signals slowly → low utilisation.
#[derive(Clone, Copy, Default)]
struct Reporter;

impl Tile<SmokeSpine> for Reporter {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<SmokeSpine>) {
        adapter.consume(|_s: Signal, _| {});
    }
}

// ── Main ────────────────────────────────────────────────────────────────────

const BASE_DIR: &str = "/tmp/flux-tile-smoke";

fn main() {
    let base = std::path::Path::new(BASE_DIR);
    let _ = std::fs::create_dir_all(base);

    // Clean up any leftover segments from a previous run.
    let _ = cleanup_shmem(base);

    let mut spine = SmokeSpine::new_with_base_dir(base, None);

    println!("Tiles running.  In another terminal:");
    println!("  cargo run -p flux-ctl -- --base-dir {BASE_DIR} watch");
    println!("Press → or 2 to switch to the Tiles tab.  Ctrl-C here to stop.\n");

    std::thread::scope(|scope| {
        let mut scoped = flux::spine::ScopedSpine::new(&mut spine, scope, None, None);
        let stop = Arc::clone(&scoped.stop_flag);

        // Hot tile — no pacing
        attach_tile(QuoteIngester::default(), &mut scoped, TileConfig::background(None, None));

        // Medium tile — slight pacing
        attach_tile(
            SignalGen::default(),
            &mut scoped,
            TileConfig::background(None, Some(Duration::from_micros(100))),
        );

        // Light tile — slow loop
        attach_tile(
            Reporter,
            &mut scoped,
            TileConfig::background(None, Some(Duration::from_millis(50))),
        );

        // Spin until Ctrl-C (signal_hook sets stop_flag via SIGINT).
        while stop.load(Ordering::Relaxed) == 0 {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    });

    println!("Cleaning up shmem…");
    let _ = cleanup_shmem(base);
    println!("Done.");
}
