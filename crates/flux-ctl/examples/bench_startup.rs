//! Startup + per-tick benchmark — measures the ShmemCache approach.
//!
//! Usage: `cargo run -p flux-ctl --release --example bench_startup`

use flux_ctl::discovery::{self, ShmemCache};
use flux_timing::Instant;

fn main() {
    let base = flux_utils::directories::local_share_dir();

    // ── First refresh (cold: dir walk + open all segments) ───────────
    let t0 = Instant::now();
    let mut cache = ShmemCache::new();
    cache.refresh(&base);
    let t1 = Instant::now();
    let entries = cache.read_entries();
    let t2 = Instant::now();
    eprintln!(
        "COLD  refresh_handles:  {:>8.2}ms  ({} segments)",
        t1.elapsed_since(t0).as_millis(),
        entries.len()
    );
    eprintln!(
        "COLD  read_entries:     {:>8.2}ms",
        t2.elapsed_since(t1).as_millis()
    );

    // ── Subsequent ticks (warm: just pointer reads) ──────────────────
    let mut total_warm = 0.0f64;
    let ticks = 100;
    for _ in 0..ticks {
        let tt0 = Instant::now();
        let _entries = cache.read_entries();
        let tt1 = Instant::now();
        total_warm += tt1.elapsed_since(tt0).as_millis();
    }
    eprintln!(
        "WARM  read_entries:     {:>8.2}ms avg over {ticks} ticks",
        total_warm / ticks as f64
    );

    // ── /proc scan (for reference) ───────────────────────────────────
    let t3 = Instant::now();
    let proc_map = discovery::scan_proc_fds();
    let t4 = Instant::now();
    eprintln!(
        "scan_proc_fds:          {:>8.2}ms  ({} backing files)",
        t4.elapsed_since(t3).as_millis(),
        proc_map.len()
    );

    eprintln!(
        "TOTAL first frame:      {:>8.2}ms",
        t4.elapsed_since(t0).as_millis()
    );
}
