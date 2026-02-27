//! Startup benchmark — measures each phase of the TUI refresh cycle.
//!
//! Usage: `cargo run -p flux-ctl --release --example bench_startup`

use flux_ctl::discovery;
use flux_timing::Instant;

fn main() {
    let base = flux_utils::directories::local_share_dir();

    let t0 = Instant::now();
    let entries = discovery::scan_base_dir(&base);
    let t1 = Instant::now();
    eprintln!(
        "scan_base_dir:      {:>10.2}ms  ({} entries)",
        t1.elapsed_since(t0).as_millis(),
        entries.len()
    );

    let proc_map = discovery::scan_proc_fds();
    let t2 = Instant::now();
    eprintln!(
        "scan_proc_fds:      {:>10.2}ms  ({} backing files)",
        t2.elapsed_since(t1).as_millis(),
        proc_map.len()
    );

    let own_pid = std::process::id();
    let mut poison_count = 0usize;
    let mut checked = 0usize;
    for e in &entries {
        let pids = e.pids(&proc_map);
        let alive = pids.iter().any(|&p| p != own_pid);
        if !alive {
            continue;
        }
        checked += 1;
        let pt0 = Instant::now();
        if discovery::PoisonInfo::check_quick(e) == Some(true) {
            poison_count += 1;
        }
        let pt1 = Instant::now();
        let dt = pt1.elapsed_since(pt0).as_millis();
        if dt > 5.0 {
            eprintln!(
                "  SLOW check_quick: {:>8.2}ms  {}/{} cap={}",
                dt, e.app_name, e.type_name, e.capacity
            );
        }
    }
    let t3 = Instant::now();
    eprintln!(
        "poison check_quick: {:>10.2}ms  ({} checked, {} poisoned)",
        t3.elapsed_since(t2).as_millis(),
        checked,
        poison_count
    );

    eprintln!("TOTAL:              {:>10.2}ms", t3.elapsed_since(t0).as_millis());
}
