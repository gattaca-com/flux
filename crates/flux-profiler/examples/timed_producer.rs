//! Minimal `#[timed]` producer for exercising the `flux-profiler` CLI end to
//! end: `cargo run -p flux-profiler --example timed_producer [app_name]`,
//! then in another terminal
//! `cargo run -p flux-profiler --bin flux-profiler -- --pid <printed pid>`.

use std::{thread, time::Duration};

use flux_profiler::{enable_profiler, timed};

#[timed]
fn inner(n: u64) -> u64 {
    (0..n).fold(0, |acc, i| acc ^ i.wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

#[timed]
fn outer() -> u64 {
    inner(10_000) ^ inner(20_000)
}

fn main() {
    let app = std::env::args().nth(1).unwrap_or_else(|| "timed-producer".to_owned());
    enable_profiler(&app);
    println!("producing #[timed] marks as pid {}", std::process::id());
    loop {
        std::hint::black_box(outer());
        thread::sleep(Duration::from_millis(10));
    }
}
