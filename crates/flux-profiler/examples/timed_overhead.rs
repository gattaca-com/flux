//! Measure the disabled-`#[timed]` overhead: ns/call of a tiny timed function
//! vs the same function untimed, profiler never enabled.

use std::{hint::black_box, time::Instant};

use flux_profiler::timed;

#[timed]
#[inline(never)]
fn timed_op(x: u64) -> u64 {
    x.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1)
}

#[inline(never)]
fn plain_op(x: u64) -> u64 {
    x.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1)
}

fn bench(label: &str, f: impl Fn(u64) -> u64) {
    const CALLS: u64 = 200_000_000;
    let mut acc = 0u64;
    let start = Instant::now();
    for i in 0..CALLS {
        acc = acc.wrapping_add(f(black_box(i)));
    }
    let elapsed = start.elapsed();
    black_box(acc);
    println!("{label}: {:.2} ns/call", elapsed.as_nanos() as f64 / CALLS as f64);
}

fn main() {
    bench("plain   ", plain_op);
    bench("timed-off", timed_op);
    bench("plain   ", plain_op);
    bench("timed-off", timed_op);
}
