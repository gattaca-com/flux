use std::{hint::black_box, time::Instant};

use flux_profiler::{InProcessReader, enable_profiler, timed};

#[cfg(feature = "alloc-profile")]
#[global_allocator]
static ALLOC_TRACKER: flux_profiler::allocator::CountingAllocator<std::alloc::System> =
    flux_profiler::allocator::CountingAllocator(std::alloc::System);

const WORK: u32 = 256;

#[inline(always)]
fn work(x: u64) -> u64 {
    let mut v = Vec::with_capacity(16);
    v.push(x);
    black_box(v.as_ptr());
    let mut acc = v[0];

    for _ in 0..WORK {
        acc = acc.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        acc ^= acc >> 29;
    }
    acc
}

#[timed]
#[inline(never)]
fn timed_op(x: u64) -> u64 {
    work(x)
}

#[inline(never)]
fn plain_op(x: u64) -> u64 {
    work(x)
}

const CALLS: u64 = 1_000_000;
const ROUNDS: usize = 9;

fn round(f: impl Fn(u64) -> u64) -> f64 {
    let mut acc = 0u64;
    let start = Instant::now();
    for i in 0..CALLS {
        acc = acc.wrapping_add(f(black_box(i)));
    }
    let ns = start.elapsed().as_nanos() as f64 / CALLS as f64;
    black_box(acc);
    ns
}

fn median(mut xs: [f64; ROUNDS]) -> f64 {
    xs.sort_by(f64::total_cmp);
    xs[ROUNDS / 2]
}

fn measure() -> (f64, f64) {
    let mut plains = [0.0; ROUNDS];
    let mut overheads = [0.0; ROUNDS];
    for k in 0..ROUNDS {
        let p = round(plain_op);
        let t = round(timed_op);
        plains[k] = p;
        overheads[k] = t - p;
    }
    (median(plains), median(overheads))
}

fn main() {
    enable_profiler("timed-overhead");

    let mut warm = 0u64;
    for _ in 0..ROUNDS {
        for i in 0..CALLS {
            warm = warm.wrapping_add(plain_op(black_box(i)));
        }
    }
    black_box(warm);

    let (plain, solo) = measure();

    let reader = InProcessReader::start();
    let (_, loaded) = measure();
    drop(reader.collect());

    let tracking = if cfg!(feature = "alloc-profile") { "ON" } else { "OFF" };
    let perf = if cfg!(feature = "perf") { "ON" } else { "OFF" };
    println!();
    println!(
        "#[timed] cost  (alloc-tracking={tracking}, perf={perf}, \
         plain call {plain:.0} ns):"
    );
    println!();
    println!("  #[timed] overhead ................ {solo:5.1} ns");
    println!("  extra when profiler is reading ... {:+5.1} ns", loaded - solo);
    println!("  ---------------------------------------------");
    println!("  total overhead under load ........ {loaded:5.1} ns");
    println!();
}
