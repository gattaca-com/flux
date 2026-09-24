//! MPMC/SPSC through Spine, with consumption telemetry disabled and enabled.
//! See `benches/README.md` for CPU selection, build flags and methodology.

#[path = "spine_queues/adapters.rs"]
mod adapters;
#[path = "../../../benches/queue_support.rs"]
mod support;

use support::{Measurement, Settings, abort_on_panic, print_header, quantile, summarize};

const CASES: [&str; 4] = ["MPMC-none", "SPSC-none", "MPMC-all", "SPSC-all"];

fn compare<const B: usize>(settings: &Settings, telemetry: Option<&str>) {
    let mut results: [Vec<Measurement>; 4] = std::array::from_fn(|_| Vec::new());
    // Reverse and rotate the order so each case runs in different positions.
    let orders = [[0, 1, 2, 3], [3, 2, 1, 0], [1, 0, 3, 2], [2, 3, 0, 1]];
    for run in 0..settings.runs {
        let seed = 0x9e37_79b9 ^ (run as u32 + 1);
        for index in orders[run % orders.len()] {
            let case = CASES[index];
            let (queue, records) = case.split_once('-').unwrap();
            if settings.queue.as_deref().is_some_and(|q| q != queue) ||
                telemetry.is_some_and(|t| t != records)
            {
                continue;
            }
            let result = if index >= 2 {
                adapters::run::<B, true>(index % 2 == 1, settings, seed)
            } else {
                adapters::run::<B, false>(index % 2 == 1, settings, seed)
            };
            if let Some(mut result) = result {
                result.latencies.sort_unstable();
                println!(
                    "run,{case},{B},{},{:.6},{},{}",
                    run + 1,
                    result.mmsg_s,
                    quantile(&result.latencies, 50),
                    quantile(&result.latencies, 95)
                );
                results[index].push(result);
            } else {
                println!("verified,{case},{B}");
            }
        }
    }
    for (case, results) in CASES.into_iter().zip(results) {
        summarize(case, B, results);
    }
}

fn main() {
    abort_on_panic();
    let settings = Settings::read(&["MPMC", "SPSC"]);
    let telemetry = std::env::var("FLUX_BENCH_TELEMETRY").ok();
    assert!(
        telemetry.as_deref().is_none_or(|t| ["none", "all"].contains(&t)),
        "choose telemetry none or all"
    );
    print_header(&settings);
    macro_rules! sizes {
        ($($size:literal),*) => { $(if settings.size.is_none_or(|b| b == $size) {
            compare::<$size>(&settings, telemetry.as_deref());
        })* };
    }
    sizes!(8, 32, 64, 128, 192, 256, 512, 1024);
}
