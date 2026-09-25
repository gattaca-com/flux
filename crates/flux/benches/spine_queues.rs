#[path = "spine_queues/adapters.rs"]
mod adapters;
#[path = "../../../benches/queue_support.rs"]
mod support;

use support::{Case, Settings, abort_on_panic, dispatch_layout};

const CASES: [&str; 6] =
    ["MPMC-none", "SPMC-none", "SPSC-none", "MPMC-all", "SPMC-all", "SPSC-all"];

fn compare<const B: usize, const SLOT_SIZE: usize>(settings: &Settings, telemetry: Option<&str>) {
    let mut cases = CASES.map(|name| Case::<B>::new(name, settings));
    let orders = [
        [0, 1, 5, 2, 4, 3],
        [1, 2, 0, 3, 5, 4],
        [2, 3, 1, 4, 0, 5],
        [3, 4, 2, 5, 1, 0],
        [4, 5, 3, 0, 2, 1],
        [5, 0, 4, 1, 3, 2],
    ];
    for run in 1..=settings.runs {
        for index in orders[(run - 1) % orders.len()] {
            let (queue, records) = CASES[index].split_once('-').unwrap();
            if settings.queue.as_deref().is_some_and(|q| q != queue) ||
                telemetry.is_some_and(|t| t != records)
            {
                continue;
            }
            let case = &mut cases[index];
            if records == "all" {
                adapters::run::<B, SLOT_SIZE, true>(queue, case, run);
            } else {
                adapters::run::<B, SLOT_SIZE, false>(queue, case, run);
            }
        }
    }
    cases.iter().for_each(Case::summarize);
}

fn main() {
    abort_on_panic();
    let settings = Settings::read(&["MPMC", "SPMC", "SPSC"]);
    let telemetry = std::env::var("FLUX_BENCH_TELEMETRY").ok();
    assert!(
        telemetry.as_deref().is_none_or(|t| ["none", "all"].contains(&t)),
        "choose telemetry none or all"
    );
    settings.print_header();
    dispatch_layout!(settings, compare, telemetry.as_deref());
}
