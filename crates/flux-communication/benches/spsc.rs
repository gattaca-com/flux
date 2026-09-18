//! Reproducible one-producer/one-consumer queue throughput benchmark.
//!
//! Run on isolated CPUs, for example:
//! `FLUX_BENCH_CPUS=12,13 taskset -c 12,13 cargo bench -p flux-communication
//! --bench spsc`. The three cases are finite, non-overwriting transfers: their
//! capacity is at least the measured message count, so the legacy overwrite
//! queues cannot lap the consumer. They do not represent a steady-state
//! bounded-queue comparison. A full-capacity produce/consume pass touches every
//! slot before timing, so allocation and initial page faults are excluded along
//! with thread setup.

use std::{
    collections::HashSet,
    env,
    hint::spin_loop,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use core_affinity::CoreId;
use flux_communication::{
    ReadError,
    queue::{
        ConsumerBare, Producer as LegacyProducer, Queue as LegacyQueue, QueueType,
        spsc::Queue as SpscQueue,
    },
};

const MESSAGES: usize = 262_144;
const CAPACITY: usize = MESSAGES;
const RUNS: usize = 3;

#[derive(Clone, Copy)]
struct Cpus {
    producer: CoreId,
    consumer: CoreId,
}

fn isolated_cpus() -> HashSet<usize> {
    let configured = std::fs::read_to_string("/sys/devices/system/cpu/isolated")
        .expect("read /sys/devices/system/cpu/isolated before benchmarking");
    let mut cpus = HashSet::new();
    for range in configured.trim().split(',').filter(|range| !range.is_empty()) {
        let (first, last) = range.split_once('-').map_or((range, range), |pair| pair);
        let first = first.parse::<usize>().expect("parse isolated CPU range");
        let last = last.parse::<usize>().expect("parse isolated CPU range");
        cpus.extend(first..=last);
    }
    assert!(!cpus.is_empty(), "no isolated CPUs are configured");
    cpus
}

fn configured_cpus() -> Cpus {
    let configured = env::var("FLUX_BENCH_CPUS")
        .expect("FLUX_BENCH_CPUS is required; set it to two isolated CPUs, e.g. 12,13");
    let ids: Vec<_> = configured
        .split(',')
        .map(|id| id.trim().parse::<usize>().expect("FLUX_BENCH_CPUS must contain CPU numbers"))
        .collect();
    assert_eq!(ids.len(), 2, "FLUX_BENCH_CPUS must name exactly producer,consumer CPUs");
    assert_ne!(ids[0], ids[1], "FLUX_BENCH_CPUS must name distinct CPUs");

    let isolated = isolated_cpus();
    let available: HashSet<_> = core_affinity::get_core_ids()
        .expect("read the process CPU affinity")
        .into_iter()
        .map(|core| core.id)
        .collect();
    for id in &ids {
        assert!(isolated.contains(id), "CPU {id} is not isolated");
        assert!(available.contains(id), "CPU {id} is outside this process's affinity mask");
    }
    Cpus { producer: CoreId { id: ids[0] }, consumer: CoreId { id: ids[1] } }
}

fn pin(core: CoreId) {
    assert!(
        core_affinity::set_for_current(core),
        "failed to pin benchmark thread to CPU {}",
        core.id
    );
}

fn transfer(
    cpus: Cpus,
    producer: impl FnOnce() + Send + 'static,
    consumer: impl FnOnce() + Send + 'static,
) -> Duration {
    let ready = Arc::new(Barrier::new(3));
    let start = Arc::new(Barrier::new(3));
    let producer_thread = {
        let ready = Arc::clone(&ready);
        let start = Arc::clone(&start);
        thread::spawn(move || {
            pin(cpus.producer);
            ready.wait();
            start.wait();
            producer();
        })
    };
    let consumer_thread = {
        let ready = Arc::clone(&ready);
        let start = Arc::clone(&start);
        thread::spawn(move || {
            pin(cpus.consumer);
            ready.wait();
            start.wait();
            consumer();
        })
    };
    ready.wait();
    let started = Instant::now();
    start.wait();
    producer_thread.join().unwrap();
    consumer_thread.join().unwrap();
    started.elapsed()
}

fn run_spsc(cpus: Cpus) -> Duration {
    let queue = SpscQueue::<u64>::new(CAPACITY);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    let mut value = 0;
    for _ in 0..CAPACITY {
        producer.produce(&0).unwrap();
        consumer.try_consume(&mut value).unwrap();
    }
    transfer(
        cpus,
        move || {
            for sequence in 0..MESSAGES as u64 {
                producer.produce(&sequence).unwrap();
            }
        },
        move || {
            for expected in 0..MESSAGES as u64 {
                let mut value = 0;
                while consumer.try_consume(&mut value).is_err() {
                    spin_loop();
                }
                assert_eq!(value, expected);
            }
        },
    )
}

fn run_legacy(kind: QueueType, cpus: Cpus) -> Duration {
    let queue = LegacyQueue::<u64>::new(CAPACITY, kind);
    let mut producer = LegacyProducer::from(queue);
    let mut consumer = ConsumerBare::new(queue, "spsc-throughput");
    consumer.subscribe_broadcast();
    let mut value = 0;
    for _ in 0..CAPACITY {
        producer.produce(&0);
        consumer.try_consume(&mut value).unwrap();
    }
    transfer(
        cpus,
        move || {
            for sequence in 0..MESSAGES as u64 {
                producer.produce(&sequence);
            }
        },
        move || {
            for expected in 0..MESSAGES as u64 {
                let mut value = 0;
                loop {
                    match consumer.try_consume(&mut value) {
                        Ok(()) => {
                            assert_eq!(value, expected);
                            break;
                        }
                        Err(ReadError::Empty) => spin_loop(),
                        Err(ReadError::SpedPast) => panic!("finite baseline lapped its consumer"),
                    }
                }
            }
        },
    )
}

fn report(label: &str, run: usize, elapsed: Duration) {
    let rate = MESSAGES as f64 / elapsed.as_secs_f64() / 1_000_000.0;
    println!("{label:<42} run={run} {rate:>8.2} Mmsg/s ({elapsed:?})");
}

fn main() {
    let cpus = configured_cpus();
    println!(
        "finite transfers: {MESSAGES} u64 messages, capacity {CAPACITY}, CPUs {},{}",
        cpus.producer.id, cpus.consumer.id
    );
    for run in 1..=RUNS {
        report("spsc finite non-overwriting transfer", run, run_spsc(cpus));
        report("spmc finite non-overwriting transfer", run, run_legacy(QueueType::SPMC, cpus));
        report("mpmc finite non-overwriting transfer", run, run_legacy(QueueType::MPMC, cpus));
    }
}
