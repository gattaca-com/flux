use std::{hint::black_box, sync::Arc, thread};

use core_affinity::CoreId;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use crossbeam_channel::TryRecvError;
use flux_communication::{
    ReadError,
    queue::{ConsumerBare, Producer, Queue, QueueType},
};
use flux_timing::{Duration, Instant};
use flux_utils::{DCache, DCacheRef};

#[derive(Clone, Copy)]
struct Msg {
    ds_ix: usize,
    r: DCacheRef,
}

const CAPACITY: usize = 64 * 1024 * 1024; // ~164 x 400KB messages before ring wraps
const QUEUE_LEN: usize = 1024 * 16;
const MSG_SIZE: usize = 64;
const LAST_CORE: usize = 15;

#[inline]
fn now_nanos() -> u64 {
    Instant::now().0
}

#[inline]
fn write_ts(buf: &mut [u8]) {
    buf[..8].copy_from_slice(&now_nanos().to_ne_bytes());
}

#[inline]
fn read_ts(buf: &[u8]) -> u64 {
    now_nanos() - u64::from_ne_bytes(buf[..8].try_into().unwrap())
}

#[inline]
fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

#[inline]
fn busy_wait(duration: Duration) {
    let curr = Instant::now();
    while curr.elapsed() < duration {}
}

// Caller must ensure n_producers * per_producer <= QUEUE_LEN to prevent
// ConsumerBare version corruption from queue lapping.

// Single shared DCache, one consumer thread. Returns sum of write-to-read
// latencies.
fn run_mp(n_producers: usize, msg_size: usize, per_producer: usize) -> Duration {
    let total = n_producers * per_producer;
    let dc: Arc<DCache> = Arc::from(DCache::new(CAPACITY));
    let queue = Queue::<DCacheRef>::new(QUEUE_LEN, QueueType::MPMC);

    let dc_c = dc.clone();
    let consumer = thread::spawn(move || {
        core_affinity::set_for_current(CoreId { id: LAST_CORE });
        let mut c = ConsumerBare::new(queue, "bench");
        let mut r = DCacheRef { offset: 0, len: 0 };
        let mut sum = 0u64;
        let mut seen = 0usize;
        while seen < total {
            match c.try_consume(&mut r) {
                Ok(()) => {
                    sum += dc_c.map(r, read_ts).unwrap();
                    seen += 1;
                }
                Err(ReadError::SpedPast) => c.recover_after_error(),
                Err(ReadError::Empty) => {}
            }
        }
        sum
    });

    thread::sleep(Duration::from_millis(100).into());

    let producers: Vec<_> = (0..n_producers)
        .map(|i| {
            let dc = dc.clone();
            let p = Producer::from(queue);
            thread::spawn(move || {
                core_affinity::set_for_current(CoreId { id: LAST_CORE - 1 - i });
                let mut rng = now_nanos();
                for _ in 0..per_producer {
                    let r = dc.write(msg_size, write_ts).unwrap();
                    p.produce_without_first(&r);
                    let duration = Duration::from_micros(xorshift(&mut rng) % 100);
                    busy_wait(duration);
                }
            })
        })
        .collect();

    for p in producers {
        p.join().unwrap();
    }
    Duration::from_nanos(consumer.join().unwrap())
}

// Per-producer DCache, one consumer thread. Returns sum of write-to-read
// latencies.
fn run_sp(n_producers: usize, msg_size: usize, per_producer: usize) -> Duration {
    let total = n_producers * per_producer;
    let stores: Vec<Arc<DCache>> =
        (0..n_producers).map(|_| Arc::from(DCache::new(CAPACITY))).collect();
    let queue = Queue::<Msg>::new(QUEUE_LEN, QueueType::MPMC);

    let consumer_stores = stores.clone();
    let consumer = thread::spawn(move || {
        core_affinity::set_for_current(CoreId { id: LAST_CORE });
        let mut c = ConsumerBare::new(queue, "bench");
        let mut slot = Msg { ds_ix: 0, r: DCacheRef { offset: 0, len: 0 } };
        let mut sum = 0u64;
        let mut seen = 0usize;
        while seen < total {
            match c.try_consume(&mut slot) {
                Ok(()) => {
                    sum += consumer_stores[slot.ds_ix].map(slot.r, read_ts).unwrap();
                    seen += 1;
                }
                Err(ReadError::SpedPast) => c.recover_after_error(),
                Err(ReadError::Empty) => {}
            }
        }
        sum
    });

    thread::sleep(Duration::from_millis(100).into());

    let producers: Vec<_> = stores
        .iter()
        .enumerate()
        .map(|(i, dc)| {
            let dc = dc.clone();
            let p = Producer::from(queue);
            thread::spawn(move || {
                core_affinity::set_for_current(CoreId { id: LAST_CORE - 1 - i });
                let mut rng = now_nanos();
                for _ in 0..per_producer {
                    let r = dc.write(msg_size, write_ts).unwrap();
                    p.produce_without_first(&Msg { ds_ix: i, r });
                    let duration = Duration::from_micros(xorshift(&mut rng) % 100);
                    busy_wait(duration);
                }
            })
        })
        .collect();

    for p in producers {
        p.join().unwrap();
    }
    Duration::from_nanos(consumer.join().unwrap())
}

// Single shared DCache, multiple consumer threads. Returns average-per-consumer
// sum of write-to-read latencies (comparable to run_mp).
fn run_mp_mc(
    n_producers: usize,
    n_consumers: usize,
    msg_size: usize,
    per_producer: usize,
) -> Duration {
    let total = n_producers * per_producer;
    let dc: Arc<DCache> = Arc::from(DCache::new(CAPACITY));
    let queue = Queue::<DCacheRef>::new(QUEUE_LEN, QueueType::MPMC);

    let consumers: Vec<_> = (0..n_consumers)
        .map(|i| {
            let dc_c = dc.clone();
            let mut c = ConsumerBare::new(queue, "bench");
            thread::spawn(move || {
                core_affinity::set_for_current(CoreId { id: LAST_CORE - i });
                let mut r = DCacheRef { offset: 0, len: 0 };
                let mut sum = 0u64;
                let mut seen = 0usize;
                while seen < total {
                    match c.try_consume(&mut r) {
                        Ok(()) => {
                            sum += dc_c.map(r, read_ts).unwrap();
                            seen += 1;
                        }
                        Err(ReadError::SpedPast) => c.recover_after_error(),
                        Err(ReadError::Empty) => {}
                    }
                }
                sum
            })
        })
        .collect();

    thread::sleep(Duration::from_millis(100).into());

    let producers: Vec<_> = (0..n_producers)
        .map(|i| {
            let dc = dc.clone();
            let p = Producer::from(queue);
            thread::spawn(move || {
                core_affinity::set_for_current(CoreId { id: LAST_CORE - n_consumers - i });
                let mut rng = now_nanos();
                for _ in 0..per_producer {
                    let r = dc.write(msg_size, write_ts).unwrap();
                    p.produce_without_first(&r);
                    let duration = Duration::from_micros(xorshift(&mut rng) % 100);
                    busy_wait(duration);
                }
            })
        })
        .collect();

    for p in producers {
        p.join().unwrap();
    }
    let total_sum: u64 = consumers.into_iter().map(|c| c.join().unwrap()).sum();
    Duration::from_nanos(total_sum / n_consumers as u64)
}

// Per-producer DCache, multiple consumer threads. Returns average-per-consumer
// sum of write-to-read latencies (comparable to run_sp).
fn run_sp_mc(
    n_producers: usize,
    n_consumers: usize,
    msg_size: usize,
    per_producer: usize,
) -> Duration {
    let total = n_producers * per_producer;
    let stores: Vec<Arc<DCache>> =
        (0..n_producers).map(|_| Arc::from(DCache::new(CAPACITY))).collect();
    let queue = Queue::<Msg>::new(QUEUE_LEN, QueueType::MPMC);

    let consumers: Vec<_> = (0..n_consumers)
        .map(|i| {
            let consumer_stores = stores.clone();
            let mut c = ConsumerBare::new(queue, "bench");
            thread::spawn(move || {
                core_affinity::set_for_current(CoreId { id: LAST_CORE - i });
                let mut slot = Msg { ds_ix: 0, r: DCacheRef { offset: 0, len: 0 } };
                let mut sum = 0u64;
                let mut seen = 0usize;
                while seen < total {
                    match c.try_consume(&mut slot) {
                        Ok(()) => {
                            sum += consumer_stores[slot.ds_ix].map(slot.r, read_ts).unwrap();
                            seen += 1;
                        }
                        Err(ReadError::SpedPast) => c.recover_after_error(),
                        Err(ReadError::Empty) => {}
                    }
                }
                sum
            })
        })
        .collect();

    thread::sleep(Duration::from_millis(100).into());

    let producers: Vec<_> = stores
        .iter()
        .enumerate()
        .map(|(i, dc)| {
            let dc = dc.clone();
            let p = Producer::from(queue);
            thread::spawn(move || {
                core_affinity::set_for_current(CoreId { id: LAST_CORE - n_consumers - i });
                let mut rng = now_nanos();
                for _ in 0..per_producer {
                    let r = dc.write(msg_size, write_ts).unwrap();
                    p.produce_without_first(&Msg { ds_ix: i, r });
                    let duration = Duration::from_micros(xorshift(&mut rng) % 100);
                    busy_wait(duration);
                }
            })
        })
        .collect();

    for p in producers {
        p.join().unwrap();
    }
    let total_sum: u64 = consumers.into_iter().map(|c| c.join().unwrap()).sum();
    Duration::from_nanos(total_sum / n_consumers as u64)
}

// Crossbeam baseline. Returns sum of write-to-read latencies.
fn run_crossbeam(n_producers: usize, msg_size: usize, per_producer: usize) -> Duration {
    let total = n_producers * per_producer;
    let (tx, rx) = crossbeam_channel::bounded::<Box<[u8]>>(QUEUE_LEN);

    let consumer = thread::spawn(move || {
        let mut sum = 0u64;
        let mut seen = 0usize;
        while seen < total {
            match rx.try_recv() {
                Ok(msg) => {
                    sum += now_nanos() - u64::from_ne_bytes(msg[..8].try_into().unwrap());
                    seen += 1;
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => break,
            }
        }
        sum
    });

    thread::sleep(Duration::from_millis(100).into());

    let producers: Vec<_> = (0..n_producers)
        .map(|_| {
            let tx = tx.clone();
            thread::spawn(move || {
                let mut rng = now_nanos();
                for _ in 0..per_producer {
                    let mut msg = vec![0u8; msg_size].into_boxed_slice();
                    write_ts(&mut msg);
                    tx.send(black_box(msg.clone())).unwrap();
                    let duration = Duration::from_micros(xorshift(&mut rng) % 100);
                    busy_wait(duration);
                }
            })
        })
        .collect();
    drop(tx);

    for p in producers {
        p.join().unwrap();
    }
    Duration::from_nanos(consumer.join().unwrap())
}

fn measure(
    iters: u64,
    np: usize,
    max_pp: usize,
    mut run: impl FnMut(usize, usize) -> Duration,
) -> Duration {
    let mut remaining = iters as usize;
    let mut elapsed = Duration::ZERO;
    while remaining > 0 {
        let per_producer = (remaining / np).clamp(1, max_pp);
        elapsed += run(np, per_producer);
        remaining = remaining.saturating_sub(np * per_producer);
    }
    elapsed
}

fn bench_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency");
    group.measurement_time(Duration::from_secs(5).into());
    group.sample_size(10);

    for msg_size in [MSG_SIZE] {
        for n in [2, 4] {
            // n_producers * per_producer <= QUEUE_LEN keeps the queue from lapping.
            let max_pp = (QUEUE_LEN / n).max(1);
            group.throughput(Throughput::Bytes(msg_size as u64));
            group.bench_with_input(
                BenchmarkId::new("shared", format!("{n}p_{msg_size}")),
                &n,
                |b, &np| {
                    b.iter_custom(|iters| {
                        measure(iters, np, max_pp, |np, pp| run_mp(np, msg_size, pp)).into()
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("per_producer", format!("{n}p_{msg_size}")),
                &n,
                |b, &np| {
                    b.iter_custom(|iters| {
                        measure(iters, np, max_pp, |np, pp| run_sp(np, msg_size, pp)).into()
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("crossbeam", format!("{n}p_{msg_size}")),
                &n,
                |b, &np| {
                    b.iter_custom(|iters| {
                        measure(iters, np, max_pp, |np, pp| run_crossbeam(np, msg_size, pp)).into()
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("shared_4c", format!("{n}p_{msg_size}")),
                &n,
                |b, &np| {
                    b.iter_custom(|iters| {
                        measure(iters, np, max_pp, |np, pp| run_mp_mc(np, 4, msg_size, pp)).into()
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("per_producer_4c", format!("{n}p_{msg_size}")),
                &n,
                |b, &np| {
                    b.iter_custom(|iters| {
                        measure(iters, np, max_pp, |np, pp| run_sp_mc(np, 4, msg_size, pp)).into()
                    })
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_latency);
criterion_main!(benches);
