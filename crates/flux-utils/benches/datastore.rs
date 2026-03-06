use std::{
    hint::{black_box, spin_loop},
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use crossbeam_channel::TryRecvError;
use flux_communication::{
    ReadError,
    queue::{ConsumerBare, Producer, Queue, QueueType},
};
use flux_utils::{DataStore, DataStoreRef};

#[derive(Clone, Copy)]
struct Msg {
    ds_ix: usize,
    r: DataStoreRef,
}

const CAPACITY: usize = 64 * 1024 * 1024; // ~164 x 400KB messages before ring wraps
const QUEUE_LEN: usize = 1024 * 16;

// Caller must ensure n_producers * per_producer <= QUEUE_LEN to prevent
// ConsumerBare version corruption from queue lapping.
fn run_mp_consumer(n_producers: usize, msg_size: usize, per_producer: usize) -> Duration {
    let total = n_producers * per_producer;

    let ds = Arc::new(DataStore::<CAPACITY>::new());
    let queue = Queue::<DataStoreRef>::new(QUEUE_LEN, QueueType::MPMC);
    let payload: Arc<[u8]> = vec![0xABu8; msg_size].into();

    let mut c = ConsumerBare::from(queue);
    let mut r = DataStoreRef { offset: 0, len: 0 };
    let mut buf = vec![0u8; msg_size];

    let producers: Vec<_> = (0..n_producers)
        .map(|_| {
            let ds = ds.clone();
            let p = Producer::from(queue);
            let payload = payload.clone();
            thread::spawn(move || {
                for _ in 0..per_producer {
                    let r = ds
                        .write(payload.len(), |head, tail| {
                            head.copy_from_slice(&payload[..head.len()]);
                            tail.copy_from_slice(&payload[head.len()..]);
                        })
                        .unwrap();
                    p.produce_without_first(&r);
                }
            })
        })
        .collect();

    let t0 = Instant::now();
    let mut seen = 0usize;
    while seen < total {
        match c.try_consume(&mut r) {
            Ok(()) => {
                let _ = ds.read(r, &mut buf[..r.len as usize]);
                black_box(buf.as_slice());
                seen += 1;
            }
            Err(ReadError::SpedPast) => c.recover_after_error(),
            Err(ReadError::Empty) => spin_loop(),
        }
    }
    let elapsed = t0.elapsed();

    for p in producers {
        p.join().unwrap();
    }
    elapsed
}

// Times the main thread's per_producer produce operations; background threads
// add queue contention.
fn run_mp_producer(n_producers: usize, msg_size: usize, per_producer: usize) -> Duration {
    let total = n_producers * per_producer;

    let ds = Arc::new(DataStore::<CAPACITY>::new());
    let queue = Queue::<DataStoreRef>::new(QUEUE_LEN, QueueType::MPMC);
    let payload: Arc<[u8]> = vec![0xABu8; msg_size].into();

    let mut c = ConsumerBare::from(queue);
    let mut r = DataStoreRef { offset: 0, len: 0 };
    let mut buf = vec![0u8; msg_size];
    let consumer = {
        let ds = ds.clone();
        thread::spawn(move || {
            let mut seen = 0usize;
            while seen < total {
                match c.try_consume(&mut r) {
                    Ok(()) => {
                        let _ = ds.read(r, &mut buf[..r.len as usize]);
                        black_box(buf.as_slice());
                        seen += 1;
                    }
                    Err(ReadError::SpedPast) => c.recover_after_error(),
                    Err(ReadError::Empty) => spin_loop(),
                }
            }
        })
    };

    // Barrier ensures all bg producers are scheduled before timing starts.
    let barrier = Arc::new(Barrier::new(n_producers));
    let bg: Vec<_> = (0..n_producers.saturating_sub(1))
        .map(|_| {
            let ds = ds.clone();
            let p = Producer::from(queue);
            let payload = payload.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..per_producer {
                    let r = ds
                        .write(payload.len(), |head, tail| {
                            head.copy_from_slice(&payload[..head.len()]);
                            tail.copy_from_slice(&payload[head.len()..]);
                        })
                        .unwrap();
                    p.produce_without_first(&r);
                }
            })
        })
        .collect();

    let p = Producer::from(queue);
    barrier.wait();
    let t0 = Instant::now();
    for _ in 0..per_producer {
        let r = ds
            .write(payload.len(), |head, tail| {
                head.copy_from_slice(&payload[..head.len()]);
                tail.copy_from_slice(&payload[head.len()..]);
            })
            .unwrap();
        black_box(p.produce_without_first(&r));
    }
    let elapsed = t0.elapsed();

    for b in bg {
        b.join().unwrap();
    }
    consumer.join().unwrap();
    elapsed
}

fn run_mp_crossbeam_consumer(n_producers: usize, msg_size: usize, per_producer: usize) -> Duration {
    let (tx, rx) = crossbeam_channel::bounded::<Box<[u8]>>(QUEUE_LEN);
    let payload: Arc<[u8]> = vec![0xABu8; msg_size].into();
    let total = n_producers * per_producer;

    let mut buf = vec![0u8; msg_size];
    let producers: Vec<_> = (0..n_producers)
        .map(|_| {
            let tx = tx.clone();
            let payload = payload.clone();
            thread::spawn(move || {
                for _ in 0..per_producer {
                    // Arc::from copies the payload, matching the memcpy cost
                    // of ds.write() in the flux variants.
                    tx.send(Box::from(&payload[..])).unwrap();
                }
            })
        })
        .collect();
    drop(tx);

    let t0 = Instant::now();
    let mut seen = 0usize;
    while seen < total {
        match rx.try_recv() {
            Ok(data) => {
                buf.copy_from_slice(&data);
                black_box(buf.as_slice());
                seen += 1;
            }
            Err(TryRecvError::Empty) => spin_loop(),
            Err(TryRecvError::Disconnected) => break,
        }
    }
    let elapsed = t0.elapsed();

    for p in producers {
        p.join().unwrap();
    }
    elapsed
}

fn run_mp_crossbeam_producer(n_producers: usize, msg_size: usize, per_producer: usize) -> Duration {
    let (tx, rx) = crossbeam_channel::bounded::<Box<[u8]>>(QUEUE_LEN);
    let payload: Arc<[u8]> = vec![0xABu8; msg_size].into();
    let total = n_producers * per_producer;

    let mut buf = vec![0u8; msg_size];
    let consumer = thread::spawn(move || {
        let mut seen = 0usize;
        while seen < total {
            match rx.try_recv() {
                Ok(data) => {
                    buf.copy_from_slice(&data);
                    black_box(buf.as_slice());
                    seen += 1;
                }
                Err(TryRecvError::Empty) => spin_loop(),
                Err(TryRecvError::Disconnected) => break,
            }
        }
    });

    let bg: Vec<_> = (0..n_producers.saturating_sub(1))
        .map(|_| {
            let tx = tx.clone();
            let payload = payload.clone();
            thread::spawn(move || {
                for _ in 0..per_producer {
                    tx.send(Box::from(&payload[..])).unwrap();
                }
            })
        })
        .collect();

    let t0 = Instant::now();
    for _ in 0..per_producer {
        tx.send(Box::from(&payload[..])).unwrap();
    }
    let elapsed = t0.elapsed();

    for b in bg {
        b.join().unwrap();
    }
    consumer.join().unwrap();
    elapsed
}

fn run_sp_consumer(n_producers: usize, msg_size: usize, per_producer: usize) -> Duration {
    let total = n_producers * per_producer;

    let stores: Vec<Arc<DataStore<CAPACITY>>> =
        (0..n_producers).map(|_| Arc::new(DataStore::new())).collect();
    let queue = Queue::<Msg>::new(QUEUE_LEN, QueueType::MPMC);
    let payload: Arc<[u8]> = vec![0xABu8; msg_size].into();

    let mut c = ConsumerBare::from(queue);
    let mut slot = Msg { ds_ix: 0, r: DataStoreRef { offset: 0, len: 0 } };
    let mut buf = vec![0u8; msg_size];

    let producers: Vec<_> = stores
        .iter()
        .enumerate()
        .map(|(i, ds)| {
            let ds = ds.clone();
            let p = Producer::from(queue);
            let payload = payload.clone();
            thread::spawn(move || {
                for _ in 0..per_producer {
                    let r = ds
                        .write(payload.len(), |head, tail| {
                            head.copy_from_slice(&payload[..head.len()]);
                            tail.copy_from_slice(&payload[head.len()..]);
                        })
                        .unwrap();
                    p.produce_without_first(&Msg { ds_ix: i, r });
                }
            })
        })
        .collect();

    let t0 = Instant::now();
    let mut seen = 0usize;
    while seen < total {
        match c.try_consume(&mut slot) {
            Ok(()) => {
                let _ = stores[slot.ds_ix].read(slot.r, &mut buf[..slot.r.len as usize]);
                black_box(buf.as_slice());
                seen += 1;
            }
            Err(ReadError::SpedPast) => c.recover_after_error(),
            Err(ReadError::Empty) => spin_loop(),
        }
    }
    let elapsed = t0.elapsed();

    for p in producers {
        p.join().unwrap();
    }
    elapsed
}

fn run_sp_producer(n_producers: usize, msg_size: usize, per_producer: usize) -> Duration {
    let total = n_producers * per_producer;

    let stores: Vec<Arc<DataStore<CAPACITY>>> =
        (0..n_producers).map(|_| Arc::new(DataStore::new())).collect();
    let queue = Queue::<Msg>::new(QUEUE_LEN, QueueType::MPMC);
    let payload: Arc<[u8]> = vec![0xABu8; msg_size].into();

    let consumer_stores = stores.clone();
    let mut c = ConsumerBare::from(queue);
    let mut buf = vec![0u8; msg_size];
    let mut slot = Msg { ds_ix: 0, r: DataStoreRef { offset: 0, len: 0 } };
    let consumer = thread::spawn(move || {
        let mut seen = 0usize;
        while seen < total {
            match c.try_consume(&mut slot) {
                Ok(()) => {
                    let _ =
                        consumer_stores[slot.ds_ix].read(slot.r, &mut buf[..slot.r.len as usize]);
                    black_box(buf.as_slice());
                    seen += 1;
                }
                Err(ReadError::SpedPast) => c.recover_after_error(),
                Err(ReadError::Empty) => spin_loop(),
            }
        }
    });

    // Barrier ensures all bg producers are scheduled before timing starts.
    let barrier = Arc::new(Barrier::new(n_producers));
    // n_producers - 1 background producers add contention on the shared queue.
    let bg: Vec<_> = stores[..n_producers - 1]
        .iter()
        .enumerate()
        .map(|(i, ds)| {
            let ds = ds.clone();
            let p = Producer::from(queue);
            let payload = payload.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..per_producer {
                    let r = ds
                        .write(payload.len(), |head, tail| {
                            head.copy_from_slice(&payload[..head.len()]);
                            tail.copy_from_slice(&payload[head.len()..]);
                        })
                        .unwrap();
                    p.produce_without_first(&Msg { ds_ix: i, r });
                }
            })
        })
        .collect();

    let measured_ds = stores[n_producers - 1].clone();
    let p = Producer::from(queue);
    barrier.wait();
    let t0 = Instant::now();
    for _ in 0..per_producer {
        let r = measured_ds
            .write(payload.len(), |head, tail| {
                head.copy_from_slice(&payload[..head.len()]);
                tail.copy_from_slice(&payload[head.len()..]);
            })
            .unwrap();
        black_box(p.produce_without_first(&Msg { ds_ix: n_producers - 1, r }));
    }
    let elapsed = t0.elapsed();

    for b in bg {
        b.join().unwrap();
    }
    consumer.join().unwrap();
    elapsed
}

fn bench_consumer(c: &mut Criterion) {
    let mut group = c.benchmark_group("consumer");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(10);

    // 50kb, 400kb
    for msg_size in [51200usize, 409600] {
        for n in [1usize, 2, 4] {
            // iters = total messages consumed. Each batch: n_producers * pp messages,
            // with pp <= max_pp guaranteeing the queue never laps.
            let max_pp = (QUEUE_LEN / n).max(1);
            group.throughput(Throughput::Bytes(msg_size as u64));
            group.bench_with_input(
                BenchmarkId::new("mp", format!("{n}p_{msg_size}")),
                &n,
                |b, &np| {
                    b.iter_custom(|iters| {
                        let mut remaining = iters as usize;
                        let mut elapsed = Duration::ZERO;
                        while remaining > 0 {
                            let per_producer = (remaining / np).min(max_pp);
                            if per_producer == 0 {
                                break;
                            }
                            elapsed += run_mp_consumer(np, msg_size, per_producer);
                            remaining -= np * per_producer;
                        }
                        elapsed
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("sp", format!("{n}p_{msg_size}")),
                &n,
                |b, &np| {
                    b.iter_custom(|iters| {
                        let mut remaining = iters as usize;
                        let mut elapsed = Duration::ZERO;
                        while remaining > 0 {
                            let per_producer = (remaining / np).min(max_pp);
                            if per_producer == 0 {
                                break;
                            }
                            elapsed += run_sp_consumer(np, msg_size, per_producer);
                            remaining -= np * per_producer;
                        }
                        elapsed
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("crossbeam", format!("{n}p_{msg_size}")),
                &n,
                |b, &np| {
                    b.iter_custom(|iters| {
                        let mut remaining = iters as usize;
                        let mut elapsed = Duration::ZERO;
                        while remaining > 0 {
                            let per_producer = (remaining / np).min(max_pp);
                            if per_producer == 0 {
                                break;
                            }
                            elapsed += run_mp_crossbeam_consumer(np, msg_size, per_producer);
                            remaining -= np * per_producer;
                        }
                        elapsed
                    })
                },
            );
        }
    }

    group.finish();
}

fn bench_producer(c: &mut Criterion) {
    let mut group = c.benchmark_group("producer");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(10);

    // 50kb, 400kb
    for msg_size in [51200usize, 409600] {
        for n in [1usize, 2, 4] {
            // iters = main-thread produces. Each batch: pp of them, with
            // n_producers * pp <= QUEUE_LEN guaranteeing the queue never laps.
            let max_pp = (QUEUE_LEN / n).max(1);
            group.throughput(Throughput::Bytes(msg_size as u64));
            group.bench_with_input(
                BenchmarkId::new("mp", format!("{n}p_{msg_size}")),
                &n,
                |b, &np| {
                    b.iter_custom(|iters| {
                        let mut remaining = iters as usize;
                        let mut elapsed = Duration::ZERO;
                        while remaining > 0 {
                            let per_producer = remaining.min(max_pp);
                            elapsed += run_mp_producer(np, msg_size, per_producer);
                            remaining -= per_producer;
                        }
                        elapsed
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("sp", format!("{n}p_{msg_size}")),
                &n,
                |b, &np| {
                    b.iter_custom(|iters| {
                        let mut remaining = iters as usize;
                        let mut elapsed = Duration::ZERO;
                        while remaining > 0 {
                            let per_producer = remaining.min(max_pp);
                            elapsed += run_sp_producer(np, msg_size, per_producer);
                            remaining -= per_producer;
                        }
                        elapsed
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("crossbeam", format!("{n}p_{msg_size}")),
                &n,
                |b, &np| {
                    b.iter_custom(|iters| {
                        let mut remaining = iters as usize;
                        let mut elapsed = Duration::ZERO;
                        while remaining > 0 {
                            let per_producer = remaining.min(max_pp);
                            elapsed += run_mp_crossbeam_producer(np, msg_size, per_producer);
                            remaining -= per_producer;
                        }
                        elapsed
                    })
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_consumer, bench_producer,);
criterion_main!(benches);
