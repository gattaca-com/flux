use std::{
    sync::{Arc, Barrier, atomic::Ordering},
    time::{Duration, Instant},
};

use rand::Rng;

use crate::{
    ReadError,
    queue::{Consumer, InnerQueue, Producer, Queue, QueueType},
};

fn get_collaborative_consumer<T: Copy + 'static>(q: &Queue<T>) -> Consumer<T> {
    Consumer::from(*q).without_log()
}

#[test]
fn collaborative_consume_race_with_write() {
    let q: Queue<usize> = Queue::new(16, QueueType::MPMC);
    let inner: &InnerQueue<usize> = &*q;
    // Advance count without writing the seqlock — mirrors the gap between
    // `next_count()` (increments count) and `lock.write()` inside `produce()`.
    inner.header.count.fetch_add(1, Ordering::Release);

    let mut val = 0;
    assert_eq!(
        inner.load(0).read_with_version(&mut val, inner.version_at(0)),
        Err(ReadError::Empty),
        "unwritten slot must appear empty (write in-flight)"
    );

    let barrier = Arc::new(Barrier::new(2));
    let b2 = Arc::clone(&barrier);
    let handle = std::thread::spawn(move || {
        let mut c = get_collaborative_consumer(&q);
        let mut received = 0;
        b2.wait();
        // consume_collaborative returns false when the slot isn't ready yet;
        // loop until the in-flight write completes.
        while !c.consume_collaborative(|x| received = *x) {}
        received
    });

    barrier.wait();
    std::thread::sleep(Duration::from_micros(500));
    inner.load(0).write(&42);

    assert_eq!(handle.join().unwrap(), 42);
}

#[test]
fn collaborative_sped_past() {
    // capacity=4, produce 7 → ring slots 0,1,2 written twice (seqlock version 4).
    // Consumer claims absolute slots 0,1,2 expecting version 2, finds 4 → SpedPast.
    // Each SpedPast advances the consumer to the next unclaimed slot; messages
    // 0,1,2 are lost.
    for typ in [QueueType::SPMC, QueueType::MPMC] {
        let q: Queue<usize> = Queue::new(4, typ);
        let mut p = Producer::from(q);
        let mut c = get_collaborative_consumer(&q);

        for i in 0..7 {
            p.produce(&i);
        }

        let mut received = Vec::new();
        while received.len() < 4 {
            c.consume_collaborative(|x| received.push(*x));
        }

        assert_eq!(received, [3, 4, 5, 6]);
    }
}

#[test]
fn collaborative_basic() {
    for typ in [QueueType::SPMC, QueueType::MPMC] {
        let q = Queue::new(16, typ);
        let mut p = Producer::from(q);
        let mut c = get_collaborative_consumer(&q);
        let mut m = 0;

        p.produce(&1);
        assert!(c.consume_collaborative(|x| m = *x));
        assert_eq!(m, 1);
        assert!(!c.consume_collaborative(|x| m = *x));

        for i in 0..16 {
            p.produce(&i);
        }
        for i in 0..16 {
            assert!(c.consume_collaborative(|x| m = *x));
            assert_eq!(m, i);
        }
        assert!(!c.consume_collaborative(|x| m = *x));
    }
}

#[test]
fn collaborative_and_broadcast_coexist() {
    const N: usize = 8;
    let q: Queue<usize> = Queue::new(16, QueueType::MPMC);
    let mut p = Producer::from(q);

    // Two broadcast consumers — do NOT claim collab slots.
    let mut bc1 = Consumer::from(q);
    let mut bc2 = Consumer::from(q);

    // Two collaborative consumers — each claims a slot via .collaborative().
    let mut cc1 = get_collaborative_consumer(&q);
    let mut cc2 = get_collaborative_consumer(&q);

    for i in 0..N {
        p.produce(&i);
    }

    let expected_sum: usize = (0..N).sum();

    fn spawn_drain<F>(mut step: F) -> std::thread::JoinHandle<(usize, usize)>
    where
        F: FnMut(&mut dyn FnMut(&mut usize)) -> bool + Send + 'static,
    {
        std::thread::spawn(move || {
            let mut sum = 0;
            let mut count = 0;
            while step(&mut |x: &mut usize| {
                sum += *x;
                count += 1;
            }) {}
            (sum, count)
        })
    }

    let bt1 = spawn_drain(move |f| bc1.consume(f));
    let bt2 = spawn_drain(move |f| bc2.consume(f));
    let ct1 = spawn_drain(move |f| cc1.consume_collaborative(f));
    let ct2 = spawn_drain(move |f| cc2.consume_collaborative(f));

    let (bs1, bn1) = bt1.join().unwrap();
    let (bs2, bn2) = bt2.join().unwrap();
    let (cs1, cn1) = ct1.join().unwrap();
    let (cs2, cn2) = ct2.join().unwrap();

    assert_eq!(bn1 + bn2, 2 * N, "broadcast delivers every message to every consumer");
    assert_eq!(bs1 + bs2, expected_sum * 2);

    assert_eq!(cn1 + cn2, N, "collaborative delivers every message to exactly one consumer");
    assert_eq!(cs1 + cs2, expected_sum);
}

#[test]
fn perf_test_collaborative_consumers() {
    const N: usize = 200_000;
    const SIZE: usize = 100_000;
    const CONSUMERS: usize = 4;
    const STOP: u64 = u64::MAX;

    let q: Queue<u64> = Queue::new(SIZE.next_power_of_two(), QueueType::SPMC);
    let mut p = Producer::from(q);

    let start = Instant::now();
    let barrier = Arc::new(Barrier::new(CONSUMERS + 1));

    let mut consumer_handles = Vec::new();
    for _ in 0..CONSUMERS {
        let barrier = Arc::clone(&barrier);
        let mut c = get_collaborative_consumer(&q);

        consumer_handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mut sum_ns: u64 = 0;
            let mut count: usize = 0;
            loop {
                let mut stop = false;
                c.consume_collaborative(|ts| {
                    if *ts == STOP {
                        stop = true;
                    } else {
                        let recv_ns = start.elapsed().as_nanos() as u64;
                        sum_ns += recv_ns.saturating_sub(*ts);
                        count += 1;
                    }
                });
                if stop {
                    break;
                }
            }
            (sum_ns, count)
        }));
    }

    let producer = {
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            let mut rng = rand::thread_rng();
            barrier.wait();
            for _ in 0..N {
                std::thread::sleep(Duration::from_micros(rng.gen_range(0..=10)));
                let ts = start.elapsed().as_nanos() as u64;
                p.produce(&ts);
            }
            for _ in 0..CONSUMERS {
                p.produce(&STOP);
            }
        })
    };

    let (total_sum, total_count) = consumer_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .fold((0u64, 0usize), |(s, c), (s2, c2)| (s + s2, c + c2));
    producer.join().unwrap();

    assert_eq!(total_count, N, "received all {} messages", N);
    println!("latency ({total_count} msgs) — mean: {}ns", total_sum / total_count as u64);
}
