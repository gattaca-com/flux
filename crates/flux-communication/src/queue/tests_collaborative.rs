use std::sync::{atomic::Ordering, Arc, Barrier};
use std::time::Duration;

use crate::queue::{Consumer, InnerQueue, Producer, Queue, QueueType};
use crate::ReadError;

#[test]
fn collaborative_consume_race_with_write() {
    let q: Queue<usize> = Queue::new(16, QueueType::MPMC);
    let inner: &InnerQueue<usize> = &*q;
    // Advance count without writing the seqlock — mirrors the gap between
    // `next_count()` (increments count) and `lock.write()` inside `produce()`.
    // mirrors the gap between `next_count()` (increments count) and `lock.write()`
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
        let mut c = Consumer::from(q).without_log();
        let mut received = 0;
        b2.wait();
        let got = c.consume_collaborative(|x| received = *x);
        (got, received)
    });

    barrier.wait();
    std::thread::sleep(Duration::from_micros(500)); // let it reach the spin
    inner.load(0).write(&42);

    let (got, received) = handle.join().unwrap();
    assert!(got, "consume_collaborative must not drop in-flight items");
    assert_eq!(received, 42);
}

#[test]
fn collaborative_sped_past() {
    // capacity=4, produce 7 → slots 0,1,2 written twice (version 2 then 4).
    // consume_cursor=0 claims slots 0,1,2 expecting version 2, finds 4 → SpedPast, messages 0,1,2 lost.
    for typ in [QueueType::SPMC, QueueType::MPMC] {
        let q: Queue<usize> = Queue::new(4, typ);
        let mut p = Producer::from(q);
        let mut c = Consumer::from(q).without_log();

        for i in 0..7 {
            p.produce(&i);
        }

        let mut received = Vec::new();
        while c.consume_collaborative(|x| received.push(*x)) {}

        assert_eq!(received, [3, 4, 5, 6]);
    }
}

#[test]
fn collaborative_basic() {
    for typ in [QueueType::SPMC, QueueType::MPMC] {
        let q = Queue::new(16, typ);
        let mut p = Producer::from(q);
        let mut c = Consumer::from(q);
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

    // Two broadcast consumers, both attached before any writes.
    let mut bc1 = Consumer::from(q);
    let mut bc2 = Consumer::from(q);

    // Two collaborative consumers sharing the single consume_cursor.
    let mut cc1 = Consumer::from(q).without_log();
    let mut cc2 = Consumer::from(q).without_log();

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
            while step(&mut |x: &mut usize| { sum += *x; count += 1; }) {}
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
