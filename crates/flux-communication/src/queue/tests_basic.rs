use std::sync::atomic::Ordering;

use crate::{
    ReadError,
    queue::{ConsumerBare, Producer, Queue, QueueHeader, QueueType},
};

#[test]
fn headersize() {
    assert_eq!(4416, std::mem::size_of::<QueueHeader>());
    assert_eq!(72, std::mem::size_of::<ConsumerBare<[u8; 60]>>())
}

#[test]
fn basic() {
    for typ in [QueueType::SPMC, QueueType::MPMC] {
        let q = Queue::new(16, typ);
        let mut p = Producer::from(q);
        let mut c = ConsumerBare::new_broadcast_test(q);
        p.produce(&1);
        let mut m = 0;

        assert_eq!(c.try_consume(&mut m), Ok(()));
        assert_eq!(m, 1);
        assert!(matches!(c.try_consume(&mut m), Err(ReadError::Empty)));
        for i in 0..16 {
            p.produce(&i);
        }
        for i in 0..16 {
            c.try_consume(&mut m).unwrap();
            assert_eq!(m, i);
        }

        assert!(matches!(c.try_consume(&mut m), Err(ReadError::Empty)));

        for _ in 0..20 {
            p.produce(&1);
        }

        assert!(matches!(c.try_consume(&mut m), Err(ReadError::SpedPast)));
    }
}

fn multithread(n_writers: usize, n_readers: usize, tot_messages: usize) {
    // Queue must hold all messages to prevent wrap-around: with a small ring, a
    // slow reader can overshoot the seqlock version across a lap boundary and
    // deadlock on Empty.
    let q = Queue::new(tot_messages.next_power_of_two(), QueueType::MPMC);

    let mut readhandles = Vec::new();
    for _ in 0..n_readers {
        let mut c1 = ConsumerBare::new_broadcast_test(q);
        let cons = std::thread::spawn(move || {
            let mut count = 0;
            let mut sum = 0;
            let mut m = 0;
            while count < tot_messages {
                c1.blocking_consume(&mut m);
                count += 1;
                sum += m;
            }
            assert_eq!(sum, (0..tot_messages).sum::<usize>());
        });
        readhandles.push(cons)
    }
    let mut writehandles = Vec::new();
    for n in 0..n_writers {
        let mut p1 = Producer::from(q);
        let prod1 = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            let mut c = n;
            while c < tot_messages {
                p1.produce(&c);
                c += n_writers;
                std::thread::yield_now();
            }
        });
        writehandles.push(prod1);
    }

    for h in readhandles {
        h.join().unwrap();
    }
    for h in writehandles {
        h.join().unwrap();
    }
}

#[test]
fn multithread_1_2() {
    multithread(1, 2, 100000);
}

#[test]
fn multithread_1_4() {
    multithread(1, 4, 100000);
}

#[test]
fn multithread_2_4() {
    multithread(2, 4, 100000);
}

#[test]
fn multithread_4_4() {
    multithread(4, 4, 100000);
}

#[test]
fn multithread_8_8() {
    multithread(8, 8, 100000);
}

#[test]
fn basic_shared() {
    for typ in [QueueType::SPMC, QueueType::MPMC] {
        let path = std::path::Path::new("/dev/shm/blabla_test");
        let _ = std::fs::remove_file(path);
        let q = Queue::create_or_open_shared(path, 16, typ);
        let mut p = Producer::from(q);
        let mut c = ConsumerBare::new_broadcast_test(q);

        p.produce(&1);
        let mut m = 0;

        assert_eq!(c.try_consume(&mut m), Ok(()));
        assert_eq!(m, 1);
        assert!(matches!(c.try_consume(&mut m), Err(ReadError::Empty)));
        for i in 0..16 {
            p.produce(&i);
        }
        for i in 0..16 {
            c.try_consume(&mut m).unwrap();
            assert_eq!(m, i);
        }

        assert!(matches!(c.try_consume(&mut m), Err(ReadError::Empty)));

        for _ in 0..20 {
            p.produce(&1);
        }

        assert!(matches!(c.try_consume(&mut m), Err(ReadError::SpedPast)));
        let _ = crate::cleanup::cleanup_flink(path);
    }
}

#[test]
fn active_groups_round_trip() {
    let q = Queue::<u64>::new(16, QueueType::SPMC);
    let header: &mut QueueHeader =
        &mut unsafe { &mut *(q.inner as *mut super::InnerQueue<u64>) }.header;

    // Initially no groups.
    assert!(header.active_groups().is_empty());

    // Register two groups and write cursor values.
    let cursor_a = header.find_or_insert_group("app.stream.broadcast");
    unsafe { &*cursor_a }.store(42, Ordering::Relaxed);

    let cursor_b = header.find_or_insert_group("relay.stream.collab");
    unsafe { &*cursor_b }.store(100, Ordering::Relaxed);

    let groups = header.active_groups();
    assert_eq!(groups.len(), 2);

    let (label_a, val_a) = &groups[0];
    assert_eq!(*label_a, "app.stream.broadcast");
    assert_eq!(*val_a, 42);

    let (label_b, val_b) = &groups[1];
    assert_eq!(*label_b, "relay.stream.collab");
    assert_eq!(*val_b, 100);

    // Same key returns existing slot — no duplicate.
    let cursor_a2 = header.find_or_insert_group("app.stream.broadcast");
    assert_eq!(cursor_a, cursor_a2);
    assert_eq!(header.active_groups().len(), 2);
}
