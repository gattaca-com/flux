use crate::{
    ReadError,
    queue::{ConsumerBare, Producer, Queue, QueueHeader, QueueType},
};

#[test]
fn headersize() {
    // Two cache lines: line 0 = producer fields (count), line 1 = consume_cursor
    assert_eq!(64, std::mem::size_of::<QueueHeader>());
    assert_eq!(72, std::mem::size_of::<ConsumerBare<[u8; 60]>>())
}

#[test]
fn basic() {
    for typ in [QueueType::SPMC, QueueType::MPMC] {
        let q = Queue::new(16, typ);
        let mut p = Producer::from(q);
        let mut c = ConsumerBare::new_basic_test(q);
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
        let mut c1 = ConsumerBare::new_basic_test(q);
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
        let mut c = ConsumerBare::new_basic_test(q);

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
