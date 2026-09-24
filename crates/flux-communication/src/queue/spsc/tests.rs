use std::{
    num::NonZeroU64,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant},
};

use tempfile::{Builder, TempDir};

use super::{Queue, QueueError};
use crate::{EmptyError, cleanup_flink};

#[test]
fn factory_preserves_full_panic_publication_and_wrapping_fifo() {
    use std::sync::atomic::Ordering;

    let queue = Queue::<[u64; 19]>::new(2);
    let start = usize::MAX - 1;
    // Seed an empty queue near counter wrap; reaching it through the public
    // API would require usize::MAX successful transfers.
    queue.storage.header().write.0.store(start, Ordering::Relaxed);
    queue.storage.header().read.0.store(start, Ordering::Relaxed);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    let first = std::array::from_fn(|i| (i as u64).wrapping_mul(31));
    let second = std::array::from_fn(|i| !(i as u64));
    let mut calls = 0;
    assert_eq!(producer.next_sequence(), start);
    assert_eq!(
        producer.produce_with(|| {
            calls += 1;
            assert!(!consumer.consume_ref(|_| panic!("factory has not returned a message")));
            first
        }),
        Ok(start)
    );
    assert_eq!(producer.next_sequence(), usize::MAX);
    let owned = String::from("FnOnce factory");
    assert_eq!(
        producer.produce_with(|| {
            drop(owned);
            second
        }),
        Ok(usize::MAX)
    );
    assert_eq!(producer.next_sequence(), 0, "sequence wraps after usize::MAX");
    assert_eq!(
        producer.produce_with(|| {
            calls += 1;
            first
        }),
        Err(super::FullError)
    );
    assert_eq!(producer.next_sequence(), 0, "Full must not advance the sequence");
    assert_eq!(calls, 1, "a full queue must not invoke the factory");
    assert!(consumer.consume_ref(|value| assert_eq!(*value, first)));

    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            producer.produce_with(|| panic!("factory panic is intentional")).unwrap();
        }))
        .is_err()
    );
    assert_eq!(producer.next_sequence(), 0, "panic must not advance the sequence");
    assert!(consumer.consume_ref(|value| assert_eq!(*value, second)));
    assert!(!consumer.consume_ref(|_| panic!("panicked factory must not publish")));

    assert_eq!(producer.produce_with(|| first), Ok(0), "panic must not advance the sequence");
    assert_eq!(producer.next_sequence(), 1);
    assert_eq!(producer.produce(&second), Ok(1));
    assert_eq!(producer.next_sequence(), 2);
    assert!(consumer.consume_ref(|value| assert_eq!(*value, first)));
    assert!(consumer.consume_ref(|value| assert_eq!(*value, second)));
    assert!(!consumer.consume_ref(|_| unreachable!()));
}

#[test]
fn borrowed_payloads_remain_usable_within_their_lifetime() {
    let value = String::from("borrowed payload");
    let queue = Queue::<_>::new(1);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    producer.produce(&value.as_str()).unwrap();
    assert!(consumer.consume(|received| assert_eq!(*received, value.as_str())));
}

#[test]
fn fifo_capacity_one_never_overwrites_unread_message() {
    let queue = Queue::<_>::new(1);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();

    assert_eq!(queue.capacity(), 1);
    assert_eq!(producer.produce(&11), Ok(0));
    assert_eq!(producer.max_writable_msgs_without_speeding_past(), 0);
    assert!(producer.produce(&22).is_err());

    let mut value = 0;
    assert_eq!(consumer.try_consume(&mut value), Ok(()));
    assert_eq!(value, 11);
}

#[test]
fn capacity_rounds_to_the_next_power_of_two() {
    for requested in 1..=33 {
        assert_eq!(Queue::<u8>::new(requested).capacity(), requested.next_power_of_two());
    }
}

#[test]
fn full_and_empty_leave_caller_values_unchanged() {
    let queue = Queue::<_>::new(2);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();

    producer.produce(&3).unwrap();
    producer.produce(&5).unwrap();
    assert!(producer.produce(&7).is_err());

    let mut value = 99;
    consumer.try_consume(&mut value).unwrap();
    assert_eq!(value, 3);
    consumer.try_consume(&mut value).unwrap();
    assert_eq!(value, 5);
    assert_eq!(consumer.try_consume(&mut value), Err(EmptyError::Empty));
    assert_eq!(value, 5);
}

#[test]
fn drain_and_refill_preserve_fifo_order_across_wraps() {
    let queue = Queue::<_>::new(3);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();

    for round in 0..128_u64 {
        for offset in 0..queue.capacity() {
            producer.produce(&(round * 10 + offset as u64)).unwrap();
        }
        assert_eq!(producer.max_writable_msgs_without_speeding_past(), 0);
        for offset in 0..queue.capacity() {
            let mut value = 0;
            consumer.try_consume(&mut value).unwrap();
            assert_eq!(value, round * 10 + offset as u64);
        }
        assert_eq!(consumer.queue_message_count(), 0);
    }
}

#[test]
fn cloned_handles_cannot_duplicate_roles_and_dropped_endpoints_handoff_state() {
    let queue = Queue::<_>::new(4);
    let clone = queue.clone();
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = clone.try_consumer().unwrap();

    assert!(matches!(clone.try_producer(), Err(QueueError::ProducerAttached)));
    assert!(matches!(queue.try_consumer(), Err(QueueError::ConsumerAttached)));

    assert_eq!(producer.next_sequence(), 0);
    assert_eq!(producer.produce(&10), Ok(0));
    let next = producer.next_sequence();
    drop(producer);
    let mut replacement_producer = clone.try_producer().unwrap();
    assert_eq!(replacement_producer.next_sequence(), next);
    assert_eq!(replacement_producer.produce(&20), Ok(next));

    let mut value = 0;
    consumer.try_consume(&mut value).unwrap();
    assert_eq!(value, 10);
    drop(consumer);
    let mut replacement_consumer = queue.try_consumer().unwrap();
    replacement_consumer.try_consume(&mut value).unwrap();
    assert_eq!(value, 20);
}

#[test]
fn nonzero_values_and_callbacks_are_copied_out() {
    let queue = Queue::<_>::new(1);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();

    producer.produce(&NonZeroU64::new(41).unwrap()).unwrap();
    let mut value = NonZeroU64::new(7).unwrap();
    consumer.try_consume(&mut value).unwrap();
    assert_eq!(value.get(), 41);

    producer.produce(&NonZeroU64::new(9).unwrap()).unwrap();
    assert!(consumer.consume(|message| *message = NonZeroU64::new(99).unwrap()));
    assert!(!consumer.consume(|_| unreachable!()));

    producer.produce(&NonZeroU64::new(12).unwrap()).unwrap();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            consumer.consume(|_| panic!("callback panic is intentional"));
        }))
        .is_err()
    );
    producer.produce(&NonZeroU64::new(13).unwrap()).unwrap();
    consumer.try_consume(&mut value).unwrap();
    assert_eq!(value.get(), 13);
}

#[test]
fn borrowed_slot_is_held_until_callback_returns_or_unwinds() {
    let queue = Queue::<_>::new(1);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    producer.produce(&41).unwrap();

    // Moving this capture out also exercises an FnOnce-only callback.
    let owned = String::from("callback state");
    assert!(consumer.consume_ref(|message| {
        drop(owned);
        assert_eq!(*message, 41);
        assert_eq!(producer.max_writable_msgs_without_speeding_past(), 0);
        assert!(producer.produce(&99).is_err());
        assert_eq!(*message, 41);
    }));
    assert_eq!(producer.max_writable_msgs_without_speeding_past(), 1);
    producer.produce(&42).unwrap();

    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            consumer.consume_ref(|message| {
                assert_eq!(*message, 42);
                assert!(producer.produce(&99).is_err());
                panic!("callback panic is intentional");
            });
        }))
        .is_err()
    );
    assert_eq!(producer.max_writable_msgs_without_speeding_past(), 1);
    drop(consumer);
    let mut consumer = queue.try_consumer().unwrap();
    assert_eq!(consumer.queue_message_count(), 0);
    assert!(!consumer.consume_ref(|_| panic!("must not replay after unwind")));
    producer.produce(&43).unwrap();
    assert!(consumer.consume_ref(|message| assert_eq!(*message, 43)));
}

#[test]
fn copying_callback_releases_capacity_before_running() {
    let queue = Queue::<_>::new(1);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    producer.produce(&1).unwrap();
    assert!(consumer.consume(|message| {
        producer.produce(&2).expect("copied consumption releases before callback");
        assert_eq!(*message, 1);
    }));
    assert!(consumer.consume_ref(|message| assert_eq!(*message, 2)));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PatternedMessage {
    sequence: u64,
    inverse: u64,
    words: [u64; 3],
}

impl PatternedMessage {
    fn new(sequence: u64) -> Self {
        Self {
            sequence,
            inverse: !sequence,
            words: [sequence.wrapping_mul(3), sequence.rotate_left(17), !sequence.rotate_right(9)],
        }
    }
}

#[test]
fn threaded_transfer_keeps_multiword_messages_intact() {
    let messages = if cfg!(miri) { 128 } else { 50_000 };
    let progress_timeout = Duration::from_secs(if cfg!(miri) { 20 } else { 5 });
    let queue = Queue::<_>::new(64);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    drop(queue);

    let producer_thread = thread::spawn(move || {
        for sequence in 0..messages {
            let deadline = Instant::now() + progress_timeout;
            let message = PatternedMessage::new(sequence as u64);
            loop {
                if producer.produce(&message).is_ok() {
                    break;
                }
                assert!(Instant::now() < deadline, "producer remained full past the deadline");
                thread::yield_now();
            }
        }
    });
    let consumer_thread = thread::spawn(move || {
        for sequence in 0..messages {
            let deadline = Instant::now() + progress_timeout;
            let mut message = PatternedMessage::new(u64::MAX);
            loop {
                if consumer.try_consume(&mut message).is_ok() {
                    assert_eq!(message, PatternedMessage::new(sequence as u64));
                    break;
                }
                assert!(Instant::now() < deadline, "consumer remained empty past the deadline");
                thread::yield_now();
            }
        }
    });

    producer_thread.join().unwrap();
    consumer_thread.join().unwrap();
}

#[test]
fn progress_queries_remain_bounded_during_transfer() {
    // While either endpoint queries, only its peer can change availability.
    // A positive observation must allow an immediate operation.
    for capacity in [1, 2, 8, 64] {
        let queue = Queue::<_>::new(capacity);
        let mut producer = queue.try_producer().unwrap();
        let mut consumer = queue.try_consumer().unwrap();
        // Exercise repeated slot reuse without requiring a particular transfer rate.
        let messages = capacity * 8;
        thread::scope(|scope| {
            scope.spawn(move || {
                for sequence in 0..messages {
                    let deadline = Instant::now() + Duration::from_secs(10);
                    loop {
                        let available = producer.max_writable_msgs_without_speeding_past();
                        assert!(available <= capacity);
                        if producer.produce(&sequence).is_ok() {
                            break;
                        }
                        assert_eq!(available, 0, "reported space must remain writable");
                        assert!(
                            Instant::now() < deadline,
                            "producer remained full: capacity={capacity}, sequence={sequence}"
                        );
                        thread::yield_now();
                    }
                }
            });
            scope.spawn(move || {
                for expected in 0..messages {
                    let deadline = Instant::now() + Duration::from_secs(10);
                    let mut value = usize::MAX;
                    loop {
                        let unread = consumer.queue_message_count();
                        assert!(unread <= capacity);
                        let received = if expected % 2 == 0 {
                            consumer.consume_ref(|message| value = *message)
                        } else {
                            consumer.try_consume(&mut value).is_ok()
                        };
                        if received {
                            assert_eq!(value, expected);
                            break;
                        }
                        assert_eq!(unread, 0, "reported values must remain readable");
                        assert!(
                            Instant::now() < deadline,
                            "consumer remained empty: capacity={capacity}, sequence={expected}"
                        );
                        thread::yield_now();
                    }
                }
            });
        });
    }
}

#[cfg(not(miri))]
fn shared_path() -> (TempDir, PathBuf) {
    let directory = Builder::new().prefix("flux-spsc-").tempdir().unwrap();
    let path = directory.path().join("queue.flink");
    (directory, path)
}

#[cfg(not(miri))]
#[test]
fn shared_mapping_outlives_queue_handles_and_enforces_roles_across_opens() {
    let (_directory, path) = shared_path();
    let queue = unsafe { Queue::<u64>::create_or_open_shared(&path, 3) }.unwrap();
    let mut producer = queue.try_producer().unwrap();
    producer.produce(&31).unwrap();
    drop(producer);
    drop(queue);

    let reopened = unsafe { Queue::<u64>::open_shared(&path) }.unwrap();
    let second_open = unsafe { Queue::<u64>::open_shared(&path) }.unwrap();
    assert!(matches!(
        unsafe { Queue::<u64>::create_or_open_shared(&path, 8) },
        Err(QueueError::IncompatibleLayout)
    ));
    assert!(matches!(
        unsafe { Queue::<u32>::open_shared(&path) },
        Err(QueueError::IncompatibleLayout)
    ));
    let mut producer = reopened.try_producer().unwrap();
    let mut consumer = second_open.try_consumer().unwrap();
    assert!(matches!(second_open.try_producer(), Err(QueueError::ProducerAttached)));
    assert!(matches!(reopened.try_consumer(), Err(QueueError::ConsumerAttached)));
    drop(second_open);
    drop(reopened);

    // Endpoints retain their mappings after every queue handle is dropped.
    producer.produce(&47).unwrap();
    let mut value = 0;
    consumer.try_consume(&mut value).unwrap();
    assert_eq!(value, 31);
    consumer.try_consume(&mut value).unwrap();
    assert_eq!(value, 47);
    drop(consumer);
    drop(producer);
    cleanup_flink(&path).unwrap();
}

#[cfg(not(miri))]
fn shared_child(mode: &str, path: &Path) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg("--exact")
        .arg("queue::spsc::tests::shared_process_child")
        .arg("--ignored")
        .env("FLUX_SPSC_CHILD_MODE", mode)
        .env("FLUX_SPSC_CHILD_PATH", path);
    command
}

#[cfg(not(miri))]
fn run_shared_child(mode: &str, path: &Path) {
    let status = shared_child(mode, path).status().unwrap();
    assert!(status.success(), "child mode {mode} failed with {status}");
}

#[cfg(not(miri))]
#[test]
fn shared_mapping_transfers_across_processes_and_crashed_claims_persist() {
    let (_directory, path) = shared_path();
    let queue = unsafe { Queue::<u64>::create_or_open_shared(&path, 8) }.unwrap();
    let producer = queue.try_producer().unwrap();
    let consumer = queue.try_consumer().unwrap();

    run_shared_child("duplicate_claims", &path);
    drop(producer);
    drop(consumer);

    run_shared_child("produce", &path);
    let mut consumer = queue.try_consumer().unwrap();
    for expected in 100..104 {
        let mut value = 0;
        consumer.try_consume(&mut value).unwrap();
        assert_eq!(value, expected);
    }
    drop(consumer);

    run_shared_child("crash_producer", &path);
    assert!(matches!(queue.try_producer(), Err(QueueError::ProducerAttached)));
    drop(queue);
    cleanup_flink(&path).unwrap();
}

#[cfg(not(miri))]
#[test]
fn concurrent_process_transfer_keeps_multiword_messages_intact() {
    let (_directory, path) = shared_path();
    let queue = unsafe { Queue::<PatternedMessage>::create_or_open_shared(&path, 4) }.unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    let mut child = shared_child("stream", &path).spawn().unwrap();
    for sequence in 0..10_000 {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !consumer.consume_ref(|message| {
            assert_eq!(*message, PatternedMessage::new(sequence));
        }) {
            assert!(Instant::now() < deadline, "child producer stopped making progress");
            thread::yield_now();
        }
    }
    assert!(child.wait().unwrap().success());
    drop(consumer);
    drop(queue);
    cleanup_flink(&path).unwrap();
}

#[cfg(not(miri))]
#[test]
#[ignore = "invoked by the parent shared-memory tests in a separate process"]
fn shared_process_child() {
    let path = PathBuf::from(std::env::var("FLUX_SPSC_CHILD_PATH").unwrap());
    if std::env::var("FLUX_SPSC_CHILD_MODE").as_deref() == Ok("stream") {
        let queue = unsafe { Queue::<PatternedMessage>::open_shared(path) }.unwrap();
        let mut producer = queue.try_producer().unwrap();
        for sequence in 0..10_000 {
            let deadline = Instant::now() + Duration::from_secs(10);
            while producer.produce(&PatternedMessage::new(sequence)).is_err() {
                assert!(Instant::now() < deadline, "parent consumer stopped making progress");
                thread::yield_now();
            }
        }
        return;
    }
    let queue = unsafe { Queue::<u64>::open_shared(path) }.unwrap();
    match std::env::var("FLUX_SPSC_CHILD_MODE").as_deref() {
        Ok("duplicate_claims") => {
            assert!(matches!(queue.try_producer(), Err(QueueError::ProducerAttached)));
            assert!(matches!(queue.try_consumer(), Err(QueueError::ConsumerAttached)));
        }
        Ok("produce") => {
            let mut producer = queue.try_producer().unwrap();
            for value in 100..104 {
                producer.produce(&value).unwrap();
            }
        }
        Ok("crash_producer") => {
            let _producer = queue.try_producer().unwrap();
            std::process::exit(0);
        }
        mode => panic!("unknown child mode: {mode:?}"),
    }
}
