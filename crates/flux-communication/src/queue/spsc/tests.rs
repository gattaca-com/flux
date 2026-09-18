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
fn borrowed_payloads_remain_usable_within_their_lifetime() {
    let value = String::from("borrowed payload");
    let queue = Queue::new(1);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    producer.produce(&value.as_str()).unwrap();
    assert!(consumer.consume(|received| assert_eq!(*received, value.as_str())));
}

#[test]
fn fifo_capacity_one_never_overwrites_unread_message() {
    let queue = Queue::new(1);
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
    let queue = Queue::new(2);
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
    let queue = Queue::new(3);
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
    let queue = Queue::new(4);
    let clone = queue.clone();
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = clone.try_consumer().unwrap();

    assert!(matches!(clone.try_producer(), Err(QueueError::ProducerAttached)));
    assert!(matches!(queue.try_consumer(), Err(QueueError::ConsumerAttached)));

    producer.produce(&10).unwrap();
    drop(producer);
    let mut replacement_producer = clone.try_producer().unwrap();
    replacement_producer.produce(&20).unwrap();

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
    let queue = Queue::new(1);
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
    let deadline = Instant::now() + Duration::from_secs(if cfg!(miri) { 20 } else { 5 });
    let queue = Queue::new(64);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    drop(queue);

    let producer_thread = thread::spawn(move || {
        for sequence in 0..messages {
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
    let producer = reopened.try_producer().unwrap();
    let mut consumer = second_open.try_consumer().unwrap();
    assert!(matches!(second_open.try_producer(), Err(QueueError::ProducerAttached)));
    assert!(matches!(reopened.try_consumer(), Err(QueueError::ConsumerAttached)));

    let mut value = 0;
    consumer.try_consume(&mut value).unwrap();
    assert_eq!(value, 31);
    drop(consumer);
    drop(producer);
    drop(second_open);
    drop(reopened);
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
    let deadline = Instant::now() + Duration::from_secs(10);
    for sequence in 0..10_000 {
        let mut message = PatternedMessage::new(u64::MAX);
        while consumer.try_consume(&mut message).is_err() {
            assert!(Instant::now() < deadline, "child producer stopped making progress");
            thread::yield_now();
        }
        assert_eq!(message, PatternedMessage::new(sequence));
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
        let deadline = Instant::now() + Duration::from_secs(10);
        for sequence in 0..10_000 {
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
