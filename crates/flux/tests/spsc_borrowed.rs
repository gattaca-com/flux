use std::panic::{AssertUnwindSafe, catch_unwind};

use flux::{
    communication::{
        ShmemData, cleanup_shmem,
        queue::{Queue, spsc::QueueError},
        timer::TimingMessage,
    },
    spine::{FluxSpine, SpineAdapter, SpineProducers, SpineSpscQueue},
    tile::{Tile, TileInfo},
};
use flux_timing::IngestionTime;
use flux_utils::{directories::shmem_dir_queues_with_base, short_typename};
use spine_derive::from_spine;

/// Large enough to exercise a real shared-memory payload path without carrying
/// process-local state that would make a Spine mapping invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
struct Payload {
    sequence: u64,
    words: [u64; 63],
}

impl Payload {
    fn new(sequence: u64) -> Self {
        Self { sequence, words: [sequence; 63] }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
struct Reply(u64);

#[from_spine("spsc-borrowed")]
#[derive(Debug)]
struct BorrowedSpine {
    tile_info: ShmemData<TileInfo>,
    #[queue(size(4), flavour("spsc"))]
    messages: SpineQueue<Payload>,
    #[queue(size(2), flavour("spsc"))]
    replies: SpineQueue<Reply>,
}

#[derive(Clone, Copy, Default)]
struct Sender;

impl<S: FluxSpine> Tile<S> for Sender {
    fn loop_body(&mut self, _: &mut SpineAdapter<S>) {}
}

#[derive(Clone, Copy, Default)]
struct Receiver;

impl<S: FluxSpine> Tile<S> for Receiver {
    fn loop_body(&mut self, _: &mut SpineAdapter<S>) {}
}

fn new_spine(base_dir: &std::path::Path) -> BorrowedSpine {
    // SAFETY: this test uses one architecture and schema, passes repr(C) Copy
    // values without pointers, and does not inherit endpoints through fork.
    unsafe { BorrowedSpine::new_with_base_dir(base_dir, None) }
}

fn payload_timing_queues(base_dir: &std::path::Path) -> [Queue<TimingMessage>; 2] {
    let directory = shmem_dir_queues_with_base(base_dir, BorrowedSpine::app_name());
    let name = format!(
        "{}-{}",
        <Receiver as Tile<BorrowedSpine>>::name(&Receiver),
        short_typename::<Payload>()
    );
    ["timing", "latency"].map(|kind| Queue::open_shared(directory.join(format!("{kind}-{name}"))))
}

#[test]
fn spine_borrowed_consumer_holds_capacity_and_does_not_replay_after_panic() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    spine.messages = SpineSpscQueue::new(1);
    let mut producer = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut consumer = SpineAdapter::connect_tile(&Receiver, &mut spine);

    producer.try_produce(Payload::new(11)).unwrap();
    assert!(
        consumer
            .consumers
            .messages
            .consume_ref_maybe_track(&mut consumer.producers, |message, _| {
                assert_eq!(*message, Payload::new(11));
                assert!(
                    producer.try_produce(Payload::new(22)).is_err(),
                    "slot stays full in callback"
                );
                false
            })
            .unwrap()
    );

    producer.try_produce(Payload::new(22)).expect("callback return releases capacity");
    let result = catch_unwind(AssertUnwindSafe(|| {
        consumer
            .consumers
            .messages
            .consume_ref(&mut consumer.producers, |message, _| {
                assert_eq!(*message, Payload::new(22));
                assert!(producer.try_produce(Payload::new(33)).is_err(), "slot remains borrowed");
                panic!("intentional callback panic");
            })
            .unwrap();
    }));
    assert!(result.is_err());

    producer.try_produce(Payload::new(33)).expect("panic releases capacity");
    assert!(
        consumer
            .consume_ref_one(|message: &Payload, _| assert_eq!(*message, Payload::new(33)))
            .unwrap()
    );
    assert!(!consumer.consume_ref_one(|_: &Payload, _| panic!("no replay after panic")).unwrap());

    drop(producer);
    drop(consumer);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn borrowed_and_copying_consumption_preserve_fifo_across_single_and_drain_calls() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    let mut producer = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut consumer = SpineAdapter::connect_tile(&Receiver, &mut spine);
    let queues = payload_timing_queues(tmp.path());
    let before = queues.map(|queue| queue.count());
    for sequence in [1, 2, 3] {
        producer.try_produce(Payload::new(sequence)).unwrap();
    }

    consumer.begin_loop(IngestionTime::now());
    let mut received = Vec::new();
    assert!(
        consumer.try_consume_one(|message: Payload, _| received.push(message.sequence)).unwrap()
    );
    assert!(
        consumer.consume_ref_one(|message: &Payload, _| received.push(message.sequence)).unwrap()
    );
    consumer.consume_ref(|message: &Payload, _| received.push(message.sequence)).unwrap();

    assert_eq!(received, [1, 2, 3]);
    for (queue, before) in queues.into_iter().zip(before) {
        assert_eq!(
            queue.count() - before,
            3,
            "copying and always-measure borrowed calls emit timing records"
        );
    }
    assert!(!consumer.consume_ref_one(|_: &Payload, _| panic!("queue was drained")).unwrap());

    drop(producer);
    drop(consumer);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn borrowed_selective_tracking_propagates_ingestion_and_marks_work() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    let mut producer = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut consumer = SpineAdapter::connect_tile(&Receiver, &mut spine);
    let queues = payload_timing_queues(tmp.path());
    let before = queues.map(|queue| queue.count());
    let ingestion = IngestionTime::now();

    for sequence in [41, 42] {
        producer.producers.try_produce_with_ingestion(Payload::new(sequence), ingestion).unwrap();
    }
    consumer.begin_loop(IngestionTime::now());
    let mut received = Vec::new();
    consumer
        .consume_ref_maybe_track(|message: &Payload, producers| {
            assert_eq!(producers.timestamp().ingestion_t, ingestion);
            received.push(message.sequence);
            message.sequence == 42
        })
        .unwrap();

    assert_eq!(received, [41, 42]);
    assert!(consumer.did_work(), "every borrowed consumption counts as work");
    for (queue, before) in queues.into_iter().zip(before) {
        assert_eq!(queue.count() - before, 1, "only selected payloads emit timing records");
    }

    producer.producers.try_produce_with_ingestion(Payload::new(43), ingestion).unwrap();
    consumer.begin_loop(IngestionTime::now());
    consumer
        .consume_ref_maybe_track(|message: &Payload, producers| {
            assert_eq!(message.sequence, 43);
            assert_eq!(producers.timestamp().ingestion_t, ingestion);
            false
        })
        .unwrap();
    assert!(consumer.did_work(), "a wholly untracked drain still counts as work");
    for (queue, before) in queues.into_iter().zip(before) {
        assert_eq!(queue.count() - before, 1, "untracked drain must not emit records");
    }

    consumer.begin_loop(IngestionTime::now());
    consumer.consume_ref_maybe_track(|_: &Payload, _| panic!("empty queue")).unwrap();
    assert!(!consumer.did_work(), "an empty borrowed drain does not count as work");

    drop(producer);
    drop(consumer);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn borrowed_callback_can_publish_to_a_different_queue() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    let mut sender = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut receiver = SpineAdapter::connect_tile(&Receiver, &mut spine);
    let ingestion = IngestionTime::now();
    sender.producers.try_produce_with_ingestion(Payload::new(71), ingestion).unwrap();

    receiver.begin_loop(IngestionTime::now());
    assert!(
        receiver
            .consume_ref_one_maybe_track(|message: &Payload, producers| {
                assert_eq!(producers.timestamp().ingestion_t, ingestion);
                producers.try_produce(Reply(message.sequence)).unwrap();
                false
            })
            .unwrap()
    );
    assert!(receiver.did_work(), "untracked borrowed consumption still counts as work");

    let mut reply = None;
    assert!(sender.consume_ref_one(|message: &Reply, _| reply = Some(*message)).unwrap());
    assert_eq!(reply, Some(Reply(71)));

    drop(sender);
    drop(receiver);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn borrowed_adapter_empty_and_attachment_failures_skip_callbacks_and_work() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    let mut empty = SpineAdapter::connect_tile(&Receiver, &mut spine);
    empty.begin_loop(IngestionTime::now());
    assert!(!empty.consume_ref_one(|_: &Payload, _| panic!("empty callback")).unwrap());
    assert!(!empty.did_work());
    drop(empty);

    let mut owner = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut other = SpineAdapter::connect_tile(&Receiver, &mut spine);
    owner.consumers.messages.try_attach().unwrap();
    other.begin_loop(IngestionTime::now());
    assert!(matches!(
        other.consume_ref_maybe_track(|_: &Payload, _| panic!("attachment failure callback")),
        Err(QueueError::ConsumerAttached)
    ));
    assert!(!other.did_work());

    drop(owner);
    drop(other);
    drop(spine);
    cleanup_shmem(tmp.path());
}
