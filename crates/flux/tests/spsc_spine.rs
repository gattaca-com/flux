#[cfg(feature = "park")]
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

#[cfg(feature = "park")]
use flux::tile::{TileConfig, tile_runner};
use flux::{
    communication::{
        ShmemData, cleanup_shmem,
        queue::{Queue, spsc::QueueError},
        timer::TimingMessage,
    },
    spine::{
        FluxSpine, QueueParams, SpineAdapter, SpineProducers, SpineQueue, SpineSpscQueue,
        SpscProduceError,
    },
    tile::{Tile, TileInfo},
};
use flux_timing::{IngestionTime, InternalMessage, TrackingTimestamp};
use flux_utils::{
    directories::{shmem_dir_queues_with_base, shmem_dir_with_base},
    short_typename,
};
use spine_derive::from_spine;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
struct LegacyMessage(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
struct SpscMessage(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
struct ReplyMessage(u64);

#[from_spine("spsc-spine-mixed")]
#[derive(Debug)]
struct MixedSpine {
    tile_info: ShmemData<TileInfo>,
    #[queue(size(8))]
    legacy: SpineQueue<LegacyMessage>,
    #[queue(size(2), flavour("spsc"))]
    spsc: flux::spine::SpineQueue<SpscMessage>,
}

#[from_spine("spsc-spine-bidirectional")]
#[derive(Debug)]
struct BidirectionalSpine {
    tile_info: ShmemData<TileInfo>,
    #[queue(size(2), flavour("spsc"))]
    requests: SpineQueue<SpscMessage>,
    #[queue(size(2), flavour("spsc"))]
    replies: SpineQueue<ReplyMessage>,
}

#[from_spine("spsc-spine-legacy")]
#[derive(Debug)]
struct LegacyOnlySpine {
    tile_info: ShmemData<TileInfo>,
    #[queue(size(2))]
    legacy: SpineQueue<LegacyMessage>,
}

#[derive(Clone, Copy, Default)]
struct TestTile;

impl<S: FluxSpine> Tile<S> for TestTile {
    fn loop_body(&mut self, _: &mut SpineAdapter<S>) {}
}

#[derive(Clone, Copy, Default)]
struct LeftTile;

impl<S: FluxSpine> Tile<S> for LeftTile {
    fn loop_body(&mut self, _: &mut SpineAdapter<S>) {}
}

#[derive(Clone, Copy, Default)]
struct RightTile;

impl<S: FluxSpine> Tile<S> for RightTile {
    fn loop_body(&mut self, _: &mut SpineAdapter<S>) {}
}

fn new_mixed_spine(base_dir: &std::path::Path) -> MixedSpine {
    // SAFETY: this test uses one architecture and one schema, passes only repr(C)
    // copy values, and does not inherit endpoints through fork.
    unsafe { MixedSpine::new_with_base_dir(base_dir, None) }
}

fn new_bidirectional_spine(base_dir: &std::path::Path) -> BidirectionalSpine {
    // SAFETY: this test uses one architecture and one schema, passes only repr(C)
    // copy values, and does not inherit endpoints through fork.
    unsafe { BidirectionalSpine::new_with_base_dir(base_dir, None) }
}

fn spsc_timing_queues(base_dir: &std::path::Path) -> [Queue<TimingMessage>; 2] {
    let dir = shmem_dir_queues_with_base(base_dir, MixedSpine::app_name());
    let name = format!(
        "{}-{}",
        <RightTile as Tile<MixedSpine>>::name(&RightTile),
        short_typename::<SpscMessage>()
    );
    ["timing", "latency"].map(|kind| Queue::open_shared(dir.join(format!("{kind}-{name}"))))
}

#[test]
fn spsc_optional_tracking_drains_untracked_values_and_records_selected_messages() {
    for internal in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let mut spine = new_mixed_spine(tmp.path());
        let mut producer = SpineAdapter::connect_tile(&LeftTile, &mut spine);
        let mut consumer = SpineAdapter::connect_tile(&RightTile, &mut spine);
        let queues = spsc_timing_queues(tmp.path());
        let before = queues.map(|queue| queue.count());
        let mut received = Vec::new();

        for batch in 0..2 {
            let ingestion = IngestionTime::now();
            let timestamp = TrackingTimestamp::new(37).with_ingestion_t(ingestion);
            for offset in 0..2 {
                producer
                    .producers
                    .try_forward(&InternalMessage::new(timestamp, SpscMessage(batch * 2 + offset)))
                    .unwrap();
            }
            consumer.begin_loop(IngestionTime::now());
            if internal {
                consumer
                    .try_consume_internal_message_maybe_track(
                        |message: &mut InternalMessage<SpscMessage>, p| {
                            assert_eq!(message.tracking_timestamp(), timestamp);
                            assert_eq!(p.timestamp().ingestion_t, ingestion);
                            received.push(message.data().0);
                            message.data().0 % 2 == 1
                        },
                    )
                    .unwrap();
            } else {
                consumer
                    .try_consume_maybe_track(|message: SpscMessage, p| {
                        assert_eq!(p.timestamp().ingestion_t, ingestion);
                        received.push(message.0);
                        message.0 % 2 == 1
                    })
                    .unwrap();
            }
            assert!(consumer.did_work(), "consumption counts as work regardless of tracking");
        }
        assert_eq!(received, [0, 1, 2, 3], "false selects telemetry, not whether draining stops");
        for (queue, before) in queues.into_iter().zip(before) {
            assert_eq!(queue.count() - before, 2, "only selected messages emit each timing record");
        }

        consumer.begin_loop(IngestionTime::now());
        consumer.try_consume_maybe_track(|_: SpscMessage, _| panic!("empty queue")).unwrap();
        consumer
            .try_consume_internal_message_maybe_track(|_: &mut InternalMessage<SpscMessage>, _| {
                panic!("empty queue")
            })
            .unwrap();
        assert!(!consumer.did_work(), "an empty drain does not count as work");

        drop(producer);
        drop(consumer);
        drop(spine);
        cleanup_shmem(tmp.path());
    }
}

#[test]
fn spsc_untracked_single_consumption_releases_capacity_before_callback() {
    for internal in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let mut spine = new_mixed_spine(tmp.path());
        let mut producer = SpineAdapter::connect_tile(&LeftTile, &mut spine);
        let mut consumer = SpineAdapter::connect_tile(&RightTile, &mut spine);
        let queues = spsc_timing_queues(tmp.path());
        let before = queues.map(|queue| queue.count());
        let ingestion = IngestionTime::now();
        for value in [11, 22] {
            producer.producers.try_produce_with_ingestion(SpscMessage(value), ingestion).unwrap();
        }
        consumer.begin_loop(IngestionTime::now());
        let mut handle = |message: SpscMessage, p: &mut MixedSpineProducers| {
            assert_eq!(message, SpscMessage(11));
            assert_eq!(p.timestamp().ingestion_t, ingestion);
            producer.try_produce(SpscMessage(33)).expect("slot released before callback");
            false
        };
        let delivered = if internal {
            consumer.try_consume_internal_message_one_maybe_track(
                |message: &mut InternalMessage<SpscMessage>, p| handle(message.into_data(), p),
            )
        } else {
            consumer.try_consume_one_maybe_track(handle)
        };
        assert!(delivered.unwrap(), "untracked consumption still returns true");
        assert!(consumer.did_work(), "untracked consumption counts as work");
        assert_eq!(queues.map(|queue| queue.count()), before, "no timing records for false");

        let mut remaining = Vec::new();
        consumer.try_consume(|message: SpscMessage, _| remaining.push(message.0)).unwrap();
        assert_eq!(remaining, [22, 33], "one-message consumption preserves the remaining FIFO");
        for (queue, before) in queues.into_iter().zip(before) {
            assert_eq!(
                queue.count() - before,
                2,
                "ordinary consumption still records every message"
            );
        }
        consumer.begin_loop(IngestionTime::now());
        assert!(
            !consumer.try_consume_one_maybe_track(|_: SpscMessage, _| panic!("empty")).unwrap()
        );
        assert!(
            !consumer
                .try_consume_internal_message_one_maybe_track(
                    |_: &mut InternalMessage<SpscMessage>, _| panic!("empty"),
                )
                .unwrap()
        );
        assert!(!consumer.did_work());

        drop(producer);
        drop(consumer);
        drop(spine);
        cleanup_shmem(tmp.path());
    }
}

#[test]
fn spsc_optional_tracking_preserves_consumer_attachment_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_mixed_spine(tmp.path());
    let mut owner = SpineAdapter::connect_tile(&LeftTile, &mut spine);
    let mut other = SpineAdapter::connect_tile(&RightTile, &mut spine);
    owner.consumers.spsc.try_attach().unwrap();
    assert!(matches!(
        other.try_consume_maybe_track(|_: SpscMessage, _| panic!("role already claimed")),
        Err(QueueError::ConsumerAttached)
    ));
    assert!(matches!(
        other.try_consume_internal_message_maybe_track(
            |_: &mut InternalMessage<SpscMessage>, _| panic!("role already claimed"),
        ),
        Err(QueueError::ConsumerAttached)
    ));
    assert!(!other.did_work());
    drop(owner);
    drop(other);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn spsc_drain_panic_preserves_completed_work_and_releases_consumed_slots() {
    for mode in 0..3 {
        for completed in 0..2 {
            for already_worked in [false, true] {
                let tmp = tempfile::tempdir().unwrap();
                let mut spine = new_mixed_spine(tmp.path());
                let mut producer = SpineAdapter::connect_tile(&LeftTile, &mut spine);
                let mut consumer = SpineAdapter::connect_tile(&RightTile, &mut spine);
                let queues = spsc_timing_queues(tmp.path());
                let before = queues.map(|queue| queue.count());
                let values = [11, 22];
                for value in values {
                    producer.try_produce(SpscMessage(value)).unwrap();
                }
                consumer.begin_loop(IngestionTime::now());
                if already_worked {
                    consumer.mark_work();
                }
                let mut received = Vec::new();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut handle = |message: SpscMessage, _: &mut MixedSpineProducers| {
                        received.push(message.0);
                        if received.len() > completed {
                            producer.try_produce(SpscMessage(33)).expect("slot already released");
                            panic!("intentional callback panic");
                        }
                        false
                    };
                    match mode {
                        0 => consumer.try_consume(|message, producers| {
                            handle(message, producers);
                        }),
                        1 => consumer.try_consume_maybe_track(handle),
                        _ => consumer.try_consume_internal_message_maybe_track(
                            |message: &mut InternalMessage<SpscMessage>, producers| {
                                handle(message.into_data(), producers)
                            },
                        ),
                    }
                    .unwrap();
                }));
                assert!(result.is_err());
                assert_eq!(received, values[..=completed]);
                assert_eq!(consumer.did_work(), already_worked || completed > 0);
                for (queue, before) in queues.into_iter().zip(before) {
                    let tracked = if mode == 0 { completed } else { 0 };
                    assert_eq!(queue.count() - before, tracked, "only completed callbacks record");
                }
                let mut remaining = Vec::new();
                consumer
                    .try_consume_maybe_track(|message: SpscMessage, _| {
                        remaining.push(message.0);
                        false
                    })
                    .unwrap();
                let expected: Vec<_> =
                    values[completed + 1..].iter().copied().chain([33]).collect();
                assert_eq!(remaining, expected, "panicking callbacks do not redeliver messages");
                drop(producer);
                drop(consumer);
                drop(spine);
                cleanup_shmem(tmp.path());
            }
        }
    }
}

#[test]
fn spsc_drain_includes_messages_published_by_callbacks() {
    for mode in 0..3 {
        let tmp = tempfile::tempdir().unwrap();
        let mut spine = new_mixed_spine(tmp.path());
        let mut producer = SpineAdapter::connect_tile(&LeftTile, &mut spine);
        let mut consumer = SpineAdapter::connect_tile(&RightTile, &mut spine);
        for value in [11, 22] {
            producer.try_produce(SpscMessage(value)).unwrap();
        }
        let mut received = Vec::new();
        let mut handle = |message: SpscMessage, _: &mut MixedSpineProducers| {
            received.push(message.0);
            if received.len() == 1 {
                producer.try_produce(SpscMessage(33)).expect("slot already released");
            }
            false
        };
        match mode {
            0 => consumer.try_consume(|message, producers| {
                handle(message, producers);
            }),
            1 => consumer.try_consume_maybe_track(handle),
            _ => consumer.try_consume_internal_message_maybe_track(
                |message: &mut InternalMessage<SpscMessage>, producers| {
                    handle(message.into_data(), producers)
                },
            ),
        }
        .unwrap();
        assert_eq!(received, [11, 22, 33], "drain continues until it observes an empty queue");
        assert!(consumer.did_work());
        drop(producer);
        drop(consumer);
        drop(spine);
        cleanup_shmem(tmp.path());
    }
}

#[test]
fn mixed_spine_configures_spsc_capacity_and_shared_memory_path() {
    let tmp = tempfile::tempdir().expect("create temp directory");
    let config =
        MixedSpineConfig { legacy: QueueParams { size: 8 }, spsc: QueueParams { size: 3 } };
    // SAFETY: this test uses one architecture and one schema, passes only repr(C)
    // copy values, and does not inherit endpoints through fork.
    let spine =
        unsafe { MixedSpine::new_with_base_dir_and_config(tmp.path(), Some("-custom"), config) };

    assert_eq!(spine.spsc.capacity(), 4, "SPSC capacity rounds the configured size");
    assert!(
        shmem_dir_with_base(tmp.path(), "spsc-spine-mixed-custom")
            .join("spsc")
            .join("spsc")
            .exists(),
        "the SPSC queue uses its dedicated shared-memory path"
    );

    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn heap_spsc_queue_connects_generated_tile_bundles() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_mixed_spine(tmp.path());
    spine.spsc = SpineSpscQueue::new(2);
    let mut producer = SpineAdapter::connect_tile(&LeftTile, &mut spine);
    let mut consumer = SpineAdapter::connect_tile(&RightTile, &mut spine);
    producer.try_produce(SpscMessage(19)).unwrap();
    assert!(
        consumer
            .try_consume_one(|message: SpscMessage, _| {
                assert_eq!(message, SpscMessage(19));
            })
            .unwrap()
    );
    drop(producer);
    drop(consumer);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn distinct_tiles_claim_opposite_spsc_roles_after_attaching_all_bundles() {
    let tmp = tempfile::tempdir().expect("create temp directory");
    let mut spine = new_bidirectional_spine(tmp.path());
    let mut left = SpineAdapter::connect_tile(&LeftTile, &mut spine);
    let mut right = SpineAdapter::connect_tile(&RightTile, &mut spine);

    left.producers.requests.try_attach().expect("left claims request producer");
    right.consumers.requests.try_attach().expect("right claims request consumer");
    right.producers.replies.try_attach().expect("right claims reply producer");
    left.consumers.replies.try_attach().expect("left claims reply consumer");

    left.try_produce(SpscMessage(11)).expect("left sends request");
    let mut request = None;
    assert!(
        right
            .try_consume_one(|message: SpscMessage, _| request = Some(message))
            .expect("right reads request")
    );
    assert_eq!(request, Some(SpscMessage(11)));

    right.try_produce(ReplyMessage(29)).expect("right sends reply");
    let mut reply = None;
    assert!(
        left.try_consume_one(|message: ReplyMessage, _| reply = Some(message))
            .expect("left reads reply")
    );
    assert_eq!(reply, Some(ReplyMessage(29)));

    drop(left);
    drop(right);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn spsc_role_errors_do_not_duplicate_roles_and_drop_releases_them() {
    let tmp = tempfile::tempdir().expect("create temp directory");
    let mut spine = new_mixed_spine(tmp.path());
    let mut first = SpineAdapter::connect_tile(&TestTile, &mut spine);
    let mut second = SpineAdapter::connect_tile(&TestTile, &mut spine);

    assert!(!first.try_consume_one(|_: SpscMessage, _| {}).expect("first empty poll"));
    first.try_produce(SpscMessage(7)).expect("first claims producer role");

    assert!(matches!(
        second.try_produce(SpscMessage(8)),
        Err(SpscProduceError::Attach(QueueError::ProducerAttached))
    ));
    assert!(matches!(
        second.try_consume_one(|_: SpscMessage, _| {}),
        Err(QueueError::ConsumerAttached)
    ));

    drop(first);

    let mut received = Vec::new();
    assert!(
        second
            .try_consume_one(|message: SpscMessage, _| received.push(message.0))
            .expect("replacement claims released consumer role")
    );
    second.try_produce(SpscMessage(9)).expect("replacement claims released producer role");
    assert!(
        second
            .try_consume_one(|message: SpscMessage, _| received.push(message.0))
            .expect("replacement reads its retried producer role")
    );
    assert_eq!(received, [7, 9]);

    drop(second);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn full_spsc_queue_preserves_unread_fifo_and_only_successful_writes_work() {
    let tmp = tempfile::tempdir().expect("create temp directory");
    let mut spine = new_mixed_spine(tmp.path());
    let mut producer = SpineAdapter::connect_tile(&TestTile, &mut spine);
    let mut consumer = SpineAdapter::connect_tile(&TestTile, &mut spine);

    producer.begin_loop(IngestionTime::now());
    producer.try_produce(SpscMessage(1)).expect("first write");
    producer.try_produce(SpscMessage(2)).expect("second write");
    assert!(producer.did_work(), "successful writes count as work");

    producer.begin_loop(IngestionTime::now());
    assert!(matches!(producer.try_produce(SpscMessage(3)), Err(SpscProduceError::Full)));
    assert!(!producer.did_work(), "a full queue does not count as work");

    let mut received = Vec::new();
    assert!(
        consumer
            .try_consume_one(|message: SpscMessage, _| received.push(message.0))
            .expect("consume first queued value")
    );

    producer.begin_loop(IngestionTime::now());
    producer.try_produce(SpscMessage(3)).expect("retry after capacity becomes available");

    while consumer
        .try_consume_one(|message: SpscMessage, _| received.push(message.0))
        .expect("consume queued value")
    {}
    assert_eq!(received, [1, 2, 3], "full writes leave unread messages in FIFO order");

    drop(producer);
    drop(consumer);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn spsc_preserves_explicit_ingestion_and_forwarded_tracking_timestamp() {
    let tmp = tempfile::tempdir().expect("create temp directory");
    let mut spine = new_mixed_spine(tmp.path());
    let mut producer = SpineAdapter::connect_tile(&TestTile, &mut spine);
    let mut consumer = SpineAdapter::connect_tile(&TestTile, &mut spine);
    let ingestion = IngestionTime::now();
    let forwarded = InternalMessage::new(TrackingTimestamp::new(123), SpscMessage(55));

    producer
        .producers
        .try_produce_with_ingestion(SpscMessage(41), ingestion)
        .expect("produce explicit ingestion timestamp");
    producer.producers.try_forward(&forwarded).expect("forward tracking metadata unchanged");

    let mut explicit_ingestion = None;
    assert!(
        consumer
            .try_consume_internal_message_one(|message: &mut InternalMessage<SpscMessage>, _| {
                explicit_ingestion = Some(message.ingestion_time());
            })
            .expect("consume explicitly timestamped message")
    );
    assert_eq!(explicit_ingestion, Some(ingestion));

    let mut received_forward = None;
    assert!(
        consumer
            .try_consume_internal_message_one(|message: &mut InternalMessage<SpscMessage>, _| {
                received_forward = Some(*message);
            })
            .expect("consume forwarded message")
    );
    let received_forward = received_forward.expect("forwarded message available");
    assert_eq!(received_forward.data(), forwarded.data());
    assert_eq!(received_forward.tracking_timestamp(), forwarded.tracking_timestamp());

    drop(producer);
    drop(consumer);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn claimed_spsc_endpoints_require_polling_through_direct_bundles_and_callbacks() {
    let tmp = tempfile::tempdir().expect("create temp directory");
    let mut spine = new_mixed_spine(tmp.path());
    let mut direct_producer = SpineAdapter::connect_tile(&TestTile, &mut spine);
    let mut callback_consumer = SpineAdapter::connect_tile(&TestTile, &mut spine);

    assert!(!direct_producer.requires_polling());
    direct_producer.producers.spsc.try_attach().expect("claim producer directly");
    assert!(direct_producer.requires_polling());

    drop(direct_producer);
    assert!(!callback_consumer.requires_polling());
    callback_consumer.subscribe_broadcast::<LegacyMessage>();
    let mut legacy_writer = SpineAdapter::connect_tile(&LeftTile, &mut spine);
    legacy_writer.produce(LegacyMessage(37));
    assert!(callback_consumer.consume_one(|message: LegacyMessage, producers| {
        producers.try_produce(SpscMessage(message.0)).expect("publish from callback");
    }));
    assert!(callback_consumer.requires_polling(), "callback claimed an SPSC producer");

    let mut reader = SpineAdapter::connect_tile(&RightTile, &mut spine);
    let mut received = Vec::new();
    reader.try_consume(|message: SpscMessage, _| received.push(message.0)).unwrap();
    assert_eq!(received, [37]);
    reader.begin_loop(IngestionTime::now());
    assert!(!reader.try_consume_one(|_: SpscMessage, _| {}).unwrap());
    assert!(!reader.did_work(), "an empty read does not count as work");
    assert!(reader.requires_polling(), "an empty SPSC consumer must keep polling");

    drop(legacy_writer);
    drop(reader);
    drop(callback_consumer);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn independently_opened_spines_transfer_spsc_messages() {
    let tmp = tempfile::tempdir().expect("create temp directory");
    let mut producer_spine = new_mixed_spine(tmp.path());
    let mut consumer_spine = new_mixed_spine(tmp.path());
    let mut producer = SpineAdapter::connect_tile(&TestTile, &mut producer_spine);
    let mut consumer = SpineAdapter::connect_tile(&TestTile, &mut consumer_spine);

    producer.try_produce(SpscMessage(73)).expect("publish through first mapping");
    let mut received = None;
    assert!(
        consumer
            .try_consume_one(|message: SpscMessage, _| received = Some(message))
            .expect("consume through second mapping")
    );
    assert_eq!(received, Some(SpscMessage(73)));

    drop(producer);
    drop(consumer);
    drop(producer_spine);
    drop(consumer_spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn legacy_spine_constructors_and_bundles_remain_copyable() {
    fn assert_clone_and_copy<T: Clone + Copy>() {}

    assert_clone_and_copy::<LegacyOnlySpineConsumers>();
    assert_clone_and_copy::<LegacyOnlySpineProducers>();

    let tmp = tempfile::tempdir().expect("create temp directory");
    let mut spine = LegacyOnlySpine::new_with_base_dir(tmp.path(), None);
    let adapter = SpineAdapter::connect_tile(&TestTile, &mut spine);
    drop(adapter);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[cfg(feature = "park")]
#[derive(Clone)]
struct ParkConsumer {
    empty_polls: Arc<AtomicUsize>,
    received: Arc<AtomicU64>,
    done: std::sync::mpsc::Sender<()>,
}

#[cfg(feature = "park")]
impl Tile<MixedSpine> for ParkConsumer {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<MixedSpine>) {
        let received = self.received.clone();
        let consumed = adapter
            .try_consume_one(|message: SpscMessage, _| received.store(message.0, Ordering::Relaxed))
            .expect("consumer owns SPSC role");
        if consumed {
            adapter.request_stop_scope();
            self.done.send(()).expect("watchdog is listening");
        } else {
            self.empty_polls.fetch_add(1, Ordering::Release);
        }
    }
}

#[cfg(feature = "park")]
struct ParkProducer {
    consumer_empty_polls: Arc<AtomicUsize>,
    sent: bool,
}

#[cfg(feature = "park")]
impl Tile<MixedSpine> for ParkProducer {
    fn try_init(&mut self, _: &mut SpineAdapter<MixedSpine>) -> bool {
        self.consumer_empty_polls.load(Ordering::Acquire) >= 2
    }

    fn loop_body(&mut self, adapter: &mut SpineAdapter<MixedSpine>) {
        if !self.sent {
            adapter.try_produce(SpscMessage(101)).expect("producer owns SPSC role");
            self.sent = true;
        }
    }
}

#[cfg(feature = "park")]
#[test]
fn parked_tile_runner_keeps_claimed_spsc_endpoints_polling() {
    let tmp = tempfile::tempdir().expect("create temp directory");
    let mut spine = new_mixed_spine(tmp.path());
    let empty_polls = Arc::new(AtomicUsize::new(0));
    let received = Arc::new(AtomicU64::new(0));
    let timed_out = Arc::new(AtomicBool::new(false));
    let (done, completion) = std::sync::mpsc::channel();

    std::thread::scope(|scope| {
        let mut scoped = flux::spine::ScopedSpine::new(&mut spine, scope, None, None);
        // Construct both runners before starting either: timer setup must not
        // provide a process-local signal that masks a parked SPSC consumer.
        let consumer = tile_runner(
            ParkConsumer { empty_polls: empty_polls.clone(), received: received.clone(), done },
            &mut scoped,
            TileConfig::background(None, None).without_metrics().with_park(),
        );
        let producer = tile_runner(
            ParkProducer { consumer_empty_polls: empty_polls, sent: false },
            &mut scoped,
            TileConfig::background(None, None).without_metrics().with_park(),
        );
        let stop = scoped.stop_flag.clone();
        let watchdog_failed = timed_out.clone();
        scope.spawn(move || {
            if completion.recv_timeout(Duration::from_secs(5)).is_err() {
                watchdog_failed.store(true, Ordering::Relaxed);
                stop.store(signal_hook::consts::SIGINT as usize, Ordering::Relaxed);
                flux::park::SIGNAL.signal();
            }
        });
        scope.spawn(consumer);
        scope.spawn(producer);
    });

    assert!(!timed_out.load(Ordering::Relaxed), "SPSC tile parked without an IPC wakeup");
    assert_eq!(received.load(Ordering::Relaxed), 101);
    drop(spine);
    cleanup_shmem(tmp.path());
}
