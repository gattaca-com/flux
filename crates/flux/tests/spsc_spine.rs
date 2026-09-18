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
    communication::{ShmemData, cleanup_shmem, queue::spsc::QueueError},
    spine::{
        FluxSpine, QueueParams, SpineAdapter, SpineProducers, SpineQueue, SpineSpscQueue,
        SpscProduceError,
    },
    tile::{Tile, TileInfo},
};
use flux_timing::{IngestionTime, InternalMessage, TrackingTimestamp};
use flux_utils::directories::shmem_dir_with_base;
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
