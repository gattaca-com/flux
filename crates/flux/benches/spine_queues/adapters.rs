//! Heap-backed, single-queue Spines. Explicit bundles allow const-generic
//! payload sizes without repeating a generated Spine for each size.

use std::path::{Path, PathBuf};

use flux::{
    TimingMessage,
    communication::{
        cleanup_shmem,
        queue::{Queue, QueueType, spsc},
    },
    spine::{
        FluxSpine, SpineAdapter, SpineConsumer, SpineProducer, SpineProducers, SpineQueue,
        SpineSpscConsumer, SpineSpscProducer, SpineSpscQueue, SpscAttachedConsumer,
        SpscConsumerAccess, SpscProduceError, SpscProducerAccess,
    },
    tile::{Tile, TileName},
    timing::{IngestionTime, InternalMessage, TrackingTimestamp},
    utils::{directories::shmem_dir_queues_with_base, short_typename},
};

use crate::support::{CAPACITY, Case, Message, Rx, SlotGeometry, Tx};

const APP: &str = "spine-queue-benchmark";

struct BenchSpine<Q> {
    base_dir: PathBuf,
    queue: Q,
    next_tile: u16,
}

struct Consumers<C> {
    message: C,
}

struct Producers<P> {
    message: P,
    timestamp: TrackingTimestamp,
}

impl<C> AsMut<C> for Consumers<C> {
    fn as_mut(&mut self) -> &mut C {
        &mut self.message
    }
}

impl<P> AsRef<P> for Producers<P> {
    fn as_ref(&self) -> &P {
        &self.message
    }
}

impl<P> AsMut<P> for Producers<P> {
    fn as_mut(&mut self) -> &mut P {
        &mut self.message
    }
}

impl<const B: usize, const SLOT_SIZE: usize> SpscProducerAccess<Message<B>>
    for Producers<SpineSpscProducer<Message<B>, SLOT_SIZE>>
{
    #[inline]
    fn spsc_try_produce_with(
        &mut self,
        make: impl FnOnce() -> InternalMessage<Message<B>>,
    ) -> Result<(), SpscProduceError> {
        self.message.try_produce_with(make)
    }
}

impl<const B: usize, const SLOT_SIZE: usize> SpscConsumerAccess<Message<B>>
    for Consumers<SpineSpscConsumer<Message<B>, SLOT_SIZE>>
{
    #[inline]
    fn spsc_try_attached(
        &mut self,
    ) -> Result<impl SpscAttachedConsumer<Message<B>> + '_, spsc::QueueError> {
        self.message.try_attached()
    }
}

impl<P> SpineProducers for Producers<P> {
    fn timestamp(&self) -> &TrackingTimestamp {
        &self.timestamp
    }
    fn timestamp_mut(&mut self) -> &mut TrackingTimestamp {
        &mut self.timestamp
    }
}

type BroadcastSpine<const B: usize> = BenchSpine<SpineQueue<Message<B>>>;
type SpscSpine<const B: usize, const SLOT_SIZE: usize> =
    BenchSpine<SpineSpscQueue<Message<B>, SLOT_SIZE>>;

impl<const B: usize> FluxSpine for BroadcastSpine<B> {
    type Consumers = Consumers<SpineConsumer<Message<B>>>;
    type Producers = Producers<SpineProducer<Message<B>>>;

    fn attach_consumers<T: Tile<Self>>(&mut self, tile: &T) -> Self::Consumers {
        Consumers { message: SpineConsumer::attach::<_, Self, _>(&self.base_dir, tile, self.queue) }
    }
    fn attach_producers<T: Tile<Self>>(&mut self, tile: &T) -> Self::Producers {
        Producers {
            message: SpineProducer::from(self.queue),
            timestamp: TrackingTimestamp::new(self.register_tile(tile.name())),
        }
    }
    unsafe fn new_in_base_dir(base: impl AsRef<Path>) -> Self {
        Self {
            base_dir: base.as_ref().to_owned(),
            queue: Queue::new(CAPACITY, QueueType::MPMC),
            next_tile: 0,
        }
    }
    fn register_tile(&mut self, _: TileName) -> u16 {
        let id = self.next_tile;
        self.next_tile += 1;
        id
    }
    fn app_name() -> &'static str {
        APP
    }
    fn base_dir(&self) -> &Path {
        &self.base_dir
    }
}

impl<const B: usize, const SLOT_SIZE: usize> FluxSpine for SpscSpine<B, SLOT_SIZE> {
    type Consumers = Consumers<SpineSpscConsumer<Message<B>, SLOT_SIZE>>;
    type Producers = Producers<SpineSpscProducer<Message<B>, SLOT_SIZE>>;

    fn attach_consumers<T: Tile<Self>>(&mut self, tile: &T) -> Self::Consumers {
        Consumers {
            message: SpineSpscConsumer::attach::<_, Self, _>(
                &self.base_dir,
                tile,
                self.queue.clone(),
            ),
        }
    }
    fn attach_producers<T: Tile<Self>>(&mut self, tile: &T) -> Self::Producers {
        Producers {
            message: SpineSpscProducer::new(self.queue.clone()),
            timestamp: TrackingTimestamp::new(self.register_tile(tile.name())),
        }
    }
    unsafe fn new_in_base_dir(base: impl AsRef<Path>) -> Self {
        Self {
            base_dir: base.as_ref().to_owned(),
            queue: SpineSpscQueue::new(CAPACITY),
            next_tile: 0,
        }
    }
    fn register_tile(&mut self, _: TileName) -> u16 {
        let id = self.next_tile;
        self.next_tile += 1;
        id
    }
    fn app_name() -> &'static str {
        APP
    }
    fn base_dir(&self) -> &Path {
        &self.base_dir
    }
}

#[derive(Clone, Copy, Default)]
struct Publisher;
#[derive(Clone, Copy, Default)]
struct Subscriber;

impl<S: FluxSpine> Tile<S> for Publisher {
    fn loop_body(&mut self, _: &mut SpineAdapter<S>) {}
}
impl<S: FluxSpine> Tile<S> for Subscriber {
    fn loop_body(&mut self, _: &mut SpineAdapter<S>) {}
}

struct Sender<S: FluxSpine>(SpineAdapter<S>);
struct Receiver<S: FluxSpine, const TRACK: bool>(SpineAdapter<S>);

impl<const B: usize> Tx<B> for Sender<BroadcastSpine<B>> {
    #[inline]
    fn begin_batch(&mut self) {
        self.0.begin_loop(IngestionTime::now());
    }
    #[inline]
    fn send(&mut self, message: &Message<B>) {
        self.0.produce(*message);
    }
}

impl<const B: usize, const SLOT_SIZE: usize> Tx<B> for Sender<SpscSpine<B, SLOT_SIZE>> {
    #[inline]
    fn begin_batch(&mut self) {
        self.0.begin_loop(IngestionTime::now());
    }
    #[inline]
    fn send(&mut self, message: &Message<B>) {
        self.0.try_produce(*message).expect("credit prevents Full");
    }
}

impl<const B: usize, const TRACK: bool> Rx<B> for Receiver<BroadcastSpine<B>, TRACK> {
    #[inline]
    fn drain(&mut self, mut callback: impl FnMut(&Message<B>)) {
        self.0.begin_loop(IngestionTime::now());
        if TRACK {
            self.0.consume(|m: Message<B>, _| callback(&m));
        } else {
            self.0.consume_maybe_track(|m: Message<B>, _| {
                callback(&m);
                false
            });
        }
    }
}

impl<const B: usize, const SLOT_SIZE: usize, const TRACK: bool> Rx<B>
    for Receiver<SpscSpine<B, SLOT_SIZE>, TRACK>
{
    #[inline]
    fn drain(&mut self, mut callback: impl FnMut(&Message<B>)) {
        self.0.begin_loop(IngestionTime::now());
        if TRACK {
            self.0.consume_ref(|m: &Message<B>, _| callback(m)).unwrap();
        } else {
            self.0
                .consume_ref_maybe_track(|m: &Message<B>, _| {
                    callback(m);
                    false
                })
                .unwrap();
        }
    }
    fn slot_geometry(&self) -> Option<SlotGeometry> {
        let wire = InternalMessage::new(TrackingTimestamp::new(0), Message([0; B]));
        let offset = std::ptr::from_ref(wire.data()).addr() - std::ptr::from_ref(&wire).addr();
        Some(SlotGeometry {
            size: spsc::Queue::<InternalMessage<Message<B>>, SLOT_SIZE>::SLOT_SIZE,
            alignment: spsc::Queue::<InternalMessage<Message<B>>, SLOT_SIZE>::SLOT_ALIGN,
            payload_offset: offset,
        })
    }
}

fn check_telemetry<const B: usize, const TRACK: bool>(base: &Path) -> impl FnMut(usize) {
    let directory = shmem_dir_queues_with_base(base, APP);
    let name = format!("{}-{}", short_typename::<Subscriber>(), short_typename::<Message<B>>());
    let queues: [Queue<TimingMessage>; 2] = ["timing", "latency"]
        .map(|kind| Queue::open_shared(directory.join(format!("{kind}-{name}"))));
    let mut previous = queues.map(|q| q.count());
    move |messages| {
        for (queue, before) in queues.into_iter().zip(&mut previous) {
            let after = queue.count();
            assert_eq!(after - *before, if TRACK { messages } else { 0 }, "telemetry records");
            *before = after;
        }
    }
}

pub fn run<const B: usize, const SLOT_SIZE: usize, const TRACK: bool>(
    queue: &str,
    case: &mut Case<B>,
    run: usize,
) {
    let directory = tempfile::Builder::new().prefix("flux-spine-bench-").tempdir().unwrap();
    // An abort deliberately retains this unique directory for diagnosis.
    eprintln!("Spine telemetry directory: {}", directory.path().display());
    if queue == "SPSC" {
        // SAFETY: this benchmark's constructor uses only local heap queue storage.
        let mut spine = unsafe { SpscSpine::<B, SLOT_SIZE>::new_in_base_dir(directory.path()) };
        let mut sender = SpineAdapter::connect_tile(&Publisher, &mut spine);
        let mut receiver = SpineAdapter::connect_tile(&Subscriber, &mut spine);
        sender.producers.message.try_attach().unwrap();
        receiver.consumers.message.try_attach().unwrap();
        let check = check_telemetry::<B, TRACK>(directory.path());
        case.run(run, Sender(sender), Receiver::<_, TRACK>(receiver), check);
    } else {
        let kind = if queue == "SPMC" { QueueType::SPMC } else { QueueType::MPMC };
        let mut spine = BroadcastSpine::<B> {
            base_dir: directory.path().to_owned(),
            queue: Queue::new(CAPACITY, kind),
            next_tile: 0,
        };
        let sender = SpineAdapter::connect_tile(&Publisher, &mut spine);
        let mut receiver = SpineAdapter::connect_tile(&Subscriber, &mut spine);
        receiver.subscribe_broadcast::<Message<B>>();
        let check = check_telemetry::<B, TRACK>(directory.path());
        case.run(run, Sender(sender), Receiver::<_, TRACK>(receiver), check);
    }
    // All adapters and the Spine have dropped; clean only this owned directory.
    cleanup_shmem(directory.path());
}
