#[path = "../../../benches/queue_support.rs"]
mod support;
use flux_communication::{
    ReadError,
    queue::{ConsumerBare, Producer, Queue, QueueType, spsc},
};
use support::{
    CAPACITY, Case, Message, Rx, Settings, SlotGeometry, Tx, abort_on_panic, dispatch_layout,
};

const QUEUES: [&str; 4] = ["MPMC", "SPMC", "SPSC", "RTRB"];

impl<const B: usize, const SLOT_SIZE: usize> Tx<B> for spsc::Producer<Message<B>, SLOT_SIZE> {
    #[inline]
    fn send(&mut self, message: &Message<B>) {
        self.produce(message).expect("credit prevents Full");
    }
}

impl<const B: usize, const SLOT_SIZE: usize> Rx<B> for spsc::Consumer<Message<B>, SLOT_SIZE> {
    #[inline]
    fn drain(&mut self, mut callback: impl FnMut(&Message<B>)) {
        while self.consume_ref(&mut callback) {}
    }
    fn slot_geometry(&self) -> Option<SlotGeometry> {
        Some(SlotGeometry {
            size: spsc::Queue::<Message<B>, SLOT_SIZE>::SLOT_SIZE,
            alignment: spsc::Queue::<Message<B>, SLOT_SIZE>::SLOT_ALIGN,
            payload_offset: 0,
        })
    }
}

impl<const B: usize> Tx<B> for rtrb::Producer<Message<B>> {
    #[inline]
    fn send(&mut self, message: &Message<B>) {
        self.push(*message).expect("credit prevents Full");
    }
}

impl<const B: usize> Rx<B> for rtrb::Consumer<Message<B>> {
    #[inline]
    fn drain(&mut self, mut callback: impl FnMut(&Message<B>)) {
        while let Ok(chunk) = self.read_chunk(1) {
            callback(&chunk.as_slices().0[0]);
            chunk.commit_all();
        }
    }
}

impl<const B: usize> Tx<B> for Producer<Message<B>> {
    #[inline]
    fn send(&mut self, message: &Message<B>) {
        self.produce(message);
    }
}

struct BroadcastRx<const B: usize> {
    consumer: ConsumerBare<Message<B>>,
    scratch: Message<B>,
}

impl<const B: usize> Rx<B> for BroadcastRx<B> {
    #[inline]
    fn drain(&mut self, mut callback: impl FnMut(&Message<B>)) {
        loop {
            match self.consumer.try_consume(&mut self.scratch) {
                Ok(()) => callback(&self.scratch),
                Err(ReadError::Empty) => break,
                Err(error) => panic!("lossless benchmark: {error}"),
            }
        }
    }
}

fn compare<const B: usize, const SLOT_SIZE: usize>(settings: &Settings) {
    let mut cases = QUEUES.map(|name| Case::<B>::new(name, settings));
    let orders = [[0, 1, 3, 2], [1, 2, 0, 3], [2, 3, 1, 0], [3, 0, 2, 1]];
    for run in 1..=settings.runs {
        for index in orders[(run - 1) % orders.len()] {
            if settings.queue.as_deref().is_some_and(|q| q != QUEUES[index]) {
                continue;
            }
            let case = &mut cases[index];
            match QUEUES[index] {
                "RTRB" => {
                    let (sender, receiver) = rtrb::RingBuffer::<Message<B>>::new(CAPACITY);
                    case.run(run, sender, receiver, |_| {});
                }
                "SPSC" => {
                    let queue = spsc::Queue::<Message<B>, SLOT_SIZE>::new(CAPACITY);
                    let (sender, receiver) = (queue.try_producer(), queue.try_consumer());
                    case.run(run, sender.unwrap(), receiver.unwrap(), |_| {});
                }
                _ => {
                    let kind =
                        if QUEUES[index] == "SPMC" { QueueType::SPMC } else { QueueType::MPMC };
                    let queue = Queue::<Message<B>>::new(CAPACITY, kind);
                    let mut consumer = ConsumerBare::new(queue, "raw-comparison");
                    consumer.subscribe_broadcast();
                    let receiver = BroadcastRx { consumer, scratch: Message([0; B]) };
                    case.run(run, Producer::from(queue), receiver, |_| {});
                }
            }
        }
    }
    cases.iter().for_each(Case::summarize);
}

fn main() {
    abort_on_panic();
    let settings = Settings::read(&QUEUES);
    settings.print_header();
    dispatch_layout!(settings, compare);
}
