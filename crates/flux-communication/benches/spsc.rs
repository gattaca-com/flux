//! Raw MPMC, SPSC and rtrb: saturated throughput and separately paced latency.
//!
//! Run with two otherwise idle, isolated physical cores (producer, consumer):
//! ```text
//! RUSTFLAGS="-C target-cpu=native -C panic=abort" \
//! CARGO_PROFILE_BENCH_CODEGEN_UNITS=1 CARGO_PROFILE_BENCH_LTO=fat \
//! FLUX_BENCH_CPUS=13,12 cargo bench -p flux-communication --bench spsc
//! ```
//! Keep their SMT siblings idle. These compiler settings match the PR study.
//! Defaults: all eight sizes, all three queues, five repetitions, 16M messages
//! per phase. Optional filters: `FLUX_BENCH_SIZE=128`, `FLUX_BENCH_QUEUE=SPSC`.
//! `FLUX_BENCH_RUNS` and `FLUX_BENCH_MESSAGES` change the measurement budget;
//! `FLUX_BENCH_VERIFY_ONLY=1` runs just the full-payload/FIFO checks.
//!
//! SPSC and rtrb borrow one slot at a time; MPMC copies into scratch storage.
//! All callbacks black-box the reference and read only the middle byte. A
//! common credit window prevents MPMC overwrite. Its bookkeeping is included in
//! throughput. Latency samples one message per 1024 after a random 2–50 x86
//! PAUSE instructions (`spin_loop` hints on other architectures). The pause is
//! excluded; clock overhead and queue backlog are included.
//! Results summarize all runs, without trimming; SD describes run variation.

#[path = "../../../benches/queue_support.rs"]
mod support;
use flux_communication::{
    ReadError,
    queue::{ConsumerBare, Producer, Queue, QueueType, spsc},
};
use support::{
    CAPACITY, Measurement, Message, Rx, Settings, Tx, abort_on_panic, print_header, quantile,
    summarize, transfer,
};

const QUEUES: [&str; 3] = ["MPMC", "SPSC", "RTRB"];

impl<const B: usize> Tx<B> for spsc::Producer<Message<B>> {
    #[inline]
    fn send(&mut self, message: &Message<B>) {
        self.produce(message).expect("credit prevents Full");
    }
}

impl<const B: usize> Rx<B> for spsc::Consumer<Message<B>> {
    #[inline]
    fn drain(&mut self, mut callback: impl FnMut(&Message<B>)) {
        while self.consume_ref(&mut callback) {}
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

struct MpmcRx<const B: usize> {
    consumer: ConsumerBare<Message<B>>,
    scratch: Message<B>,
}

impl<const B: usize> Rx<B> for MpmcRx<B> {
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

fn compare<const B: usize>(settings: &Settings) {
    let mut results: [Vec<Measurement>; 3] = std::array::from_fn(|_| Vec::new());
    let orders = [[0, 1, 2], [2, 1, 0], [1, 2, 0], [0, 2, 1], [2, 0, 1], [1, 0, 2]];
    for run in 0..settings.runs {
        let seed = 0x9e37_79b9 ^ (run as u32 + 1);
        for index in orders[run % orders.len()] {
            let queue_name = QUEUES[index];
            if settings.queue.as_deref().is_some_and(|q| q != queue_name) {
                continue;
            }
            let result = if index == 2 {
                let (sender, receiver) = rtrb::RingBuffer::<Message<B>>::new(CAPACITY);
                transfer(sender, receiver, settings, seed, |_| {})
            } else if index == 1 {
                let queue = spsc::Queue::<Message<B>>::new(CAPACITY);
                transfer(
                    queue.try_producer().unwrap(),
                    queue.try_consumer().unwrap(),
                    settings,
                    seed,
                    |_| {},
                )
            } else {
                let queue = Queue::<Message<B>>::new(CAPACITY, QueueType::MPMC);
                let mut consumer = ConsumerBare::new(queue, "raw-comparison");
                consumer.subscribe_broadcast();
                transfer(
                    Producer::from(queue),
                    MpmcRx { consumer, scratch: Message([0; B]) },
                    settings,
                    seed,
                    |_| {},
                )
            };
            if let Some(mut result) = result {
                result.latencies.sort_unstable();
                println!(
                    "run,{queue_name},{B},{},{:.6},{},{}",
                    run + 1,
                    result.mmsg_s,
                    quantile(&result.latencies, 50),
                    quantile(&result.latencies, 95)
                );
                results[index].push(result);
            } else {
                println!("verified,{queue_name},{B}");
            }
        }
    }
    for (queue, results) in QUEUES.into_iter().zip(results) {
        summarize(queue, B, results);
    }
}

fn main() {
    abort_on_panic();
    let settings = Settings::read(&QUEUES);
    print_header(&settings);
    macro_rules! sizes {
        ($($size:literal),*) => { $(if settings.size.is_none_or(|b| b == $size) { compare::<$size>(&settings); })* };
    }
    sizes!(8, 32, 64, 128, 192, 256, 512, 1024);
}
