//! Raw queue benchmark: one pinned producer, one pinned consumer, MPMC and
//! SPMC through the copying producer and broadcast consumer. A credit window
//! keeps at most `WINDOW` messages outstanding, so the producer never
//! overwrites unread slots.
//!
//! Each repetition creates a fresh queue and two workers, validates
//! `VALIDATION` messages against the expected payload sequence, warms up with
//! `WARMUP` messages, then measures `FLUX_BENCH_MESSAGES` in the selected mode:
//!
//! - `throughput`: the producer sends as fast as credits allow; messages per
//!   second from the producer's start to the consumer's finish.
//! - `latency`: the producer pauses a pseudorandom number of `PAUSE`s, stamps
//!   the message with `RDTSCP` and sends it; the consumer stamps on receipt and
//!   records the difference in an `HdrHistogram`. Latencies are post-pause
//!   send-to-receive times; the consumer pauses once after each empty read.
//!
//! Output is CSV with `#` comment lines. See `README.md` next to this file.

use std::{
    env,
    hint::{black_box, spin_loop},
    sync::{
        Barrier,
        atomic::{AtomicBool, AtomicUsize, Ordering, compiler_fence},
    },
    thread, time,
};

use core_affinity::CoreId;
use flux_communication::{
    ReadError,
    queue::{ConsumerBare, Producer, Queue, QueueType},
};
use flux_timing::{Duration, Instant};
use hdrhistogram::Histogram;

const SIZES: [usize; 6] = [8, 32, 64, 128, 256, 1024];
const QUEUES: [(&str, QueueType); 2] = [("MPMC", QueueType::MPMC), ("SPMC", QueueType::SPMC)];
const CAPACITY: usize = 1024;
const WINDOW: usize = 512;
const BATCH: usize = 64;
const POOL: usize = 4096;
const VALIDATION: usize = 32768;
const WARMUP: usize = 32768;
const PERCENTILES: [f64; 5] = [50., 90., 99., 99.9, 99.99];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
struct Message<const B: usize>([u8; B]);

fn payload<const B: usize>(id: usize) -> Message<B> {
    let mut bytes = [0; B];
    for (i, chunk) in bytes.chunks_mut(8).enumerate() {
        let mut z = (id as u64).wrapping_add((i as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        chunk.copy_from_slice(&z.to_le_bytes()[..chunk.len()]);
    }
    Message(bytes)
}

fn source_pool<const B: usize>() -> Vec<Message<B>> {
    (0..POOL).map(payload).collect()
}

trait Tx<const B: usize>: Send {
    fn send(&mut self, message: &Message<B>);
}

trait Rx<const B: usize>: Send {
    /// Delivers every available message, then returns on an empty read.
    fn drain(&mut self, callback: impl FnMut(&Message<B>));
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

/// Consumed-message count; the producer stays at most `WINDOW` ahead.
#[repr(align(128))]
struct Credit(AtomicUsize);

impl Credit {
    #[inline(always)]
    fn reserve(&self, end: usize, cached: &mut usize) -> u64 {
        let mut waits = 0;
        while end > *cached + WINDOW {
            *cached = self.0.load(Ordering::Acquire);
            if end > *cached + WINDOW {
                waits += 1;
                spin_loop();
            }
        }
        waits
    }

    #[inline(always)]
    fn release(&self, received: usize) {
        self.0.store(received, Ordering::Release);
    }
}

#[inline(always)]
fn produce<const B: usize, S: Tx<B>>(
    sender: &mut S,
    count: usize,
    credit: &Credit,
    mut send: impl FnMut(&mut S, usize),
) -> u64 {
    let (mut produced, mut credited, mut waits) = (0, 0, 0);
    while produced < count {
        let batch = BATCH.min(count - produced);
        waits += credit.reserve(produced + batch, &mut credited);
        for index in produced..produced + batch {
            send(sender, index);
        }
        produced += batch;
    }
    waits
}

fn validate<const B: usize>(rx: &mut impl Rx<B>, expected: &[Message<B>], credit: &Credit) {
    let mut received = 0;
    while received < expected.len() {
        let before = received;
        rx.drain(|message| {
            assert_eq!(*black_box(message), expected[received], "FIFO/full payload at {received}");
            received += 1;
        });
        if received != before {
            credit.release(received);
        }
    }
}

/// The controller checks each round after both workers finish; the start
/// barrier holds the next round until credits are reset.
struct Rounds {
    barrier: Barrier,
    credit: Credit,
    consumer_ready: AtomicBool,
}

#[derive(Clone, Copy)]
enum Role {
    Producer,
    Consumer,
    Controller,
}

impl Rounds {
    fn run<T>(&self, role: Role, work: impl FnOnce(&Credit) -> T) -> T {
        self.barrier.wait();
        match role {
            // Keep consumer wake-up out of the producer's timed interval.
            Role::Producer => {
                while !self.consumer_ready.load(Ordering::Acquire) {
                    spin_loop();
                }
            }
            Role::Consumer => self.consumer_ready.store(true, Ordering::Release),
            Role::Controller => {}
        }
        let result = work(&self.credit);
        self.barrier.wait();
        result
    }
}

fn workers<const B: usize, S: Tx<B>, R: Rx<B>, P: Send, C: Send>(
    sender: S,
    receiver: R,
    settings: &Settings,
    rounds_after_validation: usize,
    producer: impl FnOnce(&mut S, &Rounds) -> P + Send,
    consumer: impl FnOnce(&mut R, &Rounds) -> C + Send,
) -> (P, C) {
    let expected = &(0..VALIDATION).map(payload).collect::<Vec<_>>();
    let rounds = &Rounds {
        barrier: Barrier::new(3),
        credit: Credit(AtomicUsize::new(0)),
        consumer_ready: AtomicBool::new(false),
    };
    thread::scope(|scope| {
        let producer = scope.spawn(move || {
            pin(settings.cpus[0]);
            let mut sender = sender;
            rounds.run(Role::Producer, |credit| {
                produce(&mut sender, VALIDATION, credit, |sender, i| {
                    sender.send(black_box(&expected[i]));
                })
            });
            producer(&mut sender, rounds)
        });
        let consumer = scope.spawn(move || {
            pin(settings.cpus[1]);
            let mut receiver = receiver;
            rounds.run(Role::Consumer, |credit| validate(&mut receiver, expected, credit));
            consumer(&mut receiver, rounds)
        });
        for _ in 0..=rounds_after_validation {
            rounds.run(Role::Controller, |_| ());
            rounds.credit.0.store(0, Ordering::Relaxed);
            rounds.consumer_ready.store(false, Ordering::Relaxed);
        }
        (producer.join().unwrap(), consumer.join().unwrap())
    })
}

// ---- throughput ------------------------------------------------------------

#[inline(never)]
fn send_throughput<const B: usize>(
    tx: &mut impl Tx<B>,
    pool: &[Message<B>],
    count: usize,
    credit: &Credit,
) {
    produce(tx, count, credit, |tx, index| tx.send(black_box(&pool[index % POOL])));
}

#[inline(never)]
fn receive_throughput<const B: usize>(rx: &mut impl Rx<B>, count: usize, credit: &Credit) -> u64 {
    let mut received = 0;
    let mut sum = 0u64;
    while received < count {
        let before = received;
        rx.drain(|message| {
            sum = sum.wrapping_add(u64::from(black_box(message).0[B / 2]));
            received += 1;
        });
        if received != before {
            credit.release(received);
        }
    }
    sum
}

/// Messages per second, in millions, over the measured round.
fn throughput<const B: usize>(sender: impl Tx<B>, receiver: impl Rx<B>, settings: &Settings) -> f64 {
    let pool = &source_pool::<B>();
    let counts = [WARMUP, settings.messages];
    let (starts, ends) = workers(
        sender,
        receiver,
        settings,
        counts.len(),
        |sender, rounds| {
            counts.map(|count| {
                rounds.run(Role::Producer, |credit| {
                    let start = time::Instant::now();
                    send_throughput(sender, pool, count, credit);
                    start
                })
            })
        },
        |receiver, rounds| {
            counts.map(|count| {
                rounds.run(Role::Consumer, |credit| {
                    let sum = receive_throughput(receiver, count, credit);
                    (time::Instant::now(), sum)
                })
            })
        },
    );
    for (count, (_, sum)) in counts.into_iter().zip(ends) {
        let expected: u64 = (0..count).map(|n| u64::from(pool[n % POOL].0[B / 2])).sum();
        assert_eq!(sum, expected, "selected-byte checksum");
    }
    settings.messages as f64 / ends[1].0.duration_since(starts[1]).as_secs_f64() / 1e6
}

#[derive(Default)]
struct Moments {
    n: usize,
    mean: f64,
    m2: f64,
}

impl Moments {
    fn add(&mut self, value: f64) {
        self.n += 1;
        let delta = value - self.mean;
        self.mean += delta / self.n as f64;
        self.m2 += delta * (value - self.mean);
    }

    fn sample_sd(&self) -> f64 {
        if self.n < 2 { f64::NAN } else { (self.m2 / (self.n - 1) as f64).sqrt() }
    }
}

// ---- latency ---------------------------------------------------------------

/// RDTSCP orders earlier loads; LFENCE keeps later work behind the read.
#[inline(always)]
fn now() -> Instant {
    compiler_fence(Ordering::SeqCst);
    let mut aux = 0;
    let ticks = unsafe {
        let ticks = std::arch::x86_64::__rdtscp(&raw mut aux);
        std::arch::x86_64::_mm_lfence();
        ticks
    };
    compiler_fence(Ordering::SeqCst);
    Instant(ticks)
}

fn ns_per_tick() -> f64 {
    Duration(1 << 32).as_nanos() / (1u64 << 32) as f64
}

struct Latencies {
    histogram: Histogram<u64>,
    previous_end: u64,
    /// Sends stamped before the previous receive: a backlog diagnostic.
    overlapping_sends: u64,
}

impl Latencies {
    fn new() -> Self {
        unsafe {
            use std::arch::x86_64::__cpuid;
            assert!(__cpuid(0x8000_0001).edx & (1 << 27) != 0, "RDTSCP required");
            assert!(__cpuid(0x8000_0007).edx & (1 << 8) != 0, "invariant TSC required");
        }
        let highest = Duration::from_secs(60).0;
        let mut histogram = Histogram::new_with_bounds(1, highest, 3).unwrap();
        histogram.auto(false);
        Self { histogram, previous_end: 0, overlapping_sends: 0 }
    }

    #[inline]
    fn record(&mut self, start: Instant, end: Instant) {
        let ticks = end.0.checked_sub(start.0).expect("latency clock moved backwards");
        self.histogram.record(ticks).expect("latency exceeds histogram range");
        self.overlapping_sends += u64::from(start.0 < self.previous_end);
        self.previous_end = end.0;
    }

    fn reset(&mut self) {
        self.histogram.reset();
        self.previous_end = 0;
        self.overlapping_sends = 0;
    }

    fn csv(&self, kind: &str, name: &str, bytes: usize, n: usize) {
        let scale = ns_per_tick();
        print!(
            "{kind},{name},{bytes},{n},{},{:.1},{:.1}",
            self.histogram.len(),
            self.histogram.mean() * scale,
            self.histogram.stdev() * scale
        );
        for percent in PERCENTILES {
            print!(",{:.1}", self.histogram.value_at_quantile(percent / 100.) as f64 * scale);
        }
        println!();
    }
}

#[inline(always)]
fn xorshift(state: &mut u32) -> u32 {
    *state ^= *state << 13;
    *state ^= *state >> 17;
    *state ^= *state << 5;
    *state
}

#[inline(never)]
fn send_latency<const B: usize>(
    tx: &mut impl Tx<B>,
    pool: &mut [Message<B>],
    count: usize,
    credit: &Credit,
    seed: u32,
    pauses: [u32; 2],
) -> (u64, u64) {
    let mut state = seed;
    let mut stamps = 0u64;
    let width = u64::from(pauses[1] - pauses[0]) + 1;
    let waits = produce(tx, count, credit, |tx, index| {
        let gap = pauses[0] + ((u64::from(xorshift(&mut state)) * width) >> 32) as u32;
        for _ in 0..gap {
            spin_loop();
        }
        let message = &mut pool[index % POOL];
        let start = now();
        message.0[..8].copy_from_slice(&start.0.to_ne_bytes());
        tx.send(black_box(message));
        stamps = stamps.wrapping_add(start.0);
    });
    (stamps, waits)
}

#[inline(never)]
fn receive_latency<const B: usize>(
    rx: &mut impl Rx<B>,
    count: usize,
    credit: &Credit,
    latencies: &mut Latencies,
) -> u64 {
    let mut received = 0;
    let mut stamps = 0u64;
    while received < count {
        let before = received;
        rx.drain(|message| {
            let start = Instant(u64::from_ne_bytes(black_box(message).0[..8].try_into().unwrap()));
            let end = now();
            latencies.record(start, end);
            stamps = stamps.wrapping_add(start.0);
            received += 1;
        });
        if received < count {
            // Every drain ends with an empty read; pause before retrying, as
            // `ConsumerBare::blocking_consume` does on `Empty`.
            spin_loop();
        }
        if received != before {
            credit.release(received);
        }
    }
    stamps
}

fn latency<const B: usize>(
    sender: impl Tx<B>,
    receiver: impl Rx<B>,
    settings: &Settings,
    seed: u32,
) -> Latencies {
    let mut pool = source_pool::<B>();
    let counts = [WARMUP, settings.messages];
    let (produced, (consumed, latencies)) = workers(
        sender,
        receiver,
        settings,
        counts.len(),
        |sender, rounds| {
            counts.map(|count| {
                rounds.run(Role::Producer, |credit| {
                    let start = time::Instant::now();
                    let (stamps, waits) =
                        send_latency(sender, &mut pool, count, credit, seed, settings.pauses);
                    (start, stamps, waits)
                })
            })
        },
        |receiver, rounds| {
            let mut latencies = Latencies::new();
            let consumed = counts.map(|count| {
                latencies.reset();
                rounds.run(Role::Consumer, |credit| {
                    let stamps = receive_latency(receiver, count, credit, &mut latencies);
                    (time::Instant::now(), stamps)
                })
            });
            (consumed, latencies)
        },
    );
    for (sent, received) in produced.iter().zip(&consumed) {
        assert_eq!(sent.1, received.1, "timestamp checksum");
    }
    let ((start, _, credit_waits), (end, _)) = (produced[1], consumed[1]);
    println!(
        "# latency Mm/s={:.3} overlapping_sends={} credit_waits={credit_waits}",
        settings.messages as f64 / end.duration_since(start).as_secs_f64() / 1e6,
        latencies.overlapping_sends
    );
    latencies
}

// ---- driver ----------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Throughput,
    Latency,
}

struct Settings {
    cpus: [usize; 2],
    mode: Mode,
    messages: usize,
    runs: usize,
    size: Option<usize>,
    queue: Option<String>,
    pauses: [u32; 2],
}

fn number(name: &str, default: usize) -> usize {
    env::var(name).map_or(default, |s| s.parse().unwrap_or_else(|_| panic!("invalid {name}")))
}

fn pin(cpu: usize) {
    assert!(core_affinity::set_for_current(CoreId { id: cpu }), "cannot pin CPU {cpu}");
}

impl Settings {
    fn read() -> Self {
        let cpus: Vec<usize> = env::var("FLUX_BENCH_CPUS")
            .expect("set FLUX_BENCH_CPUS=producer,consumer")
            .split(',')
            .map(|s| s.trim().parse().expect("CPU number"))
            .collect();
        let cpus: [usize; 2] = cpus.try_into().expect("exactly two CPUs required");
        assert_ne!(cpus[0], cpus[1], "use two different CPUs");
        // Read the allowed set before pinning narrows it; the controller stays
        // off the worker cores.
        let available = core_affinity::get_core_ids().expect("CPU affinity");
        for cpu in cpus {
            pin(cpu); // Fail before starting workers if an affinity is unavailable.
        }
        let housekeeping = available
            .into_iter()
            .find(|core| !cpus.contains(&core.id))
            .expect("a third CPU for the controller");
        pin(housekeeping.id);
        let mode = match env::var("FLUX_BENCH_MODE").as_deref() {
            Err(_) | Ok("throughput") => Mode::Throughput,
            Ok("latency") => Mode::Latency,
            Ok(_) => panic!("FLUX_BENCH_MODE must be throughput or latency"),
        };
        let messages =
            number("FLUX_BENCH_MESSAGES", if mode == Mode::Latency { 1 << 20 } else { 1 << 24 });
        assert!(messages > 0);
        let runs = number("FLUX_BENCH_RUNS", 5);
        assert!(runs > 0);
        let size = env::var("FLUX_BENCH_SIZE").ok().map(|s| s.parse().expect("payload size"));
        assert!(size.is_none_or(|b| SIZES.contains(&b)), "unsupported payload size");
        let queue = env::var("FLUX_BENCH_QUEUE").ok();
        assert!(
            queue.as_deref().is_none_or(|q| QUEUES.iter().any(|(name, _)| *name == q)),
            "unsupported queue"
        );
        let pauses = [number("FLUX_BENCH_PAUSE_MIN", 25), number("FLUX_BENCH_PAUSE_MAX", 150)]
            .map(|n| u32::try_from(n).expect("pause count fits u32"));
        assert!(pauses[0] <= pauses[1], "minimum pause exceeds maximum");
        Self { cpus, mode, messages, runs, size, queue, pauses }
    }

    fn print_header(&self) {
        println!(
            "# capacity={CAPACITY} credit={WINDOW} batch={BATCH} pool={POOL} validation={VALIDATION} warmup={WARMUP} messages={} runs={} cpus={:?}",
            self.messages, self.runs, self.cpus
        );
        match self.mode {
            Mode::Throughput => {
                println!("# mode=throughput");
                println!("# run,queue,bytes,run,Mm/s");
                println!("# summary,queue,bytes,n,mean_Mm/s,sample_SD");
            }
            Mode::Latency => {
                let percentiles = PERCENTILES.map(|p| format!(",p{p}_ns")).concat();
                println!(
                    "# mode=latency: RDTSCP send-to-receive after {}..={} producer PAUSEs, one consumer PAUSE per empty read",
                    self.pauses[0], self.pauses[1]
                );
                println!("# run,queue,bytes,run,messages,mean_ns,SD_ns{percentiles}");
                println!("# summary,queue,bytes,n,messages,mean_ns,SD_ns{percentiles} (pooled)");
            }
        }
    }
}

enum Results {
    Throughput(Moments),
    Latency(Option<Latencies>),
}

struct Case<'a> {
    name: &'a str,
    runs: usize,
    results: Results,
}

impl<'a> Case<'a> {
    fn new(name: &'a str, settings: &Settings) -> Self {
        let results = match settings.mode {
            Mode::Throughput => Results::Throughput(Moments::default()),
            Mode::Latency => Results::Latency(None),
        };
        Self { name, runs: 0, results }
    }

    fn run<const B: usize>(
        &mut self,
        run: usize,
        sender: impl Tx<B>,
        receiver: impl Rx<B>,
        settings: &Settings,
    ) {
        println!("# case queue={} bytes={B} run={run}", self.name);
        self.runs += 1;
        match &mut self.results {
            Results::Throughput(moments) => {
                let mmsg_s = throughput(sender, receiver, settings);
                println!("run,{},{B},{run},{mmsg_s:.3}", self.name);
                moments.add(mmsg_s);
            }
            Results::Latency(pooled) => {
                let latencies = latency(sender, receiver, settings, 0x9e37_79b9 ^ run as u32);
                latencies.csv("run", self.name, B, run);
                pooled
                    .get_or_insert_with(Latencies::new)
                    .histogram
                    .add(&latencies.histogram)
                    .expect("matching histogram bounds");
            }
        }
    }

    fn summarize<const B: usize>(&self) {
        match &self.results {
            Results::Throughput(moments) if moments.n > 0 => println!(
                "summary,{},{B},{},{:.3},{:.3}",
                self.name,
                moments.n,
                moments.mean,
                moments.sample_sd()
            ),
            Results::Latency(Some(pooled)) => pooled.csv("summary", self.name, B, self.runs),
            _ => {}
        }
    }
}

fn compare<const B: usize>(settings: &Settings) {
    let mut cases = QUEUES.map(|(name, _)| Case::new(name, settings));
    for run in 1..=settings.runs {
        // Alternate the order between runs.
        let order = if run % 2 == 1 { [0, 1] } else { [1, 0] };
        for index in order {
            let (name, kind) = QUEUES[index];
            if settings.queue.as_deref().is_some_and(|q| q != name) {
                continue;
            }
            let queue = Queue::<Message<B>>::new(CAPACITY, kind);
            let mut consumer = ConsumerBare::new(queue, "queue-bench");
            consumer.subscribe_broadcast();
            let receiver = BroadcastRx { consumer, scratch: Message([0; B]) };
            cases[index].run(run, Producer::from(queue), receiver, settings);
        }
    }
    cases.iter().for_each(Case::summarize::<B>);
}

fn main() {
    // A failed worker must stop its peer, which may be waiting for credits.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        hook(info);
        std::process::abort();
    }));
    let settings = Settings::read();
    settings.print_header();
    for &size in &SIZES {
        if settings.size.is_some_and(|selected| selected != size) {
            continue;
        }
        match size {
            8 => compare::<8>(&settings),
            32 => compare::<32>(&settings),
            64 => compare::<64>(&settings),
            128 => compare::<128>(&settings),
            256 => compare::<256>(&settings),
            1024 => compare::<1024>(&settings),
            _ => unreachable!(),
        }
    }
}
