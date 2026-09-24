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

use std::{
    collections::HashSet,
    env,
    hint::{black_box, spin_loop},
    sync::{
        Barrier, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::Instant,
};

use core_affinity::CoreId;
use flux_communication::{
    ReadError,
    queue::{ConsumerBare, Producer, Queue, QueueType, spsc},
};

const SIZES: [usize; 8] = [8, 32, 64, 128, 192, 256, 512, 1024];
const QUEUES: [&str; 3] = ["MPMC", "SPSC", "RTRB"];
const CAPACITY: usize = 1024;
const WINDOW: usize = 512;
const BATCH: usize = 64;
const POOL: usize = 4096;
const WARMUP: usize = 32768;
const SAMPLE_BLOCK: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
struct Message<const B: usize>([u8; B]);

trait Tx<const B: usize>: Send {
    fn send(&mut self, message: &Message<B>);
}

trait Rx<const B: usize>: Send {
    fn drain(&mut self, callback: impl FnMut(&Message<B>));
}

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

#[repr(align(128))]
struct Credit(AtomicUsize);

struct Samples {
    positions: Vec<usize>,
    starts: Vec<AtomicU64>,
    epoch: Instant,
}

impl Samples {
    fn new(count: usize, seed: u32) -> Self {
        let mut state = seed ^ 0xd1b5_4a35;
        if state == 0 {
            state = 1;
        }
        let positions: Vec<_> = (0..count / SAMPLE_BLOCK)
            .map(|block| block * SAMPLE_BLOCK + (random(&mut state) as usize & (SAMPLE_BLOCK - 1)))
            .collect();
        let starts = positions.iter().map(|_| AtomicU64::new(u64::MAX)).collect();
        Self { positions, starts, epoch: Instant::now() }
    }
}

#[inline(always)]
fn random(state: &mut u32) -> u32 {
    *state ^= *state << 13;
    *state ^= *state >> 17;
    *state ^= *state << 5;
    *state
}

fn payload<const B: usize>(id: usize) -> Message<B> {
    let mut bytes = [0; B];
    for (i, chunk) in bytes[8..].chunks_mut(8).enumerate() {
        let mut z = (id as u64).wrapping_add((i as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        chunk.copy_from_slice(&z.to_le_bytes()[..chunk.len()]);
    }
    let word = (id as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x6a09_e667_f3bc_c909;
    bytes[..8].copy_from_slice(&word.to_le_bytes());
    Message(bytes)
}

// Separate specializations keep full-payload validation and clock sampling
// out of the throughput loop, without changing its queue or callback API.
#[inline(never)]
fn produce<const B: usize, const VERIFY: bool, const LATENCY: bool>(
    sender: &mut impl Tx<B>,
    pool: &[Message<B>],
    count: usize,
    credit: &Credit,
    samples: &Samples,
    seed: u32,
) {
    let mut produced = 0;
    let mut cached_credit = 0;
    let mut state = seed;
    let mut sample_index = 0;
    let mut next_sample = samples.positions.first().copied().unwrap_or(usize::MAX);
    while produced < count {
        let batch = BATCH.min(count - produced);
        while produced + batch > cached_credit + WINDOW {
            cached_credit = credit.0.load(Ordering::Acquire);
            if produced + batch > cached_credit + WINDOW {
                spin_loop();
            }
        }
        for _ in 0..batch {
            if LATENCY {
                let gap = 2 + ((u64::from(random(&mut state)) * 49) >> 32) as u32;
                for _ in 0..gap {
                    #[cfg(target_arch = "x86_64")]
                    // SAFETY: PAUSE has no memory or stack operands.
                    unsafe {
                        std::arch::asm!("pause", options(nomem, nostack, preserves_flags));
                    }
                    #[cfg(not(target_arch = "x86_64"))]
                    spin_loop();
                }
            }
            let index = if VERIFY { produced } else { produced & (POOL - 1) };
            if LATENCY && produced == next_sample {
                samples.starts[sample_index]
                    .store(samples.epoch.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
            sender.send(black_box(&pool[index]));
            if LATENCY && produced == next_sample {
                sample_index += 1;
                next_sample = samples.positions.get(sample_index).copied().unwrap_or(usize::MAX);
            }
            produced += 1;
        }
    }
    if LATENCY {
        assert_eq!(sample_index, samples.positions.len());
    }
}

#[inline(never)]
fn consume<const B: usize, const VERIFY: bool, const LATENCY: bool>(
    rx: &mut impl Rx<B>,
    pool: &[Message<B>],
    count: usize,
    credit: &Credit,
    samples: &Samples,
    latencies: &mut Vec<u64>,
) -> u64 {
    let mut received = 0;
    let mut sum = 0u64;
    let mut sample_index = 0;
    let mut next_sample = samples.positions.first().copied().unwrap_or(usize::MAX);
    while received < count {
        let before = received;
        rx.drain(|message| {
            let message = black_box(message);
            if VERIFY {
                assert_eq!(*message, pool[received], "FIFO/full payload at {received}");
            }
            sum = sum.wrapping_add(u64::from(message.0[B / 2]));
            if LATENCY && received == next_sample {
                let end = samples.epoch.elapsed().as_nanos() as u64;
                let start = samples.starts[sample_index].load(Ordering::Relaxed);
                assert_ne!(start, u64::MAX, "sample published before message");
                latencies.push(end.checked_sub(start).expect("monotonic clock"));
                sample_index += 1;
                next_sample = samples.positions.get(sample_index).copied().unwrap_or(usize::MAX);
            }
            received += 1;
        });
        if received != before {
            credit.0.store(received, Ordering::Release);
        }
    }
    assert_eq!(received, count);
    if LATENCY {
        assert_eq!(latencies.len(), samples.positions.len());
    }
    black_box(sum)
}

struct Measurement {
    mmsg_s: f64,
    latencies: Vec<u64>,
}

fn phase<const B: usize, const VERIFY: bool, const LATENCY: bool, S: Tx<B>, R: Rx<B>>(
    sender: S,
    receiver: R,
    pool: &[Message<B>],
    count: usize,
    seed: u32,
    cpus: [usize; 2],
) -> (S, R, Measurement) {
    let credit = &Credit(AtomicUsize::new(0));
    let barrier = &Barrier::new(3);
    let samples = &Samples::new(if LATENCY { count } else { 0 }, seed);
    let started = &OnceLock::new();
    let (sender, receiver, sum, seconds, latencies) = thread::scope(|scope| {
        let producer = scope.spawn(move || {
            pin(cpus[0]);
            let mut sender = sender;
            barrier.wait();
            started.set(Instant::now()).unwrap();
            produce::<B, VERIFY, LATENCY>(&mut sender, pool, count, credit, samples, seed);
            sender
        });
        let consumer = scope.spawn(move || {
            pin(cpus[1]);
            let mut receiver = receiver;
            let mut latencies = vec![0u64; samples.positions.len()];
            for page in latencies.chunks_mut(512) {
                black_box(page)[0] = 0;
            }
            latencies.clear();
            barrier.wait();
            let sum = consume::<B, VERIFY, LATENCY>(
                &mut receiver,
                pool,
                count,
                credit,
                samples,
                &mut latencies,
            );
            let finished = Instant::now();
            let seconds =
                finished.duration_since(*started.get().expect("producer started")).as_secs_f64();
            (receiver, sum, seconds, latencies)
        });
        barrier.wait();
        let sender = producer.join().unwrap();
        let (receiver, sum, seconds, latencies) = consumer.join().unwrap();
        (sender, receiver, sum, seconds, latencies)
    });
    let expected: u64 =
        (0..count).map(|n| u64::from(pool[if VERIFY { n } else { n & (POOL - 1) }].0[B / 2])).sum();
    assert_eq!(sum, expected, "selected-byte checksum");
    (sender, receiver, Measurement { mmsg_s: count as f64 / seconds / 1e6, latencies })
}

fn transfer<const B: usize>(
    sender: impl Tx<B>,
    receiver: impl Rx<B>,
    settings: &Settings,
    seed: u32,
) -> Option<Measurement> {
    let validation: Vec<_> = (0..WARMUP).map(payload::<B>).collect();
    let pool: Vec<_> = (0..POOL).map(payload::<B>).collect();
    // Move endpoints into their workers so mutable cursor caches are private.
    let (sender, receiver, _) =
        phase::<B, true, false, _, _>(sender, receiver, &validation, WARMUP, seed, settings.cpus);
    if settings.verify_only {
        return None;
    }
    let (sender, receiver, _) =
        phase::<B, false, false, _, _>(sender, receiver, &pool, WARMUP, seed, settings.cpus);
    let (sender, receiver, throughput) = phase::<B, false, false, _, _>(
        sender,
        receiver,
        &pool,
        settings.messages,
        seed,
        settings.cpus,
    );
    let (sender, receiver, _) =
        phase::<B, false, true, _, _>(sender, receiver, &pool, WARMUP, seed, settings.cpus);
    let (_, _, latency) = phase::<B, false, true, _, _>(
        sender,
        receiver,
        &pool,
        settings.messages,
        seed,
        settings.cpus,
    );
    Some(Measurement { mmsg_s: throughput.mmsg_s, latencies: latency.latencies })
}

fn quantile(sorted: &[u64], percent: usize) -> u64 {
    sorted[(percent * sorted.len()).div_ceil(100) - 1]
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
                transfer(sender, receiver, settings, seed)
            } else if index == 1 {
                let queue = spsc::Queue::<Message<B>>::new(CAPACITY);
                transfer(
                    queue.try_producer().unwrap(),
                    queue.try_consumer().unwrap(),
                    settings,
                    seed,
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
    for (queue_name, results) in QUEUES.into_iter().zip(results) {
        if results.is_empty() {
            continue;
        }
        let n = results.len();
        let mean = results.iter().map(|r| r.mmsg_s).sum::<f64>() / n as f64;
        let sd = if n > 1 {
            (results.iter().map(|r| (r.mmsg_s - mean).powi(2)).sum::<f64>() / (n - 1) as f64).sqrt()
        } else {
            f64::NAN
        };
        let mut samples: Vec<_> = results.into_iter().flat_map(|r| r.latencies).collect();
        samples.sort_unstable();
        println!(
            "summary,{queue_name},{B},{n},{mean:.6},{sd:.6},{},{}",
            quantile(&samples, 50),
            quantile(&samples, 95)
        );
    }
}

fn pin(cpu: usize) {
    assert!(core_affinity::set_for_current(CoreId { id: cpu }), "cannot pin CPU {cpu}");
}

fn cpu_list(text: &str) -> HashSet<usize> {
    text.trim()
        .split(',')
        .filter(|s| !s.is_empty())
        .flat_map(|range| {
            let (a, b) = range.split_once('-').unwrap_or((range, range));
            a.parse::<usize>().unwrap()..=b.parse::<usize>().unwrap()
        })
        .collect()
}

fn number(name: &str, default: usize) -> usize {
    env::var(name).map_or(default, |s| s.parse().unwrap_or_else(|_| panic!("invalid {name}")))
}

struct Settings {
    cpus: [usize; 2],
    messages: usize,
    runs: usize,
    size: Option<usize>,
    queue: Option<String>,
    verify_only: bool,
}

impl Settings {
    fn read() -> Self {
        let cpus: Vec<_> = env::var("FLUX_BENCH_CPUS")
            .expect("set FLUX_BENCH_CPUS=producer,consumer")
            .split(',')
            .map(|s| s.trim().parse::<usize>().expect("CPU number"))
            .collect();
        let cpus: [usize; 2] = cpus.try_into().expect("exactly two CPUs required");
        assert_ne!(cpus[0], cpus[1]);
        let isolated = cpu_list(
            &std::fs::read_to_string("/sys/devices/system/cpu/isolated")
                .expect("read isolated CPUs"),
        );
        let available = core_affinity::get_core_ids().expect("CPU affinity");
        let mut worker_siblings = HashSet::new();
        if isolated.is_empty() {
            eprintln!(
                "No isolated CPUs configured: keep the chosen cores idle; expect more noise."
            );
        }
        for cpu in cpus {
            assert!(isolated.is_empty() || isolated.contains(&cpu), "CPU {cpu} is not isolated");
            let siblings = cpu_list(
                &std::fs::read_to_string(format!(
                    "/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"
                ))
                .unwrap(),
            );
            assert!(
                !cpus.iter().any(|other| *other != cpu && siblings.contains(other)),
                "use different physical cores"
            );
            worker_siblings.extend(siblings);
            pin(cpu); // Fail before starting workers if an affinity is unavailable.
        }
        let housekeeping = available
            .into_iter()
            .find(|cpu| !isolated.contains(&cpu.id) && !worker_siblings.contains(&cpu.id))
            .expect("allow a housekeeping CPU outside the worker cores");
        pin(housekeeping.id);
        let messages = number("FLUX_BENCH_MESSAGES", 1 << 24);
        assert!(messages > 0 && messages.is_multiple_of(SAMPLE_BLOCK));
        let verify_only = env::var("FLUX_BENCH_VERIFY_ONLY").is_ok_and(|s| s == "1");
        let runs = if verify_only { 1 } else { number("FLUX_BENCH_RUNS", 5) };
        assert!(runs > 0);
        let size = env::var("FLUX_BENCH_SIZE").ok().map(|s| s.parse().expect("payload size"));
        assert!(size.is_none_or(|b| SIZES.contains(&b)), "unsupported payload size");
        let queue = env::var("FLUX_BENCH_QUEUE").ok();
        assert!(queue.as_deref().is_none_or(|q| QUEUES.contains(&q)), "choose MPMC, SPSC or RTRB");
        Self { cpus, messages, runs, size, queue, verify_only }
    }
}

fn main() {
    // A failed worker must stop its peer, which may be waiting for credits.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        hook(info);
        std::process::abort();
    }));
    let settings = Settings::read();
    println!(
        "# capacity={CAPACITY} pool={POOL} credit={WINDOW} batch={BATCH} warmup={WARMUP} messages={} cpus={:?}",
        settings.messages, settings.cpus
    );
    println!("# run,queue,bytes,run,Mm/s,p50_ns,p95_ns");
    println!("# summary,queue,bytes,n,mean_Mm/s,sample_SD,p50_ns,p95_ns (pooled)");
    macro_rules! sizes {
        ($($size:literal),*) => { $(if settings.size.is_none_or(|b| b == $size) { compare::<$size>(&settings); })* };
    }
    sizes!(8, 32, 64, 128, 192, 256, 512, 1024);
}
