use std::{
    collections::HashSet,
    env,
    hint::{black_box, spin_loop},
    sync::{
        Barrier,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};

use core_affinity::CoreId;
#[path = "latency.rs"]
mod latency;
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
#[path = "throughput.rs"]
mod throughput;
#[path = "window_stats.rs"]
mod window_stats;

pub const SIZES: [usize; 11] = [1, 2, 4, 8, 32, 64, 128, 192, 256, 512, 1024];
pub const CAPACITY: usize = 1024;
const WINDOW: usize = 512;
const BATCH: usize = 64;
const POOL: usize = 4096;
const VALIDATION: usize = 32768;
const WARMUP: usize = 32768;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct Message<const B: usize>(pub [u8; B]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotLayout {
    Natural,
    Bytes64,
    Bytes128,
    Bytes192,
    Bytes256,
}

impl SlotLayout {
    pub fn sizes(self) -> &'static [usize] {
        match self {
            Self::Natural => &SIZES,
            Self::Bytes64 => &[1, 2, 4, 8, 32],
            Self::Bytes128 => &[8, 32, 64],
            Self::Bytes192 => &[8, 32, 64, 128],
            Self::Bytes256 => &[8, 32, 64, 128, 192],
        }
    }
}

pub struct SlotGeometry {
    pub size: usize,
    pub alignment: usize,
    pub payload_offset: usize,
}

impl SlotGeometry {
    fn check(&self, payload_addresses: [usize; 2]) {
        let first = payload_addresses[0] - self.payload_offset;
        assert!(first.is_multiple_of(self.alignment), "slot alignment");
        assert_eq!(payload_addresses[1] - payload_addresses[0], self.size, "slot stride");
        println!(
            "# SPSC slot_bytes={} slot_align={} ring_base={first:#x} payload_offset={}",
            self.size, self.alignment, self.payload_offset
        );
    }
}

pub trait Tx<const B: usize>: Send {
    fn begin_batch(&mut self) {}
    fn send(&mut self, message: &Message<B>);
}

pub trait Rx<const B: usize>: Send {
    fn drain(&mut self, callback: impl FnMut(&Message<B>));
    fn slot_geometry(&self) -> Option<SlotGeometry> {
        None
    }
}

fn payload<const B: usize>(id: usize) -> Message<B> {
    let mut bytes = [0; B];
    let prefix = B.min(8);
    for (i, chunk) in bytes[prefix..].chunks_mut(8).enumerate() {
        let mut z = (id as u64).wrapping_add((i as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        chunk.copy_from_slice(&z.to_le_bytes()[..chunk.len()]);
    }
    let mut word = (id as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x6a09_e667_f3bc_c909;
    if B < 8 {
        // Mix high bits before truncation to avoid repeating ring-lap patterns.
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        word ^= word >> 31;
    }
    bytes[..prefix].copy_from_slice(&word.to_le_bytes()[..prefix]);
    Message(bytes)
}

fn source_pool<const B: usize>() -> Vec<Message<B>> {
    (0..POOL).map(payload).collect()
}

// Counts consumed messages; the producer stays at most WINDOW ahead, so MPMC
// and SPMC never overwrite unread data.
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
        sender.begin_batch();
        for index in produced..produced + batch {
            send(sender, index);
        }
        produced += batch;
    }
    waits
}

fn validate<const B: usize>(rx: &mut impl Rx<B>, expected: &[Message<B>], credit: &Credit) {
    let geometry = rx.slot_geometry();
    let mut payload_addresses = [0; 2];
    let mut received = 0;
    while received < expected.len() {
        let before = received;
        rx.drain(|message| {
            assert_eq!(*black_box(message), expected[received], "FIFO/full payload at {received}");
            if geometry.is_some() && received < payload_addresses.len() {
                payload_addresses[received] = std::ptr::from_ref(message).addr();
            }
            received += 1;
        });
        if received != before {
            credit.release(received);
        }
    }
    if let Some(geometry) = geometry {
        geometry.check(payload_addresses);
    }
}

// The controller checks each round after both workers finish. The start
// barrier holds the next round until checks and credit reset are complete.
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
    counts: &[usize],
    mut check: impl FnMut(usize),
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
        for &count in std::iter::once(&VALIDATION).chain(counts) {
            rounds.run(Role::Controller, |_| ());
            check(count);
            rounds.credit.0.store(0, Ordering::Relaxed);
            rounds.consumer_ready.store(false, Ordering::Relaxed);
        }
        (producer.join().unwrap(), consumer.join().unwrap())
    })
}

enum Results {
    Verify,
    Throughput(throughput::Summary),
    Latency(latency::Summary),
}

pub struct Case<'a, const B: usize> {
    name: &'a str,
    settings: &'a Settings,
    results: Results,
}

impl<'a, const B: usize> Case<'a, B> {
    pub fn new(name: &'a str, settings: &'a Settings) -> Self {
        let results = match settings.mode {
            Mode::Verify => Results::Verify,
            Mode::Throughput => Results::Throughput(throughput::Summary::default()),
            Mode::Latency => Results::Latency(latency::Summary::default()),
        };
        Self { name, settings, results }
    }

    pub fn run(
        &mut self,
        run: usize,
        sender: impl Tx<B>,
        receiver: impl Rx<B>,
        check: impl FnMut(usize),
    ) {
        let (name, settings) = (self.name, self.settings);
        println!("# case queue={name} bytes={B} run={run}");
        match &mut self.results {
            Results::Verify => {
                workers(sender, receiver, settings, &[], check, |_, _| (), |_, _| ());
                println!("verified,{name},{B}");
            }
            Results::Throughput(summary) => {
                summary.add(name, B, run, throughput::run(sender, receiver, settings, check));
            }
            Results::Latency(summary) => {
                let seed = 0x9e37_79b9 ^ run as u32;
                let latencies = latency::run(sender, receiver, settings, seed, check);
                summary.add(name, B, run, &latencies);
            }
        }
    }

    pub fn summarize(&self) {
        match &self.results {
            Results::Verify => {}
            Results::Throughput(summary) => summary.print(self.name, B),
            Results::Latency(summary) => summary.print(self.name, B),
        }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Throughput,
    Latency,
    Verify,
}

pub struct Settings {
    cpus: [usize; 2],
    mode: Mode,
    messages: usize,
    pub runs: usize,
    pub size: Option<usize>,
    pub queue: Option<String>,
    pub slot: SlotLayout,
    windows: bool,
    window_samples: bool,
    pauses: [u32; 2],
}

impl Settings {
    pub fn read(queues: &[&str]) -> Self {
        assert!(
            env::var_os("FLUX_BENCH_VERIFY_ONLY").is_none(),
            "use FLUX_BENCH_MODE=verify instead of FLUX_BENCH_VERIFY_ONLY"
        );
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
        let mode = match env::var("FLUX_BENCH_MODE").as_deref() {
            Err(_) | Ok("throughput") => Mode::Throughput,
            Ok("latency") => Mode::Latency,
            Ok("verify") => Mode::Verify,
            Ok(_) => panic!("FLUX_BENCH_MODE must be throughput, latency or verify"),
        };
        let messages =
            number("FLUX_BENCH_MESSAGES", if mode == Mode::Latency { 1 << 20 } else { 1 << 24 });
        assert!(messages > 0);
        let (windows, window_samples) = match env::var("FLUX_BENCH_WINDOWS").as_deref() {
            Err(_) | Ok("summary") => (true, false),
            Ok("samples") => (true, true),
            Ok("off") => (false, false),
            Ok(_) => panic!("FLUX_BENCH_WINDOWS must be summary, samples, or off"),
        };
        let runs = if mode == Mode::Verify { 1 } else { number("FLUX_BENCH_RUNS", 5) };
        assert!(runs > 0);
        let size = env::var("FLUX_BENCH_SIZE").ok().map(|s| s.parse().expect("payload size"));
        assert!(size.is_none_or(|b| SIZES.contains(&b)), "unsupported payload size");
        assert!(
            mode != Mode::Latency || size.is_none_or(|b| b >= 8),
            "latency requires at least 8 payload bytes for its timestamp"
        );
        let queue = env::var("FLUX_BENCH_QUEUE").ok();
        assert!(queue.as_deref().is_none_or(|q| queues.contains(&q)), "unsupported queue");
        let slot = match env::var("FLUX_BENCH_SLOT").as_deref().unwrap_or("natural") {
            "natural" => SlotLayout::Natural,
            "64" => SlotLayout::Bytes64,
            "128" => SlotLayout::Bytes128,
            "192" => SlotLayout::Bytes192,
            "256" => SlotLayout::Bytes256,
            _ => panic!("choose slot natural, 64, 128, 192 or 256"),
        };
        assert!(
            slot == SlotLayout::Natural || queue.as_deref() == Some("SPSC"),
            "set FLUX_BENCH_QUEUE=SPSC when selecting a padded slot"
        );
        assert!(
            size.is_none_or(|b| slot.sizes().contains(&b)),
            "unsupported payload/slot combination"
        );
        let pauses = [number("FLUX_BENCH_PAUSE_MIN", 25), number("FLUX_BENCH_PAUSE_MAX", 150)]
            .map(|n| u32::try_from(n).expect("pause count fits u32"));
        assert!(pauses[0] <= pauses[1], "minimum pause exceeds maximum");
        Self { cpus, mode, messages, runs, size, queue, slot, windows, window_samples, pauses }
    }

    pub fn print_header(&self) {
        println!(
            "# capacity={CAPACITY} pool={POOL} credit={WINDOW} batch={BATCH} validation={VALIDATION} warmup={WARMUP} messages={} cpus={:?}",
            self.messages, self.cpus
        );
        println!(
            "# SPSC slot={:?}; explicit sizes select stride and alignment in bytes",
            self.slot
        );
        match self.mode {
            Mode::Verify => println!("# mode=verify\n# verified,queue,bytes"),
            Mode::Throughput => throughput::print_header(self),
            Mode::Latency => latency::print_header(self),
        }
    }
}

// Only instantiate supported combinations: a run-time guard cannot prevent an
// invalid stride's compile-time layout assertion from being evaluated.
macro_rules! dispatch_layout {
    ($settings:ident, $run:ident $(, $extra:expr)*) => {
        for &size in $settings.slot.sizes() {
            if size < 8 && $settings.size.is_none() { continue; }
            if $settings.size.is_some_and(|selected| selected != size) { continue; }
            match ($settings.slot, size) {
                ($crate::support::SlotLayout::Natural, 1) => $run::<1, 0>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Natural, 2) => $run::<2, 0>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Natural, 4) => $run::<4, 0>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Natural, 8) => $run::<8, 0>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Natural, 32) => $run::<32, 0>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Natural, 64) => $run::<64, 0>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Natural, 128) => $run::<128, 0>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Natural, 192) => $run::<192, 0>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Natural, 256) => $run::<256, 0>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Natural, 512) => $run::<512, 0>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Natural, 1024) => $run::<1024, 0>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes64, 1) => $run::<1, 64>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes64, 2) => $run::<2, 64>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes64, 4) => $run::<4, 64>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes64, 8) => $run::<8, 64>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes64, 32) => $run::<32, 64>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes128, 8) => $run::<8, 128>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes128, 32) => $run::<32, 128>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes128, 64) => $run::<64, 128>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes192, 8) => $run::<8, 192>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes192, 32) => $run::<32, 192>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes192, 64) => $run::<64, 192>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes192, 128) => $run::<128, 192>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes256, 8) => $run::<8, 256>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes256, 32) => $run::<32, 256>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes256, 64) => $run::<64, 256>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes256, 128) => $run::<128, 256>(&$settings $(, $extra)*),
                ($crate::support::SlotLayout::Bytes256, 192) => $run::<192, 256>(&$settings $(, $extra)*),
                _ => unreachable!("validated layout and size"),
            }
        }
    };
}
pub(crate) use dispatch_layout;

pub fn abort_on_panic() {
    // A failed worker must stop its peer, which may be waiting for credits.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        hook(info);
        std::process::abort();
    }));
}
