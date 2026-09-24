// Paced one-way latency: the producer pauses, then stamps each message with an
// ordered counter read; the consumer records every difference in its own
// histogram.
#[cfg(target_arch = "x86_64")]
use std::sync::atomic::{Ordering, compiler_fence};
use std::{
    hint::{black_box, spin_loop},
    time,
};

use flux_timing::{Duration, Instant};
use hdrhistogram::Histogram;

use super::{Credit, Message, POOL, Role, Rx, Settings, Tx, WARMUP, produce, source_pool, workers};

const PERCENTILES: [f64; 7] = [10., 50., 90., 95., 99., 99.5, 99.9];

// RDTSCP orders earlier loads; LFENCE keeps subsequent work behind the read.
// Compiler barriers also keep the message access on its intended side.
#[inline(always)]
pub fn now() -> Instant {
    #[cfg(target_arch = "x86_64")]
    {
        compiler_fence(Ordering::SeqCst);
        let mut aux = 0;
        // SAFETY: Latencies::new checks RDTSCP before the first latency round. SSE2
        // (including LFENCE) is part of the x86-64 baseline.
        let ticks = unsafe {
            let ticks = std::arch::x86_64::__rdtscp(&raw mut aux);
            std::arch::x86_64::_mm_lfence();
            ticks
        };
        compiler_fence(Ordering::SeqCst);
        Instant(ticks)
    }
    #[cfg(not(target_arch = "x86_64"))]
    panic!("ordered latency clock requires x86-64");
}

#[inline]
pub fn write_timestamp(message: &mut [u8], time: Instant) {
    message[..8].copy_from_slice(&time.0.to_ne_bytes());
}

#[inline]
pub fn read_timestamp(message: &[u8]) -> Instant {
    Instant(u64::from_ne_bytes(message[..8].try_into().unwrap()))
}

pub struct Latencies {
    // Store ticks; conversion to nanoseconds happens when reporting, not on
    // the consumer's critical path. Three significant digits bound bin width.
    histogram: Histogram<u64>,
    previous_end: u64,
    pub overlapping_sends: u64,
}

impl Latencies {
    pub fn new() -> Self {
        assert!(cfg!(target_arch = "x86_64"), "ordered latency clock requires x86-64");
        #[cfg(target_arch = "x86_64")]
        // SAFETY: CPUID is available on x86-64. Match Quanta's counter requirements
        // so Flux's tick conversion describes the same TSC read by RDTSCP.
        unsafe {
            use std::arch::x86_64::__cpuid;
            assert!(__cpuid(0x8000_0000).eax >= 0x8000_0007, "extended CPUID required");
            assert!(__cpuid(0x8000_0001).edx & (1 << 27) != 0, "RDTSCP required");
            assert!(__cpuid(0x8000_0007).edx & (1 << 8) != 0, "invariant TSC required");
        }
        // Initialize the shared clock and conversion before timing.
        let highest = Duration::from_secs(60).0;
        let _ = Instant::now();
        let _ = Duration(1).as_nanos();
        let mut histogram = Histogram::new_with_bounds(1, highest, 3).unwrap();
        histogram.auto(false);
        // Touch all histogram storage before timing; reset keeps it allocated.
        histogram.record(highest).unwrap();
        histogram.reset();
        Self { histogram, previous_end: 0, overlapping_sends: 0 }
    }

    #[inline]
    pub fn record(&mut self, start: Instant, end: Instant) {
        let ticks = end.0.checked_sub(start.0).expect("latency clock moved backwards");
        self.histogram.record(ticks).expect("latency exceeds preallocated histogram range");
        // A send began before the previous receive timestamp. This is an
        // overlap diagnostic, not an exact measurement of queue occupancy.
        self.overlapping_sends += u64::from(start.0 < self.previous_end);
        self.previous_end = end.0;
    }

    pub fn len(&self) -> u64 {
        self.histogram.len()
    }

    pub fn reset(&mut self) {
        self.histogram.reset();
        self.previous_end = 0;
        self.overlapping_sends = 0;
    }

    pub fn quantile_ns(&self, quantile: f64) -> f64 {
        assert!(!self.histogram.is_empty());
        self.histogram.value_at_quantile(quantile) as f64 * ns_per_tick()
    }

    pub fn mean_and_stdev_ns(&self) -> (f64, f64) {
        // Scale fractional tick statistics without rounding to whole nanoseconds.
        (self.histogram.mean() * ns_per_tick(), self.histogram.stdev() * ns_per_tick())
    }

    pub fn add(&mut self, other: &Self) {
        self.histogram.add(&other.histogram).expect("matching histogram bounds");
    }
}

pub(super) fn ns_per_tick() -> f64 {
    Duration(1 << 32).as_nanos() / (1u64 << 32) as f64
}

#[inline(always)]
fn random(state: &mut u32) -> u32 {
    *state ^= *state << 13;
    *state ^= *state >> 17;
    *state ^= *state << 5;
    *state
}

// Pauses for a random count within `pauses`, then stamps and sends each
// message. Returns the sum of sent timestamps and the failed credit checks.
#[inline(never)]
fn send<const B: usize>(
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
        let gap = pauses[0] + ((u64::from(random(&mut state)) * width) >> 32) as u32;
        for _ in 0..gap {
            spin_loop(); // PAUSE on x86-64
        }
        let message = &mut pool[index % POOL];
        let start = now();
        write_timestamp(&mut message.0, start);
        tx.send(black_box(message));
        stamps = stamps.wrapping_add(start.0);
    });
    (stamps, waits)
}

// Returns the sum of received timestamps.
#[inline(never)]
fn receive<const B: usize>(
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
            let start = read_timestamp(&black_box(message).0);
            let end = now();
            latencies.record(start, end);
            stamps = stamps.wrapping_add(start.0);
            received += 1;
        });
        if received < count {
            // A productive drain also ends with an empty read. Pause before
            // retrying, as ConsumerBare::blocking_consume does on Empty.
            spin_loop();
        }
        if received != before {
            credit.release(received);
        }
    }
    assert_eq!(latencies.len(), count as u64, "one latency per message");
    stamps
}

pub fn run<const B: usize>(
    sender: impl Tx<B>,
    receiver: impl Rx<B>,
    settings: &Settings,
    seed: u32,
    check: impl FnMut(usize),
) -> Latencies {
    // Stamp the source pool in place rather than preparing a separate message.
    let mut pool = source_pool::<B>();
    let counts = [WARMUP, settings.messages];
    let (produced, (consumed, latencies)) = workers(
        sender,
        receiver,
        settings,
        &counts,
        check,
        |sender, rounds| {
            counts.map(|count| {
                rounds.run(Role::Producer, |credit| {
                    let start = time::Instant::now();
                    let (stamps, waits) =
                        send(sender, &mut pool, count, credit, seed, settings.pauses);
                    (start, stamps, waits)
                })
            })
        },
        |receiver, rounds| {
            let mut latencies = Latencies::new();
            let consumed = counts.map(|count| {
                latencies.reset();
                rounds.run(Role::Consumer, |credit| {
                    let stamps = receive(receiver, count, credit, &mut latencies);
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
        "# latency Mm/s={:.6} overlapping_sends={} credit_waits={credit_waits}",
        settings.messages as f64 / end.duration_since(start).as_secs_f64() / 1e6,
        latencies.overlapping_sends
    );
    latencies
}

// Pooled message latencies of one case.
#[derive(Default)]
pub struct Summary {
    runs: usize,
    pooled: Option<Latencies>,
}

impl Summary {
    pub fn add(&mut self, name: &str, bytes: usize, run: usize, latencies: &Latencies) {
        print_csv("run", name, bytes, run, latencies);
        self.runs += 1;
        self.pooled.get_or_insert_with(Latencies::new).add(latencies);
    }

    pub fn print(&self, name: &str, bytes: usize) {
        if let Some(pooled) = &self.pooled {
            print_csv("summary", name, bytes, self.runs, pooled);
        }
    }
}

fn print_csv(kind: &str, name: &str, bytes: usize, n: usize, latencies: &Latencies) {
    let (mean, sd) = latencies.mean_and_stdev_ns();
    print!("{kind},{name},{bytes},{n},{},{mean:.3},{sd:.3}", latencies.len());
    for percent in PERCENTILES {
        print!(",{:.3}", latencies.quantile_ns(percent / 100.));
    }
    println!();
}

pub fn print_header(settings: &Settings) {
    println!(
        "# mode=latency: every message, first 8 bytes, RDTSCP + LFENCE, Flux tick conversion, pauses={}..={} consumer_empty_pauses=1",
        settings.pauses[0], settings.pauses[1]
    );
    let percentiles = PERCENTILES.map(|percent| format!(",p{percent}_ns")).concat();
    println!("# run,queue,bytes,run,messages,mean_ns,population_SD_ns{percentiles}");
    println!(
        "# summary,queue,bytes,n,messages,mean_ns,population_SD_ns{percentiles} (pooled messages)"
    );
}
