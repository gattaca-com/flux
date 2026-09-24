// Saturated throughput: the producer sends as fast as credits allow, and the
// consumer reads the middle byte of each message.
use std::{hint::black_box, time::Instant};

use super::{
    Credit, Message, POOL, Role, Rx, Settings, Tx, WARMUP, produce, source_pool,
    window_stats::{Moments, WINDOW_MESSAGES, WindowSamples},
    workers,
};

#[inline(never)]
fn send<const B: usize>(tx: &mut impl Tx<B>, pool: &[Message<B>], count: usize, credit: &Credit) {
    produce(tx, count, credit, |tx, index| tx.send(black_box(&pool[index % POOL])));
}

#[inline(never)]
fn receive<const B: usize, const WINDOWS: bool>(
    rx: &mut impl Rx<B>,
    count: usize,
    credit: &Credit,
    windows: &mut WindowSamples,
) -> u64 {
    let mut received = 0;
    let mut sum = 0u64;
    let mut next_window = WINDOW_MESSAGES;
    windows.start();
    while received < count {
        let before = received;
        rx.drain(|message| {
            sum = sum.wrapping_add(u64::from(black_box(message).0[B / 2]));
            received += 1;
        });
        if received != before {
            credit.release(received);
            if WINDOWS && received >= next_window && received < count {
                // A drain cannot cross two thresholds: credits limit its size
                // to 512 messages, below the 16384-message sampling interval.
                assert!(received - next_window < WINDOW_MESSAGES);
                windows.record(received);
                next_window += WINDOW_MESSAGES;
            }
        }
    }
    assert_eq!(received, count, "completed message count");
    sum
}

// Returns millions of messages per second.
pub fn run<const B: usize>(
    sender: impl Tx<B>,
    receiver: impl Rx<B>,
    settings: &Settings,
    check: impl FnMut(usize),
) -> f64 {
    let pool = &source_pool::<B>();
    let counts = [WARMUP, settings.messages];
    let (starts, (ends, mut windows)) = workers(
        sender,
        receiver,
        settings,
        &counts,
        check,
        |sender, rounds| {
            counts.map(|count| {
                rounds.run(Role::Producer, |credit| {
                    let start = Instant::now();
                    send(sender, pool, count, credit);
                    start
                })
            })
        },
        |receiver, rounds| {
            // Allocated before timing; each round restarts the same storage.
            let mut windows = WindowSamples::new(WARMUP.max(settings.messages));
            let ends = counts.map(|count| {
                rounds.run(Role::Consumer, |credit| {
                    let sum = if settings.windows {
                        receive::<B, true>(receiver, count, credit, &mut windows)
                    } else {
                        receive::<B, false>(receiver, count, credit, &mut windows)
                    };
                    (Instant::now(), sum)
                })
            });
            (ends, windows)
        },
    );
    for (count, (_, sum)) in counts.into_iter().zip(ends) {
        let expected: u64 = (0..count).map(|n| u64::from(pool[n % POOL].0[B / 2])).sum();
        assert_eq!(sum, expected, "selected-byte checksum");
    }
    let (start, end) = (starts[1], ends[1].0);
    if settings.windows {
        windows.finish(settings.messages, start, end);
        windows.report(settings.messages, settings.window_samples);
    }
    settings.messages as f64 / end.duration_since(start).as_secs_f64() / 1e6
}

// Run-to-run throughput moments of one case.
#[derive(Default)]
pub struct Summary(Moments);

impl Summary {
    pub fn add(&mut self, name: &str, bytes: usize, run: usize, mmsg_s: f64) {
        println!("run,{name},{bytes},{run},{mmsg_s:.6}");
        self.0.add(mmsg_s);
    }

    pub fn print(&self, name: &str, bytes: usize) {
        let Moments { n, mean, .. } = self.0;
        if n > 0 {
            println!("summary,{name},{bytes},{n},{mean:.6},{:.6}", self.0.sample_sd());
        }
    }
}

pub fn print_header(settings: &Settings) {
    println!(
        "# mode=throughput windows={} messages_per_window={WINDOW_MESSAGES} raw_samples={}",
        settings.windows, settings.window_samples
    );
    println!("# run,queue,bytes,run,Mm/s");
    println!("# summary,queue,bytes,n,mean_Mm/s,sample_SD");
}
