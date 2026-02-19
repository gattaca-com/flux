use std::{fmt::Display, path::Path};

use flux_communication::shmem_dir_queues_string;
use flux_timing::{Duration, IngestionTime, Instant, Nanos, Repeater};

use crate::communication::queue::{Producer, Queue, QueueType};

const QUEUE_SIZE: usize = 4096;
const SAMPLE_WINDOW: u32 = 1024;

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
#[cfg_attr(feature = "wincode", derive(wincode_derive::SchemaRead, wincode_derive::SchemaWrite))]
pub struct TileRusage {
    pub user_micros: u64,
    pub system_micros: u64,
    pub max_rss: i64,
    pub minor_page_faults: i64,
    pub major_page_faults: i64,
    pub block_inputs: i64,
    pub block_outputs: i64,
    pub voluntary_ctx_switches: i64,
    pub involuntary_ctx_switches: i64,
}

/// Aggregated loop metrics over a sampling window
#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
#[cfg_attr(feature = "wincode", derive(wincode_derive::SchemaRead, wincode_derive::SchemaWrite))]
pub struct TileSample {
    pub window_start: Nanos,
    pub window_end: Nanos,
    pub busy_ticks: u64,
    busy_min: u64,
    pub busy_max: u64,
    pub busy_sum: u64,
    pub busy_count: u32,
    pub loop_count: u32,
    pub rusage: Option<TileRusage>,
}

impl TileSample {
    #[inline]
    fn reset(&mut self) {
        self.busy_ticks = 0;
        self.busy_min = u64::MAX;
        self.busy_max = 0;
        self.busy_sum = 0;
        self.busy_count = 0;
        self.loop_count = 0;
        self.rusage = None;
    }

    #[inline]
    pub fn total_ticks(&self) -> u64 {
        self.window_end.0.saturating_sub(self.window_start.0)
    }

    #[inline]
    pub fn idle_ticks(&self) -> u64 {
        self.total_ticks().saturating_sub(self.busy_ticks)
    }

    #[inline]
    pub fn utilisation(&self) -> f64 {
        let total = self.total_ticks();
        if total == 0 { 0.0 } else { self.busy_ticks as f64 / total as f64 }
    }

    #[inline]
    pub fn busy_avg(&self) -> u64 {
        if self.busy_count == 0 { 0 } else { self.busy_sum / self.busy_count as u64 }
    }

    /// `self.busy_min` will be `u64::MAX` if no entries.
    #[inline]
    pub fn busy_min(&self) -> u64 {
        if self.busy_count == 0 { 0 } else { self.busy_min }
    }
}

/// Per-tile loop instrumentation.
///
/// Emits a `TileSample` to `producer` every `SAMPLE_WINDOW` iterations.
/// Tracks busy time, idle time, and some per-work-iteration min/max/avg.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct TileMetrics {
    latest_begin: Instant,
    sample: TileSample,
    producer: Producer<TileSample>,
    rusage_repeater: Repeater,
}

impl TileMetrics {
    pub fn new<A: AsRef<Path>, S: Display>(app_name: A, tile_name: S) -> Self {
        let dirstr = shmem_dir_queues_string(&app_name);
        let _ = std::fs::create_dir_all(&dirstr);

        let file = format!("{dirstr}/tilemetrics-{tile_name}");
        let queue = Queue::create_or_open_shared(file, QUEUE_SIZE, QueueType::SPMC);

        Self {
            latest_begin: Instant::default(),
            sample: TileSample { busy_min: u64::MAX, ..Default::default() },
            producer: Producer::from(queue),
            rusage_repeater: Repeater::every(Duration::from_secs(1)),
        }
    }

    #[inline]
    pub fn begin(&mut self, now: IngestionTime) {
        self.latest_begin = now.internal();
        if self.sample.loop_count == 0 {
            self.sample.window_start = now.real();
        }
    }

    #[inline]
    pub fn end(&mut self, did_work: bool) {
        if did_work {
            let duration = Instant::now().0.saturating_sub(self.latest_begin.0);

            self.sample.busy_ticks += duration;
            self.sample.busy_sum += duration;
            self.sample.busy_count += 1;

            if duration < self.sample.busy_min {
                self.sample.busy_min = duration;
            }
            if duration > self.sample.busy_max {
                self.sample.busy_max = duration;
            }
        }

        self.sample.loop_count += 1;

        if self.sample.loop_count >= SAMPLE_WINDOW {
            self.emit_and_reset();
        }
    }

    #[inline]
    fn emit_and_reset(&mut self) {
        if self.rusage_repeater.fired() {
            self.sample.rusage = read_rusage();
        }
        self.sample.window_end = Nanos::now();
        self.producer.produce(&self.sample);
        self.sample.reset();
    }
}

#[cfg(unix)]
fn read_rusage() -> Option<TileRusage> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let who = libc::RUSAGE_THREAD;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let who = libc::RUSAGE_SELF;

    let rc = unsafe { libc::getrusage(who, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }

    let usage = unsafe { usage.assume_init() };
    Some(TileRusage {
        user_micros: timeval_to_micros(usage.ru_utime),
        system_micros: timeval_to_micros(usage.ru_stime),
        max_rss: usage.ru_maxrss as i64,
        minor_page_faults: usage.ru_minflt as i64,
        major_page_faults: usage.ru_majflt as i64,
        block_inputs: usage.ru_inblock as i64,
        block_outputs: usage.ru_oublock as i64,
        voluntary_ctx_switches: usage.ru_nvcsw as i64,
        involuntary_ctx_switches: usage.ru_nivcsw as i64,
    })
}

#[cfg(not(unix))]
fn read_rusage() -> Option<TileRusage> {
    None
}

#[cfg(unix)]
#[inline]
fn timeval_to_micros(tv: libc::timeval) -> u64 {
    let secs = i128::from(tv.tv_sec);
    let micros = i128::from(tv.tv_usec);
    if secs < 0 || micros < 0 {
        return 0;
    }

    secs.saturating_mul(1_000_000).saturating_add(micros).try_into().unwrap_or(u64::MAX)
}
