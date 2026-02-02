use std::fmt::Display;

use flux_communication::shmem_dir_queues_string;
use flux_timing::{IngestionTime, Instant, Nanos};

use crate::communication::queue::{Producer, Queue, QueueType};

const QUEUE_SIZE: usize = 4096;
const SAMPLE_WINDOW: u32 = 1024;

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
}

impl TileMetrics {
    pub fn new<A: AsRef<str>, S: Display>(app_name: A, tile_name: S) -> Self {
        let dirstr = shmem_dir_queues_string(&app_name);
        let _ = std::fs::create_dir_all(&dirstr);

        let file = format!("{dirstr}/tilemetrics-{tile_name}");
        let queue = Queue::create_or_open_shared(file, QUEUE_SIZE, QueueType::SPMC);

        Self {
            latest_begin: Instant::default(),
            sample: TileSample { busy_min: u64::MAX, ..Default::default() },
            producer: Producer::from(queue),
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
        self.sample.window_end = Nanos::now();
        self.producer.produce(&self.sample);
        self.sample.reset();
    }
}
