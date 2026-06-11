use std::{fmt::Display, path::Path};

use flux_communication::shmem_dir_queues_string_with_base;
use flux_timing::{IngestionTime, Instant, Nanos, global_clock_not_mocked};

use crate::communication::queue::{Producer, Queue, QueueType};
#[cfg(feature = "instr-count")]
use crate::tile::instr::HwCounter;

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

/// Hardware-counter stats over a sampling window.
///
/// Emitted on a separate `tileperf-{tile}` queue so perf builds do not
/// change the `TileSample` layout. Type is unconditional (readers need
/// it); collection and queue creation are gated behind `instr-count`.
#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
#[cfg_attr(feature = "wincode", derive(wincode_derive::SchemaRead, wincode_derive::SchemaWrite))]
pub struct TilePerfSample {
    /// Instructions retired over the window (userspace only).
    pub instr_sum: u64,
    instr_min: u64,
    pub instr_max: u64,
    /// Instructions retired in busy-classified iterations only.
    pub instr_busy_sum: u64,
    /// CPU cycles over the window (userspace only).
    pub cycles_sum: u64,
    /// CPU cycles in busy-classified iterations only.
    pub cycles_busy_sum: u64,
    /// Diagnostic: the running idle floor at window emit, and how many
    /// loops classified busy — exposes classifier behaviour (floor
    /// collapse → everything busy; margin too wide → nothing busy).
    pub idle_floor: u64,
    pub busy_loops: u32,
    /// Loops the `did_work` flag (consume/produce auto-marking + manual
    /// `mark_work`) reported busy — compare against `busy_loops`.
    pub flag_loops: u32,
    pub loop_count: u32,
}

impl TilePerfSample {
    #[inline]
    fn reset(&mut self) {
        *self = Self { instr_min: u64::MAX, ..Default::default() };
    }

    /// Smallest per-loop instruction count in the window — the idle poll
    /// path is near-deterministic, so this is the idle baseline.
    #[inline]
    pub fn instr_min(&self) -> u64 {
        if self.instr_min == u64::MAX { 0 } else { self.instr_min }
    }
}

/// Counters + window state for the perf side-queue. Exists only when the
/// instruction counter opened successfully.
#[cfg(feature = "instr-count")]
#[derive(Clone, Copy, Debug)]
struct TilePerf {
    instr: HwCounter,
    cycles: Option<HwCounter>,
    instr_begin: u64,
    cycles_begin: u64,
    /// Running min of per-loop instruction deltas — the idle poll path is
    /// near-deterministic and work loops always retire more (poll + work),
    /// so this tracks the idle baseline. Decays upward per window (see
    /// `emit_and_reset`) so one atypically short loop cannot poison
    /// classification permanently.
    idle_floor: u64,
    sample: TilePerfSample,
    producer: Producer<TilePerfSample>,
}

#[cfg(feature = "instr-count")]
impl TilePerf {
    #[inline]
    fn begin(&mut self) {
        self.instr_begin = self.instr.read();
        if let Some(c) = &self.cycles {
            self.cycles_begin = c.read();
        }
    }

    /// Read counter deltas and classify the loop: busy when instructions
    /// exceed the idle floor by 50%. Drives the instr/cycles busy sums
    /// only; `did_work` (passed for the `flag_loops` diagnostic) keeps
    /// driving the wall-time busy stats in `TileSample`.
    #[inline]
    fn record(&mut self, did_work: bool) {
        let delta = self.instr.read().saturating_sub(self.instr_begin);

        if delta < self.idle_floor {
            self.idle_floor = delta;
        }
        let busy = delta > self.idle_floor.saturating_add(self.idle_floor / 2);

        let s = &mut self.sample;
        s.instr_sum += delta;
        if busy {
            s.instr_busy_sum += delta;
            s.busy_loops += 1;
        }
        if delta < s.instr_min {
            s.instr_min = delta;
        }
        if delta > s.instr_max {
            s.instr_max = delta;
        }

        if let Some(c) = &self.cycles {
            let delta = c.read().saturating_sub(self.cycles_begin);
            s.cycles_sum += delta;
            if busy {
                s.cycles_busy_sum += delta;
            }
        }
        if did_work {
            s.flag_loops += 1;
        }
        s.loop_count += 1;
    }

    #[inline]
    fn emit_and_reset(&mut self) {
        self.sample.idle_floor = self.idle_floor;
        // Decay the floor 12.5% per window, re-clamped by the window min:
        // a collapsed floor recovers within a few windows instead of
        // misclassifying every idle loop busy forever.
        self.idle_floor =
            self.sample.instr_min.min(self.idle_floor.saturating_add(self.idle_floor / 8));
        self.producer.produce(&self.sample);
        self.sample.reset();
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
    #[cfg(feature = "instr-count")]
    perf: Option<TilePerf>,
}

impl TileMetrics {
    pub fn new<D: AsRef<Path>, A: AsRef<Path>, S: Display>(
        base_dir: D,
        app_name: A,
        tile_name: S,
    ) -> Self {
        let dirstr = shmem_dir_queues_string_with_base(base_dir, &app_name);
        let _ = std::fs::create_dir_all(&dirstr);

        let file = format!("{dirstr}/tilemetrics-{tile_name}");
        let queue = Queue::create_or_open_shared(file, QUEUE_SIZE, QueueType::SPMC);

        Self {
            latest_begin: Instant::default(),
            sample: TileSample { busy_min: u64::MAX, ..Default::default() },
            producer: Producer::from(queue),
            #[cfg(feature = "instr-count")]
            perf: None,
        }
    }

    /// Path of the perf side-queue, alongside `tilemetrics-{tile}`.
    #[cfg(feature = "instr-count")]
    pub fn perf_file<D: AsRef<Path>, A: AsRef<Path>, S: Display>(
        base_dir: D,
        app_name: A,
        tile_name: S,
    ) -> String {
        let dirstr = shmem_dir_queues_string_with_base(base_dir, &app_name);
        format!("{dirstr}/tileperf-{tile_name}")
    }

    /// Open the perf counters and the `tileperf` queue. perf binds pid=0
    /// events to the opening thread — call from the tile thread, not the
    /// spawner. No-op (no queue created) if perf is unavailable.
    #[cfg(feature = "instr-count")]
    pub fn init_hw_counters(&mut self, perf_file: &str) {
        let Some(instr) = HwCounter::instructions() else { return };
        let queue: Queue<TilePerfSample> =
            Queue::create_or_open_shared(perf_file, QUEUE_SIZE, QueueType::SPMC);
        self.perf = Some(TilePerf {
            instr,
            cycles: HwCounter::cycles(),
            instr_begin: 0,
            cycles_begin: 0,
            idle_floor: u64::MAX,
            sample: TilePerfSample { instr_min: u64::MAX, ..Default::default() },
            producer: Producer::from(queue),
        });
    }

    #[inline]
    pub fn begin(&mut self, now: IngestionTime) {
        self.latest_begin = now.internal();
        if self.sample.loop_count == 0 {
            self.sample.window_start = now.real();
        }
        #[cfg(feature = "instr-count")]
        if let Some(p) = &mut self.perf {
            p.begin();
        }
    }

    #[inline]
    pub fn end(&mut self, did_work: bool) {
        #[cfg(feature = "instr-count")]
        if let Some(p) = &mut self.perf {
            p.record(did_work);
        }

        if did_work {
            let ticks = Instant::now().0.saturating_sub(self.latest_begin.0);
            // Convert TSC ticks → nanoseconds so busy_ticks is in the same
            // unit as total_ticks() (which is derived from Nanos).
            let nanos = global_clock_not_mocked().delta_as_nanos(0, ticks);

            self.sample.busy_ticks += nanos;
            self.sample.busy_sum += nanos;
            self.sample.busy_count += 1;

            if nanos < self.sample.busy_min {
                self.sample.busy_min = nanos;
            }
            if nanos > self.sample.busy_max {
                self.sample.busy_max = nanos;
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
        #[cfg(feature = "instr-count")]
        if let Some(p) = &mut self.perf {
            p.emit_and_reset();
        }
    }
}
