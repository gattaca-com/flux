use std::{collections::HashMap, fmt::Display, path::Path, sync::Mutex};

use flux_timing::{Duration, Instant, InternalMessage, Nanos};
use flux_utils::directories::{local_share_dir, shmem_dir_queues_with_base};
use once_cell::sync::Lazy;

use crate::queue::{Producer, Queue, QueueType};

/// A single timing interval measured on one machine.
///
/// `start_t` and `stop_t` must originate from the same clock source
/// and CPU socket. Cross-socket or cross-machine values are invalid.
///
/// # Warning!
/// TimingMessage may be used to display Nano deltas as 2 instants.
/// Absolute values have no meaning. Only deltas are valid.
/// Do not compare TimingMessage instances against each other.
#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
pub struct TimingMessage {
    pub start_t: Instant,
    pub stop_t: Instant,
}

impl TimingMessage {
    #[inline(always)]
    pub fn new() -> Self {
        Self { start_t: Instant::now(), stop_t: Default::default() }
    }

    pub fn elapsed(&self) -> Duration {
        Duration(self.stop_t.0.saturating_sub(self.start_t.0))
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        self.start_t.same_socket(&self.stop_t)
    }
}

impl From<TimingMessage> for Duration {
    fn from(value: TimingMessage) -> Self {
        value.elapsed()
    }
}

const QUEUE_SIZE: usize = 2usize.pow(13);

/// A reusable timing struct that emits timing intervals to queues:
/// - processing time (local business logic)
/// - latency (queued / on-the-wire)
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Timer {
    pub curmsg: TimingMessage,
    processing_time_producer: Producer<TimingMessage>,
    latency_producer: Producer<TimingMessage>,
}

impl Timer {
    pub fn new<A: AsRef<Path>, S: Display>(app_name: A, name: S) -> Self {
        Self::new_with_base_dir(local_share_dir(), app_name, name)
    }

    pub fn new_with_base_dir<D: AsRef<Path>, A: AsRef<Path>, S: Display>(
        base_dir: D,
        app_name: A,
        name: S,
    ) -> Self {
        let dirstr = shmem_dir_queues_with_base(base_dir, app_name).to_string_lossy().to_string();
        let _ = std::fs::create_dir_all(&dirstr);

        let file = format!("{dirstr}/timing-{name}");
        let timing_queue = Queue::create_or_open_shared(file, QUEUE_SIZE, QueueType::MPMC);

        let file = format!("{dirstr}/latency-{name}");
        let latency_queue = Queue::create_or_open_shared(file, QUEUE_SIZE, QueueType::MPMC);

        Timer {
            curmsg: Default::default(),
            processing_time_producer: Producer::from(timing_queue),
            latency_producer: Producer::from(latency_queue),
        }
    }
}

unsafe impl Send for Timer {}
unsafe impl Sync for Timer {}

impl Timer {
    #[inline]
    pub fn start_t(&self) -> &Instant {
        &self.curmsg.start_t
    }

    #[inline]
    pub fn elapsed(&self) -> Duration {
        self.curmsg.start_t.elapsed()
    }

    #[inline]
    pub fn stop_t(&self) -> Instant {
        self.curmsg.stop_t
    }

    #[inline]
    pub fn set_stop(&mut self, stop: Instant) {
        self.curmsg.stop_t = stop;
    }

    #[inline]
    pub fn set_start(&mut self, start: Instant) {
        self.curmsg.start_t = start;
    }

    #[inline]
    pub fn start(&mut self) {
        self.set_start(Instant::now());
    }

    #[inline]
    fn emit_latency(&mut self) {
        if self.curmsg.is_valid() {
            self.latency_producer.produce(&self.curmsg);
        }
    }

    #[inline]
    fn emit_processing(&mut self) {
        if self.curmsg.is_valid() {
            self.processing_time_producer.produce(&self.curmsg);
        }
    }

    /// Finish the current processing interval and emit processing time.
    #[inline]
    pub fn record_processing(&mut self) {
        self.set_stop(Instant::now());
        self.emit_processing();
    }

    /// Finish processing, then emit upstream latency.
    ///
    /// Processing interval:
    ///   [start_t, now]
    ///
    /// Latency interval:
    ///   [ingestion_t, start_t]
    #[inline]
    pub fn record_processing_and_latency_from(&mut self, ingestion_t: Instant) {
        self.record_processing();
        self.set_stop(self.curmsg.start_t);
        self.set_start(ingestion_t);
        self.emit_latency();
    }

    /// Finish processing, then emit latency until now.
    ///
    /// Processing interval:
    ///   [start_t, now]
    ///
    /// Latency interval:
    ///   [ingestion_t, now]
    #[inline]
    pub fn record_processing_and_latency_until_now(&mut self, ingestion_t: Instant) {
        self.record_processing();
        self.set_start(ingestion_t);
        self.emit_latency();
    }

    /// Emit a pure latency measurement.
    ///
    /// Latency interval:
    ///   [ingestion_t, now]
    #[inline]
    pub fn record_latency_until_now(&mut self, ingestion_t: Instant) {
        let m = TimingMessage { start_t: ingestion_t, stop_t: Instant::now() };
        if m.is_valid() {
            self.latency_producer.produce(&m);
        }
    }

    /// Reset the timer into accumulation mode.
    ///
    /// After making this call user should makes call to `accumulate` to start
    /// accumulating total processing time through `stop_t`.
    /// Once all calls are finished call `emit_accumulated_processing` to send
    /// the batched processing time.
    #[inline]
    pub fn start_accumulate(&mut self) {
        self.curmsg.start_t = Instant::ZERO;
        self.curmsg.stop_t = Instant::ZERO
    }

    #[inline]
    pub fn accumulate(&mut self, duration: Duration) {
        self.curmsg.stop_t += duration
    }

    #[inline]
    pub fn emit_accumulated_processing(&mut self) {
        self.emit_processing()
    }

    /// Emit a synthetic processing interval derived from a nanos delta.
    #[inline]
    pub fn emit_processing_from_nanos(&mut self, start: Nanos, end: Nanos) {
        let delta = end.saturating_sub(start);
        let rdtsc_dur = Duration::from(delta);
        self.set_start(Instant(0));
        self.set_stop(Instant(rdtsc_dur.0));
        self.emit_processing();
    }

    /// Emit a synthetic latency interval derived from a nanos delta.
    #[inline]
    pub fn emit_latency_from_nanos(&mut self, start: Nanos, end: Nanos) {
        let delta = end.saturating_sub(start);
        let rdtsc_dur = Duration::from(delta);
        self.set_start(Instant(0));
        self.set_stop(Instant(rdtsc_dur.0));
        self.emit_latency();
    }

    #[inline]
    pub fn process<T, R>(
        &mut self,
        msg: InternalMessage<T>,
        mut f: impl FnMut(InternalMessage<T>) -> R,
    ) -> R {
        self.start();
        let in_t = (&msg).into();
        let o = f(msg);
        self.record_processing_and_latency_from(in_t);
        o
    }

    #[inline]
    pub fn time<R>(&mut self, f: impl FnOnce() -> R) -> R {
        self.start();
        let o = f();
        self.record_processing();
        o
    }
}

/// Used by timeit macro
#[allow(dead_code)]
pub static TIMERS: Lazy<Mutex<HashMap<&'static str, Timer>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Macro to be used when quickly benchmarking some piece of code, should not
/// remain in prod as it is not particularly performant
#[macro_export]
macro_rules! timeit {
    ($name:expr, $block:block) => {{
        use common::time::timer::TIMERS;
        let mut timer = {
            let mut timers = TIMERS.lock().unwrap();
            timers.entry($name).or_insert_with(|| common::time::Timer::new($name)).clone()
        };

        timer.start();
        let result = { $block };
        timer.stop();

        result
    }};
}
