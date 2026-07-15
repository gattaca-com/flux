use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use super::{
    drainer::EventsDrainer,
    perf::Schema,
    queue_dir::{QUEUE_DIR, QueueDir, enable_profiler},
    symbols::{CrossProcessSymbolsResolver, InProcessSymbolsResolver},
};

pub struct CrossProcessReader {
    drainer: EventsDrainer,
    resolver: CrossProcessSymbolsResolver,
    pid: u32,
}

impl CrossProcessReader {
    pub fn attach(app: &str) -> Option<Self> {
        let dir = QueueDir::new(app);
        let pid = dir.live_pid()?;
        let schema = dir.perf_schema().unwrap_or_else(Schema::empty);
        Some(Self {
            drainer: EventsDrainer::new(dir, schema),
            resolver: CrossProcessSymbolsResolver::new(pid),
            pid,
        })
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn poll(&mut self) {
        self.drainer.poll(&self.resolver);
    }

    pub fn events(&self) -> &EventsDrainer {
        &self.drainer
    }
}

pub struct InProcessReader {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<EventsDrainer>,
}

impl InProcessReader {
    pub fn start() -> Self {
        enable_profiler("local-profiler");
        let dir = QUEUE_DIR.get().expect("enable_profiler locked it").clone();
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let stop = stop.clone();
            thread::Builder::new()
                .name("flamegraph-reader".to_owned())
                .spawn(move || Self::run(&stop, dir))
                .expect("spawn flamegraph reader")
        };
        Self { stop, handle }
    }

    pub fn collect(self) -> EventsDrainer {
        self.stop.store(true, Ordering::Release);
        self.handle.join().unwrap_or_else(|_| {
            let dir = QUEUE_DIR.get().expect("start locked it").clone();
            EventsDrainer::new(dir, Schema::local().clone())
        })
    }

    fn run(stop: &AtomicBool, dir: QueueDir) -> EventsDrainer {
        let mut drainer = EventsDrainer::new(dir, Schema::local().clone());
        loop {
            let more = drainer.poll(&InProcessSymbolsResolver);
            let stopping = stop.load(Ordering::Acquire);
            if stopping && !more {
                break;
            }
            if !more {
                thread::sleep(Duration::from_millis(1));
            }
        }
        drainer
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::significant_drop_tightening)] // RAII guards held to scope end
    use std::thread;

    use super::*;
    use crate::{mark::Mark, producer, queue_dir::RING_CAPACITY, test_shmem::ShmemGuard};

    fn record_open(name: &'static str) {
        producer::thread_producers().expect("profiler enabled").push(Mark::open(name));
    }

    fn record_close(name: &'static str) {
        producer::thread_producers().expect("profiler enabled").push(Mark::close(name));
    }

    fn open_frames(events: &EventsDrainer, name: &str) -> usize {
        events
            .threads()
            .flat_map(|t| t.marks.iter().filter(|m| m.is_open()))
            .filter(|m| events.meta().names.get(&m.id).is_some_and(|n| n == name))
            .count()
    }

    fn lossy(events: &EventsDrainer) -> bool {
        events.threads().any(|t| t.loss.is_lossy())
    }

    #[test]
    fn drain_discovers_every_thread_ring() {
        let _guard = ShmemGuard::new();
        let reader = InProcessReader::start();

        let spawn = |tag: &'static str, reps: usize| {
            thread::Builder::new()
                .name(format!("drainer-{tag}"))
                .spawn(move || {
                    let (outer, inner) = ("outer", "inner");
                    for _ in 0..reps {
                        record_open(outer);
                        record_open(inner);
                        record_close(inner);
                        record_close(outer);
                    }
                })
                .unwrap()
        };

        spawn("a", 3).join().unwrap();
        spawn("b", 5).join().unwrap();

        let events = reader.collect();
        assert!(!lossy(&events), "test rings are larger than the few marks produced");
        assert_eq!(open_frames(&events, "outer"), 8, "both threads' outer frames");
        assert_eq!(open_frames(&events, "inner"), 8, "both threads' inner frames");
    }

    #[test]
    fn same_named_threads_do_not_share_a_ring() {
        use std::sync::{Arc, Barrier};

        let _guard = ShmemGuard::new();
        let reader = InProcessReader::start();

        let barrier = Arc::new(Barrier::new(2));
        let spawn = |reps: usize| {
            let barrier = barrier.clone();
            thread::Builder::new()
                .name("dup".to_owned())
                .spawn(move || {
                    for _ in 0..reps {
                        record_open("work");
                        record_close("work");
                    }
                    barrier.wait();
                })
                .unwrap()
        };

        let a = spawn(3);
        let b = spawn(5);
        a.join().unwrap();
        b.join().unwrap();

        let events = reader.collect();
        assert!(!lossy(&events));
        assert_eq!(events.threads().count(), 2, "one ring per thread despite the shared name");
        assert!(events.threads().all(|t| t.name == "dup"), "tid is stripped back off for display");
        let mut tids: Vec<u64> = events.threads().map(|t| t.tid).collect();
        tids.sort_unstable();
        tids.dedup();
        assert_eq!(tids.len(), 2, "each ring carries its own tid");
        assert_eq!(open_frames(&events, "work"), 8, "3 + 5 opens across both rings");
    }

    #[test]
    fn flamegraph_reader_resolves_names_cross_process() {
        let guard = ShmemGuard::new();
        enable_profiler("test");

        thread::Builder::new()
            .name("remote-producer".to_owned())
            .spawn(|| {
                let (alpha, beta) = ("alpha", "beta");
                for _ in 0..4 {
                    record_open(alpha);
                    record_open(beta);
                    record_close(beta);
                    record_close(alpha);
                }
            })
            .unwrap()
            .join()
            .unwrap();

        let mut reader = CrossProcessReader::attach(guard.app()).expect("pid published");
        reader.poll();

        assert!(!lossy(reader.events()));
        assert_eq!(open_frames(reader.events(), "alpha"), 4, "name resolved from on-disk binary");
        assert_eq!(open_frames(reader.events(), "beta"), 4);

        reader.poll();
        reader.poll();
        assert_eq!(open_frames(reader.events(), "alpha"), 4, "re-poll of a static ring grew it");
        assert_eq!(open_frames(reader.events(), "beta"), 4);
    }

    #[test]
    fn exports_fxt_trace() {
        let guard = ShmemGuard::new();
        enable_profiler("test");

        thread::Builder::new()
            .name("trace-producer".to_owned())
            .spawn(|| {
                let (outer, inner) = ("outer", "inner");
                record_open(outer);
                record_open(inner);
                record_close(inner);
                record_close(outer);
            })
            .unwrap()
            .join()
            .unwrap();

        let mut reader = CrossProcessReader::attach(guard.app()).expect("pid published");
        reader.poll();

        let trace = reader.events().fxt_trace();
        assert_eq!(&trace[..8], b"\x10\x00\x04FxT\x16\x00", "FXT magic record");
        assert!(contains(&trace, b"trace-producer"), "track named after the thread");
        assert!(contains(&trace, b"outer") && contains(&trace, b"inner"), "frame names present");
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    #[test]
    fn overrun_is_reported_as_missed_events() {
        let guard = ShmemGuard::new();
        enable_profiler("test");

        thread::Builder::new()
            .name("overrun-producer".to_owned())
            .spawn(|| {
                for _ in 0..RING_CAPACITY {
                    record_open("work");
                    record_close("work");
                }
            })
            .unwrap()
            .join()
            .unwrap();

        let mut reader = CrossProcessReader::attach(guard.app()).expect("pid published");
        reader.poll();

        assert!(lossy(reader.events()), "the loss must be reported");
        assert_eq!(
            open_frames(reader.events(), "work"),
            0,
            "no samples fabricated from the lost prefix"
        );
    }
}
