use std::sync::OnceLock;

use flux_communication::queue::Producer;

#[cfg(feature = "alloc-profile")]
use super::allocator::{self, AllocSample};
#[cfg(feature = "perf")]
use super::perf::{PerfSample, read};
use super::{mark::Mark, queue_dir::QUEUE_DIR};

thread_local! {
    static PRODUCERS: OnceLock<Producers> = const { OnceLock::new() };
}

struct Producers {
    marks: Producer<Mark>,
    #[cfg(feature = "perf")]
    perf: Producer<PerfSample>,
    #[cfg(feature = "alloc-profile")]
    alloc: Producer<AllocSample>,
}

impl Producers {
    fn for_current_thread() -> Self {
        let dir = QUEUE_DIR.get().expect("enable_profiler locks the queue dir before production");
        let token = thread_token();
        Self {
            marks: Producer::from(dir.ring::<Mark>(&token)),
            #[cfg(feature = "perf")]
            perf: Producer::from(dir.ring::<PerfSample>(&token)),
            #[cfg(feature = "alloc-profile")]
            alloc: Producer::from(dir.ring::<AllocSample>(&token)),
        }
    }

    fn push(&self, mark: Mark) {
        // Rings arrive freshly created (never adopted from a crashed writer),
        // so the per-call unpoison pass of plain `produce` is unnecessary.
        self.marks.produce_without_first(&mark);
        #[cfg(feature = "perf")]
        self.perf.produce_without_first(&read().unwrap_or_default());
        #[cfg(feature = "alloc-profile")]
        self.alloc.produce_without_first(&allocator::read());
    }
}

fn thread_token() -> String {
    let thread = std::thread::current();
    thread.name().map_or_else(|| format!("{:?}", thread.id()), ToOwned::to_owned)
}

#[inline]
fn record(mark: Mark) {
    PRODUCERS.with(|cell| cell.get_or_init(Producers::for_current_thread).push(mark));
}

#[inline]
pub(crate) fn record_open(name: &'static str) {
    record(Mark::open(name));
}

#[inline]
pub(crate) fn record_close(name: &'static str) {
    record(Mark::close(name));
}
