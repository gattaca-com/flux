use std::sync::OnceLock;

use flux_communication::queue::Producer;
use flux_utils::get_tid;

#[cfg(feature = "alloc-profile")]
use super::allocator::{self, AllocSample};
#[cfg(feature = "perf")]
use super::perf::{PerfSample, read};
use super::{mark::Mark, queue_dir::QUEUE_DIR};

thread_local! {
    static PRODUCERS: OnceLock<Producers> = const { OnceLock::new() };
}

pub(crate) struct Producers {
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

    #[inline]
    pub(crate) fn push(&self, mark: Mark) {
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
    let name = thread.name().map_or_else(|| format!("{:?}", thread.id()), ToOwned::to_owned);
    format!("{name}-{}", get_tid())
}

#[inline]
pub(crate) fn thread_producers() -> Option<&'static Producers> {
    QUEUE_DIR.get()?;
    PRODUCERS.with(|cell| {
        let producers = cell.get_or_init(Producers::for_current_thread);
        Some(unsafe { &*std::ptr::from_ref(producers) })
    })
}
