//! Capture `#[timed]` call trees into per-thread shmem rings and read them
//! back — in-process ([`InProcessReader`]) or from a running producer
//! ([`CrossProcessReader`]) — as sample-aligned event streams and FXT traces.

extern crate self as flux_profiler;

pub mod allocator;
mod drainer;
mod fxt;
mod mark;
pub mod perf;
mod producer;
mod queue_dir;
mod reader;
mod ring_drainer;
mod socket_clock;
mod symbols;
mod timing;

pub use drainer::{EventsDrainer, FlamegraphMeta, Loss, ThreadEvents};
pub use flux_profiler_macros::timed;
pub use mark::Mark;
#[cfg(any(test, feature = "test-util"))]
pub use queue_dir::test_shmem;
pub use queue_dir::{enable_profiler, live_apps, published_pid};
pub use reader::{CrossProcessReader, InProcessReader};
pub use timing::TimerGuard;
