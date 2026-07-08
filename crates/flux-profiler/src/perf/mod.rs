//! Hardware performance counters for `#[timed]`.
//!
//! The hot path is deliberately dumb: read the opened counters into a fixed
//! [`PerfSample`] that rides alongside each mark; all labelling and derived
//! ratios happen in postprocessing.
//!
//! Layers (read top-down):
//! - [`events`] — name → `perf_event_open` `(type, config)` for this CPU; the
//!   single place to add a metric. Pure logic, no I/O on the hot path.
//! - [`sample`] — [`PerfSample`], the raw value record.
//! - [`source`] — per-thread rdpmc [`read`], feature-gated.
//! - `raw` — the rdpmc primitive (only compiled with the `perf` feature).
//!
//! Collection is gated behind the `perf` feature; without it `#[timed]` reads
//! no counters, and even with it [`read`] returns `None` when
//! `perf_event_paranoid` blocks the events — so `#[timed]` degrades to timing
//! only.

mod events;
#[cfg(feature = "perf")]
mod raw;
mod sample;
#[cfg(feature = "perf")]
mod source;

pub use events::Schema;
pub use sample::PerfSample;
#[cfg(feature = "perf")]
pub(super) use source::read;
