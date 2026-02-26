//! Discovery and inspection of shared memory segments via the global registry.
//!
//! Provides the backing logic for all `flux-ctl` CLI commands (`list`, `inspect`,
//! `clean`, `scan`) and the TUI data model. Opens the [`ShmemRegistry`], enriches
//! entries with queue stats, poison detection, and per-PID process info, and
//! exposes helpers for cleanup and liveness checks.
//!
//! # Submodules
//!
//! - [`registry`] — registry access, filesystem scanning, entry visibility
//! - [`inspect`] — PID info, poison detection, backing file size, queue stats
//! - [`cli`] — CLI command implementations (`list`, `inspect`, `clean`, `stats`)

pub mod cli;
pub mod inspect;
pub mod registry;

// Re-export all public items so callers can continue using `discovery::*`.
pub use cli::{clean, inspect, list_all, list_json, stats};
pub use flux_communication::registry::is_pid_alive;
pub use inspect::{
    PidInfo, PoisonInfo, QueueStats, backing_file_size, check_poison, format_bytes, pid_info,
    pids_info, read_queue_stats,
};
pub use registry::{
    app_names, entry_visible, flink_reachable, format_pids, open_registry, scan,
};
