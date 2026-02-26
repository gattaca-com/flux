//! Discovery and inspection of shared memory segments via filesystem scanning.

pub mod cli;
pub mod inspect;
pub mod registry;

pub use cli::{clean, inspect, list_all, list_json, stats};
pub use flux_communication::is_pid_alive;
pub use inspect::{
    PidInfo, PoisonInfo, QueueStats, backing_file_size, format_bytes, scan_proc_fds,
};
pub use registry::{DiscoveredEntry, app_names, entry_visible, flink_reachable, scan_base_dir};
