//! Discovery and inspection of shared memory segments via filesystem scanning.
//!
//! Each segment is represented as a [`DiscoveredEntry`] — a plain owned struct
//! with no dependency on shared-memory–resident data structures.

pub mod cli;
pub mod inspect;

use std::path::{Path, PathBuf};

pub use cli::{clean, inspect, list_all, list_json, stats};
pub use flux_communication::is_pid_alive;
use flux_communication::{ShmemKind, array::ArrayHeader, queue::QueueHeader};
pub use inspect::{
    PidInfo, PoisonInfo, QueueStats, backing_file_size, format_bytes, scan_proc_fds,
};
use shared_memory::ShmemConf;

/// A shared-memory segment discovered by walking the filesystem.
#[derive(Debug, Clone)]
pub struct DiscoveredEntry {
    /// The kind of segment (Queue, Data, SeqlockArray).
    pub kind: ShmemKind,
    /// Application name — the immediate parent of the `shmem/` directory.
    pub app_name: String,
    /// Type name (the flink filename, which is the short type name).
    pub type_name: String,
    /// Absolute flink path on disk.
    pub flink: String,
    /// Element size in bytes (from the header, or `shmem.len()` for Data).
    pub elem_size: usize,
    /// Number of slots (mask+1 for queues, bufsize for arrays, 1 for data).
    pub capacity: usize,
}

/// Recursively walk `base_dir` looking for `shmem/{queues,data,arrays}/`
/// directories at any depth.  For each one found, the immediate parent of
/// the `shmem/` directory is used as the application name.
///
/// Returns a [`DiscoveredEntry`] for every flink whose backing shmem can
/// still be opened.  Stale flinks are silently skipped.
pub fn scan_base_dir(base_dir: &Path) -> Vec<DiscoveredEntry> {
    let mut entries = Vec::new();
    let mut shmem_dirs: Vec<(String, PathBuf)> = Vec::new();
    find_shmem_dirs(base_dir, &mut shmem_dirs);

    for (app_name, shmem_dir) in &shmem_dirs {
        collect_entries(app_name, shmem_dir, &mut entries);
    }

    entries
}

/// DFS through `dir` looking for directories named `shmem` that contain at
/// least one of `queues`, `data`, or `arrays`.  Pushes `(app_name, shmem_path)`
/// pairs and does **not** recurse below a matched `shmem/` directory.
///
/// `app_name` is the relative path from `base_dir` to the parent of `shmem/`,
/// e.g. `gattaca/LOCAL_LOUIS/builder` for a `shmem/` found at
/// `<base_dir>/gattaca/LOCAL_LOUIS/builder/shmem/`.
pub(crate) fn find_shmem_dirs(base_dir: &Path, out: &mut Vec<(String, PathBuf)>) {
    find_shmem_dirs_inner(base_dir, base_dir, out);
}

fn find_shmem_dirs_inner(base_dir: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) {
    let Ok(iter) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in iter.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if entry.file_name() == "shmem" && is_shmem_root(&path) {
            let app_name = dir.strip_prefix(base_dir).unwrap_or(dir).to_string_lossy().to_string();
            out.push((app_name, path));
        } else {
            find_shmem_dirs_inner(base_dir, &path, out);
        }
    }
}

/// Returns `true` when `dir` contains at least one of the expected shmem
/// sub-directories (`queues`, `data`, `arrays`).
fn is_shmem_root(dir: &Path) -> bool {
    dir.join("queues").is_dir() || dir.join("data").is_dir() || dir.join("arrays").is_dir()
}

/// Read all flinks under a single `shmem/` directory and append entries.
fn collect_entries(app_name: &str, shmem_dir: &Path, entries: &mut Vec<DiscoveredEntry>) {
    for (subdir, kind) in [
        ("queues", ShmemKind::Queue),
        ("data", ShmemKind::Data),
        ("arrays", ShmemKind::SeqlockArray),
    ] {
        let type_dir = shmem_dir.join(subdir);
        let Ok(flink_iter) = std::fs::read_dir(&type_dir) else {
            continue;
        };
        for flink_entry in flink_iter.flatten() {
            let flink_path = flink_entry.path();
            if flink_path.is_dir() {
                continue;
            }
            let type_name = flink_entry.file_name().to_string_lossy().to_string();

            let Ok(shmem) = ShmemConf::new().flink(&flink_path).open() else {
                continue;
            };

            let (elem_size, capacity) = match kind {
                ShmemKind::Queue => read_queue_meta(&shmem),
                ShmemKind::SeqlockArray => read_array_meta(&shmem),
                ShmemKind::Data => (shmem.len(), 1),
                ShmemKind::Unknown => (0, 0),
            };

            entries.push(DiscoveredEntry {
                kind,
                app_name: app_name.to_owned(),
                type_name,
                flink: flink_path.to_string_lossy().to_string(),
                elem_size,
                capacity,
            });
        }
    }
}

fn read_queue_meta(shmem: &shared_memory::Shmem) -> (usize, usize) {
    if shmem.len() < std::mem::size_of::<QueueHeader>() {
        return (0, 0);
    }
    let header = unsafe { &*(shmem.as_ptr() as *const QueueHeader) };
    if !header.is_initialized() || header.elsize == 0 {
        return (0, 0);
    }
    (header.elsize, header.mask + 1)
}

fn read_array_meta(shmem: &shared_memory::Shmem) -> (usize, usize) {
    if shmem.len() < std::mem::size_of::<ArrayHeader>() {
        return (0, 0);
    }
    let header = unsafe { &*(shmem.as_ptr() as *const ArrayHeader) };
    if !header.is_initialized() || header.elsize == 0 {
        return (0, 0);
    }
    (header.elsize, header.bufsize)
}

/// Return sorted, deduplicated application names from discovered entries.
pub fn app_names(entries: &[DiscoveredEntry]) -> Vec<String> {
    let mut names: Vec<String> = entries.iter().map(|e| e.app_name.clone()).collect();
    names.sort();
    names.dedup();
    names
}

/// Check whether a flink's backing file exists and is non-empty.
///
/// Returns `false` for empty flink strings or missing/empty files.
/// This is a lightweight filesystem-only check — it does **not** open or
/// mmap the shared memory segment.
pub fn flink_reachable(flink: &str) -> bool {
    if flink.is_empty() {
        return false;
    }
    std::fs::metadata(flink).map(|m| m.len() > 0).unwrap_or(false)
}

/// An entry is visible if its backing shmem flink still exists on disk.
///
/// Since `scan_base_dir` already filters out entries whose shmem cannot be
/// opened, this is a secondary liveness check for entries that may have
/// become stale since the last scan.
pub fn entry_visible(entry: &DiscoveredEntry) -> bool {
    flink_reachable(&entry.flink)
}
