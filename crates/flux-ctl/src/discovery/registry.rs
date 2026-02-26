//! Filesystem-based discovery of shared memory segments.
//!
//! Replaces the old `ShmemRegistry`-based approach with direct filesystem
//! scanning.  Each segment is represented as a [`DiscoveredEntry`] — a plain
//! owned struct with no dependency on shared-memory–resident data structures.

use std::path::Path;

use flux_communication::{ShmemKind, array::ArrayHeader, queue::QueueHeader};
use shared_memory::ShmemConf;

// ─── DiscoveredEntry ────────────────────────────────────────────────────────

/// A shared-memory segment discovered by walking the filesystem.
#[derive(Debug, Clone)]
pub struct DiscoveredEntry {
    /// The kind of segment (Queue, Data, SeqlockArray).
    pub kind: ShmemKind,
    /// Application name (the directory name directly under `base_dir`).
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

// ─── Scanning ───────────────────────────────────────────────────────────────

/// Walk `base_dir/<app>/shmem/{queues,data,arrays}/<TypeName>` and return
/// a [`DiscoveredEntry`] for every flink whose backing shmem can still be
/// opened.
///
/// Stale flinks (where `ShmemConf::open` fails) are silently skipped.
pub fn scan_base_dir(base_dir: &Path) -> Vec<DiscoveredEntry> {
    let mut entries = Vec::new();

    let Ok(app_iter) = std::fs::read_dir(base_dir) else {
        return entries;
    };

    for dir_entry in app_iter.flatten() {
        let app_dir = dir_entry.path();
        if !app_dir.is_dir() {
            continue;
        }
        let app_name = dir_entry.file_name().to_string_lossy().to_string();
        let shmem_dir = app_dir.join("shmem");

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

                // Try to open the shmem — if the backing is gone, skip.
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
                    app_name: app_name.clone(),
                    type_name,
                    flink: flink_path.to_string_lossy().to_string(),
                    elem_size,
                    capacity,
                });
            }
        }
    }

    entries
}

// ─── Header reading helpers ─────────────────────────────────────────────────

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

// ─── Helpers ────────────────────────────────────────────────────────────────

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
