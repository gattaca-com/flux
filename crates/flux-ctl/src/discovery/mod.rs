//! Discovery and inspection of shared memory segments via filesystem scanning.
//!
//! Each segment is represented as a [`DiscoveredEntry`] — a plain owned struct
//! with no dependency on shared-memory–resident data structures.
//!
//! For long-lived consumers (e.g. the TUI), [`ShmemCache`] keeps segments
//! mapped across refresh ticks so that reading headers is a plain pointer
//! dereference — zero syscalls per tick.

pub mod cli;
pub mod inspect;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

pub use cli::{clean, inspect, list_all, list_json, stats};
pub use flux_communication::is_pid_alive;
use flux_communication::{ShmemKind, array::ArrayHeader, queue::QueueHeader};
pub use inspect::{
    PidInfo, PoisonInfo, QueueStats, backing_file_size, format_bytes, resolve_backing_path,
    scan_proc_fds,
};
use shared_memory::{Shmem, ShmemConf};

// ── DiscoveredEntry ──────────────────────────────────────────────────────

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
    /// Total writes to the queue (None for non-queue segments or failed reads).
    pub queue_writes: Option<usize>,
    /// Current write position within the ring buffer (count & mask).
    pub queue_fill: Option<usize>,
    /// Cached backing path in `/dev/shm/` (None if resolution failed).
    pub backing_path: Option<PathBuf>,
    /// Quick O(1) poison probe result from the initial shmem mapping.
    /// `Some(true)` = write-position slot has an odd seqlock version,
    /// `Some(false)` = even (clean), `None` = not applicable or couldn't
    /// check.
    pub poison_quick: Option<bool>,
}

// ── ShmemCache ───────────────────────────────────────────────────────────

/// A cached open shmem handle together with its static metadata.
///
/// The metadata fields (kind, app_name, …) are determined once when the
/// segment is first opened and never change.  Dynamic fields (queue_writes,
/// poison_quick, …) are re-read from the still-mapped memory on every
/// [`ShmemCache::read_entries`] call.
struct CachedHandle {
    shmem: Shmem,
    kind: ShmemKind,
    app_name: String,
    type_name: String,
    flink: String,
    elem_size: usize,
    capacity: usize,
    backing_path: Option<PathBuf>,
}

/// Keeps shmem segments mapped across refresh ticks so that per-tick reads
/// are plain pointer dereferences — zero `shm_open`/`mmap`/`munmap` syscalls.
///
/// Call [`refresh_handles`] periodically (e.g. every 10 s) to pick up new
/// segments and drop stale ones.  Call [`read_entries`] on every tick to
/// snapshot the live header data.
pub struct ShmemCache {
    /// Open handles keyed by flink path.
    handles: HashMap<String, CachedHandle>,
    /// Cached shmem directory listing.
    cached_dirs: Vec<(String, PathBuf)>,
    /// Last time the directory tree was walked.
    dirs_last_scan: Option<flux_timing::Instant>,
}

impl Default for ShmemCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ShmemCache {
    pub fn new() -> Self {
        Self { handles: HashMap::new(), cached_dirs: Vec::new(), dirs_last_scan: None }
    }

    /// High-level refresh: periodically rescan the directory tree (10 s TTL)
    /// and reconcile the open handle set.
    ///
    /// Between rescans, this is a no-op — [`read_entries`] reads from
    /// already-mapped memory.
    pub fn refresh(&mut self, base_dir: &Path) {
        let now = flux_timing::Instant::now();
        let should_rescan = self
            .dirs_last_scan
            .is_none_or(|last| now.elapsed_since(last) >= flux_timing::Duration::from_secs(10));

        if should_rescan {
            self.cached_dirs.clear();
            find_shmem_dirs(base_dir, &mut self.cached_dirs);
            self.dirs_last_scan = Some(now);
            self.refresh_handles(&self.cached_dirs.clone());
        }
    }

    /// Open new segments and drop stale ones.
    ///
    /// `shmem_dirs` is the output of [`find_shmem_dirs`].  Only flinks whose
    /// `/dev/shm/` backing file still exists are opened, avoiding the
    /// `shared_memory` crate's 5×50 ms retry loop on stale segments.
    pub fn refresh_handles(&mut self, shmem_dirs: &[(String, PathBuf)]) {
        // Collect all flink paths that currently exist on disk.
        let mut live_flinks: HashMap<String, (ShmemKind, &str, &Path)> = HashMap::new();
        for (app_name, shmem_dir) in shmem_dirs {
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
                    let flink_str = flink_path.to_string_lossy().to_string();
                    live_flinks.insert(flink_str, (kind, app_name.as_str(), shmem_dir.as_path()));
                }
            }
        }

        // Remove handles for flinks that no longer exist on disk.
        self.handles.retain(|flink, _| live_flinks.contains_key(flink));

        // Open handles for flinks we don't have yet.
        for (flink_str, (kind, app_name, _shmem_dir)) in &live_flinks {
            if self.handles.contains_key(flink_str) {
                continue;
            }

            let Some((os_id, backing_path)) = read_and_resolve_backing(flink_str) else {
                continue;
            };
            if !backing_path.exists() {
                continue;
            }

            let Ok(shmem) = ShmemConf::new().os_id(&os_id).open() else {
                continue;
            };

            let type_name = Path::new(flink_str)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            let (elem_size, capacity) = match kind {
                ShmemKind::Queue => {
                    if shmem.len() < std::mem::size_of::<QueueHeader>() {
                        continue;
                    }
                    let h = unsafe { &*(shmem.as_ptr() as *const QueueHeader) };
                    if !h.is_initialized() || h.elsize == 0 {
                        continue;
                    }
                    (h.elsize, h.mask + 1)
                }
                ShmemKind::SeqlockArray => {
                    let (es, cap) = read_array_meta(&shmem);
                    if es == 0 {
                        continue;
                    }
                    (es, cap)
                }
                ShmemKind::Data => (shmem.len(), 1),
                ShmemKind::Unknown => continue,
            };

            self.handles.insert(flink_str.clone(), CachedHandle {
                shmem,
                kind: *kind,
                app_name: app_name.to_string(),
                type_name,
                flink: flink_str.clone(),
                elem_size,
                capacity,
                backing_path: Some(backing_path),
            });
        }
    }

    /// Snapshot every cached segment's live header data into a
    /// [`DiscoveredEntry`].
    ///
    /// This is the hot path — no file I/O, just pointer reads from
    /// already-mapped memory.
    pub fn read_entries(&self) -> Vec<DiscoveredEntry> {
        self.handles
            .values()
            .map(|h| {
                let base = h.shmem.as_ptr();
                let shmem_len = h.shmem.len();

                let (queue_writes, queue_fill, poison_quick) = match h.kind {
                    ShmemKind::Queue => {
                        let header = unsafe { &*(base as *const QueueHeader) };
                        let writes = header.count.load(Ordering::Relaxed);
                        let fill = writes & header.mask;
                        let pq = quick_poison_queue(base, shmem_len);
                        (Some(writes), Some(fill), pq)
                    }
                    ShmemKind::SeqlockArray => {
                        let pq = quick_poison_array(base, shmem_len);
                        (None, None, pq)
                    }
                    _ => (None, None, None),
                };

                DiscoveredEntry {
                    kind: h.kind,
                    app_name: h.app_name.clone(),
                    type_name: h.type_name.clone(),
                    flink: h.flink.clone(),
                    elem_size: h.elem_size,
                    capacity: h.capacity,
                    queue_writes,
                    queue_fill,
                    backing_path: h.backing_path.clone(),
                    poison_quick,
                }
            })
            .collect()
    }
}

// ── Filesystem scanning (stateless, used by CLI and initial cache fill) ──

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

/// Like [`scan_base_dir`] but accepts pre-discovered shmem directories
/// (from a previous [`find_shmem_dirs`] call).
pub fn scan_base_dir_with_dirs(shmem_dirs: &[(String, PathBuf)]) -> Vec<DiscoveredEntry> {
    let mut entries = Vec::new();
    for (app_name, shmem_dir) in shmem_dirs {
        collect_entries(app_name, shmem_dir, &mut entries);
    }
    entries
}

/// Recursively walk `base_dir` collecting `(app_name, shmem_dir)` pairs.
pub(crate) fn find_shmem_dirs(base_dir: &Path, out: &mut Vec<(String, PathBuf)>) {
    find_shmem_dirs_inner(base_dir, base_dir, out);
}

fn find_shmem_dirs_inner(base_dir: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) {
    let Ok(read_dir) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in read_dir.flatten() {
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
///
/// For each flink we read the OS shmem id from the file, then check whether
/// the backing `/dev/shm/<id>` still exists before calling `ShmemConf::open`.
/// This avoids the `shared_memory` crate's 5×50 ms retry loop on stale
/// segments that would otherwise dominate startup time.
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

            // Read the OS shmem id from the flink file and derive the
            // backing path.  Skip stale flinks whose backing is gone —
            // this is a single stat() instead of ShmemConf's 5×50ms
            // retry loop.
            let flink_str = flink_path.to_string_lossy().to_string();
            let Some((os_id, backing_path)) = read_and_resolve_backing(&flink_str) else {
                continue;
            };
            if !backing_path.exists() {
                continue;
            }

            let Ok(shmem) = ShmemConf::new().os_id(&os_id).open() else {
                continue;
            };

            let type_name = flink_entry.file_name().to_string_lossy().to_string();

            let base = shmem.as_ptr();
            let shmem_len = shmem.len();

            let (elem_size, capacity, queue_writes, queue_fill, poison_quick) = match kind {
                ShmemKind::Queue => {
                    let (es, cap, w, f) = read_queue_meta_with_stats(&shmem);
                    let pq = quick_poison_queue(base, shmem_len);
                    (es, cap, w, f, pq)
                }
                ShmemKind::SeqlockArray => {
                    let (es, cap) = read_array_meta(&shmem);
                    let pq = quick_poison_array(base, shmem_len);
                    (es, cap, None, None, pq)
                }
                ShmemKind::Data => (shmem_len, 1, None, None, None),
                ShmemKind::Unknown => (0, 0, None, None, None),
            };

            entries.push(DiscoveredEntry {
                kind,
                app_name: app_name.to_owned(),
                type_name,
                flink: flink_str,
                elem_size,
                capacity,
                queue_writes,
                queue_fill,
                backing_path: Some(backing_path),
                poison_quick,
            });
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Read a flink file's OS id and compute the `/dev/shm/` backing path in one
/// pass.  Returns `None` if the flink can't be read or is empty.
fn read_and_resolve_backing(flink: &str) -> Option<(String, PathBuf)> {
    let os_id = std::fs::read_to_string(flink).ok()?.trim().to_string();
    if os_id.is_empty() {
        return None;
    }
    let raw = if os_id.starts_with('/') {
        format!("/dev/shm{os_id}")
    } else {
        format!("/dev/shm/{os_id}")
    };
    Some((os_id, PathBuf::from(raw)))
}

fn read_queue_meta_with_stats(shmem: &Shmem) -> (usize, usize, Option<usize>, Option<usize>) {
    if shmem.len() < std::mem::size_of::<QueueHeader>() {
        return (0, 0, None, None);
    }
    let header = unsafe { &*(shmem.as_ptr() as *const QueueHeader) };
    if !header.is_initialized() || header.elsize == 0 {
        return (0, 0, None, None);
    }
    let queue_writes = header.count.load(Ordering::Relaxed);
    let queue_fill = queue_writes & header.mask;
    (header.elsize, header.mask + 1, Some(queue_writes), Some(queue_fill))
}

/// O(1) poison probe on an already-mapped queue: check the seqlock version at
/// the current write position.
fn quick_poison_queue(base: *const u8, shmem_len: usize) -> Option<bool> {
    const HEADER_SIZE: usize = 64;
    if shmem_len < std::mem::size_of::<QueueHeader>() {
        return None;
    }
    let header = unsafe { &*(base as *const QueueHeader) };
    if !header.is_initialized() || header.elsize == 0 {
        return None;
    }
    let elsize = header.elsize;
    let write_pos = header.count.load(Ordering::Acquire) & header.mask;
    if HEADER_SIZE + write_pos * elsize + std::mem::size_of::<AtomicU64>() > shmem_len {
        return None;
    }
    let slot_ptr = unsafe { base.add(HEADER_SIZE + write_pos * elsize) } as *const AtomicU64;
    let v1 = unsafe { &*slot_ptr }.load(Ordering::Acquire);
    if v1 & 1 == 0 {
        return Some(false);
    }
    std::thread::yield_now();
    let v2 = unsafe { &*slot_ptr }.load(Ordering::Acquire);
    Some(v2 & 1 != 0)
}

/// O(1) poison probe on an already-mapped array: check the first slot.
fn quick_poison_array(base: *const u8, shmem_len: usize) -> Option<bool> {
    const HEADER_SIZE: usize = 64;
    if shmem_len < std::mem::size_of::<ArrayHeader>() {
        return None;
    }
    let header = unsafe { &*(base as *const ArrayHeader) };
    if !header.is_initialized() || header.elsize == 0 {
        return None;
    }
    if HEADER_SIZE + std::mem::size_of::<AtomicU64>() > shmem_len {
        return None;
    }
    let slot_ptr = unsafe { base.add(HEADER_SIZE) } as *const AtomicU64;
    let v1 = unsafe { &*slot_ptr }.load(Ordering::Acquire);
    if v1 & 1 == 0 {
        return Some(false);
    }
    std::thread::yield_now();
    let v2 = unsafe { &*slot_ptr }.load(Ordering::Acquire);
    Some(v2 & 1 != 0)
}

fn read_array_meta(shmem: &Shmem) -> (usize, usize) {
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
