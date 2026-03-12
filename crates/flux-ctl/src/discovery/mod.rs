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
use flux_timing::{Duration, Instant};
pub use inspect::{
    ConsumerGroupInfo, PidInfo, PoisonInfo, QueueStats, backing_file_size, format_bytes,
    read_consumer_groups, resolve_backing_path, scan_proc_fds,
};
use shared_memory::{Shmem, ShmemConf};

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
    /// Total size of the backing shmem mapping in bytes.
    pub backing_size: usize,
    /// Quick O(1) poison probe result from the cached shmem mapping.
    /// `Some(true)` = write-position slot has an odd seqlock version,
    /// `Some(false)` = even (clean), `None` = not applicable or couldn't
    /// check.
    pub poison_quick: Option<bool>,
}

impl DiscoveredEntry {
    /// Sorted, deduplicated application names from a slice of entries.
    pub fn app_names(entries: &[Self]) -> Vec<String> {
        let mut names: Vec<String> = entries.iter().map(|e| e.app_name.clone()).collect();
        names.sort();
        names.dedup();
        names
    }

    /// Whether this entry's backing shmem flink still exists on disk.
    ///
    /// Since `scan_base_dir` already filters out entries whose shmem cannot be
    /// opened, this is a secondary liveness check for entries that may have
    /// become stale since the last scan.
    pub fn is_visible(&self) -> bool {
        flink_reachable(&self.flink)
    }
}

/// A cached open shmem handle together with its static metadata.
///
/// Metadata fields (kind, app_name, …) are set once when the segment is
/// first opened.  Dynamic fields (queue_writes, poison_quick, …) are
/// re-read from the still-mapped memory via [`to_entry`](Self::to_entry).
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

impl CachedHandle {
    /// Try to open a shmem segment from a flink path on disk.
    ///
    /// Reads the OS shmem id from the flink file, checks the backing
    /// `/dev/shm/` file exists (one `stat`), then opens by os_id —
    /// avoiding the `shared_memory` crate's 5×50 ms retry loop on stale
    /// segments.
    fn open(flink_str: &str, kind: ShmemKind, app_name: &str) -> Option<Self> {
        let (os_id, backing_path) = read_and_resolve_backing(flink_str)?;
        if !backing_path.exists() {
            return None;
        }

        let shmem = ShmemConf::new().os_id(&os_id).open().ok()?;

        let type_name = Path::new(flink_str)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        let (elem_size, capacity) = match kind {
            ShmemKind::Queue => {
                if shmem.len() < std::mem::size_of::<QueueHeader>() {
                    return None;
                }
                let h = unsafe { &*(shmem.as_ptr() as *const QueueHeader) };
                if !h.is_initialized() || h.elsize == 0 {
                    return None;
                }
                (h.elsize, h.mask + 1)
            }
            ShmemKind::SeqlockArray => {
                let (es, cap) = read_array_meta(&shmem);
                if es == 0 {
                    return None;
                }
                (es, cap)
            }
            ShmemKind::Data => (shmem.len(), 1),
            ShmemKind::Unknown => return None,
        };

        Some(Self {
            shmem,
            kind,
            app_name: app_name.to_owned(),
            type_name,
            flink: flink_str.to_owned(),
            elem_size,
            capacity,
            backing_path: Some(backing_path),
        })
    }

    /// Snapshot this handle's live header data into a [`DiscoveredEntry`].
    ///
    /// Pure pointer reads from already-mapped memory — no syscalls.
    fn to_entry(&self) -> DiscoveredEntry {
        let base = self.shmem.as_ptr();
        let shmem_len = self.shmem.len();

        let (queue_writes, queue_fill, poison_quick) = match self.kind {
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
            kind: self.kind,
            app_name: self.app_name.clone(),
            type_name: self.type_name.clone(),
            flink: self.flink.clone(),
            elem_size: self.elem_size,
            capacity: self.capacity,
            queue_writes,
            queue_fill,
            backing_path: self.backing_path.clone(),
            backing_size: self.shmem.len(),
            poison_quick,
        }
    }
}

/// Keeps shmem segments mapped across refresh ticks so that per-tick reads
/// are plain pointer dereferences — zero `shm_open`/`mmap`/`munmap` syscalls.
///
/// Call [`refresh`](Self::refresh) periodically to pick up new segments and
/// drop stale ones.  Call [`read_entries`](Self::read_entries) on every tick
/// to snapshot the live header data.
pub struct ShmemCache {
    handles: HashMap<String, CachedHandle>,
    cached_dirs: Vec<(String, PathBuf)>,
    dirs_last_scan: Option<Instant>,
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

    /// Rescan the directory tree (10 s TTL) and reconcile open handles.
    ///
    /// Between rescans this is a no-op — [`read_entries`](Self::read_entries)
    /// reads from already-mapped memory.
    pub fn refresh(&mut self, base_dir: &Path) {
        let now = Instant::now();
        let should_rescan = self
            .dirs_last_scan
            .is_none_or(|last| now.elapsed_since(last) >= Duration::from_secs(10));

        if should_rescan {
            self.cached_dirs.clear();
            find_shmem_dirs(base_dir, &mut self.cached_dirs);
            self.dirs_last_scan = Some(now);
            self.reconcile_handles();
        }
    }

    /// Snapshot every cached segment's live header data.
    ///
    /// Hot path — no file I/O, just pointer reads from mapped memory.
    pub fn read_entries(&self) -> Vec<DiscoveredEntry> {
        self.handles.values().map(CachedHandle::to_entry).collect()
    }

    /// Open new handles and drop ones whose flinks disappeared.
    fn reconcile_handles(&mut self) {
        // Build the set of flink paths currently on disk.
        let mut live_flinks: HashMap<String, (ShmemKind, String)> = HashMap::new();
        for (app_name, shmem_dir) in &self.cached_dirs {
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
                    live_flinks.insert(flink_str, (kind, app_name.clone()));
                }
            }
        }

        // Drop handles whose flinks are gone.
        self.handles.retain(|flink, _| live_flinks.contains_key(flink));

        // Open handles for newly-discovered flinks.
        for (flink_str, (kind, app_name)) in &live_flinks {
            if self.handles.contains_key(flink_str) {
                continue;
            }
            if let Some(handle) = CachedHandle::open(flink_str, *kind, app_name) {
                self.handles.insert(flink_str.clone(), handle);
            }
        }
    }

    /// Read consumer groups from a cached queue mapping — zero syscalls.
    pub fn consumer_groups(&self, flink: &str) -> Vec<ConsumerGroupInfo> {
        let Some(handle) = self.handles.get(flink) else {
            return Vec::new();
        };
        if handle.kind != ShmemKind::Queue {
            return Vec::new();
        }
        if handle.shmem.len() < std::mem::size_of::<QueueHeader>() {
            return Vec::new();
        }
        let header = unsafe { &*(handle.shmem.as_ptr() as *const QueueHeader) };
        if !header.is_initialized() {
            return Vec::new();
        }
        header
            .active_groups()
            .into_iter()
            .map(|(label, cursor)| ConsumerGroupInfo {
                label: label.to_owned(),
                cursor,
            })
            .collect()
    }
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
        if !path.is_dir() || entry.file_name() == "data" {
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

fn is_shmem_root(dir: &Path) -> bool {
    dir.join("queues").is_dir() || dir.join("data").is_dir() || dir.join("arrays").is_dir()
}

/// Read all flinks under a single `shmem/` directory and append entries.
///
/// Checks the backing `/dev/shm/` file exists before calling
/// `ShmemConf::open`, avoiding the crate's 5×50 ms retry loop on stale
/// segments.
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
                backing_size: shmem_len,
                poison_quick,
            });
        }
    }
}

/// Read a flink file's OS id and derive the `/dev/shm/` backing path.
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

/// O(1) poison probe on an already-mapped queue: check the seqlock version
/// at the current write position.
fn quick_poison_queue(base: *const u8, shmem_len: usize) -> Option<bool> {
    const HEADER_SIZE: usize = std::mem::size_of::<QueueHeader>();
    if shmem_len < HEADER_SIZE {
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
    const HEADER_SIZE: usize = std::mem::size_of::<ArrayHeader>();
    if shmem_len < HEADER_SIZE {
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

/// Check whether a flink's backing file exists and is non-empty.
///
/// Lightweight filesystem-only check — does **not** open or mmap the shared
/// memory segment.
pub fn flink_reachable(flink: &str) -> bool {
    if flink.is_empty() {
        return false;
    }
    std::fs::metadata(flink).map(|m| m.len() > 0).unwrap_or(false)
}
