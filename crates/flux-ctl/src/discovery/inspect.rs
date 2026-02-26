//! Segment inspection: PID info, poison detection, backing file size, queue
//! stats.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

pub use flux_communication::is_pid_alive;
use flux_communication::{ShmemKind, array::ArrayHeader, queue::QueueHeader};
use flux_timing::Nanos;
use shared_memory::ShmemConf;

use super::registry::DiscoveredEntry;

// ─── PID info ───────────────────────────────────────────────────────────────

/// Metadata about an attached process, gathered from `/proc`.
#[derive(Clone, Debug)]
pub struct PidInfo {
    pub pid: u32,
    pub alive: bool,
    pub name: String,
    pub cmdline: String,
    pub start_time: String,
}

impl PidInfo {
    /// Gather process metadata for a single PID from `/proc`.
    pub fn gather(pid: u32) -> Self {
        let alive = is_pid_alive(pid);
        if !alive {
            return Self {
                pid,
                alive: false,
                name: String::new(),
                cmdline: String::new(),
                start_time: String::new(),
            };
        }

        let name = std::fs::read_to_string(format!("/proc/{pid}/comm"))
            .map(|s| s.trim().to_string())
            .unwrap_or_default();

        let cmdline = std::fs::read(format!("/proc/{pid}/cmdline"))
            .map(|bytes| {
                bytes
                    .split(|&b| b == 0)
                    .filter(|s| !s.is_empty())
                    .map(|s| String::from_utf8_lossy(s).into_owned())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_default();

        let start_time = Self::read_start_time(pid).unwrap_or_default();

        Self { pid, alive, name, cmdline, start_time }
    }

    /// Gather process metadata for all given PIDs.
    pub fn for_pids(pids: &[u32]) -> Vec<Self> {
        pids.iter().map(|&pid| Self::gather(pid)).collect()
    }

    /// Read AT_CLKTCK from /proc/self/auxv (ELF auxiliary vector).
    /// Falls back to 100 if the file can't be read.  Cached after the first
    /// call since the value never changes.
    fn clock_ticks_per_sec() -> u64 {
        static CACHE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        *CACHE.get_or_init(|| {
            const AT_CLKTCK: u64 = 17;
            let Ok(data) = std::fs::read("/proc/self/auxv") else {
                return 100;
            };
            for chunk in data.chunks_exact(16) {
                let a_type = u64::from_ne_bytes(chunk[..8].try_into().unwrap());
                if a_type == 0 {
                    break;
                }
                if a_type == AT_CLKTCK {
                    return u64::from_ne_bytes(chunk[8..16].try_into().unwrap());
                }
            }
            100
        })
    }

    fn read_start_time(pid: u32) -> Option<String> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let after_comm = stat.rsplit_once(')')?.1;
        let fields: Vec<&str> = after_comm.split_whitespace().collect();
        let starttime_ticks: u64 = fields.get(19)?.parse().ok()?;
        let ticks_per_sec = Self::clock_ticks_per_sec();

        let proc_stat = std::fs::read_to_string("/proc/stat").ok()?;
        let btime_line = proc_stat.lines().find(|l| l.starts_with("btime "))?;
        let btime_secs: u64 = btime_line.split_whitespace().nth(1)?.parse().ok()?;

        let start_secs = btime_secs + starttime_ticks / ticks_per_sec;
        Some(Nanos::from_secs(start_secs).with_fmt_utc("%Y-%m-%dT%H:%M:%SZ"))
    }
}

// ─── Poison detection ───────────────────────────────────────────────────────

/// Summary of poisoned (stuck mid-write) seqlock slots in a shared memory
/// segment.
#[derive(Clone, Debug)]
pub struct PoisonInfo {
    pub n_poisoned: usize,
    pub first_slot: usize,
    pub total_slots: usize,
}

impl PoisonInfo {
    /// Scan a segment's seqlock buffer for poisoned slots.
    ///
    /// Works for both `Queue` and `SeqlockArray` — both use the same pattern:
    /// a `repr(C, align(64))` header followed by `Seqlock<T>` slots where the
    /// first 8 bytes of each slot are the `AtomicU64` version.
    pub fn check(entry: &DiscoveredEntry) -> Option<Self> {
        match entry.kind {
            ShmemKind::Queue => Self::check_queue(&entry.flink),
            ShmemKind::SeqlockArray => Self::check_array(&entry.flink),
            _ => None,
        }
    }

    fn check_queue(flink: &str) -> Option<Self> {
        const HEADER_SIZE: usize = 64;

        let shmem = ShmemConf::new().flink(flink).open().ok()?;
        let base = shmem.as_ptr();
        let shmem_len = shmem.len();

        if shmem_len < std::mem::size_of::<QueueHeader>() {
            return None;
        }

        let header = unsafe { &*(base as *const QueueHeader) };
        if !header.is_initialized() || header.elsize == 0 {
            return None;
        }

        let n_slots = header.mask + 1;
        let elsize = header.elsize;

        if HEADER_SIZE + n_slots * elsize > shmem_len {
            return None;
        }

        let buf_base = unsafe { base.add(HEADER_SIZE) };
        let buf_remaining = shmem_len - HEADER_SIZE;
        // shmem must stay alive while scan_seqlock_slots reads the raw ptr.
        Self::scan_seqlock_slots(buf_base, n_slots, elsize, buf_remaining)
    }

    fn check_array(flink: &str) -> Option<Self> {
        const HEADER_SIZE: usize = 64;

        let shmem = ShmemConf::new().flink(flink).open().ok()?;
        let base = shmem.as_ptr();
        let shmem_len = shmem.len();

        if shmem_len < std::mem::size_of::<ArrayHeader>() {
            return None;
        }

        let header = unsafe { &*(base as *const ArrayHeader) };
        if !header.is_initialized() || header.elsize == 0 {
            return None;
        }

        let n_slots = header.bufsize;
        let elsize = header.elsize;

        if HEADER_SIZE + n_slots * elsize > shmem_len {
            return None;
        }

        let buf_base = unsafe { base.add(HEADER_SIZE) };
        let buf_remaining = shmem_len - HEADER_SIZE;
        // shmem must stay alive while scan_seqlock_slots reads the raw ptr.
        Self::scan_seqlock_slots(buf_base, n_slots, elsize, buf_remaining)
    }

    /// Walk `n_slots` seqlock slots starting at `buf_base` with stride
    /// `elsize`, reading the `AtomicU64` version at offset 0 of each slot.
    ///
    /// Two snapshot reads separated by a yield — if a slot's version is odd in
    /// both reads it's stuck (poisoned).
    fn scan_seqlock_slots(
        buf_base: *mut u8,
        n_slots: usize,
        elsize: usize,
        buf_remaining: usize,
    ) -> Option<Self> {
        if elsize < std::mem::size_of::<AtomicU64>() {
            return None;
        }

        // First pass: collect slots with odd versions.
        let mut odd_slots: Vec<usize> = Vec::new();
        for i in 0..n_slots {
            if i * elsize + std::mem::size_of::<AtomicU64>() > buf_remaining {
                break;
            }
            let version_ptr = unsafe { buf_base.add(i * elsize) } as *const AtomicU64;
            let v = unsafe { &*version_ptr }.load(Ordering::Acquire);
            if v & 1 != 0 {
                odd_slots.push(i);
            }
        }

        if odd_slots.is_empty() {
            return None;
        }

        // Yield to let any in-flight writes complete.
        std::thread::yield_now();

        // Second pass: re-check only the odd slots.
        let mut first_slot = None;
        let mut n_poisoned = 0;
        for &i in &odd_slots {
            let version_ptr = unsafe { buf_base.add(i * elsize) } as *const AtomicU64;
            let v = unsafe { &*version_ptr }.load(Ordering::Acquire);
            if v & 1 != 0 {
                n_poisoned += 1;
                if first_slot.is_none() {
                    first_slot = Some(i);
                }
            }
        }

        if n_poisoned > 0 {
            Some(Self { n_poisoned, first_slot: first_slot.unwrap(), total_slots: n_slots })
        } else {
            None
        }
    }
}

// ─── Backing file size ──────────────────────────────────────────────────────

/// Return the size (in bytes) of the shared memory backing file.
pub fn backing_file_size(flink: &str) -> Option<u64> {
    let shmem = ShmemConf::new().flink(flink).open().ok()?;
    Some(shmem.len() as u64)
}

/// Read the OS shared-memory ID (first line of the flink file).
pub fn read_shmem_os_id(flink: &str) -> Option<String> {
    std::fs::read_to_string(flink).ok().map(|s| s.trim().to_string())
}

/// Map a flink's OS ID to the `/dev/shm/` path that appears in
/// `/proc/<pid>/fd/`.
///
/// The flink file stores the `shm_open` name (e.g. `/shmem_ABC123`); the
/// kernel exposes the backing file as `/dev/shm/shmem_ABC123`.
fn resolve_backing_path(flink: &str) -> Option<PathBuf> {
    let os_id = read_shmem_os_id(flink)?;
    let raw = if os_id.starts_with('/') {
        format!("/dev/shm{os_id}")
    } else {
        format!("/dev/shm/{os_id}")
    };
    Some(std::fs::canonicalize(&raw).unwrap_or_else(|_| PathBuf::from(raw)))
}

/// Single pass over `/proc/*/fd/` → map from backing-file path to PIDs.
///
/// Only entries under `/dev/shm/` are collected, so non-shmem fds are
/// skipped cheaply by checking the readlink target prefix.
pub fn scan_proc_fds() -> HashMap<PathBuf, Vec<u32>> {
    let mut map: HashMap<PathBuf, Vec<u32>> = HashMap::new();

    let Ok(proc_iter) = std::fs::read_dir("/proc") else {
        return map;
    };

    for proc_entry in proc_iter.flatten() {
        let fname = proc_entry.file_name();
        let Some(pid) = fname.to_str().and_then(|s| s.parse::<u32>().ok()) else {
            continue;
        };

        let fd_dir = format!("/proc/{pid}/fd");
        let Ok(fd_iter) = std::fs::read_dir(&fd_dir) else {
            continue;
        };

        for fd_entry in fd_iter.flatten() {
            if let Ok(target) = std::fs::read_link(fd_entry.path()) {
                if target.starts_with("/dev/shm/") {
                    map.entry(target).or_default().push(pid);
                }
            }
        }
    }

    // Sort + dedup each pid list for deterministic output.
    for pids in map.values_mut() {
        pids.sort_unstable();
        pids.dedup();
    }

    map
}

// ─── DiscoveredEntry methods ────────────────────────────────────────────────

impl DiscoveredEntry {
    /// Resolve the `/dev/shm/` backing path for this entry's flink.
    pub fn backing_path(&self) -> Option<PathBuf> {
        resolve_backing_path(&self.flink)
    }

    /// Look up PIDs attached to this entry using a pre-built proc-fd map.
    pub fn pids(&self, proc_map: &HashMap<PathBuf, Vec<u32>>) -> Vec<u32> {
        self.backing_path().and_then(|backing| proc_map.get(&backing)).cloned().unwrap_or_default()
    }
}

// ─── Queue stats ────────────────────────────────────────────────────────────

/// Queue statistics read from shared memory.
#[derive(Clone, Debug)]
pub struct QueueStats {
    pub writes: usize,
    pub fill: usize,
    pub capacity: usize,
}

impl QueueStats {
    /// Read queue statistics from shared memory without leaking.
    pub fn read(flink: &str) -> Option<Self> {
        let shmem = ShmemConf::new().flink(flink).open().ok()?;
        if shmem.len() < std::mem::size_of::<QueueHeader>() {
            return None;
        }
        let header = unsafe { &*(shmem.as_ptr() as *const QueueHeader) };
        if !header.is_initialized() {
            return None;
        }
        let writes = header.count.load(Ordering::Relaxed);
        let mask = header.mask;
        let capacity = mask + 1;
        let fill = writes & mask;
        Some(Self { writes, fill, capacity })
    }
}

// ─── Byte formatting ────────────────────────────────────────────────────────

/// Format a byte count as a human-readable string using binary units (KiB, MiB,
/// GiB).
pub fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;

    if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}
