//! Segment inspection: PID info, poison detection, backing file size, queue stats.

use std::sync::atomic::{AtomicU64, Ordering};

use flux_communication::array::ArrayHeader;
use flux_communication::queue::QueueHeader;
use flux_communication::registry::{ShmemEntry, ShmemKind};
use shared_memory::ShmemConf;

// ─── PID info ───────────────────────────────────────────────────────────────

/// Metadata about an attached process, gathered from `/proc`.
#[derive(Clone, Debug)]
pub struct PidInfo {
    /// The Linux process ID.
    pub pid: u32,
    /// Whether the process is currently alive (signal-0 check).
    pub alive: bool,
    /// Process name from `/proc/<pid>/comm`.
    pub name: String,
    /// Full command line from `/proc/<pid>/cmdline` (NUL-separated args joined with spaces).
    pub cmdline: String,
    /// Process start time as an RFC 3339 timestamp, or empty if unavailable.
    pub start_time: String,
}

/// Gather process metadata for a single PID.
///
/// Reads `/proc/<pid>/comm`, `/proc/<pid>/cmdline`, and `/proc/<pid>/stat`
/// to populate the returned [`PidInfo`]. If the process is no longer alive,
/// returns a [`PidInfo`] with `alive = false` and empty strings for name,
/// cmdline, and start_time.
pub fn pid_info(pid: u32) -> PidInfo {
    let alive = is_pid_alive(pid);
    if !alive {
        return PidInfo {
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

    let start_time = read_start_time(pid).unwrap_or_default();

    PidInfo { pid, alive, name, cmdline, start_time }
}

/// Gather process metadata for all PIDs attached to a registry entry.
///
/// Calls [`pid_info`] for each PID in the entry's [`PidSet`] and returns
/// the collected results. Dead PIDs are included with `alive = false`.
pub fn pids_info(entry: &ShmemEntry) -> Vec<PidInfo> {
    entry.pids.active_pids().iter().map(|&pid| pid_info(pid)).collect()
}

// ─── Poison detection ───────────────────────────────────────────────────────

/// Summary of poisoned (stuck mid-write) seqlock slots in a shared memory segment.
#[derive(Clone, Debug)]
pub struct PoisonInfo {
    /// Number of slots with an odd (stuck) version counter.
    pub n_poisoned: usize,
    /// Index of the first poisoned slot (for diagnostic display).
    pub first_slot: usize,
    /// Total number of slots in the buffer.
    pub total_slots: usize,
}

/// Scan a segment's seqlock buffer for poisoned slots.
///
/// Works for both `Queue` and `SeqlockArray` — both use the same pattern:
/// a `repr(C, align(64))` header followed by `Seqlock<T>` slots where the
/// first 8 bytes of each slot are the `AtomicU64` version.
///
/// A slot is poisoned when its version is odd (write-in-progress) and stays
/// odd for >10µs, meaning the writer crashed mid-write.
pub fn check_poison(entry: &ShmemEntry) -> Option<PoisonInfo> {
    match entry.kind {
        ShmemKind::Queue => check_queue_poison(entry.flink.as_str()),
        ShmemKind::SeqlockArray => check_array_poison(entry.flink.as_str()),
        _ => None,
    }
}

fn check_queue_poison(flink: &str) -> Option<PoisonInfo> {
    const HEADER_SIZE: usize = 64; // QueueHeader is repr(C, align(64))

    let shmem = ShmemConf::new().flink(flink).open().ok()?;
    let base = shmem.as_ptr();
    let shmem_len = shmem.len();

    if shmem_len < std::mem::size_of::<QueueHeader>() {
        drop(shmem);
        return None;
    }

    let header = unsafe { &*(base as *const QueueHeader) };
    if !header.is_initialized() || header.elsize == 0 {
        drop(shmem);
        return None;
    }

    let n_slots = header.mask + 1;
    let elsize = header.elsize;

    if HEADER_SIZE + n_slots * elsize > shmem_len {
        drop(shmem);
        return None;
    }

    let buf_base = unsafe { base.add(HEADER_SIZE) };
    let buf_remaining = shmem_len - HEADER_SIZE;
    let result = scan_seqlock_slots(buf_base, n_slots, elsize, buf_remaining);
    drop(shmem);
    result
}

fn check_array_poison(flink: &str) -> Option<PoisonInfo> {
    const HEADER_SIZE: usize = 64; // ArrayHeader is repr(C, align(64))

    let shmem = ShmemConf::new().flink(flink).open().ok()?;
    let base = shmem.as_ptr();
    let shmem_len = shmem.len();

    if shmem_len < std::mem::size_of::<ArrayHeader>() {
        drop(shmem);
        return None;
    }

    let header = unsafe { &*(base as *const ArrayHeader) };
    if !header.is_initialized() || header.elsize == 0 {
        drop(shmem);
        return None;
    }

    let n_slots = header.bufsize;
    let elsize = header.elsize;

    if HEADER_SIZE + n_slots * elsize > shmem_len {
        drop(shmem);
        return None;
    }

    let buf_base = unsafe { base.add(HEADER_SIZE) };
    let buf_remaining = shmem_len - HEADER_SIZE;
    let result = scan_seqlock_slots(buf_base, n_slots, elsize, buf_remaining);
    drop(shmem);
    result
}

/// Walk `n_slots` seqlock slots starting at `buf_base` with stride `elsize`,
/// reading the `AtomicU64` version at offset 0 of each slot.
///
/// Two snapshot reads separated by a yield — if a slot's version is odd in
/// both reads it's stuck (poisoned). Transient odd values from in-flight
/// writes resolve between snapshots. This avoids blocking the TUI thread.
fn scan_seqlock_slots(
    buf_base: *mut u8,
    n_slots: usize,
    elsize: usize,
    buf_remaining: usize,
) -> Option<PoisonInfo> {
    // Each slot must be at least 8 bytes to hold the AtomicU64 version field.
    if elsize < std::mem::size_of::<AtomicU64>() {
        return None;
    }

    // First pass: collect slots with odd versions.
    let mut odd_slots: Vec<usize> = Vec::new();
    for i in 0..n_slots {
        // Defense-in-depth: skip slots that would read beyond the mapped region.
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
        Some(PoisonInfo { n_poisoned, first_slot: first_slot.unwrap(), total_slots: n_slots })
    } else {
        None
    }
}

// ─── Backing file size ──────────────────────────────────────────────────────

/// Return the size (in bytes) of the shared memory backing file.
///
/// Opens the shmem read-only via its flink and returns `Shmem::len()`.
/// Returns `None` if the shmem cannot be opened. The shmem is properly
/// dropped (unmapped) after reading the size — dropping a non-owner `Shmem`
/// only unmaps the local mapping without unlinking the backing file.
pub fn backing_file_size(flink: &str) -> Option<u64> {
    let shmem = ShmemConf::new().flink(flink).open().ok()?;
    let size = shmem.len() as u64;
    drop(shmem);
    Some(size)
}

// ─── Queue stats ────────────────────────────────────────────────────────────

/// Queue statistics read from shared memory without leaking.
#[derive(Clone, Debug)]
pub struct QueueStats {
    /// Total number of writes (`QueueHeader::count`).
    pub writes: usize,
    /// Current fill level (`count & mask`).
    pub fill: usize,
    /// Ring buffer capacity (`mask + 1`).
    pub capacity: usize,
}

/// Read queue statistics from shared memory without leaking.
///
/// Opens the shmem, reads `QueueHeader` fields, and drops the mapping.
/// Returns `None` if the shmem cannot be opened or the header is invalid.
pub fn read_queue_stats(flink: &str) -> Option<QueueStats> {
    let shmem = ShmemConf::new().flink(flink).open().ok()?;
    if shmem.len() < std::mem::size_of::<QueueHeader>() {
        drop(shmem);
        return None;
    }
    let header = unsafe { &*(shmem.as_ptr() as *const QueueHeader) };
    if !header.is_initialized() {
        drop(shmem);
        return None;
    }
    let writes = header.count.load(Ordering::Relaxed);
    let mask = header.mask;
    let capacity = mask + 1;
    let fill = writes & mask;
    drop(shmem);
    Some(QueueStats { writes, fill, capacity })
}

// ─── Byte formatting ────────────────────────────────────────────────────────

/// Format a byte count as a human-readable string using binary units (KiB, MiB, GiB).
///
/// Values below 1 KiB are shown as plain bytes (e.g. `"512 B"`).
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

// ─── Internal helpers ───────────────────────────────────────────────────────

pub use flux_communication::registry::is_pid_alive;

/// Read AT_CLKTCK from /proc/self/auxv (ELF auxiliary vector).
/// Falls back to 100 if the file can't be read.
fn clock_ticks_per_sec() -> u64 {
    const AT_CLKTCK: u64 = 17;
    let Ok(data) = std::fs::read("/proc/self/auxv") else {
        return 100;
    };
    // auxv is a sequence of (u64 type, u64 value) pairs, terminated by AT_NULL=0.
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
}

fn read_start_time(pid: u32) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after_comm = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    // Field 19 after comm = starttime (field 21 in full stat, 0-indexed)
    let starttime_ticks: u64 = fields.get(19)?.parse().ok()?;
    let ticks_per_sec = clock_ticks_per_sec();

    let proc_stat = std::fs::read_to_string("/proc/stat").ok()?;
    let btime_line = proc_stat.lines().find(|l| l.starts_with("btime "))?;
    let btime_secs: u64 = btime_line.split_whitespace().nth(1)?.parse().ok()?;

    let start_secs = btime_secs + starttime_ticks / ticks_per_sec;
    let ts = std::time::UNIX_EPOCH + std::time::Duration::from_secs(start_secs);
    Some(humantime::format_rfc3339_seconds(ts).to_string())
}
