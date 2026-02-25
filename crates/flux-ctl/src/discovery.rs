use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use flux_communication::array::ArrayHeader;
use flux_communication::queue::QueueHeader;
use flux_communication::registry::{
    REGISTRY_FLINK_NAME, ShmemEntry, ShmemKind, ShmemRegistry, cleanup_flink,
};
use shared_memory::ShmemConf;

#[derive(Clone, Debug)]
pub struct PidInfo {
    pub pid: u32,
    pub alive: bool,
    pub name: String,
    pub cmdline: String,
    pub start_time: String,
}

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

pub fn pids_info(entry: &ShmemEntry) -> Vec<PidInfo> {
    entry.pids.active_pids().iter().map(|&pid| pid_info(pid)).collect()
}

/// Open the global registry, sweeping dead PIDs on open.
pub fn open_registry(base_dir: &Path) -> Option<&'static ShmemRegistry> {
    let registry_path = base_dir.join(REGISTRY_FLINK_NAME);
    let reg = ShmemRegistry::open(&registry_path)?;
    reg.sweep_dead_pids();
    Some(reg)
}

pub use flux_communication::registry::is_pid_alive;

pub fn app_names(registry: &ShmemRegistry) -> Vec<String> {
    let mut names: Vec<String> = registry
        .entries()
        .iter()
        .filter(|e| !e.is_empty())
        .map(|e| e.app_name.as_str().to_string())
        .collect();
    names.sort();
    names.dedup();
    names
}

pub fn list_all(base_dir: &Path, verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
    let Some(registry) = open_registry(base_dir) else {
        println!(
            "No flux shmem registry found at {}",
            base_dir.join(REGISTRY_FLINK_NAME).display()
        );
        return Ok(());
    };

    let entries: Vec<&ShmemEntry> = registry.entries().iter().filter(|e| entry_visible(e)).collect();
    if entries.is_empty() {
        println!("No active segments found");
        return Ok(());
    }

    let apps = app_names(registry);
    println!("Found {} segments across {} apps\n", entries.len(), apps.len());

    for app in &apps {
        let app_entries: Vec<&&ShmemEntry> =
            entries.iter().filter(|e| e.app_name.as_str() == app).collect();
        if app_entries.is_empty() {
            continue;
        }
        println!("📦 {} ({} segments)", app, app_entries.len());
        for entry in &app_entries {
            let alive = entry.pids.any_alive();
            let poison = check_poison(entry);
            let status = if poison.is_some() {
                "☠"
            } else if alive {
                "🟢"
            } else {
                "💀"
            };
            let pids = format_pids(entry);
            println!(
                "  {} {:14} {:>24}  elem={}B  cap={}  {}",
                status,
                entry.kind,
                entry.type_name.as_str(),
                entry.elem_size,
                entry.capacity,
                pids,
            );
            if let Some(ref pi) = poison {
                println!(
                    "    ☠ POISONED: {}/{} slots (first at slot {})",
                    pi.n_poisoned, pi.total_slots, pi.first_slot,
                );
            }
            if verbose {
                println!("    flink: {}", entry.flink.as_str());
                if entry.kind == ShmemKind::Queue {
                    match QueueHeader::open_shared(entry.flink.as_str()) {
                        Ok(header) => {
                            println!(
                                "    queue: type={:?}  writes={}  capacity={}",
                                header.queue_type,
                                header.count.load(Ordering::Relaxed),
                                header.mask + 1,
                            );
                        }
                        Err(e) => println!("    queue: (could not open: {e})"),
                    }
                }
            }
        }
        println!();
    }
    Ok(())
}

pub fn inspect(
    base_dir: &Path,
    app_filter: Option<&str>,
    segment_filter: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(registry) = open_registry(base_dir) else {
        println!("No registry found");
        return Ok(());
    };

    for entry in registry.entries() {
        if !entry_visible(entry) {
            continue;
        }
        if let Some(app) = app_filter {
            if entry.app_name.as_str() != app {
                continue;
            }
        }
        if let Some(seg) = segment_filter {
            if !entry.type_name.as_str().contains(seg) {
                continue;
            }
        }

        let alive = entry.pids.any_alive();
        println!("─── {} ───", entry.type_name.as_str());
        println!("  App:       {}", entry.app_name.as_str());
        println!("  Kind:      {}", entry.kind);
        let active = entry.pids.active_pids();
        if active.len() == 1 {
            println!(
                "  PID:       {} {}",
                active[0],
                if alive { "(alive)" } else { "(dead)" }
            );
        } else {
            println!("  PIDs:      {} attached", active.len());
            for &pid in &active {
                let s = if is_pid_alive(pid) { "alive" } else { "dead" };
                println!("             {} ({})", pid, s);
            }
        }
        println!("  Elem size: {} bytes", entry.elem_size);
        println!("  Capacity:  {}", entry.capacity);
        println!("  Flink:     {}", entry.flink.as_str());
        println!("  Type hash: 0x{:016x}", entry.type_hash);

        if entry.kind == ShmemKind::Queue {
            if let Ok(header) = QueueHeader::open_shared(entry.flink.as_str()) {
                println!("  Queue type:   {:?}", header.queue_type);
                println!(
                    "  Total writes: {}",
                    header.count.load(Ordering::Relaxed)
                );
                println!(
                    "  Mask:         {} (capacity={})",
                    header.mask,
                    header.mask + 1
                );
            }
        }
        println!();
    }
    Ok(())
}

pub fn clean(
    base_dir: &Path,
    app_filter: Option<&str>,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(registry) = open_registry(base_dir) else {
        println!("No registry found");
        return Ok(());
    };

    let mut stale = Vec::new();
    for entry in registry.entries() {
        if entry.is_empty() {
            continue;
        }
        if let Some(app) = app_filter {
            if entry.app_name.as_str() != app {
                continue;
            }
        }
        if !entry.pids.any_alive() && flink_reachable(entry.flink.as_str()) {
            stale.push(entry);
        }
    }

    if stale.is_empty() {
        println!("No stale segments found");
        return Ok(());
    }

    println!("Found {} stale segments:", stale.len());
    for entry in &stale {
        let pids = format_pids(entry);
        println!(
            "  💀 {} {} ({})",
            entry.app_name.as_str(),
            entry.type_name.as_str(),
            pids,
        );
        if force {
            let flink_path = Path::new(entry.flink.as_str());
            cleanup_flink(flink_path);
            println!("    ✓ cleaned {}", flink_path.display());
        }
    }

    if !force {
        println!("\nDry run. Use --force to actually remove flink files.");
    }
    Ok(())
}

// ─── Poison detection ───────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct PoisonInfo {
    pub n_poisoned: usize,
    pub first_slot: usize,
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

    let header = unsafe { &*(base as *const QueueHeader) };
    if !header.is_initialized() || header.elsize == 0 {
        std::mem::forget(shmem);
        return None;
    }

    let n_slots = header.mask + 1;
    let elsize = header.elsize;

    if HEADER_SIZE + n_slots * elsize > shmem_len {
        std::mem::forget(shmem);
        return None;
    }

    let buf_base = unsafe { base.add(HEADER_SIZE) };
    let result = scan_seqlock_slots(buf_base, n_slots, elsize);
    std::mem::forget(shmem);
    result
}

fn check_array_poison(flink: &str) -> Option<PoisonInfo> {
    const HEADER_SIZE: usize = 64; // ArrayHeader is repr(C, align(64))

    let shmem = ShmemConf::new().flink(flink).open().ok()?;
    let base = shmem.as_ptr();
    let shmem_len = shmem.len();

    let header = unsafe { &*(base as *const ArrayHeader) };
    if !header.is_initialized() || header.elsize == 0 {
        std::mem::forget(shmem);
        return None;
    }

    let n_slots = header.bufsize;
    let elsize = header.elsize;

    if HEADER_SIZE + n_slots * elsize > shmem_len {
        std::mem::forget(shmem);
        return None;
    }

    let buf_base = unsafe { base.add(HEADER_SIZE) };
    let result = scan_seqlock_slots(buf_base, n_slots, elsize);
    std::mem::forget(shmem);
    result
}

/// Walk `n_slots` seqlock slots starting at `buf_base` with stride `elsize`,
/// reading the `AtomicU64` version at offset 0 of each slot.
///
/// Two snapshot reads separated by a yield — if a slot's version is odd in
/// both reads it's stuck (poisoned). Transient odd values from in-flight
/// writes resolve between snapshots. This avoids blocking the TUI thread.
fn scan_seqlock_slots(buf_base: *mut u8, n_slots: usize, elsize: usize) -> Option<PoisonInfo> {
    // First pass: collect slots with odd versions.
    let mut odd_slots: Vec<usize> = Vec::new();
    for i in 0..n_slots {
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

// ─── Helpers ────────────────────────────────────────────────────────────────

pub fn flink_reachable(flink: &str) -> bool {
    if flink.is_empty() {
        return false;
    }
    if !Path::new(flink).exists() {
        return false;
    }
    ShmemConf::new().flink(flink).open().is_ok()
}

/// An entry is visible if it has live processes or its backing shmem still exists.
pub fn entry_visible(entry: &ShmemEntry) -> bool {
    if entry.is_empty() {
        return false;
    }
    if entry.pids.any_alive() {
        return true;
    }
    flink_reachable(entry.flink.as_str())
}

fn format_pids(entry: &ShmemEntry) -> String {
    let active = entry.pids.active_pids();
    match active.len() {
        0 => "pid=none".into(),
        1 => format!("pid={}", active[0]),
        n => {
            let list: Vec<String> = active.iter().map(|p| p.to_string()).collect();
            format!("pids({n})=[{}]", list.join(","))
        }
    }
}

fn read_start_time(pid: u32) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after_comm = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    // Field 19 after comm = starttime (field 21 in full stat, 0-indexed)
    let starttime_ticks: u64 = fields.get(19)?.parse().ok()?;
    let ticks_per_sec: u64 = 100; // sysconf(_SC_CLK_TCK), typically 100 on Linux

    let proc_stat = std::fs::read_to_string("/proc/stat").ok()?;
    let btime_line = proc_stat.lines().find(|l| l.starts_with("btime "))?;
    let btime_secs: u64 = btime_line.split_whitespace().nth(1)?.parse().ok()?;

    let start_secs = btime_secs + starttime_ticks / ticks_per_sec;
    let ts = std::time::UNIX_EPOCH + std::time::Duration::from_secs(start_secs);
    Some(humantime::format_rfc3339_seconds(ts).to_string())
}
