//! CLI command implementations: `list`, `list_json`, `stats`, `inspect`, `clean`.

use std::io::IsTerminal;
use std::path::Path;
use std::sync::atomic::Ordering;

use crossterm::style::Stylize;
use serde::Serialize;

use flux_communication::queue::QueueHeader;
use flux_communication::registry::{ShmemEntry, ShmemKind, cleanup_flink};
use shared_memory::ShmemConf;

use super::inspect::{check_poison, format_bytes, pids_info, backing_file_size};
use super::registry::{
    app_names, entry_visible, flink_reachable, format_pids, open_registry,
};

/// List all visible shared memory segments in a human-readable table.
///
/// Opens the global registry at `base_dir`, filters entries by `app_filter`
/// (if provided), and prints a columnar summary grouped by application name.
/// Each entry shows its kind, type, element size, capacity, attached PIDs,
/// and a status icon (`🟢` alive, `💀` dead, `☠` poisoned).
///
/// When `verbose` is `true`, the flink path and queue header details
/// (type, write count, capacity) are printed below each entry.
///
/// Output is colorised when stdout is a terminal.
pub fn list_all(
    base_dir: &Path,
    verbose: bool,
    app_filter: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(registry) = open_registry(base_dir) else {
        println!(
            "No flux shmem registry found at {}",
            base_dir.join(flux_communication::registry::REGISTRY_FLINK_NAME).display()
        );
        return Ok(());
    };

    let entries: Vec<&ShmemEntry> = registry
        .entries()
        .iter()
        .filter(|e| entry_visible(e))
        .filter(|e| app_filter.is_none_or(|f| e.app_name.as_str() == f))
        .collect();
    if entries.is_empty() {
        println!("No active segments found");
        return Ok(());
    }

    let color = std::io::stdout().is_terminal();
    let apps = app_names(registry);
    println!("Found {} segments across {} apps\n", entries.len(), apps.len());

    // Compute column widths for aligned output.
    let max_kind_w = entries.iter().map(|e| format!("{}", e.kind).len()).max().unwrap_or(14).max(4);
    let max_type_w = entries.iter().map(|e| e.type_name.as_str().len()).max().unwrap_or(24).max(4);
    let max_elem_w = entries
        .iter()
        .map(|e| format!("{}", e.elem_size).len())
        .max()
        .unwrap_or(4)
        .max(4);
    let max_cap_w = entries
        .iter()
        .map(|e| format!("{}", e.capacity).len())
        .max()
        .unwrap_or(4)
        .max(3);

    for app in &apps {
        let app_entries: Vec<&&ShmemEntry> =
            entries.iter().filter(|e| e.app_name.as_str() == app).collect();
        if app_entries.is_empty() {
            continue;
        }
        println!("📦 {} ({} segments)", app, app_entries.len());
        // Column header
        println!(
            "  {:2} {:<max_kind_w$}  {:>max_type_w$}  {:>max_elem_w$}  {:>max_cap_w$}  PIDS",
            " ", "KIND", "TYPE", "ELEM", "CAP",
        );
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
            let kind_str = format!("{}", entry.kind);
            let status_text = format!(
                "  {} {:<max_kind_w$}  {:>max_type_w$}  {:>max_elem_w$}B {:>max_cap_w$}  {}",
                status,
                kind_str,
                entry.type_name.as_str(),
                entry.elem_size,
                entry.capacity,
                pids,
            );
            if color {
                if poison.is_some() {
                    println!("{}", status_text.yellow());
                } else if alive {
                    println!("{}", status_text.green());
                } else {
                    println!("{}", status_text.red());
                }
            } else {
                println!("{status_text}");
            }
            if let Some(ref pi) = poison {
                println!(
                    "    ☠ POISONED: {}/{} slots (first at slot {})",
                    pi.n_poisoned, pi.total_slots, pi.first_slot,
                );
            }
            if verbose {
                println!("    flink: {}", entry.flink.as_str());
                if entry.kind == ShmemKind::Queue {
                    match ShmemConf::new().flink(entry.flink.as_str()).open() {
                        Ok(shmem) => {
                            if shmem.len() >= std::mem::size_of::<QueueHeader>() {
                                let header =
                                    unsafe { &*(shmem.as_ptr() as *const QueueHeader) };
                                if header.is_initialized() {
                                    println!(
                                        "    queue: type={:?}  writes={}  capacity={}",
                                        header.queue_type,
                                        header.count.load(Ordering::Relaxed),
                                        header.mask + 1,
                                    );
                                }
                            }
                            drop(shmem);
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

// ─── JSON output ────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct SegmentJson {
    app: String,
    kind: String,
    type_name: String,
    elem_size: usize,
    capacity: usize,
    flink: String,
    pids: Vec<u32>,
    alive: bool,
    poisoned: bool,
}

impl SegmentJson {
    fn from_entry(entry: &ShmemEntry) -> Self {
        let alive = entry.pids.any_alive();
        let poisoned = check_poison(entry).is_some();
        Self {
            app: entry.app_name.as_str().to_string(),
            kind: format!("{}", entry.kind),
            type_name: entry.type_name.as_str().to_string(),
            elem_size: entry.elem_size,
            capacity: entry.capacity,
            flink: entry.flink.as_str().to_string(),
            pids: entry.pids.active_pids(),
            alive,
            poisoned,
        }
    }
}

/// List all visible shared memory segments as pretty-printed JSON.
///
/// Produces an array of `SegmentJson` objects on stdout — one per visible
/// entry in the registry (optionally filtered by `app_filter`).  This is
/// intended for machine consumption and piping into `jq` or similar tools.
///
/// Prints `[]` if no registry exists at `base_dir`.
pub fn list_json(
    base_dir: &Path,
    app_filter: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(registry) = open_registry(base_dir) else {
        println!("[]");
        return Ok(());
    };

    let segments: Vec<SegmentJson> = registry
        .entries()
        .iter()
        .filter(|e| entry_visible(e))
        .filter(|e| app_filter.is_none_or(|f| e.app_name.as_str() == f))
        .map(SegmentJson::from_entry)
        .collect();

    println!("{}", serde_json::to_string_pretty(&segments)?);
    Ok(())
}

// ─── Stats ──────────────────────────────────────────────────────────────────

/// Print summary statistics for all registered shared memory segments.
///
/// Shows counts of alive/dead/poisoned segments, a breakdown by kind
/// (Queue, SeqlockArray, Data), total slot count, and estimated memory
/// usage.  Optionally filtered by `app_filter`.
///
/// When `verbose` is `true`, runs [`ShmemRegistry::health_check`] and
/// appends its diagnostic messages (duplicates, stubs, dead-unreachable
/// entries, capacity warnings).
pub fn stats(
    base_dir: &Path,
    app_filter: Option<&str>,
    verbose: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(registry) = open_registry(base_dir) else {
        println!("No registry found");
        return Ok(());
    };

    let entries: Vec<&ShmemEntry> = registry
        .entries()
        .iter()
        .filter(|e| entry_visible(e))
        .filter(|e| app_filter.is_none_or(|f| e.app_name.as_str() == f))
        .collect();
    let apps: Vec<String> = {
        let mut names: Vec<String> = entries
            .iter()
            .map(|e| e.app_name.as_str().to_string())
            .collect();
        names.sort();
        names.dedup();
        names
    };

    let total = entries.len();
    let alive = entries.iter().filter(|e| e.pids.any_alive()).count();
    let dead = total - alive;
    let poisoned = entries.iter().filter(|e| check_poison(e).is_some()).count();

    let mut queues = 0usize;
    let mut arrays = 0usize;
    let mut data = 0usize;
    let mut unknown = 0usize;
    let mut total_capacity: u64 = 0;
    let mut total_elem_bytes: u64 = 0;

    for entry in &entries {
        match entry.kind {
            ShmemKind::Queue => queues += 1,
            ShmemKind::SeqlockArray => arrays += 1,
            ShmemKind::Data => data += 1,
            ShmemKind::Unknown => unknown += 1,
        }
        total_capacity += entry.capacity as u64;
        total_elem_bytes += (entry.elem_size as u64) * (entry.capacity as u64);
    }

    let color = std::io::stdout().is_terminal();

    let title = match app_filter {
        Some(app) => format!("flux shmem stats (app: {app})"),
        None => "flux shmem stats".to_string(),
    };
    if color {
        println!("{}", title.bold().underlined());
    } else {
        println!("{title}");
    }
    println!();

    println!("  Apps:             {}", apps.len());
    println!("  Total segments:   {total}");

    if color {
        println!("  Alive:            {}", format!("{alive}").green());
        println!("  Dead:             {}", format!("{dead}").red());
        if poisoned > 0 {
            println!("  Poisoned:         {}", format!("{poisoned}").yellow());
        } else {
            println!("  Poisoned:         {poisoned}");
        }
    } else {
        println!("  Alive:            {alive}");
        println!("  Dead:             {dead}");
        println!("  Poisoned:         {poisoned}");
    }

    println!();
    println!("  By kind:");
    println!("    Queue:          {queues}");
    println!("    SeqlockArray:   {arrays}");
    println!("    Data:           {data}");
    if unknown > 0 {
        println!("    Unknown:        {unknown}");
    }

    println!();
    println!("  Total slots:      {total_capacity}");
    println!(
        "  Estimated memory: {}",
        format_bytes(total_elem_bytes)
    );

    if verbose {
        println!();
        if color {
            println!("{}", "  Health Check:".bold());
        } else {
            println!("  Health Check:");
        }
        for msg in registry.health_check() {
            println!("    • {msg}");
        }
    }

    Ok(())
}

// ─── Inspect ────────────────────────────────────────────────────────────────

/// Print detailed inspection of one or more shared memory segments.
///
/// For each matching entry (filtered by `app_filter` and `segment_filter`),
/// displays kind, status, element size, capacity, type hash, creation time,
/// flink path, backing file size, write count (for queues), poison status,
/// and a table of attached processes with PID, status, name, and command line.
///
/// Output is colorised when stdout is a terminal.
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
        if let Some(app) = app_filter
            && entry.app_name.as_str() != app
        {
            continue;
        }
        if let Some(seg) = segment_filter
            && !entry.type_name.as_str().contains(seg)
        {
            continue;
        }

        let alive = entry.pids.any_alive();
        let poison = check_poison(entry);
        let color = std::io::stdout().is_terminal();

        // ── Title ──
        let title = format!(
            "─── {} — {} ───",
            entry.app_name.as_str(),
            entry.type_name.as_str()
        );
        if color {
            println!("{}", title.bold());
        } else {
            println!("{title}");
        }

        // ── Segment info (matches TUI detail view) ──
        let status = if poison.is_some() {
            "☠ poisoned"
        } else if alive {
            "🟢 alive"
        } else {
            "💀 dead"
        };

        let created = if entry.created_at_nanos > 0 {
            let secs = entry.created_at_nanos / 1_000_000_000;
            let ts = std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs);
            humantime::format_rfc3339_seconds(ts).to_string()
        } else {
            "unknown".into()
        };

        let backing = backing_file_size(entry.flink.as_str())
            .map(format_bytes)
            .unwrap_or_else(|| "unavailable".into());

        println!("  Kind:       {}", entry.kind);
        println!("  Status:     {status}");
        println!("  Elem size:  {} bytes", entry.elem_size);
        println!("  Capacity:   {}", entry.capacity);
        println!("  Type hash:  0x{:016x}", entry.type_hash);
        println!("  Created:    {created}");
        println!("  Flink:      {}", entry.flink.as_str());
        println!("  Backing:    {backing}");

        if entry.kind == ShmemKind::Queue
            && let Ok(shmem) = ShmemConf::new().flink(entry.flink.as_str()).open()
        {
            if shmem.len() >= std::mem::size_of::<QueueHeader>() {
                let header = unsafe { &*(shmem.as_ptr() as *const QueueHeader) };
                if header.is_initialized() {
                    println!(
                        "  Writes:     {}",
                        header.count.load(Ordering::Relaxed)
                    );
                }
            }
            drop(shmem);
        }

        if let Some(ref pi) = poison {
            let poison_line = format!(
                "  ☠ Poison:   {}/{} slots poisoned (first at slot {})",
                pi.n_poisoned, pi.total_slots, pi.first_slot
            );
            if color {
                println!("{}", poison_line.red());
            } else {
                println!("{poison_line}");
            }
        }

        // ── Attached processes (matches TUI PID table) ──
        let pids = pids_info(entry);
        let n_alive = pids.iter().filter(|p| p.alive).count();
        let n_dead = pids.iter().filter(|p| !p.alive).count();
        println!();
        println!(
            "  Attached Processes ({} alive, {} dead)",
            n_alive, n_dead,
        );
        if pids.is_empty() {
            println!("    No PIDs attached");
        } else {
            println!(
                "    {:<10} {:<8} {:<20} COMMAND",
                "PID", "STATUS", "PROCESS",
            );
            for p in &pids {
                let status_str = if p.alive { "alive" } else { "dead" };
                let name = if p.name.is_empty() { "—" } else { &p.name };
                let cmd = if p.cmdline.is_empty() {
                    "—".to_string()
                } else if p.cmdline.len() > 60 {
                    format!("{}…", &p.cmdline[..59])
                } else {
                    p.cmdline.clone()
                };
                println!(
                    "    {:<10} {:<8} {:<20} {}",
                    p.pid, status_str, name, cmd,
                );
            }
        }
        println!();
    }
    Ok(())
}

// ─── Clean ──────────────────────────────────────────────────────────────────

/// Remove stale (dead-PID) shared memory segments.
///
/// Scans the registry for entries with no alive PIDs whose flink backing
/// file still exists. In dry-run mode (the default), lists the stale
/// segments without touching anything. When `force` is `true`, deletes the
/// flink files via [`cleanup_flink`] and compacts the registry afterwards.
///
/// Optionally filtered by `app_filter`.
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
        if let Some(app) = app_filter
            && entry.app_name.as_str() != app
        {
            continue;
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
            match cleanup_flink(flink_path) {
                Ok(()) => println!("    ✓ cleaned {}", flink_path.display()),
                Err(e) => eprintln!("    ✗ {e}"),
            }
        }
    }

    if force {
        let compacted = registry.compact();
        if compacted > 0 {
            println!("  Compacted registry: removed {compacted} empty slots");
        }
    } else {
        println!("\nDry run. Use --force to actually remove flink files.");
    }
    Ok(())
}
