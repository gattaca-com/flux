//! CLI command implementations: `list`, `list_json`, `stats`, `inspect`,
//! `clean`.

use std::{io::IsTerminal, path::Path, sync::atomic::Ordering};

use crossterm::style::Stylize;
use flux_communication::{
    queue::QueueHeader,
    registry::{ShmemKind, cleanup_flink},
};
use serde::Serialize;
use shared_memory::ShmemConf;

use super::{
    inspect::{PoisonInfo, backing_file_size, format_bytes},
    registry::{DiscoveredEntry, app_names, entry_visible, flink_reachable, scan_base_dir},
};

/// List all visible shared memory segments in a human-readable table.
///
/// Scans `base_dir` for shared memory flinks, filters entries by `app_filter`
/// (if provided), and prints a columnar summary grouped by application name.
/// Each entry shows its kind, type, element size, capacity,
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
    let all_entries = scan_base_dir(base_dir);

    let entries: Vec<&DiscoveredEntry> = all_entries
        .iter()
        .filter(|e| entry_visible(e))
        .filter(|e| app_filter.is_none_or(|f| e.app_name == f))
        .collect();
    if entries.is_empty() {
        println!("No active segments found");
        return Ok(());
    }

    let color = std::io::stdout().is_terminal();
    let apps = app_names(&all_entries);
    println!("Found {} segments across {} apps\n", entries.len(), apps.len());

    // Compute column widths for aligned output.
    let max_kind_w = entries.iter().map(|e| format!("{}", e.kind).len()).max().unwrap_or(14).max(4);
    let max_type_w = entries.iter().map(|e| e.type_name.len()).max().unwrap_or(24).max(4);
    let max_elem_w =
        entries.iter().map(|e| format!("{}", e.elem_size).len()).max().unwrap_or(4).max(4);
    let max_cap_w =
        entries.iter().map(|e| format!("{}", e.capacity).len()).max().unwrap_or(4).max(3);

    for app in &apps {
        let app_entries: Vec<&&DiscoveredEntry> =
            entries.iter().filter(|e| e.app_name == *app).collect();
        if app_entries.is_empty() {
            continue;
        }
        println!("📦 {} ({} segments)", app, app_entries.len());
        // Column header
        println!(
            "  {:2} {:<max_kind_w$}  {:>max_type_w$}  {:>max_elem_w$}  {:>max_cap_w$}",
            " ", "KIND", "TYPE", "ELEM", "CAP",
        );
        for entry in &app_entries {
            let reachable = flink_reachable(&entry.flink);
            let poison = PoisonInfo::check(entry);
            let status = if poison.is_some() {
                "☠"
            } else if reachable {
                "🟢"
            } else {
                "💀"
            };
            let kind_str = format!("{}", entry.kind);
            let status_text = format!(
                "  {} {:<max_kind_w$}  {:>max_type_w$}  {:>max_elem_w$}B {:>max_cap_w$}",
                status, kind_str, entry.type_name, entry.elem_size, entry.capacity,
            );
            if color {
                if poison.is_some() {
                    println!("{}", status_text.yellow());
                } else if reachable {
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
                println!("    flink: {}", entry.flink);
                if entry.kind == ShmemKind::Queue {
                    match ShmemConf::new().flink(&entry.flink).open() {
                        Ok(shmem) => {
                            if shmem.len() >= std::mem::size_of::<QueueHeader>() {
                                let header = unsafe { &*(shmem.as_ptr() as *const QueueHeader) };
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
    alive: bool,
    poisoned: bool,
}

impl SegmentJson {
    fn from_entry(entry: &DiscoveredEntry) -> Self {
        let alive = flink_reachable(&entry.flink);
        let poisoned = PoisonInfo::check(entry).is_some();
        Self {
            app: entry.app_name.clone(),
            kind: format!("{}", entry.kind),
            type_name: entry.type_name.clone(),
            elem_size: entry.elem_size,
            capacity: entry.capacity,
            flink: entry.flink.clone(),
            alive,
            poisoned,
        }
    }
}

/// List all visible shared memory segments as pretty-printed JSON.
///
/// Produces an array of `SegmentJson` objects on stdout — one per visible
/// entry discovered under `base_dir` (optionally filtered by `app_filter`).
/// This is intended for machine consumption and piping into `jq` or similar
/// tools.
///
/// Prints `[]` if no segments are found under `base_dir`.
pub fn list_json(
    base_dir: &Path,
    app_filter: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let all_entries = scan_base_dir(base_dir);

    let segments: Vec<SegmentJson> = all_entries
        .iter()
        .filter(|e| entry_visible(e))
        .filter(|e| app_filter.is_none_or(|f| e.app_name == f))
        .map(SegmentJson::from_entry)
        .collect();

    if segments.is_empty() {
        println!("[]");
    } else {
        println!("{}", serde_json::to_string_pretty(&segments)?);
    }
    Ok(())
}

// ─── Stats ──────────────────────────────────────────────────────────────────

/// Print summary statistics for all registered shared memory segments.
///
/// Shows counts of alive/dead/poisoned segments, a breakdown by kind
/// (Queue, SeqlockArray, Data), total slot count, and estimated memory
/// usage.  Optionally filtered by `app_filter`.
///
/// When `verbose` is `true`, additional per-segment detail may be shown
/// in future versions.
pub fn stats(
    base_dir: &Path,
    app_filter: Option<&str>,
    _verbose: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let all_entries = scan_base_dir(base_dir);

    let entries: Vec<&DiscoveredEntry> = all_entries
        .iter()
        .filter(|e| entry_visible(e))
        .filter(|e| app_filter.is_none_or(|f| e.app_name == f))
        .collect();
    let apps: Vec<String> = {
        let mut names: Vec<String> = entries.iter().map(|e| e.app_name.clone()).collect();
        names.sort();
        names.dedup();
        names
    };

    let total = entries.len();
    let alive = entries.iter().filter(|e| flink_reachable(&e.flink)).count();
    let dead = total - alive;
    let poisoned = entries.iter().filter(|e| PoisonInfo::check(e).is_some()).count();

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
    println!("  Estimated memory: {}", format_bytes(total_elem_bytes));

    Ok(())
}

// ─── Inspect ────────────────────────────────────────────────────────────────

/// Print detailed inspection of one or more shared memory segments.
///
/// For each matching entry (filtered by `app_filter` and `segment_filter`),
/// displays kind, status, element size, capacity, flink path, backing file
/// size, write count (for queues), and poison status.
///
/// Output is colorised when stdout is a terminal.
pub fn inspect(
    base_dir: &Path,
    app_filter: Option<&str>,
    segment_filter: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let all_entries = scan_base_dir(base_dir);

    if all_entries.is_empty() {
        println!("No segments found");
        return Ok(());
    }

    for entry in &all_entries {
        if !entry_visible(entry) {
            continue;
        }
        if let Some(app) = app_filter &&
            entry.app_name != app
        {
            continue;
        }
        if let Some(seg) = segment_filter &&
            !entry.type_name.contains(seg)
        {
            continue;
        }

        let reachable = flink_reachable(&entry.flink);
        let poison = PoisonInfo::check(entry);
        let color = std::io::stdout().is_terminal();

        // ── Title ──
        let title = format!("─── {} — {} ───", entry.app_name, entry.type_name);
        if color {
            println!("{}", title.bold());
        } else {
            println!("{title}");
        }

        // ── Segment info (matches TUI detail view) ──
        let status = if poison.is_some() {
            "☠ poisoned"
        } else if reachable {
            "🟢 alive"
        } else {
            "💀 dead"
        };

        let backing = backing_file_size(&entry.flink)
            .map(format_bytes)
            .unwrap_or_else(|| "unavailable".into());

        println!("  Kind:       {}", entry.kind);
        println!("  Status:     {status}");
        println!("  Elem size:  {} bytes", entry.elem_size);
        println!("  Capacity:   {}", entry.capacity);
        println!("  Flink:      {}", entry.flink);
        println!("  Backing:    {backing}");

        if entry.kind == ShmemKind::Queue &&
            let Ok(shmem) = ShmemConf::new().flink(&entry.flink).open()
        {
            if shmem.len() >= std::mem::size_of::<QueueHeader>() {
                let header = unsafe { &*(shmem.as_ptr() as *const QueueHeader) };
                if header.is_initialized() {
                    println!("  Writes:     {}", header.count.load(Ordering::Relaxed));
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
        println!();
    }
    Ok(())
}

// ─── Clean ──────────────────────────────────────────────────────────────────

/// Remove stale shared memory segments.
///
/// Scans the filesystem for flink entries whose backing shmem is no longer
/// reachable. In dry-run mode (the default), lists the stale segments
/// without touching anything. When `force` is `true`, deletes the
/// flink files via [`cleanup_flink`].
///
/// Optionally filtered by `app_filter`.
pub fn clean(
    base_dir: &Path,
    app_filter: Option<&str>,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let all_entries = scan_base_dir(base_dir);

    // A segment is "stale" if its flink exists on disk but the backing shmem
    // cannot be opened (scan_base_dir already filters those out, so we
    // instead look at all discovered entries whose flink is still reachable
    // but that we consider dead).  Since PID tracking is removed, we treat
    // every discovered entry with a reachable flink as potentially stale
    // only when asked by the user — the `clean` command is explicitly about
    // removing flinks whose backing shmem is unreachable.
    //
    // Re-scan the filesystem for flink files that exist but whose shmem
    // backing cannot be opened.
    let stale: Vec<&DiscoveredEntry> = all_entries
        .iter()
        .filter(|e| app_filter.is_none_or(|f| e.app_name == f))
        .filter(|e| !flink_reachable(&e.flink))
        .collect();

    if stale.is_empty() {
        println!("No stale segments found");
        return Ok(());
    }

    println!("Found {} stale segments:", stale.len());
    for entry in &stale {
        println!("  💀 {} {}", entry.app_name, entry.type_name);
        if force {
            let flink_path = Path::new(&entry.flink);
            match cleanup_flink(flink_path) {
                Ok(()) => println!("    ✓ cleaned {}", flink_path.display()),
                Err(e) => eprintln!("    ✗ {e}"),
            }
        }
    }

    if !force {
        println!("\nDry run. Use --force to actually remove flink files.");
    }
    Ok(())
}
