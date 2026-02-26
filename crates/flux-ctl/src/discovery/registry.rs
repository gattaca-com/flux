//! Registry access, entry visibility, and PID formatting helpers.
//!
//! # Safety audit (A4) — 2026-02-26
//!
//! Verified: no TUI code (`flux-ctl`) calls `open_shared()` or
//! `std::mem::forget(shmem)`.  All shmem opens go through the read-only
//! helpers in `discovery::inspect` which properly `drop(shmem)` after use.
//! The only `forget` calls live in `flux-communication::registry` (the
//! `open()` / `open_or_create()` methods on `ShmemRegistry` itself) where
//! the mapping must outlive the process.

use std::path::Path;

use flux_communication::registry::{REGISTRY_FLINK_NAME, ShmemEntry, ShmemRegistry};

/// Open the global registry, sweeping dead PIDs and populating from the
/// filesystem (so pre-existing shmem created before the registry is visible).
/// Stale flinks whose backing shmem no longer exists are removed.
pub fn open_registry(base_dir: &Path) -> Option<&'static ShmemRegistry> {
    let registry_path = base_dir.join(REGISTRY_FLINK_NAME);
    let reg = ShmemRegistry::open(&registry_path)?;
    reg.sweep_dead_pids();
    reg.populate_from_fs(base_dir);
    Some(reg)
}

/// Scan the filesystem for pre-existing shared memory segments and register them.
///
/// Creates the registry if it doesn't exist, sweeps dead PIDs, then calls
/// [`ShmemRegistry::populate_from_fs`] to discover shmem backing files under
/// `base_dir`. Prints a summary of registered, already-known, stale-removed,
/// and skipped (registry-full) entries.
pub fn scan(base_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let reg = ShmemRegistry::open_or_create(base_dir);
    reg.sweep_dead_pids();
    let result = reg.populate_from_fs(base_dir);
    println!("Scanned {}", base_dir.display());
    println!("  Registered:     {}", result.registered);
    println!("  Already known:  {}", result.already_known);
    println!("  Stale removed:  {}", result.stale_removed);
    println!("  Skipped (full): {}", result.skipped);
    Ok(())
}

/// Return sorted, deduplicated application names from all non-empty registry entries.
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

/// Check whether a flink's backing file exists and is non-empty.
///
/// Returns `false` for empty flink strings or missing/empty files.
/// This is a lightweight filesystem-only check — it does **not** open or
/// mmap the shared memory segment, avoiding the mmap leak that would occur
/// if `ShmemConf::open()` were used on a non-owner handle.
pub fn flink_reachable(flink: &str) -> bool {
    if flink.is_empty() {
        return false;
    }
    // Check that the flink file exists and is non-empty.  The shared_memory
    // crate writes the OS ID into the flink file, so a valid (reachable) flink
    // is always non-empty.  Previously this called `ShmemConf::open()` which
    // leaked the mmap on success (the returned `Shmem` was dropped without
    // being the owner, so the mapping was never unmapped).
    std::fs::metadata(flink)
        .map(|m| m.len() > 0)
        .unwrap_or(false)
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

/// Format an entry's attached PIDs as a compact display string.
///
/// Returns `"pid=none"` if no PIDs are attached, `"pid=<N>"` for a single
/// PID, or `"pids(<count>)=[<comma-separated>]"` for multiple PIDs.
pub fn format_pids(entry: &ShmemEntry) -> String {
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
