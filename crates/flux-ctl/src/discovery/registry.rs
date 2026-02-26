//! Registry access, entry visibility, and PID formatting helpers.

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

/// Create the registry if needed, then scan the filesystem for pre-existing
/// shmem and register any missing entries. Stale flinks are removed.
pub fn scan(base_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let reg = ShmemRegistry::open_or_create(base_dir);
    reg.sweep_dead_pids();
    let result = reg.populate_from_fs(base_dir);
    println!("Scanned {}", base_dir.display());
    println!("  Registered:     {}", result.registered);
    println!("  Already known:  {}", result.already_known);
    println!("  Stale removed:  {}", result.stale_removed);
    Ok(())
}

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
