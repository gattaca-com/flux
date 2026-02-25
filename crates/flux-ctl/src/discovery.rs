use std::path::Path;
use std::sync::atomic::Ordering;

use flux_communication::queue::QueueHeader;
use flux_communication::registry::{
    REGISTRY_FLINK_NAME, ShmemEntry, ShmemKind, ShmemRegistry, cleanup_flink,
};

/// Open the global registry at `<base_dir>/flux/_shmem_registry`
pub fn open_registry(base_dir: &Path) -> Option<&'static ShmemRegistry> {
    let registry_path = base_dir.join(REGISTRY_FLINK_NAME);
    ShmemRegistry::open(&registry_path)
}

/// Check if a PID is alive (Linux)
pub fn is_pid_alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

/// Get unique sorted app names from registry entries
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

    let entries = registry.entries();
    if entries.is_empty() {
        println!("Registry is empty (0 segments registered)");
        return Ok(());
    }

    let apps = app_names(registry);
    println!("Found {} segments across {} apps\n", entries.len(), apps.len());

    for app in &apps {
        let app_entries: Vec<&ShmemEntry> =
            entries.iter().filter(|e| e.app_name.as_str() == app).collect();
        println!("📦 {} ({} segments)", app, app_entries.len());
        for entry in &app_entries {
            let alive = is_pid_alive(entry.pid);
            let status = if alive { "🟢" } else { "💀" };
            println!(
                "  {} {:14} {:>24}  elem={}B  cap={}  pid={}",
                status,
                entry.kind,
                entry.type_name.as_str(),
                entry.elem_size,
                entry.capacity,
                entry.pid,
            );
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
        if entry.is_empty() {
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

        let alive = is_pid_alive(entry.pid);
        println!("─── {} ───", entry.type_name.as_str());
        println!("  App:       {}", entry.app_name.as_str());
        println!("  Kind:      {}", entry.kind);
        println!(
            "  PID:       {} {}",
            entry.pid,
            if alive { "(alive)" } else { "(dead)" }
        );
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
        if !is_pid_alive(entry.pid) {
            stale.push(entry);
        }
    }

    if stale.is_empty() {
        println!("No stale segments found");
        return Ok(());
    }

    println!("Found {} stale segments:", stale.len());
    for entry in &stale {
        println!(
            "  💀 {} {} (pid={})",
            entry.app_name.as_str(),
            entry.type_name.as_str(),
            entry.pid
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
