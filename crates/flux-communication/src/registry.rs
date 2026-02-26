use std::path::{Path, PathBuf};

use shared_memory::ShmemConf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ShmemKind {
    #[default]
    Unknown = 0,
    Queue = 1,
    Data = 2,
    SeqlockArray = 3,
}

impl std::fmt::Display for ShmemKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::Queue => write!(f, "Queue"),
            Self::Data => write!(f, "Data"),
            Self::SeqlockArray => write!(f, "SeqlockArray"),
        }
    }
}

pub fn is_pid_alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

// ─── Free functions ─────────────────────────────────────────────────────────

/// Unlink the shmem backing for a flink, then remove the flink file.
///
/// Returns `Ok(())` on success. Returns `Err` with a description if the
/// flink file could not be removed (failure to open the shmem backing is
/// not considered an error — the backing may already be gone).
pub fn cleanup_flink(flink_path: &Path) -> Result<(), String> {
    if let Ok(mut shmem) = ShmemConf::new().flink(flink_path).open() {
        shmem.set_owner(true);
    }
    std::fs::remove_file(flink_path)
        .map_err(|e| {
            // NotFound is fine — the file was already removed.
            if e.kind() == std::io::ErrorKind::NotFound {
                return String::new();
            }
            format!("failed to remove {}: {e}", flink_path.display())
        })
        .or_else(|e| if e.is_empty() { Ok(()) } else { Err(e) })
}

/// Walk a directory tree, unlink all shmem backing files, then remove the tree.
pub fn cleanup_shmem(root: &Path) {
    for flink in all_flinks_under(root) {
        if let Err(e) = cleanup_flink(&flink) {
            eprintln!("warning: {e}");
        }
    }
    let _ = std::fs::remove_dir_all(root);
}

fn all_flinks_under(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if !root.exists() {
        return out;
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(all_flinks_under(&path));
        } else {
            out.push(path);
        }
    }
    out
}
