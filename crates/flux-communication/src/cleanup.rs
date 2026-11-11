use std::path::{Path, PathBuf};

use shared_memory::ShmemConf;
use tracing::warn;

/// Check whether a process is still running via `/proc/<pid>`.
pub fn is_pid_alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}
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
            warn!(%e, flink = %flink.display(), "failed to clean flink");
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
    // `file_type` does not follow symlinks: a link must never lead the walk, and
    // the removals after it, outside `root`.
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            out.extend(all_flinks_under(&entry.path()));
        } else if file_type.is_file() {
            out.push(entry.path());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #[test]
    fn cleanup_shmem_does_not_follow_symlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("victim"), "bogus_os_id").unwrap();
        let root = tmp.path().join("root");
        std::fs::create_dir_all(root.join("queues")).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("queues").join("link")).unwrap();
        std::os::unix::fs::symlink(&root, root.join("loop")).unwrap();

        super::cleanup_shmem(&root);
        assert!(outside.join("victim").exists());
        assert!(!root.exists());
    }
}
