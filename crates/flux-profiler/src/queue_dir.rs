use std::{path::PathBuf, sync::OnceLock};

use flux_communication::{
    cleanup::{cleanup_flink, cleanup_shmem, is_pid_alive},
    queue::{Queue, QueueType},
};
use flux_utils::directories::{local_share_dir, shmem_dir_queues};

use super::{
    allocator::AllocSample,
    mark::Mark,
    perf::{PerfSample, Schema},
};

pub(super) trait RingEntry: Copy + Default {
    const PREFIX: &'static str;
}

impl RingEntry for Mark {
    const PREFIX: &'static str = "events";
}

impl RingEntry for PerfSample {
    const PREFIX: &'static str = "perf-events";
}

impl RingEntry for AllocSample {
    const PREFIX: &'static str = "alloc-events";
}

/// 256k entries ≈ 6 MiB of marks per thread.
pub(super) const RING_CAPACITY: usize = 1 << 18;

#[derive(Clone)]
pub(super) struct QueueDir(PathBuf);

pub(super) static QUEUE_DIR: OnceLock<QueueDir> = OnceLock::new();

impl QueueDir {
    pub(super) fn new(app: &str) -> Self {
        let dir = shmem_dir_queues(app);
        let _ = std::fs::create_dir_all(&dir);
        Self(dir)
    }

    pub(super) fn write_pid(&self) {
        let _ = std::fs::write(self.0.join("pid"), std::process::id().to_string());
    }

    pub(super) fn live_pid(&self) -> Option<u32> {
        let pid: u32 = std::fs::read_to_string(self.0.join("pid")).ok()?.trim().parse().ok()?;
        is_pid_alive(pid).then_some(pid)
    }

    pub(super) fn publish_perf_schema(&self) {
        let path = self.0.join("perf_schema");
        #[cfg(feature = "perf")]
        {
            let names: Vec<_> = Schema::local().iter().map(|e| e.label.as_str()).collect();
            let _ = std::fs::write(path, names.join(","));
        }
        #[cfg(not(feature = "perf"))]
        let _ = std::fs::remove_file(path);
    }

    pub(super) fn perf_schema(&self) -> Option<Schema> {
        std::fs::read_to_string(self.0.join("perf_schema")).ok().map(|s| Schema::parse(&s))
    }

    pub(super) fn path<T: RingEntry>(&self, token: &str) -> PathBuf {
        self.0.join(format!("{}-{token}", T::PREFIX))
    }

    fn entries(&self) -> impl Iterator<Item = std::fs::DirEntry> {
        std::fs::read_dir(&self.0).into_iter().flatten().flatten()
    }

    /// Unlink a prior run's rings before producers create theirs, so the reader
    /// doesn't fold a vanished thread's stale ring as live data.
    pub(super) fn clear_stale(&self) {
        for entry in self.entries() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let is_ring = name.starts_with(Mark::PREFIX) ||
                name.starts_with(PerfSample::PREFIX) ||
                name.starts_with(AllocSample::PREFIX);
            if is_ring {
                let _ = cleanup_flink(&entry.path());
            }
        }
    }

    pub(super) fn ring<T: RingEntry>(&self, token: &str) -> Queue<T> {
        let path = self.path::<T>(token);
        // Discard any leftover backing under this stable name first: a crashed
        // run's ring or another process under the same app must never be
        // shared, or two producers would write the same ring and corrupt each
        // other.
        let _ = cleanup_flink(&path);
        Queue::create_or_open_shared(path, RING_CAPACITY, QueueType::SPMC)
    }

    pub(super) fn event_threads(&self) -> Vec<String> {
        self.entries()
            .filter_map(|e| {
                e.file_name()
                    .to_string_lossy()
                    .strip_prefix(Mark::PREFIX)
                    .and_then(|rest| rest.strip_prefix('-'))
                    .map(str::to_owned)
            })
            .collect()
    }
}

pub fn enable_profiler(app_name: &str) {
    let dir = QUEUE_DIR.get_or_init(|| {
        let dir = QueueDir::new(app_name);
        dir.clear_stale();
        dir
    });
    dir.publish_perf_schema();
    dir.write_pid();
}

const IN_PROCESS_APP_PREFIX: &str = "local-profiler-";

/// In-process mark ids are addresses in the producing process, so each process
/// needs its own app: a shared one lets a reader dereference foreign ids. Apps
/// of dead pids are reaped here, as nothing else unlinks their shmem.
pub(super) fn in_process_app() -> String {
    for entry in std::fs::read_dir(local_share_dir()).into_iter().flatten().flatten() {
        let name = entry.file_name();
        let pid = name
            .to_str()
            .and_then(|name| name.strip_prefix(IN_PROCESS_APP_PREFIX))
            .and_then(|pid| pid.parse().ok());
        if pid.is_some_and(|pid| !is_pid_alive(pid)) {
            cleanup_shmem(&entry.path());
        }
    }
    format!("{IN_PROCESS_APP_PREFIX}{}", std::process::id())
}

/// Every app under `local_share_dir()` whose queue dir names a live pid.
pub fn live_apps() -> Vec<(String, u32)> {
    let Ok(apps) = std::fs::read_dir(local_share_dir()) else { return Vec::new() };
    apps.flatten()
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter_map(|app| {
            let pid = QueueDir(shmem_dir_queues(&app)).live_pid()?;
            Some((app, pid))
        })
        .collect()
}

pub fn published_pid(app: &str) -> Option<u32> {
    QueueDir::new(app).live_pid()
}

#[cfg(any(test, feature = "test-util"))]
pub mod test_shmem {
    use std::sync::{Mutex, MutexGuard};

    use flux_communication::cleanup::cleanup_shmem;
    use flux_utils::directories::local_share_dir;

    use super::{QUEUE_DIR, QueueDir};

    /// Timing tests drive the process-global mode + reader and share one shmem
    /// dir, so they must run one at a time. nextest isolates per process;
    /// `cargo test` runs them as threads, so serialize here.
    static SERIAL: Mutex<()> = Mutex::new(());

    pub struct ShmemGuard {
        app: String,
        _serial: MutexGuard<'static, ()>,
    }

    impl ShmemGuard {
        #[allow(clippy::new_without_default)] // acquires a lock; Default would hide that
        pub fn new() -> Self {
            let serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
            let app = format!("profiler_test_{}", std::process::id());
            let _ = QUEUE_DIR.set(QueueDir::new(&app));
            Self { app, _serial: serial }
        }

        /// The app this guard registered (and will clean up) — what a test
        /// attaches to, since a guard-first `enable_profiler` name loses.
        pub fn app(&self) -> &str {
            &self.app
        }
    }

    impl Drop for ShmemGuard {
        fn drop(&mut self) {
            cleanup_shmem(&local_share_dir().join(&self.app));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_process_app_reaps_dead_pids_and_keeps_live_ones() {
        const PID_MAX_LIMIT: u32 = 1 << 22;
        let app_dir = |pid: u32| local_share_dir().join(format!("{IN_PROCESS_APP_PREFIX}{pid}"));
        let dead = app_dir(PID_MAX_LIMIT + 1);
        let live = app_dir(std::process::id());
        for dir in [&dead, &live] {
            std::fs::create_dir_all(dir).unwrap();
        }

        assert_eq!(in_process_app(), format!("{IN_PROCESS_APP_PREFIX}{}", std::process::id()));
        assert!(!dead.exists(), "a dead pid's app is reaped");
        assert!(live.exists(), "this process's app is kept");

        cleanup_shmem(&live);
    }
}
