use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering, fence};
use std::time::SystemTime;

use flux_utils::ArrayStr;
use shared_memory::{ShmemConf, ShmemError};

pub const MAX_REGISTRY_ENTRIES: usize = 512;
pub const REGISTRY_FLINK_NAME: &str = "flux/_shmem_registry";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ShmemKind {
    Unknown = 0,
    Queue = 1,
    Data = 2,
    SeqlockArray = 3,
}

impl Default for ShmemKind {
    fn default() -> Self {
        Self::Unknown
    }
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

#[derive(Clone, Copy)]
#[repr(C)]
pub struct ShmemEntry {
    pub kind: ShmemKind,
    pub _pad0: [u8; 3],
    pub pid: u32,
    pub app_name: ArrayStr<64>,
    pub type_name: ArrayStr<64>,
    pub flink: ArrayStr<256>,
    pub type_hash: u64,
    pub elem_size: usize,
    pub capacity: usize,
    pub created_at_nanos: u64,
}

impl Default for ShmemEntry {
    fn default() -> Self {
        Self {
            kind: ShmemKind::Unknown,
            _pad0: [0; 3],
            pid: 0,
            app_name: ArrayStr::new(),
            type_name: ArrayStr::new(),
            flink: ArrayStr::new(),
            type_hash: 0,
            elem_size: 0,
            capacity: 0,
            created_at_nanos: 0,
        }
    }
}

impl ShmemEntry {
    pub fn is_empty(&self) -> bool {
        self.kind == ShmemKind::Unknown
    }
}

/// Global shared memory registry. ONE per base_dir, shared across all apps.
/// Stored at `<base_dir>/flux/_shmem_registry`.
///
/// Uses `ShmemConf` directly (not `ShmemData`) to avoid circular registration.
#[repr(C)]
pub struct ShmemRegistry {
    pub count: AtomicU32,
    pub _pad: u32,
    pub entries: [ShmemEntry; MAX_REGISTRY_ENTRIES],
}

impl ShmemRegistry {
    /// Open or create the global registry.
    /// Flink: `<base_dir>/flux/_shmem_registry`
    pub fn open_or_create(base_dir: &Path) -> &'static Self {
        let registry_path = base_dir.join(REGISTRY_FLINK_NAME);
        std::fs::create_dir_all(registry_path.parent().unwrap()).unwrap_or_else(|e| {
            panic!(
                "couldn't create registry dir {}: {e}",
                registry_path.parent().unwrap().display()
            )
        });

        match ShmemConf::new()
            .size(std::mem::size_of::<Self>())
            .flink(&registry_path)
            .create()
        {
            Ok(shmem) => {
                let ptr = shmem.as_ptr() as *mut Self;
                std::mem::forget(shmem);
                // Shmem is zero-initialized, count starts at 0
                unsafe { &*ptr }
            }
            Err(ShmemError::LinkExists) => match ShmemConf::new().flink(&registry_path).open() {
                Ok(shmem) => {
                    let ptr = shmem.as_ptr() as *const Self;
                    std::mem::forget(shmem);
                    unsafe { &*ptr }
                }
                Err(_) => {
                    let _ = std::fs::remove_file(&registry_path);
                    Self::open_or_create(base_dir)
                }
            },
            Err(e) => panic!("failed to create shmem registry: {e}"),
        }
    }

    /// Open existing registry read-only. Returns None if not found.
    pub fn open(registry_path: &Path) -> Option<&'static Self> {
        let shmem = ShmemConf::new().flink(registry_path).open().ok()?;
        let ptr = shmem.as_ptr() as *const Self;
        std::mem::forget(shmem);
        Some(unsafe { &*ptr })
    }

    /// Register a new shmem entry. Returns slot index or None if full.
    pub fn register(&self, entry: ShmemEntry) -> Option<u32> {
        let idx = self.count.fetch_add(1, Ordering::AcqRel);
        if idx as usize >= MAX_REGISTRY_ENTRIES {
            self.count.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        unsafe {
            // Compute the slot pointer directly from the base of self, avoiding
            // casting &T to *mut T (which is UB in Rust 2024).
            let base = self as *const Self as *mut u8;
            let offset = std::mem::offset_of!(Self, entries)
                + idx as usize * std::mem::size_of::<ShmemEntry>();
            let slot = base.add(offset) as *mut ShmemEntry;
            std::ptr::write_volatile(slot, entry);
        }
        fence(Ordering::Release);
        Some(idx)
    }

    /// Read all registered entries.
    pub fn entries(&self) -> &[ShmemEntry] {
        let count = self.count.load(Ordering::Acquire).min(MAX_REGISTRY_ENTRIES as u32);
        &self.entries[..count as usize]
    }

    pub fn entry_count(&self) -> u32 {
        self.count.load(Ordering::Acquire).min(MAX_REGISTRY_ENTRIES as u32)
    }

    /// Clean up shmem backing files for all entries matching the given app name.
    /// Opens each flink, sets `owner(true)` to unlink the `/dev/shm/` backing,
    /// then removes the flink file.
    pub fn cleanup_app(&self, app_name: &str) {
        for entry in self.entries() {
            if entry.app_name.as_str() == app_name {
                cleanup_flink(Path::new(entry.flink.as_str()));
            }
        }
    }

    /// Clean up shmem backing files for ALL registered entries.
    pub fn cleanup_all(&self) {
        for entry in self.entries() {
            if !entry.is_empty() {
                cleanup_flink(Path::new(entry.flink.as_str()));
            }
        }
    }

    /// Nuclear option: clean all registered entries, walk the filesystem for
    /// any unregistered shmem, clean the registry's own backing, remove dir tree.
    ///
    /// `base_dir` is the same directory passed to `open_or_create`.
    /// **Warning**: removes the entire directory tree under `base_dir`.
    pub fn destroy(base_dir: &Path) {
        let registry_path = base_dir.join(REGISTRY_FLINK_NAME);

        // Clean registered entries first (catches shmem whose flinks live
        // outside base_dir, e.g. in /dev/shm/).
        if let Some(registry) = Self::open(&registry_path) {
            registry.cleanup_all();
        }

        // Walk filesystem to catch any unregistered shmem (e.g. timing queues
        // created by tiles via direct Queue::create_or_open_shared calls).
        cleanup_shmem(base_dir);
    }
}

// ─── Free functions ─────────────────────────────────────────────────────────

/// Unlink the `/dev/shm/` backing file for a single flink, then remove the
/// flink itself. Safe to call on already-removed or non-existent paths.
pub fn cleanup_flink(flink_path: &Path) {
    if let Ok(mut shmem) = ShmemConf::new().flink(flink_path).open() {
        shmem.set_owner(true);
        // shmem drops here → unlinks /dev/shm/ backing file
    }
    let _ = std::fs::remove_file(flink_path);
}

/// Walk a directory tree, unlink all shmem backing files found via flinks,
/// then remove the entire directory tree.
pub fn cleanup_shmem(root: &Path) {
    for flink in all_flinks_under(root) {
        cleanup_flink(&flink);
    }
    let _ = std::fs::remove_dir_all(root);
}

fn all_flinks_under(root: &Path) -> Vec<std::path::PathBuf> {
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

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Helper to build a ShmemEntry for a queue.
pub fn queue_entry(
    app_name: &str,
    type_name: &str,
    flink: &str,
    elem_size: usize,
    capacity: usize,
) -> ShmemEntry {
    ShmemEntry {
        kind: ShmemKind::Queue,
        _pad0: [0; 3],
        pid: std::process::id(),
        app_name: ArrayStr::from_str_truncate(app_name),
        type_name: ArrayStr::from_str_truncate(type_name),
        flink: ArrayStr::from_str_truncate(flink),
        type_hash: 0,
        elem_size,
        capacity,
        created_at_nanos: now_nanos(),
    }
}

/// Helper to build a ShmemEntry for data.
pub fn data_entry(
    app_name: &str,
    type_name: &str,
    flink: &str,
    elem_size: usize,
) -> ShmemEntry {
    ShmemEntry {
        kind: ShmemKind::Data,
        _pad0: [0; 3],
        pid: std::process::id(),
        app_name: ArrayStr::from_str_truncate(app_name),
        type_name: ArrayStr::from_str_truncate(type_name),
        flink: ArrayStr::from_str_truncate(flink),
        type_hash: 0,
        elem_size,
        capacity: 1,
        created_at_nanos: now_nanos(),
    }
}
