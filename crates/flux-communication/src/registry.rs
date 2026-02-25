use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering, fence};
use std::time::SystemTime;

use flux_utils::ArrayStr;
use shared_memory::{ShmemConf, ShmemError};

pub const MAX_REGISTRY_ENTRIES: usize = 4096;
pub const MAX_PIDS_PER_ENTRY: usize = 256;
pub const REGISTRY_FLINK_NAME: &str = "flux/_shmem_registry";

const REGISTRY_MAGIC: u32 = u32::from_le_bytes(*b"FLXR");
const REGISTRY_VERSION: u32 = 2;

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

/// Fixed-size set of PIDs. Slot value 0 = empty. All operations are lock-free CAS.
#[repr(C)]
pub struct PidSet {
    pids: [AtomicU32; MAX_PIDS_PER_ENTRY],
}

impl PidSet {
    pub fn attach(&self, pid: u32) -> bool {
        debug_assert!(pid != 0, "PID 0 is reserved as the empty sentinel");
        for slot in &self.pids {
            let cur = slot.load(Ordering::Relaxed);
            if cur == pid {
                return true;
            }
            if cur == 0 {
                match slot.compare_exchange(0, pid, Ordering::AcqRel, Ordering::Relaxed) {
                    Ok(_) => return true,
                    Err(actual) if actual == pid => return true,
                    Err(_) => continue,
                }
            }
        }
        false
    }

    pub fn detach(&self, pid: u32) -> bool {
        for slot in &self.pids {
            if slot.load(Ordering::Relaxed) == pid {
                if slot.compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                    return true;
                }
            }
        }
        false
    }

    pub fn active_pids(&self) -> Vec<u32> {
        self.pids
            .iter()
            .map(|s| s.load(Ordering::Relaxed))
            .filter(|&p| p != 0)
            .collect()
    }

    pub fn creator_pid(&self) -> u32 {
        self.pids[0].load(Ordering::Relaxed)
    }

    pub fn count(&self) -> usize {
        self.pids.iter().filter(|s| s.load(Ordering::Relaxed) != 0).count()
    }

    pub fn any_alive(&self) -> bool {
        self.active_pids().iter().any(|&pid| is_pid_alive(pid))
    }

    /// CAS-zero all slots holding PIDs that are no longer running.
    pub fn sweep_dead(&self) -> usize {
        let mut removed = 0;
        for slot in &self.pids {
            let pid = slot.load(Ordering::Relaxed);
            if pid != 0 && !is_pid_alive(pid) {
                if slot.compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                    removed += 1;
                }
            }
        }
        removed
    }
}

impl Clone for PidSet {
    fn clone(&self) -> Self {
        let mut pids = [const { AtomicU32::new(0) }; MAX_PIDS_PER_ENTRY];
        for (dst, src) in pids.iter_mut().zip(self.pids.iter()) {
            *dst = AtomicU32::new(src.load(Ordering::Relaxed));
        }
        Self { pids }
    }
}

impl Default for PidSet {
    fn default() -> Self {
        Self {
            pids: [const { AtomicU32::new(0) }; MAX_PIDS_PER_ENTRY],
        }
    }
}

pub fn is_pid_alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

#[repr(C)]
pub struct ShmemEntry {
    pub kind: ShmemKind,
    pub _pad0: [u8; 3],
    pub pids: PidSet,
    pub app_name: ArrayStr<64>,
    pub type_name: ArrayStr<64>,
    pub flink: ArrayStr<256>,
    pub type_hash: u64,
    pub elem_size: usize,
    pub capacity: usize,
    pub created_at_nanos: u64,
}

impl Clone for ShmemEntry {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            _pad0: self._pad0,
            pids: self.pids.clone(),
            app_name: self.app_name,
            type_name: self.type_name,
            flink: self.flink,
            type_hash: self.type_hash,
            elem_size: self.elem_size,
            capacity: self.capacity,
            created_at_nanos: self.created_at_nanos,
        }
    }
}

impl Default for ShmemEntry {
    fn default() -> Self {
        Self {
            kind: ShmemKind::Unknown,
            _pad0: [0; 3],
            pids: PidSet::default(),
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

    pub fn creator_pid(&self) -> u32 {
        self.pids.creator_pid()
    }
}

/// Global shared memory registry. One per `base_dir`, shared across all apps.
/// Flink: `<base_dir>/flux/_shmem_registry`.
///
/// Uses `ShmemConf` directly (not `ShmemData`) to avoid circular registration.
#[repr(C)]
pub struct ShmemRegistry {
    pub magic: AtomicU32,
    pub version: AtomicU32,
    pub count: AtomicU32,
    pub _pad: u32,
    pub entries: [ShmemEntry; MAX_REGISTRY_ENTRIES],
}

impl ShmemRegistry {
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
                let reg = unsafe { &*ptr };
                reg.magic.store(REGISTRY_MAGIC, Ordering::Relaxed);
                reg.version.store(REGISTRY_VERSION, Ordering::Relaxed);
                reg
            }
            Err(ShmemError::LinkExists) => match ShmemConf::new().flink(&registry_path).open() {
                Ok(shmem) => {
                    let ptr = shmem.as_ptr() as *const Self;
                    let magic = unsafe { &*(ptr as *const AtomicU32) };
                    if magic.load(Ordering::Relaxed) != REGISTRY_MAGIC
                        || shmem.len() < std::mem::size_of::<Self>()
                    {
                        eprintln!(
                            "flux: registry version mismatch at {}, recreating",
                            registry_path.display()
                        );
                        let mut shmem = shmem;
                        shmem.set_owner(true);
                        drop(shmem);
                        let _ = std::fs::remove_file(&registry_path);
                        return Self::open_or_create(base_dir);
                    }

                    let reg = unsafe { &*ptr };
                    if reg.version.load(Ordering::Relaxed) != REGISTRY_VERSION {
                        eprintln!(
                            "flux: registry version {} != expected {}, recreating",
                            reg.version.load(Ordering::Relaxed),
                            REGISTRY_VERSION,
                        );
                        let shmem = ShmemConf::new().flink(&registry_path).open().unwrap();
                        let mut shmem = shmem;
                        shmem.set_owner(true);
                        drop(shmem);
                        let _ = std::fs::remove_file(&registry_path);
                        return Self::open_or_create(base_dir);
                    }

                    std::mem::forget(shmem);
                    reg
                }
                Err(_) => {
                    let _ = std::fs::remove_file(&registry_path);
                    Self::open_or_create(base_dir)
                }
            },
            Err(e) => panic!("failed to create shmem registry: {e}"),
        }
    }

    pub fn open(registry_path: &Path) -> Option<&'static Self> {
        let shmem = ShmemConf::new().flink(registry_path).open().ok()?;
        if shmem.len() < std::mem::size_of::<Self>() {
            return None;
        }
        let ptr = shmem.as_ptr() as *const Self;
        let reg = unsafe { &*ptr };
        if reg.magic.load(Ordering::Relaxed) != REGISTRY_MAGIC
            || reg.version.load(Ordering::Relaxed) != REGISTRY_VERSION
        {
            return None;
        }
        std::mem::forget(shmem);
        Some(reg)
    }

    /// Register a new entry or attach this PID to an existing entry with the
    /// same flink. Sweeps dead PIDs on reattach. Returns the slot index.
    pub fn register(&self, entry: ShmemEntry) -> Option<u32> {
        let pid = entry.pids.creator_pid();
        let flink = entry.flink;

        let count = self.count.load(Ordering::Acquire).min(MAX_REGISTRY_ENTRIES as u32);
        for i in 0..count {
            let existing = &self.entries[i as usize];
            if existing.flink.as_str() == flink.as_str() && !existing.is_empty() {
                existing.pids.sweep_dead();
                existing.pids.attach(pid);
                return Some(i);
            }
        }

        let idx = self.count.fetch_add(1, Ordering::AcqRel);
        if idx as usize >= MAX_REGISTRY_ENTRIES {
            self.count.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        unsafe {
            let base = self as *const Self as *mut u8;
            let offset = std::mem::offset_of!(Self, entries)
                + idx as usize * std::mem::size_of::<ShmemEntry>();
            let slot = base.add(offset) as *mut ShmemEntry;
            std::ptr::write_volatile(slot, entry);
        }
        fence(Ordering::Release);
        Some(idx)
    }

    pub fn attach(&self, flink: &str) -> bool {
        self.attach_pid(flink, std::process::id())
    }

    pub fn attach_pid(&self, flink: &str, pid: u32) -> bool {
        let count = self.count.load(Ordering::Acquire).min(MAX_REGISTRY_ENTRIES as u32);
        for i in 0..count {
            let existing = &self.entries[i as usize];
            if existing.flink.as_str() == flink && !existing.is_empty() {
                return existing.pids.attach(pid);
            }
        }
        false
    }

    pub fn detach_pid(&self, flink: &str, pid: u32) -> bool {
        let count = self.count.load(Ordering::Acquire).min(MAX_REGISTRY_ENTRIES as u32);
        for i in 0..count {
            let existing = &self.entries[i as usize];
            if existing.flink.as_str() == flink && !existing.is_empty() {
                return existing.pids.detach(pid);
            }
        }
        false
    }

    pub fn entries(&self) -> &[ShmemEntry] {
        let count = self.count.load(Ordering::Acquire).min(MAX_REGISTRY_ENTRIES as u32);
        &self.entries[..count as usize]
    }

    pub fn entry_count(&self) -> u32 {
        self.count.load(Ordering::Acquire).min(MAX_REGISTRY_ENTRIES as u32)
    }

    pub fn sweep_dead_pids(&self) -> usize {
        let mut total = 0;
        for entry in self.entries() {
            if !entry.is_empty() {
                total += entry.pids.sweep_dead();
            }
        }
        total
    }

    pub fn cleanup_app(&self, app_name: &str) {
        for entry in self.entries() {
            if entry.app_name.as_str() == app_name {
                cleanup_flink(Path::new(entry.flink.as_str()));
            }
        }
    }

    pub fn cleanup_all(&self) {
        for entry in self.entries() {
            if !entry.is_empty() {
                cleanup_flink(Path::new(entry.flink.as_str()));
            }
        }
    }

    /// Nuclear option: clean all registered entries, walk the filesystem for
    /// any unregistered shmem, then remove the entire directory tree.
    pub fn destroy(base_dir: &Path) {
        let registry_path = base_dir.join(REGISTRY_FLINK_NAME);
        if let Some(registry) = Self::open(&registry_path) {
            registry.cleanup_all();
        }
        cleanup_shmem(base_dir);
    }
}

// ─── Free functions ─────────────────────────────────────────────────────────

/// Unlink the shmem backing for a flink, then remove the flink file.
/// Safe to call on already-removed paths.
pub fn cleanup_flink(flink_path: &Path) {
    if let Ok(mut shmem) = ShmemConf::new().flink(flink_path).open() {
        shmem.set_owner(true);
    }
    let _ = std::fs::remove_file(flink_path);
}

/// Walk a directory tree, unlink all shmem backing files, then remove the tree.
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

fn pid_set_self() -> PidSet {
    let set = PidSet::default();
    set.attach(std::process::id());
    set
}

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
        pids: pid_set_self(),
        app_name: ArrayStr::from_str_truncate(app_name),
        type_name: ArrayStr::from_str_truncate(type_name),
        flink: ArrayStr::from_str_truncate(flink),
        type_hash: 0,
        elem_size,
        capacity,
        created_at_nanos: now_nanos(),
    }
}

pub fn data_entry(
    app_name: &str,
    type_name: &str,
    flink: &str,
    elem_size: usize,
) -> ShmemEntry {
    ShmemEntry {
        kind: ShmemKind::Data,
        _pad0: [0; 3],
        pids: pid_set_self(),
        app_name: ArrayStr::from_str_truncate(app_name),
        type_name: ArrayStr::from_str_truncate(type_name),
        flink: ArrayStr::from_str_truncate(flink),
        type_hash: 0,
        elem_size,
        capacity: 1,
        created_at_nanos: now_nanos(),
    }
}

pub fn seqlock_array_entry(
    app_name: &str,
    type_name: &str,
    flink: &str,
    elem_size: usize,
    capacity: usize,
) -> ShmemEntry {
    ShmemEntry {
        kind: ShmemKind::SeqlockArray,
        _pad0: [0; 3],
        pids: pid_set_self(),
        app_name: ArrayStr::from_str_truncate(app_name),
        type_name: ArrayStr::from_str_truncate(type_name),
        flink: ArrayStr::from_str_truncate(flink),
        type_hash: 0,
        elem_size,
        capacity,
        created_at_nanos: now_nanos(),
    }
}
