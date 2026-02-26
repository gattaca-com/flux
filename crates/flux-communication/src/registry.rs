use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};
use std::time::{Duration, SystemTime};

use flux_utils::ArrayStr;
use shared_memory::{Shmem, ShmemConf, ShmemError};

pub const MAX_REGISTRY_ENTRIES: usize = 4096;
pub const MAX_PIDS_PER_ENTRY: usize = 256;
pub const REGISTRY_FLINK_NAME: &str = "flux/_shmem_registry";

const REGISTRY_MAGIC: u32 = u32::from_le_bytes(*b"FLXR");
const REGISTRY_VERSION: u32 = 3;

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

/// Fixed-size set of PIDs attached to a shared memory entry.
///
/// Slot value `0` = empty (sentinel). All operations are lock-free via CAS.
/// Capacity: [`MAX_PIDS_PER_ENTRY`] (256) PIDs per entry. Slot 0 holds the
/// creator PID by convention.
#[repr(C)]
pub struct PidSet {
    pids: [AtomicU32; MAX_PIDS_PER_ENTRY],
}

impl PidSet {
    /// Attach `pid` to this set. Returns `true` if the PID was inserted or
    /// was already present. Returns `false` only if all slots are occupied.
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

    /// Remove `pid` from this set. Returns `true` if found and removed.
    pub fn detach(&self, pid: u32) -> bool {
        for slot in &self.pids {
            if slot.load(Ordering::Relaxed) == pid
                && slot.compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Relaxed).is_ok()
            {
                return true;
            }
        }
        false
    }

    /// Return all non-zero PIDs currently in the set.
    pub fn active_pids(&self) -> Vec<u32> {
        self.pids
            .iter()
            .map(|s| s.load(Ordering::Relaxed))
            .filter(|&p| p != 0)
            .collect()
    }

    /// The PID that first created this entry (slot 0).
    pub fn creator_pid(&self) -> u32 {
        self.pids[0].load(Ordering::Relaxed)
    }

    /// Number of non-zero PIDs currently in the set.
    pub fn count(&self) -> usize {
        self.pids.iter().filter(|s| s.load(Ordering::Relaxed) != 0).count()
    }

    /// `true` if at least one PID in this set corresponds to a running process.
    pub fn any_alive(&self) -> bool {
        self.active_pids().iter().any(|&pid| is_pid_alive(pid))
    }

    /// CAS-zero all slots holding PIDs that are no longer running.
    pub fn sweep_dead(&self) -> usize {
        let mut removed = 0;
        for slot in &self.pids {
            let pid = slot.load(Ordering::Relaxed);
            if pid != 0 && !is_pid_alive(pid)
                && slot.compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Relaxed).is_ok()
            {
                removed += 1;
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

/// A single shared memory segment descriptor in the global registry.
///
/// Each entry records the segment kind, the owning application, the type
/// stored in the segment, the flink path used to open it, and a set of
/// PIDs that have this segment mapped.
///
/// Layout is `repr(C, align(64))` with a compile-time size assertion to
/// detect ABI drift. Bump [`REGISTRY_VERSION`] if the layout changes.
#[repr(C, align(64))]
pub struct ShmemEntry {
    /// What kind of shared memory segment this is.
    pub kind: ShmemKind,
    pub _pad0: [u8; 3],
    /// PIDs that have this segment mapped (lock-free CAS set).
    pub pids: PidSet,
    /// Name of the application that owns this segment (e.g. `"market-data"`).
    pub app_name: ArrayStr<64>,
    /// Short type name of the element stored (e.g. `"Quote"`).
    pub type_name: ArrayStr<64>,
    /// Absolute path to the flink file for `ShmemConf::flink()`.
    pub flink: ArrayStr<256>,
    /// Reserved for future type-hash validation (currently 0).
    pub type_hash: u64,
    /// Size in bytes of a single element.
    pub elem_size: usize,
    /// Number of slots (queues/arrays) or 1 (data).
    pub capacity: usize,
    /// Timestamp of first registration (nanos since Unix epoch).
    pub created_at_nanos: u64,
}

const _: () = {
    // ShmemEntry must be a stable size across processes. If this fires,
    // a field was added/removed — bump REGISTRY_VERSION.
    assert!(std::mem::size_of::<ShmemEntry>() == 1472);
    assert!(std::mem::align_of::<ShmemEntry>() == 64);
};

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

/// Snapshot of registry statistics produced by [`ShmemRegistry::stats()`].
#[derive(Debug, Clone, Default)]
pub struct RegistryStats {
    pub total: usize,
    pub empty: usize,
    /// Entry counts indexed by `ShmemKind as u8`.
    pub by_kind: [usize; 4],
    pub total_pids: usize,
    pub alive: usize,
    pub dead: usize,
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
    /// Timestamp (nanos since epoch) of last `populate_from_fs` call.
    pub last_populated_nanos: AtomicU64,
    pub entries: [ShmemEntry; MAX_REGISTRY_ENTRIES],
}

impl ShmemRegistry {
    /// Maximum number of retry attempts when the registry is corrupt or
    /// stale before giving up. Prevents infinite recursion / stack overflow.
    const MAX_OPEN_ATTEMPTS: u32 = 3;

    pub fn open_or_create(base_dir: &Path) -> &'static Self {
        let registry_path = base_dir.join(REGISTRY_FLINK_NAME);
        std::fs::create_dir_all(registry_path.parent().unwrap()).unwrap_or_else(|e| {
            panic!(
                "couldn't create registry dir {}: {e}",
                registry_path.parent().unwrap().display()
            )
        });

        for attempt in 0..Self::MAX_OPEN_ATTEMPTS {
            match ShmemConf::new()
                .size(std::mem::size_of::<Self>())
                .flink(&registry_path)
                .create()
            {
                Ok(shmem) => {
                    let ptr = shmem.as_ptr() as *mut Self;
                    // Store magic + version BEFORE forget so that concurrent
                    // openers (via LinkExists → open) see valid header data.
                    // Release on version acts as the "commit" flag.
                    let reg = unsafe { &*ptr };
                    reg.magic.store(REGISTRY_MAGIC, Ordering::Relaxed);
                    reg.version.store(REGISTRY_VERSION, Ordering::Release);
                    std::mem::forget(shmem);
                    return reg;
                }
                Err(ShmemError::LinkExists) => {
                    match ShmemConf::new().flink(&registry_path).open() {
                        Ok(shmem) => {
                            let ptr = shmem.as_ptr() as *const Self;
                            let magic = unsafe { &*(ptr as *const AtomicU32) };
                            if magic.load(Ordering::Acquire) != REGISTRY_MAGIC
                                || shmem.len() < std::mem::size_of::<Self>()
                            {
                                eprintln!(
                                    "flux: registry magic/size mismatch at {} (attempt {}/{}), recreating",
                                    registry_path.display(),
                                    attempt + 1,
                                    Self::MAX_OPEN_ATTEMPTS,
                                );
                                let mut shmem = shmem;
                                shmem.set_owner(true);
                                drop(shmem);
                                let _ = std::fs::remove_file(&registry_path);
                                continue;
                            }

                            let reg = unsafe { &*ptr };
                            if reg.version.load(Ordering::Acquire) != REGISTRY_VERSION {
                                eprintln!(
                                    "flux: registry version {} != expected {} (attempt {}/{}), recreating",
                                    reg.version.load(Ordering::Acquire),
                                    REGISTRY_VERSION,
                                    attempt + 1,
                                    Self::MAX_OPEN_ATTEMPTS,
                                );
                                // Reuse the already-opened shmem (don't open a second time).
                                let mut shmem = shmem;
                                shmem.set_owner(true);
                                drop(shmem);
                                let _ = std::fs::remove_file(&registry_path);
                                continue;
                            }

                            std::mem::forget(shmem);
                            return reg;
                        }
                        Err(_) => {
                            let _ = std::fs::remove_file(&registry_path);
                            continue;
                        }
                    }
                }
                Err(e) => panic!("failed to create shmem registry: {e}"),
            }
        }

        panic!(
            "flux: failed to open or create shmem registry at {} after {} attempts",
            registry_path.display(),
            Self::MAX_OPEN_ATTEMPTS,
        );
    }

    pub fn open(registry_path: &Path) -> Option<&'static Self> {
        let shmem = ShmemConf::new().flink(registry_path).open().ok()?;
        if shmem.len() < std::mem::size_of::<Self>() {
            return None;
        }
        let ptr = shmem.as_ptr() as *const Self;
        let reg = unsafe { &*ptr };
        if reg.magic.load(Ordering::Acquire) != REGISTRY_MAGIC
            || reg.version.load(Ordering::Acquire) != REGISTRY_VERSION
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

        // Fast path: check existing entries for a matching flink.
        let count = self.count.load(Ordering::Acquire).min(MAX_REGISTRY_ENTRIES as u32);
        for i in 0..count {
            let existing = &self.entries[i as usize];
            if existing.flink.as_str() == flink.as_str() && !existing.is_empty() {
                existing.pids.sweep_dead();
                existing.pids.attach(pid);
                return Some(i);
            }
        }

        // Claim a slot.
        let idx = self.count.fetch_add(1, Ordering::AcqRel);
        if idx as usize >= MAX_REGISTRY_ENTRIES {
            self.count.fetch_sub(1, Ordering::Relaxed);
            return None;
        }

        // Re-scan entries 0..idx for a duplicate flink that another process
        // may have registered between our scan above and the fetch_add.
        for i in 0..idx {
            let existing = &self.entries[i as usize];
            if existing.flink.as_str() == flink.as_str() && !existing.is_empty() {
                existing.pids.sweep_dead();
                existing.pids.attach(pid);
                // Abandon our claimed slot by leaving it as zeroed/Unknown.
                return Some(i);
            }
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

    /// Return a reference to the entry at `idx`, or `None` if out of bounds.
    pub fn entry_by_index(&self, idx: u32) -> Option<&ShmemEntry> {
        let count = self.count.load(Ordering::Acquire).min(MAX_REGISTRY_ENTRIES as u32);
        if idx < count {
            Some(&self.entries[idx as usize])
        } else {
            None
        }
    }

    /// Remove empty (kind == Unknown) entries by sliding non-empty entries
    /// toward the front and updating the count.
    ///
    /// # Safety contract
    ///
    /// This is **not** safe to call concurrently with [`register()`]. It is
    /// intended for offline maintenance (e.g. after a `clean --force`).
    pub fn compact(&self) -> usize {
        let old_count = self.count.load(Ordering::Acquire).min(MAX_REGISTRY_ENTRIES as u32);
        let mut write_idx: u32 = 0;

        for read_idx in 0..old_count {
            if !self.entries[read_idx as usize].is_empty() {
                if write_idx != read_idx {
                    // Clone the entry and write it to the compacted position
                    // using volatile writes, matching the pattern in register().
                    let entry = self.entries[read_idx as usize].clone();
                    unsafe {
                        let base = self as *const Self as *mut u8;
                        let offset = std::mem::offset_of!(Self, entries)
                            + write_idx as usize * std::mem::size_of::<ShmemEntry>();
                        let slot = base.add(offset) as *mut ShmemEntry;
                        std::ptr::write_volatile(slot, entry);
                    }
                    fence(Ordering::Release);

                    // Zero out the old slot to mark it empty.
                    unsafe {
                        let base = self as *const Self as *mut u8;
                        let offset = std::mem::offset_of!(Self, entries)
                            + read_idx as usize * std::mem::size_of::<ShmemEntry>();
                        let slot = base.add(offset) as *mut ShmemEntry;
                        std::ptr::write_volatile(slot, ShmemEntry::default());
                    }
                    fence(Ordering::Release);
                }
                write_idx += 1;
            }
        }

        self.count.store(write_idx, Ordering::Release);
        (old_count - write_idx) as usize
    }

    /// Compute statistics about the current registry contents.
    ///
    /// Returns a [`RegistryStats`] snapshot.
    pub fn stats(&self) -> RegistryStats {
        let count = self.count.load(Ordering::Acquire).min(MAX_REGISTRY_ENTRIES as u32) as usize;
        let mut stats = RegistryStats { total: count, ..Default::default() };

        for i in 0..count {
            let entry = &self.entries[i];
            if entry.is_empty() {
                stats.empty += 1;
                continue;
            }

            let kind_idx = entry.kind as u8 as usize;
            if kind_idx < stats.by_kind.len() {
                stats.by_kind[kind_idx] += 1;
            }

            let pid_count = entry.pids.count();
            stats.total_pids += pid_count;

            if entry.pids.any_alive() {
                stats.alive += 1;
            } else {
                stats.dead += 1;
            }
        }

        stats
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

    pub fn cleanup_app(&self, app_name: &str) -> Vec<String> {
        let mut errors = Vec::new();
        for entry in self.entries() {
            if entry.app_name.as_str() == app_name
                && let Err(e) = cleanup_flink(Path::new(entry.flink.as_str()))
            {
                errors.push(e);
            }
        }
        errors
    }

    pub fn cleanup_all(&self) -> Vec<String> {
        let mut errors = Vec::new();
        for entry in self.entries() {
            if !entry.is_empty()
                && let Err(e) = cleanup_flink(Path::new(entry.flink.as_str()))
            {
                errors.push(e);
            }
        }
        errors
    }

    /// Nuclear option: clean all registered entries, walk the filesystem for
    /// any unregistered shmem, then remove the entire directory tree.
    pub fn destroy(base_dir: &Path) {
        let registry_path = base_dir.join(REGISTRY_FLINK_NAME);
        if let Some(registry) = Self::open(&registry_path) {
            for e in registry.cleanup_all() {
                eprintln!("warning: {e}");
            }
        }
        cleanup_shmem(base_dir);
    }

    /// Find an entry by flink path. Returns the slot index if found.
    pub fn find_by_flink(&self, flink: &str) -> Option<u32> {
        let count = self.count.load(Ordering::Acquire).min(MAX_REGISTRY_ENTRIES as u32);
        for i in 0..count {
            let entry = &self.entries[i as usize];
            if entry.flink.as_str() == flink && !entry.is_empty() {
                return Some(i);
            }
        }
        None
    }

    /// Update the `elem_size` and `capacity` fields of an existing entry
    /// by opening its backing shmem and reading the header metadata.
    ///
    /// Used to backfill entries that were registered via the mtime fast-path
    /// (which skips the shmem open for recent flinks).
    ///
    /// Returns `true` if metadata was successfully updated.
    pub fn backfill_entry_meta(&self, idx: u32) -> bool {
        let Some(entry) = self.entry_by_index(idx) else {
            return false;
        };
        if entry.elem_size != 0 {
            return false; // already has metadata
        }
        let flink = entry.flink.as_str();
        let shmem = match ShmemConf::new().flink(flink).open() {
            Ok(s) => s,
            Err(_) => return false,
        };
        let (elem_size, capacity) = match entry.kind {
            ShmemKind::Queue => read_queue_meta(&shmem),
            ShmemKind::SeqlockArray => read_array_meta(&shmem),
            ShmemKind::Data => (shmem.len(), 1),
            _ => (0, 0),
        };
        drop(shmem);
        if elem_size == 0 {
            return false;
        }
        // Write updated metadata via volatile writes to the existing slot.
        unsafe {
            let base = self as *const Self as *mut u8;
            let entry_offset = std::mem::offset_of!(Self, entries)
                + idx as usize * std::mem::size_of::<ShmemEntry>();
            let elem_size_offset =
                entry_offset + std::mem::offset_of!(ShmemEntry, elem_size);
            let capacity_offset =
                entry_offset + std::mem::offset_of!(ShmemEntry, capacity);
            std::ptr::write_volatile(base.add(elem_size_offset) as *mut usize, elem_size);
            std::ptr::write_volatile(base.add(capacity_offset) as *mut usize, capacity);
        }
        fence(Ordering::Release);
        true
    }

    /// Scan the filesystem under `base_dir` for pre-existing shared memory
    /// flinks and register any that are not yet in the registry. Removes
    /// stale flinks whose backing shared memory no longer exists.
    ///
    /// Skips the scan if fewer than 5 seconds have passed since the last
    /// successful populate (tracked via `last_populated_nanos` in the
    /// registry header).
    ///
    /// **mtime fast-path:** flinks whose backing file was modified less than
    /// 60 seconds ago are registered immediately with stub metadata
    /// (`elem_size = 0, capacity = 0`) instead of opening the shmem.
    /// On the *next* scan, already-known entries with missing metadata are
    /// backfilled by opening their shmem once.
    pub fn populate_from_fs(&self, base_dir: &Path) -> PopulateResult {
        const POPULATE_COOLDOWN_SECS: u64 = 5;
        /// flinks younger than this are registered without opening shmem.
        const MTIME_FAST_PATH_SECS: u64 = 60;

        let now = now_nanos();
        let last = self.last_populated_nanos.load(Ordering::Relaxed);
        if last > 0 && now.saturating_sub(last) < POPULATE_COOLDOWN_SECS * 1_000_000_000 {
            return PopulateResult::default();
        }

        let mut result = PopulateResult::default();

        // ── Phase 0: backfill metadata for previously-stubbed entries ──
        let count = self.count.load(Ordering::Acquire).min(MAX_REGISTRY_ENTRIES as u32);
        for i in 0..count {
            let entry = &self.entries[i as usize];
            if !entry.is_empty() && entry.elem_size == 0 && self.backfill_entry_meta(i) {
                result.backfilled += 1;
            }
        }

        let Ok(dir_entries) = std::fs::read_dir(base_dir) else {
            return result;
        };

        for dir_entry in dir_entries.flatten() {
            let app_dir = dir_entry.path();
            if !app_dir.is_dir() {
                continue;
            }
            let app_name = dir_entry.file_name().to_string_lossy().to_string();
            let shmem_dir = app_dir.join("shmem");
            if !shmem_dir.is_dir() {
                continue;
            }

            for (subdir, kind) in &[
                ("queues", ShmemKind::Queue),
                ("data", ShmemKind::Data),
                ("arrays", ShmemKind::SeqlockArray),
            ] {
                let type_dir = shmem_dir.join(subdir);
                if !type_dir.is_dir() {
                    continue;
                }
                let Ok(flink_entries) = std::fs::read_dir(&type_dir) else {
                    continue;
                };
                for flink_entry in flink_entries.flatten() {
                    let flink_path = flink_entry.path();
                    if !flink_path.is_file() {
                        continue;
                    }

                    let flink_str = flink_path.to_string_lossy();
                    let type_name = flink_entry.file_name().to_string_lossy().to_string();

                    // Already registered — skip.
                    if self.find_by_flink(&flink_str).is_some() {
                        result.already_known += 1;
                        continue;
                    }

                    // ── mtime fast-path ──────────────────────────────
                    // If the flink file was modified very recently, register
                    // a stub entry without opening the shmem.  The expensive
                    // open + metadata read is deferred until the file ages
                    // past the threshold and gets backfilled in Phase 0.
                    let is_recent = flink_path
                        .metadata()
                        .ok()
                        .and_then(|m| m.modified().ok())
                        .and_then(|t| t.elapsed().ok())
                        .is_some_and(|age| age < Duration::from_secs(MTIME_FAST_PATH_SECS));

                    if is_recent {
                        let entry = ShmemEntry {
                            kind: *kind,
                            _pad0: [0; 3],
                            pids: pid_set_self(),
                            app_name: ArrayStr::from_str_truncate(&app_name),
                            type_name: ArrayStr::from_str_truncate(&type_name),
                            flink: ArrayStr::from_str_truncate(&flink_str),
                            type_hash: 0,
                            elem_size: 0,  // stub — backfilled on next scan
                            capacity: 0,
                            created_at_nanos: now_nanos(),
                        };
                        self.register(entry);
                        result.registered += 1;
                        continue;
                    }

                    // Try to open — failure means stale flink, remove it.
                    let shmem = match ShmemConf::new().flink(&flink_path).open() {
                        Ok(s) => s,
                        Err(_) => {
                            let _ = std::fs::remove_file(&flink_path);
                            result.stale_removed += 1;
                            continue;
                        }
                    };

                    let (elem_size, capacity) = match kind {
                        ShmemKind::Queue => read_queue_meta(&shmem),
                        ShmemKind::SeqlockArray => read_array_meta(&shmem),
                        ShmemKind::Data => (shmem.len(), 1),
                        _ => (0, 0),
                    };
                    drop(shmem); // unmap without unlinking (not owner)

                    let entry = ShmemEntry {
                        kind: *kind,
                        _pad0: [0; 3],
                        pids: pid_set_self(),
                        app_name: ArrayStr::from_str_truncate(&app_name),
                        type_name: ArrayStr::from_str_truncate(&type_name),
                        flink: ArrayStr::from_str_truncate(&flink_str),
                        type_hash: 0,
                        elem_size,
                        capacity,
                        created_at_nanos: now_nanos(),
                    };

                    self.register(entry);
                    result.registered += 1;
                }
            }
        }

        self.last_populated_nanos.store(now_nanos(), Ordering::Relaxed);
        result
    }
}

/// Result of [`ShmemRegistry::populate_from_fs`].
#[derive(Debug, Default)]
pub struct PopulateResult {
    /// Number of new entries registered during this scan.
    pub registered: usize,
    /// Number of stale flink files removed (backing shmem gone).
    pub stale_removed: usize,
    /// Number of flinks that were already in the registry.
    pub already_known: usize,
    /// Number of stub entries whose metadata was backfilled this scan.
    pub backfilled: usize,
}

fn read_queue_meta(shmem: &Shmem) -> (usize, usize) {
    use crate::queue::QueueHeader;
    if shmem.len() < std::mem::size_of::<QueueHeader>() {
        return (0, 0);
    }
    let header = unsafe { &*(shmem.as_ptr() as *const QueueHeader) };
    if !header.is_initialized() || header.elsize == 0 {
        return (0, 0);
    }
    (header.elsize, header.mask + 1)
}

fn read_array_meta(shmem: &Shmem) -> (usize, usize) {
    use crate::array::ArrayHeader;
    if shmem.len() < std::mem::size_of::<ArrayHeader>() {
        return (0, 0);
    }
    let header = unsafe { &*(shmem.as_ptr() as *const ArrayHeader) };
    if !header.is_initialized() || header.elsize == 0 {
        return (0, 0);
    }
    (header.elsize, header.bufsize)
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
    std::fs::remove_file(flink_path).map_err(|e| {
        // NotFound is fine — the file was already removed.
        if e.kind() == std::io::ErrorKind::NotFound {
            return String::new();
        }
        format!("failed to remove {}: {e}", flink_path.display())
    }).or_else(|e| if e.is_empty() { Ok(()) } else { Err(e) })
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
