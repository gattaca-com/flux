//! Opening, creating and retiring the shared memory segment behind a queue.
//!
//! Every open of a queue directory's flinks holds an exclusive `flock` on the
//! directory, so validation, retirement and creation never interleave across
//! processes or threads. A segment that fails validation is retired: its
//! object is truncated to zero, then unlinked and replaced. Truncation turns
//! every remaining access to it into SIGBUS: processes that mapped it through
//! this module shut down through SIGTERM (see [`super::fault`]), others, such
//! as older binaries or tools mapping it directly, crash.
//!
//! Finding other processes' mappings and truncating are Linux only; elsewhere
//! a mismatched segment is unlinked and replaced while its mappers keep it.

use std::{
    ffi::CString,
    fs::File,
    io,
    mem::ManuallyDrop,
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::ffi::OsStrExt,
    },
    path::{Path, PathBuf},
};

use shared_memory::{ShmemConf, ShmemError};

use super::{QueueHeader, QueueIdentity, fault};
use crate::error::QueueError;

/// Set to `1` to retire a mismatched queue segment that other live processes
/// still map. Poisoned segments are retired regardless; see
/// [`may_retire_mapped`].
pub const RETIRE_MAPPED_QUEUES_ENV: &str = "FLUX_RETIRE_MAPPED_QUEUES";

/// Retire policy for a segment that other live processes still map. A
/// poisoned slot means its writer died mid-write and attached readers are
/// stuck on it, so the next process that opens the queue with
/// `create_or_open_shared` retires the segment. Any other mismatch is refused
/// unless [`RETIRE_MAPPED_QUEUES_ENV`] is `1`.
fn may_retire_mapped(reason: &QueueError) -> bool {
    matches!(reason, QueueError::ElementPoisoned(_)) ||
        std::env::var_os(RETIRE_MAPPED_QUEUES_ENV).is_some_and(|v| v == "1")
}

/// A whole shm object, mapped read-write, and its fd.
pub(super) struct Segment {
    ptr: *mut u8,
    len: usize,
    file: File,
}

impl Segment {
    /// `None` when no usable object has that name: it is gone (every flink is
    /// stale after a reboot), a retire that died before unlinking it left it
    /// empty, or the flink does not hold a valid name.
    fn open(os_id: &str) -> io::Result<Option<Self>> {
        let Ok(name) = CString::new(os_id) else {
            return Ok(None);
        };
        let fd = unsafe { libc::shm_open(name.as_ptr(), libc::O_RDWR, 0) };
        if fd < 0 {
            let e = io::Error::last_os_error();
            return match e.raw_os_error() {
                Some(libc::ENOENT | libc::EINVAL | libc::ENAMETOOLONG) => Ok(None),
                _ => Err(e),
            };
        }
        let file = unsafe { File::from_raw_fd(fd) };
        let len = file.metadata()?.len() as usize;
        if len == 0 {
            return Ok(None);
        }
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Some(Self { ptr: ptr.cast(), len, file }))
    }

    fn identity(&self) -> Option<QueueIdentity> {
        QueueHeader::from_mapping(self.ptr, self.len).ok()?.identity()
    }

    /// Unmaps the object, keeping its fd.
    fn into_file(self) -> File {
        let this = ManuallyDrop::new(self);
        unsafe {
            libc::munmap(this.ptr.cast(), this.len);
            std::ptr::read(&raw const this.file)
        }
    }

    /// Keeps the mapping and the fd for the life of the process.
    pub(super) fn leak(self) -> (*mut u8, usize, RawFd) {
        let this = ManuallyDrop::new(self);
        (this.ptr, this.len, this.file.as_raw_fd())
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.ptr.cast(), self.len) };
    }
}

/// Maps the queue segment at `flink`. An existing segment is accepted when
/// `open` accepts it and retired otherwise; a missing or retired one is
/// created and handed to `init` before the directory lock is released.
pub(crate) fn create_or_open<R>(
    flink: &Path,
    size: usize,
    type_name: &'static str,
    type_hash: u64,
    open: impl FnOnce(*mut u8, usize) -> Result<R, QueueError>,
    init: impl FnOnce(*mut u8) -> R,
) -> R {
    let dir = dir_of(flink);
    let _ = std::fs::create_dir_all(dir);
    let _lock =
        lock_dir(dir).unwrap_or_else(|e| panic!("couldn't lock queue dir {}: {e}", dir.display()));
    // Under the lock: registration happens under it too.
    fault::check_collision(flink, type_name, type_hash);

    let existing = map_existing(flink)
        .unwrap_or_else(|e| panic!("couldn't open queue segment {}: {e}", flink.display()));
    match existing {
        Existing::Missing => {}
        Existing::Stale(os_id) => remove_stale(flink, &os_id),
        Existing::Mapped(os_id, segment) => match open(segment.ptr, segment.len) {
            Ok(queue) => {
                fault::register(segment, flink, type_name);
                return queue;
            }
            Err(reason) => {
                let previous = segment.identity();
                // Unmap before truncating: this process must never touch the old object again.
                retire(flink, &os_id, &segment.into_file(), previous, &reason, type_hash);
            }
        },
    }

    let segment = create(flink, size);
    let queue = init(segment.ptr);
    fault::register(segment, flink, type_name);
    queue
}

/// Maps an existing queue segment without ever creating or retiring one.
pub(crate) fn open<R>(
    flink: &Path,
    type_name: &'static str,
    open: impl FnOnce(*mut u8, usize) -> Result<R, QueueError>,
) -> Result<R, QueueError> {
    if !flink.exists() {
        return Err(QueueError::NonExistingFile);
    }
    let _lock = lock_dir(dir_of(flink)).map_err(ShmemError::LinkOpenFailed)?;
    let Existing::Mapped(_, segment) = map_existing(flink)? else {
        return Err(QueueError::NonExistingFile);
    };
    let queue = open(segment.ptr, segment.len)?;
    fault::register(segment, flink, type_name);
    Ok(queue)
}

/// Identity of the segment `flink` currently points to.
pub(crate) fn current_identity(flink: &Path) -> Option<QueueIdentity> {
    let _lock = lock_dir(dir_of(flink)).ok()?;
    match map_existing(flink).ok()? {
        Existing::Mapped(_, segment) => segment.identity(),
        Existing::Missing | Existing::Stale(_) => None,
    }
}

fn dir_of(flink: &Path) -> &Path {
    flink.parent().filter(|dir| !dir.as_os_str().is_empty()).unwrap_or_else(|| Path::new("."))
}

// flock locks belong to the open file description, so threads of one process
// exclude each other as well as other processes. Dropping the file, or dying,
// releases the lock.
fn lock_dir(dir: &Path) -> io::Result<File> {
    let dir = File::open(dir)?;
    loop {
        if unsafe { libc::flock(dir.as_raw_fd(), libc::LOCK_EX) } == 0 {
            return Ok(dir);
        }
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            return Err(e);
        }
    }
}

enum Existing {
    Missing,
    Stale(String),
    Mapped(String, Segment),
}

fn map_existing(flink: &Path) -> Result<Existing, ShmemError> {
    let os_id = match std::fs::read_to_string(flink) {
        Ok(os_id) => os_id,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Existing::Missing),
        Err(e) if e.kind() == io::ErrorKind::InvalidData => {
            return Ok(Existing::Stale(String::new()));
        }
        Err(e) => return Err(ShmemError::LinkReadFailed(e)),
    };
    match Segment::open(&os_id) {
        Ok(Some(segment)) => Ok(Existing::Mapped(os_id, segment)),
        Ok(None) => Ok(Existing::Stale(os_id)),
        Err(e) => Err(ShmemError::MapOpenFailed(e.raw_os_error().unwrap_or(0) as u32)),
    }
}

fn create(flink: &Path, size: usize) -> Segment {
    // The crate picks a free object name and creates the flink exclusively; the
    // object is then mapped here so its fd stays reachable.
    let mut shmem = ShmemConf::new()
        .size(size)
        .flink(flink)
        .create()
        .unwrap_or_else(|e| panic!("shmem create failed at {}: {e}", flink.display()));
    shmem.set_owner(false);
    let os_id = shmem.get_os_id().to_owned();
    drop(shmem);
    match Segment::open(&os_id) {
        Ok(Some(segment)) => segment,
        Ok(None) => panic!("new queue segment {os_id} at {} vanished", flink.display()),
        Err(e) => panic!("couldn't map new queue segment {os_id} at {}: {e}", flink.display()),
    }
}

fn remove_stale(flink: &Path, os_id: &str) {
    if let Ok(name) = CString::new(os_id) {
        unsafe { libc::shm_unlink(name.as_ptr()) };
    }
    remove_flink(flink);
    tracing::info!("replacing queue {}: its segment {os_id:?} no longer exists", flink.display());
}

fn remove_flink(flink: &Path) {
    if let Err(e) = std::fs::remove_file(flink) &&
        e.kind() != io::ErrorKind::NotFound
    {
        panic!("couldn't remove queue flink {}: {e}", flink.display());
    }
}

fn retire(
    flink: &Path,
    os_id: &str,
    object: &File,
    previous: Option<QueueIdentity>,
    reason: &QueueError,
    type_hash: u64,
) {
    let mappers = mappers_of(os_id).unwrap_or_else(|e| {
        panic!("couldn't look for processes mapping queue {}: {e}", flink.display())
    });
    let me = std::process::id();
    assert!(
        !mappers.iter().any(|m| m.pid == me),
        "queue {} must be replaced ({reason}), but this process still maps the old segment \
         through another handle",
        flink.display()
    );
    // Registered mappings hold a shared flock on their object (see
    // `fault::register`), which also reveals processes whose maps are unreadable.
    let mapped = !mappers.is_empty() ||
        (unsafe { libc::flock(object.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 &&
            io::Error::last_os_error().raw_os_error() == Some(libc::EWOULDBLOCK));
    assert!(
        !mapped || may_retire_mapped(reason),
        "refusing to replace queue {} ({reason}): still mapped by {}. Stop those processes, or \
         set {RETIRE_MAPPED_QUEUES_ENV}=1 to replace it anyway; processes that opened it through \
         flux then shut down through SIGTERM, others crash with SIGBUS",
        flink.display(),
        describe(&mappers)
    );

    if cfg!(target_os = "linux") {
        object.set_len(0).unwrap_or_else(|e| {
            panic!("couldn't truncate segment {os_id} of queue {}: {e}", flink.display())
        });
    }
    if let Ok(name) = CString::new(os_id) {
        unsafe { libc::shm_unlink(name.as_ptr()) };
    }
    remove_flink(flink);

    let previous = previous.map_or_else(|| "no queue identity".to_owned(), |p| p.to_string());
    if mapped {
        tracing::warn!(
            "replacing queue {} ({reason}) and shutting down {}, which still mapped it; old \
             segment: {previous}; new type hash {type_hash:#018x}",
            flink.display(),
            describe(&mappers)
        );
    } else {
        tracing::warn!(
            "replacing queue {} ({reason}); old segment: {previous}; new type hash {type_hash:#018x}",
            flink.display()
        );
    }
}

struct Mapper {
    pid: u32,
    exe: PathBuf,
}

/// Live processes mapping the shm object `os_id`, as far as `/proc` shows:
/// the maps of other users' and of non-dumpable processes are unreadable.
fn mappers_of(os_id: &str) -> io::Result<Vec<Mapper>> {
    if !cfg!(target_os = "linux") {
        return Ok(Vec::new());
    }
    let shm = std::fs::canonicalize("/dev/shm").unwrap_or_else(|_| PathBuf::from("/dev/shm"));
    let target = shm.join(os_id.trim_start_matches('/'));
    let target = target.as_os_str().as_bytes();
    let mut mappers = Vec::new();
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else {
            continue;
        };
        // Bytes: maps holds paths verbatim, which need not be UTF-8.
        let maps = match std::fs::read(entry.path().join("maps")) {
            Ok(maps) => maps,
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
                ) || e.raw_os_error() == Some(libc::ESRCH) =>
            {
                continue;
            }
            Err(e) => return Err(e),
        };
        let maps_target = |line: &[u8]| {
            line.split(u8::is_ascii_whitespace).filter(|f| !f.is_empty()).nth(5) == Some(target)
        };
        if maps.split(|&b| b == b'\n').any(maps_target) {
            mappers.push(Mapper { pid, exe: exe_of(&entry.path()) });
        }
    }
    Ok(mappers)
}

fn exe_of(proc_dir: &Path) -> PathBuf {
    std::fs::read_link(proc_dir.join("exe"))
        .or_else(|_| std::fs::read_to_string(proc_dir.join("comm")).map(|c| c.trim().into()))
        .unwrap_or_default()
}

fn describe(mappers: &[Mapper]) -> String {
    if mappers.is_empty() {
        return "processes whose /proc/<pid>/maps this user cannot read".to_owned();
    }
    mappers
        .iter()
        .map(|m| format!("pid {} ({})", m.pid, m.exe.display()))
        .collect::<Vec<_>>()
        .join(", ")
}
