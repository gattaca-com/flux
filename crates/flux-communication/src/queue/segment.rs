//! Opening, creating and retiring the shared memory segment behind a queue.
//!
//! Every open of a queue directory's flinks holds an exclusive `flock` on the
//! directory, so validation, retirement and creation never interleave across
//! processes or threads. A segment that fails validation is retired: its
//! object is truncated to zero, then unlinked and replaced. Truncation turns
//! every remaining access to it into SIGBUS: processes that mapped it through
//! this module shut down through SIGTERM (see [`super::fault`]), others, such
//! as older binaries or tools mapping it directly, crash. While other
//! processes map a segment, only a newer layout retires it; see
//! [`LayoutVersion::may_retire_mapped`].
//!
//! Finding other processes' mappings and truncating are Linux only; elsewhere
//! a mismatched segment is unlinked and replaced while its mappers keep it.

use std::{
    ffi::CString,
    fmt,
    fs::File,
    io,
    mem::ManuallyDrop,
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::ffi::OsStrExt,
    },
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use shared_memory::{ShmemConf, ShmemError};
use type_hash::TypeHash;

use super::{LayoutVersion, QueueHeader, QueueIdentity, fault::Mapping};
use crate::error::QueueError;

/// Set to `1` for one start of a process to let it retire any mismatched queue
/// segment, as in a rollback; see
/// [`Queue::create_or_open_shared`](super::Queue::create_or_open_shared).
///
/// Left set, it lets an older binary that restarts retire the newer one's
/// segments again.
pub const RETIRE_MAPPED_QUEUES_ENV: &str = "FLUX_RETIRE_MAPPED_QUEUES";

/// How long an open that shut down other processes waits for them to exit.
const SHUTDOWN_WAIT: Duration = Duration::from_secs(10);

impl LayoutVersion {
    /// Whether this layout may retire a segment of layout `theirs` that other
    /// live processes still map, which shuts them down. Only a newer layout
    /// may: if an equal or older one could, two binaries restarted by a
    /// supervisor would keep retiring each other's segment. A poisoned slot
    /// means its writer died mid-write and attached readers are stuck on it, so
    /// a poisoned segment is retired regardless.
    fn may_retire_mapped(self, theirs: Self, reason: &QueueError) -> bool {
        self > theirs ||
            matches!(reason, QueueError::ElementPoisoned(_)) ||
            std::env::var_os(RETIRE_MAPPED_QUEUES_ENV).is_some_and(|v| v == "1")
    }
}

/// The file naming the shm object behind a queue.
#[derive(Clone, Copy)]
pub(crate) struct Flink<'a>(pub(crate) &'a Path);

impl<'a> Flink<'a> {
    /// Maps the segment of a queue of `T`. An existing segment is accepted when
    /// `open` accepts it and retired otherwise; a missing or retired one is
    /// created and handed to `init` before the directory lock is released.
    pub(crate) fn create_or_open<T: TypeHash, R>(
        self,
        size: usize,
        open: impl FnOnce(*mut u8, usize) -> Result<R, QueueError>,
        init: impl FnOnce(*mut u8) -> R,
    ) -> R {
        let type_name = std::any::type_name::<T>();
        let _ = std::fs::create_dir_all(self.dir());
        let lock = self
            .lock()
            .unwrap_or_else(|e| panic!("couldn't lock queue dir {}: {e}", self.dir().display()));
        // Under the lock: registration happens under it too.
        Mapping::check_collision(self.0, type_name, T::TYPE_HASH);

        let existing =
            self.existing().unwrap_or_else(|e| panic!("couldn't open queue segment {self}: {e}"));
        let mut retired = None;
        match existing {
            Existing::Missing => {}
            Existing::Stale(os_id) => self.remove_stale(&os_id),
            Existing::Mapped(os_id, segment) => match open(segment.ptr, segment.len) {
                Ok(queue) => {
                    // Only once `open` accepted it: a rejected segment keeps the version a
                    // retire compares against.
                    QueueHeader::from_ptr(segment.ptr).raise_type_version(T::VERSION);
                    Mapping::register(segment, self.0, type_name);
                    return queue;
                }
                Err(reason) => {
                    retired = segment.retire(self, &os_id, &reason, LayoutVersion::of::<T>());
                }
            },
        }

        let segment = self.create(size);
        let queue = init(segment.ptr);
        Mapping::register(segment, self.0, type_name);
        drop(lock);
        if let Some(retired) = retired {
            retired.wait_for_exit(self);
        }
        queue
    }

    /// Maps an existing queue segment without ever creating or retiring one.
    pub(crate) fn open<R>(
        self,
        type_name: &'static str,
        open: impl FnOnce(*mut u8, usize) -> Result<R, QueueError>,
    ) -> Result<R, QueueError> {
        if !self.0.exists() {
            return Err(QueueError::NonExistingFile);
        }
        let _lock = self.lock().map_err(ShmemError::LinkOpenFailed)?;
        let Existing::Mapped(_, segment) = self.existing()? else {
            return Err(QueueError::NonExistingFile);
        };
        let queue = open(segment.ptr, segment.len)?;
        Mapping::register(segment, self.0, type_name);
        Ok(queue)
    }

    /// Identity of the segment this flink currently points to.
    pub(crate) fn current_identity(self) -> Option<QueueIdentity> {
        let _lock = self.lock().ok()?;
        match self.existing().ok()? {
            Existing::Mapped(_, segment) => segment.identity(),
            Existing::Missing | Existing::Stale(_) => None,
        }
    }

    fn dir(self) -> &'a Path {
        self.0.parent().filter(|dir| !dir.as_os_str().is_empty()).unwrap_or_else(|| Path::new("."))
    }

    /// Locks the flink's directory. flock locks belong to the open file
    /// description, so threads of one process exclude each other as well as
    /// other processes. Dropping the file, or dying, releases the lock.
    fn lock(self) -> io::Result<File> {
        let dir = File::open(self.dir())?;
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

    fn existing(self) -> Result<Existing, ShmemError> {
        let os_id = match std::fs::read_to_string(self.0) {
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

    fn create(self, size: usize) -> Segment {
        // The crate picks a free object name and creates the flink exclusively; the
        // object is then mapped here so its fd stays reachable.
        let mut shmem = ShmemConf::new()
            .size(size)
            .flink(self.0)
            .create()
            .unwrap_or_else(|e| panic!("shmem create failed at {self}: {e}"));
        shmem.set_owner(false);
        let os_id = shmem.get_os_id().to_owned();
        drop(shmem);
        match Segment::open(&os_id) {
            Ok(Some(segment)) => segment,
            Ok(None) => panic!("new queue segment {os_id} at {self} vanished"),
            Err(e) => panic!("couldn't map new queue segment {os_id} at {self}: {e}"),
        }
    }

    fn remove_stale(self, os_id: &str) {
        if let Ok(name) = CString::new(os_id) {
            unsafe { libc::shm_unlink(name.as_ptr()) };
        }
        self.remove();
        tracing::info!("replacing queue {self}: its segment {os_id:?} no longer exists");
    }

    fn remove(self) {
        if let Err(e) = std::fs::remove_file(self.0) &&
            e.kind() != io::ErrorKind::NotFound
        {
            panic!("couldn't remove queue flink {self}: {e}");
        }
    }
}

impl fmt::Display for Flink<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.display().fmt(f)
    }
}

enum Existing {
    Missing,
    Stale(String),
    Mapped(String, Segment),
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

    /// Retires this segment, which `flink` names as the object `os_id`: its
    /// object is truncated, so every mapping of it faults, then unlinked along
    /// with the flink. `Some` when that shut down other processes still mapping
    /// it.
    fn retire(
        self,
        flink: Flink,
        os_id: &str,
        reason: &QueueError,
        ours: LayoutVersion,
    ) -> Option<Retired> {
        let previous = self.identity();
        let theirs = LayoutVersion::of_mapping(self.ptr, self.len);
        // Unmap before truncating: this process must never touch the old object again.
        let object = self.into_file();
        let mappers = Mappers::of(os_id)
            .unwrap_or_else(|e| panic!("couldn't look for processes mapping queue {flink}: {e}"));
        let me = std::process::id();
        assert!(
            !mappers.0.iter().any(|m| m.pid == me),
            "queue {flink} must be replaced ({reason}), but this process still maps the old \
             segment through another handle"
        );
        // Registered mappings hold a shared flock on their object (see
        // `Mapping::register`), which also reveals processes whose maps are unreadable.
        let mapped = !mappers.0.is_empty() ||
            (unsafe { libc::flock(object.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 &&
                io::Error::last_os_error().raw_os_error() == Some(libc::EWOULDBLOCK));
        let advice = if theirs.header == ours.header {
            format!(
                "; if this build is the newer one, raise its element type version above {}: add \
                 a version to its chain, or raise its #[type_hash(version = N)]",
                theirs.element
            )
        } else {
            String::new()
        };
        assert!(
            !mapped || ours.may_retire_mapped(theirs, reason),
            "refusing to replace queue {flink} ({reason}): it is still mapped by {mappers}, and \
             this process's layout ({ours}) is not newer than the queue's ({theirs}). Stop those \
             processes{advice}; or start this process once with {RETIRE_MAPPED_QUEUES_ENV}=1 to \
             replace it anyway, which shuts them down"
        );

        if cfg!(target_os = "linux") {
            object.set_len(0).unwrap_or_else(|e| {
                panic!("couldn't truncate segment {os_id} of queue {flink}: {e}")
            });
        }
        if let Ok(name) = CString::new(os_id) {
            unsafe { libc::shm_unlink(name.as_ptr()) };
        }
        flink.remove();

        let previous = previous.map_or_else(|| "no queue identity".to_owned(), |p| p.to_string());
        if mapped {
            tracing::warn!(
                "replacing queue {flink} ({reason}) and shutting down {mappers}, which still \
                 mapped it; old segment: {previous}; new layout: {ours}"
            );
        } else {
            tracing::warn!(
                "replacing queue {flink} ({reason}); old segment: {previous}; new layout: {ours}"
            );
        }
        // Only the truncation shuts them down.
        (mapped && cfg!(target_os = "linux")).then_some(Retired(object))
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

/// The object of a retired segment whose retirement shut down other processes.
struct Retired(File);

impl Retired {
    /// Waits for the processes the retirement shut down to exit: until then
    /// they still map the other queues this process is about to open, which it
    /// may not be allowed to replace. Their registered mappings hold a shared
    /// `flock` on the object until they exit. Runs without the directory lock,
    /// which their shutdown may need.
    fn wait_for_exit(self, flink: Flink) {
        let deadline = Instant::now() + SHUTDOWN_WAIT;
        while unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            if Instant::now() > deadline {
                tracing::warn!(
                    "processes that mapped the old segment of queue {flink} are still running \
                     {SHUTDOWN_WAIT:?} after it was replaced"
                );
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// Live processes mapping one shm object, as far as `/proc` shows: the maps of
/// other users' and of non-dumpable processes are unreadable.
struct Mappers(Vec<Mapper>);

struct Mapper {
    pid: u32,
    exe: PathBuf,
}

impl Mappers {
    fn of(os_id: &str) -> io::Result<Self> {
        if !cfg!(target_os = "linux") {
            return Ok(Self(Vec::new()));
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
            let proc_dir = entry.path();
            // Bytes: maps holds paths verbatim, which need not be UTF-8.
            let maps = match std::fs::read(proc_dir.join("maps")) {
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
                let exe = std::fs::read_link(proc_dir.join("exe"))
                    .or_else(|_| {
                        std::fs::read_to_string(proc_dir.join("comm")).map(|c| c.trim().into())
                    })
                    .unwrap_or_default();
                mappers.push(Mapper { pid, exe });
            }
        }
        Ok(Self(mappers))
    }
}

impl fmt::Display for Mappers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return f.write_str("processes whose /proc/<pid>/maps this user cannot read");
        }
        for (i, m) in self.0.iter().enumerate() {
            let sep = if i == 0 { "" } else { ", " };
            write!(f, "{sep}pid {} ({})", m.pid, m.exe.display())?;
        }
        Ok(())
    }
}
