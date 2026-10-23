//! Opening, creating and replacing the shared memory segment behind a queue.
//!
//! Every open of a queue directory's flinks holds an exclusive `flock` on the
//! directory, so validation, replacement and creation never interleave across
//! processes or threads. A segment that fails validation is unlinked and
//! replaced, unless other processes still map it: replacing it would cut them
//! off from the queue, so the open panics and names them instead.
//!
//! Finding the processes that map a segment needs `/proc` and locks on shm
//! objects, which only Linux has; elsewhere a mismatched segment is replaced
//! even while others map it.

use std::{
    ffi::CString,
    fmt,
    fs::File,
    io,
    mem::ManuallyDrop,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::ffi::OsStrExt,
    },
    path::{Path, PathBuf},
};

use shared_memory::{ShmemConf, ShmemError};

use super::{QueueHeader, QueueIdentity};
use crate::error::QueueError;

/// The file naming the shm object behind a queue.
#[derive(Clone, Copy)]
pub(crate) struct Flink<'a>(pub(crate) &'a Path);

impl<'a> Flink<'a> {
    /// Maps the segment of a queue of `T`. An existing segment is accepted when
    /// `open` accepts it and replaced otherwise; a missing or replaced one is
    /// created and handed to `init` before the directory lock is released.
    pub(crate) fn create_or_open<T, R>(
        self,
        size: usize,
        open: impl FnOnce(*mut u8, usize) -> Result<R, QueueError>,
        init: impl FnOnce(*mut u8) -> R,
    ) -> R {
        let _ = std::fs::create_dir_all(self.dir());
        let _lock = self
            .lock()
            .unwrap_or_else(|e| panic!("couldn't lock queue dir {}: {e}", self.dir().display()));
        let existing =
            self.existing().unwrap_or_else(|e| panic!("couldn't open queue segment {self}: {e}"));
        match existing {
            Existing::Missing => {}
            Existing::Stale(os_id) => {
                self.unlink(&os_id);
                tracing::info!("replacing queue {self}: its segment {os_id:?} no longer exists");
            }
            Existing::Mapped(os_id, segment) => match open(segment.ptr, segment.len) {
                Ok(queue) => {
                    segment.keep();
                    return queue;
                }
                Err(reason) => segment.remove::<T>(self, &os_id, &reason),
            },
        }

        let segment = self.create(size);
        let queue = init(segment.ptr);
        segment.keep();
        queue
    }

    /// Maps an existing queue segment without ever creating or replacing one.
    pub(crate) fn open<R>(
        self,
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
        segment.keep();
        Ok(queue)
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

    /// Unlinks the object `os_id` this flink names, then the flink.
    fn unlink(self, os_id: &str) {
        if let Ok(name) = CString::new(os_id) {
            unsafe { libc::shm_unlink(name.as_ptr()) };
        }
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
struct Segment {
    ptr: *mut u8,
    len: usize,
    file: File,
}

impl Segment {
    /// `None` when no usable object has that name: it is gone (every flink is
    /// stale after a reboot), it is empty, or the flink does not hold a valid
    /// name.
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

    /// Unlinks this segment, which `flink` names as the object `os_id`, so a
    /// new one can take its place. Panics, naming them, while other processes
    /// map it: they would keep using the old segment, cut off from the queue.
    fn remove<T>(self, flink: Flink, os_id: &str, reason: &QueueError) {
        let previous =
            self.identity().map_or_else(|| "no queue identity".to_owned(), |p| p.to_string());
        // Unmapped first, so the scan only finds other mappings.
        let object = self.into_file();
        let mappers = Mappers::of(os_id)
            .unwrap_or_else(|e| panic!("couldn't look for processes mapping queue {flink}: {e}"));
        // Kept mappings hold a shared flock on their object (see `Segment::keep`),
        // which also reveals processes whose maps are unreadable. Off Linux shm
        // objects cannot be locked.
        let locked_by_others =
            unsafe { libc::flock(object.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 && {
                let e = io::Error::last_os_error();
                match e.raw_os_error() {
                    Some(libc::EWOULDBLOCK) => true,
                    Some(libc::ENOTSUP) => false,
                    _ => panic!("couldn't check queue {flink} for other mappers: {e}"),
                }
            };
        assert!(
            mappers.0.is_empty() && !locked_by_others,
            "can't replace queue {flink} for {} ({reason}): it is still mapped by {mappers}. \
             Stop them first. The queue's segment has {previous}",
            std::any::type_name::<T>()
        );
        flink.unlink(os_id);
        tracing::warn!("replacing queue {flink} ({reason}); old segment: {previous}");
    }

    /// Unmaps the object, keeping its fd.
    fn into_file(self) -> File {
        let this = ManuallyDrop::new(self);
        unsafe {
            libc::munmap(this.ptr.cast(), this.len);
            std::ptr::read(&raw const this.file)
        }
    }

    /// Keeps the mapping and its fd for the life of the process. The shared
    /// flock on the fd shows an opener about to replace the segment that this
    /// process maps it, even when it cannot read our `/proc/<pid>/maps`.
    fn keep(self) {
        let this = ManuallyDrop::new(self);
        unsafe { libc::flock(this.file.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) };
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.ptr.cast(), self.len) };
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
        let me = std::process::id();
        for (i, m) in self.0.iter().enumerate() {
            let sep = if i == 0 { "" } else { ", " };
            let this = if m.pid == me { ", this process" } else { "" };
            write!(f, "{sep}pid {} ({}{this})", m.pid, m.exe.display())?;
        }
        Ok(())
    }
}
