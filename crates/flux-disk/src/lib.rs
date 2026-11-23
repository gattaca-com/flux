//! Nonblocking disk I/O for synchronous hot loops.
//!
//! [`DiskIo`] is the disk counterpart of `flux-network`'s poll-driven TCP
//! types: every call submits work and returns immediately, and completions
//! are delivered as borrowed [`DiskEvent`]s from [`DiskIo::poll_with`], which
//! never blocks. Readiness polling (mio/epoll) cannot express regular-file
//! I/O — files are always "ready" and reads still block — so this is built on
//! `io_uring` completions instead: operations are pushed onto the kernel's
//! submission ring and reaped from its completion ring without waiting.
//!
//! Operations on one file run concurrently, except [`DiskIo::sync_all`],
//! [`DiskIo::sync_data`], [`DiskIo::truncate`], [`DiskIo::rename`],
//! [`DiskIo::rename_path`], [`DiskIo::link`], [`DiskIo::unlink`],
//! [`DiskIo::remove_dir`], [`DiskIo::create_dir`] and [`DiskIo::close`], which
//! act as barriers: they start only after every earlier operation on that file
//! has completed, so a sync covers all earlier writes and structural changes
//! cannot race them. Truncates, renames, links, unlinks, directory removals and
//! directory creations additionally prevent later operations from starting
//! until they complete. Operations submitted before the file
//! finished opening are queued and dispatched once the descriptor is
//! available.

mod backend;
mod emulation;
mod uring;

use std::{
    collections::VecDeque,
    ffi::CString,
    io,
    mem::MaybeUninit,
    os::{
        fd::{AsRawFd, RawFd},
        unix::ffi::OsStrExt,
    },
    path::{Path, PathBuf},
};

use flux_timing::{Duration, Instant};
use tracing::warn;

use crate::backend::{Backend, Op};

const INITIAL_FILE_CAPACITY: usize = 8;
const INITIAL_IN_FLIGHT_CAPACITY: usize = 64;
const MAX_POOLED_BUFFERS: usize = 32;
const PENDING_WARNING_INTERVAL_SECS: u64 = 10;
/// Largest byte count handed to the kernel in one submission. Longer reads
/// and writes are transparently continued from where the previous chunk
/// ended.
const MAX_OP_BYTES: usize = 1 << 30;
/// Minimum and fallback buffer capacity for [`DiskIo::read_to_end`].
const READ_TO_END_CHUNK: usize = 64 * 1024;

/// Identifies one open (or opening) file. Tokens are never reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FileToken(usize);

/// Identifies an accepted sync, truncate or rename operation.
///
/// Assigned by `DiskIo` and echoed by completion and failure events. IDs are
/// unique within one `DiskIo` instance and never reused. An ID identifies
/// queued work; it does not imply that the operation has completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OperationId(u64);

impl OperationId {
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// How [`DiskIo::open`] opens a file.
///
/// Mirrors [`std::fs::OpenOptions`] (including the unix `mode` extension) in
/// names, defaults, and validation, except that the builders take and return
/// `self` by value and there is no `append` flag: `O_APPEND` would make the
/// kernel ignore the explicit offset every write here carries. Append to an
/// existing file by positioning [`DiskIo::set_write_cursor`] instead.
#[allow(clippy::struct_excessive_bools)] // same knobs as std::fs::OpenOptions
#[derive(Clone, Copy, Debug)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    create: bool,
    create_new: bool,
    truncate: bool,
    tmpfile: bool,
    mode: u32,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
    /// Blank options: at least one of [`read`](Self::read) or
    /// [`write`](Self::write) must be set before opening.
    pub fn new() -> Self {
        Self {
            read: false,
            write: false,
            create: false,
            create_new: false,
            truncate: false,
            tmpfile: false,
            mode: 0o666,
        }
    }

    /// Open for reading.
    pub fn read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }

    /// Open for writing.
    pub fn write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    /// Create the file when it does not exist. Requires `write`.
    pub fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    /// Create the file, failing when it already exists. Requires `write`;
    /// `create` and `truncate` are ignored. No file is affected when the
    /// open fails.
    pub fn create_new(mut self, create_new: bool) -> Self {
        self.create_new = create_new;
        self
    }

    /// Discard existing contents on open. Requires `write`.
    pub fn truncate(mut self, truncate: bool) -> Self {
        self.truncate = truncate;
        self
    }

    /// Open an unnamed file in the directory at the path given to
    /// [`DiskIo::open`]; [`DiskIo::link`] names it later. Requires `write`
    /// and none of `create`, `create_new` or `truncate`. Needs a filesystem
    /// with `O_TMPFILE`.
    pub fn tmpfile(mut self, tmpfile: bool) -> Self {
        self.tmpfile = tmpfile;
        self
    }

    /// Permission bits for newly created files, subject to the umask.
    pub fn mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }

    /// Same flag mapping and validation as `std::fs::OpenOptions` on unix.
    fn flags(self) -> io::Result<i32> {
        let access = match (self.read, self.write) {
            (true, false) => libc::O_RDONLY,
            (false, true) => libc::O_WRONLY,
            (true, true) => libc::O_RDWR,
            (false, false) => return Err(io::Error::from_raw_os_error(libc::EINVAL)),
        };
        if !self.write && (self.create || self.create_new || self.truncate) {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        if self.tmpfile && (!self.write || self.create || self.create_new || self.truncate) {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        let tmpfile = if self.tmpfile { libc::O_TMPFILE } else { 0 };
        let creation = match (self.create, self.truncate, self.create_new) {
            (false, false, false) => 0,
            (true, false, false) => libc::O_CREAT,
            (false, true, false) => libc::O_TRUNC,
            (true, true, false) => libc::O_CREAT | libc::O_TRUNC,
            (_, _, true) => libc::O_CREAT | libc::O_EXCL,
        };
        Ok(access | creation | tmpfile | libc::O_CLOEXEC)
    }
}

/// Configuration for one [`DiskIo`] instance.
#[derive(Clone, Copy, Debug)]
pub struct DiskConfig {
    /// `io_uring` submission queue depth. Also bounds how many operations
    /// reach the kernel per [`DiskIo::poll_with`] call.
    pub ring_entries: u32,
    /// Most operations concurrently owned by the kernel. Clamped to the
    /// ring's completion queue capacity so completions can never be dropped.
    pub max_in_flight: usize,
    /// Emit rate-limited warnings above this many queued operations. The
    /// queue is allowed to continue growing.
    pub pending_warn_ops: Option<usize>,
}

impl Default for DiskConfig {
    fn default() -> Self {
        Self { ring_entries: 256, max_in_flight: 256, pending_warn_ops: Some(4096) }
    }
}

/// Which operation a [`DiskEvent::Failed`] refers to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailedOp {
    Open,
    FileLen,
    Read { offset: u64, len: usize },
    Write { offset: u64, len: usize },
    Sync,
    Truncate { len: u64 },
    Rename,
    Link,
    Unlink,
    CreateDir,
    Close,
}

/// Event emitted by [`DiskIo::poll_with`].
pub enum DiskEvent<'a> {
    /// The file is open; queued operations are now being dispatched.
    Opened { file: FileToken },
    /// A read finished. `eof` is true when end-of-file was hit before
    /// `payload` reached the requested length; a read ending exactly at the
    /// file end reports `eof: false`.
    Read { file: FileToken, offset: u64, payload: &'a [u8], eof: bool },
    /// A write was fully applied at `offset`.
    Written { file: FileToken, offset: u64, len: usize },
    /// [`DiskIo::file_len`] finished.
    FileLen { file: FileToken, len: u64 },
    /// A [`DiskIo::read_at_into`] finished: `len` bytes landed in the
    /// caller's memory. `eof` is as for [`DiskEvent::Read`].
    ReadInto { file: FileToken, offset: u64, len: usize, eof: bool },
    /// A read queued with a caller's buffer finished, or failed with `error`.
    /// `buf` holds the bytes read and comes back either way; `eof` is as for
    /// [`DiskEvent::Read`].
    ReadOwned { file: FileToken, offset: u64, buf: Vec<u8>, eof: bool, error: Option<io::Error> },
    /// A write queued with a caller's buffer was fully applied at `offset`, or
    /// failed with `error`. `buf` comes back either way.
    WrittenOwned { file: FileToken, offset: u64, buf: Vec<u8>, error: Option<io::Error> },
    /// A [`DiskIo::sync_all`] or [`DiskIo::sync_data`] barrier finished;
    /// every
    /// write submitted before it is durable.
    Synced { file: FileToken, operation_id: OperationId },
    /// The file was truncated to `len` bytes.
    Truncated { file: FileToken, len: u64, operation_id: OperationId },
    /// The open file, or the path given to [`DiskIo::rename_path`], was
    /// atomically renamed.
    Renamed { file: FileToken, operation_id: OperationId },
    /// The open file now has the name given to [`DiskIo::link`].
    Linked { file: FileToken, operation_id: OperationId },
    /// The name given to [`DiskIo::unlink`] or [`DiskIo::remove_dir`] is gone.
    Unlinked { file: FileToken, operation_id: OperationId },
    /// The directory given to [`DiskIo::create_dir`] exists.
    DirectoryCreated { file: FileToken, operation_id: OperationId },
    /// The file was closed and its token retired.
    Closed { file: FileToken },
    /// An operation failed. `Failed { op: FailedOp::Open, .. }` retires the
    /// token and fails every queued operation on it; other failures leave the
    /// file usable. Sync, truncate and rename failures include the ID returned
    /// when the operation was queued; other operations have no ID.
    Failed { file: FileToken, op: FailedOp, operation_id: Option<OperationId>, error: io::Error },
}

enum PendingOp {
    Open {
        path: CString,
        flags: i32,
        mode: libc::mode_t,
    },
    /// `buf` is the caller's for an owned read; a pooled read takes one at
    /// dispatch.
    Read {
        offset: u64,
        len: usize,
        to_end: bool,
        buf: Option<Vec<u8>>,
    },
    ReadToEnd {
        offset: u64,
        buf: Option<Vec<u8>>,
    },
    ReadInto {
        target: CallerMemory,
        offset: u64,
        len: usize,
    },
    WriteFrom {
        source: CallerMemory,
        offset: u64,
        len: usize,
    },
    FileLen,
    Write {
        buf: Vec<u8>,
        offset: u64,
        owned: bool,
    },
    Sync {
        data_only: bool,
        operation_id: OperationId,
    },
    Truncate {
        len: u64,
        operation_id: OperationId,
    },
    /// `source: None` renames the open file, which then takes
    /// `destination_path`; otherwise `destination_path` is `None`.
    Rename {
        source: Option<CString>,
        destination: CString,
        destination_path: Option<PathBuf>,
        operation_id: OperationId,
    },
    Link {
        destination: CString,
        destination_path: PathBuf,
        operation_id: OperationId,
    },
    Unlink {
        path: CString,
        directory: bool,
        operation_id: OperationId,
    },
    CreateDir {
        path: CString,
        operation_id: OperationId,
    },
    Close,
}

enum InFlightOp {
    Open {
        path: CString,
        flags: i32,
        mode: libc::mode_t,
    },
    Read {
        buf: Vec<u8>,
        offset: u64,
        wanted: usize,
        have: usize,
        to_end: bool,
        owned: bool,
    },
    Statx {
        offset: u64,
        statx: Box<MaybeUninit<libc::statx>>,
        buf: Option<Vec<u8>>,
    },
    ReadInto {
        target: CallerMemory,
        offset: u64,
        wanted: usize,
        have: usize,
    },
    WriteFrom {
        source: CallerMemory,
        offset: u64,
        len: usize,
        written: usize,
    },
    FileLen {
        statx: Box<MaybeUninit<libc::statx>>,
    },
    Write {
        buf: Vec<u8>,
        offset: u64,
        written: usize,
        owned: bool,
    },
    Sync {
        data_only: bool,
        operation_id: OperationId,
    },
    Truncate {
        len: u64,
        operation_id: OperationId,
    },
    /// `destination_path` becomes the file's path; `None` for a path rename,
    /// which leaves the ordering file's path alone.
    Rename {
        source: CString,
        destination: CString,
        destination_path: Option<PathBuf>,
        operation_id: OperationId,
    },
    Link {
        source: CString,
        destination: CString,
        destination_path: PathBuf,
        operation_id: OperationId,
    },
    Unlink {
        path: CString,
        directory: bool,
        operation_id: OperationId,
    },
    CreateDir {
        path: CString,
        operation_id: OperationId,
    },
    Close,
}

/// Caller memory a [`DiskIo::read_at_into`] fills or a
/// [`DiskIo::write_at_from`] drains.
struct CallerMemory(*mut u8);

// SAFETY: the caller of `read_at_into` or `write_at_from` keeps the memory
// valid and untouched until its completion, whichever thread drives the
// `DiskIo`.
unsafe impl Send for CallerMemory {}

struct InFlight {
    file: FileToken,
    op: InFlightOp,
}

struct File {
    token: FileToken,
    path: PathBuf,
    fd: Option<i32>,
    closing: bool,
    write_cursor: u64,
    in_flight: usize,
    // Set by `submit_op` while a truncate or rename is in flight.
    // `dispatch_pending` pauses this file's queue until `finish_op` clears
    // it on completion, whether the operation succeeded or failed.
    exclusive_in_flight: bool,
    queue: VecDeque<PendingOp>,
}

/// A collection of files driven by one nonblocking `io_uring`.
///
/// Submitting calls never block: operations wait in a per-file queue, move to
/// the kernel as ring capacity allows, and complete through
/// [`DiskIo::poll_with`]. Dropping the instance waits for operations already
/// handed to the kernel (bounded by device latency), abandons queued ones,
/// and closes remaining descriptors; poll until [`DiskIo::is_idle`] first
/// when every completion matters.
pub struct DiskIo {
    backend: Backend,
    max_in_flight: usize,
    pending_warn_ops: Option<usize>,
    files: Vec<File>,
    slab: Vec<Option<InFlight>>,
    free_slots: Vec<usize>,
    in_flight_count: usize,
    pending_count: usize,
    /// In-flight continuations that found the submission queue full; retried
    /// first on the next poll.
    stalled: Vec<u64>,
    completions: Vec<(u64, i32)>,
    buffer_pool: Vec<Vec<u8>>,
    next_token: usize,
    next_operation_id: u64,
    need_submit: bool,
    last_pending_warning: Option<Instant>,
}

/// Pins the kernel workers that run the calling thread's blocking operations
/// to `cores`, for every ring the thread drives.
pub fn pin_io_workers(cores: &[usize]) -> io::Result<()> {
    if cores.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "no cores to pin to"));
    }
    Backend::pin_io_workers(cores)
}

impl Default for DiskIo {
    fn default() -> Self {
        Self::new(DiskConfig::default()).expect("couldn't set up an io_uring for disk io")
    }
}

impl DiskIo {
    pub fn new(config: DiskConfig) -> io::Result<Self> {
        Self::with_backend(config, Backend::new)
    }

    #[cfg(test)]
    fn emulated(config: DiskConfig) -> io::Result<Self> {
        Self::with_backend(config, Backend::emulated)
    }

    fn with_backend(
        config: DiskConfig,
        backend: impl FnOnce(u32, usize) -> io::Result<(Backend, usize)>,
    ) -> io::Result<Self> {
        assert!(config.ring_entries > 0, "ring_entries must be nonzero");
        assert!(config.max_in_flight > 0, "max_in_flight must be nonzero");
        let (backend, max_in_flight) = backend(config.ring_entries, config.max_in_flight)?;
        Ok(Self {
            backend,
            max_in_flight,
            pending_warn_ops: config.pending_warn_ops,
            files: Vec::with_capacity(INITIAL_FILE_CAPACITY),
            slab: Vec::with_capacity(INITIAL_IN_FLIGHT_CAPACITY),
            free_slots: Vec::with_capacity(INITIAL_IN_FLIGHT_CAPACITY),
            in_flight_count: 0,
            pending_count: 0,
            stalled: Vec::new(),
            completions: Vec::with_capacity(INITIAL_IN_FLIGHT_CAPACITY),
            buffer_pool: Vec::new(),
            next_token: 0,
            next_operation_id: 0,
            need_submit: false,
            last_pending_warning: None,
        })
    }

    /// Starts opening a file. The token is usable immediately: operations
    /// queue behind the open and dispatch once [`DiskEvent::Opened`] fires.
    /// If the open fails, every queued operation fails with `EBADF`.
    pub fn open<P: AsRef<Path>>(&mut self, path: P, options: OpenOptions) -> io::Result<FileToken> {
        self.open_with_flags(path.as_ref(), options.flags()?, options.mode as libc::mode_t)
    }

    /// Starts opening a directory for use with [`Self::sync_all`].
    ///
    /// A directory sync makes a completed rename durable. When a rename moves
    /// a file between directories, both directories must be synced. Directory
    /// operations are not ordered with operations on other file tokens, so
    /// queue the sync after receiving [`DiskEvent::Renamed`].
    pub fn open_directory<P: AsRef<Path>>(&mut self, path: P) -> io::Result<FileToken> {
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC;
        self.open_with_flags(path.as_ref(), flags, 0)
    }

    fn open_with_flags(
        &mut self,
        path: &Path,
        flags: i32,
        mode: libc::mode_t,
    ) -> io::Result<FileToken> {
        let cpath = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a nul byte"))?;
        let token = FileToken(self.next_token);
        self.next_token = self.next_token.checked_add(1).expect("file token space exhausted");
        let mut queue = VecDeque::new();
        queue.push_back(PendingOp::Open { path: cpath, flags, mode });
        self.files.push(File {
            token,
            path: path.to_path_buf(),
            fd: None,
            closing: false,
            write_cursor: 0,
            in_flight: 0,
            exclusive_in_flight: false,
            queue,
        });
        self.note_enqueued();
        Ok(token)
    }

    /// Queues a read of `len` bytes at `offset`, positional like
    /// [`std::os::unix::fs::FileExt::read_at`]: the append cursor is not
    /// consulted or moved. Returns `false` for empty reads and unknown or
    /// closing tokens.
    pub fn read_at(&mut self, file: FileToken, offset: u64, len: usize) -> bool {
        if len == 0 {
            return false;
        }
        let Some(state) = self.usable_file_mut(file) else { return false };
        state.queue.push_back(PendingOp::Read { offset, len, to_end: false, buf: None });
        self.note_enqueued();
        true
    }

    /// Queues a read of everything from `offset` to end-of-file, like
    /// [`std::io::Read::read_to_end`] from a fixed position: the buffer
    /// grows until a zero-byte completion marks the end, then the whole
    /// contents arrive as one [`DiskEvent::Read`] with `eof: true`. A file
    /// that grows concurrently is read until end-of-file is observed.
    /// Returns `false` for unknown or closing tokens.
    pub fn read_to_end(&mut self, file: FileToken, offset: u64) -> bool {
        let Some(state) = self.usable_file_mut(file) else { return false };
        state.queue.push_back(PendingOp::ReadToEnd { offset, buf: None });
        self.note_enqueued();
        true
    }

    /// Like [`Self::read_at`], into `buf` after clearing it: the read never
    /// touches the buffer pool and `buf` comes back in
    /// [`DiskEvent::ReadOwned`]. Hands `buf` back for empty reads and unknown
    /// or closing tokens.
    pub fn read_at_owned(
        &mut self,
        file: FileToken,
        offset: u64,
        len: usize,
        buf: Vec<u8>,
    ) -> Result<(), Vec<u8>> {
        if len == 0 {
            return Err(buf);
        }
        let Some(state) = self.usable_file_mut(file) else { return Err(buf) };
        state.queue.push_back(PendingOp::Read { offset, len, to_end: false, buf: Some(buf) });
        self.note_enqueued();
        Ok(())
    }

    /// Like [`Self::read_to_end`], into `buf` after clearing it; `buf` comes
    /// back in [`DiskEvent::ReadOwned`]. Hands `buf` back for unknown or
    /// closing tokens.
    pub fn read_to_end_owned(
        &mut self,
        file: FileToken,
        offset: u64,
        buf: Vec<u8>,
    ) -> Result<(), Vec<u8>> {
        let Some(state) = self.usable_file_mut(file) else { return Err(buf) };
        state.queue.push_back(PendingOp::ReadToEnd { offset, buf: Some(buf) });
        self.note_enqueued();
        Ok(())
    }

    pub fn file_len(&mut self, file: FileToken) -> bool {
        let Some(state) = self.usable_file_mut(file) else { return false };
        state.queue.push_back(PendingOp::FileLen);
        self.note_enqueued();
        true
    }

    /// # Safety
    ///
    /// `target..target + len` must stay valid for writes, and be neither read
    /// nor written by anything else, until the completion is delivered or
    /// this `DiskIo` is dropped.
    pub unsafe fn read_at_into(
        &mut self,
        file: FileToken,
        offset: u64,
        target: *mut u8,
        len: usize,
    ) -> bool {
        if len == 0 {
            return false;
        }
        let Some(state) = self.usable_file_mut(file) else { return false };
        state.queue.push_back(PendingOp::ReadInto { target: CallerMemory(target), offset, len });
        self.note_enqueued();
        true
    }

    /// # Safety
    ///
    /// `source..source + len` must stay valid for reads and unchanged until
    /// the completion is delivered or this `DiskIo` is dropped.
    pub unsafe fn write_at_from(
        &mut self,
        file: FileToken,
        offset: u64,
        source: *const u8,
        len: usize,
    ) -> bool {
        if len == 0 {
            return false;
        }
        let Some(state) = self.usable_file_mut(file) else { return false };
        let source = CallerMemory(source.cast_mut());
        state.queue.push_back(PendingOp::WriteFrom { source, offset, len });
        self.note_enqueued();
        true
    }

    /// Serializes one payload and queues it for writing at the file's append
    /// cursor, which starts at zero and advances by the payload length.
    /// Returns `false` for empty payloads and unknown or closing tokens.
    pub fn write_with<F>(&mut self, file: FileToken, serialise: F) -> bool
    where
        F: FnOnce(&mut Vec<u8>),
    {
        if self.usable_file_mut(file).is_none() {
            return false;
        }
        let mut buf = self.take_buffer();
        serialise(&mut buf);
        if buf.is_empty() {
            self.recycle(buf);
            return false;
        }
        let len = buf.len() as u64;
        let state = self.usable_file_mut(file).expect("file was just found");
        let offset = state.write_cursor;
        state.write_cursor += len;
        state.queue.push_back(PendingOp::Write { buf, offset, owned: false });
        self.note_enqueued();
        true
    }

    /// Serializes one payload and queues it for writing at an explicit
    /// offset, leaving the append cursor untouched. Writes to overlapping
    /// ranges complete in unspecified order; separate them with a sync
    /// barrier when ordering matters. Returns `false` for empty payloads and
    /// unknown or closing tokens.
    pub fn write_at_with<F>(&mut self, file: FileToken, offset: u64, serialise: F) -> bool
    where
        F: FnOnce(&mut Vec<u8>),
    {
        if self.usable_file_mut(file).is_none() {
            return false;
        }
        let mut buf = self.take_buffer();
        serialise(&mut buf);
        if buf.is_empty() {
            self.recycle(buf);
            return false;
        }
        let state = self.usable_file_mut(file).expect("file was just found");
        state.queue.push_back(PendingOp::Write { buf, offset, owned: false });
        self.note_enqueued();
        true
    }

    /// Like [`Self::write_with`], writing the caller's `buf` at the append
    /// cursor; `buf` never enters the buffer pool and comes back in
    /// [`DiskEvent::WrittenOwned`]. Hands `buf` back when it is empty or the
    /// token is unknown or closing.
    pub fn write_owned(&mut self, file: FileToken, buf: Vec<u8>) -> Result<(), Vec<u8>> {
        if buf.is_empty() {
            return Err(buf);
        }
        let Some(state) = self.usable_file_mut(file) else { return Err(buf) };
        let offset = state.write_cursor;
        state.write_cursor += buf.len() as u64;
        state.queue.push_back(PendingOp::Write { buf, offset, owned: true });
        self.note_enqueued();
        Ok(())
    }

    /// Like [`Self::write_at_with`], writing the caller's `buf` at `offset`;
    /// `buf` comes back in [`DiskEvent::WrittenOwned`]. Hands `buf` back when
    /// it is empty or the token is unknown or closing.
    pub fn write_at_owned(
        &mut self,
        file: FileToken,
        offset: u64,
        buf: Vec<u8>,
    ) -> Result<(), Vec<u8>> {
        if buf.is_empty() {
            return Err(buf);
        }
        let Some(state) = self.usable_file_mut(file) else { return Err(buf) };
        state.queue.push_back(PendingOp::Write { buf, offset, owned: true });
        self.note_enqueued();
        Ok(())
    }

    /// Moves the append cursor used by [`DiskIo::write_with`], e.g. to the
    /// current length of an existing file. Returns `false` for unknown or
    /// closing tokens.
    pub fn set_write_cursor(&mut self, file: FileToken, offset: u64) -> bool {
        let Some(state) = self.usable_file_mut(file) else { return false };
        state.write_cursor = offset;
        true
    }

    /// Queues an `ftruncate` barrier after all earlier operations on `file`.
    /// Later operations do not start until the truncate completes. The append
    /// cursor is unchanged; use [`Self::set_write_cursor`] when repairing an
    /// append-only file. Requires Linux 6.9 or newer.
    ///
    /// Returns an ID echoed by its completion or failure event, or `None` for
    /// unknown or closing tokens.
    pub fn truncate(&mut self, file: FileToken, len: u64) -> Option<OperationId> {
        self.enqueue_operation(file, |operation_id| PendingOp::Truncate { len, operation_id })
    }

    /// Queues an atomic rename of the open file after all earlier operations
    /// on it. Later operations do not start until the rename completes.
    ///
    /// A successful rename updates the path associated with `file`, but is not
    /// durable until the affected parent directories have been synced.
    ///
    /// Returns an ID echoed by its completion or failure event, or `None` for
    /// unknown or closing tokens. A destination containing a nul byte returns
    /// an error without queueing an operation.
    pub fn rename<P: AsRef<Path>>(
        &mut self,
        file: FileToken,
        destination: P,
    ) -> io::Result<Option<OperationId>> {
        let destination_path = destination.as_ref();
        let destination = c_path(destination_path)?;
        Ok(self.enqueue_operation(file, |operation_id| PendingOp::Rename {
            source: None,
            destination,
            destination_path: Some(destination_path.to_path_buf()),
            operation_id,
        }))
    }

    pub fn rename_path<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        file: FileToken,
        from: P,
        to: Q,
    ) -> io::Result<Option<OperationId>> {
        let source = c_path(from.as_ref())?;
        let destination = c_path(to.as_ref())?;
        Ok(self.enqueue_operation(file, |operation_id| PendingOp::Rename {
            source: Some(source),
            destination,
            destination_path: None,
            operation_id,
        }))
    }

    /// Queues giving the open file the name `path`, for a file opened with
    /// [`OpenOptions::tmpfile`], after all earlier operations on it. Fails
    /// with `EEXIST` when the name is taken. A successful link makes `path`
    /// the file's path, so a later [`DiskIo::rename`] moves it. Needs Linux
    /// 5.15.
    ///
    /// Returns an ID echoed by its completion or failure event, or `None` for
    /// unknown or closing tokens. A path containing a nul byte returns an
    /// error without queueing an operation.
    pub fn link<P: AsRef<Path>>(
        &mut self,
        file: FileToken,
        path: P,
    ) -> io::Result<Option<OperationId>> {
        let destination_path = path.as_ref();
        let destination = CString::new(destination_path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a nul byte"))?;
        Ok(self.enqueue_operation(file, |operation_id| PendingOp::Link {
            destination,
            destination_path: destination_path.to_path_buf(),
            operation_id,
        }))
    }

    /// Queues removal of the name `path`, after all earlier operations on
    /// `file` and before later ones. The file itself is untouched. Needs
    /// Linux 5.11.
    ///
    /// Returns an ID echoed by its completion or failure event, or `None` for
    /// unknown or closing tokens. A path containing a nul byte returns an
    /// error without queueing an operation.
    pub fn unlink<P: AsRef<Path>>(
        &mut self,
        file: FileToken,
        path: P,
    ) -> io::Result<Option<OperationId>> {
        let path = c_path(path.as_ref())?;
        Ok(self.enqueue_operation(file, |operation_id| PendingOp::Unlink {
            path,
            directory: false,
            operation_id,
        }))
    }

    pub fn remove_dir<P: AsRef<Path>>(
        &mut self,
        file: FileToken,
        path: P,
    ) -> io::Result<Option<OperationId>> {
        let path = c_path(path.as_ref())?;
        Ok(self.enqueue_operation(file, |operation_id| PendingOp::Unlink {
            path,
            directory: true,
            operation_id,
        }))
    }

    /// Queues creating the directory `path`, like [`std::fs::create_dir`],
    /// after all earlier operations on `file` and before later ones; `file`
    /// only orders it, so a directory opened with [`Self::open_directory`]
    /// serves. Fails with `EEXIST` when the name is taken. Parents are not
    /// created: queue one call per missing level, outermost first. Needs
    /// Linux 5.15.
    ///
    /// Returns an ID echoed by its completion or failure event, or `None` for
    /// unknown or closing tokens. A path containing a nul byte returns an
    /// error without queueing an operation.
    pub fn create_dir<P: AsRef<Path>>(
        &mut self,
        file: FileToken,
        path: P,
    ) -> io::Result<Option<OperationId>> {
        let path = CString::new(path.as_ref().as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a nul byte"))?;
        Ok(self.enqueue_operation(file, |operation_id| PendingOp::CreateDir { path, operation_id }))
    }

    /// Queues a full-integrity `fsync` barrier, the durability of
    /// [`std::fs::File::sync_all`]: it starts only after every earlier
    /// operation on this file completed, and later operations start only
    /// after the barrier was handed to the kernel.
    ///
    /// Returns an ID echoed by its completion or failure event, or `None` for
    /// unknown or closing tokens.
    pub fn sync_all(&mut self, file: FileToken) -> Option<OperationId> {
        self.enqueue_operation(file, |operation_id| PendingOp::Sync {
            data_only: false,
            operation_id,
        })
    }

    /// Queues an `fdatasync` barrier, the durability of
    /// [`std::fs::File::sync_data`]; like [`DiskIo::sync_all`] without
    /// flushing file metadata timestamps.
    ///
    /// Returns an ID echoed by its completion or failure event, or `None` for
    /// unknown or closing tokens.
    pub fn sync_data(&mut self, file: FileToken) -> Option<OperationId> {
        self.enqueue_operation(file, |operation_id| PendingOp::Sync {
            data_only: true,
            operation_id,
        })
    }

    /// Queues a close barrier after every earlier operation on this file and
    /// rejects further submissions. [`DiskEvent::Closed`] retires the token.
    /// Returns `false` for unknown or already closing tokens.
    pub fn close(&mut self, file: FileToken) -> bool {
        let Some(state) = self.usable_file_mut(file) else { return false };
        state.closing = true;
        state.queue.push_back(PendingOp::Close);
        self.note_enqueued();
        true
    }

    pub fn submit(&mut self) {
        self.dispatch_pending();
        self.flush_submissions();
    }

    /// Whether every submitted operation has completed and been reported.
    pub fn is_idle(&self) -> bool {
        self.pending_count == 0 && self.in_flight_count == 0
    }

    pub fn limit_io_workers(&mut self, bounded: u32, unbounded: u32) -> io::Result<[u32; 2]> {
        let mut limits = [bounded, unbounded];
        self.backend.limit_io_workers(&mut limits)?;
        Ok(limits)
    }

    /// Reaps finished operations, dispatches queued ones, and submits to the
    /// kernel — all without blocking. Completions that the kernel satisfied
    /// inline (e.g. page-cache hits) are reported within the same call.
    pub fn poll_with<F>(&mut self, mut handler: F)
    where
        F: for<'a> FnMut(DiskEvent<'a>),
    {
        self.drain_completions(&mut handler);
        self.dispatch_pending();
        self.flush_submissions();
        self.drain_completions(&mut handler);
    }

    fn enqueue_operation(
        &mut self,
        file: FileToken,
        make_op: impl FnOnce(OperationId) -> PendingOp,
    ) -> Option<OperationId> {
        let operation_id = OperationId(self.next_operation_id);
        let state = self.usable_file_mut(file)?;
        let next_id = operation_id.0.checked_add(1).expect("disk operation id space exhausted");
        state.queue.push_back(make_op(operation_id));
        self.next_operation_id = next_id;
        self.note_enqueued();
        Some(operation_id)
    }

    fn usable_file_mut(&mut self, token: FileToken) -> Option<&mut File> {
        self.files.iter_mut().find(|file| file.token == token && !file.closing)
    }

    fn note_enqueued(&mut self) {
        self.pending_count += 1;
        let Some(threshold) = self.pending_warn_ops else { return };
        if self.pending_count <= threshold {
            self.last_pending_warning = None;
            return;
        }
        if self
            .last_pending_warning
            .is_some_and(|last| last.elapsed() < Duration::from_secs(PENDING_WARNING_INTERVAL_SECS))
        {
            return;
        }
        warn!(pending_ops = self.pending_count, "disk io pending queue growing");
        self.last_pending_warning = Some(Instant::now());
    }

    fn take_buffer(&mut self) -> Vec<u8> {
        let mut buf = self.buffer_pool.pop().unwrap_or_default();
        buf.clear();
        buf
    }

    fn take_buffer_with_capacity(&mut self, len: usize) -> Vec<u8> {
        let mut buf = self.take_buffer();
        buf.reserve(len);
        buf
    }

    fn recycle(&mut self, buf: Vec<u8>) {
        if self.buffer_pool.len() < MAX_POOLED_BUFFERS {
            self.buffer_pool.push(buf);
        }
    }

    fn alloc_slot(&mut self, entry: InFlight) -> usize {
        if let Some(slot) = self.free_slots.pop() {
            self.slab[slot] = Some(entry);
            slot
        } else {
            self.slab.push(Some(entry));
            self.slab.len() - 1
        }
    }

    fn dispatch_pending(&mut self) {
        if !self.stalled.is_empty() {
            let stalled = std::mem::take(&mut self.stalled);
            for user_data in stalled {
                self.push_slot(user_data);
            }
        }
        let mut file_index = 0;
        while file_index < self.files.len() {
            loop {
                if self.in_flight_count >= self.max_in_flight || self.backend.submission_full() {
                    return;
                }
                let file = &self.files[file_index];
                if file.exclusive_in_flight {
                    break;
                }
                let Some(head) = file.queue.front() else { break };
                let ready = match head {
                    PendingOp::Open { .. } => true,
                    PendingOp::Read { .. } |
                    PendingOp::ReadToEnd { .. } |
                    PendingOp::ReadInto { .. } |
                    PendingOp::WriteFrom { .. } |
                    PendingOp::FileLen |
                    PendingOp::Write { .. } => file.fd.is_some(),
                    PendingOp::Sync { .. } |
                    PendingOp::Truncate { .. } |
                    PendingOp::Rename { .. } |
                    PendingOp::Link { .. } |
                    PendingOp::Unlink { .. } |
                    PendingOp::CreateDir { .. } |
                    PendingOp::Close => file.fd.is_some() && file.in_flight == 0,
                };
                if !ready {
                    break;
                }
                let op = self.files[file_index].queue.pop_front().expect("head was present");
                self.pending_count -= 1;
                self.submit_op(file_index, op);
            }
            file_index += 1;
        }
    }

    fn submit_op(&mut self, file_index: usize, op: PendingOp) {
        let token = self.files[file_index].token;
        let exclusive = matches!(
            &op,
            PendingOp::Truncate { .. } |
                PendingOp::Rename { .. } |
                PendingOp::Link { .. } |
                PendingOp::Unlink { .. } |
                PendingOp::CreateDir { .. }
        );
        let op = match op {
            PendingOp::Open { path, flags, mode } => InFlightOp::Open { path, flags, mode },
            PendingOp::Read { offset, len, to_end, buf } => InFlightOp::Read {
                owned: buf.is_some(),
                buf: self.read_buffer(buf, len),
                offset,
                wanted: len,
                have: 0,
                to_end,
            },
            PendingOp::ReadToEnd { offset, buf } => {
                InFlightOp::Statx { offset, statx: Box::new(MaybeUninit::uninit()), buf }
            }
            PendingOp::ReadInto { target, offset, len } => {
                InFlightOp::ReadInto { target, offset, wanted: len, have: 0 }
            }
            PendingOp::WriteFrom { source, offset, len } => {
                InFlightOp::WriteFrom { source, offset, len, written: 0 }
            }
            PendingOp::FileLen => InFlightOp::FileLen { statx: Box::new(MaybeUninit::uninit()) },
            PendingOp::Write { buf, offset, owned } => {
                InFlightOp::Write { buf, offset, written: 0, owned }
            }
            PendingOp::Sync { data_only, operation_id } => {
                InFlightOp::Sync { data_only, operation_id }
            }
            PendingOp::Truncate { len, operation_id } => InFlightOp::Truncate { len, operation_id },
            PendingOp::Rename { source, destination, destination_path, operation_id } => {
                let source = source.unwrap_or_else(|| {
                    c_path(&self.files[file_index].path)
                        .expect("an opened path cannot contain a nul byte")
                });
                InFlightOp::Rename { source, destination, destination_path, operation_id }
            }
            PendingOp::Link { destination, destination_path, operation_id } => {
                // `AT_EMPTY_PATH` needs `CAP_DAC_READ_SEARCH`; following the
                // descriptor's `/proc` link does not.
                let fd = self.files[file_index].fd.expect("link dispatched without an open fd");
                let source = CString::new(format!("/proc/self/fd/{fd}")).expect("no nul byte");
                InFlightOp::Link { source, destination, destination_path, operation_id }
            }
            PendingOp::Unlink { path, directory, operation_id } => {
                InFlightOp::Unlink { path, directory, operation_id }
            }
            PendingOp::CreateDir { path, operation_id } => {
                InFlightOp::CreateDir { path, operation_id }
            }
            PendingOp::Close => InFlightOp::Close,
        };
        let slot = self.alloc_slot(InFlight { file: token, op });
        self.files[file_index].in_flight += 1;
        self.files[file_index].exclusive_in_flight = exclusive;
        self.in_flight_count += 1;
        self.push_slot(slot as u64);
    }

    #[inline(always)]
    fn describe(&mut self, slot: usize) -> Op {
        let token = self.slab[slot].as_ref().expect("in-flight slot occupied").file;
        let fd = self.files.iter().find(|file| file.token == token).and_then(|file| file.fd);
        let entry = self.slab[slot].as_mut().expect("in-flight slot occupied");
        match &mut entry.op {
            InFlightOp::Open { path, flags, mode } => {
                Op::Open { path: path.as_ptr(), flags: *flags, mode: *mode }
            }
            InFlightOp::Read { buf, offset, wanted, have, .. } => {
                let fd = fd.expect("read dispatched without an open fd");
                let chunk = (*wanted - *have).min(MAX_OP_BYTES) as u32;
                // SAFETY: `have < wanted <= buf.capacity()`, so the pointer
                // stays inside the buffer's allocation.
                let ptr = unsafe { buf.as_mut_ptr().add(*have) };
                Op::Read { fd, ptr, len: chunk, offset: *offset + *have as u64 }
            }
            InFlightOp::ReadInto { target, offset, wanted, have } => {
                let fd = fd.expect("read dispatched without an open fd");
                let chunk = (*wanted - *have).min(MAX_OP_BYTES) as u32;
                // SAFETY: `have < wanted`, and the caller of `read_at_into`
                // keeps `wanted` bytes at the target valid.
                let ptr = unsafe { target.0.add(*have) };
                Op::Read { fd, ptr, len: chunk, offset: *offset + *have as u64 }
            }
            InFlightOp::WriteFrom { source, offset, len, written } => {
                let fd = fd.expect("write dispatched without an open fd");
                let chunk = (*len - *written).min(MAX_OP_BYTES) as u32;
                // SAFETY: `written < len`, and the caller of `write_at_from`
                // keeps `len` bytes at the source valid.
                let ptr = unsafe { source.0.add(*written) };
                Op::Write { fd, ptr, len: chunk, offset: *offset + *written as u64 }
            }
            InFlightOp::Statx { statx, .. } | InFlightOp::FileLen { statx } => {
                let fd = fd.expect("statx dispatched without an open fd");
                Op::Statx { fd, statx: statx.as_mut_ptr() }
            }
            InFlightOp::Write { buf, offset, written, .. } => {
                let fd = fd.expect("write dispatched without an open fd");
                let chunk = (buf.len() - *written).min(MAX_OP_BYTES) as u32;
                // SAFETY: `written < buf.len()`, so the pointer stays inside
                // the buffer's allocation.
                let ptr = unsafe { buf.as_ptr().add(*written) };
                Op::Write { fd, ptr, len: chunk, offset: *offset + *written as u64 }
            }
            InFlightOp::Sync { data_only, .. } => {
                let fd = fd.expect("sync dispatched without an open fd");
                Op::Fsync { fd, data_only: *data_only }
            }
            InFlightOp::Truncate { len, .. } => {
                let fd = fd.expect("truncate dispatched without an open fd");
                Op::Ftruncate { fd, len: *len }
            }
            InFlightOp::Rename { source, destination, .. } => {
                Op::RenameAt { source: source.as_ptr(), destination: destination.as_ptr() }
            }
            InFlightOp::Link { source, destination, .. } => {
                Op::LinkAt { source: source.as_ptr(), destination: destination.as_ptr() }
            }
            InFlightOp::Unlink { path, directory, .. } => {
                let flags = if *directory { libc::AT_REMOVEDIR } else { 0 };
                Op::UnlinkAt { path: path.as_ptr(), flags }
            }
            InFlightOp::CreateDir { path, .. } => Op::MkDirAt { path: path.as_ptr() },
            InFlightOp::Close => {
                let fd = fd.expect("close dispatched without an open fd");
                Op::Close { fd }
            }
        }
    }

    fn push_slot(&mut self, user_data: u64) {
        // The emulated push stays out of line, so on the ring path describing
        // the operation fuses with the ring's encoder.
        let pushed = if self.backend.is_ring() {
            self.push_op(user_data)
        } else {
            self.push_op_out_of_line(user_data)
        };
        if pushed {
            self.need_submit = true;
        } else {
            self.stalled.push(user_data);
        }
    }

    #[inline(always)]
    fn push_op(&mut self, user_data: u64) -> bool {
        let op = self.describe(user_data as usize);
        // SAFETY: every buffer and path `op` references lives in `self.slab`
        // until the completion for `user_data` is reaped, and its heap storage
        // is never touched while the backend owns the operation.
        unsafe { self.backend.push(op, user_data) }
    }

    #[cold]
    #[inline(never)]
    fn push_op_out_of_line(&mut self, user_data: u64) -> bool {
        self.push_op(user_data)
    }

    fn flush_submissions(&mut self) {
        if !self.need_submit {
            return;
        }
        match self.backend.submit() {
            Ok(_) => self.need_submit = false,
            Err(err)
                if matches!(err.raw_os_error(), Some(libc::EINTR | libc::EAGAIN | libc::EBUSY)) => {
            }
            Err(err) => flux_utils::safe_panic!("couldn't submit to the disk io ring: {err}"),
        }
    }

    fn drain_completions<F>(&mut self, handler: &mut F)
    where
        F: for<'a> FnMut(DiskEvent<'a>),
    {
        let mut completions = std::mem::take(&mut self.completions);
        completions.clear();
        self.backend.reap(&mut completions);
        for &(user_data, result) in &completions {
            self.complete(user_data, result, handler);
        }
        self.completions = completions;
    }

    fn complete<F>(&mut self, user_data: u64, result: i32, handler: &mut F)
    where
        F: for<'a> FnMut(DiskEvent<'a>),
    {
        let slot = user_data as usize;
        let Some(entry) = self.slab.get_mut(slot).and_then(Option::take) else {
            flux_utils::safe_panic!("disk io completion for vacant slot {slot}");
            return;
        };
        // Closes are never retried: Linux releases the descriptor even when
        // `close` reports `EINTR`, so a retry could close an unrelated,
        // reused descriptor.
        if result < 0 &&
            matches!(-result, libc::EINTR | libc::EAGAIN) &&
            !matches!(entry.op, InFlightOp::Close)
        {
            self.slab[slot] = Some(entry);
            self.push_slot(user_data);
            return;
        }
        let token = entry.file;
        let Some(file_index) = self.files.iter().position(|file| file.token == token) else {
            flux_utils::safe_panic!("disk io completion for retired file token {token:?}");
            self.free_slots.push(slot);
            self.in_flight_count = self.in_flight_count.saturating_sub(1);
            return;
        };
        match entry.op {
            InFlightOp::Open { .. } => self.complete_open(slot, file_index, result, handler),
            InFlightOp::Read { .. } | InFlightOp::Write { .. } => {
                self.complete_transfer(user_data, file_index, entry.op, result, handler);
            }
            InFlightOp::Statx { offset, statx, buf } => {
                self.complete_statx(user_data, file_index, offset, &statx, buf, result);
            }
            InFlightOp::FileLen { statx } => {
                self.finish_op(slot, file_index);
                match statx_len(&statx, result) {
                    Ok(len) => handler(DiskEvent::FileLen { file: token, len }),
                    Err(error) => handler(DiskEvent::Failed {
                        file: token,
                        op: FailedOp::FileLen,
                        operation_id: None,
                        error,
                    }),
                }
            }
            InFlightOp::ReadInto { .. } | InFlightOp::WriteFrom { .. } => {
                self.complete_caller_memory(user_data, file_index, entry.op, result, handler);
            }
            InFlightOp::Sync { operation_id, .. } => {
                self.finish_op(slot, file_index);
                if result < 0 {
                    let error = io::Error::from_raw_os_error(-result);
                    handler(DiskEvent::Failed {
                        file: token,
                        op: FailedOp::Sync,
                        operation_id: Some(operation_id),
                        error,
                    });
                } else {
                    handler(DiskEvent::Synced { file: token, operation_id });
                }
            }
            InFlightOp::Truncate { len, operation_id } => {
                self.finish_op(slot, file_index);
                if result < 0 {
                    let error = io::Error::from_raw_os_error(-result);
                    handler(DiskEvent::Failed {
                        file: token,
                        op: FailedOp::Truncate { len },
                        operation_id: Some(operation_id),
                        error,
                    });
                } else {
                    handler(DiskEvent::Truncated { file: token, len, operation_id });
                }
            }
            InFlightOp::Rename { .. } |
            InFlightOp::Link { .. } |
            InFlightOp::Unlink { .. } |
            InFlightOp::CreateDir { .. } => {
                self.complete_name_op(slot, file_index, entry.op, result, handler);
            }
            InFlightOp::Close => {
                self.finish_op(slot, file_index);
                // The descriptor is released even when `close` is interrupted.
                if result >= 0 || -result == libc::EINTR {
                    handler(DiskEvent::Closed { file: token });
                } else {
                    let error = io::Error::from_raw_os_error(-result);
                    handler(DiskEvent::Failed {
                        file: token,
                        op: FailedOp::Close,
                        operation_id: None,
                        error,
                    });
                }
                self.remove_file(file_index);
            }
        }
    }

    fn complete_name_op<F>(
        &mut self,
        slot: usize,
        file_index: usize,
        op: InFlightOp,
        result: i32,
        handler: &mut F,
    ) where
        F: for<'a> FnMut(DiskEvent<'a>),
    {
        self.finish_op(slot, file_index);
        let file = self.files[file_index].token;
        let (failed, operation_id, new_path, done) = match op {
            InFlightOp::Rename { destination_path, operation_id, .. } => {
                (FailedOp::Rename, operation_id, destination_path, DiskEvent::Renamed {
                    file,
                    operation_id,
                })
            }
            InFlightOp::Link { destination_path, operation_id, .. } => {
                (FailedOp::Link, operation_id, Some(destination_path), DiskEvent::Linked {
                    file,
                    operation_id,
                })
            }
            InFlightOp::Unlink { operation_id, .. } => {
                (FailedOp::Unlink, operation_id, None, DiskEvent::Unlinked { file, operation_id })
            }
            InFlightOp::CreateDir { operation_id, .. } => {
                (FailedOp::CreateDir, operation_id, None, DiskEvent::DirectoryCreated {
                    file,
                    operation_id,
                })
            }
            _ => unreachable!(
                "complete_name_op only receives renames, links, unlinks and directory creations"
            ),
        };
        if result < 0 {
            let error = io::Error::from_raw_os_error(-result);
            handler(DiskEvent::Failed {
                file,
                op: failed,
                operation_id: Some(operation_id),
                error,
            });
            return;
        }
        if let Some(path) = new_path {
            self.files[file_index].path = path;
        }
        handler(done);
    }

    fn complete_open<F>(&mut self, slot: usize, file_index: usize, result: i32, handler: &mut F)
    where
        F: for<'a> FnMut(DiskEvent<'a>),
    {
        self.finish_op(slot, file_index);
        let token = self.files[file_index].token;
        if result >= 0 {
            self.files[file_index].fd = Some(result);
            handler(DiskEvent::Opened { file: token });
        } else {
            let error = io::Error::from_raw_os_error(-result);
            warn!(
                path = %self.files[file_index].path.display(),
                %error,
                "couldn't open file"
            );
            handler(DiskEvent::Failed {
                file: token,
                op: FailedOp::Open,
                operation_id: None,
                error,
            });
            self.fail_queued(file_index, handler);
            self.remove_file(file_index);
        }
    }

    fn complete_statx(
        &mut self,
        user_data: u64,
        file_index: usize,
        offset: u64,
        statx: &MaybeUninit<libc::statx>,
        buf: Option<Vec<u8>>,
        result: i32,
    ) {
        let len = statx_len(statx, result).ok();
        self.start_read_to_end(user_data, file_index, offset, len, buf);
    }

    fn start_read_to_end(
        &mut self,
        user_data: u64,
        file_index: usize,
        offset: u64,
        file_len: Option<u64>,
        buf: Option<Vec<u8>>,
    ) {
        let wanted = Self::read_to_end_len(file_len, offset);
        let token = self.files[file_index].token;
        let slot = user_data as usize;
        self.slab[slot] = Some(InFlight {
            file: token,
            op: InFlightOp::Read {
                owned: buf.is_some(),
                buf: self.read_buffer(buf, wanted),
                offset,
                wanted,
                have: 0,
                to_end: true,
            },
        });
        self.push_slot(user_data);
    }

    /// The caller's buffer, cleared, else one from the pool; either with room
    /// for `len`.
    fn read_buffer(&mut self, owned: Option<Vec<u8>>, len: usize) -> Vec<u8> {
        owned.map_or_else(
            || self.take_buffer_with_capacity(len),
            |mut buf| {
                buf.clear();
                buf.reserve(len);
                buf
            },
        )
    }

    fn read_to_end_len(file_len: Option<u64>, offset: u64) -> usize {
        file_len
            .map(|len| len.saturating_sub(offset).saturating_add(1))
            .and_then(|len| usize::try_from(len).ok())
            .unwrap_or(READ_TO_END_CHUNK)
            .max(READ_TO_END_CHUNK)
    }

    fn complete_caller_memory<F>(
        &mut self,
        user_data: u64,
        file_index: usize,
        op: InFlightOp,
        result: i32,
        handler: &mut F,
    ) where
        F: for<'a> FnMut(DiskEvent<'a>),
    {
        let slot = user_data as usize;
        let file = self.files[file_index].token;
        match op {
            InFlightOp::ReadInto { offset, wanted, .. } if result < 0 => {
                self.finish_op(slot, file_index);
                let error = io::Error::from_raw_os_error(-result);
                let op = FailedOp::Read { offset, len: wanted };
                handler(DiskEvent::Failed { file, op, operation_id: None, error });
            }
            InFlightOp::ReadInto { target, offset, wanted, have } => {
                let have = have + result as usize;
                if result > 0 && have < wanted {
                    let op = InFlightOp::ReadInto { target, offset, wanted, have };
                    self.slab[slot] = Some(InFlight { file, op });
                    self.push_slot(user_data);
                    return;
                }
                self.finish_op(slot, file_index);
                handler(DiskEvent::ReadInto { file, offset, len: have, eof: result == 0 });
            }
            InFlightOp::WriteFrom { offset, len, .. } if result <= 0 => {
                self.finish_op(slot, file_index);
                let error = if result == 0 {
                    io::Error::from(io::ErrorKind::WriteZero)
                } else {
                    io::Error::from_raw_os_error(-result)
                };
                let op = FailedOp::Write { offset, len };
                handler(DiskEvent::Failed { file, op, operation_id: None, error });
            }
            InFlightOp::WriteFrom { source, offset, len, written } => {
                let written = written + result as usize;
                if written < len {
                    let op = InFlightOp::WriteFrom { source, offset, len, written };
                    self.slab[slot] = Some(InFlight { file, op });
                    self.push_slot(user_data);
                    return;
                }
                self.finish_op(slot, file_index);
                handler(DiskEvent::Written { file, offset, len });
            }
            _ => unreachable!("complete_caller_memory only receives caller-memory transfers"),
        }
    }

    /// Handles a read or write completion, transparently continuing after
    /// short transfers.
    fn complete_transfer<F>(
        &mut self,
        user_data: u64,
        file_index: usize,
        op: InFlightOp,
        result: i32,
        handler: &mut F,
    ) where
        F: for<'a> FnMut(DiskEvent<'a>),
    {
        let slot = user_data as usize;
        let token = self.files[file_index].token;
        match op {
            InFlightOp::Read { mut buf, offset, mut wanted, have, to_end, owned } => {
                if result < 0 {
                    self.finish_op(slot, file_index);
                    let error = io::Error::from_raw_os_error(-result);
                    if owned {
                        let error = Some(error);
                        handler(DiskEvent::ReadOwned {
                            file: token,
                            offset,
                            buf,
                            eof: false,
                            error,
                        });
                        return;
                    }
                    let op = FailedOp::Read { offset, len: wanted };
                    handler(DiskEvent::Failed { file: token, op, operation_id: None, error });
                    self.recycle(buf);
                    return;
                }
                let have = have + result as usize;
                // SAFETY: a successful read initialized exactly `result`
                // bytes starting at the prior `have`, within the reserved
                // read range.
                unsafe { buf.set_len(have) };
                if result == 0 || (have >= wanted && !to_end) {
                    self.finish_op(slot, file_index);
                    let eof = result == 0;
                    if owned {
                        handler(DiskEvent::ReadOwned {
                            file: token,
                            offset,
                            buf,
                            eof,
                            error: None,
                        });
                        return;
                    }
                    handler(DiskEvent::Read { file: token, offset, payload: &buf[..have], eof });
                    self.recycle(buf);
                } else {
                    if have >= wanted {
                        // The buffer filled before end-of-file: grow it and
                        // keep reading.
                        wanted = wanted.saturating_mul(2);
                        buf.reserve(wanted - buf.len());
                    }
                    self.slab[slot] = Some(InFlight {
                        file: token,
                        op: InFlightOp::Read { buf, offset, wanted, have, to_end, owned },
                    });
                    self.push_slot(user_data);
                }
            }
            InFlightOp::Write { buf, offset, written, owned } => {
                if result <= 0 {
                    self.finish_op(slot, file_index);
                    let error = if result == 0 {
                        io::Error::from(io::ErrorKind::WriteZero)
                    } else {
                        io::Error::from_raw_os_error(-result)
                    };
                    if owned {
                        let error = Some(error);
                        handler(DiskEvent::WrittenOwned { file: token, offset, buf, error });
                        return;
                    }
                    let op = FailedOp::Write { offset, len: buf.len() };
                    handler(DiskEvent::Failed { file: token, op, operation_id: None, error });
                    self.recycle(buf);
                    return;
                }
                let written = written + result as usize;
                if written >= buf.len() {
                    self.finish_op(slot, file_index);
                    if owned {
                        handler(DiskEvent::WrittenOwned { file: token, offset, buf, error: None });
                        return;
                    }
                    handler(DiskEvent::Written { file: token, offset, len: buf.len() });
                    self.recycle(buf);
                } else {
                    self.slab[slot] = Some(InFlight {
                        file: token,
                        op: InFlightOp::Write { buf, offset, written, owned },
                    });
                    self.push_slot(user_data);
                }
            }
            _ => unreachable!("complete_transfer only receives reads and writes"),
        }
    }

    fn finish_op(&mut self, slot: usize, file_index: usize) {
        self.free_slots.push(slot);
        self.files[file_index].in_flight -= 1;
        if self.files[file_index].in_flight == 0 {
            self.files[file_index].exclusive_in_flight = false;
        }
        self.in_flight_count -= 1;
    }

    fn fail_queued<F>(&mut self, file_index: usize, handler: &mut F)
    where
        F: for<'a> FnMut(DiskEvent<'a>),
    {
        let token = self.files[file_index].token;
        while let Some(op) = self.files[file_index].queue.pop_front() {
            self.pending_count -= 1;
            let error = io::Error::from_raw_os_error(libc::EBADF);
            let (failed, operation_id) = match op {
                PendingOp::Read { offset, buf: Some(mut buf), .. } |
                PendingOp::ReadToEnd { offset, buf: Some(mut buf) } => {
                    buf.clear();
                    let error = Some(error);
                    handler(DiskEvent::ReadOwned { file: token, offset, buf, eof: false, error });
                    continue;
                }
                PendingOp::Write { buf, offset, owned: true } => {
                    let error = Some(error);
                    handler(DiskEvent::WrittenOwned { file: token, offset, buf, error });
                    continue;
                }
                PendingOp::WriteFrom { offset, len, .. } => (FailedOp::Write { offset, len }, None),
                PendingOp::FileLen => (FailedOp::FileLen, None),
                PendingOp::Open { .. } => (FailedOp::Open, None),
                PendingOp::Read { offset, len, .. } | PendingOp::ReadInto { offset, len, .. } => {
                    (FailedOp::Read { offset, len }, None)
                }
                PendingOp::ReadToEnd { offset, .. } => {
                    (FailedOp::Read { offset, len: READ_TO_END_CHUNK }, None)
                }
                PendingOp::Write { buf, offset, .. } => {
                    let len = buf.len();
                    self.recycle(buf);
                    (FailedOp::Write { offset, len }, None)
                }
                PendingOp::Sync { operation_id, .. } => (FailedOp::Sync, Some(operation_id)),
                PendingOp::Truncate { len, operation_id } => {
                    (FailedOp::Truncate { len }, Some(operation_id))
                }
                PendingOp::Rename { operation_id, .. } => (FailedOp::Rename, Some(operation_id)),
                PendingOp::Link { operation_id, .. } => (FailedOp::Link, Some(operation_id)),
                PendingOp::Unlink { operation_id, .. } => (FailedOp::Unlink, Some(operation_id)),
                PendingOp::CreateDir { operation_id, .. } => {
                    (FailedOp::CreateDir, Some(operation_id))
                }
                PendingOp::Close => (FailedOp::Close, None),
            };
            handler(DiskEvent::Failed { file: token, op: failed, operation_id, error });
        }
    }

    fn remove_file(&mut self, file_index: usize) {
        let file = &self.files[file_index];
        flux_utils::safe_assert!(file.in_flight == 0);
        flux_utils::safe_assert!(!file.exclusive_in_flight);
        flux_utils::safe_assert!(file.queue.is_empty());
        self.files.swap_remove(file_index);
    }
}

fn statx_len(statx: &MaybeUninit<libc::statx>, result: i32) -> io::Result<u64> {
    if result < 0 {
        return Err(io::Error::from_raw_os_error(-result));
    }
    // SAFETY: a successful statx completion writes the output struct.
    let statx = unsafe { statx.assume_init_ref() };
    if statx.stx_mask & libc::STATX_SIZE == 0 {
        return Err(io::Error::other("statx returned no size"));
    }
    Ok(statx.stx_size)
}

fn c_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a nul byte"))
}

/// The ring's descriptor: readable while completions wait to be reaped.
impl AsRawFd for DiskIo {
    fn as_raw_fd(&self) -> RawFd {
        self.backend.as_raw_fd()
    }
}

impl Drop for DiskIo {
    fn drop(&mut self) {
        // Stalled continuations, and operations the backend never handed on,
        // are freed directly.
        self.backend.take_unsubmitted(&mut self.stalled);
        for user_data in std::mem::take(&mut self.stalled) {
            if self.slab[user_data as usize].take().is_some() {
                self.in_flight_count -= 1;
            }
        }
        // The kernel or a worker writes into slab-owned buffers, so wait for
        // every operation handed on. Regular-file operations always complete;
        // this is bounded by device latency.
        while self.in_flight_count > 0 {
            let mut completions = std::mem::take(&mut self.completions);
            completions.clear();
            self.backend.reap(&mut completions);
            for &(user_data, _) in &completions {
                let Some(entry) = self.slab[user_data as usize].take() else { continue };
                self.in_flight_count -= 1;
                if matches!(entry.op, InFlightOp::Close) &&
                    let Some(file) = self.files.iter_mut().find(|file| file.token == entry.file)
                {
                    file.fd = None;
                }
            }
            self.completions = completions;
            if self.in_flight_count == 0 {
                break;
            }
            match self.backend.wait() {
                Ok(()) => {}
                Err(err) if err.raw_os_error() == Some(libc::EINTR) => {}
                Err(err) => {
                    warn!(?err, "couldn't drain the disk io ring; leaking in-flight buffers");
                    std::mem::forget(std::mem::take(&mut self.slab));
                    // A worker may still use a descriptor by its number,
                    // which closing would free for reuse.
                    return;
                }
            }
        }
        for file in &self.files {
            if let Some(fd) = file.fd {
                // SAFETY: `fd` is owned by this instance and every kernel
                // operation referencing it has completed.
                unsafe { libc::close(fd) };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::CString,
        fs,
        os::{
            fd::{AsRawFd, IntoRawFd},
            unix::{ffi::OsStrExt, fs::OpenOptionsExt},
        },
        path::PathBuf,
        sync::mpsc,
        time::{Duration as StdDuration, Instant as StdInstant},
    };

    use super::{
        DiskConfig, DiskEvent, DiskIo, FailedOp, FileToken, OpenOptions, OperationId,
        READ_TO_END_CHUNK, emulation::WORKER_NAME_PREFIX, pin_io_workers,
    };

    /// One constructor per backend; a failing test's captured output names
    /// the backend it ran on.
    const BACKENDS: [fn(DiskConfig) -> DiskIo; 2] = [
        |config| {
            println!("backend: DiskIo::new");
            DiskIo::new(config).unwrap()
        },
        |config| {
            println!("backend: emulated");
            DiskIo::emulated(config).unwrap()
        },
    ];

    fn overwrite() -> OpenOptions {
        OpenOptions::new().write(true).create(true).truncate(true)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Ev {
        Opened(FileToken),
        Read { file: FileToken, offset: u64, payload: Vec<u8>, eof: bool },
        Written { file: FileToken, offset: u64, len: usize },
        ReadOwned { file: FileToken, offset: u64, payload: Vec<u8>, eof: bool, failed: bool },
        WrittenOwned { file: FileToken, offset: u64, len: usize, failed: bool },
        FileLen { file: FileToken, len: u64 },
        ReadInto { file: FileToken, offset: u64, len: usize, eof: bool },
        Synced(FileToken, OperationId),
        Truncated { file: FileToken, len: u64, operation_id: OperationId },
        Renamed { file: FileToken, operation_id: OperationId },
        Linked { file: FileToken, operation_id: OperationId },
        Unlinked { file: FileToken, operation_id: OperationId },
        DirectoryCreated { file: FileToken, operation_id: OperationId },
        Closed(FileToken),
        Failed { file: FileToken, op: FailedOp, operation_id: Option<OperationId> },
    }

    impl From<DiskEvent<'_>> for Ev {
        fn from(event: DiskEvent<'_>) -> Self {
            match event {
                DiskEvent::Opened { file } => Self::Opened(file),
                DiskEvent::Read { file, offset, payload, eof } => {
                    Self::Read { file, offset, payload: payload.to_vec(), eof }
                }
                DiskEvent::Written { file, offset, len } => Self::Written { file, offset, len },
                DiskEvent::ReadOwned { file, offset, buf, eof, error } => {
                    Self::ReadOwned { file, offset, payload: buf, eof, failed: error.is_some() }
                }
                DiskEvent::WrittenOwned { file, offset, buf, error } => {
                    Self::WrittenOwned { file, offset, len: buf.len(), failed: error.is_some() }
                }
                DiskEvent::FileLen { file, len } => Self::FileLen { file, len },
                DiskEvent::ReadInto { file, offset, len, eof } => {
                    Self::ReadInto { file, offset, len, eof }
                }
                DiskEvent::Synced { file, operation_id } => Self::Synced(file, operation_id),
                DiskEvent::Truncated { file, len, operation_id } => {
                    Self::Truncated { file, len, operation_id }
                }
                DiskEvent::Renamed { file, operation_id } => Self::Renamed { file, operation_id },
                DiskEvent::Linked { file, operation_id } => Self::Linked { file, operation_id },
                DiskEvent::Unlinked { file, operation_id } => Self::Unlinked { file, operation_id },
                DiskEvent::DirectoryCreated { file, operation_id } => {
                    Self::DirectoryCreated { file, operation_id }
                }
                DiskEvent::Closed { file } => Self::Closed(file),
                DiskEvent::Failed { file, op, operation_id, .. } => {
                    Self::Failed { file, op, operation_id }
                }
            }
        }
    }

    fn drive(disk: &mut DiskIo, events: &mut Vec<Ev>, done: impl Fn(&[Ev]) -> bool) {
        let deadline = StdInstant::now() + StdDuration::from_secs(5);
        while !done(events) {
            assert!(StdInstant::now() < deadline, "timed out; events: {events:?}");
            disk.poll_with(|event| events.push(Ev::from(event)));
        }
    }

    /// Whether `disk`'s descriptor is readable within `timeout_ms`.
    fn readable(disk: &DiskIo, timeout_ms: i32) -> bool {
        let mut fd = libc::pollfd { fd: disk.as_raw_fd(), events: libc::POLLIN, revents: 0 };
        // SAFETY: `fd` outlives the call and the count is one.
        unsafe { libc::poll(&raw mut fd, 1, timeout_ms) == 1 }
    }

    #[test]
    fn writes_sync_and_close_before_the_first_poll() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("log.bin");
            let mut disk = new(DiskConfig::default());
            let file = disk.open(&path, overwrite()).unwrap();
            assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"alpha")));
            assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"beta")));
            assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"gamma")));
            let sync_id = disk.sync_all(file).unwrap();
            assert!(disk.close(file));
            disk.submit();
            assert!(readable(&disk, 5_000), "a completion waits");

            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));

            assert_eq!(events[0], Ev::Opened(file));
            let synced_at =
                events.iter().position(|event| *event == Ev::Synced(file, sync_id)).unwrap();
            for (offset, len) in [(0, 5), (5, 4), (9, 5)] {
                let written = Ev::Written { file, offset, len };
                let written_at = events.iter().position(|event| *event == written).unwrap();
                assert!(written_at < synced_at, "sync must complete after writes: {events:?}");
            }
            assert_eq!(*events.last().unwrap(), Ev::Closed(file));
            assert!(disk.is_idle());
            assert!(!readable(&disk, 0), "idle yet readable");
            assert_eq!(fs::read(&path).unwrap(), b"alphabetagamma");
        }
    }

    #[test]
    fn reads_report_payload_and_eof() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("data.bin");
            fs::write(&path, b"0123456789").unwrap();
            let mut disk = new(DiskConfig::default());
            let file = disk.open(&path, OpenOptions::new().read(true)).unwrap();
            assert!(!disk.read_at(file, 0, 0));
            assert!(disk.read_at(file, 0, 10));
            assert!(disk.read_at(file, 4, 100));
            assert!(disk.read_at(file, 10, 3));

            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| {
                events.iter().filter(|event| matches!(event, Ev::Read { .. })).count() == 3
            });

            let read = |offset, payload: &[u8], eof| Ev::Read {
                file,
                offset,
                payload: payload.to_vec(),
                eof,
            };
            assert!(events.contains(&read(0, b"0123456789", false)));
            assert!(events.contains(&read(4, b"456789", true)));
            assert!(events.contains(&read(10, b"", true)));
        }
    }

    #[test]
    fn read_to_end_size_hint_uses_metadata_or_falls_back() {
        assert_eq!(DiskIo::read_to_end_len(Some(200_000), 0), 200_001);
        assert_eq!(DiskIo::read_to_end_len(Some(200_000), 199_990), READ_TO_END_CHUNK);
        assert_eq!(DiskIo::read_to_end_len(Some(0), 0), READ_TO_END_CHUNK);
        assert_eq!(DiskIo::read_to_end_len(Some(10), 20), READ_TO_END_CHUNK);
        assert_eq!(DiskIo::read_to_end_len(None, 0), READ_TO_END_CHUNK);
    }

    #[test]
    fn read_to_end_returns_full_contents_from_a_static_large_file() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("grown.bin");
            let data: Vec<u8> = (0..200_000u32).map(|i| i as u8).collect();
            fs::write(&path, &data).unwrap();
            let mut disk = new(DiskConfig::default());
            let file = disk.open(&path, OpenOptions::new().read(true)).unwrap();
            assert!(disk.read_to_end(file, 0));
            assert!(disk.read_to_end(file, 199_990));
            assert!(disk.close(file));

            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));
            assert!(events.contains(&Ev::Read {
                file,
                offset: 0,
                payload: data.clone(),
                eof: true
            }));
            assert!(events.contains(&Ev::Read {
                file,
                offset: 199_990,
                payload: data[199_990..].to_vec(),
                eof: true,
            }));
        }
    }

    #[test]
    fn read_to_end_grows_after_a_stale_statx_hint() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("grown.bin");
            let data: Vec<u8> = (0..READ_TO_END_CHUNK * 3).map(|i| i as u8).collect();
            fs::write(&path, &data).unwrap();
            let mut disk = new(DiskConfig::default());
            let file = FileToken(0);
            disk.files.push(super::File {
                token: file,
                path: path.clone(),
                fd: Some(fs::File::open(&path).unwrap().into_raw_fd()),
                closing: false,
                write_cursor: 0,
                in_flight: 1,
                exclusive_in_flight: false,
                queue: std::collections::VecDeque::new(),
            });
            disk.slab.push(None);
            disk.in_flight_count = 1;

            // Start from a deliberately stale size captured before the file grew.
            disk.start_read_to_end(0, 0, 0, Some(READ_TO_END_CHUNK as u64), None);

            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| {
                events.iter().any(|event| matches!(event, Ev::Read { .. }))
            });
            assert!(events.contains(&Ev::Read { file, offset: 0, payload: data, eof: true }));
            assert!(disk.is_idle());
        }
    }

    #[test]
    fn read_to_end_of_an_empty_file_is_empty_and_eof() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("empty.bin");
            fs::write(&path, b"").unwrap();
            let mut disk = new(DiskConfig::default());
            let file = disk.open(&path, OpenOptions::new().read(true)).unwrap();
            assert!(disk.read_to_end(file, 0));
            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| {
                events.iter().any(|event| matches!(event, Ev::Read { .. }))
            });
            assert!(events.contains(&Ev::Read { file, offset: 0, payload: Vec::new(), eof: true }));
        }
    }

    #[test]
    fn failed_open_fails_queued_ops_and_retires_the_token() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("missing").join("file.bin");
            let mut disk = new(DiskConfig::default());
            let file = disk.open(&path, overwrite()).unwrap();
            assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"data")));
            let sync_id = disk.sync_data(file).unwrap();
            let truncate_id = disk.truncate(file, 2).unwrap();
            let rename_id = disk.rename(file, dir.path().join("renamed.bin")).unwrap().unwrap();
            let link_id = disk.link(file, dir.path().join("linked.bin")).unwrap().unwrap();
            let unlink_id = disk.unlink(file, dir.path().join("gone.bin")).unwrap().unwrap();
            assert!(disk.close(file));

            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.len() >= 8);

            assert_eq!(events, vec![
                Ev::Failed { file, op: FailedOp::Open, operation_id: None },
                Ev::Failed { file, op: FailedOp::Write { offset: 0, len: 4 }, operation_id: None },
                Ev::Failed { file, op: FailedOp::Sync, operation_id: Some(sync_id) },
                Ev::Failed {
                    file,
                    op: FailedOp::Truncate { len: 2 },
                    operation_id: Some(truncate_id),
                },
                Ev::Failed { file, op: FailedOp::Rename, operation_id: Some(rename_id) },
                Ev::Failed { file, op: FailedOp::Link, operation_id: Some(link_id) },
                Ev::Failed { file, op: FailedOp::Unlink, operation_id: Some(unlink_id) },
                Ev::Failed { file, op: FailedOp::Close, operation_id: None },
            ]);
            assert!(!disk.write_with(file, |buf| buf.push(1)));
            assert!(disk.is_idle());
        }
    }

    #[test]
    fn barriers_order_overlapping_writes() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("patched.bin");
            let mut disk = new(DiskConfig::default());
            let file = disk.open(&path, overwrite()).unwrap();
            assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"aaaa")));
            disk.sync_all(file).unwrap();
            assert!(disk.write_at_with(file, 1, |buf| buf.extend_from_slice(b"bb")));
            disk.sync_data(file).unwrap();
            assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"cccc")));
            assert!(disk.close(file));

            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));
            assert_eq!(fs::read(&path).unwrap(), b"abbacccc");
        }
    }

    #[test]
    fn sync_echoes_returned_operation_id() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let mut disk = new(DiskConfig::default());
            let mut seen_ids = std::collections::HashSet::new();
            let mut events = Vec::new();
            for _ in 0..3 {
                let first = disk.open(dir.path().join("first.bin"), overwrite()).unwrap();
                let second = disk.open(dir.path().join("second.bin"), overwrite()).unwrap();
                assert!(disk.write_with(first, |buf| buf.extend_from_slice(b"durable")));
                let first_id = disk.sync_data(first).unwrap();
                let second_id = disk.sync_all(second).unwrap();
                for id in [first_id, second_id] {
                    assert!(seen_ids.insert(id), "operation IDs must not be reused");
                }
                assert!(disk.close(first));
                assert!(disk.close(second));
                drive(&mut disk, &mut events, |events| {
                    events.contains(&Ev::Closed(first)) && events.contains(&Ev::Closed(second))
                });
                assert!(events.contains(&Ev::Synced(first, first_id)));
                assert!(events.contains(&Ev::Synced(second, second_id)));
            }
        }
    }

    #[test]
    fn failed_operations_echo_returned_ids() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("original.bin");
            fs::write(&path, b"unchanged").unwrap();
            let mut disk = new(DiskConfig::default());
            let file = disk.open(&path, OpenOptions::new().read(true)).unwrap();
            let truncate_id = disk.truncate(file, 0).unwrap();
            let rename_id =
                disk.rename(file, dir.path().join("missing/file.bin")).unwrap().unwrap();
            assert_ne!(truncate_id, rename_id);
            assert!(disk.close(file));

            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));
            assert_eq!(events, vec![
                Ev::Opened(file),
                Ev::Failed {
                    file,
                    op: FailedOp::Truncate { len: 0 },
                    operation_id: Some(truncate_id),
                },
                Ev::Failed { file, op: FailedOp::Rename, operation_id: Some(rename_id) },
                Ev::Closed(file),
            ]);
            assert_eq!(fs::read(&path).unwrap(), b"unchanged");
        }
    }

    #[test]
    fn truncate_blocks_later_writes() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("repaired.bin");
            fs::write(&path, b"valid-torn-tail").unwrap();
            let mut disk = new(DiskConfig::default());
            let file = disk.open(&path, OpenOptions::new().read(true).write(true)).unwrap();
            assert!(disk.set_write_cursor(file, 5));
            let operation_id = disk.truncate(file, 5).unwrap();
            assert!(disk.file_len(file));
            disk.sync_data(file).unwrap();
            assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"-next")));
            assert!(disk.close(file));

            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));

            let truncated = Ev::Truncated { file, len: 5, operation_id };
            let truncated_at = events.iter().position(|event| *event == truncated).unwrap();
            let written = Ev::Written { file, offset: 5, len: 5 };
            let written_at = events.iter().position(|event| *event == written).unwrap();
            assert!(truncated_at < written_at, "write completed before truncate: {events:?}");
            assert!(events.contains(&Ev::FileLen { file, len: 5 }), "{events:?}");
            assert_eq!(fs::read(&path).unwrap(), b"valid-next");
        }
    }

    #[test]
    fn rename_then_directory_sync_reports_operation_ids() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("wal.tmp");
            let destination = dir.path().join("wal");
            let mut disk = new(DiskConfig::default());
            let file = disk.open(&source, overwrite()).unwrap();
            let directory = disk.open_directory(dir.path()).unwrap();
            assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"complete")));
            disk.sync_data(file).unwrap();
            let rename_id = disk.rename(file, &destination).unwrap().unwrap();
            assert!(disk.close(file));

            let mut events = Vec::new();
            let renamed = Ev::Renamed { file, operation_id: rename_id };
            drive(&mut disk, &mut events, |events| events.contains(&renamed));
            assert!(!source.exists());
            assert_eq!(fs::read(&destination).unwrap(), b"complete");

            let directory_sync_id = disk.sync_all(directory).unwrap();
            assert!(disk.close(directory));
            drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(directory)));
            assert!(events.contains(&Ev::Synced(directory, directory_sync_id)));
            assert!(disk.is_idle());
        }
    }

    #[test]
    fn file_len_and_transfers_through_caller_memory() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("payload.bin");
            fs::write(&path, b"0123456789").unwrap();
            let mut disk = new(DiskConfig::default());
            let file = disk.open(&path, OpenOptions::new().read(true)).unwrap();
            assert!(disk.file_len(file));
            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.contains(&Ev::FileLen { file, len: 10 }));

            let mut target = [0u8; 12];
            // SAFETY: `target` outlives the completion and is untouched until it.
            assert!(unsafe { disk.read_at_into(file, 2, target.as_mut_ptr(), 12) });
            let read = Ev::ReadInto { file, offset: 2, len: 8, eof: true };
            drive(&mut disk, &mut events, |events| events.contains(&read));
            assert_eq!(&target[..8], b"23456789");

            let written = dir.path().join("written.bin");
            let file = disk.open(dir.path(), OpenOptions::new().write(true).tmpfile(true)).unwrap();
            let source = *b"payload";
            // SAFETY: `source` outlives the completion and is unchanged until it.
            assert!(unsafe { disk.write_at_from(file, 0, source.as_ptr(), source.len()) });
            let linked =
                Ev::Linked { file, operation_id: disk.link(file, &written).unwrap().unwrap() };
            drive(&mut disk, &mut events, |events| events.contains(&linked));
            assert!(events.contains(&Ev::Written { file, offset: 0, len: 7 }));
            assert_eq!(fs::read(&written).unwrap(), b"payload");
        }
    }

    #[test]
    fn path_rename_and_directory_removal_order_on_a_directory_token() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("staged.bin");
            let made = dir.path().join("made");
            let destination = made.join("final.bin");
            let empty = dir.path().join("group");
            fs::write(&source, b"block").unwrap();
            fs::create_dir(&empty).unwrap();
            let mut disk = new(DiskConfig::default());
            let directory = disk.open_directory(dir.path()).unwrap();
            let create_id = disk.create_dir(directory, &made).unwrap().unwrap();
            let rename_id = disk.rename_path(directory, &source, &destination).unwrap().unwrap();
            let remove_id = disk.remove_dir(directory, &empty).unwrap().unwrap();
            let missing_id = disk.remove_dir(directory, &empty).unwrap().unwrap();
            assert!(disk.close(directory));

            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(directory)));
            let created = Ev::DirectoryCreated { file: directory, operation_id: create_id };
            let renamed = Ev::Renamed { file: directory, operation_id: rename_id };
            let removed = Ev::Unlinked { file: directory, operation_id: remove_id };
            let missing = Ev::Failed {
                file: directory,
                op: FailedOp::Unlink,
                operation_id: Some(missing_id),
            };
            let at = |event: &Ev| events.iter().position(|e| e == event).unwrap();
            assert!(
                at(&created) < at(&renamed) &&
                    at(&renamed) < at(&removed) &&
                    at(&removed) < at(&missing),
                "{events:?}"
            );
            assert!(!source.exists());
            assert_eq!(fs::read(&destination).unwrap(), b"block");
            assert!(!empty.exists());
        }
    }

    #[test]
    fn cursor_can_append_to_an_existing_file() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("existing.bin");
            fs::write(&path, b"0123").unwrap();
            let mut disk = new(DiskConfig::default());
            let file = disk.open(&path, OpenOptions::new().read(true).write(true)).unwrap();
            assert!(disk.set_write_cursor(file, 4));
            assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"45")));
            assert!(disk.close(file));

            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));
            assert_eq!(fs::read(&path).unwrap(), b"012345");
        }
    }

    #[test]
    fn tiny_ring_applies_backpressure_without_losing_ops() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("burst.bin");
            let mut disk =
                new(DiskConfig { ring_entries: 2, max_in_flight: 2, ..DiskConfig::default() });
            let file = disk.open(&path, overwrite()).unwrap();
            let mut expected = Vec::new();
            for value in 0..64u8 {
                assert!(disk.write_with(file, |buf| buf.extend_from_slice(&[value; 8])));
                expected.extend_from_slice(&[value; 8]);
            }
            assert!(disk.close(file));

            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));

            let written = events.iter().filter(|event| matches!(event, Ev::Written { .. })).count();
            assert_eq!(written, 64);
            assert!(disk.is_idle());
            assert_eq!(fs::read(&path).unwrap(), expected);
        }
    }

    #[test]
    fn closing_unknown_and_empty_submissions_are_rejected() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let mut disk = new(DiskConfig::default());
            let file = disk.open(dir.path().join("a.bin"), overwrite()).unwrap();
            assert!(!disk.write_with(file, |_| {}));
            assert!(disk.close(file));
            assert!(!disk.write_with(file, |buf| buf.push(1)));
            assert!(!disk.write_at_with(file, 0, |buf| buf.push(1)));
            assert!(!disk.read_at(file, 0, 1));
            assert!(!disk.read_to_end(file, 0));
            for token in [file, FileToken(usize::MAX)] {
                assert!(disk.sync_all(token).is_none());
                assert!(disk.sync_data(token).is_none());
                assert!(disk.truncate(token, 0).is_none());
                assert!(disk.rename(token, dir.path().join("unused.bin")).unwrap().is_none());
            }
            assert!(!disk.set_write_cursor(file, 0));
            assert!(!disk.close(file));

            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));
            assert!(!disk.write_with(file, |buf| buf.push(1)), "retired tokens are rejected");
            assert!(disk.sync_all(file).is_none());
            assert!(disk.sync_data(file).is_none());
            assert!(disk.truncate(file, 0).is_none());
            assert!(disk.rename(file, dir.path().join("unused.bin")).unwrap().is_none());
            assert!(disk.is_idle());
        }
    }

    #[test]
    fn rename_rejects_invalid_paths_without_queueing() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let mut disk = new(DiskConfig::default());
            let file = disk.open(dir.path().join("original.bin"), overwrite()).unwrap();
            let error = disk.rename(file, "invalid\0path").unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(disk.close(file));
            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));
            assert_eq!(events, vec![Ev::Opened(file), Ev::Closed(file)]);
            assert!(disk.is_idle());
        }
    }

    #[test]
    fn open_options_validate_like_std() {
        let dir = tempfile::tempdir().unwrap();
        let mut disk = DiskIo::default();
        let no_access = disk.open(dir.path().join("x"), OpenOptions::new());
        assert_eq!(no_access.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
        let create_without_write =
            disk.open(dir.path().join("x"), OpenOptions::new().read(true).create(true));
        assert_eq!(create_without_write.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    }

    fn fifo(path: PathBuf) -> PathBuf {
        let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `c_path` is a nul-terminated path that outlives the call.
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        path
    }

    /// Drop waits for operations handed on and frees those it never handed
    /// on, without hanging.
    #[test]
    fn drop_waits_for_submitted_writes() {
        for new in BACKENDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("dropped.bin");
            let payload = vec![7u8; 1 << 20];
            let mut disk = new(DiskConfig::default());
            // Readable, so the emulation can serve the read below from the
            // page cache and must also free inline completions.
            let file = disk.open(&path, overwrite().read(true)).unwrap();
            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.contains(&Ev::Opened(file)));
            assert!(disk.write_with(file, |buf| buf.extend_from_slice(&payload)));
            disk.poll_with(|_| {});
            // Reaches private state: only a continuation pushed after the last
            // submission is still unsubmitted at drop, and no public call
            // leaves one behind deterministically.
            let slot = disk.slab.len();
            disk.slab.push(None);
            disk.files[0].in_flight += 1;
            disk.in_flight_count += 1;
            disk.start_read_to_end(slot as u64, 0, 0, None, None);
            let (dropped, done) = mpsc::channel();
            std::thread::spawn(move || {
                drop(disk);
                dropped.send(()).unwrap();
            });
            done.recv_timeout(StdDuration::from_secs(5)).expect("drop hung");
            assert_eq!(fs::read(&path).unwrap(), payload);
        }

        // Opening a FIFO blocks a worker until a writer opens it, so drop
        // must wait. Emulation only: `io_uring` opens it without blocking.
        let dir = tempfile::tempdir().unwrap();
        let fifo = fifo(dir.path().join("blocked"));
        let mut disk = DiskIo::emulated(DiskConfig::default()).unwrap();
        disk.open(&fifo, OpenOptions::new().read(true)).unwrap();
        disk.submit();
        let (dropped, done) = mpsc::channel();
        std::thread::spawn(move || {
            drop(disk);
            dropped.send(()).unwrap();
        });
        assert!(done.recv_timeout(StdDuration::from_millis(200)).is_err(), "drop did not wait");
        let _writer = fs::OpenOptions::new().write(true).open(&fifo).unwrap();
        done.recv_timeout(StdDuration::from_secs(5)).expect("drop hung");
    }

    /// Whether `DiskIo::new` uses `io_uring` here; tests that count its
    /// workers skip their `io_uring` leg otherwise.
    fn uses_ring() -> bool {
        DiskIo::new(DiskConfig::default()).unwrap().backend.is_ring()
    }

    const CHILD: &str = "FLUX_DISK_TEST_CHILD";

    /// Whether this process is a child running only `test`. Otherwise runs
    /// `test` in such a child, asserts that it passed, and returns false.
    fn in_child(test: &str) -> bool {
        if std::env::var_os(CHILD).is_some() {
            return true;
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test])
            .env(CHILD, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success() && stdout.contains("1 passed"), "{stdout}{stderr}");
        false
    }

    /// `Cpus_allowed_list` of each `io_uring` worker serving the calling
    /// thread or, when `emulated`, of every emulation worker in the process:
    /// their names do not say whom they serve, so only a child process
    /// running one test can count them.
    fn io_worker_cores(emulated: bool) -> Vec<String> {
        // SAFETY: no preconditions.
        let kernel = format!("iou-wrk-{}", unsafe { libc::gettid() });
        let mut cores = Vec::new();
        for task in fs::read_dir("/proc/self/task").unwrap().flatten() {
            let comm = fs::read_to_string(task.path().join("comm")).unwrap_or_default();
            let comm = comm.trim();
            if !(if emulated { comm.starts_with(WORKER_NAME_PREFIX) } else { comm == kernel }) {
                continue;
            }
            let Ok(status) = fs::read_to_string(task.path().join("status")) else { continue };
            let allowed = status.lines().find_map(|line| line.strip_prefix("Cpus_allowed_list:"));
            cores.push(allowed.unwrap().trim().to_owned());
        }
        cores
    }

    /// The instance is built on one thread and driven by another, as a tile
    /// does. The emulated variant runs in a child process.
    #[test]
    fn pinned_io_workers_run_on_the_given_core() {
        let emulated = in_child("tests::pinned_io_workers_run_on_the_given_core");
        if !emulated && !uses_ring() {
            return;
        }
        // SAFETY: `allowed` outlives the call and its size is passed.
        let core = unsafe {
            let mut allowed: libc::cpu_set_t = std::mem::zeroed();
            libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &raw mut allowed);
            (0..libc::CPU_SETSIZE as usize).rev().find(|&core| libc::CPU_ISSET(core, &allowed))
        }
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut disk = BACKENDS[usize::from(emulated)](DiskConfig::default());
        let file = disk.open(dir.path().join("pinned.bin"), overwrite()).unwrap();

        let workers = std::thread::spawn(move || {
            let empty = pin_io_workers(&[]).unwrap_err();
            assert_eq!(empty.kind(), std::io::ErrorKind::InvalidInput);
            pin_io_workers(&[core]).unwrap();
            // A create and a sync always run on a worker.
            let synced = disk.sync_all(file).unwrap();
            let mut events = Vec::new();
            drive(&mut disk, &mut events, |events| events.contains(&Ev::Synced(file, synced)));
            io_worker_cores(emulated)
        })
        .join()
        .unwrap();

        assert!(!workers.is_empty(), "the ops ran on a worker");
        assert!(workers.iter().all(|cores| *cores == core.to_string()), "{workers:?} vs {core}");
    }

    /// Each instance is built and limited on one thread and driven by
    /// another, as a tile does. Opening a FIFO for writing blocks until it has
    /// a reader on every filesystem, and with `O_CREAT` always runs on an
    /// `io_uring` worker, so each open holds a worker until released. The
    /// emulated variant runs in a child process.
    #[test]
    fn io_worker_limit_set_before_attaching_caps_the_submitting_thread() {
        /// Opens every FIFO at once on a new thread, gives the workers a
        /// second to exceed two, releases the opens, runs `after` there and
        /// returns how many workers served the thread.
        fn busy_workers(
            mut disk: DiskIo,
            emulated: bool,
            fifos: &[PathBuf],
            after: impl FnOnce(&mut DiskIo) + Send + 'static,
        ) -> usize {
            let fifos = fifos.to_vec();
            std::thread::spawn(move || {
                let open = OpenOptions::new().write(true).create(true);
                let opened: Vec<_> =
                    fifos.iter().map(|fifo| Ev::Opened(disk.open(fifo, open).unwrap())).collect();
                let mut events = Vec::new();
                let deadline = StdInstant::now() + StdDuration::from_secs(1);
                while io_worker_cores(emulated).len() <= 2 && StdInstant::now() < deadline {
                    disk.poll_with(|event| events.push(Ev::from(event)));
                    std::thread::sleep(StdDuration::from_millis(1));
                }
                let readers: Vec<_> = fifos
                    .iter()
                    .map(|fifo| {
                        let mut read = fs::OpenOptions::new();
                        read.read(true).custom_flags(libc::O_NONBLOCK).open(fifo).unwrap()
                    })
                    .collect();
                drive(&mut disk, &mut events, |events| {
                    opened.iter().all(|open| events.contains(open))
                });
                drop(readers);
                let workers = io_worker_cores(emulated).len();
                after(&mut disk);
                workers
            })
            .join()
            .unwrap()
        }
        let emulated =
            in_child("tests::io_worker_limit_set_before_attaching_caps_the_submitting_thread");
        if !emulated && !uses_ring() {
            return;
        }
        let new = BACKENDS[usize::from(emulated)];
        let dir = tempfile::tempdir().unwrap();
        let fifos: Vec<_> = (0..16).map(|name| fifo(dir.path().join(name.to_string()))).collect();

        let mut limited = new(DiskConfig::default());
        limited.limit_io_workers(2, 0).unwrap();
        let workers = busy_workers(limited, emulated, &fifos, move |disk| {
            let previous = disk.limit_io_workers(3, 0).unwrap();
            assert_eq!(previous[0], 2);
            for _ in 0..2 {
                assert_eq!(disk.limit_io_workers(0, 0).unwrap(), [3, previous[1]]);
            }
            if emulated {
                disk.limit_io_workers(1_000, 0).unwrap();
                assert_eq!(disk.limit_io_workers(0, 0).unwrap()[0], 0x80);
            }
        });
        assert!((1..=2).contains(&workers), "{workers} workers despite a limit of 2");

        // The limited pool's workers exit once its thread and instance are gone.
        let deadline = StdInstant::now() + StdDuration::from_secs(5);
        while emulated && !io_worker_cores(true).is_empty() {
            assert!(StdInstant::now() < deadline, "the limited pool's workers stayed");
            std::thread::sleep(StdDuration::from_millis(1));
        }
        let after_dir = tempfile::tempdir().unwrap();
        let after = after_dir.path().join("after.bin");
        let workers = busy_workers(new(DiskConfig::default()), emulated, &fifos, move |disk| {
            // Lowering the limit of a grown pool only stops growth; a worker
            // that exited instead would swallow this open's wakeup.
            if emulated {
                disk.limit_io_workers(1, 0).unwrap();
                let opened = Ev::Opened(disk.open(&after, overwrite()).unwrap());
                drive(disk, &mut Vec::new(), |events| events.contains(&opened));
            }
        });
        assert!(workers > 2, "only {workers} workers without a limit");
    }
}
