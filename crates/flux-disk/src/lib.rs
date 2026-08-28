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
//! [`DiskIo::sync_data`] and [`DiskIo::close`], which act as barriers: they
//! start only after every earlier operation on that file has completed, so a
//! sync covers all earlier writes and a close cannot race them. Operations
//! submitted before the file finished opening are queued and dispatched once
//! the descriptor is available.

use std::{
    collections::VecDeque,
    ffi::CString,
    io,
    mem::MaybeUninit,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
};

use flux_timing::{Duration, Instant};
use io_uring::{IoUring, opcode, squeue, types};
use tracing::warn;

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
const EMPTY_PATH: &[u8] = b"\0";

/// Identifies one open (or opening) file. Tokens are never reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FileToken(usize);

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
        let creation = match (self.create, self.truncate, self.create_new) {
            (false, false, false) => 0,
            (true, false, false) => libc::O_CREAT,
            (false, true, false) => libc::O_TRUNC,
            (true, true, false) => libc::O_CREAT | libc::O_TRUNC,
            (_, _, true) => libc::O_CREAT | libc::O_EXCL,
        };
        Ok(access | creation | libc::O_CLOEXEC)
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
    Read { offset: u64, len: usize },
    Write { offset: u64, len: usize },
    Sync,
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
    /// A [`DiskIo::sync_all`] or [`DiskIo::sync_data`] barrier finished;
    /// every
    /// write submitted before it is durable.
    Synced { file: FileToken },
    /// The file was closed and its token retired.
    Closed { file: FileToken },
    /// An operation failed. `Failed { op: FailedOp::Open, .. }` retires the
    /// token and fails every queued operation on it; other failures leave the
    /// file usable.
    Failed { file: FileToken, op: FailedOp, error: io::Error },
}

enum PendingOp {
    Open { path: CString, flags: i32, mode: libc::mode_t },
    Read { offset: u64, len: usize, to_end: bool },
    ReadToEnd { offset: u64 },
    Write { buf: Vec<u8>, offset: u64 },
    Sync { data_only: bool },
    Close,
}

enum InFlightOp {
    Open { path: CString, flags: i32, mode: libc::mode_t },
    Read { buf: Vec<u8>, offset: u64, wanted: usize, have: usize, to_end: bool },
    Statx { offset: u64, statx: Box<MaybeUninit<libc::statx>> },
    Write { buf: Vec<u8>, offset: u64, written: usize },
    Sync { data_only: bool },
    Close,
}

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
    ring: IoUring,
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
    need_submit: bool,
    last_pending_warning: Option<Instant>,
}

impl Default for DiskIo {
    fn default() -> Self {
        Self::new(DiskConfig::default()).expect("couldn't set up an io_uring for disk io")
    }
}

impl DiskIo {
    pub fn new(config: DiskConfig) -> io::Result<Self> {
        assert!(config.ring_entries > 0, "ring_entries must be nonzero");
        assert!(config.max_in_flight > 0, "max_in_flight must be nonzero");
        let ring = IoUring::new(config.ring_entries)?;
        let max_in_flight = config.max_in_flight.min(ring.params().cq_entries() as usize);
        Ok(Self {
            ring,
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
            need_submit: false,
            last_pending_warning: None,
        })
    }

    /// Starts opening a file. The token is usable immediately: operations
    /// queue behind the open and dispatch once [`DiskEvent::Opened`] fires.
    /// If the open fails, every queued operation fails with `EBADF`.
    pub fn open<P: AsRef<Path>>(&mut self, path: P, options: OpenOptions) -> io::Result<FileToken> {
        let path = path.as_ref();
        let cpath = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a nul byte"))?;
        let token = FileToken(self.next_token);
        self.next_token = self.next_token.checked_add(1).expect("file token space exhausted");
        let mut queue = VecDeque::new();
        queue.push_back(PendingOp::Open {
            path: cpath,
            flags: options.flags()?,
            mode: options.mode as libc::mode_t,
        });
        self.files.push(File {
            token,
            path: path.to_path_buf(),
            fd: None,
            closing: false,
            write_cursor: 0,
            in_flight: 0,
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
        state.queue.push_back(PendingOp::Read { offset, len, to_end: false });
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
        state.queue.push_back(PendingOp::ReadToEnd { offset });
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
        state.queue.push_back(PendingOp::Write { buf, offset });
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
        state.queue.push_back(PendingOp::Write { buf, offset });
        self.note_enqueued();
        true
    }

    /// Moves the append cursor used by [`DiskIo::write_with`], e.g. to the
    /// current length of an existing file. Returns `false` for unknown or
    /// closing tokens.
    pub fn set_write_cursor(&mut self, file: FileToken, offset: u64) -> bool {
        let Some(state) = self.usable_file_mut(file) else { return false };
        state.write_cursor = offset;
        true
    }

    /// Queues a full-integrity `fsync` barrier, the durability of
    /// [`std::fs::File::sync_all`]: it starts only after every earlier
    /// operation on this file completed, and later operations start only
    /// after the barrier was handed to the kernel. Returns `false` for
    /// unknown or closing tokens.
    pub fn sync_all(&mut self, file: FileToken) -> bool {
        self.push_sync(file, false)
    }

    /// Queues an `fdatasync` barrier, the durability of
    /// [`std::fs::File::sync_data`]; like [`DiskIo::sync_all`] without
    /// flushing file metadata timestamps.
    pub fn sync_data(&mut self, file: FileToken) -> bool {
        self.push_sync(file, true)
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

    /// Whether every submitted operation has completed and been reported.
    pub fn is_idle(&self) -> bool {
        self.pending_count == 0 && self.in_flight_count == 0
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

    fn push_sync(&mut self, file: FileToken, data_only: bool) -> bool {
        let Some(state) = self.usable_file_mut(file) else { return false };
        state.queue.push_back(PendingOp::Sync { data_only });
        self.note_enqueued();
        true
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
                if self.in_flight_count >= self.max_in_flight || self.ring.submission().is_full() {
                    return;
                }
                let file = &self.files[file_index];
                let Some(head) = file.queue.front() else { break };
                let ready = match head {
                    PendingOp::Open { .. } => true,
                    PendingOp::Read { .. } |
                    PendingOp::ReadToEnd { .. } |
                    PendingOp::Write { .. } => file.fd.is_some(),
                    PendingOp::Sync { .. } | PendingOp::Close => {
                        file.fd.is_some() && file.in_flight == 0
                    }
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
        let op = match op {
            PendingOp::Open { path, flags, mode } => InFlightOp::Open { path, flags, mode },
            PendingOp::Read { offset, len, to_end } => InFlightOp::Read {
                buf: self.take_buffer_with_capacity(len),
                offset,
                wanted: len,
                have: 0,
                to_end,
            },
            PendingOp::ReadToEnd { offset } => {
                InFlightOp::Statx { offset, statx: Box::new(MaybeUninit::uninit()) }
            }
            PendingOp::Write { buf, offset } => InFlightOp::Write { buf, offset, written: 0 },
            PendingOp::Sync { data_only } => InFlightOp::Sync { data_only },
            PendingOp::Close => InFlightOp::Close,
        };
        let slot = self.alloc_slot(InFlight { file: token, op });
        self.files[file_index].in_flight += 1;
        self.in_flight_count += 1;
        self.push_slot(slot as u64);
    }

    fn build_sqe(&mut self, user_data: u64) -> squeue::Entry {
        let slot = user_data as usize;
        let token = self.slab[slot].as_ref().expect("in-flight slot occupied").file;
        let fd = self.files.iter().find(|file| file.token == token).and_then(|file| file.fd);
        let entry = self.slab[slot].as_mut().expect("in-flight slot occupied");
        let sqe = match &mut entry.op {
            InFlightOp::Open { path, flags, mode } => {
                opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr())
                    .flags(*flags)
                    .mode(*mode)
                    .build()
            }
            InFlightOp::Read { buf, offset, wanted, have, .. } => {
                let fd = fd.expect("read dispatched without an open fd");
                let chunk = (*wanted - *have).min(MAX_OP_BYTES) as u32;
                // SAFETY: `have < wanted <= buf.capacity()`, so the pointer
                // stays inside the buffer's allocation.
                let ptr = unsafe { buf.as_mut_ptr().add(*have) };
                opcode::Read::new(types::Fd(fd), ptr, chunk).offset(*offset + *have as u64).build()
            }
            InFlightOp::Statx { statx, .. } => {
                let fd = fd.expect("statx dispatched without an open fd");
                opcode::Statx::new(
                    types::Fd(fd),
                    EMPTY_PATH.as_ptr().cast(),
                    statx.as_mut_ptr().cast(),
                )
                .flags(libc::AT_STATX_SYNC_AS_STAT | libc::AT_EMPTY_PATH)
                .mask(libc::STATX_SIZE)
                .build()
            }
            InFlightOp::Write { buf, offset, written } => {
                let fd = fd.expect("write dispatched without an open fd");
                let chunk = (buf.len() - *written).min(MAX_OP_BYTES) as u32;
                // SAFETY: `written < buf.len()`, so the pointer stays inside
                // the buffer's allocation.
                let ptr = unsafe { buf.as_ptr().add(*written) };
                opcode::Write::new(types::Fd(fd), ptr, chunk)
                    .offset(*offset + *written as u64)
                    .build()
            }
            InFlightOp::Sync { data_only } => {
                let fd = fd.expect("sync dispatched without an open fd");
                let mut sync = opcode::Fsync::new(types::Fd(fd));
                if *data_only {
                    sync = sync.flags(types::FsyncFlags::DATASYNC);
                }
                sync.build()
            }
            InFlightOp::Close => {
                let fd = fd.expect("close dispatched without an open fd");
                opcode::Close::new(types::Fd(fd)).build()
            }
        };
        sqe.user_data(user_data)
    }

    fn push_slot(&mut self, user_data: u64) {
        let sqe = self.build_sqe(user_data);
        // SAFETY: every buffer and path the entry references lives in
        // `self.slab` until the completion for `user_data` is reaped, and its
        // heap storage is never touched while the kernel owns the operation.
        if unsafe { self.ring.submission().push(&sqe) }.is_ok() {
            self.need_submit = true;
        } else {
            self.stalled.push(user_data);
        }
    }

    fn flush_submissions(&mut self) {
        if !self.need_submit {
            return;
        }
        match self.ring.submit() {
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
        completions.extend(self.ring.completion().map(|cqe| (cqe.user_data(), cqe.result())));
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
            InFlightOp::Statx { offset, statx } => {
                self.complete_statx(user_data, file_index, offset, statx, result);
            }
            InFlightOp::Sync { .. } => {
                self.finish_op(slot, file_index);
                if result < 0 {
                    let error = io::Error::from_raw_os_error(-result);
                    handler(DiskEvent::Failed { file: token, op: FailedOp::Sync, error });
                } else {
                    handler(DiskEvent::Synced { file: token });
                }
            }
            InFlightOp::Close => {
                self.finish_op(slot, file_index);
                // The descriptor is released even when `close` is interrupted.
                if result >= 0 || -result == libc::EINTR {
                    handler(DiskEvent::Closed { file: token });
                } else {
                    let error = io::Error::from_raw_os_error(-result);
                    handler(DiskEvent::Failed { file: token, op: FailedOp::Close, error });
                }
                self.remove_file(file_index);
            }
        }
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
            handler(DiskEvent::Failed { file: token, op: FailedOp::Open, error });
            self.fail_queued(file_index, handler);
            self.remove_file(file_index);
        }
    }

    fn complete_statx(
        &mut self,
        user_data: u64,
        file_index: usize,
        offset: u64,
        statx: Box<MaybeUninit<libc::statx>>,
        result: i32,
    ) {
        let len = if result == 0 {
            // SAFETY: a successful statx completion writes the output struct.
            let statx = unsafe { statx.assume_init() };
            (statx.stx_mask & libc::STATX_SIZE != 0).then_some(statx.stx_size)
        } else {
            None
        };
        self.start_read_to_end(user_data, file_index, offset, len);
    }

    fn start_read_to_end(
        &mut self,
        user_data: u64,
        file_index: usize,
        offset: u64,
        file_len: Option<u64>,
    ) {
        let wanted = Self::read_to_end_len(file_len, offset);
        let token = self.files[file_index].token;
        let slot = user_data as usize;
        self.slab[slot] = Some(InFlight {
            file: token,
            op: InFlightOp::Read {
                buf: self.take_buffer_with_capacity(wanted),
                offset,
                wanted,
                have: 0,
                to_end: true,
            },
        });
        self.push_slot(user_data);
    }

    fn read_to_end_len(file_len: Option<u64>, offset: u64) -> usize {
        file_len
            .map(|len| len.saturating_sub(offset).saturating_add(1))
            .and_then(|len| usize::try_from(len).ok())
            .unwrap_or(READ_TO_END_CHUNK)
            .max(READ_TO_END_CHUNK)
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
            InFlightOp::Read { mut buf, offset, mut wanted, have, to_end } => {
                if result < 0 {
                    self.finish_op(slot, file_index);
                    let error = io::Error::from_raw_os_error(-result);
                    let op = FailedOp::Read { offset, len: wanted };
                    handler(DiskEvent::Failed { file: token, op, error });
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
                        op: InFlightOp::Read { buf, offset, wanted, have, to_end },
                    });
                    self.push_slot(user_data);
                }
            }
            InFlightOp::Write { buf, offset, written } => {
                if result <= 0 {
                    self.finish_op(slot, file_index);
                    let error = if result == 0 {
                        io::Error::from(io::ErrorKind::WriteZero)
                    } else {
                        io::Error::from_raw_os_error(-result)
                    };
                    let op = FailedOp::Write { offset, len: buf.len() };
                    handler(DiskEvent::Failed { file: token, op, error });
                    self.recycle(buf);
                    return;
                }
                let written = written + result as usize;
                if written >= buf.len() {
                    self.finish_op(slot, file_index);
                    handler(DiskEvent::Written { file: token, offset, len: buf.len() });
                    self.recycle(buf);
                } else {
                    self.slab[slot] = Some(InFlight {
                        file: token,
                        op: InFlightOp::Write { buf, offset, written },
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
        self.in_flight_count -= 1;
    }

    fn fail_queued<F>(&mut self, file_index: usize, handler: &mut F)
    where
        F: for<'a> FnMut(DiskEvent<'a>),
    {
        let token = self.files[file_index].token;
        while let Some(op) = self.files[file_index].queue.pop_front() {
            self.pending_count -= 1;
            let failed = match op {
                PendingOp::Open { .. } => FailedOp::Open,
                PendingOp::Read { offset, len, .. } => FailedOp::Read { offset, len },
                PendingOp::ReadToEnd { offset } => {
                    FailedOp::Read { offset, len: READ_TO_END_CHUNK }
                }
                PendingOp::Write { buf, offset } => {
                    let len = buf.len();
                    self.recycle(buf);
                    FailedOp::Write { offset, len }
                }
                PendingOp::Sync { .. } => FailedOp::Sync,
                PendingOp::Close => FailedOp::Close,
            };
            let error = io::Error::from_raw_os_error(libc::EBADF);
            handler(DiskEvent::Failed { file: token, op: failed, error });
        }
    }

    fn remove_file(&mut self, file_index: usize) {
        let file = &self.files[file_index];
        flux_utils::safe_assert!(file.in_flight == 0);
        flux_utils::safe_assert!(file.queue.is_empty());
        self.files.swap_remove(file_index);
    }
}

impl Drop for DiskIo {
    fn drop(&mut self) {
        // Stalled continuations were never handed to the kernel; free them
        // directly.
        for user_data in std::mem::take(&mut self.stalled) {
            if self.slab[user_data as usize].take().is_some() {
                self.in_flight_count -= 1;
            }
        }
        // The kernel writes into slab-owned buffers, so wait for every
        // operation it has seen. Regular-file operations always complete;
        // this is bounded by device latency.
        while self.in_flight_count > 0 {
            let mut completions = std::mem::take(&mut self.completions);
            completions.clear();
            completions.extend(self.ring.completion().map(|cqe| (cqe.user_data(), cqe.result())));
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
            match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(err) if err.raw_os_error() == Some(libc::EINTR) => {}
                Err(err) => {
                    warn!(?err, "couldn't drain the disk io ring; leaking in-flight buffers");
                    std::mem::forget(std::mem::take(&mut self.slab));
                    break;
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
        fs,
        os::fd::IntoRawFd,
        time::{Duration as StdDuration, Instant as StdInstant},
    };

    use super::{
        DiskConfig, DiskEvent, DiskIo, FailedOp, FileToken, OpenOptions, READ_TO_END_CHUNK,
    };

    fn overwrite() -> OpenOptions {
        OpenOptions::new().write(true).create(true).truncate(true)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Ev {
        Opened(FileToken),
        Read { file: FileToken, offset: u64, payload: Vec<u8>, eof: bool },
        Written { file: FileToken, offset: u64, len: usize },
        Synced(FileToken),
        Closed(FileToken),
        Failed { file: FileToken, op: FailedOp },
    }

    impl From<DiskEvent<'_>> for Ev {
        fn from(event: DiskEvent<'_>) -> Self {
            match event {
                DiskEvent::Opened { file } => Self::Opened(file),
                DiskEvent::Read { file, offset, payload, eof } => {
                    Self::Read { file, offset, payload: payload.to_vec(), eof }
                }
                DiskEvent::Written { file, offset, len } => Self::Written { file, offset, len },
                DiskEvent::Synced { file } => Self::Synced(file),
                DiskEvent::Closed { file } => Self::Closed(file),
                DiskEvent::Failed { file, op, .. } => Self::Failed { file, op },
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

    #[test]
    fn writes_sync_and_close_before_the_first_poll() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.bin");
        let mut disk = DiskIo::default();
        let file = disk.open(&path, overwrite()).unwrap();
        assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"alpha")));
        assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"beta")));
        assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"gamma")));
        assert!(disk.sync_all(file));
        assert!(disk.close(file));

        let mut events = Vec::new();
        drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));

        assert_eq!(events[0], Ev::Opened(file));
        let synced_at = events.iter().position(|event| *event == Ev::Synced(file)).unwrap();
        for (offset, len) in [(0, 5), (5, 4), (9, 5)] {
            let written = Ev::Written { file, offset, len };
            let written_at = events.iter().position(|event| *event == written).unwrap();
            assert!(written_at < synced_at, "sync must complete after writes: {events:?}");
        }
        assert_eq!(*events.last().unwrap(), Ev::Closed(file));
        assert!(disk.is_idle());
        assert_eq!(fs::read(&path).unwrap(), b"alphabetagamma");
    }

    #[test]
    fn reads_report_payload_and_eof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.bin");
        fs::write(&path, b"0123456789").unwrap();
        let mut disk = DiskIo::default();
        let file = disk.open(&path, OpenOptions::new().read(true)).unwrap();
        assert!(!disk.read_at(file, 0, 0));
        assert!(disk.read_at(file, 0, 10));
        assert!(disk.read_at(file, 4, 100));
        assert!(disk.read_at(file, 10, 3));

        let mut events = Vec::new();
        drive(&mut disk, &mut events, |events| {
            events.iter().filter(|event| matches!(event, Ev::Read { .. })).count() == 3
        });

        let read =
            |offset, payload: &[u8], eof| Ev::Read { file, offset, payload: payload.to_vec(), eof };
        assert!(events.contains(&read(0, b"0123456789", false)));
        assert!(events.contains(&read(4, b"456789", true)));
        assert!(events.contains(&read(10, b"", true)));
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
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grown.bin");
        let data: Vec<u8> = (0..200_000u32).map(|i| i as u8).collect();
        fs::write(&path, &data).unwrap();
        let mut disk = DiskIo::default();
        let file = disk.open(&path, OpenOptions::new().read(true)).unwrap();
        assert!(disk.read_to_end(file, 0));
        assert!(disk.read_to_end(file, 199_990));
        assert!(disk.close(file));

        let mut events = Vec::new();
        drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));
        assert!(events.contains(&Ev::Read { file, offset: 0, payload: data.clone(), eof: true }));
        assert!(events.contains(&Ev::Read {
            file,
            offset: 199_990,
            payload: data[199_990..].to_vec(),
            eof: true,
        }));
    }

    #[test]
    fn read_to_end_grows_after_a_stale_statx_hint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grown.bin");
        let data: Vec<u8> = (0..READ_TO_END_CHUNK * 3).map(|i| i as u8).collect();
        fs::write(&path, &data).unwrap();
        let mut disk = DiskIo::default();
        let file = FileToken(0);
        disk.files.push(super::File {
            token: file,
            path: path.clone(),
            fd: Some(fs::File::open(&path).unwrap().into_raw_fd()),
            closing: false,
            write_cursor: 0,
            in_flight: 1,
            queue: std::collections::VecDeque::new(),
        });
        disk.slab.push(None);
        disk.in_flight_count = 1;

        // Start from a deliberately stale size captured before the file grew.
        disk.start_read_to_end(0, 0, 0, Some(READ_TO_END_CHUNK as u64));

        let mut events = Vec::new();
        drive(&mut disk, &mut events, |events| {
            events.iter().any(|event| matches!(event, Ev::Read { .. }))
        });
        assert!(events.contains(&Ev::Read { file, offset: 0, payload: data, eof: true }));
        assert!(disk.is_idle());
    }

    #[test]
    fn read_to_end_of_an_empty_file_is_empty_and_eof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bin");
        fs::write(&path, b"").unwrap();
        let mut disk = DiskIo::default();
        let file = disk.open(&path, OpenOptions::new().read(true)).unwrap();
        assert!(disk.read_to_end(file, 0));
        let mut events = Vec::new();
        drive(&mut disk, &mut events, |events| {
            events.iter().any(|event| matches!(event, Ev::Read { .. }))
        });
        assert!(events.contains(&Ev::Read { file, offset: 0, payload: Vec::new(), eof: true }));
    }

    #[test]
    fn failed_open_fails_queued_ops_and_retires_the_token() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing").join("file.bin");
        let mut disk = DiskIo::default();
        let file = disk.open(&path, overwrite()).unwrap();
        assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"data")));
        assert!(disk.close(file));

        let mut events = Vec::new();
        drive(&mut disk, &mut events, |events| events.len() >= 3);

        assert_eq!(events, vec![
            Ev::Failed { file, op: FailedOp::Open },
            Ev::Failed { file, op: FailedOp::Write { offset: 0, len: 4 } },
            Ev::Failed { file, op: FailedOp::Close },
        ]);
        assert!(!disk.write_with(file, |buf| buf.push(1)));
        assert!(disk.is_idle());
    }

    #[test]
    fn barriers_order_overlapping_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("patched.bin");
        let mut disk = DiskIo::default();
        let file = disk.open(&path, overwrite()).unwrap();
        assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"aaaa")));
        assert!(disk.sync_all(file));
        assert!(disk.write_at_with(file, 1, |buf| buf.extend_from_slice(b"bb")));
        assert!(disk.sync_data(file));
        assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"cccc")));
        assert!(disk.close(file));

        let mut events = Vec::new();
        drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));
        assert_eq!(fs::read(&path).unwrap(), b"abbacccc");
    }

    #[test]
    fn cursor_can_append_to_an_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.bin");
        fs::write(&path, b"0123").unwrap();
        let mut disk = DiskIo::default();
        let file = disk.open(&path, OpenOptions::new().read(true).write(true)).unwrap();
        assert!(disk.set_write_cursor(file, 4));
        assert!(disk.write_with(file, |buf| buf.extend_from_slice(b"45")));
        assert!(disk.close(file));

        let mut events = Vec::new();
        drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));
        assert_eq!(fs::read(&path).unwrap(), b"012345");
    }

    #[test]
    fn tiny_ring_applies_backpressure_without_losing_ops() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("burst.bin");
        let config = DiskConfig { ring_entries: 2, max_in_flight: 2, ..DiskConfig::default() };
        let mut disk = DiskIo::new(config).unwrap();
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

    #[test]
    fn closing_unknown_and_empty_submissions_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut disk = DiskIo::default();
        let file = disk.open(dir.path().join("a.bin"), overwrite()).unwrap();
        assert!(!disk.write_with(file, |_| {}));
        assert!(disk.close(file));
        assert!(!disk.write_with(file, |buf| buf.push(1)));
        assert!(!disk.write_at_with(file, 0, |buf| buf.push(1)));
        assert!(!disk.read_at(file, 0, 1));
        assert!(!disk.read_to_end(file, 0));
        assert!(!disk.sync_all(file));
        assert!(!disk.set_write_cursor(file, 0));
        assert!(!disk.close(file));

        let mut events = Vec::new();
        drive(&mut disk, &mut events, |events| events.contains(&Ev::Closed(file)));
        assert!(!disk.write_with(file, |buf| buf.push(1)), "retired tokens are rejected");
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

    #[test]
    fn drop_waits_for_submitted_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dropped.bin");
        let payload = vec![7u8; 1 << 20];
        let mut disk = DiskIo::default();
        let file = disk.open(&path, overwrite()).unwrap();
        let mut events = Vec::new();
        drive(&mut disk, &mut events, |events| events.contains(&Ev::Opened(file)));
        assert!(disk.write_with(file, |buf| buf.extend_from_slice(&payload)));
        disk.poll_with(|_| {});
        drop(disk);
        assert_eq!(fs::read(&path).unwrap(), payload);
    }
}
