use std::{
    collections::VecDeque,
    io, iter,
    mem::{self, MaybeUninit},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::thread::JoinHandleExt,
    },
    ptr,
    sync::{
        Arc, Condvar, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
};

use tracing::warn;

use crate::backend::{
    LINK_FLAGS, MKDIR_MODE, Op, PATH_DIR_FD, STATX_FLAGS, STATX_MASK, STATX_PATH,
};

/// Workers a pool grows to while no limit applies.
const DEFAULT_WORKERS: usize = 4;
/// Largest worker limit; higher limits are clamped to it.
const MAX_WORKERS: u32 = 0x80;
/// Followed by the worker's index in its pool as two hex digits, below
/// [`MAX_WORKERS`]: 15 bytes, the longest thread name Linux keeps.
pub(crate) const WORKER_NAME_PREFIX: &str = "flux-disk-io-";

thread_local! {
    static POOL: Pool = Pool::new();
}

/// Runs operations on the worker pool of the thread that submits them, like
/// `io_uring`'s io-wq, and reports completions through an eventfd.
pub(crate) struct Emulation {
    completions: Arc<Completions>,
    /// Completions taken from `completions`; equal to its `pushed` once every
    /// signalled completion is reaped.
    consumed: u64,
    batch: Vec<Job>,
    batch_limit: usize,
    /// Reads the page cache served during `push`. Published at the next
    /// `submit` like a worker's completions, so a caller that waits on the
    /// eventfd after submitting wakes, as it would for a ring's inline
    /// completions.
    inline: Vec<(u64, i32)>,
    pool: Option<Pool>,
    /// `[bounded, unbounded]` as last set through `limit_io_workers`, at most
    /// [`MAX_WORKERS`]; `0` while unset. Only the bounded limit sizes a pool:
    /// every operation here is bounded.
    limits: [u32; 2],
}

impl Emulation {
    pub(crate) fn new(entries: u32) -> io::Result<Self> {
        // SAFETY: no preconditions.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` is a fresh descriptor nothing else owns.
        let eventfd = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Self {
            completions: Arc::new(Completions {
                queue: Mutex::new(VecDeque::new()),
                pushed: AtomicU64::new(0),
                eventfd,
            }),
            consumed: 0,
            batch: Vec::new(),
            batch_limit: entries as usize,
            inline: Vec::new(),
            pool: None,
            limits: [0; 2],
        })
    }

    /// Workers that the calling thread's pool spawns from now on run on
    /// `cores`.
    pub(crate) fn pin_io_workers(cores: libc::cpu_set_t) {
        let _ = POOL.try_with(|pool| lock(&pool.shared().state).affinity = Some(cores));
    }

    #[inline]
    pub(crate) fn submission_full(&self) -> bool {
        self.batch.len() + self.inline.len() >= self.batch_limit
    }

    /// # Safety
    ///
    /// As for [`crate::backend::Backend::push`].
    // Out of line, like `submit` and `reap`, so the `io_uring` path through
    // `Backend` stays small.
    #[inline(never)]
    pub(crate) unsafe fn push(&mut self, op: Op, user_data: u64) -> bool {
        if self.submission_full() {
            return false;
        }
        // SAFETY: the caller upholds `Backend::push`'s contract.
        if let Some(result) = unsafe { try_nowait(op) } {
            self.inline.push((user_data, result));
            return true;
        }
        self.batch.push(Job { op, user_data, completions: Arc::clone(&self.completions) });
        true
    }

    /// Publishes inline completions and hands the batch to the calling
    /// thread's pool.
    #[inline(never)]
    pub(crate) fn submit(&mut self) -> usize {
        if !self.inline.is_empty() {
            self.completions.publish(self.inline.drain(..));
        }
        let submitted = self.batch.len();
        if submitted > 0 {
            attach(&mut self.pool, self.limits[0]).submit(&mut self.batch);
        }
        submitted
    }

    pub(crate) fn take_unsubmitted(&mut self, unsubmitted: &mut Vec<u64>) {
        unsubmitted.extend(self.batch.drain(..).map(|job| job.user_data));
        unsubmitted.extend(self.inline.drain(..).map(|(user_data, _)| user_data));
    }

    pub(crate) fn wait(&self) -> io::Result<()> {
        let mut ready = libc::pollfd { fd: self.as_raw_fd(), events: libc::POLLIN, revents: 0 };
        // SAFETY: `ready` outlives the call and the count is one.
        if unsafe { libc::poll(&raw mut ready, 1, -1) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Takes exactly as many completions as the eventfd counted, so it is
    /// readable while a signalled completion waits and not once all are
    /// taken.
    #[inline(never)]
    pub(crate) fn reap(&mut self, completions: &mut Vec<(u64, i32)>) {
        // A stale count only defers the drain to a later call: the eventfd
        // stays readable until then.
        if self.completions.pushed.load(Ordering::Relaxed) == self.consumed {
            return;
        }
        let mut signalled = 0u64;
        // SAFETY: an eventfd read fills exactly the 8-byte counter.
        let read =
            unsafe { libc::read(self.as_raw_fd(), (&raw mut signalled).cast(), size_of::<u64>()) };
        if read != size_of::<u64>() as isize {
            return;
        }
        // Each signal follows the push of its completion, so the queue holds
        // at least `signalled` entries.
        completions.extend(lock(&self.completions.queue).drain(..signalled as usize));
        self.consumed += signalled;
    }

    /// Returns the previous values; `0` leaves a value unchanged.
    pub(crate) fn limit_io_workers(&mut self, limits: &mut [u32; 2]) {
        let previous = self.limits;
        for (limit, new) in self.limits.iter_mut().zip(*limits) {
            if new != 0 {
                *limit = new.min(MAX_WORKERS);
            }
        }
        if let Some(pool) = &self.pool &&
            self.limits[0] != 0
        {
            lock(&pool.shared().state).max_workers = self.limits[0] as usize;
        }
        *limits = previous;
    }

    pub(crate) fn as_raw_fd(&self) -> RawFd {
        self.completions.eventfd.as_raw_fd()
    }
}

/// Shared by an [`Emulation`] and its jobs, so the eventfd outlives every
/// worker that may still write to it.
struct Completions {
    queue: Mutex<VecDeque<(u64, i32)>>,
    pushed: AtomicU64,
    eventfd: OwnedFd,
}

impl Completions {
    /// Push, count, then signal: a completion is always queued before the
    /// eventfd counts it.
    fn publish(&self, results: impl ExactSizeIterator<Item = (u64, i32)>) {
        let count = results.len() as u64;
        lock(&self.queue).extend(results);
        self.pushed.fetch_add(count, Ordering::Relaxed);
        // SAFETY: an eventfd write takes exactly the 8-byte addend.
        unsafe {
            libc::write(self.eventfd.as_raw_fd(), (&raw const count).cast(), size_of::<u64>())
        };
    }
}

struct Job {
    op: Op,
    user_data: u64,
    completions: Arc<Completions>,
}

// SAFETY: `Backend::push`'s contract keeps everything `op` points to valid,
// and untouched by the submitting thread, until the completion is reaped.
unsafe impl Send for Job {}

impl Job {
    fn run(self) {
        // SAFETY: as for `Send`; the worker is the only user of `op`'s memory.
        let result = unsafe { execute(self.op) };
        self.completions.publish(iter::once((self.user_data, result)));
    }

    fn fail(self, errno: i32) {
        self.completions.publish(iter::once((self.user_data, -errno)));
    }
}

/// The workers of one submitting thread, shared by every [`Emulation`] that
/// thread drives. Once the last handle is gone the workers finish the queued
/// jobs and exit.
#[derive(Clone)]
struct Pool(Arc<Handle>);

struct Handle(Arc<Shared>);

struct Shared {
    state: Mutex<State>,
    work: Condvar,
}

struct State {
    jobs: VecDeque<Job>,
    workers: usize,
    idle: usize,
    max_workers: usize,
    affinity: Option<libc::cpu_set_t>,
    shutdown: bool,
}

impl Pool {
    fn new() -> Self {
        let state = State {
            jobs: VecDeque::new(),
            workers: 0,
            idle: 0,
            max_workers: DEFAULT_WORKERS,
            affinity: None,
            shutdown: false,
        };
        Self(Arc::new(Handle(Arc::new(Shared { state: Mutex::new(state), work: Condvar::new() }))))
    }

    fn shared(&self) -> &Arc<Shared> {
        &(self.0).0
    }

    /// Queues the batch, wakes an idle worker per job and spawns more, up to
    /// the limit, for queued jobs no idle worker takes. A worker woken by an
    /// earlier batch still counts as idle until it runs, so spawning is sized
    /// from the whole queue.
    fn submit(&self, batch: &mut Vec<Job>) {
        let shared = self.shared();
        let mut state = lock(&shared.state);
        let queued = batch.len();
        state.jobs.extend(batch.drain(..));
        let wake = queued.min(state.idle);
        let spawn = state
            .jobs
            .len()
            .saturating_sub(state.idle)
            .min(state.max_workers.saturating_sub(state.workers));
        let first = state.workers;
        state.workers += spawn;
        let affinity = state.affinity;
        drop(state);
        for _ in 0..wake {
            shared.work.notify_one();
        }
        for index in first..first + spawn {
            if let Err(error) = spawn_worker(shared, index, affinity) {
                self.spawn_failed(first + spawn - index, &error);
                break;
            }
        }
    }

    /// Without any worker the queued jobs would never run, so they fail with
    /// `ENOMEM`: the spawn error itself is `EAGAIN`, which `DiskIo` retries.
    fn spawn_failed(&self, unspawned: usize, error: &io::Error) {
        static LOGGED: AtomicBool = AtomicBool::new(false);
        if !LOGGED.swap(true, Ordering::Relaxed) {
            warn!(%error, "couldn't spawn a disk io worker; operations without one fail with ENOMEM");
        }
        let mut state = lock(&self.shared().state);
        state.workers -= unspawned;
        if state.workers > 0 {
            return;
        }
        let jobs = mem::take(&mut state.jobs);
        drop(state);
        for job in jobs {
            job.fail(libc::ENOMEM);
        }
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        lock(&self.0.state).shutdown = true;
        self.0.work.notify_all();
    }
}

/// The calling thread's pool, after attaching `attached` to it when the
/// previous submission came from another thread. Jobs already on the old pool
/// still complete into the same completion state.
fn attach(attached: &mut Option<Pool>, max_workers: u32) -> &Pool {
    let switch = POOL.try_with(|current| match attached {
        Some(pool) if Arc::ptr_eq(&pool.0, &current.0) => None,
        _ => Some(current.clone()),
    });
    // A thread whose pool is already destroyed keeps the pool it used, else
    // gets one of its own.
    let switch = switch.unwrap_or_else(|_| attached.is_none().then(Pool::new));
    if let Some(pool) = switch {
        if max_workers != 0 {
            lock(&pool.shared().state).max_workers = max_workers as usize;
        }
        *attached = Some(pool);
    }
    attached.as_ref().expect("attached above")
}

fn spawn_worker(
    shared: &Arc<Shared>,
    index: usize,
    affinity: Option<libc::cpu_set_t>,
) -> io::Result<()> {
    let shared = Arc::clone(shared);
    let name = format!("{WORKER_NAME_PREFIX}{index:02x}");
    let worker = thread::Builder::new().name(name).spawn(move || work(&shared))?;
    // Pinned from here, not by the worker: sharing a spinning or real-time
    // spawner's core, the worker might never run to pin itself.
    if let Some(cores) = affinity {
        // SAFETY: `cores` outlives the call and its size is passed; the
        // thread is neither joined nor detached until `worker` drops.
        let error = unsafe {
            libc::pthread_setaffinity_np(
                worker.as_pthread_t(),
                size_of::<libc::cpu_set_t>(),
                &raw const cores,
            )
        };
        if error != 0 {
            let cores: Vec<_> = (0..libc::CPU_SETSIZE as usize)
                // SAFETY: every core is below the set's capacity.
                .filter(|&core| unsafe { libc::CPU_ISSET(core, &cores) })
                .collect();
            let error = io::Error::from_raw_os_error(error);
            warn!(%error, ?cores, "couldn't pin a disk io worker");
        }
    }
    Ok(())
}

fn work(shared: &Shared) {
    // Blocked signals cannot interrupt the blocking calls with `EINTR`.
    // SAFETY: `signals` is initialized by `sigfillset` before use.
    unsafe {
        let mut signals = MaybeUninit::<libc::sigset_t>::uninit();
        libc::sigfillset(signals.as_mut_ptr());
        libc::pthread_sigmask(libc::SIG_BLOCK, signals.as_ptr(), ptr::null_mut());
    }
    let mut state = lock(&shared.state);
    loop {
        if let Some(job) = state.jobs.pop_front() {
            drop(state);
            job.run();
            state = lock(&shared.state);
        } else if state.shutdown {
            return;
        } else {
            state.idle += 1;
            state = shared.work.wait(state).unwrap_or_else(PoisonError::into_inner);
            state.idle -= 1;
        }
    }
}

/// Workers never panic, so a poisoned lock still guards consistent state.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs `op` on the calling thread and returns its result as an `io_uring`
/// completion would: a byte count, descriptor or zero, else a negated errno.
///
/// # Safety
///
/// Everything `op` points to is valid for the operation and not otherwise
/// accessed during it.
unsafe fn execute(op: Op) -> i32 {
    // SAFETY: the caller keeps every pointer in `op` valid.
    let result: i64 = unsafe {
        match op {
            Op::Open { path, flags, mode } => libc::openat(PATH_DIR_FD, path, flags, mode).into(),
            Op::Read { fd, ptr, len, offset } => read(fd, ptr, len, offset, 0) as i64,
            Op::Write { fd, ptr, len, offset } => write(fd, ptr, len, offset) as i64,
            // The `statx` wrapper is missing from older musl.
            Op::Statx { fd, statx } => libc::syscall(
                libc::SYS_statx,
                fd,
                STATX_PATH.as_ptr(),
                STATX_FLAGS,
                STATX_MASK,
                statx,
            ),
            Op::Fsync { fd, data_only: true } => libc::fdatasync(fd).into(),
            Op::Fsync { fd, data_only: false } => libc::fsync(fd).into(),
            Op::Ftruncate { fd, len } => libc::ftruncate(fd, len as libc::off_t).into(),
            Op::RenameAt { source, destination } => {
                libc::renameat(PATH_DIR_FD, source, PATH_DIR_FD, destination).into()
            }
            Op::LinkAt { source, destination } => {
                libc::linkat(PATH_DIR_FD, source, PATH_DIR_FD, destination, LINK_FLAGS).into()
            }
            Op::UnlinkAt { path, flags } => libc::unlinkat(PATH_DIR_FD, path, flags).into(),
            Op::MkDirAt { path } => libc::mkdirat(PATH_DIR_FD, path, MKDIR_MODE).into(),
            Op::Close { fd } => libc::close(fd).into(),
        }
    };
    if result < 0 {
        return -io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO);
    }
    result as i32
}

/// Reads only as far as the page cache allows without blocking. Writes are
/// never tried: ext4 never supports nowait buffered writes and tmpfs and
/// overlayfs refuse them, so most attempts would fail. `None` leaves the
/// operation to a worker, which produces the authoritative result: after
/// `EAGAIN` or `EOPNOTSUPP` (filesystems without support), and after `0`,
/// which Linux 5.9 and 5.10 may report before end-of-file.
///
/// # Safety
///
/// As for [`execute`].
unsafe fn try_nowait(op: Op) -> Option<i32> {
    let Op::Read { fd, ptr, len, offset } = op else { return None };
    // SAFETY: the caller keeps every pointer in `op` valid.
    let result = unsafe { read(fd, ptr, len, offset, libc::RWF_NOWAIT) };
    (result > 0).then_some(result as i32)
}

/// `offset` `u64::MAX` becomes `-1`, the file position, as in `io_uring`.
///
/// # Safety
///
/// `ptr..ptr + len` is valid for writes and not otherwise accessed.
unsafe fn read(fd: RawFd, ptr: *mut u8, len: u32, offset: u64, flags: i32) -> isize {
    let iov = libc::iovec { iov_base: ptr.cast(), iov_len: len as usize };
    // SAFETY: `iov` outlives the call and the caller keeps its memory valid.
    unsafe { libc::preadv2(fd, &raw const iov, 1, offset as libc::off_t, flags) }
}

/// As [`read`] without flags, for a write from `ptr..ptr + len`.
///
/// # Safety
///
/// `ptr..ptr + len` is valid for reads and unchanged during the call.
unsafe fn write(fd: RawFd, ptr: *const u8, len: u32, offset: u64) -> isize {
    let iov = libc::iovec { iov_base: ptr.cast_mut().cast(), iov_len: len as usize };
    // SAFETY: `iov` outlives the call and the caller keeps its memory valid.
    unsafe { libc::pwritev2(fd, &raw const iov, 1, offset as libc::off_t, 0) }
}
