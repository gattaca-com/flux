//! Keeps a process alive and stoppable after another process retires a queue
//! segment it maps.
//!
//! Retiring truncates the segment, so the next access to it raises SIGBUS.
//! When the object behind the faulting mapping did shrink, the handler swaps
//! private zeroed pages, carrying the header words the hot path reads, over
//! the whole mapping and raises SIGTERM once. The faulting access then
//! re-executes: consumers read empty slots, producers write into private
//! pages, and the process shuts down as if an operator had sent SIGTERM. A
//! message this process was writing at that moment still gets published, so
//! consumers in this process can receive it zero-filled or torn before they
//! stop. A syscall that reads or writes the segment, such as a socket read
//! into a dcache, can fail with `EFAULT` instead of faulting, so I/O on it can
//! report errors before the shutdown. Any other SIGBUS, such as a full
//! `/dev/shm`, crashes as it would without the handler.

use std::{
    fmt,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicI32, AtomicU8, AtomicUsize, Ordering},
    },
};

use super::{
    QueueHeader, QueueIdentity,
    segment::{self, Segment},
};

const MAX_MAPPINGS: usize = 4096;

/// What the handler needs about one mapping, readable from a signal handler.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
struct Slot {
    start: AtomicUsize,
    len: AtomicUsize,
    /// The mapped object, kept open to tell a retire from any other SIGBUS.
    fd: AtomicI32,
    elsize: AtomicUsize,
    mask: AtomicUsize,
    queue_type: AtomicU8,
    state: AtomicU8,
}

const LIVE: u8 = 0;
const RETIRED: u8 = 2;

impl Slot {
    const fn empty() -> Self {
        Self {
            start: AtomicUsize::new(0),
            len: AtomicUsize::new(0),
            fd: AtomicI32::new(-1),
            elsize: AtomicUsize::new(0),
            mask: AtomicUsize::new(0),
            queue_type: AtomicU8::new(0),
            state: AtomicU8::new(LIVE),
        }
    }
}

static SLOTS: [Slot; MAX_MAPPINGS] = [const { Slot::empty() }; MAX_MAPPINGS];
static N_SLOTS: AtomicUsize = AtomicUsize::new(0);
static OVERFLOW_WARNED: AtomicBool = AtomicBool::new(false);

/// Per mapping, in registration order; slot `i` describes `MAPPINGS[i]`.
static MAPPINGS: Mutex<Vec<Mapping>> = Mutex::new(Vec::new());

struct Mapping {
    path: PathBuf,
    type_name: &'static str,
    type_hash: u64,
}

/// Keeps a validated or freshly initialised queue mapping for the life of the
/// process.
// The lock covers publishing the slot: slots must become visible in order, or
// `N_SLOTS` could move backwards.
#[allow(clippy::significant_drop_tightening)]
pub(super) fn register(segment: Segment, path: &Path, type_name: &'static str) {
    #[cfg(target_os = "linux")]
    handler::install();
    let (ptr, len, fd) = segment.leak();
    // Lets a retiring process see this mapping even when it cannot read our
    // /proc/<pid>/maps.
    unsafe { libc::flock(fd, libc::LOCK_SH | libc::LOCK_NB) };
    #[allow(clippy::cast_ptr_alignment)]
    let header = unsafe { &*ptr.cast::<QueueHeader>() };
    let mut mappings = MAPPINGS.lock().unwrap();
    let i = mappings.len();
    mappings.push(Mapping { path: path.to_owned(), type_name, type_hash: header.type_hash });
    let Some(slot) = SLOTS.get(i) else {
        if !OVERFLOW_WARNED.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                "over {MAX_MAPPINGS} queue mappings: {} and later ones crash this process if \
                 retired",
                path.display()
            );
        }
        return;
    };
    slot.start.store(ptr as usize, Ordering::Relaxed);
    slot.len.store(len, Ordering::Relaxed);
    slot.fd.store(fd, Ordering::Relaxed);
    slot.elsize.store(header.elsize, Ordering::Relaxed);
    slot.mask.store(header.mask, Ordering::Relaxed);
    slot.queue_type.store(header.queue_type as u8, Ordering::Relaxed);
    N_SLOTS.store(i + 1, Ordering::Release);
}

/// Panics if this process already maps `path` for another element type:
/// two types whose queue names collide would retire each other's segment.
pub(super) fn check_collision(path: &Path, type_name: &'static str, type_hash: u64) {
    let other = MAPPINGS
        .lock()
        .unwrap()
        .iter()
        .find(|m| m.path == path && m.type_hash != type_hash)
        .map(|m| m.type_name);
    if let Some(other) = other {
        panic!(
            "{other} and {type_name} share queue {}; opening one would retire the other",
            path.display()
        );
    }
}

/// A queue this process mapped that another process has since retired.
#[derive(Debug, Clone)]
pub struct RetiredQueue {
    pub path: PathBuf,
    pub type_name: &'static str,
    /// The segment the flink points to now.
    pub replacement: Option<QueueIdentity>,
}

impl fmt::Display for RetiredQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "queue {} ({}) was retired", self.path.display(), self.type_name)?;
        match &self.replacement {
            Some(replacement) => write!(f, "; its replacement has {replacement}"),
            None => write!(f, "; it has no replacement"),
        }
    }
}

/// Queues this process mapped that another process retired, one per path.
/// Non-empty once a retire has raised SIGTERM in this process.
pub fn retired_queues() -> Vec<RetiredQueue> {
    let mut retired: Vec<_> = MAPPINGS
        .lock()
        .unwrap()
        .iter()
        .zip(&SLOTS)
        .filter(|(_, slot)| slot.state.load(Ordering::Relaxed) == RETIRED)
        .map(|(m, _)| (m.path.clone(), m.type_name))
        .collect();
    retired.sort_unstable();
    retired.dedup();
    retired
        .into_iter()
        .map(|(path, type_name)| RetiredQueue {
            replacement: segment::current_identity(&path),
            path,
            type_name,
        })
        .collect()
}

// Linux only: `mremap` swaps the pages in atomically. Elsewhere a retired
// mapping crashes its process with SIGBUS.
#[cfg(target_os = "linux")]
mod handler {
    use std::{
        ffi::{c_int, c_void},
        mem::MaybeUninit,
        sync::{
            Once, OnceLock,
            atomic::{AtomicBool, Ordering},
        },
    };

    use super::{LIVE, N_SLOTS, RETIRED, SLOTS, Slot};
    use crate::queue::{QueueHeader, QueueType};

    const REPLACING: u8 = 1;

    static SIGNALLED: AtomicBool = AtomicBool::new(false);
    static PREVIOUS: OnceLock<libc::sigaction> = OnceLock::new();

    impl Slot {
        /// Async-signal-safe.
        fn truncated(&self, len: usize) -> bool {
            let mut stat = MaybeUninit::<libc::stat>::uninit();
            unsafe {
                libc::fstat(self.fd.load(Ordering::Relaxed), stat.as_mut_ptr()) == 0 &&
                    (stat.assume_init().st_size as usize) < len
            }
        }

        /// Async-signal-safe.
        fn replace(&self, start: usize, len: usize) {
            unsafe {
                let fresh = libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                    -1,
                    0,
                );
                if fresh == libc::MAP_FAILED {
                    libc::abort();
                }
                // Fill the header before the pages become visible: a thread reading
                // `queue_type == Unknown` would panic in `produce`.
                #[allow(clippy::cast_ptr_alignment)]
                let header = &mut *fresh.cast::<QueueHeader>();
                header.queue_type = match self.queue_type.load(Ordering::Relaxed) {
                    1 => QueueType::MPMC,
                    2 => QueueType::SPMC,
                    _ => QueueType::Unknown,
                };
                header.elsize = self.elsize.load(Ordering::Relaxed);
                header.mask = self.mask.load(Ordering::Relaxed);
                header.is_initialized.store(1, Ordering::Relaxed);
                let moved = libc::mremap(
                    fresh,
                    len,
                    len,
                    libc::MREMAP_MAYMOVE | libc::MREMAP_FIXED,
                    start as *mut c_void,
                );
                if moved == libc::MAP_FAILED {
                    libc::abort();
                }
            }
        }
    }

    const RETIRED_MSG: &[u8] =
        b"flux-communication: another process retired a shared queue segment \
                                 this process maps; raising SIGTERM\n";

    extern "C" fn on_sigbus(_sig: c_int, info: *mut libc::siginfo_t, _ctx: *mut c_void) {
        unsafe {
            let errno = libc::__errno_location();
            let saved = *errno;
            handle_sigbus((*info).si_addr() as usize);
            *errno = saved;
        }
    }

    fn handle_sigbus(addr: usize) {
        let n = N_SLOTS.load(Ordering::Acquire);
        let hit = SLOTS[..n].iter().find_map(|slot| {
            let start = slot.start.load(Ordering::Relaxed);
            let len = slot.len.load(Ordering::Relaxed);
            (addr.wrapping_sub(start) < len).then_some((slot, start, len))
        });
        if let Some((slot, start, len)) = hit &&
            slot.truncated(len)
        {
            // One thread swaps the pages in; the others retry their access, faulting
            // again until it is done.
            if slot
                .state
                .compare_exchange(LIVE, REPLACING, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                slot.replace(start, len);
                slot.state.store(RETIRED, Ordering::Release);
                if !SIGNALLED.swap(true, Ordering::Relaxed) {
                    raise_sigterm();
                }
            }
            return;
        }
        // Not a retired queue: the access faults again under the previous disposition.
        if let Some(previous) = PREVIOUS.get() {
            unsafe { libc::sigaction(libc::SIGBUS, previous, std::ptr::null_mut()) };
        }
    }

    /// Async-signal-safe. A default SIGTERM ends the process inside `kill`, so
    /// the message goes first then; with a handler, SIGTERM goes first so the
    /// shutdown never waits on stderr.
    fn raise_sigterm() {
        unsafe {
            let mut term: libc::sigaction = std::mem::zeroed();
            libc::sigaction(libc::SIGTERM, std::ptr::null(), &raw mut term);
            let default = term.sa_sigaction == libc::SIG_DFL;
            if !default {
                libc::kill(libc::getpid(), libc::SIGTERM);
            }
            // Skipped when stderr is full, so a stalled reader does not keep this
            // thread in the handler.
            let mut stderr =
                libc::pollfd { fd: libc::STDERR_FILENO, events: libc::POLLOUT, revents: 0 };
            if libc::poll(&raw mut stderr, 1, 0) == 1 && stderr.revents & libc::POLLOUT != 0 {
                libc::write(libc::STDERR_FILENO, RETIRED_MSG.as_ptr().cast(), RETIRED_MSG.len());
            }
            if default {
                libc::kill(libc::getpid(), libc::SIGTERM);
            }
        }
    }

    pub(super) fn install() {
        static INSTALLED: Once = Once::new();
        INSTALLED.call_once(|| unsafe {
            let mut previous: libc::sigaction = std::mem::zeroed();
            libc::sigaction(libc::SIGBUS, std::ptr::null(), &raw mut previous);
            let _ = PREVIOUS.set(previous);
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = on_sigbus as usize;
            action.sa_flags = libc::SA_SIGINFO | libc::SA_ONSTACK;
            libc::sigemptyset(&raw mut action.sa_mask);
            libc::sigaction(libc::SIGBUS, &raw const action, std::ptr::null_mut());
        });
    }
}
