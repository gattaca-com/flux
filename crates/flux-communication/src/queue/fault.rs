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
    os::fd::RawFd,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicPtr, Ordering},
};

use super::{
    QueueHeader, QueueIdentity, QueueType,
    segment::{Flink, Segment},
};

/// Every registered mapping, newest first.
static MAPPINGS: AtomicPtr<Mapping> = AtomicPtr::new(std::ptr::null_mut());

/// A queue mapping this process keeps for its lifetime. Published once, never
/// moved or freed, and only `retired` changes afterwards, so the SIGBUS handler
/// reads it without locks.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(super) struct Mapping {
    next: *const Mapping,
    path: PathBuf,
    type_name: &'static str,
    type_hash: u64,
    start: usize,
    len: usize,
    /// The mapped object, kept open to tell a retire from any other SIGBUS.
    fd: RawFd,
    // For the header of the pages that replace a retired mapping. Consumers
    // resync through `mask`, so it must match theirs.
    queue_type: QueueType,
    elsize: usize,
    mask: usize,
    retired: AtomicBool,
}

impl Mapping {
    /// Keeps a validated or freshly initialised queue mapping for the life of
    /// the process.
    pub(super) fn register(segment: Segment, path: &Path, type_name: &'static str) {
        #[cfg(target_os = "linux")]
        handler::SigbusHandler::install();
        let (ptr, len, fd) = segment.leak();
        // Lets a retiring process see this mapping even when it cannot read our
        // /proc/<pid>/maps.
        unsafe { libc::flock(fd, libc::LOCK_SH | libc::LOCK_NB) };
        #[allow(clippy::cast_ptr_alignment)]
        let header = unsafe { &*ptr.cast::<QueueHeader>() };
        let mapping = Box::leak(Box::new(Self {
            next: std::ptr::null(),
            path: path.to_owned(),
            type_name,
            type_hash: header.type_hash,
            start: ptr as usize,
            len,
            fd,
            queue_type: header.queue_type,
            elsize: header.elsize,
            mask: header.mask,
            retired: AtomicBool::new(false),
        }));
        let _ = MAPPINGS.fetch_update(Ordering::Release, Ordering::Relaxed, |head| {
            mapping.next = head;
            Some(&raw mut *mapping)
        });
    }

    /// Async-signal-safe.
    fn registered() -> impl Iterator<Item = &'static Self> {
        let mut next = MAPPINGS.load(Ordering::Acquire).cast_const();
        std::iter::from_fn(move || {
            let mapping = unsafe { next.as_ref()? };
            next = mapping.next;
            Some(mapping)
        })
    }

    /// Panics if this process already maps `path` for another element type:
    /// two types whose queue names collide would retire each other's segment.
    /// Across processes nothing catches that: their versions get compared, and
    /// the higher one retires the other's segment.
    pub(super) fn check_collision(path: &Path, type_name: &'static str, type_hash: u64) {
        if let Some(other) = Self::registered().find(|m| m.path == path && m.type_hash != type_hash)
        {
            panic!(
                "{} and {type_name} share queue {}; opening one would retire the other",
                other.type_name,
                path.display()
            );
        }
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

impl RetiredQueue {
    /// Queues this process mapped that another process retired, one per path.
    /// Non-empty once a retire has raised SIGTERM in this process.
    pub fn all() -> Vec<Self> {
        let mut retired: Vec<_> = Mapping::registered()
            .filter(|m| m.retired.load(Ordering::Relaxed))
            .map(|m| (m.path.clone(), m.type_name))
            .collect();
        retired.sort_unstable();
        retired.dedup();
        retired
            .into_iter()
            .map(|(path, type_name)| Self {
                replacement: Flink(&path).current_identity(),
                path,
                type_name,
            })
            .collect()
    }
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

    use super::Mapping;
    use crate::queue::QueueHeader;

    static SIGNALLED: AtomicBool = AtomicBool::new(false);
    static PREVIOUS: OnceLock<libc::sigaction> = OnceLock::new();

    impl Mapping {
        /// Async-signal-safe.
        fn containing(addr: usize) -> Option<&'static Self> {
            Self::registered().find(|m| addr.wrapping_sub(m.start) < m.len)
        }

        /// Async-signal-safe.
        fn truncated(&self) -> bool {
            let mut stat = MaybeUninit::<libc::stat>::uninit();
            unsafe {
                libc::fstat(self.fd, stat.as_mut_ptr()) == 0 &&
                    (stat.assume_init().st_size as usize) < self.len
            }
        }

        /// Swaps private pages in for this truncated mapping, once, and raises
        /// SIGTERM for the first one in the process. Async-signal-safe.
        fn retire(&self) {
            // One thread swaps the pages in; the others retry their access, faulting
            // again until it is done.
            if !self.retired.swap(true, Ordering::Relaxed) {
                self.replace();
                if !SIGNALLED.swap(true, Ordering::Relaxed) {
                    SigbusHandler::raise_sigterm();
                }
            }
        }

        /// Async-signal-safe.
        fn replace(&self) {
            unsafe {
                let fresh = libc::mmap(
                    std::ptr::null_mut(),
                    self.len,
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
                header.queue_type = self.queue_type;
                header.elsize = self.elsize;
                header.mask = self.mask;
                header.is_initialized.store(1, Ordering::Relaxed);
                let moved = libc::mremap(
                    fresh,
                    self.len,
                    self.len,
                    libc::MREMAP_MAYMOVE | libc::MREMAP_FIXED,
                    self.start as *mut c_void,
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

    /// The process's SIGBUS handler, installed with its first queue mapping.
    pub(super) struct SigbusHandler;

    impl SigbusHandler {
        pub(super) fn install() {
            static INSTALLED: Once = Once::new();
            INSTALLED.call_once(|| unsafe {
                let mut previous: libc::sigaction = std::mem::zeroed();
                libc::sigaction(libc::SIGBUS, std::ptr::null(), &raw mut previous);
                let _ = PREVIOUS.set(previous);
                let mut action: libc::sigaction = std::mem::zeroed();
                action.sa_sigaction = Self::handle as usize;
                action.sa_flags = libc::SA_SIGINFO | libc::SA_ONSTACK;
                libc::sigemptyset(&raw mut action.sa_mask);
                libc::sigaction(libc::SIGBUS, &raw const action, std::ptr::null_mut());
            });
        }

        extern "C" fn handle(_sig: c_int, info: *mut libc::siginfo_t, _ctx: *mut c_void) {
            unsafe {
                let errno = libc::__errno_location();
                let saved = *errno;
                // A fault recurs when the access re-executes. Any other SIGBUS, such as
                // one sent by a process or an asynchronous memory failure notice, does
                // not, and has no address to check.
                let fault = matches!(
                    (*info).si_code,
                    libc::BUS_ADRALN | libc::BUS_ADRERR | libc::BUS_OBJERR | libc::BUS_MCEERR_AR
                );
                match fault.then(|| Mapping::containing((*info).si_addr() as usize)).flatten() {
                    Some(mapping) if mapping.truncated() => mapping.retire(),
                    _ => {
                        if let Some(previous) = PREVIOUS.get() {
                            libc::sigaction(libc::SIGBUS, previous, std::ptr::null_mut());
                        }
                        // Delivered again once this handler returns.
                        if !fault {
                            libc::raise(libc::SIGBUS);
                        }
                    }
                }
                *errno = saved;
            }
        }

        /// Async-signal-safe. A default SIGTERM ends the process inside `kill`,
        /// so the message goes first then; with a handler, SIGTERM goes first
        /// so the shutdown never waits on stderr.
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
                    libc::write(
                        libc::STDERR_FILENO,
                        RETIRED_MSG.as_ptr().cast(),
                        RETIRED_MSG.len(),
                    );
                }
                if default {
                    libc::kill(libc::getpid(), libc::SIGTERM);
                }
            }
        }
    }
}
