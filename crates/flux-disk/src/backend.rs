use std::{
    ffi::{CStr, c_char},
    io,
    os::fd::RawFd,
};

use crate::{emulation::Emulation, uring::Uring};

/// Directory every path in an [`Op`] is resolved against.
pub(crate) const PATH_DIR_FD: RawFd = libc::AT_FDCWD;
pub(crate) const LINK_FLAGS: i32 = libc::AT_SYMLINK_FOLLOW;
pub(crate) const MKDIR_MODE: libc::mode_t = 0o777;
pub(crate) const STATX_PATH: &CStr = c"";
pub(crate) const STATX_FLAGS: i32 = libc::AT_STATX_SYNC_AS_STAT | libc::AT_EMPTY_PATH;
pub(crate) const STATX_MASK: u32 = libc::STATX_SIZE;

/// One operation in syscall terms. Its pointers reference the operation's slab
/// entry or, for `read_at_into` and `write_at_from`, caller memory.
#[derive(Clone, Copy)]
pub(crate) enum Op {
    Open { path: *const c_char, flags: i32, mode: libc::mode_t },
    Read { fd: RawFd, ptr: *mut u8, len: u32, offset: u64 },
    Write { fd: RawFd, ptr: *const u8, len: u32, offset: u64 },
    Statx { fd: RawFd, statx: *mut libc::statx },
    Fsync { fd: RawFd, data_only: bool },
    Ftruncate { fd: RawFd, len: u64 },
    RenameAt { source: *const c_char, destination: *const c_char },
    LinkAt { source: *const c_char, destination: *const c_char },
    UnlinkAt { path: *const c_char, flags: i32 },
    MkDirAt { path: *const c_char },
    Close { fd: RawFd },
}

/// Executes [`Op`]s and reports each completion as `(user_data, result)`,
/// where `result` is a byte count or a negated errno.
// One per `DiskIo`; boxing the ring would add an indirection to every call.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Backend {
    Uring(Uring),
    Emulated(Emulation),
}

impl Backend {
    /// Also returns `max_in_flight` clamped to the completions the backend
    /// can hold.
    pub(crate) fn new(entries: u32, max_in_flight: usize) -> io::Result<(Self, usize)> {
        let (ring, max_in_flight) = Uring::new(entries, max_in_flight)?;
        Ok((Self::Uring(ring), max_in_flight))
    }

    /// Runs operations on threads of the submitting thread's pool, at most
    /// `entries` per submission.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn emulated(entries: u32, max_in_flight: usize) -> io::Result<(Self, usize)> {
        Ok((Self::Emulated(Emulation::new(entries)?), max_in_flight))
    }

    pub(crate) fn pin_io_workers(cores: &[usize]) -> io::Result<()> {
        let set = flux_utils::cpu_set(cores)?;
        Emulation::pin_io_workers(set);
        Uring::pin_io_workers(&set)
    }

    #[inline]
    pub(crate) fn is_ring(&self) -> bool {
        matches!(self, Self::Uring(_))
    }

    #[inline]
    pub(crate) fn submission_full(&mut self) -> bool {
        match self {
            Self::Uring(ring) => ring.submission_full(),
            Self::Emulated(emulation) => emulation.submission_full(),
        }
    }

    /// Queues `op` for the next [`Self::submit`]; `false` when the submission
    /// queue is full.
    ///
    /// # Safety
    ///
    /// Every buffer and path `op` points to stays valid, and is not otherwise
    /// accessed, until the completion for `user_data` is reaped.
    #[inline]
    pub(crate) unsafe fn push(&mut self, op: Op, user_data: u64) -> bool {
        match self {
            // SAFETY: the caller upholds this function's contract.
            Self::Uring(ring) => unsafe { ring.push(op, user_data) },
            // SAFETY: the caller upholds this function's contract.
            Self::Emulated(emulation) => unsafe { emulation.push(op, user_data) },
        }
    }

    #[inline]
    pub(crate) fn submit(&mut self) -> io::Result<usize> {
        match self {
            Self::Uring(ring) => ring.submit(),
            Self::Emulated(emulation) => Ok(emulation.submit()),
        }
    }

    /// Forgets operations pushed but never handed on, moving their
    /// `user_data` into `unsubmitted`. The ring keeps its pushed entries for
    /// [`Self::wait`] to submit.
    pub(crate) fn take_unsubmitted(&mut self, unsubmitted: &mut Vec<u64>) {
        match self {
            Self::Uring(_) => {}
            Self::Emulated(emulation) => emulation.take_unsubmitted(unsubmitted),
        }
    }

    /// Blocks until a completion can be reaped; the ring first submits what
    /// was pushed.
    pub(crate) fn wait(&self) -> io::Result<()> {
        match self {
            Self::Uring(ring) => ring.submit_and_wait(1).map(drop),
            Self::Emulated(emulation) => emulation.wait(),
        }
    }

    #[inline]
    pub(crate) fn reap(&mut self, completions: &mut Vec<(u64, i32)>) {
        match self {
            Self::Uring(ring) => ring.reap(completions),
            Self::Emulated(emulation) => emulation.reap(completions),
        }
    }

    pub(crate) fn limit_io_workers(&mut self, limits: &mut [u32; 2]) -> io::Result<()> {
        match self {
            Self::Uring(ring) => ring.limit_io_workers(limits),
            Self::Emulated(emulation) => {
                emulation.limit_io_workers(limits);
                Ok(())
            }
        }
    }

    pub(crate) fn as_raw_fd(&self) -> RawFd {
        match self {
            Self::Uring(ring) => ring.as_raw_fd(),
            Self::Emulated(emulation) => emulation.as_raw_fd(),
        }
    }
}
