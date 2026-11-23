use std::{ffi::c_char, io, os::fd::RawFd};

use crate::uring::Uring;

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
pub(crate) enum Backend {
    Uring(Uring),
}

impl Backend {
    /// Also returns `max_in_flight` clamped to the completions the backend
    /// can hold.
    pub(crate) fn new(entries: u32, max_in_flight: usize) -> io::Result<(Self, usize)> {
        let (ring, max_in_flight) = Uring::new(entries, max_in_flight)?;
        Ok((Self::Uring(ring), max_in_flight))
    }

    pub(crate) fn pin_io_workers(cores: &[usize]) -> io::Result<()> {
        Uring::pin_io_workers(cores)
    }

    #[inline]
    pub(crate) fn submission_full(&mut self) -> bool {
        match self {
            Self::Uring(ring) => ring.submission_full(),
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
        }
    }

    #[inline]
    pub(crate) fn submit(&self) -> io::Result<usize> {
        match self {
            Self::Uring(ring) => ring.submit(),
        }
    }

    pub(crate) fn submit_and_wait(&self, want: usize) -> io::Result<usize> {
        match self {
            Self::Uring(ring) => ring.submit_and_wait(want),
        }
    }

    #[inline]
    pub(crate) fn reap(&mut self, completions: &mut Vec<(u64, i32)>) {
        match self {
            Self::Uring(ring) => ring.reap(completions),
        }
    }

    pub(crate) fn limit_io_workers(&self, limits: &mut [u32; 2]) -> io::Result<()> {
        match self {
            Self::Uring(ring) => ring.limit_io_workers(limits),
        }
    }

    pub(crate) fn as_raw_fd(&self) -> RawFd {
        match self {
            Self::Uring(ring) => ring.as_raw_fd(),
        }
    }
}
