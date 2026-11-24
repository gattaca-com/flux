use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
};

use crate::{
    backend::Op,
    emulation::{self, Emulation},
    uring::Uring,
};

/// `io_uring` for every operation except truncates, which run on the
/// submitting thread's worker pool: for rings that lack only
/// `IORING_OP_FTRUNCATE` (before Linux 6.9).
pub(crate) struct Hybrid {
    ring: Uring,
    truncates: Emulation,
    /// Level-triggered on the ring and the pool's eventfd, so readable exactly
    /// while either has a completion waiting.
    epoll: OwnedFd,
}

// Methods `Backend` dispatches to are out of line, so its `io_uring` arms
// stay small.
impl Hybrid {
    pub(crate) fn new(ring: Uring, entries: u32) -> io::Result<Self> {
        let truncates = Emulation::new(entries)?;
        // SAFETY: no preconditions.
        let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` is a fresh descriptor nothing else owns.
        let epoll = unsafe { OwnedFd::from_raw_fd(fd) };
        for watched in [ring.as_raw_fd(), truncates.as_raw_fd()] {
            let mut event = libc::epoll_event { events: libc::EPOLLIN as u32, u64: 0 };
            // SAFETY: `event` outlives the call.
            if unsafe { libc::epoll_ctl(fd, libc::EPOLL_CTL_ADD, watched, &raw mut event) } < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(Self { ring, truncates, epoll })
    }

    #[inline(never)]
    pub(crate) fn submission_full(&mut self) -> bool {
        self.ring.submission_full()
    }

    /// # Safety
    ///
    /// As for [`crate::backend::Backend::push`].
    #[inline(never)]
    pub(crate) unsafe fn push(&mut self, op: Op, user_data: u64) -> bool {
        match op {
            // SAFETY: the caller upholds `Backend::push`'s contract.
            Op::Ftruncate { .. } => unsafe { self.truncates.push(op, user_data) },
            // SAFETY: the caller upholds `Backend::push`'s contract.
            _ => unsafe { self.ring.push(op, user_data) },
        }
    }

    #[inline(never)]
    pub(crate) fn submit(&mut self) -> io::Result<usize> {
        self.truncates.submit();
        self.ring.submit()
    }

    #[inline(never)]
    pub(crate) fn take_unsubmitted(&mut self, unsubmitted: &mut Vec<u64>) {
        self.truncates.take_unsubmitted(unsubmitted);
    }

    #[inline(never)]
    pub(crate) fn wait(&self) -> io::Result<()> {
        self.ring.submit()?;
        emulation::wait_readable(self.as_raw_fd())
    }

    #[inline(never)]
    pub(crate) fn reap(&mut self, completions: &mut Vec<(u64, i32)>) {
        self.ring.reap(completions);
        self.truncates.reap(completions);
    }

    /// Returns the ring's previous values; the pool takes the same limits.
    #[inline(never)]
    pub(crate) fn limit_io_workers(&mut self, limits: &mut [u32; 2]) -> io::Result<()> {
        let mut requested = *limits;
        self.ring.limit_io_workers(limits)?;
        self.truncates.limit_io_workers(&mut requested);
        Ok(())
    }

    #[inline(never)]
    pub(crate) fn as_raw_fd(&self) -> RawFd {
        self.epoll.as_raw_fd()
    }
}
