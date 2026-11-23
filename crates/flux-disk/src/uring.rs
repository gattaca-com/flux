use std::{
    io,
    os::fd::{AsRawFd, RawFd},
};

use io_uring::{IoUring, opcode, squeue, types};

use crate::backend::Op;

const EMPTY_PATH: &[u8] = b"\0";

pub(crate) struct Uring(IoUring);

impl Uring {
    pub(crate) fn new(entries: u32, max_in_flight: usize) -> io::Result<(Self, usize)> {
        let ring = IoUring::new(entries)?;
        let max_in_flight = max_in_flight.min(ring.params().cq_entries() as usize);
        Ok((Self(ring), max_in_flight))
    }

    pub(crate) fn pin_io_workers(cores: &[usize]) -> io::Result<()> {
        // Setting up a ring gives the calling thread the worker pool that holds
        // the mask.
        let ring = IoUring::new(1)?;
        ring.submitter().register_iowq_aff(&flux_utils::cpu_set(cores)?)
    }

    #[inline]
    pub(crate) fn submission_full(&mut self) -> bool {
        self.0.submission().is_full()
    }

    /// # Safety
    ///
    /// As for [`crate::backend::Backend::push`].
    #[inline]
    pub(crate) unsafe fn push(&mut self, op: Op, user_data: u64) -> bool {
        let sqe = entry(op, user_data);
        // SAFETY: the caller keeps everything the entry references valid and
        // untouched until the completion for `user_data` is reaped.
        unsafe { self.0.submission().push(&sqe) }.is_ok()
    }

    #[inline]
    pub(crate) fn submit(&self) -> io::Result<usize> {
        self.0.submit()
    }

    pub(crate) fn submit_and_wait(&self, want: usize) -> io::Result<usize> {
        self.0.submit_and_wait(want)
    }

    #[inline]
    pub(crate) fn reap(&mut self, completions: &mut Vec<(u64, i32)>) {
        completions.extend(self.0.completion().map(|cqe| (cqe.user_data(), cqe.result())));
    }

    pub(crate) fn limit_io_workers(&self, limits: &mut [u32; 2]) -> io::Result<()> {
        self.0.submitter().register_iowq_max_workers(limits)
    }

    pub(crate) fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

#[inline]
fn entry(op: Op, user_data: u64) -> squeue::Entry {
    let sqe = match op {
        Op::Open { path, flags, mode } => {
            opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path).flags(flags).mode(mode).build()
        }
        Op::Read { fd, ptr, len, offset } => {
            opcode::Read::new(types::Fd(fd), ptr, len).offset(offset).build()
        }
        Op::Write { fd, ptr, len, offset } => {
            opcode::Write::new(types::Fd(fd), ptr, len).offset(offset).build()
        }
        Op::Statx { fd, statx } => {
            opcode::Statx::new(types::Fd(fd), EMPTY_PATH.as_ptr().cast(), statx.cast())
                .flags(libc::AT_STATX_SYNC_AS_STAT | libc::AT_EMPTY_PATH)
                .mask(libc::STATX_SIZE)
                .build()
        }
        Op::Fsync { fd, data_only } => {
            let mut sync = opcode::Fsync::new(types::Fd(fd));
            if data_only {
                sync = sync.flags(types::FsyncFlags::DATASYNC);
            }
            sync.build()
        }
        Op::Ftruncate { fd, len } => opcode::Ftruncate::new(types::Fd(fd), len).build(),
        Op::RenameAt { source, destination } => opcode::RenameAt::new(
            types::Fd(libc::AT_FDCWD),
            source,
            types::Fd(libc::AT_FDCWD),
            destination,
        )
        .build(),
        Op::LinkAt { source, destination } => opcode::LinkAt::new(
            types::Fd(libc::AT_FDCWD),
            source,
            types::Fd(libc::AT_FDCWD),
            destination,
        )
        .flags(libc::AT_SYMLINK_FOLLOW)
        .build(),
        Op::UnlinkAt { path, flags } => {
            opcode::UnlinkAt::new(types::Fd(libc::AT_FDCWD), path).flags(flags).build()
        }
        Op::MkDirAt { path } => {
            opcode::MkDirAt::new(types::Fd(libc::AT_FDCWD), path).mode(0o777).build()
        }
        Op::Close { fd } => opcode::Close::new(types::Fd(fd)).build(),
    };
    sqe.user_data(user_data)
}
