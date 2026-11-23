use io_uring::{opcode, squeue, types};

use crate::backend::Op;

const EMPTY_PATH: &[u8] = b"\0";

#[inline]
pub(crate) fn entry(op: Op, user_data: u64) -> squeue::Entry {
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
