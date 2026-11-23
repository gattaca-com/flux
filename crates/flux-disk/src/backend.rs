use std::{ffi::c_char, os::fd::RawFd};

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
