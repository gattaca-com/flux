//! Hardware counters (instructions retired, CPU cycles) via
//! `perf_event_open` + userspace rdpmc.
//!
//! Reads cost ~30 cycles. Counts userspace only (`exclude_kernel|exclude_hv`),
//! so it works at `perf_event_paranoid` <= 2 without privileges.
//!
//! perf binds pid=0 events to the *opening* thread — construct on the tile
//! thread, never the spawner.
//!
//! ABI structs are defined locally: libc has no `perf_event_mmap_page` and we
//! need only a prefix of `perf_event_attr`.

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod imp {
    use std::sync::atomic::{Ordering, compiler_fence};

    use tracing::warn;

    const PERF_TYPE_HARDWARE: u32 = 0;
    const PERF_COUNT_HW_CPU_CYCLES: u64 = 0;
    const PERF_COUNT_HW_INSTRUCTIONS: u64 = 1;
    const PERF_FLAG_FD_CLOEXEC: libc::c_ulong = 8;
    // perf_event_attr flag bits.
    const ATTR_EXCLUDE_KERNEL: u64 = 1 << 5;
    const ATTR_EXCLUDE_HV: u64 = 1 << 6;
    // perf_event_mmap_page.capabilities bits.
    const CAP_USER_RDPMC: u64 = 1 << 2;

    /// Prefix of linux `perf_event_attr`, zero-padded to 128 bytes.
    /// Kernel accepts any size with zeroed bytes beyond its known struct.
    #[repr(C)]
    struct PerfEventAttr {
        type_: u32,
        size: u32,
        config: u64,
        sample_period: u64,
        sample_type: u64,
        read_format: u64,
        flags: u64,
        _pad: [u64; 10],
    }

    /// Prefix of linux `perf_event_mmap_page`.
    #[repr(C)]
    struct PerfEventMmapPage {
        version: u32,
        compat_version: u32,
        lock: u32,
        index: u32,
        offset: i64,
        time_enabled: u64,
        time_running: u64,
        capabilities: u64,
        pmc_width: u16,
    }

    #[derive(Clone, Copy, Debug)]
    pub struct HwCounter {
        page: *const PerfEventMmapPage,
        fd: i32,
    }

    // Used from exactly one tile thread; raw ptr blocks the auto impl.
    unsafe impl Send for HwCounter {}

    impl HwCounter {
        pub fn instructions() -> Option<Self> {
            Self::new(PERF_COUNT_HW_INSTRUCTIONS)
        }

        pub fn cycles() -> Option<Self> {
            Self::new(PERF_COUNT_HW_CPU_CYCLES)
        }

        /// Returns None (with a warn) if `perf_event_open` or rdpmc is
        /// unavailable — typically `kernel.perf_event_paranoid` > 2.
        /// fd and mapping live for the process; no Drop (keeps Copy).
        fn new(config: u64) -> Option<Self> {
            let mut attr: PerfEventAttr = unsafe { std::mem::zeroed() };
            attr.type_ = PERF_TYPE_HARDWARE;
            attr.size = size_of::<PerfEventAttr>() as u32;
            attr.config = config;
            attr.flags = ATTR_EXCLUDE_KERNEL | ATTR_EXCLUDE_HV;

            let fd = unsafe {
                libc::syscall(libc::SYS_perf_event_open, &attr, 0, -1, -1, PERF_FLAG_FD_CLOEXEC)
            } as i32;
            if fd < 0 {
                let err = std::io::Error::last_os_error();
                warn!(
                    ?err,
                    "perf_event_open failed; instruction counts disabled \
                     (check kernel.perf_event_paranoid <= 2)"
                );
                return None;
            }

            let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
            let page = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    page_size,
                    libc::PROT_READ,
                    libc::MAP_SHARED,
                    fd,
                    0,
                )
            };
            if page == libc::MAP_FAILED {
                let err = std::io::Error::last_os_error();
                warn!(?err, "perf mmap failed; instruction counts disabled");
                unsafe { libc::close(fd) };
                return None;
            }
            let page = page as *const PerfEventMmapPage;

            let caps = unsafe { std::ptr::read_volatile(&raw const (*page).capabilities) };
            if caps & CAP_USER_RDPMC == 0 {
                warn!(
                    "perf cap_user_rdpmc unset; instruction counts disabled \
                     (check /sys/bus/event_source/devices/cpu/rdpmc)"
                );
                unsafe {
                    libc::munmap(page as *mut libc::c_void, page_size);
                    libc::close(fd);
                }
                return None;
            }

            Some(Self { page, fd })
        }

        /// Current instructions-retired count. Userspace mmap-page seqlock
        /// read (see `perf_event_mmap_page` kernel docs); falls back to the
        /// read(2) path if the event is momentarily descheduled (index == 0).
        #[inline]
        pub fn read(&self) -> u64 {
            unsafe {
                loop {
                    let lock = std::ptr::read_volatile(&raw const (*self.page).lock);
                    compiler_fence(Ordering::Acquire);

                    let index = std::ptr::read_volatile(&raw const (*self.page).index);
                    let offset = std::ptr::read_volatile(&raw const (*self.page).offset);
                    let width =
                        u32::from(std::ptr::read_volatile(&raw const (*self.page).pmc_width));

                    if index == 0 {
                        return self.read_slow();
                    }
                    // rdpmc yields pmc_width valid bits; sign-extend before
                    // adding the kernel-maintained offset.
                    let pmc = rdpmc(index - 1);
                    let pmc = ((pmc << (64 - width)) as i64) >> (64 - width);
                    let count = offset.wrapping_add(pmc) as u64;

                    compiler_fence(Ordering::Acquire);
                    if std::ptr::read_volatile(&raw const (*self.page).lock) == lock {
                        return count;
                    }
                }
            }
        }

        #[cold]
        fn read_slow(&self) -> u64 {
            let mut count = 0u64;
            let n = unsafe { libc::read(self.fd, (&raw mut count).cast::<libc::c_void>(), 8) };
            if n == 8 { count } else { 0 }
        }
    }

    #[inline]
    fn rdpmc(counter: u32) -> u64 {
        let lo: u32;
        let hi: u32;
        unsafe {
            core::arch::asm!(
                "rdpmc",
                in("ecx") counter,
                out("eax") lo,
                out("edx") hi,
                options(nostack, preserves_flags),
            );
        }
        ((hi as u64) << 32) | lo as u64
    }
}

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
mod imp {
    #[derive(Clone, Copy, Debug)]
    pub struct HwCounter {}

    impl HwCounter {
        pub fn instructions() -> Option<Self> {
            None
        }

        pub fn cycles() -> Option<Self> {
            None
        }

        #[inline]
        pub fn read(&self) -> u64 {
            0
        }
    }
}

pub use imp::HwCounter;

#[cfg(all(test, target_os = "linux", target_arch = "x86_64"))]
mod tests {
    use super::HwCounter;

    /// Skips (None) where `perf_event_open` is restricted, e.g. CI.
    #[test]
    fn counts_loop_instructions() {
        let Some(c) = HwCounter::instructions() else {
            eprintln!("perf unavailable; skipping");
            return;
        };
        let start = c.read();
        let mut acc = 0u64;
        for i in 0..100_000u64 {
            acc = acc.wrapping_add(std::hint::black_box(i));
        }
        std::hint::black_box(acc);
        let delta = c.read().saturating_sub(start);
        // 100k iterations retire >= 100k instructions.
        assert!(delta >= 100_000, "delta = {delta}");
    }

    #[test]
    fn counts_cycles() {
        let Some(c) = HwCounter::cycles() else {
            eprintln!("perf unavailable; skipping");
            return;
        };
        let start = c.read();
        let mut acc = 0u64;
        for i in 0..100_000u64 {
            acc = acc.wrapping_add(std::hint::black_box(i));
        }
        std::hint::black_box(acc);
        let delta = c.read().saturating_sub(start);
        // 100k dependent adds take >= ~25k cycles on any core.
        assert!(delta >= 10_000, "delta = {delta}");
    }
}
