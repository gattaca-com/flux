use std::{
    cell::UnsafeCell,
    fmt,
    sync::atomic::{AtomicU64, Ordering, compiler_fence},
};

use crate::error::{EmptyError, ReadError};

/// A sequential lock. Use a ArcSwap if you want something similar in
/// performance but for none copy types
#[repr(C, align(64))]
pub struct Seqlock<T> {
    pub version: AtomicU64,
    pub data: UnsafeCell<T>,
}
unsafe impl<T: Send> Send for Seqlock<T> {}
unsafe impl<T: Send> Sync for Seqlock<T> {}

// TODO @lopo: Try 32 bit version
impl<T: Copy> Seqlock<T> {
    #[inline]
    pub const fn new(val: T) -> Seqlock<T> {
        Seqlock { version: AtomicU64::new(2), data: UnsafeCell::new(val) }
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub fn set_version_unsafe(&self, v: u64) {
        self.version.store(v, Ordering::Relaxed)
    }

    #[inline]
    pub fn was_ever_written(&self) -> bool {
        self.version() > 1
    }

    #[inline(never)]
    pub fn read_with_version(
        &self,
        result: &mut T,
        expected_version: u64,
    ) -> Result<(), ReadError> {
        let v1 = self.version.load(Ordering::Acquire);
        if v1 < expected_version {
            return Err(ReadError::Empty);
        }

        compiler_fence(Ordering::AcqRel);
        *result = unsafe { *self.data.get() };
        compiler_fence(Ordering::AcqRel);
        let v2 = self.version.load(Ordering::Acquire);
        if v2 == expected_version { Ok(()) } else { Err(ReadError::SpedPast) }
    }

    #[inline(never)]
    pub fn read_copy_if_updated(&self, expected_version: u64) -> Result<(T, u64), ReadError> {
        if self.version.load(Ordering::Acquire) < expected_version {
            return Err(ReadError::Empty);
        }
        self.read_copy()
    }

    #[inline(never)]
    pub fn read(&self, result: &mut T) -> Result<(), EmptyError> {
        loop {
            let v1 = self.version.load(Ordering::Acquire);
            if v1 < 2 {
                return Err(EmptyError::Empty);
            }
            compiler_fence(Ordering::AcqRel);
            unsafe {
                *result = *self.data.get();
            }
            compiler_fence(Ordering::AcqRel);
            let v2 = self.version.load(Ordering::Acquire);
            if v1 == v2 && v1 & 1 == 0 {
                return Ok(());
            }
            #[cfg(target_arch = "x86_64")]
            unsafe {
                std::arch::x86_64::_mm_pause()
            };
        }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub fn view_unsafe(&self) -> Result<&mut T, ReadError> {
        if !self.was_ever_written() {
            Err(ReadError::Empty)
        } else {
            Ok(unsafe { &mut *self.data.get() })
        }
    }

    #[inline(never)]
    pub fn write(&self, data: &T) {
        // Increment the sequence number. At this point, the number will be odd,
        // which will force readers to spin until we finish writing.
        let v = self.version.fetch_add(1, Ordering::Release);
        compiler_fence(Ordering::AcqRel);
        // Make sure any writes to the data happen after incrementing the
        // sequence number. What we ideally want is a store(Acquire), but the
        // Acquire ordering is not available on stores.
        unsafe { *self.data.get() = *data };
        compiler_fence(Ordering::AcqRel);
        // unsafe {asm!("sti");}
        self.version.store(v.wrapping_add(2), Ordering::Release);
    }

    #[inline(never)]
    pub fn write_unpoison(&self, data: &T) {
        let v = self.version.load(Ordering::Relaxed);
        self.version.store(v.wrapping_add(v.wrapping_sub(1) & 1), Ordering::Release);
        // Make sure any writes to the data happen after incrementing the
        // sequence number. What we ideally want is a store(Acquire), but the
        // Acquire ordering is not available on stores.
        compiler_fence(Ordering::AcqRel);
        if v > 1 {
            unsafe { *self.data.get() = *data };
        } else {
            unsafe { std::ptr::write(self.data.get(), *data) }
        }
        compiler_fence(Ordering::AcqRel);
        self.version.store(v.wrapping_add(1), Ordering::Relaxed);
    }

    #[inline(never)]
    pub fn write_multi_producer(&self, data: &T) {
        // Increment the sequence number. At this point, the number will be odd,
        // which will force readers to spin until we finish writing.
        let mut v = self.version.fetch_or(1, Ordering::AcqRel);
        while v & 1 == 1 {
            v = self.version.fetch_or(1, Ordering::AcqRel);
        }
        // Make sure any writes to the data happen after incrementing the
        // sequence number. What we ideally want is a store(Acquire), but the
        // Acquire ordering is not available on stores.
        if v > 1 {
            unsafe { *self.data.get() = *data };
        } else {
            unsafe { std::ptr::write(self.data.get(), *data) }
        }
        compiler_fence(Ordering::AcqRel);
        self.version.store(v.wrapping_add(2), Ordering::Release);
    }

    #[inline(never)]
    pub fn write_at_version(&self, data: &T, current_version: u64) -> bool {
        // Increment the sequence number. At this point, the number will be odd,
        // which will force readers to spin until we finish writing.
        let Ok(v) = self.version.compare_exchange(
            current_version,
            current_version.wrapping_add(1),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) else {
            return false;
        };

        // Make sure any writes to the data happen after incrementing the
        // sequence number. What we ideally want is a store(Acquire), but the
        // Acquire ordering is not available on stores.
        if v > 1 {
            unsafe { *self.data.get() = *data };
        } else {
            unsafe { std::ptr::write(self.data.get(), *data) }
        }
        compiler_fence(Ordering::AcqRel);
        self.version.store(v.wrapping_add(2), Ordering::Release);
        true
    }

    #[inline(never)]
    pub fn reset(&self) {
        compiler_fence(Ordering::AcqRel);
        self.version.store(0, Ordering::Release);
    }

    #[inline(never)]
    pub fn read_copy(&self) -> Result<(T, u64), ReadError> {
        loop {
            let v1 = self.version.load(Ordering::Acquire);
            if v1 < 2 {
                return Err(ReadError::Empty);
            }
            if v1 & 1 != 0 {
                continue;
            }
            compiler_fence(Ordering::AcqRel);
            let result = unsafe { *self.data.get() };
            compiler_fence(Ordering::AcqRel);
            let v2 = self.version.load(Ordering::Acquire);
            if v1 == v2 {
                return Ok((result, v2));
            }
            #[cfg(target_arch = "x86_64")]
            unsafe {
                std::arch::x86_64::_mm_pause()
            };
        }
    }
}

impl<T: Default> Default for Seqlock<T> {
    #[inline]
    fn default() -> Seqlock<T> {
        Seqlock { version: AtomicU64::new(0), data: UnsafeCell::new(T::default()) }
    }
}

impl<T: Copy + fmt::Debug> fmt::Debug for Seqlock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SeqLock {{ data: {:?} }}", unsafe { *self.data.get() })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::AtomicBool,
        time::{Duration, Instant},
    };

    use super::*;

    #[test]
    fn lock_size() {
        assert_eq!(std::mem::size_of::<Seqlock<[u8; 48]>>(), 64);
        assert_eq!(std::mem::size_of::<Seqlock<[u8; 61]>>(), 128)
    }

    fn consumer_loop<const N: usize>(lock: &Seqlock<[usize; N]>, done: &AtomicBool) {
        let mut msg = [0usize; N];
        while !done.load(Ordering::Relaxed) {
            let _ = lock.read(&mut msg);
            let first = msg[0];
            for i in msg {
                assert_eq!(first, i);
            }
        }
    }

    fn producer_loop<const N: usize>(lock: &Seqlock<[usize; N]>, done: &AtomicBool, multi: bool) {
        let curt = Instant::now();
        let mut count = 0;
        let mut msg = [0usize; N];
        while curt.elapsed() < Duration::from_secs(1) {
            msg.fill(count);
            if multi {
                lock.write_multi_producer(&msg);
            } else {
                lock.write(&msg);
            }
            count = count.wrapping_add(1);
        }
        done.store(true, Ordering::Relaxed);
    }

    fn read_test<const N: usize>() {
        let lock = Seqlock::new([0usize; N]);
        let done = AtomicBool::new(false);
        std::thread::scope(|s| {
            s.spawn(|| {
                consumer_loop(&lock, &done);
            });
            s.spawn(|| {
                producer_loop(&lock, &done, false);
            });
        });
    }

    fn read_test_multi<const N: usize>() {
        let lock = Seqlock::new([0usize; N]);
        let done = AtomicBool::new(false);
        std::thread::scope(|s| {
            s.spawn(|| {
                consumer_loop(&lock, &done);
            });
            s.spawn(|| {
                producer_loop(&lock, &done, true);
            });
            s.spawn(|| {
                producer_loop(&lock, &done, true);
            });
        });
    }

    #[test]
    fn read_16() {
        read_test::<16>()
    }
    #[test]
    fn read_32() {
        read_test::<32>()
    }
    #[test]
    fn read_64() {
        read_test::<64>()
    }
    #[test]
    fn read_128() {
        read_test::<128>()
    }
    #[test]
    fn read_large() {
        read_test::<8192>()
    }

    #[test]
    fn read_16_multi() {
        read_test_multi::<16>()
    }
    #[test]
    fn read_32_multi() {
        read_test_multi::<32>()
    }
    #[test]
    fn read_64_multi() {
        read_test_multi::<64>()
    }
    #[test]
    fn read_128_multi() {
        read_test_multi::<128>()
    }
    #[test]
    fn read_large_multi() {
        read_test_multi::<8192>()
    }

    #[test]
    fn write_unpoison() {
        let lock = Seqlock::default();
        lock.set_version_unsafe(1);
        lock.write_unpoison(&1);
        assert_eq!(lock.version(), 2);
    }
}
