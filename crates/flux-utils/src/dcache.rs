use std::{
    alloc::{self, Layout},
    cell::UnsafeCell,
    mem::size_of_val,
    sync::atomic::{AtomicUsize, Ordering::*, compiler_fence},
};

#[derive(Debug, Clone, Copy)]
pub struct DCacheRef {
    pub offset: usize,
    pub len: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum DCacheError {
    #[error("data length {0} exceeds capacity {1}")]
    DataLenExceedsCapacity(usize, usize),
    #[error("output buffer {0} too small for {1} bytes")]
    BufferTooSmall(usize, usize),
    #[error("referenced data store region was overwritten")]
    StaleData,
}

/// Ring buffer data storage inspired by Firedancer's `dcache`. Provides
/// synchronised multi-producer allocation and leaves read-write ordering and
/// synchronisation to the caller.
///
/// Intended use: the caller exchanges [`DCacheRef`]s through an external
/// ordered channel (e.g. a spine queue) and the channel's Release/Acquire
/// pair is the synchronisation point for the payload bytes stored here.
#[repr(C, align(64))]
pub struct DCache {
    reserved: AtomicUsize,
    _pad: [u8; CACHELINE - size_of::<AtomicUsize>()],
    data: UnsafeCell<[u8]>,
}

const CACHELINE: usize = 64;

impl DCache {
    const OFFSET: usize = size_of::<usize>();

    pub fn new(n: usize) -> Box<Self> {
        assert!(n.is_power_of_two() && n.is_multiple_of(CACHELINE));
        let layout = Layout::from_size_align(CACHELINE + n, CACHELINE).unwrap();
        unsafe {
            let ptr = alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                alloc::handle_alloc_error(layout);
            }
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, n) as *mut Self)
        }
    }

    pub fn from_ptr(ptr: *mut u8, n: usize) -> *const Self {
        std::ptr::slice_from_raw_parts_mut(ptr, n) as *const Self
    }

    #[inline]
    fn capacity(&self) -> usize {
        size_of_val(&self.data)
    }

    #[inline]
    pub fn read(&self, r: DCacheRef, buf: &mut [u8]) -> Result<(), DCacheError> {
        if r.len > buf.len() {
            return Err(DCacheError::BufferTooSmall(buf.len(), r.len));
        }

        let n = self.capacity();
        let base = self.data.get() as *mut u8;
        let offset_ix = r.offset & (n - 1);

        if r.len > n - offset_ix - Self::OFFSET {
            return Err(DCacheError::DataLenExceedsCapacity(r.len, n - Self::OFFSET));
        }

        if !Self::offsets_match(base, offset_ix, r.offset) {
            return Err(DCacheError::StaleData);
        }

        unsafe {
            std::ptr::copy_nonoverlapping(
                base.add(offset_ix + Self::OFFSET),
                buf.as_mut_ptr(),
                r.len,
            );
        }

        compiler_fence(AcqRel);

        if !Self::offsets_match(base, offset_ix, r.offset) {
            return Err(DCacheError::StaleData);
        }

        Ok(())
    }

    /// Applies `f` to the payload slice. The caller must be able to tolerate
    /// execution on stale / invalid data; prefer `read` if that is
    /// unacceptable.
    #[inline]
    pub fn map<T, F>(&self, r: DCacheRef, f: F) -> Result<T, DCacheError>
    where
        F: FnOnce(&[u8]) -> T,
    {
        let n = self.capacity();
        let base = self.data.get() as *mut u8;
        let offset_ix = r.offset & (n - 1);

        if r.len > n - offset_ix - Self::OFFSET {
            return Err(DCacheError::DataLenExceedsCapacity(r.len, n - Self::OFFSET));
        }

        if !Self::offsets_match(base, offset_ix, r.offset) {
            return Err(DCacheError::StaleData);
        }

        let result =
            unsafe { f(std::slice::from_raw_parts(base.add(offset_ix + Self::OFFSET), r.len)) };

        compiler_fence(AcqRel);

        if !Self::offsets_match(base, offset_ix, r.offset) {
            return Err(DCacheError::StaleData);
        }

        Ok(result)
    }

    #[inline]
    fn offsets_match(base: *mut u8, offset_ix: usize, expected_offset: usize) -> bool {
        let offset = unsafe { base.add(offset_ix).cast::<usize>().read() };
        offset == expected_offset
    }

    #[inline]
    fn next_multiple_of_64(x: usize) -> usize {
        (x + 63) & !63
    }

    #[inline]
    pub fn write<F>(&self, len: usize, f: F) -> Result<DCacheRef, DCacheError>
    where
        F: FnOnce(&mut [u8]),
    {
        let n = self.capacity();
        if len > n - Self::OFFSET {
            return Err(DCacheError::DataLenExceedsCapacity(len, n - Self::OFFSET));
        }

        let slot_size = Self::next_multiple_of_64(Self::OFFSET + len);

        let from = loop {
            let curr = self.reserved.load(Relaxed);
            let from_ix = curr & (n - 1);
            let (actual, next) = if from_ix + Self::OFFSET + len > n {
                // skip to from_ix = 0
                let aligned = (curr | (n - 1)) + 1;
                (aligned, aligned + slot_size)
            } else {
                (curr, curr + slot_size)
            };
            if self.reserved.compare_exchange_weak(curr, next, Relaxed, Relaxed).is_ok() {
                break actual;
            }
        };

        let from_ix = from & (n - 1);
        let base = self.data.get() as *mut u8;

        unsafe { base.add(from_ix).cast::<usize>().write(from) };

        compiler_fence(Release);

        unsafe {
            f(std::slice::from_raw_parts_mut(base.add(from_ix + Self::OFFSET), len));
        }

        Ok(DCacheRef { offset: from, len })
    }
}

unsafe impl Sync for DCache {}
unsafe impl Send for DCache {}

#[cfg(test)]
mod tests {
    use std::{
        cell::UnsafeCell,
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread,
    };

    use super::*;

    struct TestQueue<T> {
        lock: AtomicBool,
        q: UnsafeCell<VecDeque<T>>,
        writers_done: AtomicUsize,
    }

    unsafe impl<T: Send> Send for TestQueue<T> {}
    unsafe impl<T: Send> Sync for TestQueue<T> {}

    impl<T> TestQueue<T> {
        fn new() -> Self {
            Self {
                lock: AtomicBool::new(false),
                q: UnsafeCell::new(VecDeque::new()),
                writers_done: AtomicUsize::new(0),
            }
        }

        fn lock(&self) {
            while self.lock.swap(true, Ordering::AcqRel) {
                std::hint::spin_loop();
            }
        }

        fn unlock(&self) {
            self.lock.store(false, Ordering::Release);
        }

        fn push(&self, item: T) {
            self.lock();
            unsafe { (*self.q.get()).push_back(item) };
            self.unlock();
        }

        fn writer_done(&self) {
            self.writers_done.fetch_add(1, Ordering::AcqRel);
        }

        // Returns None when all n_writers are done and the queue is drained.
        fn pop(&self, n_writers: usize) -> Option<T> {
            loop {
                self.lock();
                let item = unsafe { (*self.q.get()).pop_front() };
                self.unlock();
                if item.is_some() {
                    return item;
                }
                if self.writers_done.load(Ordering::Acquire) == n_writers {
                    return None;
                }
                std::hint::spin_loop();
            }
        }
    }

    #[test]
    fn roundtrip() {
        let dc = DCache::new(64);
        let r = dc.write(5, |s| s.copy_from_slice(b"hello")).unwrap();
        let mut buf = [0u8; 5];
        dc.read(r, &mut buf).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn no_wrap() {
        // First write(10): from_ix=0, fits → offset=0, reserved=64.
        // Second write(80): from_ix=64, 64+8+80=152>128 → skip to lap 128.
        // offset=128, from_ix=0, payload at [8,88).
        let dc = DCache::new(128);
        let _ = dc.write(10, |s| s.fill(0xAA)).unwrap();
        let r = dc.write(80, |s| s.fill(0xBB)).unwrap();
        assert_eq!(r.offset, 128);
        let mut buf = [0u8; 80];
        dc.read(r, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn map_contiguous() {
        let dc = DCache::new(64);
        let r = dc.write(5, |s| s.copy_from_slice(b"hello")).unwrap();
        let got = dc.map(r, |s| s.to_vec()).unwrap();
        assert_eq!(got, b"hello");
    }

    #[test]
    fn map_lap_boundary() {
        // Second write lands at lap boundary (offset=128, from_ix=0); map must
        // present contiguous data with no copy.
        let dc = DCache::new(128);
        let _ = dc.write(10, |s| s.fill(0xAA)).unwrap();
        let r = dc.write(80, |s| s.fill(0xBB)).unwrap();
        assert_eq!(r.offset, 128);
        let got = dc.map(r, |s| s.to_vec()).unwrap();
        assert!(got.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn stale() {
        let dc = DCache::new(64);
        let r = dc.write(4, |s| s.copy_from_slice(b"AAAA")).unwrap();
        let _ = dc.write(4, |s| s.copy_from_slice(b"BBBB")).unwrap();
        // Second write at from_ix=0 overwrites header[0..8] with 64 != r.offset(0) →
        // stale
        let mut buf = [0u8; 4];
        assert!(matches!(dc.read(r, &mut buf), Err(DCacheError::StaleData)));
    }

    #[test]
    fn len_too_large() {
        let dc = DCache::new(64);
        assert!(matches!(dc.write(57, |_| {}), Err(DCacheError::DataLenExceedsCapacity(57, 56))));
        let r = dc.write(2, |s| s.copy_from_slice(b"AB")).unwrap();
        let mut small = [0u8; 1];
        assert!(matches!(dc.read(r, &mut small), Err(DCacheError::BufferTooSmall(1, 2))));
    }

    #[test]
    fn cacheline_aligned_offsets() {
        let dc = DCache::new(1024);
        let sizes = [1usize, 63, 64, 65, 127, 128];
        let mut expected = 0usize;
        for size in sizes {
            let r = dc.write(size, |_| {}).unwrap();
            assert_eq!(r.offset, expected, "wrong offset for size {size}");
            assert_eq!(r.offset % 64, 0, "offset not cacheline-aligned for size {size}");
            expected += (size_of::<usize>() + size).next_multiple_of(64);
        }
    }

    #[test]
    fn mpmc() {
        const WRITERS: usize = 4;
        const READERS: usize = 4;
        const PER: usize = 16;
        const MSG: usize = 8;

        // 4 writers × 16 msgs × 64B each = 4096B reserved total.
        // N=4096 ensures the oldest slot is never stale.
        let dc: Arc<DCache> = Arc::from(DCache::new(4096));
        let q: Arc<TestQueue<(DCacheRef, [u8; MSG])>> = Arc::new(TestQueue::new());

        let write_handles: Vec<_> = (0..WRITERS)
            .map(|i| {
                let dc = Arc::clone(&dc);
                let q = Arc::clone(&q);
                thread::spawn(move || {
                    for j in 0..PER {
                        let payload = [(i * PER + j) as u8; MSG];
                        let r = dc.write(MSG, |s| s.copy_from_slice(&payload)).unwrap();
                        q.push((r, payload));
                    }
                    q.writer_done();
                })
            })
            .collect();

        let read_handles: Vec<_> = (0..READERS)
            .map(|_| {
                let dc = Arc::clone(&dc);
                let q = Arc::clone(&q);
                thread::spawn(move || {
                    while let Some((r, expected)) = q.pop(WRITERS) {
                        let mut buf = [0u8; MSG];
                        dc.read(r, &mut buf).unwrap();
                        assert_eq!(buf, expected);
                    }
                })
            })
            .collect();

        for h in write_handles {
            h.join().unwrap();
        }
        for h in read_handles {
            h.join().unwrap();
        }
    }
}
