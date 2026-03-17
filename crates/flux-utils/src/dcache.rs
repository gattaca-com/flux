use std::{
    alloc::{self, Layout},
    cell::UnsafeCell,
    mem::size_of_val,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
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
    #[error("Invalid offset {0} for a reserved slot of length {1}")]
    InvalidWriteIntoOffset(usize, usize),
    #[error("producer overrun consumer during payload read")]
    SpedPast,
}

/// Ring buffer data storage inspired by Firedancer's `dcache`. Provides
/// synchronised multi-producer allocation and leaves read-write ordering and
/// epoch tracking to the caller.
///
/// Intended use: the caller exchanges [`DCacheRef`]s through an external
/// ordered channel (e.g. a spine queue) and the channel's Release/Acquire
/// pair is the synchronisation point for the payload bytes stored here.
///
/// Epoch tracking should be done via the spine queue seqlock:
/// use `try_consume_with_epoch` + `slot_version` on the consumer side.
#[repr(C, align(64))]
pub struct DCache {
    reserved: AtomicUsize,
    _pad: [u8; CACHELINE - size_of::<AtomicUsize>()],
    data: UnsafeCell<[u8]>,
}

unsafe impl Send for DCache {}
unsafe impl Sync for DCache {}

const CACHELINE: usize = 64;

impl DCache {
    /// Minimum dcache capacity for the epoch check in `consume_dcache` to be
    /// sufficient. The producer must publish at least `queue_depth` messages of
    /// max possible size to lap dcache, guaranteeing the consumer's
    /// held slot seqlock version will have changed before any region is reused.
    pub fn required_capacity(queue_depth: usize, mtu: usize) -> usize {
        (queue_depth + 1) * Self::next_multiple_of_64(mtu)
    }

    pub fn from_ptr(ptr: *mut u8, n: usize) -> *const Self {
        std::ptr::slice_from_raw_parts_mut(ptr, n) as *const Self
    }

    pub fn new(n: usize) -> Arc<Self> {
        assert!(n.is_power_of_two() && n.is_multiple_of(CACHELINE));
        let layout = Layout::from_size_align(CACHELINE + n, CACHELINE).unwrap();
        unsafe {
            let ptr = alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                alloc::handle_alloc_error(layout);
            }
            Arc::from(Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, n) as *mut Self))
        }
    }

    #[inline]
    pub fn write<F>(&self, len: usize, f: F) -> Result<DCacheRef, DCacheError>
    where
        F: FnOnce(&mut [u8]),
    {
        let r = self.reserve(len)?;
        self.write_into(r, 0, f)?;
        Ok(r)
    }

    /// Reserves a slot of `len` bytes and returns a [`DCacheRef`].
    #[inline]
    pub fn reserve(&self, len: usize) -> Result<DCacheRef, DCacheError> {
        let n = self.capacity();
        if len > n {
            return Err(DCacheError::DataLenExceedsCapacity(len, n));
        }

        let slot_size = Self::next_multiple_of_64(len);

        loop {
            let curr = self.reserved.load(Ordering::Relaxed);
            let from_ix = curr & (n - 1);
            let (actual, next) = if from_ix + len > n {
                let aligned = (curr | (n - 1)) + 1;
                (aligned, aligned + slot_size)
            } else {
                (curr, curr + slot_size)
            };
            if self
                .reserved
                .compare_exchange_weak(curr, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(DCacheRef { offset: actual, len });
            }
        }
    }

    /// Calls `f` with a mutable view of `r`'s data region starting at `offset`.
    #[inline]
    pub fn write_into<F, R>(&self, r: DCacheRef, offset: usize, f: F) -> Result<R, DCacheError>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        if offset > r.len {
            return Err(DCacheError::InvalidWriteIntoOffset(offset, r.len));
        }
        let base = self.data.get() as *mut u8;
        let offset_ix = r.offset & (self.capacity() - 1);
        let buf =
            unsafe { std::slice::from_raw_parts_mut(base.add(offset_ix + offset), r.len - offset) };
        Ok(f(buf))
    }

    #[inline]
    pub fn read(&self, r: DCacheRef, buf: &mut [u8]) -> Result<(), DCacheError> {
        if r.len > buf.len() {
            return Err(DCacheError::BufferTooSmall(buf.len(), r.len));
        }
        let (base, offset_ix) = self.deref(r)?;
        unsafe { std::ptr::copy_nonoverlapping(base.add(offset_ix), buf.as_mut_ptr(), r.len) };
        Ok(())
    }

    /// Applies `f` to the payload slice without copying.
    #[inline]
    pub fn map<T, F>(&self, r: DCacheRef, f: F) -> Result<T, DCacheError>
    where
        F: FnOnce(&[u8]) -> T,
    {
        let (base, offset_ix) = self.deref(r)?;
        Ok(unsafe { f(std::slice::from_raw_parts(base.add(offset_ix), r.len)) })
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        size_of_val(&self.data)
    }

    #[inline]
    fn deref(&self, r: DCacheRef) -> Result<(*mut u8, usize), DCacheError> {
        let n = self.capacity();
        let base = self.data.get() as *mut u8;
        let offset_ix = r.offset & (n - 1);
        if r.len > n - offset_ix {
            return Err(DCacheError::DataLenExceedsCapacity(r.len, n));
        }
        Ok((base, offset_ix))
    }

    #[inline]
    fn next_multiple_of_64(x: usize) -> usize {
        (x + 63) & !63
    }
}

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
        let reader = dc.clone();
        let mut buf = [0u8; 5];
        reader.read(r, &mut buf).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn no_wrap() {
        // First write(10): from_ix=0, fits → offset=0, reserved=64.
        // Second write(80): from_ix=64, 64+80=144>128 → skip to lap 128.
        // offset=128, from_ix=0, data at [0,80).
        let dc = DCache::new(128);
        let reader = dc.clone();
        let _ = dc.write(10, |s| s.fill(0xAA)).unwrap();
        let r = dc.write(80, |s| s.fill(0xBB)).unwrap();
        assert_eq!(r.offset, 128);
        let mut buf = [0u8; 80];
        reader.read(r, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn map_contiguous() {
        let dc = DCache::new(64);
        let r = dc.write(5, |s| s.copy_from_slice(b"hello")).unwrap();
        let reader = dc.clone();
        let got = reader.map(r, |s| s.to_vec()).unwrap();
        assert_eq!(got, b"hello");
    }

    #[test]
    fn map_lap_boundary() {
        let dc = DCache::new(128);
        let reader = dc.clone();
        let _ = dc.write(10, |s| s.fill(0xAA)).unwrap();
        let r = dc.write(80, |s| s.fill(0xBB)).unwrap();
        assert_eq!(r.offset, 128);
        let got = reader.map(r, |s| s.to_vec()).unwrap();
        assert!(got.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn len_too_large() {
        let dc = DCache::new(64);
        assert!(matches!(dc.write(65, |_| {}), Err(DCacheError::DataLenExceedsCapacity(65, 64))));
        let r = dc.write(2, |s| s.copy_from_slice(b"AB")).unwrap();
        let reader = dc.clone();
        let mut small = [0u8; 1];
        assert!(matches!(reader.read(r, &mut small), Err(DCacheError::BufferTooSmall(1, 2))));
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
            expected += size.next_multiple_of(64);
        }
    }

    #[test]
    fn spsc() {
        const N: usize = 16;
        const MSG: usize = 8;

        let writer = DCache::new(1024);
        let reader = Arc::new(writer.clone());
        let q: Arc<TestQueue<(DCacheRef, [u8; MSG])>> = Arc::new(TestQueue::new());

        let q_w = Arc::clone(&q);
        let write_handle = thread::spawn(move || {
            for i in 0..N {
                let payload = [i as u8; MSG];
                let r = writer.write(MSG, |s| s.copy_from_slice(&payload)).unwrap();
                q_w.push((r, payload));
            }
            q_w.writer_done();
        });

        let read_handle = thread::spawn(move || {
            while let Some((r, expected)) = q.pop(1) {
                let mut buf = [0u8; MSG];
                reader.read(r, &mut buf).unwrap();
                assert_eq!(buf, expected);
            }
        });

        write_handle.join().unwrap();
        read_handle.join().unwrap();
    }

    #[test]
    fn mpmc() {
        const N_WRITERS: usize = 4;
        const N: usize = 16;
        const MSG: usize = 8;

        let writer = DCache::new(4096);
        let reader = Arc::new(writer.clone());
        let q: Arc<TestQueue<(DCacheRef, [u8; MSG])>> = Arc::new(TestQueue::new());

        let write_handles: Vec<_> = (0..N_WRITERS)
            .map(|w| {
                let dc = writer.clone();
                let q_w = Arc::clone(&q);
                thread::spawn(move || {
                    for i in 0..N {
                        let payload = [(w * N + i) as u8; MSG];
                        let r = dc.write(MSG, |s| s.copy_from_slice(&payload)).unwrap();
                        q_w.push((r, payload));
                    }
                    q_w.writer_done();
                })
            })
            .collect();

        let read_handle = thread::spawn(move || {
            while let Some((r, expected)) = q.pop(N_WRITERS) {
                let mut buf = [0u8; MSG];
                reader.read(r, &mut buf).unwrap();
                assert_eq!(buf, expected);
            }
        });

        for h in write_handles {
            h.join().unwrap();
        }
        read_handle.join().unwrap();
    }
}
