use std::{
    cell::UnsafeCell,
    sync::atomic::{AtomicUsize, Ordering::*, compiler_fence},
};

#[derive(Debug, Clone, Copy)]
pub struct DataStoreRef {
    pub offset: usize,
    pub len: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum DataStoreError {
    #[error("data length {0} exceeds capacity {1}")]
    DataLenExceedsCapacity(usize, usize),
    #[error("output buffer {0} too small for {1} bytes")]
    BufferTooSmall(usize, usize),
    #[error("referenced data store region was overwritten")]
    StaleData,
}

/// Ring buffer data storage inspired by Firedancer's 'dcache'. Provides
/// synchronised multi-producer allocation and leaves read-write ordering and
/// synchronisation to the caller.
///
/// Intended use: the caller exchanges [`DataStoreRef`]s through an external
/// ordered channel (e.g. a spine queue) and the channel's Release/Acquire
/// pair is the synchronisation point for the payload bytes stored here.
pub struct DataStore<const N: usize> {
    data: Box<UnsafeCell<[u8; N]>>,
    reserved: AtomicUsize,
}

impl<const N: usize> DataStore<N> {
    const CACHELINE: usize = 64;

    pub fn new() -> Self {
        const { assert!(N.is_power_of_two() && N.is_multiple_of(Self::CACHELINE)) };
        Self { data: Box::new(UnsafeCell::new([0u8; N])), reserved: AtomicUsize::new(0) }
    }

    #[inline]
    pub fn read(&self, r: DataStoreRef, buf: &mut [u8]) -> Result<(), DataStoreError> {
        if r.len > N {
            return Err(DataStoreError::DataLenExceedsCapacity(r.len, N));
        }
        if r.len > buf.len() {
            return Err(DataStoreError::BufferTooSmall(buf.len(), r.len));
        }

        if r.offset + N < self.reserved.load(Relaxed) {
            return Err(DataStoreError::StaleData);
        }

        self.copy(r.len, r.offset, buf);

        // Prevent the compiler from sinking the data reads past the staleness
        // check below.
        compiler_fence(AcqRel);

        if r.offset + N < self.reserved.load(Relaxed) {
            return Err(DataStoreError::StaleData);
        }

        Ok(())
    }

    #[inline]
    fn copy(&self, r_len: usize, r_offset: usize, buf: &mut [u8]) {
        let base = self.data.get().cast::<u8>();
        let from_ix = r_offset & (N - 1);
        let head_len = (N - from_ix).min(r_len);
        unsafe {
            std::ptr::copy_nonoverlapping(base.add(from_ix), buf.as_mut_ptr(), head_len);
            let tail_len = r_len - head_len;
            if tail_len > 0 {
                std::ptr::copy_nonoverlapping(base, buf[head_len..].as_mut_ptr(), tail_len);
            }
        }
    }

    #[inline]
    pub fn map<T, F, const C: usize>(&self, r: DataStoreRef, f: F) -> Result<T, DataStoreError>
    where
        F: FnOnce(&[u8]) -> T,
    {
        if r.len > N {
            return Err(DataStoreError::DataLenExceedsCapacity(r.len, N));
        }

        if r.offset + N < self.reserved.load(Relaxed) {
            return Err(DataStoreError::StaleData);
        }

        let from_ix = r.offset & (N - 1);
        let result = if r.len > N - from_ix {
            if r.len > C {
                return Err(DataStoreError::DataLenExceedsCapacity(r.len, C));
            }
            let mut buf = [0u8; C];
            self.copy(r.len, r.offset, &mut buf);
            f(buf.as_slice())
        } else {
            let base = self.data.get().cast::<u8>();
            f(unsafe { std::slice::from_raw_parts(base.add(r.offset & (N - 1)), r.len) })
        };

        compiler_fence(AcqRel);

        if r.offset + N < self.reserved.load(Relaxed) {
            return Err(DataStoreError::StaleData);
        }

        Ok(result)
    }

    #[inline]
    pub fn write(&self, src: &[u8]) -> Result<DataStoreRef, DataStoreError> {
        if src.len() > N {
            return Err(DataStoreError::DataLenExceedsCapacity(src.len(), N));
        }

        let from = self.reserved.fetch_add(src.len().next_multiple_of(Self::CACHELINE), Relaxed);
        let from_ix = from & (N - 1);
        let base = self.data.get().cast::<u8>();
        let head_len = (N - from_ix).min(src.len());
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), base.add(from_ix), head_len);
            let tail_len = src.len() - head_len;
            if tail_len > 0 {
                std::ptr::copy_nonoverlapping(src[head_len..].as_ptr(), base, tail_len);
            }
        }

        Ok(DataStoreRef { offset: from, len: src.len() })
    }
}

impl<const N: usize> Default for DataStore<N> {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl<const N: usize> Sync for DataStore<N> {}
unsafe impl<const N: usize> Send for DataStore<N> {}

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
        let dc: DataStore<64> = DataStore::new();
        let r = dc.write(b"hello").unwrap();
        let mut buf = [0u8; 5];
        dc.read(r, &mut buf).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn wrap_around() {
        // First write (10B) bumps reserved by 64 → from_ix for second write = 64.
        // Second write (80B): from_ix=64, head_len=64, tail_len=16 → crosses N
        // boundary.
        let dc: DataStore<128> = DataStore::new();
        let _ = dc.write(&[0xAA; 10]).unwrap();
        let r = dc.write(&[0xBB; 80]).unwrap();
        assert_eq!(r.offset, 64);
        let mut buf = [0u8; 80];
        dc.read(r, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn map_contiguous() {
        let dc: DataStore<64> = DataStore::new();
        let r = dc.write(b"hello").unwrap();
        // from_ix=0, 0+5 <= 64 → zero-copy path, C unused
        let got = dc.map::<_, _, 64>(r, |s| s.to_vec()).unwrap();
        assert_eq!(got, b"hello");
    }

    #[test]
    fn map_wrap() {
        // Same setup as wrap_around: second write straddles the N boundary.
        // map must copy into the scratch ArrayVec and present contiguous data.
        let dc: DataStore<128> = DataStore::new();
        let _ = dc.write(&[0xAA; 10]).unwrap();
        let r = dc.write(&[0xBB; 80]).unwrap();
        assert_eq!(r.offset, 64); // from_ix=64, 64+80=144>128 → wrap path
        let got = dc.map::<_, _, 80>(r, |s| s.to_vec()).unwrap();
        assert!(got.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn stale() {
        let dc: DataStore<64> = DataStore::new();
        let r = dc.write(b"AAAA").unwrap();
        let _ = dc.write(b"BBBB").unwrap();
        // reserved=128, r.offset(0)+N(64)=64 < 128 → stale
        let mut buf = [0u8; 4];
        assert!(matches!(dc.read(r, &mut buf), Err(DataStoreError::StaleData)));
    }

    #[test]
    fn len_too_large() {
        let dc: DataStore<64> = DataStore::new();
        assert!(matches!(
            dc.write(&[0u8; 65]),
            Err(DataStoreError::DataLenExceedsCapacity(65, 64))
        ));
        let r = dc.write(b"AB").unwrap();
        let mut small = [0u8; 1];
        assert!(matches!(dc.read(r, &mut small), Err(DataStoreError::BufferTooSmall(1, 2))));
    }

    #[test]
    fn cacheline_aligned_offsets() {
        let dc: DataStore<1024> = DataStore::new();
        let sizes = [1usize, 63, 64, 65, 127, 128];
        let mut expected = 0usize;
        for size in sizes {
            let r = dc.write(&vec![0u8; size]).unwrap();
            assert_eq!(r.offset, expected, "wrong offset for size {size}");
            assert_eq!(r.offset % 64, 0, "offset not cacheline-aligned for size {size}");
            expected += size.next_multiple_of(64);
        }
    }

    #[test]
    fn mpmc() {
        const WRITERS: usize = 4;
        const READERS: usize = 4;
        const PER: usize = 16;
        const MSG: usize = 8;

        // 4 writers × 16 msgs × next_multiple_of(64, 8)=64B each = 4096B reserved
        // total. N=4096 ensures oldest slot (offset=0, 0+4096=4096) is never
        // stale.
        let dc = Arc::new(DataStore::<4096>::new());
        let q: Arc<TestQueue<(DataStoreRef, [u8; MSG])>> = Arc::new(TestQueue::new());

        let write_handles: Vec<_> = (0..WRITERS)
            .map(|i| {
                let dc = Arc::clone(&dc);
                let q = Arc::clone(&q);
                thread::spawn(move || {
                    for j in 0..PER {
                        let payload = [(i * PER + j) as u8; MSG];
                        let r = dc.write(&payload).unwrap();
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
