use std::{
    cell::UnsafeCell,
    sync::atomic::{AtomicUsize, Ordering::*},
};

#[derive(Debug, Clone, Copy)]
pub struct DataStoreRef {
    pub offset: usize,
    pub len: usize,
}

#[derive(Debug)]
pub enum DataStoreError {
    DataLenExceedsCapacity(usize, usize),
    BufferTooSmall(usize, usize),
    StaleData,
}

impl std::error::Error for DataStoreError {}

impl std::fmt::Display for DataStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DataLenExceedsCapacity(n, cap) => {
                write!(f, "data length {n} exceeds capacity {cap}")
            }
            Self::BufferTooSmall(n, m) => write!(f, "output buffer {n} too small for {m} bytes"),
            Self::StaleData => f.write_str("referenced data store region was overwritten"),
        }
    }
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
    const _POW_2_CHECK: () = assert!(N.is_power_of_two());

    pub fn new() -> Self {
        let _ = Self::_POW_2_CHECK;
        Self { data: Box::new(UnsafeCell::new([0u8; N])), reserved: AtomicUsize::new(0) }
    }

    pub fn read(&self, r: DataStoreRef, buf: &mut [u8]) -> Result<(), DataStoreError> {
        if r.len > N {
            return Err(DataStoreError::DataLenExceedsCapacity(r.len, N));
        }
        if r.len > buf.len() {
            return Err(DataStoreError::BufferTooSmall(buf.len(), r.len));
        }

        if r.offset + N < self.reserved.load(Acquire) {
            return Err(DataStoreError::StaleData);
        }

        let base = self.data.get().cast::<u8>();
        let from_ix = r.offset % N;
        let head_len = (N - from_ix).min(r.len);
        unsafe {
            std::ptr::copy_nonoverlapping(base.add(from_ix), buf.as_mut_ptr(), head_len);
            let tail_len = r.len - head_len;
            if tail_len > 0 {
                std::ptr::copy_nonoverlapping(base, buf[head_len..].as_mut_ptr(), tail_len);
            }
        }

        if r.offset + N < self.reserved.load(Acquire) {
            return Err(DataStoreError::StaleData);
        }

        Ok(())
    }

    pub fn write(&self, src: &[u8]) -> Result<DataStoreRef, DataStoreError> {
        if src.len() > N {
            return Err(DataStoreError::DataLenExceedsCapacity(src.len(), N));
        }

        let from = self.reserved.fetch_add(src.len(), Release);
        let from_ix = from % N;
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
        let dc: DataStore<16> = DataStore::new();
        let r = dc.write(b"hello").unwrap();
        let mut buf = [0u8; 5];
        dc.read(r, &mut buf).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn wrap_around() {
        let dc: DataStore<8> = DataStore::new();
        let _ = dc.write(b"AAAAAA").unwrap();
        let r = dc.write(b"BBBB").unwrap();
        assert_eq!(r.offset, 6);
        let mut buf = [0u8; 4];
        dc.read(r, &mut buf).unwrap();
        assert_eq!(&buf, b"BBBB");
    }

    #[test]
    fn stale() {
        let dc: DataStore<4> = DataStore::new();
        let r = dc.write(b"AAAA").unwrap();
        let _ = dc.write(b"BBBB").unwrap();
        // reserved=8, r.offset(0)+N(4)=4 < 8 → stale
        let mut buf = [0u8; 4];
        assert!(matches!(dc.read(r, &mut buf), Err(DataStoreError::StaleData)));
    }

    #[test]
    fn len_too_large() {
        let dc: DataStore<4> = DataStore::new();
        assert!(matches!(dc.write(&[0u8; 5]), Err(DataStoreError::DataLenExceedsCapacity(5, 4))));
        let r = dc.write(b"AB").unwrap();
        let mut small = [0u8; 1];
        assert!(matches!(dc.read(r, &mut small), Err(DataStoreError::BufferTooSmall(1, 2))));
    }

    #[test]
    fn mpmc() {
        const WRITERS: usize = 4;
        const READERS: usize = 4;
        const PER: usize = 16;
        const MSG: usize = 8;

        let dc = Arc::new(DataStore::<1024>::new());
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
