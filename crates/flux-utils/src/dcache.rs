use std::{
    cell::UnsafeCell,
    sync::atomic::{AtomicUsize, Ordering::*},
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

/// Ring buffer data storage inspired by Firedancer's 'dcache'. Provides
/// synchronised multi-producer allocation and leaves read-write ordering and
/// synchronisation to the caller.
///
/// Intended use: the caller exchanges [`DCacheRef`]s through an external
/// ordered channel (e.g. a spine queue) and the channel's Release/Acquire
/// pair is the synchronisation point for the payload bytes stored here.
#[repr(C)]
pub struct DCache<const N: usize> {
    reserved: AtomicUsize,
    _pad: [u8; CACHELINE - size_of::<AtomicUsize>()],
    data: Box<UnsafeCell<[u8; N]>>,
}

const CACHELINE: usize = 64;

impl<const N: usize> DCache<N> {
    const OFFSET: usize = size_of::<usize>();

    pub fn new() -> Self {
        const { assert!(N.is_power_of_two() && N.is_multiple_of(CACHELINE)) };
        Self {
            reserved: AtomicUsize::new(0),
            _pad: [0; CACHELINE - size_of::<AtomicUsize>()],
            data: Box::new(UnsafeCell::new([0u8; N])),
        }
    }

    #[inline]
    pub fn read(&self, r: DCacheRef, buf: &mut [u8]) -> Result<(), DCacheError> {
        if r.len > N - Self::OFFSET {
            return Err(DCacheError::DataLenExceedsCapacity(r.len, N - Self::OFFSET));
        }
        if r.len > buf.len() {
            return Err(DCacheError::BufferTooSmall(buf.len(), r.len));
        }

        let base = self.data.get().cast::<u8>();
        let offset_ix = r.offset & (N - 1);

        if !Self::offsets_match(base, offset_ix, r.offset) {
            return Err(DCacheError::StaleData);
        }

        self.copy(base, r.len, r.offset + Self::OFFSET, buf);

        if !Self::offsets_match(base, offset_ix, r.offset) {
            return Err(DCacheError::StaleData);
        }

        Ok(())
    }

    #[inline]
    fn copy(&self, base: *mut u8, r_len: usize, r_offset: usize, buf: &mut [u8]) {
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

    /// Applies the function to the data referenced by DCacheRef.
    /// The user must be prepared to tolerate execution on stale / invalid data,
    /// otherwise they should prefer to use read instead
    #[inline]
    pub fn map<T, F, const C: usize>(&self, r: DCacheRef, f: F) -> Result<T, DCacheError>
    where
        F: FnOnce(&[u8]) -> T,
    {
        if r.len > N - Self::OFFSET {
            return Err(DCacheError::DataLenExceedsCapacity(r.len, N - Self::OFFSET));
        }

        let base = self.data.get().cast::<u8>();
        let offset_ix = r.offset & (N - 1);

        if !Self::offsets_match(base, offset_ix, r.offset) {
            return Err(DCacheError::StaleData);
        }

        let payload_ix = (r.offset + Self::OFFSET) & (N - 1);
        let result = if r.len > N - payload_ix {
            if r.len > C {
                return Err(DCacheError::DataLenExceedsCapacity(r.len, C));
            }
            let mut buf = [0u8; C];
            self.copy(base, r.len, r.offset + Self::OFFSET, &mut buf);
            f(&buf[..r.len])
        } else {
            f(unsafe { std::slice::from_raw_parts(base.add(payload_ix), r.len) })
        };

        if !Self::offsets_match(base, offset_ix, r.offset) {
            return Err(DCacheError::StaleData);
        }

        Ok(result)
    }

    #[inline]
    fn offsets_match(base: *mut u8, offset_ix: usize, expected_offset: usize) -> bool {
        let offset = unsafe { base.add(offset_ix).cast::<usize>().read_unaligned() };
        offset == expected_offset
    }

    #[inline]
    pub fn write<F>(&self, len: usize, f: F) -> Result<DCacheRef, DCacheError>
    where
        F: FnOnce(&mut [u8], &mut [u8]),
    {
        if Self::OFFSET + len > N {
            return Err(DCacheError::DataLenExceedsCapacity(len, N - Self::OFFSET));
        }

        let from =
            self.reserved.fetch_add((Self::OFFSET + len).next_multiple_of(CACHELINE), Relaxed);
        let from_ix = from & (N - 1);
        let base = self.data.get().cast::<u8>();

        // from_ix is cacheline-aligned so from_ix + Self::OFFSET <= N; no wrap for
        // header.
        unsafe { base.add(from_ix).cast::<usize>().write_unaligned(from) };

        let payload_ix = from_ix + Self::OFFSET;
        let head_len = (N - payload_ix).min(len);
        unsafe {
            let head = std::slice::from_raw_parts_mut(base.add(payload_ix), head_len);
            let tail = std::slice::from_raw_parts_mut(base, len - head_len);
            f(head, tail);
        }

        Ok(DCacheRef { offset: from, len })
    }
}

impl<const N: usize> Default for DCache<N> {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl<const N: usize> Sync for DCache<N> {}
unsafe impl<const N: usize> Send for DCache<N> {}

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
        let dc: DCache<64> = DCache::new();
        let r = dc.write(5, |head, _| head.copy_from_slice(b"hello")).unwrap();
        let mut buf = [0u8; 5];
        dc.read(r, &mut buf).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn wrap_around() {
        // First write (10B) bumps reserved by 64 → from_ix for second write = 64.
        // Second write (80B): from_ix=64, head_len=64, tail_len=16 → crosses N
        // boundary.
        let dc: DCache<128> = DCache::new();
        let _ = dc
            .write(10, |head, tail| {
                head.fill(0xAA);
                tail.fill(0xAA);
            })
            .unwrap();
        let r = dc
            .write(80, |head, tail| {
                head.fill(0xBB);
                tail.fill(0xBB);
            })
            .unwrap();
        assert_eq!(r.offset, 64);
        let mut buf = [0u8; 80];
        dc.read(r, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn map_contiguous() {
        let dc: DCache<64> = DCache::new();
        let r = dc.write(5, |head, _| head.copy_from_slice(b"hello")).unwrap();
        // from_ix=0, 0+5 <= 64 → zero-copy path, C unused
        let got = dc.map::<_, _, 64>(r, |s| s.to_vec()).unwrap();
        assert_eq!(got, b"hello");
    }

    #[test]
    fn map_wrap() {
        // Same setup as wrap_around: second write straddles the N boundary.
        // map must copy into the scratch ArrayVec and present contiguous data.
        let dc: DCache<128> = DCache::new();
        let _ = dc
            .write(10, |head, tail| {
                head.fill(0xAA);
                tail.fill(0xAA);
            })
            .unwrap();
        let r = dc
            .write(80, |head, tail| {
                head.fill(0xBB);
                tail.fill(0xBB);
            })
            .unwrap();
        assert_eq!(r.offset, 64); // from_ix=64, payload_ix=72, 72+80=152>128 → wrap path
        let got = dc.map::<_, _, 80>(r, |s| s.to_vec()).unwrap();
        assert!(got.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn stale() {
        let dc: DCache<64> = DCache::new();
        let r = dc.write(4, |head, _| head.copy_from_slice(b"AAAA")).unwrap();
        let _ = dc.write(4, |head, _| head.copy_from_slice(b"BBBB")).unwrap();
        // Second write at from_ix=0 overwrites header[0] with 64 != r.offset(0) → stale
        let mut buf = [0u8; 4];
        assert!(matches!(dc.read(r, &mut buf), Err(DCacheError::StaleData)));
    }

    #[test]
    fn len_too_large() {
        let dc: DCache<64> = DCache::new();
        assert!(matches!(
            dc.write(57, |_, _| {}),
            Err(DCacheError::DataLenExceedsCapacity(57, 56)) // N(64) - Self::OFFSET(8)
        ));
        let r = dc.write(2, |head, _| head.copy_from_slice(b"AB")).unwrap();
        let mut small = [0u8; 1];
        assert!(matches!(dc.read(r, &mut small), Err(DCacheError::BufferTooSmall(1, 2))));
    }

    #[test]
    fn cacheline_aligned_offsets() {
        let dc: DCache<1024> = DCache::new();
        let sizes = [1usize, 63, 64, 65, 127, 128];
        let mut expected = 0usize;
        for size in sizes {
            let r = dc.write(size, |_, _| {}).unwrap();
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

        // 4 writers × 16 msgs × next_multiple_of(64, 8)=64B each = 4096B reserved
        // total. N=4096 ensures oldest slot (offset=0, 0+4096=4096) is never
        // stale.
        let dc = Arc::new(DCache::<4096>::new());
        let q: Arc<TestQueue<(DCacheRef, [u8; MSG])>> = Arc::new(TestQueue::new());

        let write_handles: Vec<_> = (0..WRITERS)
            .map(|i| {
                let dc = Arc::clone(&dc);
                let q = Arc::clone(&q);
                thread::spawn(move || {
                    for j in 0..PER {
                        let payload = [(i * PER + j) as u8; MSG];
                        let r = dc
                            .write(MSG, |head, tail| {
                                head.copy_from_slice(&payload[..head.len()]);
                                tail.copy_from_slice(&payload[head.len()..]);
                            })
                            .unwrap();
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
