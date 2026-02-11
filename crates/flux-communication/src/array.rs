use std::{alloc::Layout, borrow::Borrow, ops::Deref, path::Path};

use shared_memory::{ShmemConf, ShmemError};

use crate::{
    Seqlock,
    error::{EmptyError, QueueError, ReadError},
};

#[derive(Debug)]
#[repr(C, align(64))]
pub struct ArrayHeader {
    elsize: usize,
    bufsize: usize,
    is_initialized: u8,
}

impl ArrayHeader {
    pub fn from_ptr(ptr: *mut u8) -> &'static mut Self {
        unsafe { &mut *(ptr as *mut Self) }
    }

    pub fn open_shared<S: AsRef<Path>>(path: S) -> Result<&'static mut Self, ShmemError> {
        let path = path.as_ref();
        let _ = std::fs::create_dir_all(path);
        let shmem = ShmemConf::new()
            .flink(path)
            .open()?;
        let ptr = shmem.as_ptr();
        std::mem::forget(shmem);
        Ok(Self::from_ptr(ptr))
    }

    pub fn is_initialized(&self) -> bool {
        self.is_initialized != 0
    }
}

#[repr(C, align(64))]
pub struct InnerSeqlockArray<T> {
    header: ArrayHeader,
    buffer: [Seqlock<T>],
}
impl<T: Copy> InnerSeqlockArray<T> {
    fn new(len: usize) -> *const Self {
        // because we don't need len to be power of 2
        let size = Self::size_of(len);

        unsafe {
            let ptr = std::alloc::alloc_zeroed(
                Layout::array::<u8>(size).unwrap().align_to(64).unwrap().pad_to_align(),
            );
            Self::from_uninitialized_ptr(ptr, len)
        }
    }

    const fn size_of(len: usize) -> usize {
        std::mem::size_of::<ArrayHeader>() + len * std::mem::size_of::<Seqlock<T>>()
    }

    fn from_uninitialized_ptr(ptr: *mut u8, len: usize) -> *const Self {
        unsafe {
            // why len? because the size in the fat pointer ONLY cares about the unsized
            // part of the struct i.e. the length of the buffer
            let q = std::ptr::slice_from_raw_parts_mut(ptr as *mut Seqlock<T>, len) as *mut Self;
            let elsize = std::mem::size_of::<Seqlock<T>>();
            (*q).header.bufsize = len;
            (*q).header.elsize = elsize;
            (*q).header.is_initialized = 1;
            q
        }
    }

    #[allow(dead_code)]
    fn from_initialized_ptr(ptr: *mut ArrayHeader) -> *const Self {
        unsafe {
            let len = (*ptr).bufsize;
            // TODO @lopo: I think this is slightly wrong
            std::ptr::slice_from_raw_parts_mut(ptr as *mut Seqlock<T>, len) as *const Self
        }
    }

    //TODO @lopo: ErrorHandling
    #[inline]
    fn load(&self, pos: usize) -> &Seqlock<T> {
        unsafe { self.buffer.get_unchecked(pos) }
    }

    /// Use this is you are sure you are the only writer.
    pub fn write(&self, pos: usize, item: &T) {
        let lock = self.load(pos);
        lock.write(item);
    }

    pub fn reset_lock(&self, pos: usize) {
        let lock = self.load(pos);
        lock.reset()
    }

    /// Use this to write if there are multiple producers.
    pub fn write_multi_producer(&self, pos: usize, item: &T) {
        let lock = self.load(pos);
        lock.write_multi_producer(item);
    }

    pub fn read(&self, pos: usize, result: &mut T) -> Result<(), EmptyError> {
        let lock = self.load(pos);
        lock.read(result)
    }

    #[inline]
    pub fn view_unsafe(&self, pos: usize) -> Result<&mut T, ReadError> {
        self.load(pos).view_unsafe()
    }

    #[inline]
    pub fn read_copy(&self, pos: usize) -> Result<(T, u64), ReadError> {
        let lock = self.load(pos);
        lock.read_copy()
    }

    #[inline]
    pub fn read_copy_if_updated(
        &self,
        pos: usize,
        expected_version: u64,
    ) -> Result<(T, u64), ReadError> {
        let lock = self.load(pos);
        lock.read_copy_if_updated(expected_version)
    }

    pub fn len(&self) -> usize {
        self.header.bufsize
    }

    pub fn is_empty(&self) -> bool {
        self.header.bufsize == 0
    }

    pub fn version(&self, pos: usize) -> u64 {
        self.load(pos).version()
    }

    fn create_or_open_shared<P: AsRef<Path>>(
        shmem_flink: P,
        len: usize,
    ) -> Result<*const Self, QueueError> {
        use shared_memory::{ShmemConf, ShmemError};
        match ShmemConf::new().size(Self::size_of(len)).flink(&shmem_flink).create() {
            Ok(shmem) => {
                let ptr = shmem.as_ptr();
                std::mem::forget(shmem);
                Ok(Self::from_uninitialized_ptr(ptr, len))
            }
            Err(ShmemError::LinkExists) => {
                let v = match Self::open_shared(shmem_flink.as_ref()) {
                    Ok(v) => v,
                    Err(e) if shmem_flink.as_ref().exists() => {
                        tracing::warn!("There was an error opening {:?}, removing and recreating: {e}", shmem_flink.as_ref());
                        let _ = std::fs::remove_file(shmem_flink.as_ref());
                        return Self::create_or_open_shared(shmem_flink, len);
                    },
                    Err(e) => return Err(e)
                };
                if unsafe { (*v).header.bufsize } < len { Err(QueueError::TooSmall) } else { Ok(v) }
            }
            Err(e) => Err(e.into()),
        }
    }

    fn open_shared<S: AsRef<Path>>(shmem_file: S) -> Result<*const Self, QueueError> {
        let path = std::path::Path::new(shmem_file.as_ref());
        if !path.exists() {
            return Err(QueueError::NonExistingFile);
        }
        let header = ArrayHeader::open_shared(shmem_file.as_ref())?;
        if !header.is_initialized() {
            return Err(QueueError::UnInitialized);
        }
        Ok(Self::from_initialized_ptr(header))
    }

    fn clear(&self) {
        for i in 0..self.len() {
            self.reset_lock(i)
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for InnerSeqlockArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SeqlockVector:\nHeader:\n{:?}", self.header)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SeqlockArray<T> {
    inner: *const InnerSeqlockArray<T>,
}

impl<T: Copy> SeqlockArray<T> {
    pub fn new(len: usize) -> Self {
        Self { inner: InnerSeqlockArray::new(len) }
    }

    pub fn create_or_open_shared<P: AsRef<Path>>(
        shmem_file: P,
        len: usize,
    ) -> Result<Self, QueueError> {
        InnerSeqlockArray::create_or_open_shared(shmem_file, len).map(|inner| Self { inner })
    }

    pub fn open_shared<P: AsRef<Path>>(shmem_file: P) -> Result<Self, QueueError> {
        InnerSeqlockArray::open_shared(shmem_file).map(|inner| Self { inner })
    }

    pub fn iter(&self) -> VectorIterator<'_, T> {
        VectorIterator { vector: self, next_id: 0 }
    }

    pub fn clear(&self) {
        unsafe { (*self.inner).clear() }
    }
}

unsafe impl<T> Send for SeqlockArray<T> {}
unsafe impl<T> Sync for SeqlockArray<T> {}

impl<T> Borrow<InnerSeqlockArray<T>> for SeqlockArray<T> {
    fn borrow(&self) -> &InnerSeqlockArray<T> {
        unsafe { &*self.inner }
    }
}
impl<T> Deref for SeqlockArray<T> {
    type Target = InnerSeqlockArray<T>;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner }
    }
}
impl<T> AsRef<InnerSeqlockArray<T>> for SeqlockArray<T> {
    fn as_ref(&self) -> &InnerSeqlockArray<T> {
        unsafe { &*self.inner }
    }
}

pub struct VectorIterator<'a, T> {
    vector: &'a SeqlockArray<T>,
    next_id: usize,
}

impl<T: Copy + Default> Iterator for VectorIterator<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.next_id >= self.vector.len() {
            None
        } else {
            let out = self.vector.read_copy(self.next_id).ok()?;
            self.next_id = self.next_id.wrapping_add(1);
            Some(out.0)
        }
    }
}
