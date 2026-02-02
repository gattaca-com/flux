use std::{
    alloc::Layout,
    borrow::Borrow,
    mem::{MaybeUninit, size_of},
    ops::Deref,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use flux_utils::safe_panic;
use shared_memory::ShmemConf;

use crate::{
    Seqlock,
    error::{EmptyError, QueueError, ReadError},
};

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum QueueType {
    Unknown,
    MPMC,
    SPMC,
}

#[derive(Debug)]
#[repr(C, align(64))]
struct QueueHeader {
    queue_type: QueueType, // 1
    is_initialized: u8,    // 2
    _pad1: [u8; 6],        // 8
    elsize: usize,         // 16
    mask: usize,           // 24
    count: AtomicUsize,    /* 32 */

                           /* add queue hash mismatch */
}
#[allow(dead_code)]
impl QueueHeader {
    /// in bytes
    pub fn size_of(&self) -> usize {
        (self.mask + 1) * self.elsize
    }

    pub fn len(&self) -> usize {
        self.mask + 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn from_ptr(ptr: *mut u8) -> &'static mut Self {
        unsafe { &mut *(ptr as *mut Self) }
    }

    pub fn is_initialized(&self) -> bool {
        self.is_initialized == 1
    }

    pub fn elsize(&self) -> usize {
        self.elsize
    }

    pub fn open_shared<S: AsRef<Path>>(path: S) -> Result<&'static mut Self, QueueError> {
        let path = path.as_ref();
        let shmem = ShmemConf::new().flink(path).open()?;
        let ptr = shmem.as_ptr();
        std::mem::forget(shmem);
        Ok(Self::from_ptr(ptr))
    }
}

//TODO @lopo: this should in reality really also implement drop and most likely
// return an Arc instead of &'static or whatever.
#[repr(C, align(64))]
pub struct InnerQueue<T> {
    header: QueueHeader,
    buffer: [Seqlock<T>],
}

impl<T: Copy> InnerQueue<T> {
    /// Allocs (unshared) memory and initializes a new queue from it.
    ///     QueueType::MPMC = multi producer multi consumer
    ///     QueueType::SPMC = single producer multi consumer
    fn new(len: usize, queue_type: QueueType) -> *const Self {
        let real_len = len.next_power_of_two();
        let size = size_of::<QueueHeader>() + real_len * size_of::<Seqlock<T>>();

        unsafe {
            let ptr = std::alloc::alloc_zeroed(
                Layout::array::<u8>(size).unwrap().align_to(64).unwrap().pad_to_align(),
            );
            // Why real len you may ask. The size of the fat pointer ONLY includes the
            // length of the unsized part of the struct i.e. the buffer.
            Self::from_uninitialized_ptr(ptr, real_len, queue_type)
        }
    }

    const fn size_of(len: usize) -> usize {
        size_of::<QueueHeader>() + len.next_power_of_two() * size_of::<Seqlock<T>>()
    }

    fn from_uninitialized_ptr(ptr: *mut u8, len: usize, queue_type: QueueType) -> *const Self {
        unsafe {
            let q = std::ptr::slice_from_raw_parts_mut(ptr, len) as *mut Self;
            let elsize = size_of::<Seqlock<T>>();
            let mask = len - 1;

            (*q).header.queue_type = queue_type;
            (*q).header.mask = mask;
            (*q).header.elsize = elsize;
            (*q).header.is_initialized = true as u8;
            (*q).header.count = AtomicUsize::new(0);
            q
        }
    }

    #[allow(dead_code)]
    fn from_initialized_ptr(ptr: *mut QueueHeader) -> Result<*const Self, QueueError> {
        unsafe {
            let len = (*ptr).mask + 1;
            if !len.is_power_of_two() {
                return Err(QueueError::LengthNotPowerOfTwo);
            }
            if (*ptr).is_initialized != true as u8 {
                return Err(QueueError::UnInitialized);
            }
            // TODO @lopo: I think this is slightly wrong
            Ok(std::ptr::slice_from_raw_parts_mut(ptr as *mut Seqlock<T>, len) as *const Self)
        }
    }

    // Note: Calling any fns below this comment (fns that touch `count`) from
    // anywhere that's not a producer -> false sharing

    #[inline]
    pub fn count(&self) -> usize {
        self.header.count.load(Ordering::Relaxed)
    }

    #[inline]
    fn next_count(&self) -> usize {
        match self.header.queue_type {
            QueueType::Unknown => panic!("Unknown queue"),
            QueueType::MPMC => self.header.count.fetch_add(1, Ordering::AcqRel),
            QueueType::SPMC => {
                let c = self.header.count.load(Ordering::Relaxed);
                self.header.count.store(c.wrapping_add(1), Ordering::Relaxed);
                c
            }
        }
    }

    #[inline]
    fn load(&self, pos: usize) -> &Seqlock<T> {
        unsafe { self.buffer.get_unchecked(pos) }
    }

    #[inline]
    fn cur_pos(&self) -> usize {
        self.count() & self.header.mask
    }

    #[inline]
    fn last_pos(&self) -> usize {
        (self.count().saturating_sub(1)) & self.header.mask
    }

    #[inline]
    fn version(&self) -> u64 {
        (((self.count() / (self.header.mask + 1)) << 1) + 2) as u64
    }

    #[inline]
    fn last_version(&self) -> u64 {
        ((((self.count().saturating_sub(1)) / (self.header.mask + 1)) << 1) + 2) as u64
    }

    #[allow(dead_code)]
    fn version_of(&self, pos: usize) -> u64 {
        self.load(pos).version()
    }

    #[inline]
    fn produce(&self, item: &T) -> usize {
        let next_count = self.next_count();
        let lock = self.load(next_count & self.header.mask);
        lock.write(item);
        next_count
    }

    #[inline]
    fn consume(&self, el: &mut T, ri: usize, ri_ver: u64) -> Result<(), ReadError> {
        self.load(ri).read_with_version(el, ri_ver)
    }

    #[inline]
    fn consume_always(&self, el: &mut T, ri: usize) -> Result<(), EmptyError> {
        self.load(ri).read(el)
    }

    #[inline]
    fn len(&self) -> usize {
        self.header.mask + 1
    }

    // This exists just to check the state of the queue for debugging purposes
    #[allow(dead_code)]
    fn verify(&self) {
        let mut prev_v = self.load(0).version();
        let mut n_changes = 0;
        for i in 1..=self.header.mask {
            let lck = self.load(i);
            let v = lck.version();
            if v & 1 == 1 {
                panic!("odd version at {i}: {prev_v} -> {v}");
            }
            if v != prev_v && v & 1 == 0 {
                n_changes += 1;
                println!("version change at {i}: {prev_v} -> {v}");
                prev_v = v;
            }
        }
        if n_changes > 1 {
            panic!("what")
        }
    }

    #[inline]
    fn produce_first(&self, item: &T) -> usize {
        match self.header.queue_type {
            QueueType::Unknown => panic!("Unknown queue"),
            QueueType::MPMC => self.produce(item),
            QueueType::SPMC => {
                let m = self.header.mask;
                let c = self.count();
                let p = c & m;
                let lock = self.load(p);
                if lock.version() & 1 == 1 {
                    lock.write_unpoison(item);
                    p
                } else {
                    self.produce(item)
                }
            }
        }
    }

    #[inline]
    pub fn n_slots(&self) -> usize {
        self.header.len()
    }

    fn is_poisoned(&self) -> Option<usize> {
        // We assume that nothing would take longer than 10 micros to be written.
        // If it does that means that nothing will actually be written and the queue is
        // poisoned.
        const MAX_CHECK_TIME: Duration = Duration::from_micros(10);
        for i in 0..=self.header.mask {
            let lck = self.load(i);

            if lck.version() & 1 == 0 {
                continue;
            }
            let curt = Instant::now();
            while lck.version() & 1 == 1 {
                if curt.elapsed() > MAX_CHECK_TIME {
                    return Some(i);
                }
            }
        }
        None
    }

    fn validate(&self, len: usize) -> Result<(), QueueError> {
        let elsize = std::mem::size_of::<Seqlock<T>>();
        if self.header.len() < len {
            return Err(QueueError::TooSmall);
        }
        if !self.header.len().is_power_of_two() {
            return Err(QueueError::LengthNotPowerOfTwo);
        }
        if self.header.elsize != elsize {
            return Err(QueueError::ElementSizeChanged(self.header.elsize, elsize));
        };
        if let Some(poisoned_element) = self.is_poisoned() {
            return Err(QueueError::ElementPoisoned(poisoned_element));
        }
        Ok(())
    }

    fn create_or_open_shared<P: AsRef<Path>>(
        shmem_file: P,
        mut len: usize,
        typ: QueueType,
    ) -> *const Self {
        use shared_memory::{ShmemConf, ShmemError};
        let _ = std::fs::create_dir_all(shmem_file.as_ref().parent().unwrap());
        len = len.next_power_of_two();
        match ShmemConf::new().size(Self::size_of(len)).flink(&shmem_file).create() {
            Ok(shmem) => {
                let ptr = shmem.as_ptr();
                std::mem::forget(shmem);
                Self::from_uninitialized_ptr(ptr, len, typ)
            }
            Err(ShmemError::LinkExists) => {
                let Ok(v) = Self::open_shared(&shmem_file).inspect_err(|e| {
                    tracing::warn!("issue opening preexisting shmem at {:?}: {e}. Removing the link file and recreating.", shmem_file.as_ref())
                }) else {
                    let _ = std::fs::remove_file(&shmem_file);
                    return Self::create_or_open_shared(shmem_file, len, typ);
                };
                if let Err(e) = unsafe { (&*v).validate(len) } {
                    tracing::error!(
                        "issue with preexisting shmem at {:?}: {e}. Removing the link file and recreating. Should probably upgrade and reaattach any other processes.",
                        shmem_file.as_ref()
                    );
                    let _ = std::fs::remove_file(&shmem_file);
                    return Self::create_or_open_shared(shmem_file, len, typ);
                }
                v
            }
            Err(e) => panic!("{e}"),
        }
    }

    fn open_shared<S: AsRef<Path>>(shmem_file: S) -> Result<*const Self, QueueError> {
        if !shmem_file.as_ref().exists() {
            return Err(QueueError::NonExistingFile);
        }
        let mut tries = 0;
        let mut header = QueueHeader::open_shared(shmem_file.as_ref())?;
        while !header.is_initialized() {
            // This is to handle the case where two people are trying to initialize the same
            // queue
            std::thread::sleep(std::time::Duration::from_millis(1));
            header = QueueHeader::open_shared(shmem_file.as_ref())?;
            if tries == 10 {
                return Err(QueueError::UnInitialized);
            }
            tries += 1;
        }
        Self::from_initialized_ptr(header)
    }
}

unsafe impl<T> Send for InnerQueue<T> {}
unsafe impl<T> Sync for InnerQueue<T> {}

impl<T: std::fmt::Debug> std::fmt::Debug for InnerQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Queue:\nHeader:\n{:?}", self.header)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Queue<T> {
    inner: *const InnerQueue<T>,
}

impl<T: Copy> Queue<T> {
    pub fn new(len: usize, queue_type: QueueType) -> Self {
        Self { inner: InnerQueue::new(len, queue_type) }
    }

    pub fn create_or_open_shared<P: AsRef<Path>>(
        shmem_file: P,
        len: usize,
        queue_type: QueueType,
    ) -> Self {
        Self { inner: InnerQueue::create_or_open_shared(shmem_file, len, queue_type) }
    }

    pub fn open_shared<P: AsRef<Path>>(shmem_file: P) -> Self {
        Self {
            inner: InnerQueue::open_shared(shmem_file)
                .expect("Couldn't open shared queue, was it initialized?"),
        }
    }
}

unsafe impl<T> Send for Queue<T> {}
unsafe impl<T> Sync for Queue<T> {}

impl<T> Borrow<InnerQueue<T>> for Queue<T> {
    fn borrow(&self) -> &InnerQueue<T> {
        unsafe { &*self.inner }
    }
}

impl<T> Deref for Queue<T> {
    type Target = InnerQueue<T>;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner }
    }
}

impl<T> AsRef<InnerQueue<T>> for Queue<T> {
    fn as_ref(&self) -> &InnerQueue<T> {
        unsafe { &*self.inner }
    }
}

/// Simply exists for the automatic produce_first
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Producer<T> {
    pub produced_first: u8, // 1
    pub queue: Queue<T>,
}
impl<T: Copy> Default for Producer<T> {
    fn default() -> Self {
        let dummy_queue = Queue::new(2, QueueType::MPMC);
        dummy_queue.into()
    }
}

impl<T: Copy> From<Queue<T>> for Producer<T> {
    fn from(queue: Queue<T>) -> Self {
        Self { produced_first: 0, queue }
    }
}

impl<T: Copy> Producer<T> {
    pub fn produce(&mut self, msg: &T) -> usize {
        if self.produced_first == 0 {
            self.produced_first = 1;
            self.queue.produce_first(msg)
        } else {
            self.queue.produce(msg)
        }
    }

    pub fn produce_without_first(&self, msg: &T) -> usize {
        self.queue.produce(msg)
    }
}

impl<T> AsMut<Producer<T>> for Producer<T> {
    fn as_mut(&mut self) -> &mut Producer<T> {
        self
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ConsumerBare<T> {
    pos: usize,            // 8
    mask: usize,           // 16
    expected_version: u64, // 24
    is_running: u8,        // 25
    _pad: [u8; 11],        // 36
    queue: Queue<T>,       // 56 fat ptr: (usize, pointer)
}

impl<T: Copy> ConsumerBare<T> {
    #[inline]
    pub fn recover_after_error(&mut self) {
        self.expected_version += 2;
    }

    #[inline]
    fn update_pos(&mut self) {
        self.pos = (self.pos + 1) & self.mask;
        self.expected_version = self.expected_version.wrapping_add(2 * (self.pos == 0) as u64);
    }

    /// Nonblocking consume returning either Ok(()) or a ReadError
    #[inline]
    pub fn try_consume(&mut self, el: &mut T) -> Result<(), ReadError> {
        self.queue.consume(el, self.pos, self.expected_version)?;
        self.update_pos();
        Ok(())
    }

    /// Blocking consume
    #[inline]
    pub fn blocking_consume(&mut self, el: &mut T) {
        loop {
            match self.try_consume(el) {
                Ok(_) => {
                    return;
                }
                Err(ReadError::Empty) => {
                    #[cfg(target_arch = "x86_64")]
                    unsafe {
                        std::arch::x86_64::_mm_pause()
                    };
                    continue;
                }
                Err(ReadError::SpedPast) => {
                    self.recover_after_error();
                }
            }
        }
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn init_header(consumer_ptr: *mut ConsumerBare<T>, queue: Queue<T>) {
        unsafe {
            (*consumer_ptr).pos = queue.cur_pos();
            (*consumer_ptr).expected_version = queue.version();
            (*consumer_ptr).mask = queue.header.mask;
            (*consumer_ptr).queue = queue
        }
    }

    #[inline]
    pub fn queue_message_count(&self) -> usize {
        (*self.queue).count()
    }

    #[inline]
    fn try_consume_last(&mut self, message: &mut T) -> Result<(), EmptyError> {
        let last = self.queue.last_pos();
        if last < self.pos {
            return Err(EmptyError::Empty);
        }
        self.pos = self.queue.last_pos();
        self.expected_version = self.queue.last_version();
        self.queue.consume_always(message, self.pos)?;
        self.update_pos();
        Ok(())
    }
}

impl<T> AsMut<ConsumerBare<T>> for ConsumerBare<T> {
    fn as_mut(&mut self) -> &mut ConsumerBare<T> {
        self
    }
}

impl<T: Copy> From<Queue<T>> for ConsumerBare<T> {
    fn from(queue: Queue<T>) -> Self {
        let pos = queue.cur_pos();
        let expected_version = queue.version();
        Self { pos, mask: queue.header.mask, _pad: [0; 11], expected_version, is_running: 1, queue }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Consumer<T: 'static + Copy> {
    consumer: ConsumerBare<T>,
    message: T,
    should_log: bool,
}

impl<T: 'static + Copy> Consumer<T> {
    /// Maybe consume one message in a queue with error recovery and logging,
    /// and return whether one was read
    #[inline]
    pub fn consume<F>(&mut self, mut f: F) -> bool
    where
        F: FnMut(&mut T),
    {
        loop {
            match self.consumer.try_consume(&mut self.message) {
                Ok(()) => {
                    f(&mut self.message);
                    return true;
                }
                Err(ReadError::SpedPast) => {
                    self.log_and_recover();
                }
                Err(ReadError::Empty) => return false,
            }
        }
    }

    #[inline]
    pub fn consume_last<F>(&mut self, mut f: F) -> bool
    where
        F: FnMut(&mut T),
    {
        match self.consumer.try_consume_last(&mut self.message) {
            Ok(()) => {
                f(&mut self.message);
                true
            }
            Err(EmptyError::Empty) => false,
        }
    }

    #[inline(never)]
    fn log_and_recover(&mut self) {
        if self.should_log {
            safe_panic!(
                "Consumer<{}> got sped past. Lost {} messages",
                std::any::type_name::<T>(),
                self.consumer.queue.len()
            );
        }
        self.consumer.recover_after_error();
    }

    #[inline]
    pub fn without_log(self) -> Self {
        Self { should_log: false, ..self }
    }

    #[inline]
    pub fn queue_message_count(&self) -> usize {
        self.consumer.queue_message_count()
    }

    pub fn set_logging(&mut self, arg: bool) {
        self.should_log = arg
    }
}

impl<T: Copy, Q: Into<ConsumerBare<T>>> From<Q> for Consumer<T> {
    #[allow(clippy::uninit_assumed_init)]
    fn from(queue: Q) -> Self {
        Self {
            consumer: queue.into(),
            message: unsafe { MaybeUninit::uninit().assume_init() },
            should_log: true,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn headersize() {
        assert_eq!(64, std::mem::size_of::<QueueHeader>());
        assert_eq!(56, std::mem::size_of::<ConsumerBare<[u8; 60]>>())
    }

    #[test]
    fn basic() {
        for typ in [QueueType::SPMC, QueueType::MPMC] {
            let q = Queue::new(16, typ);
            let mut p = Producer::from(q);
            let mut c = ConsumerBare::from(q);
            p.produce(&1);
            let mut m = 0;

            assert_eq!(c.try_consume(&mut m), Ok(()));
            assert_eq!(m, 1);
            assert!(matches!(c.try_consume(&mut m), Err(ReadError::Empty)));
            for i in 0..16 {
                p.produce(&i);
            }
            for i in 0..16 {
                c.try_consume(&mut m).unwrap();
                assert_eq!(m, i);
            }

            assert!(matches!(c.try_consume(&mut m), Err(ReadError::Empty)));

            for _ in 0..20 {
                p.produce(&1);
            }

            assert!(matches!(c.try_consume(&mut m), Err(ReadError::SpedPast)));
        }
    }

    fn multithread(n_writers: usize, n_readers: usize, tot_messages: usize) {
        let q = Queue::new(16, QueueType::MPMC);

        let mut readhandles = Vec::new();
        for _ in 0..n_readers {
            let mut c1 = ConsumerBare::from(q);
            let cons = std::thread::spawn(move || {
                let mut c = 0;
                let mut m = 0;
                while c < tot_messages {
                    c1.blocking_consume(&mut m);
                    c += m;
                }
                assert_eq!(c, (0..tot_messages).sum::<usize>());
            });
            readhandles.push(cons)
        }
        let mut writehandles = Vec::new();
        for n in 0..n_writers {
            let mut p1 = Producer::from(q);
            let prod1 = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(20));
                let mut c = n;
                while c < tot_messages {
                    p1.produce(&c);
                    c += n_writers;
                    std::thread::yield_now();
                }
            });
            writehandles.push(prod1);
        }

        for h in readhandles {
            let _ = h.join();
        }
        for h in writehandles {
            let _ = h.join();
        }
    }
    #[test]
    fn multithread_1_2() {
        multithread(1, 2, 100000);
    }
    #[test]
    fn multithread_1_4() {
        multithread(1, 4, 100000);
    }
    #[test]
    fn multithread_2_4() {
        multithread(2, 4, 100000);
    }
    #[test]
    fn multithread_4_4() {
        multithread(4, 4, 100000);
    }
    #[test]
    fn multithread_8_8() {
        multithread(8, 8, 100000);
    }
    #[test]
    fn basic_shared() {
        for typ in [QueueType::SPMC, QueueType::MPMC] {
            let path = std::path::Path::new("/dev/shm/blabla_test");
            let _ = std::fs::remove_file(path);
            let q = Queue::create_or_open_shared(path, 16, typ);
            let mut p = Producer::from(q);
            let mut c = ConsumerBare::from(q);

            p.produce(&1);
            let mut m = 0;

            assert_eq!(c.try_consume(&mut m), Ok(()));
            assert_eq!(m, 1);
            assert!(matches!(c.try_consume(&mut m), Err(ReadError::Empty)));
            for i in 0..16 {
                p.produce(&i);
            }
            for i in 0..16 {
                c.try_consume(&mut m).unwrap();
                assert_eq!(m, i);
            }

            assert!(matches!(c.try_consume(&mut m), Err(ReadError::Empty)));

            for _ in 0..20 {
                p.produce(&1);
            }

            assert!(matches!(c.try_consume(&mut m), Err(ReadError::SpedPast)));
            let _ = std::fs::remove_file(path);
        }
    }
}
