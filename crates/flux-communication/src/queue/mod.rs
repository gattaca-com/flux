use std::{
    alloc::Layout,
    borrow::Borrow,
    collections::HashMap,
    mem::size_of,
    ops::Deref,
    path::Path,
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

fn broadcast_id_for(label: &str, queue: &str) -> usize {
    static COUNTERS: OnceLock<Mutex<HashMap<(String, String), usize>>> = OnceLock::new();
    let mut map = COUNTERS.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
    let id = map.entry((label.to_owned(), queue.to_owned())).or_insert(0);
    let result = *id;
    *id += 1;
    result
}

fn binary_name() -> &'static str {
    static NAME: OnceLock<String> = OnceLock::new();
    NAME.get_or_init(|| {
        std::env::current_exe()
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
            .unwrap_or_else(|| "unknown".to_owned())
    })
}

use flux_utils::{ArrayStr, safe_panic};
use rand::Rng;
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

pub const MAX_GROUPS: usize = 32;
pub const GROUP_LABEL_LEN: usize = 64;

#[derive(Debug)]
#[repr(C, align(64))]
pub struct AlignedCursor {
    pub cursor: AtomicUsize,
}

#[derive(Debug)]
#[repr(C, align(64))]
pub struct QueueHeader {
    pub queue_type: QueueType, // 1
    is_initialized: u8,        // 2
    _pad1: [u8; 6],            // 8
    pub elsize: usize,         // 16
    pub mask: usize,           // 24
    pub count: AtomicUsize,    /* 32 */

    group_labels: [ArrayStr<GROUP_LABEL_LEN>; MAX_GROUPS],
    group_cursors: [AlignedCursor; MAX_GROUPS],
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

    pub fn find_or_insert_group(&mut self, key: &str) -> *const AtomicUsize {
        let key = ArrayStr::<GROUP_LABEL_LEN>::from_str_truncate(key);

        // TODO: use better fix of potential data race, either spinlock or something
        // similar to what is done in the collaborative consume
        std::thread::sleep(Duration::from_micros(rand::rng().random_range(0..10)));

        for i in 0..MAX_GROUPS {
            if self.group_labels[i] == key {
                return &raw const self.group_cursors[i].cursor;
            }
        }
        for i in 0..MAX_GROUPS {
            if self.group_labels[i] == ArrayStr::<GROUP_LABEL_LEN>::new() {
                self.group_labels[i] = key;
                return &raw const self.group_cursors[i].cursor;
            }
        }

        panic!("no group slots available (max {} groups)", MAX_GROUPS);
    }

    /// Returns all non-empty consumer group slots as `(label, cursor_value)`
    /// pairs.
    ///
    /// Queues created by older flux versions may not have the group_labels
    /// region initialised — the raw `ArrayStr::len` field will contain
    /// garbage.  We detect this (len > GROUP_LABEL_LEN) and bail early
    /// with an empty vec instead of letting `from_raw_parts` abort.
    pub fn active_groups(&self) -> Vec<(&str, usize)> {
        let mut out = Vec::new();
        for i in 0..MAX_GROUPS {
            let label = &self.group_labels[i];

            // Guard: if the stored length exceeds the fixed-size buffer the
            // memory is uninitialised / from an incompatible header layout.
            if label.len() > GROUP_LABEL_LEN {
                return out;
            }

            if !label.is_empty() {
                let label = label.as_str();
                let cursor = self.group_cursors[i].cursor.load(Ordering::Relaxed);
                out.push((label, cursor));
            }
        }
        out
    }
}

//TODO @lopo: this should in reality really also implement drop and most likely
// return an Arc instead of &'static or whatever.
#[repr(C, align(64))]
pub struct InnerQueue<T> {
    pub(crate) header: QueueHeader,
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
    pub(crate) fn load(&self, pos: usize) -> &Seqlock<T> {
        unsafe { self.buffer.get_unchecked(pos) }
    }

    #[inline]
    fn last_count(&self) -> usize {
        self.count().saturating_sub(1)
    }

    #[inline]
    pub(crate) fn version_at(&self, count: usize) -> u64 {
        ((count / self.len()) * 2 + 2) as u64
    }

    #[inline]
    pub fn count_at(&self, pos: usize, version: u64) -> usize {
        ((version as usize - 2) / 2) * self.len() + (pos & self.header.mask)
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
        let shmem_file = shmem_file.as_ref();
        Self { inner: InnerQueue::create_or_open_shared(shmem_file, len, queue_type) }
    }

    pub fn open_shared<P: AsRef<Path>>(shmem_file: P) -> Self {
        let shmem_file = shmem_file.as_ref();
        Self {
            inner: InnerQueue::open_shared(shmem_file)
                .expect("Couldn't open shared queue, was it initialized?"),
        }
    }

    fn group_cursor(&self, key: &str) -> *const AtomicUsize {
        unsafe { &mut *(self.inner as *mut InnerQueue<T>) }.header.find_or_insert_group(key)
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
    pos: usize,                 // 8
    mask: usize,                // 16
    expected_version: u64,      // 24
    is_running: u8,             // 25
    _pad: [u8; 7],              // 32
    cursor: *const AtomicUsize, // 40
    label: &'static str,        // 56 (ptr + len)
    queue: Queue<T>,            // 64 fat ptr: (usize, pointer)
}

unsafe impl<T> Send for ConsumerBare<T> {}

impl<T: Copy> ConsumerBare<T> {
    pub fn new(queue: Queue<T>, label: &'static str) -> Self {
        Self {
            pos: usize::MAX,
            mask: queue.header.mask,
            expected_version: 0,
            is_running: 1,
            _pad: [0; 7],
            cursor: std::ptr::null(),
            label,
            queue,
        }
    }

    #[cfg(test)]
    pub fn new_broadcast_test(queue: Queue<T>) -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);

        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let label = std::boxed::Box::leak(format!("{}", id).into_boxed_str());

        let mut s = Self::new(queue, label);
        s.try_init_broadcast();
        s
    }

    #[cfg(test)]
    pub fn new_collaborative_test(queue: Queue<T>, label: &'static str) -> Self {
        let mut s = Self::new(queue, label);
        s.try_init_collaborative();
        s
    }

    #[inline]
    fn get_pos(&self, count: usize) -> usize {
        count & self.mask
    }

    pub fn set_collaborative_cursor(&mut self, cursor: *const AtomicUsize) {
        self.cursor = cursor;
    }

    #[inline]
    pub fn recover_after_error(&mut self) {
        self.set_broadcast_pos(self.queue.count());
    }

    #[inline]
    fn set_pos(&mut self, count: usize) {
        self.pos = self.get_pos(count);
        self.expected_version = self.queue.version_at(count);
    }

    #[inline]
    fn set_broadcast_pos(&mut self, count: usize) {
        self.set_pos(count);
        unsafe { &*self.cursor }.store(count + 1, Ordering::Relaxed);
    }

    #[inline]
    fn acquire_specific_slot(&mut self, delta: usize) {
        let cursor = unsafe { &*self.cursor }.fetch_add(delta, Ordering::Relaxed);
        self.set_pos(cursor + delta - 1);
    }

    #[inline]
    fn acquire_next_slot(&mut self) {
        self.acquire_specific_slot(1);
    }

    #[inline]
    fn acquire_earliest_available_slot(&mut self) {
        let collaborative_cursor = unsafe { &*self.cursor };
        let delta = self
            .queue
            .count()
            .saturating_sub(collaborative_cursor.load(Ordering::Relaxed) + self.queue.len())
            .max(1);

        self.acquire_specific_slot(delta);
    }

    #[inline]
    fn try_init_broadcast(&mut self) {
        if self.cursor.is_null() {
            self.init_broadcast();
        }
    }

    #[inline(never)]
    fn init_broadcast(&mut self) {
        let id = broadcast_id_for(self.label, std::any::type_name::<T>());
        let id = if id == 0 { "" } else { &format!(".{}", id) };

        self.cursor =
            self.queue.group_cursor(&format!("{}.{}{}.broadcast", binary_name(), self.label, id));

        // Always set current producer position without restoring value from cursor
        self.set_broadcast_pos(self.queue.count());
    }

    #[inline]
    pub fn try_init_collaborative(&mut self) {
        if self.cursor.is_null() {
            self.init_collaborative();
        }
    }

    #[inline(never)]
    fn init_collaborative(&mut self) {
        self.cursor = self.queue.group_cursor(&format!("{}.{}.collab", binary_name(), self.label));
        self.acquire_next_slot();
    }

    /// Nonblocking consume returning either Ok(()) or a ReadError
    #[inline]
    pub fn try_consume(&mut self, el: &mut T) -> Result<(), ReadError> {
        self.try_init_broadcast();
        self.queue.consume(el, self.pos, self.expected_version)?;
        self.acquire_next_slot();
        Ok(())
    }

    /// Like `try_consume` but also returns `(slot_pos, slot_version)` for the
    /// consumed slot.
    #[inline]
    pub fn try_consume_with_epoch(&mut self, el: &mut T) -> Result<(usize, u64), ReadError> {
        self.try_init_broadcast();
        let slot_pos = self.pos;
        let slot_ver = self.expected_version;
        self.queue.consume(el, slot_pos, slot_ver)?;
        self.acquire_next_slot();
        Ok((slot_pos, slot_ver))
    }

    #[inline]
    pub fn slot_version(&self, slot_pos: usize) -> u64 {
        self.queue.load(slot_pos).version.load(Ordering::Acquire)
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

    pub fn set_collaborative_group(&mut self, group_label: &'static str) {
        self.label = group_label;
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn init_header(consumer_ptr: *mut ConsumerBare<T>, queue: Queue<T>, label: &'static str) {
        unsafe {
            (*consumer_ptr).pos = usize::MAX;
            (*consumer_ptr).expected_version = 0;
            (*consumer_ptr).mask = queue.header.mask;
            (*consumer_ptr).cursor = std::ptr::null();
            (*consumer_ptr).label = label;
            (*consumer_ptr).queue = queue;
        }
    }

    #[inline]
    pub fn queue_message_count(&self) -> usize {
        (*self.queue).count()
    }

    #[inline]
    fn try_consume_last(&mut self, message: &mut T) -> Result<(), EmptyError> {
        self.try_init_broadcast();
        let last_count = self.queue.last_count();
        if last_count < self.queue.count_at(self.pos, self.expected_version) {
            return Err(EmptyError::Empty);
        }

        self.queue.consume_always(message, self.get_pos(last_count))?;
        self.set_broadcast_pos(last_count + 1);
        Ok(())
    }
}

impl<T> AsMut<ConsumerBare<T>> for ConsumerBare<T> {
    fn as_mut(&mut self) -> &mut ConsumerBare<T> {
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Consumer<T: 'static + Copy> {
    consumer: ConsumerBare<T>,
    message: T,
    should_log: bool,
}

impl<T: 'static + Copy> Consumer<T> {
    #[allow(clippy::uninit_assumed_init)]
    fn from_bare(consumer: ConsumerBare<T>) -> Self {
        Self {
            consumer,
            message: unsafe { std::mem::MaybeUninit::uninit().assume_init() },
            should_log: true,
        }
    }

    pub fn new(queue: Queue<T>, label: &'static str) -> Self {
        Self::from_bare(ConsumerBare::new(queue, label))
    }

    #[cfg(test)]
    pub fn new_broadcast_test(queue: Queue<T>) -> Self {
        Self::from_bare(ConsumerBare::new_broadcast_test(queue))
    }

    #[cfg(test)]
    pub fn new_collaborative_test(queue: Queue<T>, label: &'static str) -> Self {
        Self::from_bare(ConsumerBare::new_collaborative_test(queue, label))
    }

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
    pub fn consume_collaborative<F>(&mut self, mut f: F) -> bool
    where
        F: FnMut(&mut T),
    {
        self.consumer.try_init_collaborative();

        match self
            .consumer
            .queue
            .load(self.consumer.pos)
            .read_with_version(&mut self.message, self.consumer.expected_version)
        {
            Ok(()) => {
                f(&mut self.message);
                self.consumer.acquire_next_slot();
                return true;
            }
            Err(ReadError::Empty) => return false,
            Err(ReadError::SpedPast) => {
                if self.should_log {
                    safe_panic!(
                        "Consumer<{}> collaborative got sped past.",
                        std::any::type_name::<T>()
                    );
                }
                self.consumer.acquire_earliest_available_slot();
                return false;
            }
        }
    }

    /// Consumes one message, returning `(&message, slot_pos, slot_ver)`.
    #[inline]
    pub fn try_consume_with_epoch(&mut self) -> Result<(&T, usize, u64), ReadError> {
        let (pos, ver) = self.consumer.try_consume_with_epoch(&mut self.message)?;
        Ok((&self.message, pos, ver))
    }

    #[inline]
    pub fn slot_version(&self, slot_pos: usize) -> u64 {
        self.consumer.slot_version(slot_pos)
    }

    #[inline]
    pub fn recover_after_error(&mut self) {
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

    pub fn set_collaborative_group(&mut self, group_label: &'static str) {
        self.consumer.set_collaborative_group(group_label);
    }
}

#[cfg(test)]
mod tests_basic;

#[cfg(test)]
mod tests_collaborative;
