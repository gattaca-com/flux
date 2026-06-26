#[cfg(not(target_os = "linux"))]
use std::sync::Mutex;
use std::{
    cell::UnsafeCell,
    hint::spin_loop,
    mem::MaybeUninit,
    sync::atomic::{AtomicU32, AtomicUsize, Ordering},
};

use mio::Waker;

pub const MAX_SIGNAL_WAKERS: usize = 8;

struct WakerSlot {
    waker: UnsafeCell<MaybeUninit<Waker>>,
}

// SAFETY: a slot is written at most once before publication, then only read
// through shared references until `Signal` is dropped with exclusive access.
unsafe impl Sync for WakerSlot {}

impl WakerSlot {
    const fn uninit() -> Self {
        Self { waker: UnsafeCell::new(MaybeUninit::uninit()) }
    }

    fn write(&self, waker: Waker) {
        // SAFETY: each slot is reserved by a single successful `register_waker` call.
        unsafe {
            (*self.waker.get()).write(waker);
        }
    }

    unsafe fn get(&self) -> &Waker {
        // SAFETY: callers only read slots below `published_wakers`, which are
        // initialized and never mutated again.
        unsafe { &*(*self.waker.get()).as_ptr() }
    }

    unsafe fn drop_initialized(&mut self) {
        // SAFETY: callers only drop slots known to have been initialized.
        unsafe {
            self.waker.get_mut().assume_init_drop();
        }
    }
}

pub struct Signal {
    counter: AtomicU32,
    reserved_wakers: AtomicUsize,
    published_wakers: AtomicUsize,
    wakers: [WakerSlot; MAX_SIGNAL_WAKERS],
    #[cfg(not(target_os = "linux"))]
    cond: std::sync::Condvar,
    #[cfg(not(target_os = "linux"))]
    mutex: Mutex<()>,
}

pub static SIGNAL: Signal = Signal::new();

impl Signal {
    pub const fn new() -> Self {
        Self {
            counter: AtomicU32::new(0),
            reserved_wakers: AtomicUsize::new(0),
            published_wakers: AtomicUsize::new(0),
            wakers: [const { WakerSlot::uninit() }; MAX_SIGNAL_WAKERS],
            #[cfg(not(target_os = "linux"))]
            cond: std::sync::Condvar::new(),
            #[cfg(not(target_os = "linux"))]
            mutex: Mutex::new(()),
        }
    }

    /// Read the current state of the atomic counter.
    #[inline]
    pub fn read_counter(&self) -> u32 {
        self.counter.load(Ordering::Acquire)
    }

    /// Signal the sticky event. Increment the counter, wake all parked threads
    /// via FUTEX_WAKE (on Linux) or Condvar (on non-Linux), and wake all
    /// registered mio Wakers.
    pub fn signal(&self) {
        self.counter.fetch_add(1, Ordering::Release);

        #[cfg(target_os = "linux")]
        {
            unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    self.counter.as_ptr(),
                    libc::FUTEX_WAKE,
                    libc::c_int::MAX,
                    std::ptr::null::<libc::timespec>(),
                    std::ptr::null::<libc::c_int>(),
                    0 as libc::c_int,
                );
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            // Acquire lock to avoid race conditions with wait, though Condvar::notify_all
            // itself doesn't require the lock to be held.
            self.cond.notify_all();
        }

        let published_wakers = self.published_wakers.load(Ordering::Acquire);
        for slot in &self.wakers[..published_wakers] {
            // SAFETY: `published_wakers` is only advanced after slots in the initialized
            // prefix have been written, and initialized slots are never mutated
            // again.
            let waker = unsafe { slot.get() };
            if let Err(e) = waker.wake() {
                tracing::warn!("Failed to wake mio waker: {e}");
            }
        }
    }

    /// Park the current thread if the atomic counter is still equal to
    /// `expected`.
    pub fn park(&self, expected: u32) {
        #[cfg(target_os = "linux")]
        {
            unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    self.counter.as_ptr(),
                    libc::FUTEX_WAIT,
                    expected as libc::c_int,
                    std::ptr::null::<libc::timespec>(),
                    std::ptr::null::<libc::c_int>(),
                    0 as libc::c_int,
                );
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let mut guard = self.mutex.lock().unwrap();
            while self.counter.load(Ordering::Acquire) == expected {
                guard = self.cond.wait(guard).unwrap();
            }
        }
    }

    /// Register a `mio::Waker` instance. It will be woken whenever `signal` is
    /// called.
    ///
    /// # Panics
    ///
    /// Panics if more than [`MAX_SIGNAL_WAKERS`] are registered.
    pub fn register_waker(&self, waker: Waker) {
        let Ok(slot) =
            self.reserved_wakers.fetch_update(Ordering::AcqRel, Ordering::Acquire, |registered| {
                (registered < MAX_SIGNAL_WAKERS).then_some(registered + 1)
            })
        else {
            panic!("cannot register more than {MAX_SIGNAL_WAKERS} signal wakers");
        };

        self.wakers[slot].write(waker);

        while self
            .published_wakers
            .compare_exchange_weak(slot, slot + 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            spin_loop();
        }
    }
}

impl Drop for Signal {
    fn drop(&mut self) {
        let initialized = *self.published_wakers.get_mut();

        for slot in &mut self.wakers[..initialized] {
            // SAFETY: `published_wakers` is the initialized prefix.
            unsafe {
                slot.drop_initialized();
            }
        }
    }
}
