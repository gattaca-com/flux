use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use mio::Waker;

pub struct Signal {
    counter: AtomicU32,
    wakers: Mutex<Vec<(u64, Waker)>>,
    #[cfg(not(target_os = "linux"))]
    cond: std::sync::Condvar,
    #[cfg(not(target_os = "linux"))]
    mutex: Mutex<()>,
}

pub static SIGNAL: Signal = Signal::new();

static NEXT_WAKER_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

impl Signal {
    pub const fn new() -> Self {
        Self {
            counter: AtomicU32::new(0),
            wakers: Mutex::new(Vec::new()),
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

    /// Signal the sticky event. Increment the counter, wake all parked threads via
    /// FUTEX_WAKE (on Linux) or Condvar (on non-Linux), and wake all registered mio Wakers.
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

        // Wake registered mio wakers
        let wakers = self.wakers.lock().unwrap();
        for (_, waker) in wakers.iter() {
            if let Err(e) = waker.wake() {
                tracing::warn!("Failed to wake mio waker: {e}");
            }
        }
    }

    /// Park the current thread if the atomic counter is still equal to `expected`.
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

    /// Register a `mio::Waker` instance. It will be woken whenever `signal` is called.
    /// Returns a unique token ID that can be used to unregister.
    pub fn register_waker(&self, waker: Waker) -> u64 {
        let id = NEXT_WAKER_ID.fetch_add(1, Ordering::Relaxed);
        self.wakers.lock().unwrap().push((id, waker));
        id
    }

    /// Unregister a `mio::Waker` instance by its ID.
    pub fn unregister_waker(&self, id: u64) {
        let mut guard = self.wakers.lock().unwrap();
        if let Some(pos) = guard.iter().position(|(w_id, _)| *w_id == id) {
            guard.swap_remove(pos);
        }
    }
}
