use core_affinity::CoreId;
use tracing::warn;

#[derive(Clone, Copy, Debug)]
pub enum ThreadNiceness {
    Low,
    Medium,
    High,
    Highest,
    Custom(i32),
}

impl ThreadNiceness {
    const fn value(self) -> i32 {
        match self {
            Self::Low => 10,
            Self::Medium => 0,
            Self::High => -10,
            Self::Highest => -20,
            Self::Custom(niceness) => niceness,
        }
    }
}

#[cfg(target_os = "linux")]
const fn validate_thread_niceness(niceness: i32) {
    assert!(niceness >= -20 && niceness <= 19, "thread niceness must be between -20 and 19");
}

#[cfg(target_os = "linux")]
fn set_thread_niceness(niceness: Option<ThreadNiceness>) {
    if let Some(niceness) = niceness {
        let niceness = niceness.value();
        validate_thread_niceness(niceness);
        let code = unsafe { libc::setpriority(libc::PRIO_PROCESS, 0, niceness) };
        if code != 0 {
            let error = std::io::Error::last_os_error();
            warn!(niceness, %error, "couldn't set thread niceness");
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn set_thread_niceness(niceness: Option<ThreadNiceness>) {
    if let Some(niceness) = niceness {
        warn!(?niceness, "thread niceness setting only supported on linux");
    }
}

fn set_thread_affinity(core: usize) {
    if !core_affinity::set_for_current(CoreId { id: core }) {
        warn!(?core, "couldn't set core affinity");
    }
}

#[cfg(target_os = "linux")]
pub fn get_tid() -> i64 {
    unsafe { libc::gettid() as i64 }
}

#[cfg(target_os = "macos")]
pub fn get_tid() -> i64 {
    // pthread_threadid_np(self) returns the calling thread's unique 64-bit id
    // — the per-thread ring identity the profiler relies on. Without it every
    // thread shares tid 0 and collides on one ring file.
    let mut tid: u64 = 0;
    unsafe {
        libc::pthread_threadid_np(libc::pthread_self(), &mut tid);
    }
    tid as i64
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn get_tid() -> i64 {
    0
}

pub fn thread_boot(core: Option<usize>, niceness: Option<ThreadNiceness>) {
    if let Some(core) = core {
        set_thread_affinity(core);
    }

    set_thread_niceness(niceness);
}
