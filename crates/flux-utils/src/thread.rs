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

#[cfg(not(target_os = "linux"))]
pub fn get_tid() -> i64 {
    0
}

pub fn thread_boot(core: Option<usize>, niceness: Option<ThreadNiceness>) {
    if let Some(core) = core {
        set_thread_affinity(core);
    }

    set_thread_niceness(niceness);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    #[should_panic(expected = "thread niceness must be between -20 and 19")]
    fn rejects_invalid_niceness() {
        set_thread_niceness(Some(ThreadNiceness::Custom(20)));
    }
}
