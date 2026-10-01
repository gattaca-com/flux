use std::io;

#[cfg(not(target_os = "linux"))]
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
fn set_thread_niceness(niceness: Option<ThreadNiceness>) -> io::Result<()> {
    if let Some(niceness) = niceness {
        let niceness = niceness.value();
        validate_thread_niceness(niceness);
        let code = unsafe { libc::setpriority(libc::PRIO_PROCESS, 0, niceness) };
        if code != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn set_thread_niceness(niceness: Option<ThreadNiceness>) -> io::Result<()> {
    match niceness {
        Some(_) => Err(io::Error::new(io::ErrorKind::Unsupported, "only supported on linux")),
        None => Ok(()),
    }
}

#[cfg(target_os = "linux")]
fn set_thread_affinity(cores: &[usize]) -> io::Result<()> {
    if cores.is_empty() {
        return Ok(());
    }
    let max = 8 * std::mem::size_of::<libc::cpu_set_t>();
    if let Some(&core) = cores.iter().find(|&&core| core >= max) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("core {core} is beyond the {max} a CPU set holds"),
        ));
    }
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for &core in cores {
            libc::CPU_SET(core, &mut set);
        }
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &raw const set) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn set_thread_affinity(cores: &[usize]) -> io::Result<()> {
    if let Some(&core) = cores.first() {
        if cores.len() > 1 {
            warn!(?cores, "core-set pinning only supported on linux; pinning to first core");
        }
        if !core_affinity::set_for_current(CoreId { id: core }) {
            return Err(io::Error::other(format!("couldn't pin to core {core}")));
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn get_tid() -> i64 {
    unsafe { libc::gettid() as i64 }
}

#[cfg(not(target_os = "linux"))]
pub fn get_tid() -> i64 {
    0
}

/// Pins the calling thread to `cores` and sets its `niceness`, warning if
/// either fails.
pub fn thread_boot(cores: &[usize], niceness: Option<ThreadNiceness>) {
    if let Err(error) = set_thread_affinity(cores) {
        warn!(?cores, %error, "couldn't set core affinity");
    }
    if let Err(error) = set_thread_niceness(niceness) {
        warn!(?niceness, %error, "couldn't set thread niceness");
    }
}

/// [`thread_boot`], but a failure is an error.
pub fn try_thread_boot(cores: &[usize], niceness: Option<ThreadNiceness>) -> io::Result<()> {
    set_thread_affinity(cores)?;
    set_thread_niceness(niceness)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn a_core_beyond_the_cpu_set_is_an_error() {
        let error = try_thread_boot(&[4096], None).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
