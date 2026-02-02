use core_affinity::CoreId;
#[cfg(target_os = "linux")]
use libc::{SCHED_FIFO, sched_param, sched_setscheduler};
use tracing::warn;

/// Any variant other than `OSDefault` requests `SCHED_FIFO` realtime scheduling
/// if the process has permission (CAP_SYS_NICE).  
/// If setting the policy fails, execution continues under the OS default (CFS).
#[derive(Clone, Copy, Debug)]
pub enum ThreadPriority {
    OSDefault,
    Low,
    Medium,
    High,
    Custom(i32),
}

#[cfg(target_os = "linux")]
impl ThreadPriority {
    fn to_sched_param(self) -> Option<sched_param> {
        let prio = match self {
            ThreadPriority::OSDefault => return None,
            ThreadPriority::Low => 40,
            ThreadPriority::Medium => 60,
            ThreadPriority::High => 75,
            ThreadPriority::Custom(p) => p,
        };
        Some(sched_param { sched_priority: prio })
    }
}

#[cfg(target_os = "linux")]
fn set_thread_prio(prio: ThreadPriority) {
    if let Some(param) = prio.to_sched_param() {
        unsafe {
            let code = sched_setscheduler(0, SCHED_FIFO, &param);
            if code != 0 {
                warn!(%code, ?param, "couldn't set thread priority");
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn set_thread_prio(prio: ThreadPriority) {
    if !matches!(prio, ThreadPriority::OSDefault) {
        warn!(?prio, "thread priority setting only supported on linux");
    }
}

fn set_thread_affinity(core: usize) {
    if !core_affinity::set_for_current(CoreId { id: core }) {
        warn!(?core, "couldn't set core affinity");
    }
}

pub fn thread_boot(core: Option<usize>, prio: ThreadPriority) {
    if let Some(core) = core {
        set_thread_affinity(core);
    }

    set_thread_prio(prio);
}
