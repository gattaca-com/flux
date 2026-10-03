#![cfg(all(target_os = "linux", feature = "park"))]

use std::{io, time::Duration};

use flux::park::Signal;
use io_uring::{IoUring, Probe, opcode, squeue, types};

const WAIT: u64 = 1;
const ARMED: u64 = 2;
const CANCEL: u64 = 3;
// Linux FUTEX2_SIZE_U32; libc does not yet expose this flag.
const FUTEX2_SIZE_U32: u32 = 2;

struct FutexRing {
    ring: IoUring,
    signal: &'static Signal,
}

impl FutexRing {
    fn new(signal: &'static Signal, sqpoll: bool) -> Option<Self> {
        let mut builder = IoUring::builder();
        if sqpoll {
            builder.setup_sqpoll(10);
        }
        let ring = match builder.build(8) {
            Ok(ring) => ring,
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EPERM | libc::EACCES | libc::ENOSYS | libc::EOPNOTSUPP)
                ) =>
            {
                eprintln!("SKIP io_uring futex test (sqpoll={sqpoll}): {error}");
                return None;
            }
            Err(error) => panic!("create io_uring: {error}"),
        };
        let mut probe = Probe::new();
        ring.submitter().register_probe(&mut probe).unwrap();
        if !probe.is_supported(opcode::FutexWait::CODE) {
            eprintln!("SKIP io_uring futex test: FUTEX_WAIT is unavailable");
            return None;
        }
        Some(Self { ring, signal })
    }

    fn push(&mut self, entry: &squeue::Entry) {
        // All submitted pointers refer to static signals, including on test failure.
        unsafe { self.ring.submission().push(entry).unwrap() };
    }

    fn queue_wait(&mut self, expected: u32) {
        self.push(
            &opcode::FutexWait::new(
                self.signal.futex_ptr(),
                u64::from(expected),
                u64::from(u32::MAX),
                FUTEX2_SIZE_U32,
            )
            .build()
            .user_data(WAIT),
        );
    }

    fn arm_wait(&mut self) {
        self.queue_wait(self.signal.read_counter());
        // FUTEX_WAIT installs its waiter before the following NOP is issued.
        self.push(&opcode::Nop::new().build().user_data(ARMED));
        assert_eq!(self.completion(), (ARMED, 0));
    }

    fn wait(&self, timeout: Duration) -> io::Result<usize> {
        let timeout = types::Timespec::from(timeout);
        let args = types::SubmitArgs::new().timespec(&timeout);
        self.ring.submitter().submit_with_args(1, &args)
    }

    fn completion(&mut self) -> (u64, i32) {
        self.wait(Duration::from_secs(2)).expect("completion deadline");
        let completion = self.ring.completion().next().unwrap();
        (completion.user_data(), completion.result())
    }
}

#[test]
fn signal_wakes_and_rearms_uring_wait() {
    static SIGNAL: Signal = Signal::new();
    for sqpoll in [false, true] {
        let Some(mut waiter) = FutexRing::new(&SIGNAL, sqpoll) else { continue };
        for _ in 0..2 {
            waiter.arm_wait();
            SIGNAL.signal();
            assert_eq!(waiter.completion(), (WAIT, 0));
        }
    }
}

#[test]
fn signal_before_uring_wait_completes_without_sleeping() {
    static SIGNAL: Signal = Signal::new();
    for sqpoll in [false, true] {
        let Some(mut waiter) = FutexRing::new(&SIGNAL, sqpoll) else { continue };
        let expected = SIGNAL.read_counter();
        SIGNAL.signal();
        waiter.queue_wait(expected);
        assert_eq!(waiter.completion(), (WAIT, -libc::EAGAIN));
    }
}

#[test]
fn uring_wait_can_be_cancelled_and_rearmed() {
    static SIGNAL: Signal = Signal::new();
    for sqpoll in [false, true] {
        let Some(mut waiter) = FutexRing::new(&SIGNAL, sqpoll) else { continue };
        waiter.arm_wait();
        waiter.push(&opcode::AsyncCancel::new(WAIT).build().user_data(CANCEL));
        let mut completions = [waiter.completion(), waiter.completion()];
        completions.sort_unstable();
        assert_eq!(completions[0], (WAIT, -libc::ECANCELED));
        let (user_data, result) = completions[1];
        assert_eq!(user_data, CANCEL);
        assert!(result >= 0, "cancel failed: {result}");

        waiter.arm_wait();
        SIGNAL.signal();
        assert_eq!(waiter.completion(), (WAIT, 0));
    }
}

#[test]
fn completion_timeout_keeps_the_futex_wait_armed() {
    static SIGNAL: Signal = Signal::new();
    for sqpoll in [false, true] {
        let Some(mut waiter) = FutexRing::new(&SIGNAL, sqpoll) else { continue };
        waiter.arm_wait();
        let error = waiter.wait(Duration::from_millis(5)).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ETIME));
        SIGNAL.signal();
        assert_eq!(waiter.completion(), (WAIT, 0));
    }
}
