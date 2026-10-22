use std::{
    io::{BufRead, BufReader},
    mem::ManuallyDrop,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Output, Stdio},
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use shared_memory::ShmemConf;
use type_hash::TypeHash;
use type_hash_derive::TypeHash;

use super::*;
use crate::cleanup_shmem;

// Same slot size, different type hashes.
type Old = [u32; 8];

#[derive(Clone, Copy, Default, TypeHash)]
#[type_hash(version = 1)]
struct New([u64; 4]);

#[derive(Clone, Copy, Default, TypeHash)]
#[type_hash(version = 1)]
struct Other([u16; 16]);

// One layout and queue name at two versions, as in two builds.
mod v1 {
    #[derive(Clone, Copy, Default, type_hash_derive::TypeHash)]
    #[type_hash(version = 1)]
    pub struct Msg(pub [u64; 4]);
}

mod v2 {
    #[derive(Clone, Copy, Default, type_hash_derive::TypeHash)]
    #[type_hash(version = 2)]
    pub struct Msg(pub [u64; 4]);
}

const ROLE: &str = "FLUX_QUEUE_TEST_ROLE";
const PATH: &str = "FLUX_QUEUE_TEST_PATH";
const READY: &str = "FLUX_QUEUE_TEST_READY";

/// One test's queue, and the children it re-executes the test binary as to
/// play other processes. Removes the queue directory when dropped.
struct Scenario {
    test: &'static str,
    path: PathBuf,
}

impl Scenario {
    /// In a re-executed child, plays its role and exits instead.
    fn new(test: &'static str) -> Self {
        if let (Ok(role), Some(path)) = (std::env::var(ROLE), std::env::var_os(PATH)) {
            // Not dropped: the queue belongs to the test, also when a role panics.
            ManuallyDrop::new(Self { test, path: path.into() }).play(&role);
        }
        let dir =
            std::env::temp_dir().join(format!("flux-queue-retire-{}-{test}", std::process::id()));
        Self { test, path: dir.join("queue") }
    }

    fn play(&self, role: &str) -> ! {
        let path = &self.path;
        match role {
            "create_unversioned" => {
                // As created before element type versions.
                let queue = Queue::<New>::create_or_open_shared(path, 16, QueueType::MPMC);
                queue.header.type_version.store(0, Ordering::Relaxed);
            }
            "hold" => {
                let queue = Queue::<New>::create_or_open_shared(path, 16, QueueType::MPMC);
                Self::hold_forever(|| queue.count())
            }
            "hold_future_header" => {
                // A later header layout, smaller than this one, with only the magic and
                // the header version where this code looks for them.
                std::fs::create_dir_all(self.dir()).unwrap();
                let shmem = ShmemConf::new().size(8192).flink(path).create().unwrap();
                let ptr = shmem.as_ptr();
                #[allow(clippy::cast_ptr_alignment)]
                unsafe {
                    ptr.add(offset_of!(QueueHeader, magic)).cast::<u64>().write(QUEUE_MAGIC);
                    ptr.add(offset_of!(QueueHeader, header_version))
                        .cast::<u32>()
                        .write(HEADER_VERSION + 1);
                }
                Self::hold_forever(|| unsafe { ptr.read_volatile() })
            }
            "hold_poisoned" => {
                let queue = Queue::<Old>::create_or_open_shared(path, 16, QueueType::MPMC);
                // A writer that died mid-write.
                queue.load(0).set_version_unsafe(1);
                Self::hold_forever(|| queue.count())
            }
            "consume" => {
                let term = Arc::new(AtomicBool::new(false));
                signal_hook::flag::register(libc::SIGTERM, Arc::clone(&term)).unwrap();
                let queue = Queue::<Old>::create_or_open_shared(path, 16, QueueType::MPMC);
                let mut consumer = Consumer::new(queue, "retire-test");
                consumer.subscribe_broadcast();
                println!("{READY}");
                while !term.load(Ordering::Relaxed) {
                    consumer.consume(|_| {});
                }
                // A full lap of produces into the replacement pages must still drain.
                let mut producer = Producer::from(queue);
                for _ in 0..16 {
                    producer.produce(&Old::default());
                }
                while consumer.consume(|_| {}) {}
                std::process::exit(if RetiredQueue::all().is_empty() { 2 } else { 0 });
            }
            "hold_two" => {
                let term = Arc::new(AtomicBool::new(false));
                signal_hook::flag::register(libc::SIGTERM, Arc::clone(&term)).unwrap();
                let first = Queue::<Old>::create_or_open_shared(path, 16, QueueType::MPMC);
                let second =
                    Queue::<Old>::create_or_open_shared(self.second_queue(), 16, QueueType::MPMC);
                println!("{READY}");
                while !term.load(Ordering::Relaxed) {
                    std::hint::black_box((first.count(), second.count()));
                    thread::sleep(Duration::from_millis(1));
                }
                // A spine's shutdown: it reports retired queues, which takes the
                // directory lock, and then winds down its tiles.
                assert!(!RetiredQueue::all().is_empty());
                thread::sleep(Duration::from_millis(200));
            }
            "hold_dcache" => {
                let (queue, _) = crate::shmem_queue_dcache_with_base_dir::<_, _, v1::Msg>(
                    self.dir(),
                    "app",
                    16,
                    64,
                    QueueType::MPMC,
                );
                Self::hold_forever(|| queue.count())
            }
            "open_old" => {
                Queue::<Old>::create_or_open_shared(path, 16, QueueType::MPMC);
            }
            "raise_sigbus" => {
                let no_core = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
                unsafe {
                    libc::setrlimit(libc::RLIMIT_CORE, &raw const no_core);
                    // Replace std's stack overflow handler, which swallows a sent SIGBUS
                    // once on its own.
                    libc::signal(libc::SIGBUS, libc::SIG_DFL);
                }
                Queue::<Old>::create_or_open_shared(path, 16, QueueType::MPMC);
                unsafe { libc::raise(libc::SIGBUS) };
            }
            "open_other" => {
                Queue::<Other>::create_or_open_shared(path, 16, QueueType::MPMC);
            }
            _ => unreachable!("{role}"),
        }
        std::process::exit(0)
    }

    /// Signals readiness, then keeps touching the queue through `touch`, so a
    /// retire faults this process.
    fn hold_forever<R>(touch: impl Fn() -> R) -> ! {
        println!("{READY}");
        loop {
            std::hint::black_box(touch());
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn command(&self, role: &str) -> Command {
        let mut cmd = Command::new(std::env::current_exe().unwrap());
        cmd.args([&format!("queue::tests_retire::{}", self.test), "--exact", "--nocapture"])
            .env(ROLE, role)
            .env(PATH, &self.path)
            .stdout(Stdio::piped());
        cmd
    }

    /// Plays `role` to completion.
    fn run(&self, role: &str) -> Output {
        self.command(role).output().unwrap()
    }

    /// Plays `role` until it holds its queue.
    fn hold(&self, role: &str) -> Holder {
        let mut child = self.command(role).spawn().unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        assert!(
            lines.by_ref().map_while(Result::ok).any(|l| l.contains(READY)),
            "{role} exited before it was ready"
        );
        thread::spawn(move || lines.for_each(drop));
        Holder(child)
    }

    fn dir(&self) -> &Path {
        self.path.parent().unwrap()
    }

    fn second_queue(&self) -> PathBuf {
        self.path.with_file_name("second")
    }

    fn backing_object(&self) -> PathBuf {
        let os_id = std::fs::read_to_string(&self.path).unwrap();
        Path::new("/dev/shm").join(os_id.trim_start_matches('/'))
    }
}

impl Drop for Scenario {
    fn drop(&mut self) {
        cleanup_shmem(self.dir());
    }
}

/// A child process holding a queue.
struct Holder(Child);

impl Drop for Holder {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Holder {
    fn pid(&self) -> u32 {
        self.0.id()
    }

    fn running(&mut self) -> bool {
        self.0.try_wait().unwrap().is_none()
    }

    fn stop(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }

    /// Kills the child and panics if it outlives `timeout`.
    fn exit_within(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            if Instant::now() > deadline {
                self.stop();
                panic!("child {} did not exit within {timeout:?}", self.pid());
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}

#[test]
fn mismatch_without_mappers_retires() {
    let scenario = Scenario::new("mismatch_without_mappers_retires");
    assert!(scenario.run("open_old").status.success());
    let old_object = scenario.backing_object();
    assert!(old_object.exists());

    let queue = Queue::<New>::create_or_open_shared(&scenario.path, 16, QueueType::MPMC);

    let identity = queue.header.identity().unwrap();
    assert_eq!(identity.type_hash, New::TYPE_HASH);
    assert_eq!(identity.type_version, 1);
    assert_eq!(identity.creator_pid, std::process::id());
    assert!(!old_object.exists());
}

#[test]
fn not_newer_version_refuses_retire_of_live_mapper() {
    let scenario = Scenario::new("not_newer_version_refuses_retire_of_live_mapper");
    assert!(scenario.run("create_unversioned").status.success());
    let mut holder = scenario.hold("hold");

    // An older binary restarting next to a newer one, then a layout change
    // without a version bump.
    for role in ["open_old", "open_other"] {
        let opener = scenario.run(role);
        let stderr = String::from_utf8_lossy(&opener.stderr);
        assert!(!opener.status.success(), "{role}: {stderr}");
        assert!(stderr.contains(&format!("pid {}", holder.pid())), "{role}: {stderr}");
        assert!(stderr.contains(RETIRE_MAPPED_QUEUES_ENV), "{role}: {stderr}");
    }
    assert!(holder.running());

    let forced = scenario.command("open_old").env(RETIRE_MAPPED_QUEUES_ENV, "1").output().unwrap();
    assert!(forced.status.success(), "{}", String::from_utf8_lossy(&forced.stderr));
    // The holder has no SIGTERM handler, so the retire's SIGTERM ends it.
    assert_eq!(holder.exit_within(Duration::from_secs(10)).signal(), Some(libc::SIGTERM));
}

#[test]
fn newer_header_refuses_retire_of_live_mapper() {
    let scenario = Scenario::new("newer_header_refuses_retire_of_live_mapper");
    let mut holder = scenario.hold("hold_future_header");

    // A higher element type version does not outrank a newer header layout.
    let opener = scenario.run("open_other");

    let holder_alive = holder.running();
    holder.stop();
    let stderr = String::from_utf8_lossy(&opener.stderr);
    assert!(!opener.status.success(), "{stderr}");
    assert!(stderr.contains(&format!("header version {}", HEADER_VERSION + 1)), "{stderr}");
    assert!(holder_alive);
}

#[test]
fn poison_retires_despite_live_mapper() {
    let scenario = Scenario::new("poison_retires_despite_live_mapper");
    let mut holder = scenario.hold("hold_poisoned");

    let queue = Queue::<Old>::create_or_open_shared(&scenario.path, 16, QueueType::MPMC);

    assert_eq!(queue.load(0).version(), 0);
    // The holder has no SIGTERM handler, so the retire's SIGTERM ends it.
    assert_eq!(holder.exit_within(Duration::from_secs(10)).signal(), Some(libc::SIGTERM));
}

#[test]
fn retire_stops_live_mapper() {
    let scenario = Scenario::new("retire_stops_live_mapper");
    let mut consumer = scenario.hold("consume");

    Queue::<New>::create_or_open_shared(&scenario.path, 16, QueueType::MPMC);

    // 0 only if the consumer stopped through its SIGTERM flag with the queue
    // reported retired.
    assert_eq!(consumer.exit_within(Duration::from_secs(10)).code(), Some(0));
}

#[test]
fn retire_waits_for_the_processes_it_stops() {
    let scenario = Scenario::new("retire_waits_for_the_processes_it_stops");
    let mut holder = scenario.hold("hold_two");

    Queue::<New>::create_or_open_shared(&scenario.path, 16, QueueType::MPMC);
    // Only larger, so not newer: replaceable only once the holder exited.
    Queue::<Old>::create_or_open_shared(scenario.second_queue(), 32, QueueType::MPMC);

    assert!(holder.exit_within(Duration::from_secs(10)).success());
}

#[test]
fn newer_version_retires_dcache_after_mtu_change() {
    let scenario = Scenario::new("newer_version_retires_dcache_after_mtu_change");
    assert_eq!(v1::Msg::TYPE_HASH, v2::Msg::TYPE_HASH);
    let mut holder = scenario.hold("hold_dcache");

    crate::shmem_queue_dcache_with_base_dir::<_, _, v2::Msg>(
        scenario.dir(),
        "app",
        16,
        128,
        QueueType::MPMC,
    );

    assert_eq!(holder.exit_within(Duration::from_secs(10)).signal(), Some(libc::SIGTERM));
}

#[test]
fn sent_sigbus_still_kills() {
    let scenario = Scenario::new("sent_sigbus_still_kills");
    assert_eq!(scenario.run("raise_sigbus").status.signal(), Some(libc::SIGBUS));
}

#[test]
fn concurrent_openers_share_one_segment() {
    const OPENERS: usize = 8;
    let scenario = Scenario::new("concurrent_openers_share_one_segment");
    let barrier = Barrier::new(OPENERS);
    let queues: Vec<Queue<u64>> = thread::scope(|s| {
        let openers: Vec<_> = (0..OPENERS)
            .map(|_| {
                s.spawn(|| {
                    barrier.wait();
                    Queue::<u64>::create_or_open_shared(&scenario.path, 16, QueueType::MPMC)
                })
            })
            .collect();
        openers.into_iter().map(|o| o.join().unwrap()).collect()
    });

    Producer::from(queues[0]).produce(&1);
    assert!(queues.iter().all(|q| q.count() == 1));
}
