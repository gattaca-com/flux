use std::{
    io::{BufRead, BufReader},
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
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

/// Runs in a re-executed test binary when `ROLE` is set.
fn child(role: &str) -> ! {
    let path = PathBuf::from(std::env::var(PATH).unwrap());
    match role {
        "create_unversioned" => {
            // As created before element type versions.
            let queue = Queue::<New>::create_or_open_shared(&path, 16, QueueType::MPMC);
            queue.header.type_version.store(0, Ordering::Relaxed);
        }
        "hold" => {
            let queue = Queue::<New>::create_or_open_shared(&path, 16, QueueType::MPMC);
            println!("{READY}");
            loop {
                std::hint::black_box(queue.count());
                thread::sleep(Duration::from_millis(1));
            }
        }
        "hold_future_header" => {
            // A later header layout, smaller than this one, with only the magic and
            // the header version where this code looks for them.
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let shmem = ShmemConf::new().size(8192).flink(&path).create().unwrap();
            let ptr = shmem.as_ptr();
            #[allow(clippy::cast_ptr_alignment)]
            unsafe {
                ptr.add(offset_of!(QueueHeader, magic)).cast::<u64>().write(QUEUE_MAGIC);
                ptr.add(offset_of!(QueueHeader, header_version))
                    .cast::<u32>()
                    .write(HEADER_VERSION + 1);
            }
            println!("{READY}");
            loop {
                std::hint::black_box(unsafe { ptr.read_volatile() });
                thread::sleep(Duration::from_millis(1));
            }
        }
        "hold_poisoned" => {
            let queue = Queue::<Old>::create_or_open_shared(&path, 16, QueueType::MPMC);
            // A writer that died mid-write.
            queue.load(0).set_version_unsafe(1);
            println!("{READY}");
            loop {
                std::hint::black_box(queue.count());
                thread::sleep(Duration::from_millis(1));
            }
        }
        "consume" => {
            let term = Arc::new(AtomicBool::new(false));
            signal_hook::flag::register(libc::SIGTERM, Arc::clone(&term)).unwrap();
            let queue = Queue::<Old>::create_or_open_shared(&path, 16, QueueType::MPMC);
            let mut consumer = ConsumerBare::new(queue, "retire-test");
            let mut msg = Old::default();
            consumer.subscribe_broadcast();
            println!("{READY}");
            while !term.load(Ordering::Relaxed) {
                let _ = consumer.try_consume(&mut msg);
            }
            std::process::exit(if retired_queues().is_empty() { 2 } else { 0 });
        }
        "hold_two" => {
            let term = Arc::new(AtomicBool::new(false));
            signal_hook::flag::register(libc::SIGTERM, Arc::clone(&term)).unwrap();
            let first = Queue::<Old>::create_or_open_shared(&path, 16, QueueType::MPMC);
            let second =
                Queue::<Old>::create_or_open_shared(second_queue(&path), 16, QueueType::MPMC);
            println!("{READY}");
            while !term.load(Ordering::Relaxed) {
                std::hint::black_box((first.count(), second.count()));
                thread::sleep(Duration::from_millis(1));
            }
            // A spine's shutdown: it reports retired queues, which takes the
            // directory lock, and then winds down its tiles.
            assert!(!retired_queues().is_empty());
            thread::sleep(Duration::from_millis(200));
        }
        "hold_dcache" => {
            let (queue, _) = crate::shmem_queue_dcache_with_base_dir::<_, _, v1::Msg>(
                path.parent().unwrap(),
                "app",
                16,
                64,
                QueueType::MPMC,
            );
            println!("{READY}");
            loop {
                std::hint::black_box(queue.count());
                thread::sleep(Duration::from_millis(1));
            }
        }
        "open_old" => {
            Queue::<Old>::create_or_open_shared(&path, 16, QueueType::MPMC);
        }
        "open_other" => {
            Queue::<Other>::create_or_open_shared(&path, 16, QueueType::MPMC);
        }
        _ => unreachable!("{role}"),
    }
    std::process::exit(0)
}

fn child_command(test: &str, role: &str, path: &Path) -> Command {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args([&format!("queue::tests_retire::{test}"), "--exact", "--nocapture"])
        .env(ROLE, role)
        .env(PATH, path)
        .stdout(Stdio::piped());
    cmd
}

fn spawn(test: &str, role: &str, path: &Path) -> Child {
    child_command(test, role, path).spawn().unwrap()
}

fn wait_ready(child: &mut Child) {
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    assert!(
        lines.by_ref().map_while(Result::ok).any(|l| l.contains(READY)),
        "child exited before it was ready"
    );
    thread::spawn(move || lines.for_each(drop));
}

fn wait_for(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() > deadline {
            child.kill().unwrap();
            panic!("child {} did not exit within {timeout:?}", child.id());
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn queue_path(test: &str) -> PathBuf {
    std::env::temp_dir()
        .join(format!("flux-queue-retire-{}-{test}", std::process::id()))
        .join("queue")
}

fn second_queue(first: &Path) -> PathBuf {
    first.with_file_name("second")
}

fn backing_object(flink: &Path) -> PathBuf {
    let os_id = std::fs::read_to_string(flink).unwrap();
    Path::new("/dev/shm").join(os_id.trim_start_matches('/'))
}

#[test]
fn mismatch_without_mappers_retires() {
    if let Ok(role) = std::env::var(ROLE) {
        child(&role);
    }
    let test = "mismatch_without_mappers_retires";
    let path = queue_path(test);
    assert!(spawn(test, "open_old", &path).wait().unwrap().success());
    let old_object = backing_object(&path);
    assert!(old_object.exists());

    let queue = Queue::<New>::create_or_open_shared(&path, 16, QueueType::MPMC);

    let identity = queue.header.identity().unwrap();
    assert_eq!(identity.type_hash, New::TYPE_HASH);
    assert_eq!(identity.type_version, 1);
    assert_eq!(identity.creator_pid, std::process::id());
    assert!(!old_object.exists());
    cleanup_shmem(path.parent().unwrap());
}

#[test]
fn not_newer_version_refuses_retire_of_live_mapper() {
    if let Ok(role) = std::env::var(ROLE) {
        child(&role);
    }
    let test = "not_newer_version_refuses_retire_of_live_mapper";
    let path = queue_path(test);
    assert!(spawn(test, "create_unversioned", &path).wait().unwrap().success());
    let mut holder = spawn(test, "hold", &path);
    wait_ready(&mut holder);

    // An older binary restarting next to a newer one, then a layout change
    // without a version bump.
    for role in ["open_old", "open_other"] {
        let opener = child_command(test, role, &path).output().unwrap();
        let stderr = String::from_utf8_lossy(&opener.stderr);
        assert!(!opener.status.success(), "{role}: {stderr}");
        assert!(stderr.contains(&format!("pid {}", holder.id())), "{role}: {stderr}");
        assert!(stderr.contains(RETIRE_MAPPED_QUEUES_ENV), "{role}: {stderr}");
    }
    assert!(holder.try_wait().unwrap().is_none());

    let forced =
        child_command(test, "open_old", &path).env(RETIRE_MAPPED_QUEUES_ENV, "1").output().unwrap();
    assert!(forced.status.success(), "{}", String::from_utf8_lossy(&forced.stderr));
    // The holder has no SIGTERM handler, so the retire's SIGTERM ends it.
    assert_eq!(wait_for(&mut holder, Duration::from_secs(10)).signal(), Some(libc::SIGTERM));
    cleanup_shmem(path.parent().unwrap());
}

#[test]
fn newer_header_refuses_retire_of_live_mapper() {
    if let Ok(role) = std::env::var(ROLE) {
        child(&role);
    }
    let test = "newer_header_refuses_retire_of_live_mapper";
    let path = queue_path(test);
    let mut holder = spawn(test, "hold_future_header", &path);
    wait_ready(&mut holder);

    // A higher element type version does not outrank a newer header layout.
    let opener = child_command(test, "open_other", &path).output().unwrap();

    let holder_alive = holder.try_wait().unwrap().is_none();
    holder.kill().unwrap();
    holder.wait().unwrap();
    let stderr = String::from_utf8_lossy(&opener.stderr);
    assert!(!opener.status.success(), "{stderr}");
    assert!(stderr.contains(&format!("header version {}", HEADER_VERSION + 1)), "{stderr}");
    assert!(holder_alive);
    cleanup_shmem(path.parent().unwrap());
}

#[test]
fn poison_retires_despite_live_mapper() {
    if let Ok(role) = std::env::var(ROLE) {
        child(&role);
    }
    let test = "poison_retires_despite_live_mapper";
    let path = queue_path(test);
    let mut holder = spawn(test, "hold_poisoned", &path);
    wait_ready(&mut holder);

    let queue = Queue::<Old>::create_or_open_shared(&path, 16, QueueType::MPMC);

    assert_eq!(queue.load(0).version(), 0);
    // The holder has no SIGTERM handler, so the retire's SIGTERM ends it.
    assert_eq!(wait_for(&mut holder, Duration::from_secs(10)).signal(), Some(libc::SIGTERM));
    cleanup_shmem(path.parent().unwrap());
}

#[test]
fn retire_stops_live_mapper() {
    if let Ok(role) = std::env::var(ROLE) {
        child(&role);
    }
    let test = "retire_stops_live_mapper";
    let path = queue_path(test);
    let mut consumer = spawn(test, "consume", &path);
    wait_ready(&mut consumer);

    Queue::<New>::create_or_open_shared(&path, 16, QueueType::MPMC);

    // 0 only if the consumer stopped through its SIGTERM flag with the queue
    // reported retired.
    assert_eq!(wait_for(&mut consumer, Duration::from_secs(10)).code(), Some(0));
    cleanup_shmem(path.parent().unwrap());
}

#[test]
fn retire_waits_for_the_processes_it_stops() {
    if let Ok(role) = std::env::var(ROLE) {
        child(&role);
    }
    let test = "retire_waits_for_the_processes_it_stops";
    let path = queue_path(test);
    let mut holder = spawn(test, "hold_two", &path);
    wait_ready(&mut holder);

    Queue::<New>::create_or_open_shared(&path, 16, QueueType::MPMC);
    // Only larger, so not newer: replaceable only once the holder exited.
    Queue::<Old>::create_or_open_shared(second_queue(&path), 32, QueueType::MPMC);

    assert!(wait_for(&mut holder, Duration::from_secs(10)).success());
    cleanup_shmem(path.parent().unwrap());
}

#[test]
fn newer_version_retires_dcache_after_mtu_change() {
    if let Ok(role) = std::env::var(ROLE) {
        child(&role);
    }
    let test = "newer_version_retires_dcache_after_mtu_change";
    let path = queue_path(test);
    assert_eq!(v1::Msg::TYPE_HASH, v2::Msg::TYPE_HASH);
    let mut holder = spawn(test, "hold_dcache", &path);
    wait_ready(&mut holder);

    crate::shmem_queue_dcache_with_base_dir::<_, _, v2::Msg>(
        path.parent().unwrap(),
        "app",
        16,
        128,
        QueueType::MPMC,
    );

    assert_eq!(wait_for(&mut holder, Duration::from_secs(10)).signal(), Some(libc::SIGTERM));
    cleanup_shmem(path.parent().unwrap());
}

#[test]
fn concurrent_openers_share_one_segment() {
    const OPENERS: usize = 8;
    let path = queue_path("concurrent_openers_share_one_segment");
    let barrier = Barrier::new(OPENERS);
    let queues: Vec<Queue<u64>> = thread::scope(|s| {
        let openers: Vec<_> = (0..OPENERS)
            .map(|_| {
                s.spawn(|| {
                    barrier.wait();
                    Queue::<u64>::create_or_open_shared(&path, 16, QueueType::MPMC)
                })
            })
            .collect();
        openers.into_iter().map(|o| o.join().unwrap()).collect()
    });

    Producer::from(queues[0]).produce(&1);
    assert!(queues.iter().all(|q| q.count() == 1));
    cleanup_shmem(path.parent().unwrap());
}
