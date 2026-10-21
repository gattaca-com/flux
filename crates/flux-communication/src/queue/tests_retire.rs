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

use type_hash::TypeHash;

use super::*;
use crate::cleanup_shmem;

// Same slot size, different type hash.
type Old = [u32; 8];
type New = [u64; 4];

const ROLE: &str = "FLUX_QUEUE_TEST_ROLE";
const PATH: &str = "FLUX_QUEUE_TEST_PATH";
const READY: &str = "FLUX_QUEUE_TEST_READY";

/// Runs in a re-executed test binary when `ROLE` is set.
fn child(role: &str) -> ! {
    let path = PathBuf::from(std::env::var(PATH).unwrap());
    match role {
        "create" => {
            Queue::<Old>::create_or_open_shared(&path, 16, QueueType::MPMC);
        }
        "hold" => {
            let _queue = Queue::<Old>::create_or_open_shared(&path, 16, QueueType::MPMC);
            println!("{READY}");
            loop {
                thread::park();
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
        "open_new" => {
            Queue::<New>::create_or_open_shared(&path, 16, QueueType::MPMC);
        }
        _ => unreachable!("{role}"),
    }
    std::process::exit(0)
}

fn spawn(test: &str, role: &str, path: &Path, retire_mapped: bool) -> Child {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args([&format!("queue::tests_retire::{test}"), "--exact", "--nocapture"])
        .env(ROLE, role)
        .env(PATH, path)
        .stdout(Stdio::piped());
    if retire_mapped {
        cmd.env(RETIRE_MAPPED_QUEUES_ENV, "1");
    }
    cmd.spawn().unwrap()
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
    assert!(spawn(test, "create", &path, false).wait().unwrap().success());
    let old_object = backing_object(&path);
    assert!(old_object.exists());

    let queue = Queue::<New>::create_or_open_shared(&path, 16, QueueType::MPMC);

    let identity = queue.header.identity().unwrap();
    assert_eq!(identity.type_hash, New::TYPE_HASH);
    assert_eq!(identity.creator_pid, std::process::id());
    assert!(!old_object.exists());
    cleanup_shmem(path.parent().unwrap());
}

#[test]
fn live_mapper_refuses_retire() {
    if let Ok(role) = std::env::var(ROLE) {
        child(&role);
    }
    let test = "live_mapper_refuses_retire";
    let path = queue_path(test);
    let mut holder = spawn(test, "hold", &path, false);
    wait_ready(&mut holder);

    let refusal = std::panic::catch_unwind(|| {
        Queue::<New>::create_or_open_shared(&path, 16, QueueType::MPMC)
    })
    .unwrap_err();

    holder.kill().unwrap();
    holder.wait().unwrap();
    let msg = refusal.downcast_ref::<String>().unwrap();
    assert!(msg.contains(&format!("pid {}", holder.id())), "{msg}");
    assert!(msg.contains(RETIRE_MAPPED_QUEUES_ENV), "{msg}");
    cleanup_shmem(path.parent().unwrap());
}

#[test]
fn poison_retires_despite_live_mapper() {
    if let Ok(role) = std::env::var(ROLE) {
        child(&role);
    }
    let test = "poison_retires_despite_live_mapper";
    let path = queue_path(test);
    let mut holder = spawn(test, "hold_poisoned", &path, false);
    wait_ready(&mut holder);

    let queue = Queue::<Old>::create_or_open_shared(&path, 16, QueueType::MPMC);

    assert_eq!(queue.load(0).version(), 0);
    // The holder has no SIGTERM handler, so the retire's SIGTERM ends it.
    assert_eq!(wait_for(&mut holder, Duration::from_secs(10)).signal(), Some(libc::SIGTERM));
    cleanup_shmem(path.parent().unwrap());
}

#[test]
fn forced_retire_stops_live_mapper() {
    if let Ok(role) = std::env::var(ROLE) {
        child(&role);
    }
    let test = "forced_retire_stops_live_mapper";
    let path = queue_path(test);
    let mut consumer = spawn(test, "consume", &path, false);
    wait_ready(&mut consumer);

    let mut opener = spawn(test, "open_new", &path, true);
    assert!(wait_for(&mut opener, Duration::from_secs(10)).success());

    // 0 only if the consumer stopped through its SIGTERM flag with the queue
    // reported retired.
    assert_eq!(wait_for(&mut consumer, Duration::from_secs(10)).code(), Some(0));
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
