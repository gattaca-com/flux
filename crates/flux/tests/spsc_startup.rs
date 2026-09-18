// Expected startup panics run in a separate test process from tile runners:
// ScopedSpine's global panic hook stops runners even for a caught panic.
use flux::{
    communication::{cleanup_shmem, queue::spsc::Queue},
    spine::{SpineSpscProducer, SpineSpscQueue},
};
use flux_timing::{InternalMessage, TrackingTimestamp};
use flux_utils::directories::shmem_dir_with_base;

#[test]
fn spine_startup_waits_for_shared_queue_initialization() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let tmp = tempfile::tempdir().unwrap();
    let directory = shmem_dir_with_base(tmp.path(), "initializing").join("spsc");
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("messages");
    // SAFETY: isolated fixture, exact payload/ABI, no endpoints or fork.
    let queue = unsafe {
        flux::communication::queue::spsc::Queue::<InternalMessage<u64>>::create_or_open_shared(
            &path, 2,
        )
        .unwrap()
    };
    let mapping = shared_memory::ShmemConf::new().flink(&path).open().unwrap();
    // Initialization fault injection needs the private layout's first word:
    // its atomic publication marker. The public constructor cannot pause at
    // this boundary. No endpoint accesses the fixture while it is unpublished;
    // all other header fields and slots retain their initialized layout.
    let ptr = std::ptr::NonNull::new(mapping.as_ptr()).unwrap();
    assert!(mapping.len() >= std::mem::size_of::<AtomicU64>());
    assert!((ptr.as_ptr() as usize).is_multiple_of(std::mem::align_of::<AtomicU64>()));
    let ready = unsafe { ptr.cast::<AtomicU64>().as_ref() };
    let magic = ready.swap(0, Ordering::Relaxed);
    assert_ne!(magic, 0, "fixture starts from an initialized queue");
    std::thread::scope(|scope| {
        scope.spawn(|| {
            std::thread::sleep(std::time::Duration::from_millis(50));
            ready.store(magic, Ordering::Release);
        });
        // SAFETY: the fixture models only a delayed creator's publication.
        let opened = unsafe {
            SpineSpscQueue::<u64>::create_or_open_shared_with_base_dir(
                tmp.path(),
                "initializing",
                "messages",
                2,
            )
        };
        assert_eq!(opened.capacity(), queue.capacity());
    });
    drop(mapping);
    drop(queue);
    cleanup_shmem(tmp.path());
}

fn panic_message(error: &(dyn std::any::Any + Send)) -> &str {
    error
        .downcast_ref::<String>()
        .map_or_else(|| *error.downcast_ref::<&str>().unwrap(), String::as_str)
}

#[test]
fn incompatible_startup_names_the_queue_and_preserves_unread_messages() {
    let tmp = tempfile::tempdir().unwrap();
    // SAFETY: isolated fixture, one schema and no inherited endpoints.
    let queue = unsafe {
        SpineSpscQueue::<u64>::create_or_open_shared_with_base_dir(
            tmp.path(),
            "capacity",
            "messages",
            2,
        )
    };
    let mut producer = SpineSpscProducer::new(queue.clone());
    producer.try_produce(&InternalMessage::new(TrackingTimestamp::new(1), 83)).unwrap();
    let path = shmem_dir_with_base(tmp.path(), "capacity").join("spsc/messages");
    let error = std::panic::catch_unwind(|| {
        // SAFETY: same payload contract; only the requested capacity differs.
        unsafe {
            SpineSpscQueue::<u64>::create_or_open_shared_with_base_dir(
                tmp.path(),
                "capacity",
                "messages",
                4,
            )
        }
    })
    .unwrap_err();
    let message = panic_message(error.as_ref());
    assert!(message.contains(path.to_str().unwrap()));
    assert!(message.contains("requested length 4"));
    assert!(message.contains("existing capacity 2"));
    // SAFETY: the core endpoint uses the wrapper's exact wire type and protocol.
    let reopened = unsafe { Queue::<InternalMessage<u64>>::open_shared(&path) }.unwrap();
    let mut consumer = reopened.try_consumer().unwrap();
    assert!(consumer.consume(|message| assert_eq!(*message.data(), 83)));
    drop(consumer);
    drop(reopened);
    drop(producer);
    drop(queue);
    cleanup_shmem(tmp.path());
}

#[test]
fn stale_queue_link_reports_explicit_cleanup_without_replacing_storage() {
    let tmp = tempfile::tempdir().unwrap();
    let path = shmem_dir_with_base(tmp.path(), "stale").join("spsc/messages");
    // SAFETY: this fixture has one schema and no other participants.
    let queue = unsafe {
        SpineSpscQueue::<u64>::create_or_open_shared_with_base_dir(
            tmp.path(),
            "stale",
            "messages",
            2,
        )
    };
    let link = std::fs::read(&path).unwrap();
    drop(queue);
    flux::communication::cleanup_flink(&path).unwrap();
    std::fs::write(&path, &link).unwrap(); // Model a flink surviving loss of its backing storage.
    let error = std::panic::catch_unwind(|| {
        // SAFETY: the stale link has no backing storage or attached participants.
        unsafe {
            SpineSpscQueue::<u64>::create_or_open_shared_with_base_dir(
                tmp.path(),
                "stale",
                "messages",
                2,
            )
        }
    })
    .unwrap_err();
    let message = panic_message(error.as_ref());
    assert!(message.contains(path.to_str().unwrap()));
    assert!(message.contains("cleanup_flink"));
    assert_eq!(std::fs::read(&path).unwrap(), link);
    cleanup_shmem(tmp.path());
}
