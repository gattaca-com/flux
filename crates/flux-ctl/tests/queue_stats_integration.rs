// Integration test to verify queue stats are correctly read during discovery
use std::sync::atomic::{AtomicUsize, Ordering};

use flux_communication::queue::QueueHeader;
use flux_ctl::discovery::{read_consumer_groups, scan_base_dir};
use shared_memory::ShmemConf;
use tempfile::tempdir;

#[test]
fn queue_stats_populated_during_discovery() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();

    // Create a queue shmem segment manually
    let flink_dir = base.join("testapp").join("shmem").join("queues");
    std::fs::create_dir_all(&flink_dir).unwrap();
    let flink_path = flink_dir.join("TestQueue");

    let mut shmem = ShmemConf::new()
        .size(std::mem::size_of::<QueueHeader>() + 1024)
        .flink(&flink_path)
        .create()
        .unwrap();

    // Initialize header with test data
    let header = unsafe { &mut *(shmem.as_ptr() as *mut QueueHeader) };
    header.elsize = 32;
    header.mask = 63; // capacity = 64
    header.count = AtomicUsize::new(150);

    // Mark as initialized (this is what flux does)
    unsafe {
        let is_init_ptr = shmem.as_ptr().add(1) as *mut u8;
        *is_init_ptr = 1;
    }

    // Make sure we don't unlink on drop
    shmem.set_owner(false);
    drop(shmem);

    // Now scan and verify the stats are read correctly
    let entries = scan_base_dir(base);
    assert_eq!(entries.len(), 1, "Should discover exactly one entry");

    let entry = &entries[0];

    assert_eq!(entry.app_name, "testapp");
    assert_eq!(entry.type_name, "TestQueue");
    assert_eq!(entry.elem_size, 32);
    assert_eq!(entry.capacity, 64); // mask + 1
    assert_eq!(entry.queue_writes, Some(150));
    assert_eq!(entry.queue_fill, Some(150 & 63)); // 150 & mask = 22

    // Cleanup
    if let Ok(mut shmem) = ShmemConf::new().flink(&flink_path).open() {
        shmem.set_owner(true);
    }
}

#[test]
fn data_segments_have_no_queue_stats() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();

    // Create a data segment (not a queue)
    let flink_dir = base.join("dataapp").join("shmem").join("data");
    std::fs::create_dir_all(&flink_dir).unwrap();
    let flink_path = flink_dir.join("Config");

    let mut shmem = ShmemConf::new().size(256).flink(&flink_path).create().unwrap();

    shmem.set_owner(false);
    drop(shmem);

    let entries = scan_base_dir(base);
    assert_eq!(entries.len(), 1);

    let entry = &entries[0];
    assert_eq!(entry.app_name, "dataapp");
    assert_eq!(entry.type_name, "Config");
    assert_eq!(entry.elem_size, 256);
    assert_eq!(entry.capacity, 1);
    assert_eq!(entry.queue_writes, None); // No queue stats for data segments
    assert_eq!(entry.queue_fill, None);

    // Cleanup
    if let Ok(mut shmem) = ShmemConf::new().flink(&flink_path).open() {
        shmem.set_owner(true);
    }
}

/// Smoke test: create a real shmem queue with consumer groups registered,
/// then read them back through `read_consumer_groups` — the same path the
/// TUI detail view uses.
#[test]
fn consumer_groups_readable_from_shmem() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();

    let flink_dir = base.join("myapp").join("shmem").join("queues");
    std::fs::create_dir_all(&flink_dir).unwrap();
    let flink_path = flink_dir.join("Events");

    let mut shmem = ShmemConf::new()
        .size(std::mem::size_of::<QueueHeader>() + 2048)
        .flink(&flink_path)
        .create()
        .unwrap();

    // Initialize a valid queue header.
    let header = unsafe { &mut *(shmem.as_ptr() as *mut QueueHeader) };
    header.elsize = 64;
    header.mask = 127; // capacity = 128
    header.count = AtomicUsize::new(5000);

    // Mark initialized.
    unsafe {
        let is_init_ptr = shmem.as_ptr().add(1) as *mut u8;
        *is_init_ptr = 1;
    }

    // Register two consumer groups and set their cursors.
    let cursor_a = header.find_or_insert_group("builder.events.broadcast");
    unsafe { &*cursor_a }.store(4990, Ordering::Relaxed);

    let cursor_b = header.find_or_insert_group("relay.events.collab");
    unsafe { &*cursor_b }.store(4800, Ordering::Relaxed);

    // Keep shmem alive but don't unlink on drop — the read function opens
    // by flink.
    shmem.set_owner(false);

    // ── Read back through the discovery API ───────────────────────────
    let flink_str = flink_path.to_str().unwrap();
    let groups = read_consumer_groups(flink_str);

    assert_eq!(groups.len(), 2, "should find exactly 2 consumer groups");

    assert_eq!(groups[0].label, "builder.events.broadcast");
    assert_eq!(groups[0].cursor, 4990);

    assert_eq!(groups[1].label, "relay.events.collab");
    assert_eq!(groups[1].cursor, 4800);

    // ── Also verify via scan_base_dir that the queue itself is found ──
    let entries = scan_base_dir(base);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].queue_writes, Some(5000));

    // Cleanup.
    if let Ok(mut shmem) = ShmemConf::new().flink(&flink_path).open() {
        shmem.set_owner(true);
    }
}

/// Smoke test: `read_consumer_groups` returns empty vec for non-existent
/// flinks and non-queue segments.
#[test]
fn consumer_groups_empty_for_missing_or_data_segment() {
    // Non-existent flink.
    let groups = read_consumer_groups("/dev/shm/does_not_exist_abc123");
    assert!(groups.is_empty(), "non-existent flink should return empty");

    // Data segment (not a queue — header won't be initialized as a queue).
    let tmp = tempdir().unwrap();
    let flink_dir = tmp.path().join("app").join("shmem").join("data");
    std::fs::create_dir_all(&flink_dir).unwrap();
    let flink_path = flink_dir.join("Blob");

    let mut shmem = ShmemConf::new().size(256).flink(&flink_path).create().unwrap();
    shmem.set_owner(false);
    drop(shmem);

    let groups = read_consumer_groups(flink_path.to_str().unwrap());
    assert!(groups.is_empty(), "data segment should return empty consumer groups");

    // Cleanup.
    if let Ok(mut shmem) = ShmemConf::new().flink(&flink_path).open() {
        shmem.set_owner(true);
    }
}
