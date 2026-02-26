// Integration test to verify queue stats are correctly read during discovery
use std::sync::atomic::AtomicUsize;

use flux_communication::queue::QueueHeader;
use flux_ctl::discovery::scan_base_dir;
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
