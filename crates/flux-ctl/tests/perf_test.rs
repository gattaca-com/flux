//! Performance regression test for `scan_base_dir` function.
//!
//! This test creates ~50 mock shmem segments using tempfile dirs with properly
//! structured shmem/{queues,data,arrays}/ layout and valid flink files pointing
//! to real `shared_memory` segments. It times `scan_base_dir` and asserts it
//! completes in <200ms to catch future performance regressions.

use std::sync::atomic::AtomicUsize;

use flux_communication::{ShmemKind, array::ArrayHeader, queue::QueueHeader};
use flux_ctl::discovery::scan_base_dir;
use flux_timing::{Duration, Instant};
use shared_memory::ShmemConf;
use tempfile::tempdir;

/// Helper function to create a queue segment with proper `QueueHeader`
/// initialization
fn create_queue_segment(
    base_dir: &std::path::Path,
    app: &str,
    name: &str,
    elem_size: usize,
    capacity: usize,
) {
    let flink_dir = base_dir.join(app).join("shmem").join("queues");
    std::fs::create_dir_all(&flink_dir).unwrap();
    let flink_path = flink_dir.join(name);

    let header_size = std::mem::size_of::<QueueHeader>();
    let total_size = header_size + (capacity * elem_size);

    let mut shmem = ShmemConf::new().size(total_size).flink(&flink_path).create().unwrap();

    // Initialize header with realistic test data
    #[allow(clippy::cast_ptr_alignment)]
    let header = unsafe { &mut *shmem.as_ptr().cast::<QueueHeader>() };
    header.elsize = elem_size;
    header.mask = capacity - 1; // capacity must be power of 2
    header.count = AtomicUsize::new(capacity / 4); // 25% full

    // Mark as initialized (critical for scan_base_dir)
    unsafe {
        let is_init_ptr = shmem.as_ptr().add(1);
        *is_init_ptr = 1;
    }

    // Don't unlink on drop so scan_base_dir can find it
    shmem.set_owner(false);
    drop(shmem);
}

/// Helper function to create an array segment with proper `ArrayHeader`
/// initialization
fn create_array_segment(
    base_dir: &std::path::Path,
    app: &str,
    name: &str,
    elem_size: usize,
    buf_size: usize,
) {
    let flink_dir = base_dir.join(app).join("shmem").join("arrays");
    std::fs::create_dir_all(&flink_dir).unwrap();
    let flink_path = flink_dir.join(name);

    let header_size = std::mem::size_of::<ArrayHeader>();
    let total_size = header_size + (buf_size * elem_size);

    let mut shmem = ShmemConf::new().size(total_size).flink(&flink_path).create().unwrap();

    // Initialize header
    #[allow(clippy::cast_ptr_alignment)]
    let header = unsafe { &mut *shmem.as_ptr().cast::<ArrayHeader>() };
    header.elsize = elem_size;
    header.bufsize = buf_size;
    header.is_initialized = 1;

    shmem.set_owner(false);
    drop(shmem);
}

/// Helper function to create a data segment
fn create_data_segment(base_dir: &std::path::Path, app: &str, name: &str, size: usize) {
    let flink_dir = base_dir.join(app).join("shmem").join("data");
    std::fs::create_dir_all(&flink_dir).unwrap();
    let flink_path = flink_dir.join(name);

    let mut shmem = ShmemConf::new().size(size).flink(&flink_path).create().unwrap();

    shmem.set_owner(false);
    drop(shmem);
}

/// Helper function to cleanup all segments for a given app
fn cleanup_app_segments(
    base_dir: &std::path::Path,
    app: &str,
    segments: &[(String, &str, ShmemKind)],
) {
    for (name, subdir, _) in segments {
        let flink_path = base_dir.join(app).join("shmem").join(subdir).join(name);
        if let Ok(mut shmem) = ShmemConf::new().flink(&flink_path).open() {
            shmem.set_owner(true); // This will unlink on drop
        }
    }
}

#[test]
fn performance_scan_base_dir_50_segments() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();

    // Track all segments for cleanup
    let mut all_segments: Vec<(String, Vec<(String, &str, ShmemKind)>)> = Vec::new();

    // Create 10 apps with 5 segments each = 50 total segments
    for app_idx in 0..10 {
        let app_name = format!("app{app_idx:02}");
        let mut app_segments = Vec::new();

        // 2 queue segments per app
        for queue_idx in 0..2 {
            let queue_name = format!("Queue{queue_idx}");
            create_queue_segment(base, &app_name, &queue_name, 64, 1024);
            app_segments.push((queue_name, "queues", ShmemKind::Queue));
        }

        // 2 array segments per app
        for array_idx in 0..2 {
            let array_name = format!("Array{array_idx}");
            create_array_segment(base, &app_name, &array_name, 32, 512);
            app_segments.push((array_name, "arrays", ShmemKind::SeqlockArray));
        }

        // 1 data segment per app
        let data_name = "Config".to_string();
        create_data_segment(base, &app_name, &data_name, 256);
        app_segments.push((data_name, "data", ShmemKind::Data));

        all_segments.push((app_name, app_segments));
    }

    // Warm up: do one scan to eliminate any cold start effects
    let _warmup = scan_base_dir(base);

    // Time the actual scan
    let start = Instant::now();
    let entries = scan_base_dir(base);
    let elapsed = start.elapsed();

    // Verify we found all 50 segments
    assert_eq!(entries.len(), 50, "Should discover exactly 50 segments");

    // Verify we found all 10 apps
    let app_names = flux_ctl::discovery::DiscoveredEntry::app_names(&entries);
    assert_eq!(app_names.len(), 10, "Should discover exactly 10 apps");

    // Verify segment distribution by kind
    let queue_count = entries.iter().filter(|e| e.kind == ShmemKind::Queue).count();
    let array_count = entries.iter().filter(|e| e.kind == ShmemKind::SeqlockArray).count();
    let data_count = entries.iter().filter(|e| e.kind == ShmemKind::Data).count();

    assert_eq!(queue_count, 20, "Should have 20 queue segments");
    assert_eq!(array_count, 20, "Should have 20 array segments");
    assert_eq!(data_count, 10, "Should have 10 data segments");

    // Verify queue stats are populated correctly
    let queue_entries: Vec<_> = entries.iter().filter(|e| e.kind == ShmemKind::Queue).collect();
    for entry in queue_entries {
        assert!(entry.queue_writes.is_some(), "Queue segments should have write stats");
        assert!(entry.queue_fill.is_some(), "Queue segments should have fill stats");
        assert_eq!(entry.capacity, 1024, "Queue capacity should be 1024");
        assert_eq!(entry.elem_size, 64, "Queue element size should be 64");

        // Verify the stats values make sense (25% full as configured)
        let writes = entry.queue_writes.unwrap();
        let fill = entry.queue_fill.unwrap();
        assert_eq!(writes, 256, "Queue writes should be 256 (25% of 1024)");
        assert_eq!(fill, 256, "Queue fill should be 256 (256 & 1023)");
    }

    // Verify array segments
    let array_entries: Vec<_> =
        entries.iter().filter(|e| e.kind == ShmemKind::SeqlockArray).collect();
    for entry in array_entries {
        assert!(entry.queue_writes.is_none(), "Array segments should not have queue stats");
        assert!(entry.queue_fill.is_none(), "Array segments should not have queue fill stats");
        assert_eq!(entry.capacity, 512, "Array capacity should be 512");
        assert_eq!(entry.elem_size, 32, "Array element size should be 32");
    }

    // Verify data segments
    let data_entries: Vec<_> = entries.iter().filter(|e| e.kind == ShmemKind::Data).collect();
    for entry in data_entries {
        assert!(entry.queue_writes.is_none(), "Data segments should not have queue stats");
        assert!(entry.queue_fill.is_none(), "Data segments should not have queue fill stats");
        assert_eq!(entry.capacity, 1, "Data capacity should be 1");
        assert_eq!(entry.elem_size, 256, "Data element size should be 256");
    }

    // The key performance assertion: scan_base_dir should complete in <200ms
    let elapsed_ms = elapsed.as_millis();
    println!("scan_base_dir with 50 segments completed in {elapsed_ms}ms");

    assert!(
        elapsed < Duration::from_millis(200),
        "scan_base_dir took {elapsed_ms}ms, expected <200ms - potential performance regression!",
    );

    // Additional performance insights for debugging
    if elapsed_ms > 100.0 {
        println!("WARNING: scan_base_dir took {elapsed_ms}ms - close to the 200ms limit",);
    }

    // Cleanup all segments
    for (app_name, app_segments) in &all_segments {
        cleanup_app_segments(base, app_name, app_segments);
    }
}

#[test]
fn performance_baseline_empty_scan() {
    // Establish baseline performance for empty directory scan
    let tmp = tempdir().unwrap();
    let base = tmp.path();

    let start = Instant::now();
    let entries = scan_base_dir(base);
    let elapsed = start.elapsed();

    assert_eq!(entries.len(), 0, "Empty directory should return no entries");

    let elapsed_ms = elapsed.as_millis();
    println!("Empty scan_base_dir completed in {elapsed_ms}ms");

    // Empty scan should be very fast (<10ms)
    assert!(
        elapsed < Duration::from_millis(10),
        "Empty scan_base_dir took {elapsed_ms}ms, expected <10ms",
    );
}

#[test]
fn performance_single_large_segment() {
    // Test performance with one very large segment to check scaling behavior
    let tmp = tempdir().unwrap();
    let base = tmp.path();

    // Create a large queue (4M slots = 4MB ring buffer + header)
    create_queue_segment(base, "bigapp", "LargeQueue", 1, 4 * 1024 * 1024);

    let start = Instant::now();
    let entries = scan_base_dir(base);
    let elapsed = start.elapsed();

    assert_eq!(entries.len(), 1, "Should discover the large segment");
    assert_eq!(entries[0].capacity, 4 * 1024 * 1024, "Should read correct capacity");

    let elapsed_ms = elapsed.as_millis();
    println!("scan_base_dir with 1 large segment completed in {elapsed_ms}ms");

    // Even large segments should scan quickly since we only read headers
    assert!(
        elapsed < Duration::from_millis(50),
        "Large segment scan took {elapsed_ms}ms, expected <50ms",
    );

    // Cleanup
    cleanup_app_segments(base, "bigapp", &[("LargeQueue".to_string(), "queues", ShmemKind::Queue)]);
}
