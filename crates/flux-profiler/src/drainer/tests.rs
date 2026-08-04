#![allow(clippy::significant_drop_tightening)] // RAII guards held to scope end
use flux_communication::queue::Producer;

use super::*;
use crate::{queue_dir::RING_CAPACITY, test_shmem::ShmemGuard};

struct NoResolver;

impl FrameResolver for NoResolver {
    fn resolve(&self, _id: u64, _len: u16) -> Option<String> {
        None
    }
}

/// A hole closes the frames spanning it with synthetic closes at the last
/// retained mark, records the gap as a `<missed>` span carrying the gap's
/// counter delta, and drops closes whose open was lost — nothing already
/// retained is discarded.
#[test]
fn hole_closes_spanning_frames_and_records_a_missed_span() {
    let guard = ShmemGuard::new();
    let dir = QueueDir::new(guard.app());
    let mut mark_producer = Producer::from(dir.ring::<Mark>("drainer-test"));
    let mut alloc_producer = Producer::from(dir.ring::<AllocSample>("drainer-test"));
    let mut thread = ThreadDrainer::open(&dir, "drainer-test").unwrap();
    let mut names = FxHashMap::default();

    let mut push = |mark: Mark, allocated: u64| {
        mark_producer.produce(&mark);
        alloc_producer.produce(&AllocSample { allocated, freed: 0 });
    };

    push(Mark::from_parts(1, 10, true), 100);
    push(Mark::from_parts(2, 20, true), 200);
    thread.poll(&mut names, &NoResolver, &SocketClocks::identity(), 0);

    // Lap both rings, then recover: everything produced so far after the
    // two retained opens — including close(2)/close(1) — is a hole.
    for _ in 0..RING_CAPACITY as u64 + 5 {
        push(Mark::from_parts(3, 30, true), 300);
    }
    thread.poll(&mut names, &NoResolver, &SocketClocks::identity(), 0);

    // First post-hole events: an unmatched close (its open was lost), then
    // a clean frame.
    push(Mark::from_parts(9, 40, false), 900);
    push(Mark::from_parts(4, 50, true), 1000);
    push(Mark::from_parts(4, 60, false), 1100);
    thread.poll(&mut names, &NoResolver, &SocketClocks::identity(), 0);

    let events: Vec<_> = thread.events.marks.iter().map(|m| (m.id, m.is_open(), m.ts)).collect();
    assert_eq!(events, [
        (1, true, 10),
        (2, true, 20),
        (2, false, 20),
        (1, false, 20),
        (MISSED_ID, true, 20),
        (MISSED_ID, false, 40),
        (4, true, 50),
        (4, false, 60),
    ]);
    let allocated: Vec<_> = thread.events.alloc.iter().map(|a| a.allocated).collect();
    assert_eq!(
        allocated,
        [100, 200, 200, 200, 200, 900, 1000, 1100],
        "closed frames carry the last pre-hole sample; the missed close carries the first \
         post-hole one, so the gap's delta lands on <missed>"
    );
    assert_eq!(thread.unmatched_closes, 1, "only the unmatched close is discarded");
    assert!(thread.loss().missed > 0);
    assert!(thread.open_ids.is_empty());
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

fn retained_marks(drainer: &EventsDrainer) -> Vec<(u64, bool)> {
    drainer.threads().flat_map(|t| t.marks.iter()).map(|m| (m.id, m.is_open())).collect()
}

/// Consecutive dumps partition the event stream: completed frames land in
/// exactly one dump, and a top-level frame still open at dump time stays
/// retained — with its completed subframes — until it closes.
#[test]
fn dumps_partition_the_stream_and_keep_open_frames_whole() {
    let guard = ShmemGuard::new();
    let dir = QueueDir::new(guard.app());
    let mut marks = Producer::from(dir.ring::<Mark>("dump-test-7"));
    let mut drainer = EventsDrainer::new(dir, Schema::empty());

    // A completed frame, then a still-open top-level frame with a
    // completed subframe.
    marks.produce(&Mark::from_parts(1, 10, true));
    marks.produce(&Mark::from_parts(1, 20, false));
    marks.produce(&Mark::from_parts(2, 30, true));
    marks.produce(&Mark::from_parts(3, 40, true));
    marks.produce(&Mark::from_parts(3, 50, false));
    drainer.poll(&NoResolver);

    let mut first = Vec::new();
    drainer.dump_and_release(&mut first).unwrap();
    assert_eq!(&first[..8], b"\x10\x00\x04FxT\x16\x00", "FXT magic record");
    assert!(contains(&first, b"unknown_1"), "the completed frame is dumped");
    assert!(!contains(&first, b"unknown_2"), "the in-flight frame is not");
    assert!(!contains(&first, b"unknown_3"), "its completed subframe stays with it");
    assert_eq!(retained_marks(&drainer), [(2, true), (3, true), (3, false)]);

    // Close the top-level frame: the next dump carries it whole, and
    // nothing from the first dump repeats.
    marks.produce(&Mark::from_parts(2, 60, false));
    drainer.poll(&NoResolver);
    let mut second = Vec::new();
    drainer.dump_and_release(&mut second).unwrap();
    assert!(!contains(&second, b"unknown_1"), "dumps do not overlap");
    assert!(contains(&second, b"unknown_2") && contains(&second, b"unknown_3"));
    assert!(retained_marks(&drainer).is_empty());
}

#[test]
fn dump_reports_interval_loss_once() {
    let guard = ShmemGuard::new();
    let dir = QueueDir::new(guard.app());
    let mut marks = Producer::from(dir.ring::<Mark>("dump-loss-3"));
    let mut drainer = EventsDrainer::new(dir, Schema::empty());

    marks.produce(&Mark::from_parts(1, 10, true));
    marks.produce(&Mark::from_parts(1, 20, false));
    drainer.poll(&NoResolver);
    // Lap the ring: everything produced since the first poll is a hole.
    for _ in 0..RING_CAPACITY as u64 + 5 {
        marks.produce(&Mark::from_parts(2, 30, true));
    }
    drainer.poll(&NoResolver);

    assert!(drainer.threads().all(|t| t.loss.missed > 0), "the hole shows before the dump");
    drainer.dump_and_release(&mut Vec::new()).unwrap();
    assert!(drainer.threads().all(|t| !t.loss.is_lossy()), "the dump resets the interval's loss");

    // A post-hole frame: the dump emptied retention, so the gap has no
    // anchor and no `<missed>` span — the loss was already reported.
    marks.produce(&Mark::from_parts(4, 50, true));
    marks.produce(&Mark::from_parts(4, 60, false));
    drainer.poll(&NoResolver);
    assert_eq!(retained_marks(&drainer), [(4, true), (4, false)]);
    assert!(drainer.threads().all(|t| !t.loss.is_lossy()), "no new loss this interval");

    let mut second = Vec::new();
    drainer.dump_and_release(&mut second).unwrap();
    assert!(contains(&second, b"unknown_4"));
}

#[test]
fn retained_capacity_survives_a_dump() {
    let guard = ShmemGuard::new();
    let dir = QueueDir::new(guard.app());
    let mut marks = Producer::from(dir.ring::<Mark>("dump-cap-1"));
    let mut drainer = EventsDrainer::new(dir, Schema::empty());

    for ts in 0..64 {
        marks.produce(&Mark::from_parts(1, 2 * ts, true));
        marks.produce(&Mark::from_parts(1, 2 * ts + 1, false));
    }
    drainer.poll(&NoResolver);

    let bytes = drainer.retained_bytes();
    assert!(bytes > 0);
    drainer.dump_and_release(&mut Vec::new()).unwrap();
    assert!(retained_marks(&drainer).is_empty());
    assert_eq!(drainer.retained_bytes(), bytes, "capacity is kept for the next interval");
}

#[test]
fn failed_dump_releases_nothing() {
    struct FailingSink;

    impl std::io::Write for FailingSink {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("sink failed"))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let guard = ShmemGuard::new();
    let dir = QueueDir::new(guard.app());
    let mut marks = Producer::from(dir.ring::<Mark>("dump-fail-2"));
    let mut drainer = EventsDrainer::new(dir, Schema::empty());

    marks.produce(&Mark::from_parts(1, 10, true));
    marks.produce(&Mark::from_parts(1, 20, false));
    drainer.poll(&NoResolver);

    assert!(drainer.dump_and_release(FailingSink).is_err());
    assert_eq!(retained_marks(&drainer), [(1, true), (1, false)], "nothing was released");

    let mut out = Vec::new();
    drainer.dump_and_release(&mut out).unwrap();
    assert!(contains(&out, b"unknown_1"), "the events land in the next dump");
    assert!(retained_marks(&drainer).is_empty());
}

#[test]
fn short_top_level_frames_are_discarded() {
    const SHORT: u64 = 100;
    const LONG: u64 = 10_000_000;

    let guard = ShmemGuard::new();
    let dir = QueueDir::new(guard.app());
    let mut mark_producer = Producer::from(dir.ring::<Mark>("filter-test"));
    let mut alloc_producer = Producer::from(dir.ring::<AllocSample>("filter-test"));
    let mut thread = ThreadDrainer::open(&dir, "filter-test").unwrap();
    let mut names = FxHashMap::default();

    let mut push = |mark: Mark, allocated: u64| {
        mark_producer.produce(&mark);
        alloc_producer.produce(&AllocSample { allocated, freed: 0 });
    };

    // Short top-level frame with a nested frame: discarded whole.
    push(Mark::from_parts(1, 1000, true), 100);
    push(Mark::from_parts(2, 1010, true), 200);
    push(Mark::from_parts(2, 1020, false), 300);
    push(Mark::from_parts(1, 1000 + SHORT, false), 400);
    // Long top-level frame with a short nested frame: kept whole.
    push(Mark::from_parts(1, 2000, true), 500);
    push(Mark::from_parts(2, 2010, true), 600);
    push(Mark::from_parts(2, 2020, false), 700);
    push(Mark::from_parts(1, 2000 + LONG, false), 800);
    // Trailing frame with no close yet: kept.
    push(Mark::from_parts(3, 3_000_000_000, true), 900);
    thread.poll(&mut names, &NoResolver, &SocketClocks::identity(), 1000);

    let events: Vec<_> = thread.events.marks.iter().map(|m| (m.id, m.is_open(), m.ts)).collect();
    assert_eq!(events, [
        (1, true, 2000),
        (2, true, 2010),
        (2, false, 2020),
        (1, false, 2000 + LONG),
        (3, true, 3_000_000_000),
    ]);
    let allocated: Vec<_> = thread.events.alloc.iter().map(|a| a.allocated).collect();
    assert_eq!(allocated, [500, 600, 700, 800, 900], "samples truncate with their marks");
    assert!(!thread.loss().is_lossy(), "filtering is not loss");
}
