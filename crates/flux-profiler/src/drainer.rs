//! Joins a run's per-thread shmem rings into retained events by sequence
//! number: the producer pushes a mark and its counter samples back-to-back,
//! so entry N of every ring belongs to the same `#[timed]` event. Ring
//! overruns are consumed at the join, so the
//! retained events are always well-formed, sample-aligned stacks and
//! folds/exports need no gap handling of their own.
//!
//! Frame names are resolved as marks are drained, not at read time: a
//! cross-process reader reads them from the live producer's binary, which may
//! be gone by then.

use std::collections::hash_map::Entry;

use rustc_hash::FxHashMap;

use super::{
    allocator::AllocSample,
    fxt,
    mark::{MISSED_ID, Mark},
    perf::{PerfSample, Schema},
    queue_dir::QueueDir,
    ring_drainer::Rings,
    symbols::FrameResolver,
};

/// A drain's render/match labels: frame-id → resolved name, and the perf event
/// vocabulary the counter samples are positional in.
#[derive(Clone)]
pub struct FlamegraphMeta {
    pub names: FxHashMap<u64, String>,
    pub schema: Schema,
}

/// How much of a thread's stream was lost.
///
/// `missed` events were overwritten in a ring before the reader drained them
/// (producer outran reader), and `dropped` drained closes were discarded
/// because a gap took their open. The gaps themselves are retained as
/// `<missed>` frames.
#[derive(Clone, Copy, Default)]
pub struct Loss {
    pub missed: u64,
    pub dropped: u64,
}

impl Loss {
    pub fn is_lossy(&self) -> bool {
        self.missed > 0 || self.dropped > 0
    }
}

/// One drained thread's retained event stream: `marks[i]`, `perf[i]` and
/// `alloc[i]` belong to the same `#[timed]` event (a sample slice is empty
/// when its ring is absent).
pub struct ThreadEvents<'a> {
    pub name: &'a str,
    pub marks: &'a [Mark],
    pub perf: &'a [PerfSample],
    pub alloc: &'a [AllocSample],
    pub loss: Loss,
}

pub struct EventsDrainer {
    dir: QueueDir,
    threads: FxHashMap<String, ThreadDrainer>,
    meta: FlamegraphMeta,
}

impl EventsDrainer {
    pub(super) fn new(dir: QueueDir, schema: Schema) -> Self {
        let mut names = FxHashMap::default();
        names.insert(MISSED_ID, "<missed>".to_string());
        let meta = FlamegraphMeta { names, schema };
        Self { dir, threads: FxHashMap::default(), meta }
    }

    pub(super) fn poll(&mut self, resolver: &impl FrameResolver) {
        for thread in self.dir.event_threads() {
            if let Entry::Vacant(slot) = self.threads.entry(thread) {
                if let Some(thread) = ThreadDrainer::open(&self.dir, slot.key()) {
                    slot.insert(thread);
                }
            }
        }
        for thread in self.threads.values_mut() {
            thread.poll(&mut self.meta.names, resolver);
        }
    }

    pub fn threads(&self) -> impl Iterator<Item = ThreadEvents<'_>> {
        self.threads.iter().map(|(name, t)| ThreadEvents {
            name,
            marks: &t.events.marks,
            perf: &t.events.perf,
            alloc: &t.events.alloc,
            loss: t.loss(),
        })
    }

    pub fn meta(&self) -> &FlamegraphMeta {
        &self.meta
    }

    pub fn retained_bytes(&self) -> usize {
        self.threads.values().map(|t| t.events.retained_bytes()).sum()
    }

    pub fn fxt_trace(&self) -> Vec<u8> {
        fxt::trace(self.threads(), &self.meta)
    }
}

/// Index-aligned event vecs, appended only as a unit so an event's samples
/// never separate from its mark; a sample vec stays empty when its ring is
/// absent.
#[derive(Default)]
struct EventsData {
    marks: Vec<Mark>,
    perf: Vec<PerfSample>,
    alloc: Vec<AllocSample>,
}

impl EventsData {
    fn push(&mut self, mark: Mark, perf: Option<PerfSample>, alloc: Option<AllocSample>) {
        self.marks.push(mark);
        self.perf.extend(perf);
        self.alloc.extend(alloc);
    }

    fn last_samples(&self) -> (Option<PerfSample>, Option<AllocSample>) {
        (self.perf.last().copied(), self.alloc.last().copied())
    }

    fn retained_bytes(&self) -> usize {
        self.marks.capacity() * size_of::<Mark>() +
            self.perf.capacity() * size_of::<PerfSample>() +
            self.alloc.capacity() * size_of::<AllocSample>()
    }
}

struct ThreadDrainer {
    rings: Rings,
    events: EventsData,
    /// Stack of opens whose close hasn't arrived yet.
    open_ids: Vec<u64>,
    unmatched_closes: u64,
    expected_seq: u64,
}

impl ThreadDrainer {
    fn open(dir: &QueueDir, token: &str) -> Option<Self> {
        Some(Self {
            rings: Rings::open(dir, token)?,
            events: EventsData::default(),
            open_ids: Vec::new(),
            unmatched_closes: 0,
            expected_seq: 0,
        })
    }

    fn poll(&mut self, names: &mut FxHashMap<u64, String>, resolver: &impl FrameResolver) {
        self.rings.drain();
        let slowest_cursor = self.rings.slowest_cursor();

        while let Some((seq, mark)) = self.rings.marks.pop_ready(slowest_cursor) {
            let (perf, alloc) = self.take_samples(seq);

            if seq != self.expected_seq {
                self.record_gap(mark.ts, perf, alloc);
            }
            self.expected_seq = seq + 1;

            if mark.is_open() {
                let id = mark.id;
                names.entry(id).or_insert_with(|| {
                    resolver.resolve(id, mark.name_len()).unwrap_or_else(|| format!("unknown_{id}"))
                });
                self.open_ids.push(id);
                self.events.push(mark, perf, alloc);
            } else if let Some(open_id) = self.open_ids.pop() {
                debug_assert_eq!(open_id, mark.id, "timed close under a non-matching open");
                self.events.push(mark, perf, alloc);
            } else {
                self.unmatched_closes += 1;
            }
        }
    }

    /// The samples pushed with mark `seq`, or the last retained ones if a
    /// ring lost it to a hole; `None` when the ring doesn't exist.
    fn take_samples(&mut self, seq: u64) -> (Option<PerfSample>, Option<AllocSample>) {
        let (last_perf, last_alloc) = self.events.last_samples();
        (
            self.rings.perf.as_mut().map(|ring| ring.take_at(seq, last_perf)),
            self.rings.alloc.as_mut().map(|ring| ring.take_at(seq, last_alloc)),
        )
    }

    /// Record a ring hole: every pending open spans it and will never see its
    /// real close (that close later arrives unmatched and drops), so close
    /// them all at the last retained mark and keep the gap as a `<missed>`
    /// frame carrying the gap's time and counter delta.
    fn record_gap(
        &mut self,
        gap_end_ts: u64,
        perf: Option<PerfSample>,
        alloc: Option<AllocSample>,
    ) {
        let Some(gap_start_ts) = self.events.marks.last().map(|mark| mark.ts) else {
            // Nothing retained yet (attached mid-run): no gap to anchor.
            return;
        };
        let (last_perf, last_alloc) = self.events.last_samples();
        while let Some(id) = self.open_ids.pop() {
            self.events.push(Mark::from_parts(id, gap_start_ts, false), last_perf, last_alloc);
        }
        self.events.push(Mark::from_parts(MISSED_ID, gap_start_ts, true), last_perf, last_alloc);
        self.events.push(Mark::from_parts(MISSED_ID, gap_end_ts, false), perf, alloc);
    }

    fn loss(&self) -> Loss {
        Loss { missed: self.rings.missed(), dropped: self.unmatched_closes }
    }
}

#[cfg(test)]
mod tests {
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
        thread.poll(&mut names, &NoResolver);

        // Lap both rings, then recover: everything produced so far after the
        // two retained opens — including close(2)/close(1) — is a hole.
        for _ in 0..RING_CAPACITY as u64 + 5 {
            push(Mark::from_parts(3, 30, true), 300);
        }
        thread.poll(&mut names, &NoResolver);

        // First post-hole events: an unmatched close (its open was lost), then
        // a clean frame.
        push(Mark::from_parts(9, 40, false), 900);
        push(Mark::from_parts(4, 50, true), 1000);
        push(Mark::from_parts(4, 60, false), 1100);
        thread.poll(&mut names, &NoResolver);

        let events: Vec<_> =
            thread.events.marks.iter().map(|m| (m.id, m.is_open(), m.ts)).collect();
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
}
