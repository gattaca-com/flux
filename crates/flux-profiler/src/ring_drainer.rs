use std::collections::VecDeque;

use flux_communication::{
    QueueError, ReadError,
    queue::{ConsumerBare, Queue},
};

use super::{
    allocator::AllocSample,
    mark::Mark,
    perf::PerfSample,
    queue_dir::{QueueDir, RingEntry},
};

pub(super) struct RingDrainer<T: RingEntry> {
    consumer: ConsumerBare<T>,
    pending: VecDeque<T>,
    next_seq: u64,
    missed: u64,
}

impl<T: RingEntry> RingDrainer<T> {
    fn open(dir: &QueueDir, token: &str) -> Result<Self, QueueError> {
        let queue = Queue::<T>::try_open_shared(dir.path::<T>(token))?;
        let mut consumer = ConsumerBare::new(queue, T::PREFIX);

        // Collaborative so the cursor starts at the beginning of the ring, `next_seq`
        // at 0
        consumer.try_init_collaborative();
        Ok(Self { consumer, pending: VecDeque::new(), next_seq: 0, missed: 0 })
    }

    fn drain(&mut self) {
        const DRAIN_BATCH: usize = 512;

        let mut scratch = T::default();
        for _ in 0..DRAIN_BATCH {
            match self.consumer.try_consume(&mut scratch) {
                Ok(()) => {
                    self.pending.push_back(scratch);
                    self.next_seq += 1;
                }
                Err(ReadError::Empty) => break,
                Err(ReadError::SpedPast) => {
                    self.consumer.recover_after_error();
                    let head = self.consumer.queue_message_count() as u64;
                    self.missed += head.saturating_sub(self.next_seq) + self.pending.len() as u64;
                    self.next_seq = head.max(self.next_seq);
                    self.pending.clear();
                }
            }
        }
    }

    fn first_seq(&self) -> u64 {
        self.next_seq - self.pending.len() as u64
    }

    /// Next pending entry strictly below `limit`, with its sequence number
    pub(super) fn pop_ready(&mut self, limit: u64) -> Option<(u64, T)> {
        let seq = self.first_seq();
        if seq >= limit {
            return None;
        }
        self.pending.pop_front().map(|entry| (seq, entry))
    }

    /// Pending entry at `seq`, or `fallback` if the ring lost it
    pub(super) fn take_at(&mut self, seq: u64, fallback: Option<T>) -> T {
        let taken = (seq >= self.first_seq()).then(|| {
            while self.first_seq() < seq {
                self.pending.pop_front();
            }
            self.pending.pop_front()
        });
        taken.flatten().or(fallback).unwrap_or_default()
    }
}

pub(super) struct Rings {
    pub(super) marks: RingDrainer<Mark>,
    pub(super) perf: Option<RingDrainer<PerfSample>>,
    pub(super) alloc: Option<RingDrainer<AllocSample>>,
}

impl Rings {
    pub(super) fn open(dir: &QueueDir, token: &str) -> Result<Self, QueueError> {
        Ok(Self {
            marks: RingDrainer::<Mark>::open(dir, token)?,
            perf: RingDrainer::<PerfSample>::open(dir, token).ok(),
            alloc: RingDrainer::<AllocSample>::open(dir, token).ok(),
        })
    }

    pub(super) fn drain(&mut self) {
        self.marks.drain();
        if let Some(perf) = &mut self.perf {
            perf.drain();
        }
        if let Some(alloc) = &mut self.alloc {
            alloc.drain();
        }
    }

    pub(super) fn slowest_cursor(&self) -> u64 {
        self.marks
            .next_seq
            .min(self.perf.as_ref().map_or(u64::MAX, |r| r.next_seq))
            .min(self.alloc.as_ref().map_or(u64::MAX, |r| r.next_seq))
    }

    pub(super) fn missed(&self) -> u64 {
        self.marks.missed
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::significant_drop_tightening)] // RAII guards held to scope end
    use flux_communication::queue::Producer;

    use super::*;
    use crate::{queue_dir::RING_CAPACITY, test_shmem::ShmemGuard};

    #[test]
    fn overrun_is_counted_exactly_and_indexing_resumes_at_the_head() {
        let guard = ShmemGuard::new();
        let dir = QueueDir::new(guard.app());
        let mut producer = Producer::from(dir.ring::<Mark>("ring-drainer-test"));
        let mut drainer = RingDrainer::<Mark>::open(&dir, "ring-drainer-test").unwrap();

        let mark = Mark::from_parts(1, 0, true);
        for _ in 0..3 {
            producer.produce(&mark);
        }
        drainer.drain();
        assert_eq!(drainer.pop_ready(u64::MAX).unwrap().0, 0);
        assert_eq!(drainer.pop_ready(u64::MAX).unwrap().0, 1);

        let lapped = RING_CAPACITY as u64 + 5;
        for _ in 0..lapped {
            producer.produce(&mark);
        }
        drainer.drain();
        assert_eq!(drainer.missed, lapped + 1);
        assert!(drainer.pop_ready(u64::MAX).is_none(), "nothing readable inside the hole");

        producer.produce(&mark);
        drainer.drain();
        assert_eq!(drainer.pop_ready(u64::MAX).unwrap().0, 3 + lapped, "seq resumes at the head");
        assert_eq!(drainer.missed, lapped + 1, "post-recovery reads are not miscounted");

        for _ in 0..lapped {
            producer.produce(&mark);
        }
        drainer.drain();
        assert_eq!(drainer.missed, 2 * (lapped + 1) - 1, "every hole is counted");
    }

    #[test]
    fn take_at_falls_back_to_last_retained_on_a_hole() {
        let guard = ShmemGuard::new();
        let dir = QueueDir::new(guard.app());
        let mut producer = Producer::from(dir.ring::<AllocSample>("take-at-test"));
        let mut drainer = RingDrainer::<AllocSample>::open(&dir, "take-at-test").unwrap();

        producer.produce(&AllocSample { allocated: 42, freed: 0 });
        drainer.drain();
        let last_joined = drainer.take_at(0, None);
        assert_eq!(last_joined.allocated, 42);

        for _ in 0..RING_CAPACITY as u64 + 5 {
            producer.produce(&AllocSample { allocated: 7, freed: 0 });
        }
        drainer.drain();

        assert_eq!(drainer.take_at(3, Some(last_joined)).allocated, 42, "not a burst sample");
        assert_eq!(drainer.take_at(4, None).allocated, 0, "default when nothing was joined");
    }
}
