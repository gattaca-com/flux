use std::{
    cmp::max,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

pub struct SharedVector<T> {
    data: Box<[spin::RwLock<Option<Arc<T>>>]>,
    next_position: AtomicUsize,
    start_clearing_position: AtomicUsize,
    mask: usize,
}

impl<T> Default for SharedVector<T> {
    fn default() -> Self {
        Self::with_capacity(16 * 1024)
    }
}

impl<T> SharedVector<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        let real_size = capacity.next_power_of_two();
        let mut uninit_data = Box::new_uninit_slice(real_size);

        for elem in &mut uninit_data {
            elem.write(spin::RwLock::new(None));
        }

        let data = unsafe { uninit_data.assume_init() };

        Self {
            data,
            next_position: AtomicUsize::new(0),
            start_clearing_position: AtomicUsize::new(0),
            mask: real_size - 1,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    #[inline]
    fn convert_to_index(&self, position: usize) -> usize {
        position & self.mask
    }

    #[inline]
    fn is_outdated_position(&self, position: usize) -> bool {
        position + self.capacity() < self.next_position.load(Ordering::Acquire)
    }

    #[inline]
    fn push_internal(&self, item: Option<Arc<T>>) -> usize {
        let current_position = self.next_position.fetch_add(1, Ordering::Release);
        let current_index = self.convert_to_index(current_position);
        let current_address = unsafe { self.data.get_unchecked(current_index) };

        {
            let mut guard = current_address.write();
            *guard = item;
        }

        current_position
    }

    /// Pushes an item and returns id of the latest item.
    /// Returns the position where the item was pushed.
    /// Designed for multiple producer usage.
    #[inline]
    pub fn push(&self, item: T) -> usize {
        self.push_internal(Some(Arc::new(item)))
    }

    /// Retrieves an item by index.
    /// Designed for multiple concurrent readers.
    #[inline]
    pub fn get(&self, position: usize) -> Option<Arc<T>> {
        let index = self.convert_to_index(position);
        let position_address = unsafe { self.data.get_unchecked(index) };

        let result;
        {
            let guard = position_address.read();
            result = guard.as_ref().map(Arc::clone);
        }
        if self.is_outdated_position(position) {
            return None;
        }
        result
    }

    /// Clears all items and invalidates existing indices, only objects that was
    /// received from get will be still valid. Returns the latest position
    /// which was cleared. Designed for only single clearing at a time and
    /// supports multiple push and get at the same time.
    #[inline]
    pub fn clear(&self) -> usize {
        let last_pos = self.next_position.load(Ordering::Acquire);
        let start_pos = max(
            last_pos.saturating_sub(self.capacity()),
            self.start_clearing_position.load(Ordering::Relaxed),
        );
        if last_pos == start_pos {
            return last_pos.saturating_sub(1);
        }

        let capacity_used = (last_pos.saturating_sub(start_pos)) as f64 / self.capacity() as f64;

        if capacity_used > 0.9 {
            tracing::warn!(
                "Shared vector reached more than {}% of the capacity between clearing, consider increasing capacity",
                capacity_used * 100.0
            );
        }

        for i in start_pos..last_pos {
            let pos = self.convert_to_index(i);
            let current_address = unsafe { self.data.get_unchecked(pos) };
            {
                let mut guard = current_address.write();
                *guard = None;
            }
        }

        // Clear called only from single thread, so we can use relaxed ordering.
        // Variable is atomic to allow use immutable shared_vector in clear from Arc.
        self.start_clearing_position.store(last_pos, Ordering::Relaxed);
        last_pos.saturating_sub(1)
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn test_new_shared_vector() {
        let vector: SharedVector<i32> = SharedVector::default();
        assert!(vector.get(0).is_none());
    }

    #[test]
    fn test_push_and_get() {
        let vector = SharedVector::default();

        let index = vector.push(42);
        let retrieved = vector.get(index);

        assert!(retrieved.is_some());
        assert_eq!(*retrieved.unwrap(), 42);
    }

    #[test]
    fn test_push_multiple_items() {
        let vector = SharedVector::default();

        let index1 = vector.push(1);
        let index2 = vector.push(2);
        let index3 = vector.push(3);

        assert_eq!(*vector.get(index1).unwrap(), 1);
        assert_eq!(*vector.get(index2).unwrap(), 2);
        assert_eq!(*vector.get(index3).unwrap(), 3);
    }

    #[test]
    fn test_clear_invalidates_indices() {
        let vector = SharedVector::default();

        let index1 = vector.push(100);
        let index2 = vector.push(200);

        assert_eq!(*vector.get(index1).unwrap(), 100);
        assert_eq!(*vector.get(index2).unwrap(), 200);

        vector.clear();

        assert!(vector.get(index1).is_none());
        assert!(vector.get(index2).is_none());
    }

    #[test]
    fn test_push_after_clear() {
        let vector = SharedVector::default();

        let old_index = vector.push(1);
        vector.clear();
        let new_index = vector.push(2);

        assert!(vector.get(old_index).is_none());
        assert_eq!(*vector.get(new_index).unwrap(), 2);
    }

    #[test]
    fn test_custom_struct() {
        #[derive(Clone, Debug, PartialEq)]
        struct TestData {
            id: u32,
            name: String,
        }

        let vector = SharedVector::default();
        let test_item = TestData { id: 1, name: "test".to_string() };

        let index = vector.push(test_item.clone());
        let retrieved = vector.get(index);

        assert!(retrieved.is_some());
        assert_eq!(*retrieved.unwrap(), test_item);
    }

    #[test]
    fn test_multiple_clears() {
        let vector = SharedVector::default();

        let index = vector.push(1);
        assert_eq!(*vector.get(index).unwrap(), 1);

        vector.clear();
        vector.clear();

        assert!(vector.get(index).is_none());

        let new_index = vector.push(2);
        assert_eq!(*vector.get(new_index).unwrap(), 2);
    }

    #[test]
    fn test_arc_sharing() {
        let vector = SharedVector::default();

        let index = vector.push(42);
        let element = vector.get(index).unwrap();
        vector.clear();

        assert_eq!(*element, 42);
    }

    #[test]
    fn test_sequential_read() {
        const NUM_READERS: usize = 4;
        const N: usize = 1_000_000;

        let vector = Arc::new(SharedVector::with_capacity(N));
        vector.push(0);

        thread::scope(|s| {
            {
                let vector = vector.clone();
                s.spawn(move || {
                    for i in 1..N {
                        vector.push(i);
                    }
                });
            }

            for _ in 0..NUM_READERS {
                let vector = vector.clone();
                s.spawn(move || {
                    let mut i = 0;
                    while i < N {
                        if let Some(value) = vector.get(i) {
                            assert_eq!(*value, i);
                            i += 1;
                        }
                    }
                });
            }
        });
    }

    #[test]
    fn test_random_read() {
        const NUM_READERS: usize = 4;
        const N: usize = 1_000_000;

        let vector = Arc::new(SharedVector::with_capacity(N));
        vector.push(0);

        let latest_id = Arc::new(AtomicUsize::new(0));
        thread::scope(|s| {
            {
                let vector = vector.clone();
                let latest_id = latest_id.clone();
                s.spawn(move || {
                    for i in 1..N {
                        latest_id.store(vector.push(i), Ordering::Release);
                    }
                });
            }

            for _ in 0..NUM_READERS {
                let vector = vector.clone();
                let latest_id = latest_id.clone();
                s.spawn(move || {
                    use rand::Rng;
                    let mut rng = rand::rng();
                    for _ in 0..N {
                        let vector_size = latest_id.load(Ordering::Acquire) + 1;
                        let element_id = rng.random_range(0..vector_size);

                        let value = vector.get(element_id).unwrap();
                        assert_eq!(*value, element_id);
                    }
                });
            }
        });
    }

    #[test]
    fn test_latest_read() {
        const NUM_READERS: usize = 4;
        const N: usize = 1_000_000;

        // Capacity is 1 to catch more concurrency issues which rarely happen, but still
        // possible
        let vector = Arc::new(SharedVector::with_capacity(1));
        vector.push(0);

        let latest_id = Arc::new(AtomicUsize::new(0));
        thread::scope(|s| {
            {
                let vector = vector.clone();
                let latest_id = latest_id.clone();
                s.spawn(move || {
                    for i in 1..N {
                        latest_id.store(vector.push(i), Ordering::Release);
                    }
                });
            }

            for _ in 0..NUM_READERS {
                let vector = vector.clone();
                let latest_id = latest_id.clone();
                s.spawn(move || {
                    for _ in 0..N {
                        let latest_id = latest_id.load(Ordering::Acquire);
                        if let Some(value) = vector.get(latest_id) {
                            assert_eq!(*value, latest_id);
                        }
                    }
                });
            }
        });
    }

    #[test]
    fn test_latest_read_with_clear() {
        use rand::Rng;

        const NUM_READERS: usize = 4;
        const N: usize = 10_000_000;

        let vector = Arc::new(SharedVector::with_capacity(32));

        let success_count = Arc::new(AtomicUsize::new(0));
        let read_count = Arc::new(AtomicUsize::new(0));

        let reader_latest_id = Arc::new(AtomicUsize::new(usize::MAX));
        thread::scope(|s| {
            {
                let vector = vector.clone();
                let reader_latest_id = reader_latest_id.clone();
                s.spawn(move || {
                    let mut rng = rand::rng();
                    let mut writer_latest_id = vector.push(0);
                    for _ in 1..N {
                        let operation = rng.random_range(0..1_000_000);
                        match operation {
                            0..100 => {
                                reader_latest_id.store(usize::MAX, Ordering::Release);
                                writer_latest_id = vector.clear();
                            }
                            100..1_000 => {
                                std::thread::sleep(std::time::Duration::from_micros(1));
                            }
                            _ => {
                                writer_latest_id = vector.push(writer_latest_id + 1);
                                reader_latest_id.store(writer_latest_id, Ordering::Release);
                            }
                        }
                    }
                });
            }

            for _ in 0..NUM_READERS {
                let vector = vector.clone();
                let reader_latest_id = reader_latest_id.clone();
                let success_count = success_count.clone();
                let read_count = read_count.clone();
                s.spawn(move || {
                    let mut rng = rand::rng();
                    for _ in 0..N {
                        let latest_id = loop {
                            let id = reader_latest_id.load(Ordering::Acquire);
                            if id != usize::MAX {
                                break id;
                            }
                        };
                        if rng.random_range(0..1_000) == 0 {
                            let sleep_time = rng.random_range(0..100);
                            std::thread::sleep(std::time::Duration::from_micros(sleep_time));
                        }

                        read_count.fetch_add(1, Ordering::Relaxed);
                        if let Some(value) = vector.get(latest_id) {
                            assert_eq!(*value, latest_id);
                            success_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });

        let read_rate = read_count.load(Ordering::Relaxed) as f64 / (N * NUM_READERS) as f64;
        let success_read_rate = success_count.load(Ordering::Relaxed) as f64 /
            (read_count.load(Ordering::Relaxed)) as f64;

        assert!(read_rate > 0.99, "read_rate: {read_rate}");
        assert!(success_read_rate > 0.99, "success_read_rate: {success_read_rate}");

        println!(
            "Success rate: {}, operations: {}",
            success_read_rate,
            read_count.load(Ordering::Relaxed)
        );
    }
}
