use super::*;

#[repr(C, align(64))]
struct Slot64 {
    _bytes: [u8; 64],
    _not_send: PhantomData<std::rc::Rc<()>>,
}

impl Drop for Slot64 {
    fn drop(&mut self) {
        panic!("a slot layout marker must never be dropped");
    }
}

#[test]
fn padded_slots_preserve_addresses_fifo_and_endpoint_handoff() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    assert_send_sync::<Queue<u64, Slot64>>();
    assert_send::<Producer<u64, Slot64>>();
    assert_send::<Consumer<u64, Slot64>>();

    let queue = Queue::<u64, Slot64>::new(4);
    let alias = queue.clone();
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    let mut addresses = Vec::new();
    for batch in 0..3 {
        for i in 0..queue.capacity() {
            producer.produce(&((batch * queue.capacity() + i) as u64)).unwrap();
        }
        assert_eq!(producer.produce(&99), Err(FullError));
        for i in 0..queue.capacity() {
            assert!(consumer.consume_ref(|value| {
                assert_eq!(*value, (batch * queue.capacity() + i) as u64);
                let address = std::ptr::from_ref(value).addr();
                assert_eq!(address % align_of::<Slot64>(), 0);
                if batch == 0 {
                    addresses.push(address);
                } else {
                    assert_eq!(address, addresses[i], "ring reuse retains its slot geometry");
                }
                if i == 0 {
                    assert_eq!(producer.produce(&99), Err(FullError), "borrow holds the slot");
                }
            }));
        }
        drop(producer);
        producer = alias.try_producer().unwrap();
        drop(consumer);
        consumer = alias.try_consumer().unwrap();
    }
    for pair in addresses.windows(2) {
        assert_eq!(pair[1] - pair[0], size_of::<Slot64>());
    }
    drop(queue);
    drop(alias);
    drop(consumer);
    std::thread::spawn(move || drop(producer)).join().unwrap();
}

#[test]
fn zero_sized_payloads_can_use_nonzero_slots() {
    let queue = Queue::<(), Slot64>::new(2);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    producer.produce(&()).unwrap();
    producer.produce(&()).unwrap();
    assert_eq!(producer.produce(&()), Err(FullError));
    let mut addresses = Vec::new();
    for _ in 0..queue.capacity() {
        assert!(consumer.consume_ref(|value| addresses.push(std::ptr::from_ref(value).addr())));
    }
    assert_eq!(addresses[0] % align_of::<Slot64>(), 0);
    assert_eq!(addresses[1] - addresses[0], size_of::<Slot64>());
    assert!(!consumer.consume_ref(|()| panic!("queue is empty")));
}

#[test]
fn sequence_rollover_preserves_slot_ownership() {
    let queue = Queue::<u64>::new(4);
    // Counter wrap is part of the sequence contract. Seed an empty queue near
    // wrap, with no endpoints attached, instead of performing usize::MAX writes.
    let start = usize::MAX - 1;
    queue.storage.header().write.0.store(start, Ordering::Relaxed);
    queue.storage.header().read.0.store(start, Ordering::Relaxed);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    for batch in 0..3 {
        for i in 0..queue.capacity() {
            let value = batch * queue.capacity() + i;
            let next = producer.next_sequence();
            assert_eq!(next, start.wrapping_add(value));
            assert_eq!(producer.produce(&(value as u64)), Ok(next));
        }
        let next = producer.next_sequence();
        assert_eq!(producer.produce(&99), Err(FullError));
        assert_eq!(producer.next_sequence(), next);
        assert_eq!(consumer.queue_message_count(), queue.capacity());
        for i in 0..queue.capacity() {
            let mut value = 0;
            if i % 2 == 0 {
                consumer.try_consume(&mut value).unwrap();
            } else {
                assert!(consumer.consume_ref(|message| value = *message));
            }
            assert_eq!(value, (batch * queue.capacity() + i) as u64);
            if i == 1 {
                drop(consumer);
                consumer = queue.try_consumer().unwrap();
            }
        }
        assert_eq!(producer.max_writable_msgs_without_speeding_past(), queue.capacity());
        drop(producer);
        producer = queue.try_producer().unwrap();
        assert_eq!(producer.next_sequence(), next);
    }
}

#[test]
fn invalid_capacities_are_rejected_before_allocation() {
    for capacity in [0, usize::MAX, 1 << (usize::BITS - 1)] {
        assert!(std::panic::catch_unwind(|| Queue::<u64>::new(capacity)).is_err());
    }
}

#[test]
fn aligned_and_zero_sized_payloads() {
    #[derive(Clone, Copy)]
    #[repr(C, align(4096))]
    struct Aligned(u64);
    #[derive(Clone, Copy)]
    #[repr(align(4096))]
    struct AlignedZero;

    let queue = Queue::<_>::new(2);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    producer.produce(&Aligned(17)).unwrap();
    producer.produce(&Aligned(18)).unwrap();
    let mut value = Aligned(0);
    consumer.try_consume(&mut value).unwrap();
    assert_eq!(value.0, 17);
    assert!(consumer.consume_ref(|message| {
        assert_eq!(std::ptr::from_ref(message).addr() % align_of::<Aligned>(), 0);
        assert_eq!(message.0, 18);
    }));

    let queue = Queue::<_>::new(1);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    producer.produce(&()).unwrap();
    assert_eq!(producer.produce(&()), Err(FullError));
    consumer.try_consume(&mut ()).unwrap();
    assert_eq!(consumer.try_consume(&mut ()), Err(EmptyError::Empty));

    let queue = Queue::<_>::new(2);
    let mut producer = queue.try_producer().unwrap();
    let mut consumer = queue.try_consumer().unwrap();
    producer.produce(&AlignedZero).unwrap();
    producer.produce(&AlignedZero).unwrap();
    assert_eq!(producer.produce(&AlignedZero), Err(FullError));
    consumer.try_consume(&mut AlignedZero).unwrap();
    assert!(consumer.consume_ref(|message| {
        assert_eq!(std::ptr::from_ref(message).addr() % align_of::<AlignedZero>(), 0);
    }));
    assert_eq!(consumer.try_consume(&mut AlignedZero), Err(EmptyError::Empty));
}

#[cfg(not(miri))]
mod shared_layout {
    use super::*;

    #[test]
    fn padded_shared_slots_validate_both_layouts_and_preserve_unread_data() {
        #[repr(C, align(64))]
        struct EquivalentSlot([u64; 8]);
        #[repr(C, align(128))]
        struct LargerSlot([u8; 128]);
        #[repr(C, align(32))]
        struct DifferentAlignment([u8; 64]);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("padded");
        // SAFETY: every successful participant uses the same u64 payload and
        // matching slot geometry; no endpoint accesses the mismatch fixtures.
        let queue = unsafe { Queue::<u64, Slot64>::create_or_open_shared(&path, 2) }.unwrap();
        let mut producer = queue.try_producer().unwrap();
        producer.produce(&11).unwrap();
        producer.produce(&22).unwrap();
        drop(producer);
        drop(queue);
        // SAFETY: the layout marker's identity is immaterial; its geometry matches.
        let reopened = unsafe { Queue::<u64, EquivalentSlot>::open_shared(&path) }.unwrap();
        assert!(matches!(
            unsafe { Queue::<u64>::open_shared(&path) },
            Err(QueueError::IncompatibleLayout)
        ));
        assert!(matches!(
            unsafe { Queue::<u64, LargerSlot>::open_shared(&path) },
            Err(QueueError::IncompatibleLayout)
        ));
        assert!(matches!(
            unsafe { Queue::<u64, DifferentAlignment>::open_shared(&path) },
            Err(QueueError::IncompatibleLayout)
        ));
        assert!(matches!(
            unsafe { Queue::<[u64; 2], Slot64>::open_shared(&path) },
            Err(QueueError::IncompatibleLayout)
        ));
        assert!(matches!(
            unsafe { Queue::<[u8; 8], Slot64>::open_shared(&path) },
            Err(QueueError::IncompatibleLayout)
        ));

        let mut consumer = reopened.try_consumer().unwrap();
        let mut addresses = Vec::new();
        for expected in [11, 22] {
            assert!(consumer.consume_ref(|value| {
                assert_eq!(*value, expected);
                addresses.push(std::ptr::from_ref(value).addr());
            }));
        }
        assert_eq!(addresses[0] % align_of::<EquivalentSlot>(), 0);
        assert_eq!(addresses[1] - addresses[0], size_of::<EquivalentSlot>());
        drop(consumer);
        drop(reopened);
        crate::cleanup_flink(&path).unwrap();
    }

    // These fixtures model incomplete/incompatible mappings, with no live
    // endpoints. The public open operation must reject them before slot access.
    fn check_header(mut change: impl FnMut(*mut Header), expected_uninitialized: bool) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spsc");
        let shared = ShmemConf::new().flink(&path).size(size_of::<Header>()).create().unwrap();
        let ptr = NonNull::new(shared.as_ptr()).unwrap().cast::<Header>().as_ptr();
        // SAFETY: a fresh, aligned mapping holds a Header; no other references
        // exist. Fields not assigned here have valid zero representations.
        unsafe {
            (&raw mut (*ptr).capacity).write(1);
            (&raw mut (*ptr).element_size).write(size_of::<u64>());
            (&raw mut (*ptr).element_align).write(align_of::<u64>());
            (&raw mut (*ptr).payload_size).write(size_of::<u64>());
            (&raw mut (*ptr).payload_align).write(align_of::<u64>());
            (*ptr).ready.store(MAGIC, Ordering::Release);
        }
        change(ptr);
        // SAFETY: fixture fields are initialized, no process accesses slots,
        // and the malformed layout must be rejected before a queue is returned.
        let result = unsafe { Queue::<u64>::open_shared(&path) };
        if expected_uninitialized {
            assert!(matches!(result, Err(QueueError::Uninitialized)));
        } else {
            assert!(matches!(
                result,
                Err(QueueError::IncompatibleLayout | QueueError::InvalidCapacity)
            ));
        }
    }

    #[test]
    fn incomplete_and_incompatible_mappings_are_rejected() {
        // SAFETY: check_header provides exclusive access to valid header memory.
        check_header(|ptr| unsafe { (*ptr).ready.store(0, Ordering::Release) }, true);
        check_header(|ptr| unsafe { (*ptr).ready.store(MAGIC ^ 1, Ordering::Release) }, false);
        check_header(|ptr| unsafe { (*ptr).element_size = 1 }, false);
        check_header(|ptr| unsafe { (*ptr).element_align = 1 }, false);
        check_header(|ptr| unsafe { (*ptr).payload_size = 1 }, false);
        check_header(|ptr| unsafe { (*ptr).payload_align = 1 }, false);
        check_header(
            |ptr| unsafe {
                (*ptr).ready.store(u64::from_le_bytes(*b"FXSPSC01"), Ordering::Release);
            },
            false,
        );
        for capacity in [0, 3, usize::MAX, 1 << (usize::BITS - 1)] {
            check_header(|ptr| unsafe { (*ptr).capacity = capacity }, false);
        }
        // A plausible header without enough room for even one slot.
        check_header(|_| {}, false);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short");
        let _shared = ShmemConf::new().flink(&path).size(1).create().unwrap();
        // SAFETY: the fixture contains no header or messages; open must check
        // its size before dereferencing a Header.
        assert!(matches!(
            unsafe { Queue::<u64>::open_shared(path) },
            Err(QueueError::IncompatibleLayout)
        ));
    }
}
