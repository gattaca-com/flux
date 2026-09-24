use std::{marker::PhantomData, mem, ptr, rc::Rc};

use flux::{
    communication::{
        ShmemData, cleanup_shmem,
        queue::spsc::{Queue, QueueError},
    },
    spine::{DCacheMsg, DCacheRead, FluxSpine, SpineAdapter, SpineProducers, SpscProduceError},
    tile::{Tile, TileInfo},
};
use flux_timing::{IngestionTime, InternalMessage, TrackingTimestamp};
use flux_utils::directories::shmem_dir_with_base;
use spine_derive::from_spine;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
struct Reading(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
struct Frame(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
struct DefaultReading(u64);

// The slot is only a layout marker. Neither its bytes nor its Rc are
// constructed.
#[repr(C, align(128))]
struct Slot256 {
    bytes: [u8; 256],
    no_send: PhantomData<Rc<()>>,
}

#[from_spine("spsc-slot-layout")]
#[derive(Debug)]
struct LayoutSpine {
    tile_info: ShmemData<TileInfo>,
    #[queue(size(2), flavour("spsc"), slot(Slot256))]
    readings: flux::spine::SpineQueue<Reading>,
    #[queue(size(2), flavour("spsc"))]
    defaults: flux::spine::SpineQueue<DefaultReading>,
    #[queue(size(2), flavour("spsc"), mtu(16), slot(Slot256))]
    frames: flux::spine::SpineQueue<Frame>,
}

#[derive(Clone, Copy)]
struct Sender;
impl<S: FluxSpine> Tile<S> for Sender {
    fn loop_body(&mut self, _: &mut SpineAdapter<S>) {}
}

#[derive(Clone, Copy)]
struct Receiver;
impl<S: FluxSpine> Tile<S> for Receiver {
    fn loop_body(&mut self, _: &mut SpineAdapter<S>) {}
}

fn new_spine(base: &std::path::Path) -> LayoutSpine {
    // SAFETY: all endpoints use the same schema on one architecture and the
    // payloads contain no process-local pointers.
    unsafe { LayoutSpine::new_with_base_dir(base, None) }
}

#[test]
fn slot_marker_does_not_constrain_endpoint_traits() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    assert_send_sync::<flux::spine::SpineSpscQueue<Reading, Slot256>>();
    assert_send::<flux::spine::SpineSpscProducer<Reading, Slot256>>();
    assert_send::<flux::spine::SpineSpscConsumer<Reading, Slot256>>();
    assert_send_sync::<flux::spine::SpineSpscDCacheQueue<Frame, Slot256>>();
}

#[test]
fn padded_slots_preserve_addresses_values_and_factory_capacity() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    let mut producer = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut consumer = SpineAdapter::connect_tile(&Receiver, &mut spine);

    producer.try_produce(DefaultReading(9)).unwrap();
    producer.try_produce_with(|| Reading(1)).unwrap();
    producer.try_produce_with(|| Reading(2)).unwrap();
    let mut called = false;
    assert!(matches!(
        producer.try_produce_with(|| {
            called = true;
            Reading(3)
        }),
        Err(SpscProduceError::Full)
    ));
    assert!(!called);

    let mut addresses = Vec::new();
    for expected in [1, 2] {
        assert!(
            consumer
                .consume_ref_one::<Reading, _>(|reading, _| {
                    assert_eq!(*reading, Reading(expected));
                    addresses.push(ptr::from_ref(reading) as usize);
                })
                .unwrap()
        );
    }
    assert_eq!(addresses[1] - addresses[0], mem::size_of::<Slot256>());
    let sample = InternalMessage::new(TrackingTimestamp::new(1), Reading(0));
    let data_offset = ptr::from_ref(sample.data()) as usize - ptr::from_ref(&sample) as usize;
    assert_eq!((addresses[0] - data_offset) % mem::align_of::<Slot256>(), 0);
    assert!(
        consumer
            .try_consume_one::<DefaultReading, _>(|value, _| assert_eq!(value, DefaultReading(9)))
            .unwrap()
    );

    producer.try_produce(Reading(3)).unwrap();
    let mut wrapped = 0;
    assert!(
        consumer
            .consume_ref_one::<Reading, _>(|reading, _| {
                assert_eq!(*reading, Reading(3));
                wrapped = ptr::from_ref(reading) as usize;
            })
            .unwrap()
    );
    assert_eq!(wrapped, addresses[0]);
    drop(producer);
    drop(consumer);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn padded_slot_forwards_tracking_and_managed_payloads_reuse() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    // The managed consumer exposes payload bytes, not metadata slot addresses.
    // Reopen its metadata queue to check that the macro applied the slot layout.
    let path = shmem_dir_with_base(tmp.path(), "spsc-slot-layout").join("spsc/frames");
    // SAFETY: the exact stored payload/schema, no endpoints or fork; the
    // mismatched default slot is rejected before accessing any payload.
    unsafe {
        drop(Queue::<InternalMessage<DCacheMsg<Frame>>, Slot256>::open_shared(&path).unwrap());
        assert!(matches!(
            Queue::<InternalMessage<DCacheMsg<Frame>>>::open_shared(&path),
            Err(QueueError::IncompatibleLayout)
        ));
    }
    let mut producer = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut consumer = SpineAdapter::connect_tile(&Receiver, &mut spine);

    let ingestion = IngestionTime::now();
    let message =
        InternalMessage::new(TrackingTimestamp::new(7).with_ingestion_t(ingestion), Reading(14));
    producer.producers.try_forward(&message).unwrap();
    assert!(
        consumer
            .try_consume_internal_message_one_maybe_track::<Reading, _>(|received, producers| {
                assert_eq!(received.data(), &Reading(14));
                assert_eq!(received.ingestion_time(), ingestion);
                assert_eq!(producers.timestamp().ingestion_t(), ingestion);
                false
            })
            .unwrap()
    );

    producer
        .try_produce_with_dcache(
            Frame(1),
            Some((3, |bytes: &mut [u8]| bytes.copy_from_slice(b"one"))),
        )
        .unwrap();
    producer.try_produce_with_dcache(Frame(2), None::<(usize, fn(&mut [u8]))>).unwrap();
    let mut seen = Vec::new();
    for _ in 0..2 {
        assert!(
            consumer
                .try_consume_with_dcache_one::<Frame, _, _, _>(
                    |_, bytes| bytes.to_vec(),
                    |result, _| match result {
                        DCacheRead::Ok((frame, bytes)) => seen.push((frame.0, Some(bytes))),
                        DCacheRead::NoRef(frame) => seen.push((frame.0, None)),
                        other => panic!("unexpected result: {other:?}"),
                    },
                )
                .unwrap()
        );
    }
    assert_eq!(seen, [(1, Some(b"one".to_vec())), (2, None)]);
    producer
        .try_produce_with_dcache(
            Frame(3),
            Some((3, |bytes: &mut [u8]| bytes.copy_from_slice(b"two"))),
        )
        .unwrap();
    assert!(
        consumer
            .try_consume_with_dcache_one::<Frame, _, _, _>(
                |_, bytes| bytes.to_vec(),
                |result, _| assert!(
                    matches!(result, DCacheRead::Ok((Frame(3), bytes)) if bytes == b"two")
                ),
            )
            .unwrap()
    );
    drop(producer);
    drop(consumer);
    drop(spine);
    cleanup_shmem(tmp.path());
}
