use std::{
    cell::RefCell,
    panic::{AssertUnwindSafe, catch_unwind},
    process::Command,
};

use flux::{
    communication::{
        ShmemData, cleanup_shmem,
        queue::{Queue, spsc::QueueError},
        timer::TimingMessage,
    },
    spine::{
        DCacheRead, FluxSpine, SpineAdapter, SpineProducers, SpineSpscDCacheQueue,
        SpscDCacheProduceError,
    },
    tile::{Tile, TileInfo},
};
use flux_timing::IngestionTime;
use flux_utils::{DCacheError, directories::shmem_dir_queues_with_base, short_typename};
use spine_derive::from_spine;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
struct Frame(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
struct PlainFrame(u64);

#[from_spine("spsc-dcache-integration")]
#[derive(Debug)]
struct DCacheSpine {
    tile_info: ShmemData<TileInfo>,
    #[queue(size(4), flavour("spsc"), mtu(16))]
    frames: SpineQueue<Frame>,
    #[queue(size(2), flavour("spsc"))]
    plain: flux::spine::SpineQueue<PlainFrame>,
}

#[derive(Clone, Copy, Default)]
struct Sender;

impl<S: FluxSpine> Tile<S> for Sender {
    fn loop_body(&mut self, _: &mut SpineAdapter<S>) {}
}

#[derive(Clone, Copy, Default)]
struct Receiver;

impl<S: FluxSpine> Tile<S> for Receiver {
    fn loop_body(&mut self, _: &mut SpineAdapter<S>) {}
}

fn new_spine(base_dir: &std::path::Path) -> DCacheSpine {
    // SAFETY: every opener uses this schema and architecture. Frames contain
    // no process-local pointers, and no endpoint is inherited through fork.
    unsafe { DCacheSpine::new_with_base_dir(base_dir, None) }
}

fn timing_queues(base_dir: &std::path::Path) -> [Queue<TimingMessage>; 2] {
    let directory = shmem_dir_queues_with_base(base_dir, DCacheSpine::app_name());
    let name = format!(
        "{}-{}",
        <Receiver as Tile<DCacheSpine>>::name(&Receiver),
        short_typename::<Frame>()
    );
    ["timing", "latency"].map(|kind| Queue::open_shared(directory.join(format!("{kind}-{name}"))))
}

#[test]
fn plain_spsc_factory_runs_only_for_free_slots_and_panic_does_not_publish() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    let mut sender = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut receiver = SpineAdapter::connect_tile(&Receiver, &mut spine);
    sender.try_produce_with(|| PlainFrame(1)).unwrap();

    // A panicking factory cannot skip a slot or replace the unread sentinel.
    for _ in 0..5 {
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                sender
                    .try_produce_with(|| -> PlainFrame { panic!("intentional factory panic") })
                    .unwrap();
            }))
            .is_err()
        );
    }
    sender.try_produce_with(|| PlainFrame(2)).unwrap();
    let mut called = false;
    assert!(matches!(
        sender.try_produce_with(|| {
            called = true;
            PlainFrame(3)
        }),
        Err(flux::spine::SpscProduceError::Full)
    ));
    assert!(!called, "Full must skip the plain message factory");

    let mut observed = Vec::new();
    receiver.try_consume(|frame: PlainFrame, _| observed.push(frame.0)).unwrap();
    assert_eq!(observed, [1, 2]);

    drop(sender);
    drop(receiver);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn dcache_full_and_invalid_length_skip_writer_and_preserve_unread_payloads() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    let mut sender = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut receiver = SpineAdapter::connect_tile(&Receiver, &mut spine);
    let payloads = [b"alpha".as_slice(), b"bravo", b"charlie", b"delta"];

    sender.begin_loop(IngestionTime::now());
    for (index, bytes) in payloads.iter().enumerate() {
        sender
            .try_produce_with_dcache(
                Frame(index as u64),
                Some((bytes.len(), |slot: &mut [u8]| {
                    slot.copy_from_slice(bytes);
                })),
            )
            .unwrap();
    }
    assert!(sender.did_work());
    sender.begin_loop(IngestionTime::now());
    let mut called = false;
    assert!(matches!(
        sender.try_produce_with_dcache(Frame(99), Some((1, |_: &mut [u8]| called = true))),
        Err(SpscDCacheProduceError::Full)
    ));
    assert!(!called, "Full must not run the payload writer");
    assert!(!sender.did_work());

    for length in [0, 17] {
        assert!(matches!(
            sender.try_produce_with_dcache(
                Frame(99),
                Some((length, |_: &mut [u8]| {
                    called = true;
                }))
            ),
            Err(SpscDCacheProduceError::Payload(
                DCacheError::ReserveZero | DCacheError::DataLenExceedsCapacity(..)
            ))
        ));
        assert!(!called, "invalid length must not run the payload writer");
        assert!(!sender.did_work());
    }

    let mut observed = Vec::new();
    receiver
        .try_consume_with_dcache(
            |frame: Frame, bytes| (frame.0, bytes.to_vec()),
            |result, _| match result {
                DCacheRead::Ok(value) => observed.push(value),
                other => panic!("expected payload, got {other:?}"),
            },
        )
        .unwrap();
    let expected: Vec<_> = payloads
        .iter()
        .enumerate()
        .map(|(index, bytes)| (Frame(index as u64), (index as u64, bytes.to_vec())))
        .collect();
    assert_eq!(observed, expected, "failed writes cannot corrupt unread regions");

    drop(sender);
    drop(receiver);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn dcache_read_holds_slot_and_handler_runs_after_release() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    spine.frames = SpineSpscDCacheQueue::new(1, 16);
    let mut sender = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut receiver = SpineAdapter::connect_tile(&Receiver, &mut spine);
    sender
        .try_produce_with_dcache(
            Frame(1),
            Some((3, |bytes: &mut [u8]| {
                bytes.copy_from_slice(b"one");
            })),
        )
        .unwrap();
    let sender = RefCell::new(sender);

    // A payload read borrows its slot; the later handler can publish again.
    assert!(
        receiver
            .try_consume_with_dcache_one(
                |frame: Frame, bytes| {
                    assert_eq!(frame, Frame(1));
                    assert_eq!(bytes, b"one");
                    assert!(matches!(
                        sender
                            .borrow_mut()
                            .try_produce_with_dcache(Frame(2), None::<(usize, fn(&mut [u8]))>),
                        Err(SpscDCacheProduceError::Full)
                    ));
                    bytes.to_vec()
                },
                |result, _| {
                    assert!(matches!(result, DCacheRead::Ok((Frame(1), bytes)) if bytes == b"one"));
                    sender
                        .borrow_mut()
                        .try_produce_with_dcache(
                            Frame(2),
                            Some((3, |bytes: &mut [u8]| {
                                bytes.copy_from_slice(b"two");
                            })),
                        )
                        .expect("handler runs after slot release");
                },
            )
            .unwrap()
    );
    let mut observed = None;
    assert!(
        receiver
            .try_consume_with_dcache_one(
                |_: Frame, bytes| bytes.to_vec(),
                |result, _| observed = Some(result),
            )
            .unwrap()
    );
    assert!(matches!(observed, Some(DCacheRead::Ok((Frame(2), bytes))) if bytes == b"two"));

    drop(sender);
    drop(receiver);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn dcache_panic_does_not_publish_or_advance_and_consumed_slot_is_released() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    spine.frames = SpineSpscDCacheQueue::new(2, 16);
    let mut sender = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut receiver = SpineAdapter::connect_tile(&Receiver, &mut spine);
    sender
        .try_produce_with_dcache(
            Frame(1),
            Some((3, |bytes: &mut [u8]| bytes.copy_from_slice(b"one"))),
        )
        .unwrap();
    sender.begin_loop(IngestionTime::now());

    // Failed factories must not advance into the still-unread first region.
    for _ in 0..5 {
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                sender
                    .try_produce_with_dcache(
                        Frame(99),
                        Some((3, |bytes: &mut [u8]| {
                            bytes.copy_from_slice(b"bad");
                            panic!("intentional writer panic");
                        })),
                    )
                    .unwrap();
            }))
            .is_err()
        );
    }
    assert!(!sender.did_work());
    sender
        .try_produce_with_dcache(
            Frame(2),
            Some((3, |bytes: &mut [u8]| bytes.copy_from_slice(b"two"))),
        )
        .unwrap();

    let mut borrowed = None;
    let result = catch_unwind(AssertUnwindSafe(|| {
        receiver
            .try_consume_with_dcache_one(
                |frame: Frame, bytes| {
                    borrowed = Some((frame, bytes.to_vec()));
                    panic!("intentional reader panic");
                },
                |_, _| panic!("handler must not run after reader panic"),
            )
            .unwrap();
    }));
    assert!(result.is_err());
    assert_eq!(
        borrowed,
        Some((Frame(1), b"one".to_vec())),
        "panicking writers must preserve unread bytes"
    );
    sender
        .try_produce_with_dcache(
            Frame(3),
            Some((5, |bytes: &mut [u8]| {
                bytes.copy_from_slice(b"three");
            })),
        )
        .expect("reader panic releases the consumed slot");
    let mut observed = Vec::new();
    receiver
        .try_consume_with_dcache(
            |_: Frame, bytes| bytes.to_vec(),
            |result, _| match result {
                DCacheRead::Ok(value) => observed.push(value),
                other => panic!("expected payload, got {other:?}"),
            },
        )
        .unwrap();
    assert_eq!(observed, [(Frame(2), b"two".to_vec()), (Frame(3), b"three".to_vec())]);

    drop(sender);
    drop(receiver);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn dcache_handler_panic_does_not_replay_or_hold_capacity() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    spine.frames = SpineSpscDCacheQueue::new(1, 16);
    let mut sender = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut receiver = SpineAdapter::connect_tile(&Receiver, &mut spine);
    sender
        .try_produce_with_dcache(
            Frame(1),
            Some((3, |bytes: &mut [u8]| bytes.copy_from_slice(b"one"))),
        )
        .unwrap();

    let result = catch_unwind(AssertUnwindSafe(|| {
        receiver
            .try_consume_with_dcache_one(
                |_: Frame, bytes| bytes.to_vec(),
                |result, _| {
                    assert!(matches!(result, DCacheRead::Ok((Frame(1), bytes)) if bytes == b"one"));
                    panic!("intentional handler panic");
                },
            )
            .unwrap();
    }));
    assert!(result.is_err());
    sender
        .try_produce_with_dcache(
            Frame(2),
            Some((3, |bytes: &mut [u8]| bytes.copy_from_slice(b"two"))),
        )
        .expect("handler panic cannot retain the consumed slot");
    let mut observed = None;
    assert!(
        receiver
            .try_consume_with_dcache_one(
                |_: Frame, bytes| bytes.to_vec(),
                |result, _| observed = Some(result),
            )
            .unwrap()
    );
    assert!(matches!(observed, Some(DCacheRead::Ok((Frame(2), bytes))) if bytes == b"two"));
    assert!(
        !receiver
            .try_consume_with_dcache_one(
                |_: Frame, _| panic!("replayed reader"),
                |_, _| panic!("replayed handler"),
            )
            .unwrap()
    );

    drop(sender);
    drop(receiver);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn dcache_no_ref_skips_reader_and_selective_tracking_preserves_ingestion() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    let mut sender = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut receiver = SpineAdapter::connect_tile(&Receiver, &mut spine);
    let queues = timing_queues(tmp.path());
    let before = queues.map(|queue| queue.count());
    let ingestion = IngestionTime::now();
    sender
        .producers
        .try_produce_with_dcache_and_ingestion(Frame(10), None::<(usize, fn(&mut [u8]))>, ingestion)
        .unwrap();
    sender
        .producers
        .try_produce_with_dcache_and_ingestion(
            Frame(11),
            Some((4, |bytes: &mut [u8]| bytes.copy_from_slice(b"data"))),
            ingestion,
        )
        .unwrap();

    receiver.begin_loop(IngestionTime::now());
    let mut read_count = 0;
    let mut handled = Vec::new();
    assert!(
        receiver
            .try_consume_with_dcache_maybe_track(
                |frame: Frame, bytes| {
                    read_count += 1;
                    (frame, bytes.to_vec())
                },
                |result, producers| {
                    assert_eq!(producers.timestamp().ingestion_t, ingestion);
                    match result {
                        DCacheRead::NoRef(frame) => {
                            handled.push(frame.0);
                            false
                        }
                        DCacheRead::Ok((frame, (read_frame, bytes))) => {
                            assert_eq!(frame, read_frame);
                            assert_eq!(bytes, b"data");
                            handled.push(frame.0);
                            true
                        }
                        other => panic!("unexpected result {other:?}"),
                    }
                },
            )
            .unwrap()
    );
    assert_eq!(read_count, 1, "NoRef does not invoke the payload reader");
    assert_eq!(handled, [10, 11]);
    assert!(receiver.did_work(), "untracked messages still count as work");
    for (queue, before) in queues.into_iter().zip(before) {
        assert_eq!(queue.count() - before, 1, "only selected messages emit timings");
    }
    sender.try_produce_with_dcache(Frame(12), None::<(usize, fn(&mut [u8]))>).unwrap();
    receiver.begin_loop(IngestionTime::now());
    assert!(
        receiver
            .try_consume_with_dcache_one_maybe_track(
                |_: Frame, _| panic!("NoRef reader"),
                |result, _| {
                    assert!(matches!(result, DCacheRead::NoRef(Frame(12))));
                    false
                },
            )
            .unwrap()
    );
    assert!(receiver.did_work(), "an entirely untracked iteration is work");
    for (queue, before) in queues.into_iter().zip(before) {
        assert_eq!(queue.count() - before, 1);
    }
    sender.try_produce_with_dcache(Frame(13), None::<(usize, fn(&mut [u8]))>).unwrap();
    receiver
        .try_consume_with_dcache_one(
            |_: Frame, _| panic!("NoRef reader"),
            |result, _| assert!(matches!(result, DCacheRead::NoRef(Frame(13)))),
        )
        .unwrap();
    for (queue, before) in queues.into_iter().zip(before) {
        assert_eq!(queue.count() - before, 2, "ordinary consumption measures NoRef too");
    }
    receiver.begin_loop(IngestionTime::now());
    assert!(
        !receiver
            .try_consume_with_dcache_one_maybe_track(
                |_: Frame, _| panic!("empty reader"),
                |_, _| panic!("empty handler"),
            )
            .unwrap()
    );
    assert!(!receiver.did_work());

    drop(sender);
    drop(receiver);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn dcache_producer_handoff_and_shared_reopen_preserve_unread_fifo() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut spine = new_spine(tmp.path());
        let mut first = SpineAdapter::connect_tile(&Sender, &mut spine);
        let mut contender = SpineAdapter::connect_tile(&Sender, &mut spine);
        let idle = spine.attach_producers(&Sender);
        first
            .try_produce_with_dcache(
                Frame(1),
                Some((3, |bytes: &mut [u8]| {
                    bytes.copy_from_slice(b"one");
                })),
            )
            .unwrap();
        first.try_produce_with_dcache(Frame(2), None::<(usize, fn(&mut [u8]))>).unwrap();
        let mut called = false;
        assert!(matches!(
            contender.try_produce_with_dcache(Frame(99), Some((1, |_: &mut [u8]| called = true))),
            Err(SpscDCacheProduceError::Attach(QueueError::ProducerAttached))
        ));
        assert!(!called, "failed role acquisition must skip the payload writer");
        drop(first);
        drop(idle); // An unattached bundle must not interfere with role handoff.
        contender
            .try_produce_with_dcache(
                Frame(3),
                Some((5, |bytes: &mut [u8]| {
                    bytes.copy_from_slice(b"three");
                })),
            )
            .expect("replacement resumes at the metadata queue's next sequence");
        drop(contender);
        drop(spine);
    }

    let mut reopened = new_spine(tmp.path());
    let mut receiver = SpineAdapter::connect_tile(&Receiver, &mut reopened);
    let mut got = Vec::new();
    receiver
        .try_consume_with_dcache(
            |_: Frame, bytes| bytes.to_vec(),
            |result, _| match result {
                DCacheRead::Ok((frame, bytes)) => got.push((frame.0, Some(bytes))),
                DCacheRead::NoRef(frame) => got.push((frame.0, None)),
                other => panic!("unexpected result {other:?}"),
            },
        )
        .unwrap();
    assert_eq!(got, [(1, Some(b"one".to_vec())), (2, None), (3, Some(b"three".to_vec()))]);

    drop(receiver);
    drop(reopened);
    cleanup_shmem(tmp.path());
}

#[test]
fn dcache_child_producer_continues_shared_slot() {
    let Ok(path) = std::env::var("FLUX_TEST_DCACHE_PATH") else { return };
    let mut spine = new_spine(std::path::Path::new(&path));
    let mut sender = SpineAdapter::connect_tile(&Sender, &mut spine);
    sender
        .try_produce_with_dcache(
            Frame(3),
            Some((5, |bytes: &mut [u8]| bytes.copy_from_slice(b"three"))),
        )
        .unwrap();
}

#[test]
fn dcache_new_process_producer_resumes_sequence_with_unread_backlog() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut spine = new_spine(tmp.path());
        let mut sender = SpineAdapter::connect_tile(&Sender, &mut spine);
        sender
            .try_produce_with_dcache(
                Frame(1),
                Some((3, |bytes: &mut [u8]| bytes.copy_from_slice(b"one"))),
            )
            .unwrap();
        sender.try_produce_with_dcache(Frame(2), None::<(usize, fn(&mut [u8]))>).unwrap();
    }

    // An exec'd producer has no process-local slot index from the first owner.
    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("dcache_child_producer_continues_shared_slot")
        .env("FLUX_TEST_DCACHE_PATH", tmp.path())
        .output()
        .unwrap();
    assert!(output.status.success(), "child failed: {}", String::from_utf8_lossy(&output.stderr));

    let mut spine = new_spine(tmp.path());
    let mut receiver = SpineAdapter::connect_tile(&Receiver, &mut spine);
    let mut observed = Vec::new();
    receiver
        .try_consume_with_dcache(
            |_: Frame, bytes| bytes.to_vec(),
            |result, _| match result {
                DCacheRead::Ok((frame, bytes)) => observed.push((frame.0, Some(bytes))),
                DCacheRead::NoRef(frame) => observed.push((frame.0, None)),
                other => panic!("unexpected result {other:?}"),
            },
        )
        .unwrap();
    assert_eq!(observed, [(1, Some(b"one".to_vec())), (2, None), (3, Some(b"three".to_vec()))]);
    drop(receiver);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn dcache_consumer_attachment_error_skips_callbacks_and_work() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    let mut first = SpineAdapter::connect_tile(&Receiver, &mut spine);
    let mut second = SpineAdapter::connect_tile(&Receiver, &mut spine);
    first.consumers.frames.try_attach().unwrap();
    second.begin_loop(IngestionTime::now());
    assert!(matches!(
        second.try_consume_with_dcache_one(
            |_: Frame, _| panic!("reader must not run"),
            |_, _| panic!("handler must not run"),
        ),
        Err(QueueError::ConsumerAttached)
    ));
    assert!(!second.did_work());
    drop(first);
    assert!(
        !second
            .try_consume_with_dcache_one(
                |_: Frame, _| panic!("empty reader"),
                |_, _| panic!("empty handler"),
            )
            .unwrap()
    );

    drop(second);
    drop(spine);
    cleanup_shmem(tmp.path());
}

#[test]
fn dcache_repeated_threaded_wraps_preserve_variable_payloads() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    spine.frames = SpineSpscDCacheQueue::new(4, 1024);
    let mut sender = SpineAdapter::connect_tile(&Sender, &mut spine);
    let mut receiver = SpineAdapter::connect_tile(&Receiver, &mut spine);
    drop(spine); // Endpoints retain both allocations.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let messages = 2000;
    let producer = std::thread::spawn(move || {
        for sequence in 0..messages {
            loop {
                match sender.try_produce_with_dcache(
                    Frame(sequence),
                    Some((sequence as usize % 1024 + 1, |bytes: &mut [u8]| {
                        bytes.fill(sequence as u8);
                    })),
                ) {
                    Ok(()) => break,
                    Err(SpscDCacheProduceError::Full) => {
                        assert!(std::time::Instant::now() < deadline, "consumer stalled");
                        std::hint::spin_loop();
                    }
                    error => panic!("unexpected publication result {error:?}"),
                }
            }
        }
    });
    for expected in 0..messages {
        loop {
            if receiver
                .try_consume_with_dcache_one_maybe_track(
                    |frame: Frame, bytes| {
                        assert_eq!(frame, Frame(expected));
                        assert_eq!(bytes.len(), expected as usize % 1024 + 1);
                        assert!(bytes.iter().all(|byte| *byte == expected as u8));
                    },
                    |result, _| {
                        assert!(matches!(result, DCacheRead::Ok(_)));
                        false
                    },
                )
                .unwrap()
            {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "producer stalled");
            std::hint::spin_loop();
        }
    }
    producer.join().unwrap();
    drop(receiver);
    cleanup_shmem(tmp.path());
}

#[test]
fn dcache_configuration_mismatch_and_missing_arena_are_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let mut spine = new_spine(tmp.path());
    let mut sender = SpineAdapter::connect_tile(&Sender, &mut spine);
    sender
        .try_produce_with_dcache(
            Frame(1),
            Some((3, |bytes: &mut [u8]| {
                bytes.copy_from_slice(b"one");
            })),
        )
        .unwrap();
    drop(sender);
    drop(spine);

    assert!(
        catch_unwind(|| {
            let mut config = DCacheSpineConfig::default();
            config.frames.mtu += 1;
            // SAFETY: matching schema/access rules. An MTU mismatch must be
            // rejected before any payload access or modification.
            unsafe { DCacheSpine::new_with_base_dir_and_config(tmp.path(), None, config) }
        })
        .is_err()
    );

    // A rejected opener leaves the original queue and its payload intact.
    spine = new_spine(tmp.path());
    let mut receiver = SpineAdapter::connect_tile(&Receiver, &mut spine);
    assert!(
        receiver
            .try_consume_with_dcache_one(
                |frame: Frame, bytes| {
                    assert_eq!(frame, Frame(1));
                    assert_eq!(bytes, b"one");
                },
                |_, _| {},
            )
            .unwrap()
    );
    drop(receiver);
    drop(spine);
    let arena = flux_utils::directories::shmem_dir_with_base(tmp.path(), DCacheSpine::app_name())
        .join("spsc/frames.dcache");
    flux::communication::cleanup_flink(&arena).unwrap();
    assert!(catch_unwind(|| new_spine(tmp.path())).is_err());
    assert!(!arena.exists(), "must not create replacement bytes for existing metadata");
    cleanup_shmem(tmp.path());
}
