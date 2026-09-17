//! End-to-end gather path: a sender spine with `#[queue(gather)]` fields
//! drains through `GatherTile` (disk + TCP) into `BlobReceiver` +
//! `BlobRouter` (disk + hook), and `BlobReader` reads both disks back.

use std::{
    fs,
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

use flux::{
    communication::{ShmemData, cleanup_shmem},
    spine::{ScopedSpine, SpineAdapter, SpineQueue},
    tile::{Tile, TileConfig, TileInfo, attach_tile},
};
use flux_gather::{
    Blob, BlobReader, BlobReceiver, BlobRouter, Boundary, GatherConfig, GatherMeta,
    GatherReceiverSpine, GatherTile, ReadError,
};
use flux_timing::Duration;
use flux_versioned_types::{Versioned, VersionedLeaves, versioned_struct};
use mio::Token;
use spine_derive::from_spine;
use type_hash_derive::type_hash_lock;

versioned_struct!(Price =>
    #[type_hash_lock(hash = 5743759228102669910)]
    PriceV1 { pub id: u64, pub value: u64 }
);

versioned_struct!(Fill =>
    #[type_hash_lock(hash = 12390809355146961144)]
    FillV1 { pub id: u64, pub qty: u64 }
);

versioned_struct!(Ignored =>
    #[type_hash_lock(hash = 3345810391355477536)]
    IgnoredV1 { pub x: u64 }
);

versioned_struct!(SlotEnd =>
    #[type_hash_lock(hash = 220327059579474723)]
    SlotEndV1 { pub slot: u64 }
);

#[derive(Clone, Copy, Debug, VersionedLeaves)]
enum Telemetry {
    Price(Price),
    Fill(Fill),
    SlotEnd(SlotEnd),
}

impl Boundary for SlotEnd {
    fn gather_boundary(&self) -> Option<u64> {
        Some(self.slot)
    }
}

#[from_spine("gather-test")]
#[derive(Debug)]
struct GatherTestSpine {
    pub tile_info: ShmemData<TileInfo>,
    #[queue(size(2usize.pow(10)), gather)]
    pub prices: SpineQueue<Price>,
    #[queue(size(2usize.pow(10)), mtu(1024), gather)]
    pub fills: SpineQueue<Fill>,
    #[queue(size(2usize.pow(10)))]
    pub ignored: SpineQueue<Ignored>,
    #[queue(size(64), gather(boundary))]
    pub slot_end: SpineQueue<SlotEnd>,
}

#[derive(Clone, Debug)]
struct BlobRecord {
    meta: GatherMeta,
    type_name: String,
    n_messages: u32,
}

const N_SLOTS: u64 = 3;
const PRICES_PER_SLOT: u64 = 5;
const FILLS_PER_SLOT: u64 = 3;
const IGNORED_PER_SLOT: u64 = 2;
const PARTIAL_PRICES: u64 = 2;
/// Slots 1..=3 ship Price/Fill/SlotEnd each; the teardown flush ships one
/// Price.
const EXPECTED_BLOBS: usize = 10;
const DEADLINE: StdDuration = StdDuration::from_secs(20);
/// Longer than the sender deadline, so a sender-deadline burst still lands on
/// a live receiver instead of a closed port.
const RECEIVER_DEADLINE: StdDuration = StdDuration::from_secs(30);

fn free_loopback() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind free port");
    let addr = listener.local_addr().expect("listener addr");
    drop(listener);
    addr
}

fn background() -> TileConfig {
    TileConfig::background(None, Some(Duration::from_millis(1)))
}

struct StopTile {
    stop: Arc<AtomicBool>,
    seen: Arc<Mutex<Vec<BlobRecord>>>,
    want_blobs: usize,
    receiver_done: Arc<AtomicBool>,
    deadline: Instant,
    last_beat: Option<Instant>,
}

impl Tile<GatherReceiverSpine> for StopTile {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<GatherReceiverSpine>) {
        // Time-driven tile: never park, so the stop checks below run even
        // when no other tile signals.
        adapter.mark_work();
        // Periodic no-op produce keeps parked IO tiles polling under the park
        // feature: each produce wakes every parked tile process-wide, and the
        // receiver consumes the heartbeat as work before polling the socket.
        // disconnect() is a no-op for unknown tokens.
        if self.last_beat.is_none_or(|beat| beat.elapsed() >= StdDuration::from_millis(10)) {
            adapter.produce(Token(usize::MAX));
            self.last_beat = Some(Instant::now());
        }
        if self.seen.lock().unwrap().len() >= self.want_blobs ||
            self.stop.load(Ordering::Relaxed) ||
            Instant::now() > self.deadline
        {
            self.receiver_done.store(true, Ordering::Relaxed);
            adapter.request_stop_scope();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_receiver(
    base: PathBuf,
    disk: PathBuf,
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<BlobRecord>>>,
    stop: Arc<AtomicBool>,
    receiver_done: Arc<AtomicBool>,
    want_blobs: usize,
    deadline: Instant,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let spine = GatherReceiverSpine::new_with_base_dir(&base, None);
        spine.start(None, None, |scoped| {
            attach_tile(BlobReceiver::new(addr), scoped, background());
            let hook_seen = seen.clone();
            attach_tile(
                BlobRouter::new(Some(disk), move |meta: &GatherMeta, blob: &Blob| {
                    hook_seen.lock().unwrap().push(BlobRecord {
                        meta: *meta,
                        type_name: blob.type_name().to_owned(),
                        n_messages: blob.header.n_messages,
                    });
                }),
                scoped,
                background(),
            );
            attach_tile(
                StopTile { stop, seen, want_blobs, receiver_done, deadline, last_beat: None },
                scoped,
                background(),
            );
        });
        cleanup_shmem(&base);
    })
}

struct ProducerTile {
    next_slot: u64,
    grace_until: Option<Instant>,
    partial_at: Option<Instant>,
    seen: Arc<Mutex<Vec<BlobRecord>>>,
    receiver_done: Arc<AtomicBool>,
    deadline: Instant,
}

impl ProducerTile {
    fn slot_received(&self, slot: u64) -> bool {
        self.seen.lock().unwrap().iter().filter(|r| r.meta.slot == slot).count() >= 3
    }

    fn expired(&self) -> bool {
        self.receiver_done.load(Ordering::Relaxed) || Instant::now() > self.deadline
    }
}

impl Tile<GatherTestSpine> for ProducerTile {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<GatherTestSpine>) {
        // Time-driven tile: never park, so pacing/deadline checks below run
        // even when no other tile signals.
        adapter.mark_work();
        if let Some(until) = self.grace_until {
            if Instant::now() < until {
                return;
            }
        } else {
            // Broadcast consumers start at the head on first read, so the gather
            // tile must run its first (empty) pass before anything is produced,
            // else the first batch is silently skipped.
            self.grace_until = Some(Instant::now() + StdDuration::from_millis(500));
            return;
        }
        if self.partial_at.is_none() {
            if self.next_slot <= N_SLOTS {
                if self.next_slot > 1 && !self.slot_received(self.next_slot - 1) && !self.expired()
                {
                    return;
                }
                let slot = self.next_slot;
                for i in 0..PRICES_PER_SLOT {
                    adapter.produce(Price { id: slot * 100 + i, value: i });
                }
                for i in 0..FILLS_PER_SLOT {
                    let id = slot * 1000 + i;
                    let payload = id.to_le_bytes();
                    adapter
                        .produce_with_dcache(
                            Fill { id, qty: i },
                            Some((payload.len(), |buf: &mut [u8]| {
                                buf.copy_from_slice(&payload);
                            })),
                        )
                        .expect("fill dcache produce");
                }
                for i in 0..IGNORED_PER_SLOT {
                    adapter.produce(Ignored { x: slot * 10 + i });
                }
                adapter.produce(SlotEnd { slot });
                self.next_slot += 1;
            } else {
                if !self.slot_received(N_SLOTS) && !self.expired() {
                    return;
                }
                for i in 0..PARTIAL_PRICES {
                    adapter.produce(Price { id: 400 + i, value: i });
                }
                self.partial_at = Some(Instant::now());
            }
        } else if self.expired() ||
            Instant::now() > self.partial_at.unwrap() + StdDuration::from_millis(500)
        {
            // The teardown flush ships the partial batch as slot 4; the receiver
            // cannot acknowledge it before this scope stops, so stop on a grace
            // period rather than waiting for it.
            adapter.request_stop_scope();
        }
    }
}

#[test]
fn gather_end_to_end_sender_to_receiver() {
    let send_base = tempfile::tempdir().expect("send base");
    let recv_base = tempfile::tempdir().expect("recv base");
    // Disk roots live outside the shmem bases: cleanup_shmem removes the whole
    // base tree, so a disk dir nested under it would vanish with the queues.
    let send_disk_tmp = tempfile::tempdir().expect("send disk");
    let recv_disk_tmp = tempfile::tempdir().expect("recv disk");
    let send_disk = send_disk_tmp.path().to_path_buf();
    let recv_disk = recv_disk_tmp.path().to_path_buf();
    let addr = free_loopback();

    let seen: Arc<Mutex<Vec<BlobRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let receiver_done = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + DEADLINE;

    let receiver = spawn_receiver(
        recv_base.path().to_path_buf(),
        recv_disk.clone(),
        addr,
        seen.clone(),
        stop,
        receiver_done.clone(),
        EXPECTED_BLOBS,
        Instant::now() + RECEIVER_DEADLINE,
    );
    std::thread::sleep(StdDuration::from_millis(500));

    let mut spine = GatherTestSpine::new_with_base_dir(send_base.path(), None);
    std::thread::scope(|scope| {
        let mut scoped = ScopedSpine::new(&mut spine, scope, None, None);
        attach_tile(
            ProducerTile {
                next_slot: 1,
                grace_until: None,
                partial_at: None,
                seen: seen.clone(),
                receiver_done,
                deadline,
            },
            &mut scoped,
            background(),
        );
        attach_tile(
            GatherTile::new(GatherConfig {
                instance_id: "inst".into(),
                addrs: vec![addr],
                disk_dir: Some(send_disk.clone()),
                zstd_level: 1,
                disk_skip: vec![Fill::NAME.to_string()],
                ..Default::default()
            }),
            &mut scoped,
            background(),
        );
    });
    receiver.join().expect("receiver thread");

    let seen: Vec<BlobRecord> = seen.lock().unwrap().clone();
    assert_hook_records(&seen);
    assert_disk_identity(&send_disk, &recv_disk);
    assert_decoded_content(&send_disk, &recv_disk);

    cleanup_shmem(send_base.path());
}

fn assert_hook_records(seen: &[BlobRecord]) {
    assert_eq!(seen.len(), EXPECTED_BLOBS, "hook records: {seen:?}");
    for record in seen {
        assert_eq!(record.meta.instance_id.as_str(), "inst", "{record:?}");
        assert_eq!(record.meta.app.as_str(), "gather-test", "{record:?}");
    }
    for slot in 1..=N_SLOTS {
        let batch: Vec<&BlobRecord> = seen.iter().filter(|r| r.meta.slot == slot).collect();
        assert_eq!(batch.len(), 3, "slot {slot}: {batch:?}");
        let mut names: Vec<&str> = batch.iter().map(|r| r.type_name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, [Fill::NAME, Price::NAME, SlotEnd::NAME], "slot {slot}");
        let flush_t = batch[0].meta.flush_t;
        assert!(batch.iter().all(|r| r.meta.flush_t == flush_t), "slot {slot}");
        assert!(batch.iter().all(|r| r.meta.n_blobs == 3), "slot {slot}: {batch:?}");
        assert!(!batch.iter().any(|r| r.type_name == Ignored::NAME), "slot {slot}");
    }
    let tail: Vec<&BlobRecord> = seen.iter().filter(|r| r.meta.slot == N_SLOTS + 1).collect();
    assert_eq!(tail.len(), 1, "slot 4: {tail:?}");
    assert_eq!(tail[0].type_name, Price::NAME);
    assert_eq!(tail[0].meta.n_blobs, 1);
    assert_eq!(tail[0].n_messages, 2);
}

fn assert_disk_identity(send_disk: &Path, recv_disk: &Path) {
    for slot in 1..=N_SLOTS {
        for name in [Price::NAME, SlotEnd::NAME] {
            let send_files = BlobReader::slot_files(send_disk, "inst", "gather-test", name, slot)
                .expect("send slot files");
            let recv_files = BlobReader::slot_files(recv_disk, "inst", "gather-test", name, slot)
                .expect("recv slot files");
            assert_eq!(send_files.len(), 1, "{name} slot {slot}");
            assert_eq!(recv_files.len(), 1, "{name} slot {slot}");
            assert_eq!(
                fs::read(&send_files[0]).expect("read send file"),
                fs::read(&recv_files[0]).expect("read recv file"),
                "{name} slot {slot} byte identity"
            );
        }
        assert!(
            BlobReader::slot_files(send_disk, "inst", "gather-test", Fill::NAME, slot)
                .expect("send fill files")
                .is_empty(),
            "Fill is disk-skipped on the sender"
        );
        let recv_fills = BlobReader::slot_files(recv_disk, "inst", "gather-test", Fill::NAME, slot)
            .expect("recv fill files");
        assert_eq!(recv_fills.len(), 1, "Fill slot {slot}");
    }
    let send_tail =
        BlobReader::slot_files(send_disk, "inst", "gather-test", Price::NAME, N_SLOTS + 1)
            .expect("send slot-4 files");
    let recv_tail =
        BlobReader::slot_files(recv_disk, "inst", "gather-test", Price::NAME, N_SLOTS + 1)
            .expect("recv slot-4 files");
    assert_eq!(send_tail.len(), 1);
    assert_eq!(recv_tail.len(), 1);
    assert_eq!(
        fs::read(&send_tail[0]).expect("read send slot 4"),
        fs::read(&recv_tail[0]).expect("read recv slot 4")
    );
}

fn assert_decoded_content(send_disk: &Path, recv_disk: &Path) {
    let mut reader = BlobReader::new();
    for slot in 1..=N_SLOTS {
        let files = BlobReader::slot_files(recv_disk, "inst", "gather-test", Price::NAME, slot)
            .expect("recv price files");
        let decoded = reader.read::<Telemetry>(&files[0]).expect("decode price file");
        assert_eq!(decoded.len(), 1);
        let (meta, msgs) = &decoded[0];
        assert_eq!(meta.slot, slot);
        assert_eq!(msgs.len(), PRICES_PER_SLOT as usize);
        for (i, msg) in msgs.iter().enumerate() {
            let i = i as u64;
            match msg.data() {
                Telemetry::Price(price) => {
                    assert_eq!(price.id, slot * 100 + i);
                    assert_eq!(price.value, i);
                }
                other => panic!("slot {slot} price file held {other:?}"),
            }
        }
        let leaves = reader.read::<Price>(&files[0]).expect("decode price leaves");
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].1.len(), PRICES_PER_SLOT as usize);
        for (i, msg) in leaves[0].1.iter().enumerate() {
            let i = i as u64;
            assert_eq!(msg.data().id, slot * 100 + i);
            assert_eq!(msg.data().value, i);
        }
        assert!(
            matches!(reader.read::<Ignored>(&files[0]), Err(ReadError::ForeignType { .. })),
            "price file is foreign to Ignored"
        );

        let fill_files = BlobReader::slot_files(recv_disk, "inst", "gather-test", Fill::NAME, slot)
            .expect("recv fill files");
        let decoded = reader.read::<Fill>(&fill_files[0]).expect("decode fill file");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1.len(), FILLS_PER_SLOT as usize);
        for (i, msg) in decoded[0].1.iter().enumerate() {
            let i = i as u64;
            assert_eq!(msg.data().id, slot * 1000 + i);
            assert_eq!(msg.data().qty, i);
        }
    }
    let tail = BlobReader::slot_files(recv_disk, "inst", "gather-test", Price::NAME, N_SLOTS + 1)
        .expect("recv slot-4 files");
    assert_eq!(tail.len(), 1);
    let decoded = reader.read::<Telemetry>(&tail[0]).expect("decode slot-4 file");
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].0.slot, N_SLOTS + 1);
    assert_eq!(decoded[0].1.len(), PARTIAL_PRICES as usize);

    for disk in [send_disk, recv_disk] {
        assert!(
            !disk.join("inst").join("gather-test").join(Ignored::NAME).exists(),
            "no ignored dir under {}",
            disk.display()
        );
    }
}

#[test]
fn non_blob_peer_is_disconnected() {
    let recv_base = tempfile::tempdir().expect("recv base");
    let recv_disk_tmp = tempfile::tempdir().expect("recv disk");
    let recv_disk = recv_disk_tmp.path().to_path_buf();
    let addr = free_loopback();

    let seen: Arc<Mutex<Vec<BlobRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let receiver_done = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + DEADLINE;

    let receiver = spawn_receiver(
        recv_base.path().to_path_buf(),
        recv_disk,
        addr,
        seen.clone(),
        stop.clone(),
        receiver_done,
        usize::MAX,
        deadline,
    );
    std::thread::sleep(StdDuration::from_millis(500));

    let mut stream = TcpStream::connect(addr).expect("connect receiver");
    let payload = [0xABu8; 200];
    let mut frame = Vec::with_capacity(12 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&12345u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    stream.write_all(&frame).expect("write non-blob frame");

    stream.set_read_timeout(Some(StdDuration::from_secs(5))).expect("read timeout");
    let mut byte = [0u8; 1];
    let dropped = loop {
        match stream.read(&mut byte) {
            Ok(0) => break true,
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => {
                break matches!(
                    error.kind(),
                    ErrorKind::ConnectionReset |
                        ErrorKind::ConnectionAborted |
                        ErrorKind::BrokenPipe |
                        ErrorKind::UnexpectedEof
                );
            }
        }
    };
    assert!(dropped, "receiver kept the non-blob peer connected");
    assert!(seen.lock().unwrap().is_empty(), "hook saw blobs from a non-blob peer");

    stop.store(true, Ordering::Relaxed);
    receiver.join().expect("receiver thread");
    cleanup_shmem(recv_base.path());
}
