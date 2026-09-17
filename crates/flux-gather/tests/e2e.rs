// `#[from_spine]` emits `extern "C"` FFI checks for queue message types.
#![allow(improper_ctypes)]

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
    Blob, BlobCache, BlobConsumer, BlobHandler, BlobReader, BlobReceiver, BlobShipper, BlobWriter,
    GatherQueues, IncomingBlob, ReadError, Token,
};
use flux_timing::{Duration, InternalMessage};
use flux_utils::ArrayStr;
use flux_versioned_types::{Versioned, VersionedLeaves, versioned_struct};
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

versioned_struct!(TestMeta =>
    #[type_hash_lock(hash = 2679133347977192113)]
    TestMetaV1 { pub slot: u64, pub n_blobs: u64, pub instance: ArrayStr<16> }
);

const _: () = assert!(size_of::<TestMeta>() == 40);

impl TestMeta {
    fn path(&self, base: &Path, type_name: &str) -> PathBuf {
        base.join(self.instance.as_str()).join(type_name).join(format!("{}.bin", self.slot))
    }
}

fn test_meta(slot: u64, n_blobs: u64) -> TestMeta {
    TestMeta { slot, n_blobs, instance: ArrayStr::from_str_truncate("inst") }
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
    #[queue(size(64))]
    pub slot_end: SpineQueue<SlotEnd>,
}

struct Gatherer {
    ready: Arc<AtomicBool>,
    cache: BlobCache,
    shipper: BlobShipper,
    writer: BlobWriter,
    base: PathBuf,
    last_slot: u64,
}

impl Gatherer {
    fn flush(&mut self, slot: u64) {
        let meta = test_meta(slot, self.cache.n_blobs() as u64);
        self.cache.flush(&meta, 1, |blob| {
            self.shipper.ship(blob);
            if blob.type_name() != Fill::NAME {
                self.writer.write(blob, &meta.path(&self.base, blob.type_name()));
            }
        });
        self.last_slot = slot;
    }
}

impl Tile<GatherTestSpine> for Gatherer {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<GatherTestSpine>) {
        GatherTestSpine::gather_into(adapter, &mut self.cache);
        let mut boundary = None;
        if adapter.consume_internal_message_one(|m: &mut InternalMessage<SlotEnd>, _| {
            boundary = Some(*m);
        }) {
            // rings are independent; drain again so the closing slot is complete
            GatherTestSpine::gather_into(adapter, &mut self.cache);
            let m = boundary.unwrap();
            self.cache.push(&m);
            self.flush(m.slot);
        }
        if self.shipper.drive() | self.writer.poll() {
            adapter.mark_work();
        }
        self.ready.store(true, Ordering::Relaxed);
    }

    fn teardown(mut self, adapter: &mut SpineAdapter<GatherTestSpine>) {
        GatherTestSpine::gather_into(adapter, &mut self.cache);
        self.flush(self.last_slot + 1);
        self.writer.drain();
        let deadline = Instant::now() + StdDuration::from_millis(200);
        while Instant::now() < deadline {
            self.shipper.drive();
            std::thread::sleep(StdDuration::from_millis(1));
        }
    }
}

#[from_spine("gather-test-recv")]
#[derive(Debug)]
struct RecvSpine {
    pub tile_info: ShmemData<TileInfo>,
    #[queue(size(64), mtu(1 << 20))]
    pub blobs: SpineQueue<IncomingBlob>,
    #[queue(size(64))]
    pub disconnect: SpineQueue<Token>,
}

#[derive(Clone, Debug)]
struct Seen {
    meta: TestMeta,
    type_name: String,
    n_messages: u32,
}

struct RecordingHandler {
    writer: BlobWriter,
    base: PathBuf,
    seen: Arc<Mutex<Vec<Seen>>>,
    done: Arc<AtomicBool>,
}

impl Tile<RecvSpine> for RecordingHandler {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<RecvSpine>) {
        if self.writer.poll() {
            adapter.mark_work();
        }
    }

    fn teardown(mut self, _adapter: &mut SpineAdapter<RecvSpine>) {
        self.writer.drain();
    }
}

impl BlobHandler<RecvSpine, TestMeta> for RecordingHandler {
    fn on_blob(&mut self, meta: &TestMeta, blob: &Blob, _adapter: &mut SpineAdapter<RecvSpine>) {
        self.writer.write(blob, &meta.path(&self.base, blob.type_name()));
        let mut seen = self.seen.lock().unwrap();
        seen.push(Seen {
            meta: *meta,
            type_name: blob.type_name().to_owned(),
            n_messages: blob.header.n_messages,
        });
        if seen.len() >= EXPECTED_BLOBS {
            self.done.store(true, Ordering::Relaxed);
        }
    }
}

const N_SLOTS: u64 = 3;
const PRICES_PER_SLOT: u64 = 5;
const FILLS_PER_SLOT: u64 = 3;
const IGNORED_PER_SLOT: u64 = 2;
const PARTIAL_PRICES: u64 = 2;
const EXPECTED_BLOBS: usize = 10;
const DEADLINE: StdDuration = StdDuration::from_secs(20);
const RECEIVER_DEADLINE: StdDuration = StdDuration::from_secs(30);
static PORT_LOCK: Mutex<()> = Mutex::new(());

struct ShmemGuard(PathBuf);
impl Drop for ShmemGuard {
    fn drop(&mut self) {
        cleanup_shmem(&self.0);
    }
}

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
    seen: Arc<Mutex<Vec<Seen>>>,
    want_blobs: usize,
    receiver_done: Arc<AtomicBool>,
    deadline: Instant,
}

impl Tile<RecvSpine> for StopTile {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<RecvSpine>) {
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
    seen: Arc<Mutex<Vec<Seen>>>,
    stop: Arc<AtomicBool>,
    receiver_done: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    want_blobs: usize,
    deadline: Instant,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let spine = RecvSpine::new_with_base_dir(&base, None);
        spine.start(None, None, |scoped| {
            attach_tile(BlobReceiver::new(addr), scoped, background());
            attach_tile(
                BlobConsumer::new(RecordingHandler {
                    writer: BlobWriter::new(),
                    base: disk,
                    seen: seen.clone(),
                    done,
                }),
                scoped,
                background(),
            );
            attach_tile(
                StopTile { stop, seen, want_blobs, receiver_done, deadline },
                scoped,
                background(),
            );
        });
        cleanup_shmem(&base);
    })
}

struct ProducerTile {
    next_slot: u64,
    ready: Arc<AtomicBool>,
    waited_since: Instant,
    partial_at: Option<Instant>,
    seen: Arc<Mutex<Vec<Seen>>>,
    done: Arc<AtomicBool>,
    receiver_done: Arc<AtomicBool>,
    deadline: Instant,
}

impl ProducerTile {
    fn slot_received(&self, slot: u64) -> bool {
        self.seen.lock().unwrap().iter().filter(|r| r.meta.slot == slot).count() >= 3
    }

    fn expired(&self) -> bool {
        self.done.load(Ordering::Relaxed) ||
            self.receiver_done.load(Ordering::Relaxed) ||
            Instant::now() > self.deadline
    }
}

impl Tile<GatherTestSpine> for ProducerTile {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<GatherTestSpine>) {
        if !self.ready.load(Ordering::Relaxed) {
            // consumers subscribe at their first read
            if self.waited_since.elapsed() < StdDuration::from_secs(5) {
                return;
            }
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
            adapter.request_stop_scope();
        }
    }
}

#[test]
fn gather_end_to_end_sender_to_receiver() {
    let _port = PORT_LOCK.lock().unwrap();
    let send_base = tempfile::tempdir().expect("send base");
    let recv_base = tempfile::tempdir().expect("recv base");
    let send_disk_tmp = tempfile::tempdir().expect("send disk");
    let recv_disk_tmp = tempfile::tempdir().expect("recv disk");
    let send_disk = send_disk_tmp.path().to_path_buf();
    let recv_disk = recv_disk_tmp.path().to_path_buf();
    let _send_shmem = ShmemGuard(send_base.path().to_path_buf());
    let _recv_shmem = ShmemGuard(recv_base.path().to_path_buf());
    let addr = free_loopback();

    let seen: Arc<Mutex<Vec<Seen>>> = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let receiver_done = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + DEADLINE;
    let ready = Arc::new(AtomicBool::new(false));

    let receiver = spawn_receiver(
        recv_base.path().to_path_buf(),
        recv_disk.clone(),
        addr,
        seen.clone(),
        stop,
        receiver_done.clone(),
        done.clone(),
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
                ready: ready.clone(),
                waited_since: Instant::now(),
                partial_at: None,
                seen: seen.clone(),
                done,
                receiver_done,
                deadline,
            },
            &mut scoped,
            background(),
        );
        attach_tile(
            Gatherer {
                ready: ready.clone(),
                cache: BlobCache::new(),
                shipper: BlobShipper::new(vec![addr]),
                writer: BlobWriter::new(),
                base: send_disk.clone(),
                last_slot: 0,
            },
            &mut scoped,
            background(),
        );
    });
    receiver.join().expect("receiver thread");

    let seen: Vec<Seen> = seen.lock().unwrap().clone();
    assert_hook_records(&seen);
    assert_disk_identity(&send_disk, &recv_disk);
    assert_decoded_content(&send_disk, &recv_disk);
    assert_single_blob_replay(&recv_disk);

    cleanup_shmem(send_base.path());
}

fn assert_hook_records(seen: &[Seen]) {
    assert_eq!(seen.len(), EXPECTED_BLOBS, "hook records: {seen:?}");
    for record in seen {
        assert_eq!(record.meta.instance.as_str(), "inst", "{record:?}");
    }
    for slot in 1..=N_SLOTS {
        let batch: Vec<&Seen> = seen.iter().filter(|r| r.meta.slot == slot).collect();
        assert_eq!(batch.len(), 3, "slot {slot}: {batch:?}");
        let mut names: Vec<&str> = batch.iter().map(|r| r.type_name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, [Fill::NAME, Price::NAME, SlotEnd::NAME], "slot {slot}");
        assert!(batch.iter().all(|r| r.meta.n_blobs == 3), "slot {slot}: {batch:?}");
        assert!(!batch.iter().any(|r| r.type_name == Ignored::NAME), "slot {slot}");
    }
    let tail: Vec<&Seen> = seen.iter().filter(|r| r.meta.slot == N_SLOTS + 1).collect();
    assert_eq!(tail.len(), 1, "slot 4: {tail:?}");
    assert_eq!(tail[0].type_name, Price::NAME);
    assert_eq!(tail[0].meta.n_blobs, 1);
    assert_eq!(tail[0].n_messages, 2);
}

fn assert_disk_identity(send_disk: &Path, recv_disk: &Path) {
    for slot in 1..=N_SLOTS {
        for name in [Price::NAME, SlotEnd::NAME] {
            let send_file = test_meta(slot, 3).path(send_disk, name);
            let recv_file = test_meta(slot, 3).path(recv_disk, name);
            assert_eq!(
                fs::read(&send_file).expect("read send file"),
                fs::read(&recv_file).expect("read recv file"),
                "{name} slot {slot} byte identity"
            );
        }
        assert!(
            !test_meta(slot, 3).path(send_disk, Fill::NAME).exists(),
            "Fill is disk-skipped on the sender, slot {slot}"
        );
        assert!(
            test_meta(slot, 3).path(recv_disk, Fill::NAME).exists(),
            "Fill slot {slot} on the receiver"
        );
    }
    assert_eq!(
        fs::read(test_meta(N_SLOTS + 1, 1).path(send_disk, Price::NAME)).expect("read send slot 4"),
        fs::read(test_meta(N_SLOTS + 1, 1).path(recv_disk, Price::NAME)).expect("read recv slot 4")
    );
}

fn assert_decoded_content(send_disk: &Path, recv_disk: &Path) {
    let mut reader = BlobReader::new();
    for slot in 1..=N_SLOTS {
        let file = test_meta(slot, 3).path(recv_disk, Price::NAME);
        let decoded = reader.read::<TestMeta, Telemetry>(&file).expect("decode price file");
        assert_eq!(decoded.len(), 1);
        let (meta, msgs) = &decoded[0];
        assert_eq!(meta.slot, slot);
        assert_eq!(meta.n_blobs, 3);
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
        let leaves = reader.read::<TestMeta, Price>(&file).expect("decode price leaves");
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].1.len(), PRICES_PER_SLOT as usize);
        for (i, msg) in leaves[0].1.iter().enumerate() {
            let i = i as u64;
            assert_eq!(msg.data().id, slot * 100 + i);
            assert_eq!(msg.data().value, i);
        }
        assert!(
            matches!(reader.read::<TestMeta, Ignored>(&file), Err(ReadError::ForeignType { .. })),
            "price file is foreign to Ignored"
        );

        let fill_file = test_meta(slot, 3).path(recv_disk, Fill::NAME);
        let decoded = reader.read::<TestMeta, Fill>(&fill_file).expect("decode fill file");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1.len(), FILLS_PER_SLOT as usize);
        for (i, msg) in decoded[0].1.iter().enumerate() {
            let i = i as u64;
            assert_eq!(msg.data().id, slot * 1000 + i);
            assert_eq!(msg.data().qty, i);
        }
    }
    let tail = test_meta(N_SLOTS + 1, 1).path(recv_disk, Price::NAME);
    let decoded = reader.read::<TestMeta, Telemetry>(&tail).expect("decode slot-4 file");
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].0.slot, N_SLOTS + 1);
    assert_eq!(decoded[0].1.len(), PARTIAL_PRICES as usize);

    for disk in [send_disk, recv_disk] {
        assert!(
            !disk.join("inst").join(Ignored::NAME).exists(),
            "no ignored dir under {}",
            disk.display()
        );
    }
}

fn assert_single_blob_replay(recv_disk: &Path) {
    let file = test_meta(1, 3).path(recv_disk, Price::NAME);
    let bytes = fs::read(&file).expect("read price file");
    let mut reader = BlobReader::new();
    let mut count = 0;
    reader
        .for_each_blob(&file, |blob| {
            count += 1;
            assert_eq!(blob.as_bytes(), bytes.as_slice());
            Ok(())
        })
        .expect("walk blobs");
    assert_eq!(count, 1);
}

#[test]
fn non_blob_peer_is_disconnected() {
    let _port = PORT_LOCK.lock().unwrap();
    let recv_base = tempfile::tempdir().expect("recv base");
    let recv_disk_tmp = tempfile::tempdir().expect("recv disk");
    let recv_disk = recv_disk_tmp.path().to_path_buf();
    let _recv_shmem = ShmemGuard(recv_base.path().to_path_buf());
    let addr = free_loopback();

    let seen: Arc<Mutex<Vec<Seen>>> = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let receiver_done = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + DEADLINE;

    let receiver = spawn_receiver(
        recv_base.path().to_path_buf(),
        recv_disk,
        addr,
        seen.clone(),
        stop.clone(),
        receiver_done,
        done,
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
    stop.store(true, Ordering::Relaxed);
    assert!(dropped, "receiver kept the non-blob peer connected");
    assert!(seen.lock().unwrap().is_empty(), "hook saw blobs from a non-blob peer");

    receiver.join().expect("receiver thread");
    cleanup_shmem(recv_base.path());
}
