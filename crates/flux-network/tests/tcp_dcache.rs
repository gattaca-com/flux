use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use flux::{
    communication::{ShmemData, cleanup_shmem},
    spine::{DCacheRead, ScopedSpine, SpineAdapter, SpineProducerWithDCache},
    tile::{Tile, TileConfig, TileInfo, attach_tile},
};
use flux_network::tcp::{PollEvent, SendBehavior, TcpConnector};
use spine_derive::from_spine;

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct Payload([u8; 8]);

#[from_spine("tcp-dcache-test")]
#[derive(Debug)]
struct TcpDcacheSpine {
    pub tile_info: ShmemData<TileInfo>,
    #[queue(mtu(64))]
    pub msg: SpineQueue<Payload>,
}

const BIND_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 24713);

struct NetworkTile {
    conn: Option<TcpConnector>,
    ready: Arc<AtomicBool>,
}

impl Tile<TcpDcacheSpine> for NetworkTile {
    fn try_init(&mut self, adapter: &mut SpineAdapter<TcpDcacheSpine>) -> bool {
        let sp: &SpineProducerWithDCache<Payload> = adapter.producers.as_ref();
        let mut conn = TcpConnector::default().with_dcache(sp.dcache_ptr());
        conn.listen_at(BIND_ADDR).unwrap();
        self.conn = Some(conn);
        self.ready.store(true, Ordering::Release);
        true
    }

    fn loop_body(&mut self, adapter: &mut SpineAdapter<TcpDcacheSpine>) {
        let Some(conn) = &mut self.conn else { return };
        conn.poll_with_produce(&mut adapter.producers, |ev| {
            let PollEvent::Message { payload: bytes, .. } = ev else { return None };
            bytes.try_into().ok().map(Payload)
        });
    }
}

struct ReaderTile {
    received: Arc<Mutex<Vec<Payload>>>,
    count: usize,
    expected: usize,
}

impl Tile<TcpDcacheSpine> for ReaderTile {
    fn loop_body(&mut self, adapter: &mut SpineAdapter<TcpDcacheSpine>) {
        let received = &self.received;
        let count = &mut self.count;
        adapter.consume_with_dcache::<Payload, (), _, _>(
            |msg, bytes| assert_eq!(&msg.0[..], bytes),
            |result, _| {
                if let DCacheRead::Ok((msg, ())) = result {
                    received.lock().unwrap().push(msg);
                    *count += 1;
                }
            },
        );
        if self.count >= self.expected {
            adapter.request_stop_scope();
        }
    }
}

/// Two TCP streams into the same dcache-backed spine queue.
/// Verifies dcache bytes match the queue message (same shmem region).
#[test]
fn dcache_multi_stream() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    let mut spine = TcpDcacheSpine::new_with_base_dir(base, None);

    const MSG_A: &[u8; 8] = b"stream-a";
    const MSG_B: &[u8; 8] = b"stream-b";

    let ready = Arc::new(AtomicBool::new(false));
    let received: Arc<Mutex<Vec<Payload>>> = Arc::new(Mutex::new(Vec::new()));

    let ready_c = ready.clone();
    let client = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !ready_c.load(Ordering::Acquire) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        for msg in [MSG_A, MSG_B] {
            let mut conn = TcpConnector::default();
            let tok = conn.connect(BIND_ADDR).unwrap();
            conn.write_or_enqueue_with(SendBehavior::Single(tok), |buf| {
                buf.extend_from_slice(msg);
            });
            let flush = Instant::now() + Duration::from_millis(100);
            while Instant::now() < flush {
                conn.poll_with(|_| {});
                thread::sleep(Duration::from_micros(50));
            }
        }
    });

    thread::scope(|scope| {
        let mut scoped = ScopedSpine::new(&mut spine, scope, None, None);
        attach_tile(
            NetworkTile { conn: None, ready: ready.clone() },
            &mut scoped,
            TileConfig::background(None, Some(flux::timing::Duration::from_millis(1))),
        );
        attach_tile(
            ReaderTile { received: received.clone(), count: 0, expected: 2 },
            &mut scoped,
            TileConfig::background(None, None),
        );
    });

    client.join().unwrap();

    {
        let guard = received.lock().unwrap();
        assert_eq!(guard.len(), 2);
        assert!(guard.iter().any(|p| p.0 == *MSG_A));
        assert!(guard.iter().any(|p| p.0 == *MSG_B));
    }
    cleanup_shmem(base);
}
