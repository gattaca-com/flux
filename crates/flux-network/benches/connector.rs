//! TCP vs reliable UDP through `Connector`, on loopback.
//!
//! `rtt`: one message to the server and its echo back, both connectors polled
//! from this thread. `throughput`: a burst of messages one way, timed until
//! the last one lands. `rtt_loss`: UDP through a relay dropping 1 in 100
//! datagrams. TCP has no loss variant: a byte-dropping proxy is not packet
//! loss, and netem needs root.

use std::{
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use flux_network::{Connector, PollEvent, SendBehavior, Transport, UdpConfig};
use mio::Token;

const SIZES: [(&str, usize); 3] = [("2k", 2 * 1024), ("64k", 64 * 1024), ("2m", 2 * 1024 * 1024)];
const BURST: usize = 256;
const LOSS_ONE_IN: usize = 100;

fn free_addr() -> SocketAddr {
    UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap()
}

/// A connected pair. The server echoes everything back on `Single(accepted)`.
struct Pair {
    server: Connector,
    client: Connector,
    accepted: Token,
    client_token: Token,
}

fn udp_config() -> UdpConfig {
    // Loopback holds a 2 MiB burst without dropping when the kernel buffers
    // are raised; the bench sets them on the connector.
    UdpConfig { max_message_size: 4 * 1024 * 1024, ..UdpConfig::lan() }
}

fn connector(transport: Transport) -> Connector {
    Connector::default().with_transport(transport).with_socket_buf_size(16 * 1024 * 1024)
}

fn pair(transport: Transport, listen: SocketAddr, dial: SocketAddr) -> Pair {
    let mut server = connector(transport);
    let mut client = connector(transport);
    server.listen_at(listen).unwrap();
    let client_token = client.connect(dial).unwrap();
    let mut accepted = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted.is_none() || client.currently_disconnected().count() != 0 {
        assert!(Instant::now() < deadline, "handshake");
        server.poll_with(|e| {
            if let PollEvent::Accept { stream, .. } = e {
                accepted = Some(stream);
            }
        });
        client.poll_with(|_| {});
    }
    Pair { server, client, accepted: accepted.unwrap(), client_token }
}

impl Pair {
    /// Sends `msg` and spins until the echo is back.
    fn round_trip(&mut self, msg: &[u8]) {
        let Self { server, client, accepted, client_token } = self;
        client.write_or_enqueue_with(SendBehavior::Single(*client_token), |b| {
            b.extend_from_slice(msg);
        });
        let mut got = false;
        while !got {
            client.poll_with(|_| {});
            server.poll_with(|e| got |= matches!(e, PollEvent::Message { .. }));
        }
        server.write_or_enqueue_with(SendBehavior::Single(*accepted), |b| {
            b.extend_from_slice(msg);
        });
        got = false;
        while !got {
            server.poll_with(|_| {});
            client.poll_with(|e| {
                if let PollEvent::Message { payload, .. } = e {
                    assert_eq!(payload.len(), msg.len());
                    got = true;
                }
            });
        }
    }

    /// Sends `count` copies of `msg` client to server with at most `window`
    /// outstanding, returns when all landed. The bound keeps a large-message
    /// burst inside the UDP send window instead of dropping.
    fn burst(&mut self, msg: &[u8], count: usize, window: usize) {
        let mut sent = 0;
        let mut got = 0;
        while got < count {
            while sent < count && sent - got < window {
                self.client.write_or_enqueue_with(SendBehavior::Single(self.client_token), |b| {
                    b.extend_from_slice(msg);
                });
                sent += 1;
            }
            self.client.poll_with(|_| {});
            self.server.poll_with(|e| {
                if let PollEvent::Message { .. } = e {
                    got += 1;
                }
            });
        }
    }
}

/// Raises the relay's socket buffers so it never drops a burst itself.
fn big_buffers(socket: &UdpSocket) {
    use std::os::fd::AsRawFd;
    let size: libc::c_int = 16 * 1024 * 1024;
    for opt in [libc::SO_RCVBUF, libc::SO_SNDBUF] {
        unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                opt,
                std::ptr::from_ref(&size).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

/// Forwards between one client and the server, dropping 1 in `drop_every`.
struct LossyRelay {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl LossyRelay {
    fn start(server: SocketAddr, drop_every: usize) -> Self {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        big_buffers(&socket);
        socket.set_read_timeout(Some(Duration::from_millis(5))).unwrap();
        let addr = socket.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_c = stop.clone();
        let handle = thread::spawn(move || {
            let mut buf = vec![0u8; 65_536];
            let mut client: Option<SocketAddr> = None;
            let mut count = 0usize;
            while !stop_c.load(Ordering::Relaxed) {
                let Ok((n, from)) = socket.recv_from(&mut buf) else { continue };
                let to = if from == server {
                    let Some(c) = client else { continue };
                    c
                } else {
                    client = Some(from);
                    server
                };
                count += 1;
                if count.is_multiple_of(drop_every) {
                    continue;
                }
                let _ = socket.send_to(&buf[..n], to);
            }
        });
        Self { addr, stop, handle: Some(handle) }
    }
}

impl Drop for LossyRelay {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.take().unwrap().join().unwrap();
    }
}

fn transports() -> [(&'static str, Transport); 2] {
    [("tcp", Transport::default()), ("udp", Transport::Udp(udp_config()))]
}

fn bench_rtt(c: &mut Criterion) {
    let mut group = c.benchmark_group("rtt");
    for (size_name, size) in SIZES {
        let msg = vec![0xA5u8; size];
        group.throughput(Throughput::Bytes(2 * size as u64));
        for (name, transport) in transports() {
            let addr = free_addr();
            let mut pair = pair(transport, addr, addr);
            group.bench_function(BenchmarkId::new(name, size_name), |b| {
                b.iter(|| pair.round_trip(&msg));
            });
        }
    }
    group.finish();
}

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    group.sample_size(20);
    for (size_name, size) in SIZES {
        let msg = vec![0x5Au8; size];
        group.throughput(Throughput::Bytes((BURST * size) as u64));
        for (name, transport) in transports() {
            let addr = free_addr();
            let mut pair = pair(transport, addr, addr);
            // At most 2 MiB in flight: the single-threaded receiver cannot drain
            // while the sender bursts, so stay inside its kernel buffer.
            let window = (2 * 1024 * 1024 / size).clamp(1, BURST);
            group.bench_function(BenchmarkId::new(name, size_name), |b| {
                b.iter(|| pair.burst(&msg, BURST, window));
            });
        }
    }
    group.finish();
}

fn bench_rtt_loss(c: &mut Criterion) {
    let mut group = c.benchmark_group("rtt_loss");
    group.sample_size(30);
    for (size_name, size) in SIZES {
        let msg = vec![0xC3u8; size];
        group.throughput(Throughput::Bytes(2 * size as u64));
        let server_addr = free_addr();
        let relay = LossyRelay::start(server_addr, LOSS_ONE_IN);
        let mut pair = pair(Transport::Udp(udp_config()), server_addr, relay.addr);
        group.bench_function(BenchmarkId::new("udp", size_name), |b| {
            b.iter(|| pair.round_trip(&msg));
        });
        drop(pair);
        drop(relay);
    }
    group.finish();
}

criterion_group!(benches, bench_rtt, bench_throughput, bench_rtt_loss);
criterion_main!(benches);
