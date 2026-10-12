//! TCP vs reliable UDP through `Network`, on loopback.
//!
//! `rtt`: one message to the server and its echo back, both connectors polled
//! from this thread. `throughput`: a burst of messages one way, timed until
//! the last one lands. `rtt_loss`: UDP through a relay dropping 1 in 100
//! datagrams. TCP has no loss variant: a byte-dropping proxy is not packet
//! loss, and netem needs root.

// TODO: delete, benching against loopback is misleading.

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
use flux_network::{
    Group, GroupConfig, Network, NetworkEvent, ReplayPolicy, TcpGroupConfig, UdpConfig,
    UdpGroupConfig,
};
use mio::Token;

const SIZES: [(&str, usize); 3] = [("2k", 2 * 1024), ("64k", 64 * 1024), ("2m", 2 * 1024 * 1024)];
const BURST: usize = 256;
const LOSS_ONE_IN: usize = 100;

fn free_addr() -> SocketAddr {
    loop {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        if UdpSocket::bind(addr).is_ok() {
            return addr;
        }
    }
}

/// Where one side sends: a TCP connection, or the UDP group it publishes on.
#[derive(Clone, Copy)]
enum Route {
    Token(Token),
    Group(Group),
}

fn send(network: &mut Network, route: Route, msg: &[u8]) {
    match route {
        Route::Token(token) => {
            network.send_with(token, |b| b.extend_from_slice(msg));
        }
        Route::Group(group) => {
            network.broadcast_with(group, |b| b.extend_from_slice(msg));
        }
    }
}

/// A connected pair. UDP is one-way, so over UDP each side publishes and
/// subscribes to the other.
struct Pair {
    server: Network,
    client: Network,
    to_client: Route,
    to_server: Route,
}

fn udp_config() -> UdpConfig {
    // Loopback holds a 2 MiB burst without dropping when the kernel buffers
    // are raised; the bench sets them on the connector.
    UdpConfig { max_message_size: 4 * 1024 * 1024, ..UdpConfig::lan() }
}

fn connector(config: GroupConfig) -> (Network, Group) {
    let mut network = Network::default();
    let group = network.add_group(config.with_socket_buf_size(16 * 1024 * 1024));
    (network, group)
}

/// `back` is where the client publishes and where the server dials it,
/// which differ when a relay sits between; TCP ignores it.
fn pair(
    transport: GroupConfig,
    listen: SocketAddr,
    dial: SocketAddr,
    back: (SocketAddr, SocketAddr),
) -> Pair {
    let udp = matches!(transport, GroupConfig::Udp(_));
    let (mut server, server_group) = connector(transport.clone());
    let (mut client, client_group) = connector(transport);
    server.listen(server_group, listen).unwrap();
    let client_token = client.connect(client_group, dial);
    if udp {
        client.listen(client_group, back.0).unwrap();
        let _ = server.connect(server_group, back.1);
    }
    let mut accepted = None;
    let mut client_accepted = !udp;
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted.is_none() ||
        !client_accepted ||
        client.currently_disconnected().count() != 0 ||
        server.currently_disconnected().count() != 0
    {
        assert!(Instant::now() < deadline, "handshake");
        server.poll_with(|e| {
            if let NetworkEvent::Accepted { token: stream, .. } = e {
                accepted = Some(stream);
            }
        });
        client.poll_with(|e| client_accepted |= matches!(e, NetworkEvent::Accepted { .. }));
    }
    let (to_client, to_server) = if udp {
        (Route::Group(server_group), Route::Group(client_group))
    } else {
        (Route::Token(accepted.unwrap()), Route::Token(client_token))
    };
    Pair { server, client, to_client, to_server }
}

impl Pair {
    /// Sends `msg` and spins until the echo is back.
    fn round_trip(&mut self, msg: &[u8]) {
        let Self { server, client, to_client, to_server } = self;
        send(client, *to_server, msg);
        let mut got = false;
        while !got {
            client.poll_with(|_| {});
            server.poll_with(|e| got |= matches!(e, NetworkEvent::Message { .. }));
        }
        send(server, *to_client, msg);
        got = false;
        while !got {
            server.poll_with(|_| {});
            client.poll_with(|e| {
                if let NetworkEvent::Message { payload, .. } = e {
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
                send(&mut self.client, self.to_server, msg);
                sent += 1;
            }
            self.client.poll_with(|_| {});
            self.server.poll_with(|e| {
                if let NetworkEvent::Message { .. } = e {
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

fn transports() -> [(&'static str, GroupConfig); 2] {
    [
        (
            "tcp",
            TcpGroupConfig {
                aligned_payloads: true,
                replay: ReplayPolicy::Replay,
                ..Default::default()
            }
            .into(),
        ),
        ("udp", UdpGroupConfig { udp: udp_config(), ..Default::default() }.into()),
    ]
}

fn bench_rtt(c: &mut Criterion) {
    let mut group = c.benchmark_group("rtt");
    for (size_name, size) in SIZES {
        let msg = vec![0xA5u8; size];
        group.throughput(Throughput::Bytes(2 * size as u64));
        for (name, transport) in transports() {
            let (addr, back) = (free_addr(), free_addr());
            let mut pair = pair(transport, addr, addr, (back, back));
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
            let (addr, back) = (free_addr(), free_addr());
            let mut pair = pair(transport, addr, addr, (back, back));
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
        let (server_addr, client_addr) = (free_addr(), free_addr());
        let relay = LossyRelay::start(server_addr, LOSS_ONE_IN);
        let back_relay = LossyRelay::start(client_addr, LOSS_ONE_IN);
        let mut pair = pair(
            UdpGroupConfig { udp: udp_config(), ..Default::default() }.into(),
            server_addr,
            relay.addr,
            (client_addr, back_relay.addr),
        );
        group.bench_function(BenchmarkId::new("udp", size_name), |b| {
            b.iter(|| pair.round_trip(&msg));
        });
        drop(pair);
        drop(relay);
        drop(back_relay);
    }
    group.finish();
}

criterion_group!(benches, bench_rtt, bench_throughput, bench_rtt_loss);
criterion_main!(benches);
