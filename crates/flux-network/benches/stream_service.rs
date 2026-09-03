//! What retaining a message costs against delivering it to a sink.
//!
//! `StreamService<Retained>` copies each payload once into its arena and
//! lends it back at the pull; a caller's `StreamSink` sees the payload
//! borrowed from the connection's read buffer and copies nothing. The two
//! are measured per frame at the frame sizes the consumers send, twice: the
//! sink layer alone, which isolates the copy and the record, and a loopback
//! round trip through the network, which places it against everything else
//! an iteration costs. A third group sends a full FEC set of shred-sized
//! payloads to one peer as eight sends and as one batch, so what coalescing
//! saves is read against the same sink.

use std::{
    hint::black_box,
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use flux_network::{
    Token,
    stream::{
        ConnectionGroupConfig, Endpoint, Framing, StreamEvent, StreamNetwork, StreamService,
        StreamSink,
    },
};
use flux_timing::Nanos;

/// A shred-sized batch, a full FEC set of them, and a stress point no batch
/// reaches.
const SIZES: &[(&str, usize)] = &[("1KiB", 1024), ("64KiB", 64 * 1024), ("1MiB", 1024 * 1024)];

/// A sink that consumes in place: what a forwarding or parsing handler costs
/// the service, and nothing else.
#[derive(Default)]
struct Counting {
    bytes: usize,
}

impl StreamSink for Counting {
    fn on_event(&mut self, event: StreamEvent<'_>) {
        if let StreamEvent::Message { payload, .. } = event {
            self.bytes += payload.len();
        }
    }

    /// Counted on the spot: nothing is held.
    fn has_pending(&self) -> bool {
        false
    }
}

fn config(name: &'static str) -> ConnectionGroupConfig {
    ConnectionGroupConfig {
        name,
        framing: Framing::LengthPrefixed,
        ..ConnectionGroupConfig::default()
    }
}

const ZERO: Option<flux_timing::Duration> = Some(flux_timing::Duration(0));

fn bound_addr(bound: std::io::Result<Endpoint>) -> SocketAddr {
    match bound.unwrap() {
        Endpoint::Tcp(addr) => addr,
        Endpoint::Unix(_) => unreachable!("a TCP listener"),
    }
}

/// The sink layer alone: one message handed to the sink and, for the retained
/// one, pulled straight back.
fn sink_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("sink_only");
    let mut net = StreamNetwork::default();
    for (label, size) in SIZES {
        group.throughput(Throughput::Bytes(*size as u64));
        let payload = vec![7; *size];

        let mut retained = StreamService::new(net.add_group(config("retained")));
        group.bench_with_input(BenchmarkId::new("retained", label), size, |b, _| {
            b.iter(|| {
                let payload = black_box(&payload[..]);
                retained.sink_mut().on_event(StreamEvent::Message {
                    token: Token(1),
                    payload,
                    send_ts: Nanos(0),
                });
                let mut pulled = 0;
                while let Some(StreamEvent::Message { payload, .. }) = retained.next_event() {
                    pulled += payload.len();
                }
                black_box(pulled)
            });
        });

        let mut sink = StreamService::with_sink(net.add_group(config("sink")), Counting::default());
        group.bench_with_input(BenchmarkId::new("sink", label), size, |b, _| {
            b.iter(|| {
                let payload = black_box(&payload[..]);
                sink.sink_mut().on_event(StreamEvent::Message {
                    token: Token(1),
                    payload,
                    send_ts: Nanos(0),
                });
                black_box(sink.sink().bytes)
            });
        });
    }
    group.finish();
}

/// One frame sent by the dialler and received whole by the listener, with
/// the pull that reads it back where there is one.
fn loopback(c: &mut Criterion) {
    let mut group = c.benchmark_group("loopback");
    group.measurement_time(Duration::from_secs(8));
    for (label, size) in SIZES {
        group.throughput(Throughput::Bytes(*size as u64));
        let payload = vec![7; *size];

        {
            let mut net = StreamNetwork::default();
            let mut server = StreamService::new(net.add_group(config("server")));
            let addr = bound_addr(server.listen(Endpoint::Tcp((Ipv4Addr::LOCALHOST, 0).into())));
            let mut dialler = StreamService::new(net.add_group(config("dialler")));
            let token = dialler.connect(Endpoint::Tcp(addr));
            let mut services = [server, dialler];
            while services[0].pending() == 0 || services[1].pending() == 0 {
                let _ = net.drive(ZERO, &mut services);
            }
            while services[0].next_event().is_some() {}
            while services[1].next_event().is_some() {}

            group.bench_with_input(BenchmarkId::new("retained", label), size, |b, _| {
                b.iter(|| {
                    assert!(services[1].send_with(token, |out| out.extend_from_slice(&payload)));
                    let mut received = 0;
                    while received < *size {
                        let _ = net.drive(ZERO, &mut services);
                        while let Some(event) = services[0].next_event() {
                            if let StreamEvent::Message { payload, .. } = event {
                                received += payload.len();
                            }
                        }
                    }
                    black_box(received)
                });
            });
        }

        {
            let mut net = StreamNetwork::default();
            let mut server =
                StreamService::with_sink(net.add_group(config("server")), Counting::default());
            let addr = bound_addr(server.listen(Endpoint::Tcp((Ipv4Addr::LOCALHOST, 0).into())));
            let mut dialler =
                StreamService::with_sink(net.add_group(config("dialler")), Counting::default());
            let token = dialler.connect(Endpoint::Tcp(addr));
            let mut services = [server, dialler];
            // Until the dialler's first send succeeds, the connection is not up.
            while !services[1].send_with(token, |out| out.extend_from_slice(b"up")) {
                let _ = net.drive(ZERO, &mut services);
            }
            while services[0].sink().bytes < 2 {
                let _ = net.drive(ZERO, &mut services);
            }

            group.bench_with_input(BenchmarkId::new("sink", label), size, |b, _| {
                b.iter(|| {
                    let before = services[0].sink().bytes;
                    assert!(services[1].send_with(token, |out| out.extend_from_slice(&payload)));
                    while services[0].sink().bytes < before + *size {
                        let _ = net.drive(ZERO, &mut services);
                    }
                    black_box(services[0].sink().bytes)
                });
            });
        }
    }
    group.finish();
}

/// A full FEC set of shred-sized payloads: how many, and how large each.
const BATCH: (usize, usize) = (8, 1280);

fn batch(c: &mut Criterion) {
    let (count, size) = BATCH;
    let mut group = c.benchmark_group("batch");
    group.measurement_time(Duration::from_secs(8));
    group.throughput(Throughput::Bytes((count * size) as u64));
    let payload = vec![7; size];
    let items: Vec<&[u8]> = vec![payload.as_slice(); count];

    let mut net = StreamNetwork::default();
    let mut server = StreamService::with_sink(net.add_group(config("server")), Counting::default());
    let addr = bound_addr(server.listen(Endpoint::Tcp((Ipv4Addr::LOCALHOST, 0).into())));
    let mut dialler =
        StreamService::with_sink(net.add_group(config("dialler")), Counting::default());
    let token = dialler.connect(Endpoint::Tcp(addr));
    let mut services = [server, dialler];
    // Until the dialler's first send succeeds, the connection is not up.
    while !services[1].send_with(token, |out| out.extend_from_slice(b"up")) {
        let _ = net.drive(ZERO, &mut services);
    }
    while services[0].sink().bytes < 2 {
        let _ = net.drive(ZERO, &mut services);
    }

    group.bench_function(BenchmarkId::new("eight_sends", "8x1280B"), |b| {
        b.iter(|| {
            let before = services[0].sink().bytes;
            for _ in 0..count {
                assert!(services[1].send_with(token, |out| out.extend_from_slice(&payload)));
            }
            while services[0].sink().bytes < before + count * size {
                let _ = net.drive(ZERO, &mut services);
            }
            black_box(services[0].sink().bytes)
        });
    });
    group.bench_function(BenchmarkId::new("one_batch", "8x1280B"), |b| {
        b.iter(|| {
            let before = services[0].sink().bytes;
            assert!(services[1].send_many_with(token, items.iter().copied(), |out, item| {
                out.extend_from_slice(item);
            }));
            while services[0].sink().bytes < before + count * size {
                let _ = net.drive(ZERO, &mut services);
            }
            black_box(services[0].sink().bytes)
        });
    });
    group.finish();
}

criterion_group!(benches, sink_only, loopback, batch);
criterion_main!(benches);
