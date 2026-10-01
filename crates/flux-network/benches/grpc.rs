//! Server cost of fanning one prepared message out to N open streams,
//! including the TCP write in `drive`. Only `send` + `poll` are timed; between
//! rounds the bench waits for a reader thread to drain the socket, so every
//! round starts with writable sockets, as for subscribers that keep up.

use std::{
    io::{Read, Write},
    net::{Ipv4Addr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use flux_network::{
    Network,
    grpc::{GrpcConfig, GrpcServer, Response, Stream},
    http2::{self, Decoder, Payload, encode_frame, flags, kind},
};

/// Below the default 64 KiB windows, so streams never stall on credit.
const CREDIT_STEP: u32 = 16 * 1024;

fn frame(out: &mut Vec<u8>, ty: u8, fl: u8, id: u32, data: &[u8]) {
    encode_frame(ty, fl, id, data, http2::DEFAULT_MAX_FRAME_SIZE, out).unwrap();
}

fn subscribe(id: u32, out: &mut Vec<u8>) {
    let mut block = vec![0x83, 0x86];
    for (name, value) in [
        (":path", "/bench.Feed/Subscribe"),
        (":authority", "localhost"),
        ("content-type", "application/grpc"),
        ("te", "trailers"),
    ] {
        block.extend_from_slice(&[0, name.len() as u8]);
        block.extend_from_slice(name.as_bytes());
        block.push(value.len() as u8);
        block.extend_from_slice(value.as_bytes());
    }
    frame(out, kind::HEADERS, flags::END_HEADERS, id, &block);
    frame(out, kind::DATA, flags::END_STREAM, id, &[0; 5]);
}

/// Reads everything and returns credit per stream and for the connection.
fn drain(mut socket: TcpStream, read: &AtomicU64) {
    let mut writer = socket.try_clone().unwrap();
    let mut decoder = Decoder::new(http2::DEFAULT_MAX_FRAME_SIZE, 65536).unwrap();
    let mut input = Vec::new();
    let mut unreturned = std::collections::HashMap::<u32, u32>::new();
    let mut buffer = vec![0; 1 << 16];
    loop {
        let Ok(n @ 1..) = socket.read(&mut buffer) else { return };
        input.extend_from_slice(&buffer[..n]);
        read.fetch_add(n as u64, Ordering::Release);
        let mut consumed = 0;
        let mut updates = Vec::new();
        while let Ok(Some((f, n))) = decoder.decode(&input[consumed..]) {
            consumed += n;
            if let Payload::Data { flow_controlled_len, .. } = f.payload {
                for id in [0, f.stream_id] {
                    let owed = unreturned.entry(id).or_default();
                    *owed += flow_controlled_len;
                    if *owed >= CREDIT_STEP {
                        frame(&mut updates, kind::WINDOW_UPDATE, 0, id, &owed.to_be_bytes());
                        *owed = 0;
                    }
                }
            }
        }
        input.drain(..consumed);
        if !updates.is_empty() && writer.write_all(&updates).is_err() {
            return;
        }
    }
}

fn fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("grpc_fanout");
    for streams in [1u32, 64] {
        let addr = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap();
        let mut net = Network::default();
        let config = GrpcConfig::default();
        let mut server = GrpcServer::new(&mut net, config).unwrap();
        server.listen(&mut net, addr).unwrap();
        let mut socket = TcpStream::connect(addr).unwrap();
        let mut wire = http2::CLIENT_PREFACE.to_vec();
        frame(&mut wire, kind::SETTINGS, 0, 0, &[]);
        for n in 0..streams {
            subscribe(2 * n + 1, &mut wire);
        }
        socket.write_all(&wire).unwrap();
        let read = Arc::new(AtomicU64::new(0));
        let reader = thread::spawn({
            let read = read.clone();
            move || drain(socket, &read)
        });

        let mut open: Vec<Stream> = Vec::new();
        while open.len() < streams as usize {
            server.poll(&mut net, |request, _| {
                open.push(request.stream());
                Response::Stream
            });
        }
        // Let the response headers drain before counting bytes.
        let settle = Instant::now() + Duration::from_millis(100);
        while Instant::now() < settle {
            server.poll(&mut net, |_, _| unreachable!());
        }
        let message = [0x5a; 1024];
        // One DATA frame per stream per round.
        let round_bytes = u64::from(streams) * (1024 + 5 + 9);
        let mut expected = read.load(Ordering::Acquire);
        group.throughput(Throughput::Elements(u64::from(streams)));
        group.bench_function(BenchmarkId::new("1KiB", streams), |b| {
            b.iter_custom(|rounds| {
                let mut timed = Duration::ZERO;
                for _ in 0..rounds {
                    let start = Instant::now();
                    for &stream in &open {
                        server.send(stream, &message).unwrap();
                    }
                    server.poll(&mut net, |_, _| unreachable!());
                    timed += start.elapsed();
                    expected += round_bytes;
                    while read.load(Ordering::Acquire) < expected {
                        server.poll(&mut net, |_, _| unreachable!());
                    }
                }
                timed
            });
        });
        drop(server);
        drop(net);
        reader.join().unwrap();
    }
    group.finish();
}

criterion_group!(benches, fanout);
criterion_main!(benches);
