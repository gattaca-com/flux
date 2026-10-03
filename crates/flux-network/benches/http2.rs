//! HTTP/2 protocol costs on one thread, without sockets, TLS, or application
//! work. Persistent connections reuse HPACK tables and output capacity.
//! Flow-control credit is restored each iteration, and that processing is
//! included in timing.

use std::{hint::black_box, time::Duration};

use criterion::{BenchmarkId, Criterion, Throughput};
use flux_network::http2::{
    CLIENT_PREFACE, DEFAULT_MAX_FRAME_SIZE, Decoder, Http2Config, ServerConnection, flags, kind,
};
use http::{HeaderMap, HeaderValue, StatusCode};

#[path = "common/http2.rs"]
mod fixtures;

use fixtures::{
    MINIMAL_INITIAL, MINIMAL_WARM, SENSITIVE_INITIAL, SENSITIVE_WARM, TYPICAL_INITIAL,
    TYPICAL_WARM, append_frame, credit, request, set_stream_id,
};

fn flush(server: &mut ServerConnection) {
    black_box(server.output());
    server.consume_output(server.output().len());
}

fn receive(server: &mut ServerConnection, input: &[u8]) {
    let consumed = server
        .receive(black_box(input), flux_timing::IngestionTime::default(), |event| {
            black_box(event);
        })
        .unwrap();
    assert_eq!(consumed, input.len());
}

fn connection() -> ServerConnection {
    let mut server = ServerConnection::new(Http2Config::default()).unwrap();
    let mut preface = CLIENT_PREFACE.to_vec();
    append_frame(&mut preface, kind::SETTINGS, 0, 0, &[]);
    append_frame(&mut preface, kind::SETTINGS, flags::ACK, 0, &[]);
    receive(&mut server, &preface);
    flush(&mut server);
    server
}

struct Exchange {
    server: ServerConnection,
    request: Vec<u8>,
    body: Vec<u8>,
    credit: Vec<u8>,
    response: HeaderMap,
    id: u32,
}

impl Exchange {
    fn new(initial: &[u8], warm: &[u8], size: usize) -> Self {
        let mut server = connection();
        receive(&mut server, &request(initial, &[]));
        server.send_headers(1, StatusCode::OK, &HeaderMap::new(), true).unwrap();
        flush(&mut server);
        let body = vec![0x5a; size];
        let mut response = HeaderMap::new();
        response.insert("content-type", HeaderValue::from_static("application/octet-stream"));
        let request = request(warm, &body);
        Self { server, request, body, credit: credit(size), response, id: 3 }
    }

    fn run(&mut self) {
        set_stream_id(&mut self.request, self.id);
        receive(&mut self.server, &self.request);
        if !self.body.is_empty() {
            self.server.release_capacity(self.id, self.body.len() as u32).unwrap();
        }
        self.server
            .send_headers(self.id, StatusCode::OK, &self.response, self.body.is_empty())
            .unwrap();
        let mut sent = 0;
        while sent < self.body.len() {
            sent += self.server.send_data(self.id, black_box(&self.body[sent..]), true).unwrap();
        }
        flush(&mut self.server);
        receive(&mut self.server, &self.credit);
        self.id = self.id.checked_add(2).unwrap();
    }
}

fn benchmarks(c: &mut Criterion) {
    {
        let mut codec = c.benchmark_group("http2_codec");
        let mut wire = Vec::new();
        append_frame(&mut wire, kind::DATA, 0, 1, &[0x5a; 2048]);
        let mut decoder = Decoder::new(DEFAULT_MAX_FRAME_SIZE, 64 * 1024).unwrap();
        codec.bench_function("borrowed_data_2k", |b| {
            b.iter(|| black_box(decoder.decode(black_box(&wire)).unwrap()));
        });
        codec.finish();
    }

    {
        let mut requests = c.benchmark_group("http2_exchange");
        requests.throughput(Throughput::Elements(1));
        for (name, initial, warm, size) in [
            ("minimal_empty", MINIMAL_INITIAL, MINIMAL_WARM, 0),
            ("typical_empty", TYPICAL_INITIAL, TYPICAL_WARM, 0),
            ("typical_echo_2k", TYPICAL_INITIAL, TYPICAL_WARM, 2048),
            ("sensitive_auth_echo_2k", SENSITIVE_INITIAL, SENSITIVE_WARM, 2048),
            ("typical_echo_32k", TYPICAL_INITIAL, TYPICAL_WARM, 32 * 1024),
        ] {
            let mut exchange = Exchange::new(initial, warm, size);
            exchange.run();
            requests.bench_function(name, |b| b.iter(|| exchange.run()));
        }
        requests.finish();
    }

    {
        let mut streams = c.benchmark_group("http2_stream_send");
        for size in [2048, 16 * 1024] {
            for count in [1, 64] {
                let mut server = connection();
                let mut updates = credit(size);
                append_frame(&mut updates, kind::WINDOW_UPDATE, 0, 1, &(size as u32).to_be_bytes());
                for i in 0..count {
                    let mut wire =
                        request(if i == 0 { MINIMAL_INITIAL } else { MINIMAL_WARM }, &[]);
                    set_stream_id(&mut wire, 1 + 2 * i);
                    receive(&mut server, &wire);
                    server
                        .send_headers(1 + 2 * i, StatusCode::OK, &HeaderMap::new(), false)
                        .unwrap();
                }
                flush(&mut server);
                let body = vec![0x5a; size];
                let mut next = 0;
                streams.throughput(Throughput::Bytes(size as u64));
                streams.bench_function(BenchmarkId::new(format!("{size}B"), count), |b| {
                    b.iter(|| {
                        let id = 1 + 2 * next;
                        assert_eq!(server.send_data(id, black_box(&body), false).unwrap(), size);
                        flush(&mut server);
                        updates[18..22].copy_from_slice(&id.to_be_bytes());
                        receive(&mut server, &updates);
                        next = (next + 1) % count;
                    });
                });
            }
        }
        streams.finish();
    }

    let mut preface = CLIENT_PREFACE.to_vec();
    append_frame(&mut preface, kind::SETTINGS, 0, 0, &[]);
    append_frame(&mut preface, kind::SETTINGS, flags::ACK, 0, &[]);
    c.bench_function("http2_connection/new_and_preface", |b| {
        b.iter(|| {
            let mut server = ServerConnection::new(Http2Config::default()).unwrap();
            receive(&mut server, &preface);
            flush(&mut server);
            black_box(server)
        });
    });
}

fn main() {
    let cores = core_affinity::get_core_ids().unwrap();
    let core = std::env::var("FLUX_BENCH_CPU").ok().map_or_else(
        || *cores.last().unwrap(),
        |value| core_affinity::CoreId { id: value.parse().unwrap() },
    );
    assert!(core_affinity::set_for_current(core));
    eprintln!("HTTP/2 protocol benchmark pinned to CPU {}", core.id);
    let mut c = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(50)
        .configure_from_args();
    benchmarks(&mut c);
    c.final_summary();
}
