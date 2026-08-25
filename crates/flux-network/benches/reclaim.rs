//! How an HTTP connection buffer reclaims the bytes it has answered.
//!
//! Under pipelined keep-alive the buffer always holds a partial tail, so
//! dropping the answered prefix after every response moves that tail once per
//! request. The cursor keeps the prefix and moves the tail only when the
//! prefix has grown to half the buffer.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

/// A modest request: a head and a small body.
const REQUEST: usize = 200;
/// Requests arriving together on one connection.
const PIPELINE: usize = 16;
/// The head of the next request, still on its way.
const TAIL: usize = 120;
/// Batches per measured iteration.
const ROUNDS: usize = 64;

fn arrivals() -> Vec<u8> {
    (0..PIPELINE * REQUEST).map(|index| index as u8).collect()
}

/// Drops the answered prefix after every response.
fn drain_per_response(batch: &[u8]) -> u64 {
    let mut buffer = vec![0; TAIL];
    let mut parsed = 0;
    for _ in 0..ROUNDS {
        buffer.extend_from_slice(batch);
        for _ in 0..PIPELINE {
            parsed += u64::from(buffer[0]);
            buffer.drain(..REQUEST);
        }
    }
    parsed
}

/// Keeps the answered prefix until it is half the buffer.
fn cursor_with_compaction(batch: &[u8]) -> u64 {
    let mut buffer = vec![0; TAIL];
    let mut start = 0;
    let mut consumed = 0;
    let mut parsed = 0;
    for _ in 0..ROUNDS {
        buffer.extend_from_slice(batch);
        for _ in 0..PIPELINE {
            parsed += u64::from(buffer[start]);
            consumed = start + REQUEST;
            start = consumed;
            if start >= buffer.len() / 2 {
                buffer.drain(..start);
                consumed -= start;
                start = 0;
            }
        }
    }
    let _ = consumed;
    parsed
}

fn bench_reclaim(c: &mut Criterion) {
    let batch = arrivals();
    let mut group = c.benchmark_group("connection_buffer_reclaim");
    group.bench_function("drain_per_response", |b| {
        b.iter(|| black_box(drain_per_response(black_box(&batch))));
    });
    group.bench_function("cursor_with_compaction", |b| {
        b.iter(|| black_box(cursor_with_compaction(black_box(&batch))));
    });
    group.finish();
}

criterion_group!(benches, bench_reclaim);
criterion_main!(benches);
