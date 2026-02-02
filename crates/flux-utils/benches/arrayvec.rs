use std::hint::black_box;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

const N: usize = 1024;

fn bench_push_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_pop");

    group.bench_function("ArrayVec", |b| {
        b.iter(|| {
            let mut v: flux_utils::ArrayVec<u32, N> = flux_utils::ArrayVec::new();
            for i in 0..N as u32 {
                v.push(black_box(i));
            }
            while v.pop().is_some() {}
            black_box(v);
        })
    });

    group.bench_function("tinyvec::ArrayVec", |b| {
        b.iter(|| {
            let mut v: tinyvec::ArrayVec<[u32; N]> = tinyvec::ArrayVec::new();
            for i in 0..N as u32 {
                v.push(black_box(i));
            }
            while v.pop().is_some() {}
            black_box(v);
        })
    });

    group.finish();
}

fn bench_indexed_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexed_access");

    group.bench_function("ArrayVec", |b| {
        b.iter_batched(
            || {
                let mut v: flux_utils::ArrayVec<u32, N> = flux_utils::ArrayVec::new();
                for i in 0..N as u32 {
                    v.push(i);
                }
                v
            },
            |v| {
                // access pattern that the compiler can't trivially fold away
                let mut acc = 0u32;
                for i in 0..N {
                    acc = acc.wrapping_add(black_box(v[i]));
                }
                black_box(acc);
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("tinyvec::ArrayVec", |b| {
        b.iter_batched(
            || {
                let mut v: tinyvec::ArrayVec<[u32; N]> = tinyvec::ArrayVec::new();
                for i in 0..N as u32 {
                    v.push(i);
                }
                v
            },
            |v| {
                let mut acc = 0u32;
                for i in 0..N {
                    acc = acc.wrapping_add(black_box(v[i]));
                }
                black_box(acc);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_get_mut(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_mut");

    group.bench_function("ArrayVec", |b| {
        b.iter_batched(
            || {
                let mut v: flux_utils::ArrayVec<u32, N> = flux_utils::ArrayVec::new();
                for i in 0..N as u32 {
                    v.push(i);
                }
                v
            },
            |mut v| {
                for i in 0..N {
                    v[i] = black_box(i as u32).wrapping_mul(3);
                }
                black_box(v);
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("tinyvec::ArrayVec", |b| {
        b.iter_batched(
            || {
                let mut v: tinyvec::ArrayVec<[u32; N]> = tinyvec::ArrayVec::new();
                for i in 0..N as u32 {
                    v.push(i);
                }
                v
            },
            |mut v| {
                for i in 0..N {
                    v[i] = black_box(i as u32).wrapping_mul(3);
                }
                black_box(v);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(benches, bench_push_pop, bench_indexed_access, bench_get_mut);
criterion_main!(benches);
