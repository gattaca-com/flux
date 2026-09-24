# Spine queue comparison

`spine_queues` compares MPMC and SPSC through `SpineAdapter`, with consumption telemetry disabled and enabled. SPSC borrows messages; MPMC uses its copying consumer. Every callback black-boxes its reference and reads the middle byte into a checked sum. It uses the same timing and validation code as the [raw queue comparison](../../flux-communication/benches/README.md).

## Run

Choose two idle physical cores and keep their SMT siblings idle. On Linux, check `lscpu -e=CPU,CORE,SOCKET,NODE,CACHE` and `cat /sys/devices/system/cpu/isolated`. If isolation is configured, workers must use those CPUs. For producer 13 and consumer 12:

```sh
RUSTFLAGS="-C target-cpu=native -C panic=abort" \
CARGO_PROFILE_BENCH_CODEGEN_UNITS=1 CARGO_PROFILE_BENCH_LTO=fat \
FLUX_BENCH_CPUS=13,12 cargo bench -p flux --bench spine_queues --locked
```

The default is five repetitions of all four cases at 8, 32, 64, 128, 192, 256, 512 and 1024 application bytes. Allow several minutes after compilation. The options are:

| Variable | Default | Purpose |
|---|---|---|
| `FLUX_BENCH_SIZE` | all sizes | Select one of the compiled payload sizes. |
| `FLUX_BENCH_QUEUE` | both | `MPMC` or `SPSC`. |
| `FLUX_BENCH_TELEMETRY` | both | `none` or `all` consumption records. |
| `FLUX_BENCH_RUNS` | `5` | Repetitions per queue/size/telemetry case. |
| `FLUX_BENCH_MESSAGES` | `16777216` | Messages per measured phase; positive multiple of 1024. |
| `FLUX_BENCH_VERIFY_ONLY` | unset | Set to `1` for full-payload/FIFO and telemetry checks only. |

## What is measured

Each data queue is heap-backed and holds 1024 messages. Spine adds its `InternalMessage` metadata to the application payload. The source pool holds 4096 messages; a common 512-message credit window prevents MPMC overwrite. Producer bookkeeping uses 64-message groups, with individual message publication and release. The producer calls `begin_loop(IngestionTime::now())` per group, and the consumer calls it before each drain.

`none` uses `consume_maybe_track` or `consume_ref_maybe_track` with a callback returning `false`. It retains ingestion propagation and the initial consumer clock read. `all` uses the ordinary tracking consumer and writes both a timing record and a latency record per message. Those records use real shared-memory telemetry queues; no observer drains them during the benchmark. After each phase, outside timing, the benchmark checks that each telemetry queue advanced by exactly the expected count.

Throughput is saturated and has no artificial pauses or per-message latency timestamps. A separate latency phase pauses the producer for 2–50 pseudorandom x86 `PAUSE` instructions before each enqueue (`spin_loop` elsewhere). One message per 1024 is sampled from enqueue entry through the callback's byte read. This interval excludes the preceding pause, includes backlog and clock overhead, and does not include subsequent consumption-telemetry writes.

Each case validates 32768 unique messages for full payload and FIFO order, then settles each measured specialization with 32768 messages. Counts and byte sums are checked after every phase. CSV `run` and `summary` lines use case names such as `SPSC-none` and `MPMC-all`. Summary throughput is mean ± sample SD across all runs, without trimming; p50/p95 latency pools their samples. With one run, SD is `NaN`.

Setup uses a fresh temporary directory, printed to stderr. Successful cases remove only their own mappings and directory after the adapters are dropped. An abort leaves the directory for inspection and manual cleanup. The benchmark does not touch an application's existing Spine directories.

This measures adapter and telemetry costs, without the tile runner, parking, DCaches or a telemetry-draining thread. It is representative of a very small callback, not a prediction for every application. Compiler, machine and code layout changes can alter results; raw-versus-Spine differences are not a controlled subtraction of adapter overhead.

`spine_queues.rs` selects the cases; `spine_queues/adapters.rs` contains the single-queue Spine bundles, API calls and telemetry checks. Explicit bundles allow one const-generic definition for all payload sizes. The shared workload and statistics live in the workspace's `benches/queue_support.rs`.
