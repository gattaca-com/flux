# Spine queue comparison

`spine_queues` compares MPMC, SPMC and SPSC through `SpineAdapter`, with consumption telemetry disabled and enabled. SPSC borrows messages; MPMC and SPMC use their copying consumer. The [shared methodology](../../../benches/README.md) describes the options and measurements shared with the [raw queue comparison](../../flux-communication/benches/README.md).

## Run

For producer CPU 13 and consumer CPU 12:

```sh
RUSTFLAGS="-C target-cpu=native -C panic=abort" \
CARGO_PROFILE_BENCH_CODEGEN_UNITS=1 CARGO_PROFILE_BENCH_LTO=fat \
FLUX_BENCH_CPUS=13,12 cargo bench -p flux --bench spine_queues --locked
```

Add `FLUX_BENCH_MODE=latency` for latency. `FLUX_BENCH_QUEUE` selects `MPMC`, `SPMC` or `SPSC`, and `FLUX_BENCH_TELEMETRY` selects `none` or `all`. By default each mode runs five repetitions of all six cases at all eight sizes. CSV records name the cases by queue and telemetry, for example `SPMC-none` and `SPMC-all`.

## What differs from the raw benchmark

Each data queue is heap-backed and holds 1024 messages. Spine adds its `InternalMessage` metadata to the application payload. The producer calls `begin_loop(IngestionTime::now())` per 64-message group, and the consumer calls it before each drain, including latency retries after an empty read.

`none` uses `consume_maybe_track` or `consume_ref_maybe_track` with a callback returning `false`. It retains ingestion propagation and the initial consumer clock read. `all` enables consumption tracking and writes both a timing record and a latency record per message. Those records use real shared-memory telemetry queues; no observer drains them during the benchmark. Between rounds, the controller checks that each telemetry queue advanced by exactly that round's message count, or not at all for `none`.

Setup uses a fresh temporary directory, printed to stderr. Successful cases remove only their own mappings and directory after the adapters are dropped. An abort leaves the directory for inspection and manual cleanup. The benchmark does not touch an application's existing Spine directories.

This measures queues through their adapters, including telemetry when enabled, without a tile runner, parking, DCaches or a telemetry-draining thread. It is representative of a very small callback, not a prediction for every application. Compiler, machine and code layout changes can alter results; raw-versus-Spine differences are not a controlled subtraction of adapter overhead.

`spine_queues.rs` selects the cases; `spine_queues/adapters.rs` contains the single-queue Spine bundles, API calls and telemetry checks. Explicit bundles allow one const-generic definition for all payload sizes.

## SPSC slot layouts

Set `FLUX_BENCH_QUEUE=SPSC FLUX_BENCH_SLOT=64`, `128`, `192` or `256` to select the exact slot stride. Alignment matches the stride except for 192 B slots, which use 64 B alignment. The default, `natural`, retains the stored message layout. Only the message is copied; padding is unused. The same selection works with throughput, latency and verify modes.

Supported application sizes are 8/32 B for 64 B slots, 8/32/64 B for 128 B slots, 8/32/64/128 B for 192 B slots, and 8/32/64/128/192 B for 256 B slots. These combinations also accommodate Spine tracking metadata. Without a size filter, only supported sizes run. Natural slots support all eight sizes.

For a 192 B payload in a 256 B slot, set `FLUX_BENCH_QUEUE=SPSC FLUX_BENCH_SIZE=192 FLUX_BENCH_SLOT=256`.

Full-payload validation checks borrowed slot addresses for the selected stride and alignment, accounting for Spine metadata before the application payload. Geometry checks are outside measured loops. Keep the printed slot geometry with CSV results.
