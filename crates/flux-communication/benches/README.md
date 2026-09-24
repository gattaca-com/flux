# Raw queue comparison

`spsc.rs` compares Flux MPMC, Flux SPMC, Flux SPSC and rtrb with one producer and one consumer. SPSC and rtrb borrow a slot for the callback; MPMC and SPMC use their copying reader. rtrb consumes and releases one slot at a time with `read_chunk(1)` and `commit_all()`. Producers use Flux SPSC's `produce(&message)` and rtrb's `push(message)` copying APIs. The [shared methodology](../../../benches/README.md) describes the options, rounds, measurements and output.

## Run

For example, with producer CPU 13 and consumer CPU 12:

```sh
RUSTFLAGS="-C target-cpu=native -C panic=abort" \
CARGO_PROFILE_BENCH_CODEGEN_UNITS=1 CARGO_PROFILE_BENCH_LTO=fat \
FLUX_BENCH_CPUS=13,12 cargo bench -p flux-communication --bench spsc --locked
```

Add `FLUX_BENCH_MODE=latency` for latency. Use the same compiler settings for every case in a comparison. `FLUX_BENCH_QUEUE` selects `MPMC`, `SPMC`, `SPSC` or `RTRB`. By default each mode runs five repetitions of every queue at all eight sizes; allow several minutes after compilation.

The queue adapters are near the top of `spsc.rs`; the payload-size dispatch is in `main`.

## SPSC slot layouts

Set `FLUX_BENCH_QUEUE=SPSC FLUX_BENCH_SLOT=64`, `128` or `256` to select the exact slot size and alignment. The default, `natural`, retains the stored message layout. Only the message is copied; padding is unused. The same selection works with throughput, latency and verify modes.

Supported application sizes are 8/32 B for 64 B slots, 8/32/64 B for 128 B slots, and 8/32/64/128/192 B for 256 B slots. These combinations also accommodate Spine tracking metadata. Without a size filter, only supported sizes run. Natural slots support all eight sizes.

Full-payload validation checks borrowed slot addresses for the selected stride and alignment, accounting for Spine metadata before the application payload. Geometry checks are outside measured loops. Keep the printed slot geometry with CSV results.
