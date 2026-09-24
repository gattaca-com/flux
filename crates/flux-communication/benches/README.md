# Raw queue comparison

`spsc.rs` compares Flux MPMC, Flux SPSC and rtrb with one producer and one consumer. It reports saturated throughput separately from paced one-way latency. SPSC and rtrb borrow a slot for the callback; MPMC uses its copying reader. Each callback reads one byte in the middle of the message into a checked sum. Producer and consumer references pass through `black_box`.

## Run

On Linux, select two otherwise idle physical cores, preferably sharing L3. Keep their SMT siblings idle too. Check the topology and any isolated cores:

```sh
cat /sys/devices/system/cpu/isolated
lscpu -e=CPU,CORE,SOCKET,NODE,CACHE
```

For example, with producer CPU 13 and consumer CPU 12:

```sh
RUSTFLAGS="-C target-cpu=native -C panic=abort" \
CARGO_PROFILE_BENCH_CODEGEN_UNITS=1 CARGO_PROFILE_BENCH_LTO=fat \
FLUX_BENCH_CPUS=13,12 cargo bench -p flux-communication --bench spsc --locked
```

Use those compiler settings when comparing with the PR results. The executable pins its workers and keeps the controller off their physical cores. If the machine has isolated CPUs, the workers must use them. If it has none, it warns and uses the explicitly selected CPUs. It does not check for competing jobs; coordinate other benchmarks yourself. Different CPUs, placement, compiler versions and generated code can change both results and queue rankings.

The default runs five repetitions for each queue at 8, 32, 64, 128, 192, 256, 512 and 1024 bytes. Allow several minutes after compilation. To narrow the comparison, add environment variables before `cargo bench`:

| Variable | Default | Purpose |
|---|---|---|
| `FLUX_BENCH_SIZE` | all sizes | Select one of the compiled payload sizes. |
| `FLUX_BENCH_QUEUE` | all queues | Select `MPMC`, `SPSC` or `RTRB`. |
| `FLUX_BENCH_RUNS` | `5` | Repetitions per queue/size. |
| `FLUX_BENCH_MESSAGES` | `16777216` | Messages per measured phase; positive multiple of 1024. |
| `FLUX_BENCH_VERIFY_ONLY` | unset | Set to `1` to run only full-payload/FIFO validation. |

## Workload and output

The ring holds 1024 messages and the precomputed source pool holds 4096. Publication and release happen per message, including rtrb's single-slot `read_chunk(1)`/`commit_all()`. Both SPSC implementations use their normal copying producer interface. A shared limit of 512 outstanding messages prevents MPMC from overwriting unread data; producer credit checks run in groups of 64. These checks are included in timing. Endpoint storage is separated so the harness does not make the workers' private cursors share cache lines.

Each case first checks 32768 unique messages for FIFO order and full payload integrity. Before each measurement it settles with 32768 messages using the measured callback. Setup and thread joins are excluded from the timing bracket. Every measured phase checks the completed count and selected-byte sum.

Throughput has no artificial pauses or per-message timestamps. The separate latency phase inserts 2–50 pseudorandom x86 `PAUSE` instructions before every enqueue (`spin_loop` hints on other architectures). One randomly selected message per 1024 is timestamped outside the payload. Latency runs from enqueue entry through the consumer's byte read; it excludes the pause and includes queue backlog and clock overhead. It need not include subsequent slot release.

CSV lines beginning with `run` contain throughput in millions of messages per second and p50/p95 latency in nanoseconds. `summary` lines contain the arithmetic mean and sample standard deviation of all throughput runs, plus p50/p95 pooled over their latency samples. Nothing is trimmed or discarded. With one run, SD is `NaN`. These measurements describe a small callback and cache-hot repeated payloads; they do not predict every application workload.

The queue adapters are near the top of `spsc.rs`; `produce`, `consume` and `phase` contain the shared measurement loop. Workload constants are at the top, and the payload-size dispatch is in `main`.
