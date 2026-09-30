# Queue benchmark

`queue.rs` measures the MPMC and SPMC queues with one pinned producer and one
pinned broadcast consumer, through the copying `produce` and `try_consume`
API. A credit window keeps at most 512 messages outstanding so the producer
never overwrites unread slots. Each repetition creates a fresh queue, validates 32768 messages against
the expected payload sequence, warms up with 32768 more, then measures.

```sh
FLUX_BENCH_CPUS=14,15 cargo bench -p flux-communication --bench queue
FLUX_BENCH_CPUS=14,15 FLUX_BENCH_MODE=latency cargo bench -p flux-communication --bench queue
```

Choose two idle physical cores, preferably sharing an L3, with idle SMT
siblings, and use the same compiler settings for every case in a comparison.

| Variable | Default | Purpose |
|---|---|---|
| `FLUX_BENCH_CPUS` | required | Producer and consumer CPUs, for example `14,15`. |
| `FLUX_BENCH_MODE` | `throughput` | `throughput` or `latency`. |
| `FLUX_BENCH_SIZE` | all | One payload size: 8, 32, 64, 128, 256 or 1024 bytes. |
| `FLUX_BENCH_QUEUE` | both | `MPMC` or `SPMC`. |
| `FLUX_BENCH_RUNS` | `5` | Repetitions per case; the queue order alternates between runs. |
| `FLUX_BENCH_MESSAGES` | throughput `16777216`, latency `1048576` | Measured messages per repetition. |
| `FLUX_BENCH_PAUSE_MIN`, `FLUX_BENCH_PAUSE_MAX` | `25`, `150` | Producer `PAUSE` count range before each latency message. |

**Throughput** sends as fast as credits allow and reports millions of messages
per second from the producer's start to the consumer's finish, with mean and
sample SD across runs.

**Latency** stamps each message with `RDTSCP` after a pseudorandom pause,
stamps again on receipt and records the difference in an HdrHistogram. The
report gives mean, SD and percentiles in nanoseconds per run and pooled over
all runs, plus the achieved rate and how many sends were stamped before the
previous receive (a backlog diagnostic). These are post-pause send-to-receive
times including clock overhead; they exclude producer credit waits.

Lines beginning with `#` are settings and diagnostics; the rest is CSV with
the columns named in the header.
