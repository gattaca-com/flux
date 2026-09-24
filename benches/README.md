# Queue benchmark methodology

The [raw queue](../crates/flux-communication/benches/README.md) and [Spine](../crates/flux/benches/README.md) benchmarks share a workload and worker lifecycle in `queue_support.rs`. Measurement loops and reporting live in `throughput.rs`, `latency.rs` and `window_stats.rs`; tests live in `tests.rs`. Each invocation selects one mode.

| Variable | Default | Purpose |
|---|---|---|
| `FLUX_BENCH_CPUS` | required | Producer and consumer CPUs, for example `13,12`. |
| `FLUX_BENCH_MODE` | `throughput` | `throughput`, `latency`, or `verify` (validation only, one repetition). |
| `FLUX_BENCH_SIZE` | all sizes | One of 8, 32, 64, 128, 192, 256, 512 and 1024 bytes. |
| `FLUX_BENCH_QUEUE` | all queues | One queue name; see each benchmark's README. |
| `FLUX_BENCH_SLOT` | `natural` | SPSC slot size and alignment: `natural`, `64`, `128`, or `256`. Padding requires `FLUX_BENCH_QUEUE=SPSC`; see the supported payload sizes in each benchmark's README. |
| `FLUX_BENCH_RUNS` | `5` | Repetitions per case. |
| `FLUX_BENCH_MESSAGES` | throughput `16777216`, latency `1048576` | Measured messages per repetition. |
| `FLUX_BENCH_WINDOWS` | `summary` | Throughput windows: `samples` also prints each window; `off` disables collection. |
| `FLUX_BENCH_PAUSE_MIN` | `25` | Minimum producer `PAUSE` count before each latency message. |
| `FLUX_BENCH_PAUSE_MAX` | `150` | Maximum producer `PAUSE` count, inclusive. |

## Placement and lifecycle

On Linux, choose two otherwise idle physical cores, preferably sharing L3, and keep their SMT siblings idle (see `/sys/devices/system/cpu/isolated` and `lscpu -e`). If isolated CPUs exist, the workers must use them. The controller runs outside both worker cores. Competing jobs are not detected. CPUs, placement, compilers and generated code can change results and rankings.

The ring holds 1024 messages; the source pool holds 4096. Messages are published individually; SPSC and rtrb also release each slot individually. At most 512 messages are outstanding, so MPMC and SPMC cannot overwrite unread data; the producer checks this per 64-message group, inside the timing. Both workers pass message references through `black_box`.

Each repetition creates a fresh queue and two pinned workers that own their endpoints. They check 32768 unique messages for full payload and FIFO order, then warm up with 32768 messages and measure `FLUX_BENCH_MESSAGES` using the selected mode's loops. Verify mode stops after the payload checks. The same workers handle all rounds; the main thread coordinates them.

All three threads meet at a barrier before and after every round. The producer waits for the consumer to leave the start barrier before starting its clock. Between rounds, the controller checks the completed round and resets credits before the next round can start. Recording storage is preallocated and reused. Every round checks its message count; throughput also checks the sum of bytes read, and latency checks timestamp sums and one histogram entry per message. Repetitions vary the case order; latency cases in the same repetition share a pause sequence.

## Throughput

The producer sends as fast as credits allow; the consumer reads each message's middle byte. Timing runs from the producer's start after the start barrier to the consumer's finish.

Windows record consumer progress after a drain reaches each 16384-message threshold. Counts can overshoot; the final window can be partial. The first and last boundaries are the run's start and finish. Collection adds a check after productive drains and an occasional `std::time::Instant` read. `off` removes collection but also changes generated code, so the difference does not isolate clock cost.

Windows describe **amortized nanoseconds per message**, not latency. Each run's `# windows` line gives their count, mean, sample SD, SD/mean (`cv`), sum, sum of squares and centered sum of squares (`m2`). Each window has equal weight, so the window mean can differ from total elapsed time divided by total messages. A short final window can dominate SD and CV; use a message count divisible by 16384 for noise analysis, or inspect individual windows with `samples`.

Pool window statistics using `m2` and the between-run term; subtracting large raw moments loses precision. Windows are ordered, correlated observations. Their pooled SD combines variation within and between runs and does not replace run-to-run SD. The benchmark retains all runs for analysis.

## Latency

The producer executes a pseudorandom number of pauses within the configured bounds, then writes a `flux_timing::Instant` into the first eight bytes of a pooled message, in place, and sends it. Message size is unchanged. Use one pause range for all compared cases. The consumer loads the stamp, takes an ordered end timestamp and records the tick difference in its own preallocated HdrHistogram, with three significant digits, including zero differences. Reversed clocks and values outside the preallocated range (about 60 seconds) fail the run; no sample is dropped.

Both reads call `__rdtscp()` directly, with compiler barriers and a trailing `LFENCE`; this requires x86-64 with RDTSCP and synchronized invariant TSCs. Flux's calibrated conversion to nanoseconds is applied when reporting. Fractional nanosecond output avoids whole-nanosecond truncation; it does not imply sub-nanosecond accuracy. Clock granularity depends on the CPU; the histogram cannot recover detail absent from the readings. Clock overhead is not subtracted.

Latency includes timestamp publication, enqueue, backlog, the stamp load and clock overhead. It excludes producer credit waiting, batch setup and the pause before the timestamp, and the consumer's histogram update, slot release and Spine telemetry writes for that message. Those operations still delay the next receive. These are post-pause send-to-receive latencies, not scheduled-arrival response times. After each drain, which ends with an empty read, the consumer executes one `PAUSE` before retrying while messages remain, as `ConsumerBare::blocking_consume` does on `Empty`.

Each run's `# latency` line gives the achieved rate, producer credit-wait iterations and `overlapping_sends` (sends stamped before the previous receive timestamp: an overlap diagnostic, not queue occupancy). Equal pause sequences do not guarantee equal arrival rates, and zero overlap does not prove that recording had no effect. To choose a pause floor, compare repeated runs at several fixed pause counts (equal pause bounds), then use a common range with low mean latency, low SD and little overlap. No finite pause guarantees zero backlog.

## Output

Lines beginning with `#` hold settings and diagnostics. Other lines are CSV, with columns named in the header:

- throughput `run` and `summary` (mean and sample SD across runs; `NaN` SD for one run);
- latency `run` and `summary`: mean and population SD, then p10, p50, p90, p95, p99, p99.5 and p99.9, in nanoseconds. Summaries pool every run's messages into one histogram;
- `verified`, in verify mode.

Latency mean and SD are histogram estimates of variation among messages, not of run means. Messages within a run are correlated. Nothing is trimmed. Results describe small callbacks and a reused source pool, not every application.
