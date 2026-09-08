# UDP send segmentation benchmark

Baseline: merged UDP PR #156, `ad01665`. Candidate: the `udp/sys.rs` change on
`gd/udp-large-payload-perf`. Measured 2026-09-08.

## Change

Use Linux `UDP_SEGMENT` for an already-staged batch of at least four datagrams
with one destination and equal sizes (the final datagram may be shorter).
The existing batch limits it to 32 segments and the aggregate to 65,507 bytes.
The header/payload iovecs are used directly; no coalescing allocation or copy.
There is no extra wait to accumulate a batch. Wire headers, packet sizes,
acknowledgements, recovery, and the public API are unchanged.

One successful `sendmsg` accepts the entire segmented batch. The caller still
gets a count of wire datagrams. Ineligible batches retain the existing
`sendmmsg` path and its partial-send accounting. Offload-rejection errors retry
the unchanged batch normally and disable further GSO attempts for that manager.
`WouldBlock` returns zero progress through the existing caller path.

The useful precedent in `networ-opts` (`ba1bec1`, based on `new-udp-transport`
`3b6aaad`) was scatter/gather GSO. Its cluster report found that plain syscall
batching did little, whereas GSO reduced kernel packet work. This change takes
that mechanism without its cross-message queues, batching timers, multicast,
GRO, or AF_XDP implementation.

Linux describes the mechanism in [udp(7)](https://man7.org/linux/man-pages/man7/udp.7.html):
segmentation is deferred until late in the kernel transmit path, or to hardware
when supported. The wire datagrams still obey the configured MTU budget.

## Method

Unmodified `udp_pipeline` benchmark, release profile, built separately for
baseline and candidate; binaries saved before rebuilding. Three runs each,
interleaved baseline/candidate, candidate/baseline, baseline/candidate. No
instrumentation during timing. Every run completed every scenario and its
expected delivery-count assertion.

- AMD Ryzen 9 9950X, Linux 7.1.5-arch1-2, local loopback.
- Benchmark sender pinned to CPU 30 and receiver to CPU 31, different physical
  cores (SMT siblings 14 and 15). Eight broadcast clients share the receiver
  thread, as in the existing benchmark.
- Same 1,200-byte UDP datagrams and default 16,384-datagram windows.
- Both versions request 16 MiB socket buffers; host rmem_max/wmem_max are
  4 MiB, so both are subject to the same kernel clamp. No sysctls changed.
- The `paced` case requests 100 us spacing, but 2 MiB cannot sustain that rate
  in either version; that case effectively measures sustained large messages.
- This is a shared workstation, not an isolated benchmark host or an AWS link.

Build each revision with:

```sh
cargo bench -p flux-network --bench udp_pipeline --no-run --message-format=json
```

Save the executable reported by Cargo for each revision, then run those saved
executables sequentially in the order above. Do not run competing benchmarks
or builds concurrently. The benchmark needs loopback networking permissions.

## Results

Medians of three per-run results. Latencies are microseconds; throughput is
MiB/s. Each cell is baseline → candidate.

| Scenario | p50 | p99 | Throughput |
|---|---:|---:|---:|
| `paced/udp/2k` | 4.9 → 4.8 | 496.1 → 496.0 | 20 → 20 |
| `paced/udp/64k` | 92.7 → 19.2 | 612.7 → 911.4 | 607 → 625 |
| `paced/udp/2m` | 2,900.2 → 527.4 | 3,087.8 → 573.9 | 652 → 3,511 |
| `burst/udp/2m` | 2,887.2 → 519.0 | 3,104.9 → 3,344.3 | 644 → 2,816 |
| `bcast/udp/2k` | 18.4 → 18.9 | 1,481.9 → 1,168.3 | 557 → 556 |
| `bcast/udp/64k` | 451.1 → 243.5 | 2,930.6 → 816.6 | 660 → 1,223 |
| `bcast/udp/2m` | 12,556.5 → 2,916.6 | 23,270.3 → 5,164.1 | 651 → 2,941 |

The sustained 2 MiB case improves p50 by 81.8% and throughput by 5.38x.
The 2 KiB path stays on ordinary sends. Paced 64 KiB p99 worsens, and the short
2 MiB burst p99 does not improve; this is not a claim of uniformly better
latency tails. TCP remains faster for a single large message on this host.

Per-run sustained 2 MiB evidence (2,000 messages per run):

```text
udp-perf-run-1-baseline: paced/udp/2m                 n=2000   p50=   2867.8µs p99=   2935.9µs max=   3270.7µs       661 MiB/s
udp-perf-run-2-gso: paced/udp/2m                 n=2000   p50=    525.3µs p99=    603.0µs max=   3227.9µs      3513 MiB/s
udp-perf-run-3-gso: paced/udp/2m                 n=2000   p50=    527.4µs p99=    573.9µs max=   3397.3µs      3511 MiB/s
udp-perf-run-4-baseline: paced/udp/2m                 n=2000   p50=   2944.9µs p99=   4594.9µs max=   5609.6µs       601 MiB/s
udp-perf-run-5-baseline: paced/udp/2m                 n=2000   p50=   2900.2µs p99=   3087.8µs max=   4172.1µs       652 MiB/s
udp-perf-run-6-gso: paced/udp/2m                 n=2000   p50=    534.0µs p99=    551.4µs max=   3561.4µs      3488 MiB/s
```

The one-dropped-packet relay scenario completed in all six runs. Its single
message and scheduling-sensitive retransmission counts are insufficient for a
recovery-performance claim. Use the loss integration tests for correctness.

## Verification and limits

`just fmt`, `just clippy`, and `cargo test -p flux-network --all-features --locked`.
Focused syscall tests cover IPv4/IPv6 payload equality and datagram boundaries,
a short final segment, mixed destinations/sizes, and fallback when checksum
configuration rejects segmentation. Existing integration tests cover loss,
restart, replay, backlog, broadcast, and dcache.

No physical-NIC or AWS measurement, no changed deployment configuration, and no
non-Linux cross-build. The non-Linux send implementation is unchanged and the
new code is Linux-only. Physical-path speedup depends on the NIC/kernel path;
these results establish the local improvement, not cross-region performance.
