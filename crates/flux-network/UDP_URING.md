# UDP io_uring

An opt-in Linux backend for `NetworkDriver`. TCP and the default UDP syscall
backend retain their existing behavior.

```rust
use flux_network::{NetworkDriver, Transport, UdpConfig};
use flux_network::udp::{UdpIo, UringConfig};

let mut network = NetworkDriver::default()
    .with_transport(Transport::Udp(UdpConfig {
        io: UdpIo::Uring(UringConfig::default()),
        ..UdpConfig::lan()
    }))
    .with_socket_buf_size(16 * 1024 * 1024);
// Use connect/listen_at, write_or_enqueue_with and poll_with as usual.
```

Both peers may choose their backend independently. There is no wire-format
change. Selecting io_uring is explicit: socket creation returns `None` and logs
the setup error if the kernel denies or lacks the required facilities. Linux
6.0+ is required for multishot recvmsg and synchronous cancellation; kernel
security policy may still disable them.

## Implementation and costs

- One ring per UDP socket, with one multishot receive and a provided-buffer ring.
  GRO entries use the existing datagram validation and reassembly path.
- Ordinary io_uring `SendMsg` operations carry both data and control packets.
  Compatible runs within a batch retain GSO, including two-fragment messages
  and batches crossing peer boundaries. GSO failure retries the original
  datagrams without segmentation.
- Each pending send owns a pooled copy of its bytes. Admission to this bounded
  queue counts as acceptance by the local socket backend. Completion errors
  become packet loss, handled by the existing reliability protocol. Neither
  ACK processing nor reconnect can invalidate kernel-owned bytes.
- Completion passes are bounded. ACKs are emitted between receive passes so
  callback processing cannot defer them behind a whole send window. Buffer
  exhaustion ends/rearms the multishot request after buffers are recycled.
- Normal polling uses no waiting for completions and skips idle kernel entries
  using the task-work flag. There is no async executor, SQPOLL thread, or
  zero-copy send machinery.
- Moving an active driver to another thread synchronously retires outstanding
  requests before rearming them on the new issuer. Drop also synchronously
  cancels requests before releasing buffers and the socket. Bind on the worker
  that will drive I/O to avoid handoff work. If kernel cancellation fails,
  borrowed allocations are retained rather than freed underneath the kernel.

Default limits are 64 send slots and 32 receive buffers: approximately 6 MiB
per socket, plus ring metadata and the existing protocol state. A GSO group
uses one send slot. Counts are configurable; receive count must be a power of
two, both counts must be nonzero, and their sum must not exceed 4096. Ring
capacity is separate from the protocol's send/receive windows and kernel
socket-buffer limits. UDP still has no receiver flow control.

The workspace already depended on `io-uring`; `flux-network` now uses the same
version as a Linux-only dependency. The new public `UdpConfig::io` field means
exhaustive struct literals need an update; literals using `..Default::default()`
or `..UdpConfig::lan()` keep compiling. Account for this source compatibility
change when preparing the next release. This branch does not bump versions.

## Measurements

Measured on 2026-09-08 against main
`e59fe83be3d31f29b5a149d736bc0c5cb399cbea`, which already includes UDP GSO/GRO.

AMD Ryzen 9 9950X, Linux `7.1.5-arch1-2`, Rust `1.91.0`, release optimization,
`target-cpu=native`. IPv4 loopback; sender on CPU 30 and receiver on CPU 31
(different physical cores). Eight broadcast receivers share the receiver
thread. Socket buffers request 16 MiB; this host's send/receive sysctl maxima
are 4 MiB (Linux reports doubled effective socket accounting limits).

Five measured rounds followed one discarded run of each binary. Main and
branch alternate order; UDP and io_uring also alternate order within the
branch binary. No builds or tests ran concurrently with the measured rounds.
The isolated main build changes only the benchmark harness, using the saved
[benchmark patch](benches/results/udp-uring/baseline-benchmark.patch); its
networking implementation is unchanged.

These are medians of each run's reported statistic, not pooled percentiles.
`FLUX_BENCH_SCALE=16` gives 65,536 / 16,384 / 512 deliveries for 2 KiB / 64 KiB /
2 MiB burst and broadcast cases. Paced cases have 2,000 messages and a 100 µs
minimum send interval; the large case cannot sustain that interval. The same
outstanding-message bounds apply to both backends. There is no per-scenario
warmup: initial message costs remain in the measurements. Socket setup and
handshake are outside the timed interval.

Latency is from the transport's wire timestamp to the receive callback; it
excludes serialization before that timestamp. Throughput includes the whole
send/receive interval. CPU ns/B sums sender and receiver thread CPU time,
including their busy polling; it is not an idle-efficiency measurement.

| Workload | Main MiB/s | io_uring MiB/s | Change | Main p50 / p99 µs | io_uring p50 / p99 µs | Main / io_uring CPU ns/B |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| paced/2k | 20 | 20 | +0.0% | 4.7 / 5.7 | 2.4 / 3.4 | 95.436 / 96.388 |
| paced/64k | 625 | 625 | +0.0% | 8.9 / 356.5 | 9.5 / 36.8 | 2.959 / 2.955 |
| paced/2m | 7,446 | 7,397 | -0.7% | 229.2 / 283.5 | 231.8 / 240.6 | 0.193 / 0.201 |
| burst/2k | 442 | 772 | +74.7% | 4.8 / 5.8 | 2.4 / 3.3 | 2.420 / 1.393 |
| burst/64k | 6,891 | 7,108 | +3.1% | 9.0 / 12.2 | 9.5 / 12.6 | 0.201 / 0.191 |
| burst/2m | 7,589 | 7,329 | -3.4% | 219.2 / 259.1 | 230.7 / 242.1 | 0.187 / 0.201 |
| bcast/2k | 562 | 950 | +69.0% | 17.0 / 34.4 | 18.7 / 35.2 | 1.778 / 1.075 |
| bcast/64k | 1,402 | 7,499 | +434.9% | 207.7 / 345.3 | 44.1 / 69.4 | 0.765 / 0.181 |
| bcast/2m | 6,820 | 6,700 | -1.8% | 1,344.5 / 4,861.2 | 3,431.1 / 6,699.6 | 0.211 / 0.246 |

The strongest gains are 2 KiB bursts (+75% throughput, roughly half median
latency) and 64 KiB broadcasts (5.35x throughput, p99 345 → 69 µs). The latter
also benefits from retaining GSO across mixed-peer batches: these results
measure the complete backend implementation, not an isolated io_uring syscall
substitution. The syscall backend could independently adopt that grouping.

Large messages are a tradeoff: 2 MiB burst throughput is 3.4% lower, and 2 MiB
broadcast median latency increases from 1.34 ms to 3.43 ms while throughput
falls 1.8%. The extra send copy and different batching/scheduling costs remain.
This is why io_uring stays opt-in. Paced p99 varies substantially between runs;
consult the raw samples rather than treating a median as a guarantee.

The single-loss relay case completed in a median 6.21 ms on main and 5.26 ms
with io_uring. Every run observed 1,793 data datagrams, including 2 repeated
sequences. Each run sends only one message, so this is a recovery check, not a
useful p99 measurement. Relay CPU is excluded from the CPU column.

All raw runs are in [benches/results/udp-uring](benches/results/udp-uring).
`main-N.txt` is the isolated main build; `branch-N.txt` includes both backends
on this branch. The branch's syscall control preserves similar throughput
(e.g. 441 vs 442 MiB/s for 2 KiB bursts, 1,406 vs 1,402 for 64 KiB broadcasts).
These are local loopback measurements, not physical-NIC, WAN, or AWS results.
Non-Linux builds were not cross-compiled.

## Reproduce and validate

```sh
FLUX_BENCH_TRANSPORT=udp,uring FLUX_BENCH_SCALE=16 \
  cargo bench -p flux-network --bench udp_pipeline
FLUX_BENCH_TRANSPORT=udp,uring FLUX_BENCH_SCALE=16 FLUX_BENCH_REVERSE=1 \
  cargo bench -p flux-network --bench udp_pipeline
```

To reconstruct the main baseline, extract the commit above into a separate
directory with `git archive`, apply `baseline-benchmark.patch` there with
`patch -p1`, and run the same benchmark with `FLUX_BENCH_TRANSPORT=udp`.
The patch only adds filtering, sample scaling, and CPU-time instrumentation.
The branch also accepts `FLUX_BENCH_SIZE=2m` to restrict size-dependent cases.

Validation passed:

- `just fmt`
- `just clippy`
- `cargo test --workspace --all-features --locked`: 362 passed, 4 existing
  ignored documentation tests, no failures.

Both backends run the same UDP integration suite. Additional coverage checks
mixed-backend peers, DCache delivery, sustained large messages with slow debug
callbacks, owned send buffers, queue saturation, IPv4/IPv6, mixed GSO groups,
fallback, GRO validation, receive-pool exhaustion, 16-bit descriptor-tail
wraparound, and driver movement across threads. Tests require local socket
and io_uring permissions. Local runs used `RUSTC_WRAPPER=` and a temporary
`CARGO_TARGET_DIR` because the shared build cache was sandbox-restricted.
