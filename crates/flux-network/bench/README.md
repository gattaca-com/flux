# Four-node UDP/TCP benchmark

This benchmark compares the current Flux TCP and reliable-UDP publishers with one publisher on
`solana-node1-lat-fr` and independent subscribers on nodes 2–4. It does not use loopback results as
evidence and it does not change transport behavior.

Run from the repository root:

```bash
bash crates/flux-network/bench/run-four-node.sh
```

The default matrix uses 64, 1232, and 4096-byte application messages; 50k, 100k, 200k, and 300k
messages/s; explicit bursts of 1 and 32; and three repetitions. Each run has a 5-second warm-up,
20-second measurement, and 5-second drain. TCP and UDP order alternates between repetitions. At
three subscribers, the publisher must transmit three copies, so the 4096-byte high-rate cases are
intentional overload tests on a 10-Gbit/s NIC.

For a short shakedown before the full matrix:

```bash
BENCH_PAYLOADS=1232 BENCH_RATES="50000 100000" BENCH_BURSTS=1 \
  BENCH_REPETITIONS=1 BENCH_DURATION_SECS=10 \
  bash crates/flux-network/bench/run-four-node.sh
```

Results and a read-only host/network inventory go under `bench-results/<UTC timestamp>/`. Each log
contains a machine-readable `RESULT {json}` line. A run is valid only when all three subscribers
stay connected, the publisher meets its pacing schedule, and the kernel/NIC drop counters do not
show unrelated host pressure.

Generate a validation-oriented CSV after a run:

```bash
python3 crates/flux-network/bench/summarize.py bench-results/<UTC timestamp> > summary.csv
```

One-way latency is sampled rather than timestamping every message. Before connecting the transport,
each subscriber performs 64 NTP-style exchanges over a separate TCP control port and uses the
minimum-RTT sample to estimate clock offset. The result includes the minimum RTT and its half-RTT
uncertainty bound; do not compare latency differences smaller than that bound. Throughput and loss
measurements do not depend on synchronized clocks.

UDP results include subscriber-side transport counters reset at the measurement boundary. These
report `recvmmsg` batch occupancy, direct UDP messages, repair requests, repair deliveries, and
unavailable responses, in addition to application-visible delivery latency, final missing
sequences, ordering, and sampled ingest-to-delivery time.

All matrix settings can be overridden through the `BENCH_*` environment variables defined at the
top of the runner. `BENCH_CPU=2` pins each benchmark process to CPU 2 by default; the environment
capture is needed to check that this CPU is not also handling the busy NIC IRQ queue.
`BENCH_REALTIME_PRIORITY=1..99` runs the benchmark processes under temporary `SCHED_FIFO` via
passwordless `sudo`; zero keeps the normal scheduler. The outer timeout remains non-real-time and
terminates each process at the end of its case.
`BENCH_LATENCY_SAMPLE_EVERY` controls the sampling interval used for one-way latency; lower values
provide more p99 samples at low rates at the cost of more clock reads.
The default `BENCH_PUBLISHER_LOOP=spin-poll` continuously polls transport I/O while pacing and
between publications, matching a production network thread pinned to a core. By default this
preserves the legacy combined data/control poll on every iteration. Set
`BENCH_CONTROL_POLL_INTERVAL_US` to a nonzero interval to keep checking UDP batch deadlines and
receiving UDP continuously while only entering the TCP subscription/repair control plane at that
cadence. Both publisher and subscribers report their resulting poll-call counts. Set
`BENCH_DATA_POLL_INTERVAL_US` to pace empty subscriber `recvmmsg` probes while retaining the pinned
busy loop. This is diagnostic; it reduced system CPU time in the current tests but did not raise
the single-flow receive ceiling. `BENCH_END_MARKERS` repeats the final marker for raw-loss tests in
which repair is intentionally delayed.
`BENCH_PUBLISHER_LOOP=paced` to reproduce the older benchmark behavior, which sleeps or spins
without polling between send deadlines and only polls every `BENCH_IO_POLL_EVERY` publications.
`BENCH_TRANSPORTS="tcp"` or `"udp"` restricts a targeted sweep, and
`BENCH_TIMEOUT_SLACK_SECS` extends the safety timeout for deliberately overloaded cases.
Use `BENCH_UDP_CONFIGS="1:0 4:1"` to compare specific batch/GSO pairs without running the full
cross product.
For a final selected matrix, `BENCH_CASES` accepts whitespace-separated
`payload:rate:batch:gso` UDP entries or `transport:payload:rate:batch:gso` entries and bypasses the
broader matrix, for example `BENCH_CASES="udp:64:30000:4:1 tcp:64:10000:1:0"`.
Append `:repair-delay-ms` to a transport-prefixed entry for targeted UDP repair-delay sweeps, for
example `udp:4096:12000:4:1:5`. Further fields are `:gro`, `:subscribers`,
`:adaptive-batching`, `:gso-copy`, `:multicast`, and `:batch-delay-us`; for example
`udp:4096:12000:4:1:1:1:3:0:1` selects GRO, three subscribers, fixed batching, and contiguous GSO.
Appending `:1` to that entry enables one IPv4 multicast data stream while retaining the three
per-subscriber TCP control/repair connections. The group defaults to `239.255.42.42` and can be
changed with `BENCH_UDP_MULTICAST_GROUP`; each run uses its data port as the group port. The runner
passes the `10.9.0.x` address of each authorized node as the multicast interface.
Append another field to override the partial-batch deadline for one case, such as
`udp:1232:10000:4:1:1:1:3:0:1:1:100` for 100 us.
Append a thirteenth field to override the control-poll interval for one case; for example,
`udp:1232:10000:4:1:1:1:3:0:1:1:20:100` retains a 20 us data flush deadline while polling TCP
control every 100 us. Zero selects the legacy combined poll.
The corresponding `BENCH_*` defaults remain available for shorter case forms.

Set `BENCH_UDP_XDP=1` to use the experimental `AF_XDP` publisher. With
`BENCH_UDP_MULTICAST=1`, one frame is sent to the multicast group. With multicast disabled, the
runner obtains the physical MAC from each of the three authorized receiver hosts, binds each
receiver to the known run port, and configures one static unicast destination per receiver. The
runner binds the physical `BENCH_XDP_INTERFACE` (default `eno2`) and queue `BENCH_XDP_QUEUE`
(default 2), derives its interface index and source MAC on node 1, and emits VLAN
`BENCH_XDP_VLAN_ID` (default 2135). The publisher runs through passwordless `sudo` because the XSK
and pinned UMEM need elevated resource permissions on the lab image. Benchmark runs pass
`--udp-xdp-no-fallback`, so unsupported XSK setup fails explicitly instead of silently measuring
the kernel socket backend. The subscriber remains the normal UDP/GRO implementation, isolating the
sender path.
`BENCH_XDP_MODE=zero-copy` forces the native driver path; the default `copy` mode retains the
simpler SKB-backed path. A zero-copy request never silently degrades to copy mode. The zero-copy
runner attaches a minimal native-mode `XDP_PASS` program for the publisher lifetime and the kernel
detaches it automatically when the process closes its BPF link. It never replaces an existing XDP
program; startup fails if the interface already has one.
The runner refuses zero-copy on a kernel whose release contains `debug`: the lab's former
`6.12.0-211.37.1.el10_2.x86_64+debug` boot produced a KASAN slab-out-of-bounds in
`ixgbe_txrx_ring_disable` during XSK bind. `BENCH_ALLOW_DEBUG_XDP=1` exists only for deliberate
kernel debugging and must not be used for performance runs.

Native TX must use a queue excluded from ordinary RX RSS because a TX-only XSK does not populate
its fill ring. Set `BENCH_XDP_RSS_QUEUES` to the number of lower-numbered queues that should retain
RSS traffic and select a higher XSK queue. The validated lab layout uses 12 combined `eno2` queues,
`BENCH_XDP_QUEUE=7`, and `BENCH_XDP_RSS_QUEUES=6`. ixgbe resets its queue/RSS configuration during
native XDP attachment, so the runner creates the publisher first, waits 12 seconds for link
recovery, reapplies RSS with `ethtool -X`, and only then starts subscriber control connections. The
publisher allows 60 seconds for this setup. These queue-count and SMT changes are runtime host
settings unless separately provisioned and must be checked after a reboot.

`BENCH_SOCKET_BUFFER_BYTES` defaults to 64 MiB. The runner now requires both `net.core.rmem_max`
and `net.core.wmem_max` to be at least that large on every host and fails before deployment if the
kernel would clamp the request. These sysctls returned to 212,992 bytes after the lab reboot; a
60-second near-line-rate run then lost about 50-53 ppm at the socket receive buffer despite zero
XDP/NIC drops. Restoring 64 MiB removed every measured gap. The sysctl change is runtime-only unless
provisioned separately.

The final matched-kernel campaign is summarized in `udp-final-benchmark.md`. The native sender uses
a 32,768-entry ring and 65,536 UMEM frames. Three 60-second 1232-byte AF_XDP multicast repetitions
at 930k/s delivered all 55.8 million messages to every subscriber with zero XDP, socket, NIC, or
repair drops. Socket GSO/GRO was repeatedly clean through 920k/s; at 930k/s two repetitions were
clean and one subscriber missed 5,961 of 55.8M, making 930k a socket edge rather than a confirmed
clean point. A 950k/s overload crosses the physical link boundary.

At the balanced 850k/s point, the final socket GSO/GRO run produced worst-receiver median
p50/p99/p99.9 of 56/115/140 us, versus 169/227/242 us for AF_XDP. Socket UDP therefore has the
better latency below saturation; AF_XDP buys only the final roughly 1% of confirmed multicast
throughput. At 4096 bytes both paths are clean at 285k/s and overload at 290k/s, so the link is the
shared boundary.

This supersedes the old 65.1k/s socket ceiling, which was measured on a `+debug` kernel with only
six logical CPUs online. The old GSO call took roughly 64-75 us; the current batch-5 call takes
about 3.0 us. Targeted final publisher profiles show approximately 3.15 us per five-message socket
GSO submission versus 0.68 us per one-message AF_XDP zero-copy submission. Amortized per message
these are approximately 0.63 and 0.68 us. Because the profiler loses events near line rate, use it
to explain the path and use the uninstrumented counters for headline throughput.

The GRO-off control is in `bench-results/socket-multicast-current-gro-off1`. All three receivers
handled 850k individual socket datagrams/s without loss. At 930k/s the worst subscriber missed 559
of 18.6M packets (30 ppm), confirming that GRO adds headroom but is not the source of an artificial
order-of-magnitude result.

The final three-stream AF_XDP unicast confirmation is in
`bench-results/final-regular-1232-xdp-zero-copy-unicast-confirmation1`. At 1232 bytes, both socket
GSO/GRO and AF_XDP are clean at 310k logical messages/s (930k transmitted packets/s) and overload
at 320k. Socket UDP has lower latency at the matched 280k and 310k points. Multicast's primary
advantage remains three times the logical fanout throughput because the switch replicates one
transmitted stream.

For 64-byte messages, sender-only AF_XDP is receiver-limited and is not a production win. Socket
GSO/GRO at 1.5M unicast messages/s coalesces about 32 datagrams into each receive buffer and is
repeatedly clean. AF_XDP unicast at 900k/s records no GRO aggregation and loses bursts despite zero
XDP ring/frame drops. The next meaningful AF_XDP small-packet experiment needs an AF_XDP receiver,
receive sharding, or equivalent segmentation/coalescing support.

For repeatable CPU-bound runs, `BENCH_STABILIZE_IRQS=1` temporarily stops `irqbalance`, moves the
physical `eno2` MSI-X interrupts away from `BENCH_CPU` onto `BENCH_IRQ_CPUS` (CPUs 4 and 5 by
default), and restores every original affinity and the prior service state when the runner exits.
This requires passwordless `sudo` on the four lab nodes and makes no persistent configuration
change. Per-run scheduler and CPU-core deltas remain in each `RESULT` for contamination checks.

## Targeted Flux Profiler capture

Keep the repeatable throughput matrix uninstrumented. For a targeted publisher-side diagnosis,
build `transport_bench` with `--features profiling`, deploy it as
`/home/rocky/flux-transport-bench-profile` on all four nodes, and deploy the `flux-profiler` binary
as `/home/rocky/flux-profiler` on node 1. Then run, for example:

```bash
bash crates/flux-network/bench/profile-four-node.sh udp 4096 6000 24010 udp4096-sat
```

The arguments are transport, payload bytes, offered messages/s, data port, and case name. The
script runs the same one-publisher/three-subscriber topology, prints inclusive per-frame timing
quantiles, and downloads an FXT trace to `bench-results/profiles/<case-name>/trace.fxt` for Perfetto
or Magic Trace. Profiling results are diagnostic and should not replace the uninstrumented
throughput result.

Set `BENCH_PROFILE_TARGET=subscriber-1` to profile node2 instead (subscriber 2 maps to node3 and 3
to node4). Subscriber profiling is intentionally targeted because per-datagram frames can become
intrusive near the receive-side ceiling; use the uninstrumented transport counters for throughput.

## Multicast

`run-next-multicast.sh` runs a stdlib-only IPv4 multicast connectivity probe on the same four
authorized hosts. It is deliberately not a transport throughput benchmark: it verifies that all
three receivers can join the group and receive the same sequenced datagram stream without loss.
The final multicast bit in `BENCH_CASES` runs the Flux reliable-UDP data plane through the normal
realistic benchmark, including its pinned spin loops, GSO/GRO, clock calibration, loss accounting,
and TCP repair path.
