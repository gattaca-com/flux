# AF_XDP UDP Publisher Plan and Status

## Objective

Provide an optional Linux AF_XDP transmit backend for the existing reliable UDP publisher while
preserving the wire format, multicast fanout, per-subscriber TCP control/repair plane, and kernel
UDP fallback. Agave and Firedancer are implementation references only; no code or runtime
dependency is taken from either project.

The first production target is one pinned publisher thread, one dedicated NIC TX queue, IPv4
multicast, and bounded non-blocking backpressure. Kernel UDP remains the default.

## Implementation status

The direct-UAPI sender is implemented. It:

- creates UMEM, TX, completion, and empty fill rings;
- supports forced copy and native zero-copy modes with `XDP_USE_NEED_WAKEUP`;
- optionally owns a temporary driver-mode `XDP_PASS` link required by ixgbe zero-copy;
- builds Ethernet, optional 802.1Q, IPv4, UDP, and Flux fragments directly in UMEM;
- reclaims completed frames and reports enqueue, completion, wakeup, ring, and frame-pool counters;
- drops rather than blocking when bounded resources are exhausted, leaving recovery to the
  existing TCP repair plane; and
- can fail startup or fall back to the kernel socket backend according to configuration.

AF_XDP is created as part of `UdpPublisher` construction. Deferring setup until after subscriber
connections was tested and caused a zero-copy TX stall on the lab ixgbe driver.

Three benchmark/setup defects were found and fixed:

1. The TX-only XSK queue was also receiving ordinary RSS traffic despite having no RX fill
   buffers. Zero-copy now uses queue 7 while normal RSS is restricted to queues 0-5.
2. ixgbe rebuilds queues and resets RSS while native XDP is attached. The benchmark waits for the
   link to settle, then reapplies RSS isolation before starting subscribers.
3. The userspace TX-ring availability cache was only refreshed after it appeared completely full.
   A batch crossing the 32,768-entry wrap could therefore drop a valid tail. It now refreshes the
   kernel consumer whenever cached capacity is smaller than the requested batch.

Copy mode is functional but is not a performance win. Its historical 60-62k 1232-byte result and
the former 65.1k socket comparison were both obtained around the debug-kernel investigation and do
not describe the current regular-kernel socket ceiling. Native zero-copy remains useful for direct
queue ownership and small-packet work, but matched socket GSO reaches the 1232-byte link boundary
too.

## Realistic benchmark contract

Use node 1 as publisher and nodes 2-4 as independent subscribers. All data travels over the bare
metal 10-Gbit/s VLAN; loopback, veth, or same-host measurements are not performance evidence.

- Pin each publisher/subscriber process to an application core and use the production-style
  spin-poll loop.
- Keep NIC IRQs off the application core and record CPU, scheduler, softirq, kernel, and NIC
  counters for every run.
- For native TX, use a hardware queue excluded from normal RX RSS. Apply the RSS mapping only
  after XDP attachment and link recovery.
- Warm up first, then measure fixed-duration intervals at fixed offered rates. Record both offered
  and achieved publisher rates.
- Require all three subscribers to remain connected. Report successful delivery at the slowest
  subscriber, not just sender enqueue rate.
- Treat repair traffic, application gaps, XDP ring/frame drops, kernel/NIC drops, publisher pacing
  misses, or persistent tail-latency growth as saturation signals.
- Test 64, 1232, 1400, and 4096-byte logical messages. Include low-rate latency points, a ramp to
  first loss, and repeated 10-second confirmation runs near the selected ceiling.
- Report p50, p99, p99.9, and maximum one-way application latency, plus clock-calibration
  uncertainty. Software clock calibration is suitable for broad comparisons only; use PTP/NIC
  hardware timestamps before claiming small absolute latency differences.
- Keep queues bounded. Do not let either sender hide overload in an unbounded userspace backlog.

The lab zero-copy configuration uses 12 combined ixgbe queues, XSK queue 7, RSS queues 0-5, a
32,768-entry TX/completion ring, 65,536 UMEM frames, and subscriber UDP GRO. SMT and the 12-queue
configuration are runtime host settings and must be reapplied after reboot unless provisioned
persistently.

## Results so far

The canonical matched-kernel result is in `udp-final-benchmark.md`; it supersedes the earlier short
curves in this document. All final cases use three remote subscribers, pinned spin loops, the
regular untainted kernel, the isolated XSK queue, and application-level sequence accounting.

| Logical message | AF_XDP result | Matched socket result | Conclusion |
|---|---|---|---|
| 1232 B multicast | 930k/s clean in 3x60 s; 153/199/2,255 us p50/p99/p99.9 | 920k/s clean; 850k balanced at 56/115/140 us | XDP buys about 1% ceiling; socket has better latency |
| 1232 B 3-stream unicast | 310k/s clean; 320k overload | Same 310k/s boundary, lower latency | Aggregate link bandwidth dominates |
| 4096 B multicast | 285k/s clean; 290k overload | Same boundary; socket has the better balanced tail at 280k | Both saturate 10 GbE |
| 4096 B 3-stream unicast | 90-94k/s clean; 96k overload | Same boundary | Both saturate 10 GbE |
| 64 B 3-stream unicast | Loses bursts at 900k/s despite zero XDP drops | GSO/GRO is repeatedly clean at 1.5M/s | Sender-only XDP loses receive coalescing |
| 64 B multicast | 2.2-3.0M/s has small long-run gaps | Socket GSO/GRO has lower latency and fewer gaps | Longer tests expose rare receive/egress stalls |

The decisive 64-byte counter is GRO. Socket GSO/GRO at 1.5M unicast messages/s delivers 45M
datagrams per receiver through about 1.41M socket messages, roughly 32 datagrams per receive
buffer. AF_XDP unicast at 900k/s delivers about 27M datagrams as 27M individual socket messages and
records zero GRO aggregation. An AF_XDP receiver, receive sharding, or AF_XDP-side segmentation is
needed before the raw tiny-packet sender ceiling is useful end to end.

Forced-copy AF_XDP is functional on the regular kernel and can sustain high throughput, but at
1232 bytes its p99 is about 2.5 ms at 860k/s and 4.6 ms at 920k/s. It is not a latency candidate.

Final publisher profiles explain the close throughput result. A socket batch-5 GSO `sendmmsg`
takes about 3.15 us, or 0.63 us amortized per message. AF_XDP `xdp.send_batch` takes about 0.68 us
per single message, of which about 0.52 us is the TX kick. AF_XDP makes one submission cheaper, but
GSO amortizes the socket submission to essentially the same per-message cost.

The 64 MiB `rmem`/`wmem` limits remain mandatory. The runtime queue, SMT, RSS, and buffer settings
also remain non-persistent unless provisioned at boot.

## Remaining work

1. Add an AF_XDP subscriber, or shard receive across multiple RSS queues/socket workers, so the
   64-byte sender can be tested to the physical line-rate boundary.
2. Add bounded-overload, repair, reconnect, and clean-shutdown burn tests around the selected
   socket 850k/s operating point and the AF_XDP 930k/s throughput point. Investigate the one
   AF_XDP 4096-byte publisher teardown timeout observed after complete subscriber delivery.
3. Add a privileged namespace/veth integration test for copy mode and hardware-gated zero-copy
   setup tests. Exercise ring wrap, completion recycling, VLAN frames, fragmentation, checksum,
   fallback, and forced-failure behavior.
4. Add PTP/NIC hardware timestamps and isolated production cores before optimizing or claiming
   smaller latency differences.
5. Make queue count, RSS isolation, IRQ affinity, memlock, and XDP lifecycle explicit deployment
   checks. Keep zero-copy opt-in until those checks and recovery behavior are automated.
6. Only then consider dynamic route/neighbor discovery for the existing static unicast fanout,
   IPv6, multiple TX queues, and making AF_XDP a non-experimental backend.
