# UDP batching and GSO benchmark results

## Conclusion

### Matched regular-kernel correction

The former 65.1k/s socket-multicast ceiling is not a valid comparison against the later AF_XDP
results. It came from the Rocky `+debug` kernel with SMT disabled and only six logical CPUs online.
In that environment a batch-5 GSO `sendmmsg` took roughly 64-75 us. After moving all four machines
to the regular kernel, enabling all 12 logical CPUs, restoring 64 MiB socket limits, and applying
the same IRQ/RSS stabilization used by the XDP runs, an equivalent socket call takes about 3.0 us.

The final repeated socket-multicast results are:

| 1232-byte rate | Delivery to each of 3 subscribers | Worst p50 / p99 / p99.9 | Worst maximum |
|---:|---:|---:|---:|
| 850k/s | 51.0M/51.0M in all 3 runs; zero loss | median 56 / 115 / 140 us | 3,338 us |
| 920k/s | 55.2M/55.2M in all 3 runs; zero loss | median 88 / 185 / 1,503 us | 3,350 us |
| 930k/s | two runs clean; one receiver missed 5,961/55.8M | median 107 / 184 / 2,159 us | 5,040 us |
| 950k/s, 20-second overload | about 99.24%; link saturated | about 24.4 / 24.7 / 24.7 ms | 24.8 ms |

The physical link becomes the boundary between 920k and 950k/s. Use 850k/s as the balanced socket
point, 920k/s as its repeatedly clean maximum, and 930k/s only as an edge. AF_XDP zero-copy was
clean in all three 930k/s repetitions, but socket UDP had lower latency below saturation.

Targeted Flux-profiler captures corroborate the raw counters. At a full batch, socket
`udp.sendmmsg` averaged about 3.15 us in the final sampled trace, while zero-copy AF_XDP
`xdp.send_batch` averaged about 0.68 us per single message. Amortized over five GSO segments, the
socket submission cost is about 0.63 us per message and is competitive with AF_XDP for this packet
size. The profiler lost events at these rates, so these timings are diagnostic; the uninstrumented
delivery and kernel/NIC counters remain authoritative.

Subscriber GRO is helpful but is not concealing a low receive ceiling. A 20-second GRO-off A/B
delivered 850k/s losslessly to every subscriber. At 930k/s one subscriber was lossless and the
other two missed only 19 and 559 of 18.6M packets, with zero socket-buffer and NIC drop counters.
The much lower three-stream unicast logical rate is instead expected link arithmetic: node 1 emits
three copies, so roughly 930k wire packets/s corresponds to only 310k logical messages/s. With
multicast it emits one copy and the switch replicates it, allowing about 930k logical messages/s.

The full final matrix, including TCP, unicast, multicast, GSO/GRO controls, AF_XDP copy and
zero-copy, 64/1232/4096-byte messages, and long-run loss behavior is in
`udp-final-benchmark.md`. Sections below retain the earlier debug-kernel measurements as
investigation history. Any claim that 65.1k/s is the current socket ceiling or that AF_XDP is
14.3x faster is superseded by the final matched rerun.

The latest fanout, GSO-buffer, adaptive-batching, and multicast experiments materially refine the
earlier conclusion below:

- With one subscriber, scatter-gather GSO reaches median ceilings of 90.3k/86.9k/28.8k messages/s
  at 64/1232/4096 bytes. With three subscribers those fall to 31.9k/31.0k/14.5k. At the two small
  sizes, aggregate fanout work rises by only 6-7%, showing that publisher cost is nearly linear in
  subscriber count.
- Removing the two userspace GSO payload copies is not automatically faster. Contiguous and
  scatter-gather are tied at 64/1232 bytes, while contiguous GSO is 9.9% faster for 4096-byte
  messages with three subscribers (16.0k versus 14.5k/s). A targeted profile attributes this to
  `sendmmsg`, not encoding: its p50 fell from 262.4 to 235.8 us while the extra contiguous encoding
  and coalescing work was only about 0.6 us per flush.
- Adaptive batching is useful for sparse fragmented traffic: at 4096 bytes and 3k/s it reduced
  median worst-subscriber p50/p99 from 336/737 to 287/704 us. It did not consistently improve the
  tail at the small sizes or at high load, so fixed batch-4/50-us remains the predictable default.
- The IPv4 multicast data plane is now implemented. It sends one UDP stream while retaining one
  TCP control/repair connection per subscriber. Its exploratory plateaus are 55.4k/54.8k/22.8k
  messages/s, 1.73x/1.77x/1.42x the three-subscriber unicast ceilings. At matched 4096-byte load,
  publisher NIC bytes fell from 1.208 GB to 410 MB, confirming that the switch—not the publisher—
  performs the three-way replication.
- A new same-build, interleaved latency campaign favors multicast over unicast at all three sizes.
  Median worst-subscriber p50/p99 was 135/1,020, 140/984, and 237/825 us for multicast versus
  218/1,337, 231/1,387, and 337/2,182 us for unicast at 64/1232/4096 bytes. TCP still had the best
  median p99 at 64 and 4096 bytes, while multicast led at 1232 bytes and had a lower p50 at the two
  small sizes.
- Multicast does not free the publisher core on these hosts. The application consumes about 85-92%
  and the core reports about 8-12% softirq time, for roughly 99% combined utilization. Per-class
  softirq invocation counts are dominated by TIMER/RCU rather than NET_TX, so attributing all of
  that time specifically to the network TX softirq requires kernel profiling. The cost nevertheless
  remains after NIC IRQ isolation and helps explain why one stream yields a 1.4-1.8x throughput
  gain rather than 3x.

The next sender optimization is therefore AF_XDP copy mode against both socket multicast and the
unicast fallback. Multicast removes userspace fanout duplication; AF_XDP now has a concrete target
in the remaining sender-side softirq and kernel packet-processing cost.

The production-style result now supports the intended pinned spin loop. `UdpPublisher::poll_with()`
services control and repair traffic continuously but flushes a partial publication batch only when
its 50-us deadline expires; explicit `flush_with()` remains the unconditional phase/handoff
boundary. Batch-4 `UDP_SEGMENT` again sustains 30k messages/s for 64- and 1232-byte messages and
12k messages/s for 4096-byte messages.

Opt-in subscriber `UDP_GRO` then cuts receive socket messages by about 4x for high-rate small
messages and 5.1x for 4096-byte messages. At the selected 30k/30k/12k operating points, median
worst-subscriber repair deliveries fell from 233/227/264 without GRO to 205/0/0 with GRO. Every
confirmation run had zero final application loss, although individual 64-byte and 4096-byte GRO
runs still needed as many as 255 and 134 TCP repairs respectively.

The repeated production-loop saturation ceilings with GRO are 32.04k, 31.05k, and 16.03k
messages/s. The selected lower rates preserve materially better latency and repair behavior, so
the ceiling figures should be treated as headroom rather than recommended operating points.

Plain cross-publication `sendmmsg` batching was not worthwhile on its own. It added an application
copy and reduced syscall count, but its saturated throughput was effectively unchanged. GSO is
where the gain comes from because it also reduces per-datagram kernel work.

## Implementation under test

The UDP transport now has seven opt-in settings:

- `send_batch_size` (default 1)
- `send_batch_max_delay` (default zero; the benchmark used 50 us with batching)
- `use_udp_segment` (default false)
- `use_udp_gro` (default false, subscriber only)
- `copy_udp_segment_payloads` (default false; contiguous GSO tuning fallback)
- `send_batch_mode=Adaptive` (default is fixed)
- `multicast=Some(UdpMulticastConfig)` (default is unicast)

The existing immediate path remains the default and retains its history-backed encoding path.
The batched path queues logical sequence numbers, flushes on batch size or cooperative deadline,
and can coalesce equal-sized wire datagrams with Linux `UDP_SEGMENT`. GSO groups are bounded by
the kernel segment-count and UDP payload limits.

Publisher counters distinguish logical publications, flush reasons, batch dwell, `sendmmsg` calls,
send entries, and wire datagrams. Subscriber counters distinguish socket receive messages, logical
datagrams, GRO packets/segments, `recvmmsg` occupancy, direct UDP delivery, repair requests, repair
delivery, and unavailable responses.

In multicast mode, normal publications go once to the configured IPv4 group and port. Subscription,
progress, reconnect, and repair remain on the existing per-subscriber TCP connections. The
publisher still waits for the configured active subscriber count before measuring, and subscribers
still filter UDP by the publisher's unicast source address and port.

## Test topology and controls

- Publisher: `solana-node1-lat-fr`
- Subscribers: `solana-node2-lat-fr`, `solana-node3-lat-fr`, and `solana-node4-lat-fr`
- Cross-host unicast over the private 10-Gbit/s VLAN; no loopback throughput evidence
- One publisher and three independent subscriber processes, each pinned to CPU 2
- 64 MiB socket-buffer request, MTU 1500, one logical publication per pacing burst
- Final confirmation: 5 s warm-up, 20 s measurement, 5 s drain, three repetitions
- Repetition 2 reversed case order to reduce thermal/order bias

An early rate curve was invalidated because `irqbalance` moved ixgbe work onto the benchmark CPU,
leaving the publisher only 53-71% of a core. The final harness temporarily stops `irqbalance`,
moves only `eno2` MSI-X interrupts to CPUs 4/5, records scheduler and per-core accounting, and
restores every original affinity and the service state on exit. The restoration path was verified
on all four nodes. Final high-rate publishers used about 97-99% of CPU 2, with no CPU steal and no
kernel/NIC drop counters.

## Final repeated results

The table reports medians across the three 20-second repetitions. “Repairs” is the median of the
worst subscriber's exact repair-delivery count. Every row had zero final application loss.

| Payload | Configuration | Offered/delivered msg/s | p50 | p99 | Repairs |
|---:|---|---:|---:|---:|---:|
| 64 B | current UDP | 10,000 | 150 us | 288 us | 0 |
| 64 B | GSO batch 4, same load | 10,000 | 284 us | 533 us | 0 |
| 64 B | GSO batch 4, high load | 30,000 | 258 us | 1,782 us | 219 / 600,000 (0.0365%) |
| 1232 B | current UDP | 9,869 | 158 us | 764 us | 0 |
| 1232 B | GSO batch 4, same load | 10,000 | 279 us | 781 us | 0 |
| 1232 B | GSO batch 4, high load | 30,000 | 267 us | 1,759 us | 147 / 600,000 (0.0245%) |
| 4096 B | current UDP | 3,000 | 359 us | 1,031 us | 0 |
| 4096 B | GSO batch 4, same load | 3,000 | 663 us | 1,168 us | 0 |
| 4096 B | GSO batch 4, high load | 12,000 | 550 us | 1,883 us | 288 / 240,000 (0.12%) |

At the same offered load, batching adds approximately 121-304 us to median latency because a
partial batch waits for a later publication or the 50-us cooperative deadline; scheduling and
one-way clock uncertainty also contribute. A production configuration should expose the batch
size and maximum delay rather than hard-code the benchmark values.

The high-rate receive batches averaged about four datagrams per nonempty `recvmmsg` call for the
small messages and about six for 4096-byte messages. A 4096-byte logical message produces three
Flux wire datagrams at the configured 1400-byte maximum datagram size.

## IPv4 multicast data plane

The socket multicast implementation was tested through the same Flux publisher/subscriber and
benchmark path—not the earlier Python viability probe. Each subscriber joins the group on its
`10.9.0.x` interface before establishing its normal TCP subscription. The publisher sends normal
UDP data once to the group; TCP progress and repair remain independent per subscriber.

The latest low-rate benchmark used one connected multicast UDP socket, three 12-second repetitions,
3-second warm-up/drain periods, reversed case order on repetition two, pinned spin loops, NIC IRQ
isolation, contiguous GSO, subscriber GRO, and all three remote subscribers. Latency was sampled on
one in 25 publications. Each run-level percentile below is the maximum across subscribers; the
table reports the median across the three repetitions.

| Payload/rate | TCP p50 / p99 | UDP unicast p50 / p99 | UDP multicast p50 / p99 | Multicast mode |
|---:|---:|---:|---:|---|
| 64 B at 10k/s | 233 / 803 us | 218 / 1,337 us | **135 / 1,020 us** | batch 1, zero dwell |
| 1232 B at 10k/s | 245 / 997 us | 231 / 1,387 us | **140 / 984 us** | batch 1, zero dwell |
| 4096 B at 3k/s | **237 / 609 us** | 337 / 2,182 us | 237 / 825 us | batch 4, 10-us deadline |

Every row in that table completed all three repetitions with zero application loss, no repair
delivery, and no kernel/NIC drop. The worst repetition p99s for TCP/unicast/multicast were
860/2,183/1,063 us at 64 bytes, 1,012/1,917/1,100 us at 1232 bytes, and
1,722/2,745/1,250 us at 4096 bytes. Thus multicast is both faster and more stable than the unicast
socket path in this campaign, but does not uniformly beat TCP tail latency.

Batch-1 GSO/GRO is the best small-message latency mode: it keeps cross-publication dwell near zero
while retaining the connected multicast route and receiver GRO. At 4096 bytes, batch 4 with a 10-us
deadline is the clean tail-oriented mode. Batch-1 multicast had a slightly lower median p50 there
but one of three repetitions required eight TCP repair deliveries, so it is excluded from the
headline comparison.

The measured one-way clock uncertainty remains about 115-123 us, so the exact small p50 deltas are
not HFT-grade measurements even though their direction repeated consistently. P99.9 was also noisy
(roughly 1.6-23 ms depending on transport and repetition). Retained worst-sequence diagnostics show
both receiver-local stalls and occasional stalls common to all three subscribers. Hardware PTP/NIC
timestamps and boot-time CPU isolation are required before making sub-100-us or p99.9 production
claims.

A temporary `SCHED_FIFO` experiment is intentionally excluded: the hosts have
`kernel.sched_rt_runtime_us=950000`, so continuously spinning RT threads were throttled for part of
each one-second period and produced 13-50-ms stalls. Pinned real-time threads only become a valid
low-latency setup after RT throttling and CPU isolation are configured coherently.

The selected multicast operating points retain margin below the sender plateau:

| Payload | Delivered | Worst-subscriber p50 / p99 | Median worst-subscriber repairs |
|---:|---:|---:|---:|
| 64 B | 45.00k/s | 323 / 2,031 us | 1,058 / 900,000 (0.118%) |
| 1232 B | 45.00k/s | 318 / 1,682 us | 0 / 900,000 |
| 4096 B | 16.00k/s | 488 / 2,589 us | 515 / 320,000 (0.161%) |

The 64- and 4096-byte repair spikes are variable and localized rather than final loss, but they
make these throughput-oriented points unsuitable as latency targets. The lower-rate rows above had
zero median GSO repair deliveries and substantially tighter tails.

An exploratory overload sweep found the following publisher plateaus. The multicast values are
medians across the overloaded offered-rate points, not the final three-repetition operating rates.

| Payload | Three-subscriber unicast | Socket multicast | Multicast / unicast | Prior TCP ceiling |
|---:|---:|---:|---:|---:|
| 64 B | 32.04k/s | 55.38k/s | 1.73x | 13.84k/s |
| 1232 B | 31.05k/s | 54.81k/s | 1.77x | 10.89k/s |
| 4096 B | 16.03k/s | 22.82k/s | 1.42x | 11.24k/s |

At 4096 bytes and 12k/s, publisher NIC transmit bytes fell from 1.208 GB for unicast to 410 MB for
multicast while all three subscribers received the complete stream. The remaining scaling gap is
CPU: multicast runs spend about 11-15% of the pinned publisher core in transmit softirq and about
85-88% in the process itself. Combined utilization is approximately 99% even at low offered rates
because the benchmark intentionally spin-polls.

GSO remains essential. Without GSO, multicast plateaued at about 16.5k, 15.4k, and 5.05k/s for the
three payloads; with GSO it reached about 55k, 55k, and 22.8k/s. Batch 3 missed the 45k small-message
target, and batch 1/2 reached only 7.3k/14.2k at 4096 bytes versus 16k for batch 4. The existing
batch-4/50-us setting therefore remains the throughput default, with immediate mode as the
latency-oriented option.

## IRQ-controlled TCP comparison

TCP was rerun after the UDP confirmation with the identical four hosts, CPU/IRQ isolation,
warm-up, measurement, drain, repetition count, and alternating order. The overload cases give the
single-publisher-core TCP ceiling. The GSO column is the repeated selected high rate; it is not the
short exploratory UDP ceiling.

| Payload | TCP ceiling | GSO UDP delivered | UDP / TCP | Reliability note |
|---:|---:|---:|---:|---|
| 64 B | 13.84k/s | 30.00k/s | 2.17x | UDP: zero final loss, median worst-subscriber repair fraction 0.0365% |
| 1232 B | 10.89k/s | 30.00k/s | 2.75x | UDP: zero final loss, median worst-subscriber repair fraction 0.0245% |
| 4096 B | 11.24k/s | 12.00k/s | 1.07x | UDP: zero final loss, median worst-subscriber repair fraction 0.12% |

TCP had zero application loss and preserves stream order. The UDP figures are reliable application
delivery but include rare TCP repair and visible out-of-order delivery at the high points. The
advantage is therefore decisive for small messages and marginal for 4096-byte messages.

At offered rates both transports can meet, the latency comparison is:

| Payload and rate | TCP p50 / p99 | GSO UDP p50 / p99 |
|---|---:|---:|
| 64 B at 10k/s | 227 / 755 us | 284 / 533 us |
| 1232 B at 10k/s | 242 / 964 us | 279 / 781 us |
| 4096 B at 3k/s | 235 / 498 us | 663 / 1,168 us |

The small-message p50 differences are below the roughly 110-120 us one-way clock uncertainty; UDP
has the better median p99 there. For 4096-byte messages, batch-4 GSO is materially slower at equal
load because the partial batch/deadline and three-fragment path dominate at only 3k messages/s.

At overload, TCP's median worst-subscriber p50/p99 was 273/1,480 us, 253/1,211 us, and 326/2,080 us
for the three payload sizes. GSO UDP's corresponding values were 258/1,782 us, 267/1,759 us, and
550/1,883 us, but TCP delivered only its lower ceiling in those offered-rate cases.

## Production spin-poll correction

The preceding UDP results used a publisher that slept or spun until each pacing deadline without
polling transport I/O, then called `poll()` once per 64 publications. The intended production
architecture instead pins the network thread to a core and continuously loops over sending and
receiving. The benchmark now models that explicitly with `publisher_loop=spin-poll`: it polls while
waiting and before every publication. The older behavior remains available as `paced`.

This initially changed the result materially because `UdpPublisher::poll_with()` called
`flush_pending()` unconditionally. Polling after every publication therefore defeated
cross-publication batching. All nine pre-fix spin-poll UDP overload runs reported a configured
batch size of four but `max_publications_per_flush=1`.

Median results across three 20-second repetitions were:

| Payload | Spin-poll UDP ceiling | Spin-poll TCP ceiling | UDP / TCP |
|---:|---:|---:|---:|
| 64 B | 10.42k/s | 13.30k/s | 0.78x |
| 1232 B | 9.85k/s | 10.36k/s | 0.95x |
| 4096 B | 4.62k/s | 11.60k/s | 0.40x |

At the lower latency loads:

| Payload and offered rate | Spin-poll UDP p50 / p99 | Spin-poll TCP p50 / p99 |
|---|---:|---:|
| 64 B at 10k/s | 153 / 467 us | 233 / 948 us |
| 1232 B at 10k/s | 160 / 614 us | 249 / 1,119 us |
| 4096 B at 3k/s | 287 / 948 us | 239 / 676 us |

The 1232-byte UDP publisher delivered a median 9.77k/s at the nominal 10k/s latency point, so that
row is already near its spin-poll ceiling. One-way clock uncertainty was 110-123 us; the p50
differences are not large enough to establish a transport winner. Every spin-poll run had zero
final application loss, zero UDP repair deliveries, zero kernel/NIC drops, no CPU steal, and about
98% publisher-core utilization.

The spin-poll latency improvement versus the paced-loop UDP result is real but comes from flushing
single-publication batches immediately: 64/1232/4096-byte p50 fell from 284/279/663 us to
153/160/287 us. It is not a successful latency-throughput tradeoff because the earlier batching
throughput gain disappeared.

## Deadline-aware spin polling and UDP GRO

The publisher now separates polling from an explicit flush. `poll_with()` flushes only a full or
expired partial batch; `flush_with()` remains unconditional. Flush-reason and batch-dwell counters
verify the behavior directly. At low rate, most partial batches dwell about 50 us and expire. At
high rate, nearly all small-message batches fill to four, while 4096-byte traffic uses a mix of full
and deadline-triggered batches.

With subscriber GRO disabled, the fixed spin-loop implementation recovered the full selected
rates, but high-load receive repairs and tails remained:

| Payload and rate | p50 / p99 | Median worst-subscriber repairs |
|---|---:|---:|
| 64 B at 30k/s | 260 / 1,617 us | 233 / 600,000 |
| 1232 B at 30k/s | 257 / 476 us | 227 / 600,000 |
| 4096 B at 12k/s | 427 / 8,074 us | 264 / 240,000 |

A 1/2/5-ms subscriber repair-delay sweep did not materially reduce repair volume. Five
milliseconds worsened 4096-byte p99.9 to about 76 ms, so the default remains 1 ms.

UDP GRO coalesces equal-sized datagrams in the kernel, exposes their segment size through ancillary
metadata, and lets the subscriber split them without copying. At the same selected rates:

| Payload and rate | GRO p50 / p99 | Median repairs | Logical datagrams / socket message |
|---|---:|---:|---:|
| 64 B at 30k/s | 245 / 699 us | 205 | 4.00x |
| 1232 B at 30k/s | 242 / 757 us | 0 | 4.00x |
| 4096 B at 12k/s | 377 / 959 us | 0 | 5.12x |

All nine selected high-load GRO runs had zero final loss. Repair behavior is still variable rather
than eliminated: one 64-byte run repaired 255 messages and one 4096-byte run repaired 134. GRO
nonetheless materially reduces the receiver work and the 4096-byte tail.

At the lower latency points, GRO p50/p99 was 225/468 us, 224/663 us, and 336/801 us for
64/1232/4096-byte messages. The corresponding spin-loop TCP values were 233/1,591 us,
245/1,171 us, and 235/862 us. P50 differences around 100 us or less remain within the measured
110-123-us one-way clock uncertainty.

The production-loop GRO saturation sweep offered 50k/50k/20k messages/s and found:

| Payload | GRO UDP ceiling | Median p50 / p99 | Median repairs |
|---:|---:|---:|---:|
| 64 B | 32.04k/s | 253 / 454 us | 0 |
| 1232 B | 31.05k/s | 252 / 1,973 us | 271 |
| 4096 B | 16.03k/s | 368 / 797 us | 242 |

Every saturation run also reached zero final loss after repair, but p99.9 remained around
22-35 ms. This is why 30k/30k/12k is the better latency-throughput operating region.

## Exploratory saturation and profiling

A shorter overload sweep found the following publisher ceilings:

| Payload | Current UDP | GSO batch 4 | Ratio |
|---:|---:|---:|---:|
| 64 B | 10.70k/s | 32.96k/s | 3.08x |
| 1232 B | 9.60k/s | 32.05k/s | 3.34x |
| 4096 B | 3.27k/s | 15.55k/s | 4.75x |

Plain batching changed the 4096-byte ceiling only from approximately 3.27k/s to 3.33k/s despite
cutting `sendmmsg` calls by roughly 6x. This demonstrates that syscall entry count was not the
dominant cost; per-datagram kernel work was.

GSO batch 8 raised publisher throughput further, but subscribers crossed their receive/processing
boundary. TCP repairs then preserved final delivery while out-of-order counts and p99/p99.9
latency rose sharply. Subscriber profiling also showed the process at approximately one full core.
Per-datagram profiler frames were intrusive near this boundary, so throughput conclusions use the
uninstrumented counters; profiler traces are diagnostic only.

## Implication for AF_XDP

The scheduling mistake and the first subscriber syscall bottleneck are now addressed. At the
selected unicast operating points, batch-4 GSO plus subscriber GRO provides a production-style
baseline of 30k/30k/12k messages/s. Multicast raises the selected points to 45k/45k/16k and the
exploratory plateaus to about 55k/55k/22.8k. Both paths consume approximately one full publisher
core once process and softirq time are combined, so AF_XDP copy mode can now be evaluated against
meaningful socket baselines rather than an avoidable batching or fanout bug.

### Hot-loop and loss localization update

Splitting the publication deadline poll from the TCP control-plane poll modestly raises the
1232-byte multicast ceiling, but does not explain the order-of-magnitude gap to line rate. The
batch-size cliff is sharp:

| 1232-byte multicast setting | Publisher rate | Receiver result |
|---|---:|---|
| scatter GSO, batch 5 | 65.1k/s | clean; zero repairs |
| scatter GSO, batch 6 | 78.1k/s | 3-5% initial loss |
| scatter GSO, batch 8 | 102.2k/s | 18-19% initial loss |
| scatter GSO, batch 16, repairs delayed | 195k/s | receiver near 100k/s; about 49% initial loss |

The publisher's direct `sendmmsg` duration is roughly 64-75 us per GSO submission across the
batch-size sweep. Plain `sendmmsg` remains near 15-16k datagrams/s regardless of batch size, which
again points to per-datagram kernel work rather than syscall entry count. Receiver pacing reduces
empty `recvmmsg` calls and moves CPU time from system to user space, but does not raise the
approximately 82-100k/s single-flow receive ceiling. Disabling GRO makes it substantially worse.

The loss is not multicast fanout or a nominal link cap: one multicast receiver loses at essentially
the same rate as three, one unicast UDP receiver tops out near 105k/s, one TCP stream reaches about
1.4-1.5 Gbit/s, and four TCP streams reach about 3.65 Gbit/s in aggregate. The hosts use an ixgbe
X550-class 10 Gbit/s NIC and a debug kernel. This leaves distinct sender socket and single-flow
receiver/kernel costs. Batch-5 scatter GSO at about 65k/s is therefore the selected clean
1232-byte socket baseline for the first AF_XDP copy-mode comparison.

### AF_XDP sender results

The sender uses the AF_XDP UAPI directly and constructs Ethernet, 802.1Q, IPv4, UDP, and Flux
headers in UMEM. It supports forced copy and native zero-copy modes, completion recycling,
`XDP_RING_NEED_WAKEUP`, bounded drops, setup fallback, and detailed ring/frame counters.

At 1232 bytes, forced copy mode does not beat the selected socket baseline:

| Sender | Setting | Successfully transmitted | Receiver result |
|---|---|---:|---|
| socket multicast | scatter GSO, batch 5 | 65.1k/s | clean |
| AF_XDP copy | batch 1 | 49.4k/s | clean at its ceiling |
| AF_XDP copy | batch 16-64 | about 60-62k/s | overload above receiver/application target |

Copy mode removes socket protocol work but still takes the per-packet SKB/driver copy path and asks
for frequent TX wakeups. The socket path's GSO amortization is better in the matched regular-kernel
rerun, so copy mode is useful as an implementation milestone and fallback experiment, not as the
path to line rate. The 60-65k comparison in the table above is historical debug-kernel data.

Native zero-copy works with a process-owned driver-mode `XDP_PASS` BPF link. The debug-kernel KASAN
failure was avoided by booting an untainted regular kernel; a one-shot debug/regular comparison
then confirmed that the remaining TX stall was in application/NIC setup, not scheduling or KASAN.
Three setup/implementation defects were corrected:

1. AF_XDP must be created with the publisher. The benchmark's deferred activation sequence left
   the native TX consumer at zero on this ixgbe setup.
2. The TX-only XSK uses a dedicated hardware queue outside the normal RSS set. ixgbe rebuilds queues
   and resets RSS during native XDP attachment, so the runner waits for link recovery and reapplies
   the RSS mapping before it starts subscribers.
3. Batched TX now refreshes the cached kernel consumer whenever cached ring capacity is smaller
   than the requested batch. This removes false tail drops when a batch crosses the 32,768-entry
   ring wrap.

With queue 7 reserved for the XSK, RSS restricted to queues 0-5, a 32,768-entry ring, and 65,536
UMEM frames, the earlier short native zero-copy curve reached the physical link boundary:

| Logical message | Clean achieved rate | Approximate wire load | Worst receiver p50 / p99 / p99.9 |
|---|---:|---:|---:|
| 1232 B | 850k/s | 90.2% of 10 GbE | 139 / 189 / 200 us |
| 1232 B | 900k/s | 95.5% of 10 GbE | 142 / 189 / 543 us |
| 1232 B | 930k/s | 98.7% of 10 GbE | 142 / 190 / 2,243 us |
| 4096 B (3 fragments) | 270k/s | 810k wire pps; 9.46 Gbit/s | 117 / 157 / 281 us |

Three 10-second repetitions at both 900k and 930k 1232-byte messages/s delivered every measured
logical message to all three subscribers with zero XDP ring/frame drops. One later matched socket
run also reached 930k/s cleanly, but the final three-run campaign found one socket gap at 930k and
confirmed 920k as its repeatably clean maximum. The earlier 14.3x claim compared unlike
kernel/host setups and is superseded.

The 60-second A/B found that reboots had reset `net.core.rmem_max` and `net.core.wmem_max` from
64 MiB to 212,992 bytes. With the small limit, receiver `Udp.RcvbufErrors` caused about 50-53 ppm
loss even though XDP and the NIC dropped nothing. Restoring 64 MiB produced zero loss, repair,
socket, NIC, and XDP drops at both 930k/s with 1232-byte messages and 270k/s with 4096-byte
messages. The runner now refuses to start when the configured socket buffer would be clamped.

Those long runs first exposed a sharp tail-latency frontier hidden by the 10-second confirmations.
The final matched campaign refines the values: socket GSO/GRO at 850k/s has 56/115/140 us median
worst-receiver p50/p99/p99.9 and is the recommended balanced point; AF_XDP is clean at 930k/s but
has 153/199/2,255 us latency there.

For 64-byte messages, unbatched zero-copy is publisher-limited near 1.49M/s and a short batch-32
curve reaches about 4.49M/s at the publisher. The final long end-to-end run is much stricter:
three-stream AF_XDP unicast loses bursts at 900k/s because it records no receive GRO aggregation,
while socket GSO/GRO is repeatedly clean at 1.5M/s. AF_XDP RX, segmentation, or receive sharding is
required before the raw sender ceiling is useful.

The resulting backend choice is now clear: use socket multicast/GSO/GRO as the production default,
retain socket unicast as the fallback, and keep TCP repair per subscriber. Native zero-copy AF_XDP
remains an opt-in bare-metal experiment where queue/RSS/XDP lifecycle can be provisioned and
monitored. Copy mode remains useful for functional validation but not for latency.

### Native zero-copy three-stream unicast

For completeness, the AF_XDP sender now also supports static IPv4 unicast fanout through one XSK.
Every logical datagram is encoded once per configured destination using an explicit receiver IP,
UDP port, and Ethernet MAC. The realistic benchmark binds each remote receiver to the known run
port; the publisher still uses one pinned spin-loop core and one isolated hardware TX queue.

The 60-second confirmation produced:

| Mode | Logical rate | Aggregate wire rate | Wire load | Worst receiver p50 / p99 / p99.9 | Loss/drops/repair |
|---|---:|---:|---:|---:|---:|
| 3-stream unicast, balanced | 280k/s | 840k pps | 89.1% | 146 / 199 / 210 us | zero |
| 3-stream unicast, maximum tested clean | 310k/s | 930k pps | 98.7% | 141 / 190 / 2,276 us | zero |
| multicast, balanced reference | 850k/s | 850k pps | 90.2% | 139 / 189 / 200 us | zero |
| multicast, maximum reference | 930k/s | 930k pps | 98.7% | 142 / 190 / 2,243 us | zero |

All three unicast receivers got all 16.8 million logical messages at 280k/s and all 18.6 million
at 310k/s. At 320k offered logical messages/s, the 10-Gbit/s link sustained about 943k wire pps,
equivalent to 314.4k three-way logical messages/s; bounded AF_XDP ring drops and roughly 35 ms
latency correctly exposed overload. Unicast and multicast latency are effectively the same at the
same aggregate wire load. Multicast therefore provides almost exactly three times the logical
fanout throughput without a measurable latency penalty in this topology.

## Result locations

- Final confirmation: `bench-results/udp-opt-final-confirmation1`
- IRQ-controlled TCP confirmation: `bench-results/tcp-final-confirmation1`
- Production spin-poll comparison: `bench-results/spin-poll-final-comparison1`
- Deadline-aware spin-poll comparison: `bench-results/spin-poll-deadline-flush1`
- Repair-delay sweep: `bench-results/repair-delay-sweep1`
- UDP GRO confirmation: `bench-results/udp-gro-confirmation1`
- UDP GRO saturation: `bench-results/udp-gro-saturation1`
- Copy-free one-versus-three fanout: `bench-results/copyfree-fanout-final2`
- Contiguous GSO comparison: `bench-results/gso-contiguous-copy-final1`
- Fixed-versus-adaptive comparison: `bench-results/adaptive-contiguous-final1`
- Multicast viability probe: `bench-results/multicast-viability1`
- Multicast paired shakedown: `bench-results/multicast-shakedown1`
- Multicast overload ceiling: `bench-results/multicast-ceiling1`
- Multicast GSO/non-GSO comparison: `bench-results/multicast-gso-modes1`
- Multicast batch-size tuning: `bench-results/multicast-batch-tune1`
- Final repeated multicast result: `bench-results/multicast-final1`
- Connected multicast and softirq classification: `bench-results/multicast-connected1`
- Interleaved deadline sweep: `bench-results/multicast-deadline-sweep1`
- Final repeated TCP/unicast/multicast latency: `bench-results/tri-transport-final-latency1`
- Split control-poll factorial: `bench-results/multicast-hotloop-factorial1`
- GSO batch-size sweep: `bench-results/multicast-send-batch-sweep1`
- Direct send timing and clean-loss cliff: `bench-results/multicast-gso-cliff1`
- Raw multicast/unicast loss localization: `bench-results/multicast-raw-loss-localization1`
- Receiver polling sweep: `bench-results/multicast-recv-poll-sweep1`
- Independent TCP link probes: `bench-results/private-link-stream1` and
  `bench-results/private-link-stream2`
- AF_XDP copy setup and checksum/wakeup validation: `bench-results/xdp-copy-shakedown3` and
  `bench-results/xdp-copy-checksum-wakeup1`
- AF_XDP copy large-batch saturation: `bench-results/xdp-copy-large-batch2`
- First valid native zero-copy runs: `bench-results/xdp-zero-copy-shakedown3` and the 60k case in
  `bench-results/xdp-zero-copy-throughput1`
- Debug-kernel ixgbe failure reproduction: `bench-results/xdp-zero-copy-throughput2` plus the node
  1 kernel journal at 2026-07-29 10:05:35 Europe/London
- Immediate-activation and dedicated-queue validation:
  `bench-results/xdp-zero-copy-regular-immediate-activation-shakedown2` and
  `bench-results/xdp-zero-copy-regular-throughput-ramp4-persistent-reservation`
- Repeated 900k/930k line-rate confirmation:
  `bench-results/xdp-zero-copy-regular-line-rate-confirmation`
- Long-run receive-buffer A/B: the clamped-buffer loss in
  `bench-results/xdp-zero-copy-regular-burn3` and clean 60-second result in
  `bench-results/xdp-zero-copy-regular-burn4-buffered`
- Clean 60-second 850k/900k latency frontier:
  `bench-results/xdp-zero-copy-regular-latency-frontier1`
- Small-packet sender/receiver boundary after the TX-ring refresh fix:
  `bench-results/xdp-zero-copy-regular-small-packet-ramp5-ring-refresh-fix`
- 4096-byte near-line-rate ramp: `bench-results/xdp-zero-copy-regular-large-message-ramp1`
- Three-stream AF_XDP unicast smoke test, frontier, and 60-second confirmation:
  `bench-results/xdp-zero-copy-unicast-smoke1`,
  `bench-results/xdp-zero-copy-unicast-frontier1`, and
  `bench-results/xdp-zero-copy-unicast-confirmation1`
- Excluded RT-throttling experiment: `bench-results/tri-transport-rt1`
- GSO scatter/contiguous profiler captures: `bench-results/profiles/gso-scatter-4096-s3` and
  `bench-results/profiles/gso-contiguous-4096-s3`
- Controlled rate curves: `bench-results/udp-opt-small-rate-curve2` and
  `bench-results/udp-opt-4096-rate-curve2`
- Batch sweeps: `bench-results/udp-opt-small-sweep1` and
  `bench-results/udp-opt-4096-sweep2`
- Targeted traces: `bench-results/profiles`

The `*-rate-curve1` directories are deliberately excluded because their publisher CPU was
contaminated by IRQ placement.
