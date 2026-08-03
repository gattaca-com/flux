# Final four-node UDP/TCP/AF_XDP benchmark

Date: 2026-07-29

## Executive result

For this hardware and the current normal socket receivers, the best production candidate is
socket UDP with multicast, GSO, and GRO. AF_XDP zero-copy is functional and reaches the same
physical limit, but sender-only AF_XDP does not improve the balanced latency point. It only moves
the 1232-byte multicast clean ceiling from 920k to 930k logical messages/s (about 1%), while socket
UDP has lower latency below that edge. AF_XDP copy mode is simpler but has unacceptable tails.

Multicast is the architectural win. At 1232 bytes, optimized unicast and AF_XDP unicast both stop
around 310k logical messages/s because node 1 transmits three copies. Multicast lets the switch
replicate one transmitted stream and sustains 920-930k logical messages/s, delivering 2.76-2.79M
application messages/s in aggregate to the three subscribers.

For 64-byte messages, socket GSO/GRO is materially better than sender-only AF_XDP. Socket unicast
is repeatedly clean at 1.5M logical messages/s, while AF_XDP unicast loses bursts at 900k/s. The
socket path coalesces about 32 datagrams into each receive buffer; AF_XDP unicast arrives as one
socket message per datagram and shows no GRO aggregation. An AF_XDP receiver, receive sharding, or
AF_XDP-side segmentation would be needed for a fair tiny-packet line-rate design.

For 4096-byte messages, TCP, socket UDP, and AF_XDP all reach the same 10-Gbit/s boundary. Socket
UDP multicast is repeatedly clean at 285k logical messages/s, but 280k has much better tails.

## Benchmark contract

- Publisher: `solana-node1-lat-fr`; independent subscribers: nodes 2, 3, and 4.
- Bare-metal 10-Gbit/s `ixgbe` NICs over the private VLAN; MTU 1500; no loopback evidence.
- Matched untainted `6.12.0-211.39.1.el10_2.0.1.x86_64` regular kernel on all hosts.
- All 12 logical CPUs online, performance governor, 12 combined NIC queues.
- Publisher and each receiver pinned to CPU 2 and continuously spin-polling. NIC IRQs are moved to
  CPUs 4 and 5 while a case runs.
- 64 MiB `rmem`/`wmem` limits, bounded transport queues, three active subscribers required.
- Short rate curves locate boundaries. Selected cases use three repetitions, reversed order on
  repetition two, five-second warm-up, and either 30- or 60-second measurement intervals.
- A rate is clean only if every subscriber receives every sequence and XDP, socket, NIC, and
  repair counters are zero. Sender enqueue rate alone is never treated as throughput.
- Latency is sampled one-way publish-to-application delivery. Clock calibration uncertainty is
  23-39 us (median 25 us), so small p50 differences require hardware timestamping to confirm.

Rates below are logical publications per second delivered to each subscriber. Multicast aggregate
application delivery is three times the listed rate.

## 1232-byte comparison

This is the most complete campaign: selected cases use three 60-second repetitions.

### Sustainable throughput frontier

| Transport/configuration | Confirmed point | Boundary observed | Reliability interpretation |
|---|---:|---:|---|
| TCP, three streams | 258k/s | about 259k/s sender ceiling | Clean |
| UDP unicast, current default, no GSO/GRO | 150k/s | about 155k/s sender ceiling | Clean at 150k |
| UDP unicast, batching only, no GSO | - | about 164k/s | Batching alone is a small gain |
| UDP unicast, GSO/GRO | 310k/s | 320k overload | Clean at 310k |
| AF_XDP zero-copy unicast | 310k/s | 320k overload | Clean at 310k |
| UDP multicast, current default, no GSO/GRO | 350k/s | about 363k/s sender ceiling | Clean at 350k |
| UDP multicast, batching only, no GSO | - | about 414k/s | Multicast plus batching, still CPU-bound |
| UDP multicast, GSO/GRO | 920k/s | 930k edge; 950k overload | 920k clean; one 930k repetition lost 5,961/55.8M |
| AF_XDP zero-copy multicast | 930k/s | 950k overload | All three 930k repetitions clean |
| AF_XDP forced-copy multicast | 920k/s | sender can keep up | Clean delivery but multi-ms tails |

GSO is the large software unlock; cross-publication batching without GSO is only a modest change.
GRO adds receiver headroom near the physical boundary: GRO-off can handle 850k/s, but begins to
show small gaps around 930k/s.

### Latency at useful operating points

Values are median across repetitions of each run's worst subscriber, in microseconds.

| Configuration | Rate | p50 | p99 | p99.9 | Loss |
|---|---:|---:|---:|---:|---:|
| TCP | 250k | 136 | 251 | 287 | 0 |
| UDP unicast default | 150k | 89 | 148 | 155 | 0 |
| UDP unicast GSO/GRO | 280k | 104 | 163 | 174 | 0 |
| AF_XDP zero-copy unicast | 280k | 156 | 213 | 225 | 0 |
| UDP multicast default | 350k | 64 | 119 | 129 | 0 |
| UDP multicast GSO/GRO, balanced | 850k | 56 | 115 | 140 | 0 |
| AF_XDP zero-copy multicast, balanced | 850k | 169 | 227 | 242 | 0 |
| UDP multicast GSO/GRO, maximum clean | 920k | 88 | 185 | 1,503 | 0 |
| AF_XDP zero-copy multicast, maximum clean | 930k | 153 | 199 | 2,255 | 0 |
| AF_XDP forced-copy multicast | 860k | 151 | 2,477 | 2,562 | 0 |

The balanced socket multicast point is 850k/s. It preserves large throughput margin over TCP and
has the best measured tails. The 920-930k points are capacity results, not latency targets.

## 64-byte comparison

Short curves show socket UDP multicast can enqueue about 3.3M/s and AF_XDP about 4.5M/s, but the
long end-to-end runs expose rare receive or egress stalls before those sender ceilings. The table
below uses three 30-second repetitions.

| Configuration | Rate | p50 | p99 | p99.9 | Worst sequence gap across repetitions |
|---|---:|---:|---:|---:|---:|
| TCP | 250k | 88 | 163 | 178 | 0 |
| UDP unicast default | 150k | 48 | 139 | 141 | 0 |
| UDP unicast GSO/GRO | 1.5M | 62 | 83 | 94 | 0 |
| UDP unicast GSO/GRO, edge | 1.8M | 55 | 73 | 80 | 4,839/54M in one repetition |
| AF_XDP zero-copy unicast | 900k | 79 | 152 | 755 | 13,537/27M |
| UDP multicast default | 350k | 60 | 121 | 129 | 0 |
| UDP multicast GSO/GRO, near-clean | 2.5M | 45 | 67 | 103 | 34/75M on worst receiver in one repetition |
| UDP multicast GSO/GRO, edge | 2.8M | 42 | 49 | 90 | 4,331/84M |
| AF_XDP zero-copy multicast | 2.5M | 71 | 96 | 104 | 2,929/75M |
| AF_XDP zero-copy multicast, edge | 3.0M | 70 | 90 | 111 | 5,246/90M |

The loss bursts above have zero `Udp.RcvbufErrors`, zero generic NIC drops, and no AF_XDP
ring/frame drops. They are real application sequence gaps and must not be hidden by the much
better average latency. TCP repair can recover them, but a strict no-loss deployment should keep
more headroom and validate longer runs after boot-time host isolation.

The receive counters explain the AF_XDP unicast regression:

- Socket GSO/GRO at 1.5M/s: 45.0M datagrams arrive through about 1.41M socket messages per
  receiver, close to 32 datagrams per GRO buffer.
- AF_XDP unicast at 900k/s: about 27.0M datagrams arrive as about 27.0M socket messages, with zero
  GRO packets/segments recorded.

Sender-only AF_XDP therefore removes work on node 1 while creating substantially more work on
every receiving socket for this case.

## 4096-byte comparison

Selected socket cases use three 30-second repetitions. AF_XDP has three valid multicast
repetitions and two valid unicast publisher results; one additional unicast run delivered all data
to all subscribers but its publisher timed out during teardown and is excluded.

| Configuration | Rate | p50 | p99 | p99.9 | Loss |
|---|---:|---:|---:|---:|---:|
| TCP | 90k | 151 | 268 | 434 | 0 |
| UDP unicast default | 40k | 103 | 166 | 170 | 0 |
| UDP unicast GSO/GRO | 90k | 141 | 203 | 1,349 | 0 |
| AF_XDP zero-copy unicast | 90k | 159-162 | 214-216 | 1,321-1,354 | 0 |
| UDP multicast default | 140k | 64 | 132 | 146 | 0 |
| UDP multicast GSO/GRO, balanced | 280k | 94 | 162 | 220 | 0 |
| AF_XDP zero-copy multicast, balanced | 280k | 121 | 160 | 2,470 | 0 |
| UDP multicast GSO/GRO, edge | 285k | 144 | 2,574 | 3,060 | 0 |
| AF_XDP zero-copy multicast, edge | 285k | 125 | 2,559 | 3,044 | 0 |

All paths overload by 290k multicast or 96-98k three-stream unicast. That common boundary is the
physical 10-Gbit/s link, not the userspace API. At 285k the queueing tail is unstable, so 280k is
the sensible multicast operating point.

## Publisher profile attribution

The profiler is diagnostic and loses events near line rate; uninstrumented delivery counters are
the throughput authority.

At 1232 bytes and 850k multicast messages/s:

| Publisher path | Sampled operation | Mean | Amortized per logical message |
|---|---|---:|---:|
| Socket GSO, batch 5 | `udp.sendmmsg` | 3.15 us/batch | about 0.63 us |
| AF_XDP zero-copy, batch 1 | `xdp.send_batch` | 0.68 us/message | about 0.68 us |
| AF_XDP inner wakeup | `xdp.kick` | 0.52 us/message | about 0.52 us |

AF_XDP makes one submission cheaper, but socket GSO amortizes almost exactly the same cost across
five messages. This explains why AF_XDP does not create a large throughput or latency advantage
on the matched regular kernel.

## Recommendation

1. Use socket multicast with opportunistic batching, GSO, and GRO where the network supports
   multicast. For 1232-byte traffic, start around the 850k/s balanced point rather than 920k/s.
2. Keep socket GSO/GRO for unicast fallback. It matches AF_XDP's large-message/link ceiling and is
   much better for 64-byte traffic with the current receivers.
3. Keep AF_XDP zero-copy experimental. Its value is explicit queue ownership and a path toward an
   AF_XDP end-to-end design, not a demonstrated sender-only production win.
4. Do not use forced-copy AF_XDP for the latency path; the regular kernel fixes its old throughput
   collapse, but its p99 is still multi-millisecond near high rates.
5. Before tightening latency claims, add PTP/NIC hardware timestamps, boot-time CPU/IRQ isolation,
   C-state/interrupt-moderation experiments, and longer loss-burst tests.
6. If tiny-packet line rate is a requirement, next test an AF_XDP receiver or RSS-sharded socket
   receivers. The current single-core socket receiver is the limiting component for raw AF_XDP
   unicast packets.

## Canonical artifacts

- 1232-byte socket: `bench-results/final-regular-1232-socket-confirmation1` and
  `bench-results/final-regular-1232-socket-mcast-refinement1`
- 1232-byte AF_XDP: `bench-results/final-regular-1232-xdp-zero-copy-unicast-confirmation1`,
  `bench-results/final-regular-1232-xdp-zero-copy-mcast-confirmation1`, and
  `bench-results/final-regular-1232-xdp-copy-confirmation1`
- 64-byte socket: `bench-results/final-regular-64-socket-confirmation1` and
  `bench-results/final-regular-64-socket-clean-refinement1`
- 64-byte AF_XDP: `bench-results/final-regular-64-xdp-zero-copy-confirmation1`
- 4096-byte socket: `bench-results/final-regular-4096-socket-confirmation1`
- 4096-byte AF_XDP: `bench-results/final-regular-4096-xdp-zero-copy-confirmation1` and
  `bench-results/final-regular-4096-xdp-zero-copy-confirmation2`
- Profiles: `bench-results/final-regular-profile-socket-mcast-1232-850k` and
  `bench-results/final-regular-profile-xdp-mcast-1232-850k`

The short frontier directories remain useful for overload boundaries, but the repeated
confirmation directories above take precedence whenever the results differ.
