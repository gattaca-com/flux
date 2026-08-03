# UDP follow-ups

## Goal

This transport is for low-latency fan-out of small, independent, unordered messages. The primary
workload is transaction-sized messages rather than bulk transfer. TCP remains the reliable repair
and control channel.

The normal path should therefore remain simple:

- One application message per UDP datagram whenever the configured MTU permits it.
- Immediate transmission without intentional batching delay.
- Immediate delivery without waiting for older sequences.
- Bounded history and subscriber state.
- TCP repair only for messages that do not arrive over UDP.

The benchmarks showed that successful UDP delivery can improve p50 and tail latency, especially
for independent messages, but that unrestricted sender throughput is not useful by itself. GSO
allowed the publisher to enqueue data much faster than the path or subscriber could consume it,
causing loss and large amounts of repair traffic. Optimizations must therefore distinguish between
successful-message latency, sustainable capacity, and loss-recovery latency.

## 1. Improve measurement first

The main benchmark should focus on the intended workload instead of bulk completion:

- Payloads around 64 and 1232 bytes, with 4096 bytes retained as a fragmentation comparison.
- One subscriber and multiple subscribers on separate hosts.
- Fixed offered rates and explicit burst sizes rather than an unlimited send loop.
- Publish-to-handler p50, p99, p99.9, and maximum latency.
- UDP delivery, repair delivery, duplicates, unavailable messages, and repair requests.
- Publisher and subscriber CPU time or cycles per delivered message.
- Socket and NIC drop counters before and after each phase.

Sampled kernel timestamps should separate userspace-to-qdisc, qdisc-to-driver, and
driver-to-subscriber costs. This will show whether the next bottleneck is application code, the
kernel socket path, NIC queueing, or the network. Timestamping every packet should not become part
of the production hot path.

Success criterion: identify a sustainable offered-rate range where repairs remain negligible and
compare transport latency inside that range. Sender enqueue rate is not a useful result when most
messages are subsequently repaired.

## 2. Tune the host and NIC path

Deployment tuning may improve clean-path latency more than another transport abstraction:

- Pin publisher and subscriber threads to dedicated cores.
- Keep application cores, NIC queues, and memory on the same NUMA node.
- Configure RSS/RPS and XPS so the flow is processed by the intended cores and TX queues.
- Reduce or disable interrupt moderation for latency-sensitive queues.
- Avoid deep CPU sleep states on dedicated polling cores.
- Test `SO_BUSY_POLL` and NAPI busy polling against the current `recvmmsg` spin loop.
- Use a dedicated traffic class or NIC queue when the deployment supports it.

These settings must be benchmarked rather than enabled universally. Busy polling and reduced
interrupt moderation trade CPU and interrupt load for latency.

Success criterion: lower handler latency or CPU cost without increasing loss or long-tail latency.

## 3. Detect missing messages from later UDP traffic

A message that is completely lost is currently discovered from the periodic TCP progress state.
With a 10 ms progress interval, this can add several milliseconds before repair begins.

Receiving sequence `N` can immediately mark earlier unseen sequences as potentially missing while
still delivering `N`. The existing repair delay can provide a short reordering window before the
subscriber requests TCP repair. A reordered packet that arrives during the delay completes
normally and cancels the pending repair.

This improves loss recovery without adding bandwidth or delaying successful messages. The main
risk is spurious repair on paths with significant UDP reordering, so the reorder delay must be
measured and configurable.

Success criterion: substantially lower recovery latency under sparse loss with negligible repair
requests on a loss-free but reordered path.

## 4. Add multicast for controlled deployments

Multicast is the largest architectural improvement when the same message is sent to many
subscribers. The publisher emits one UDP packet and the network replicates it, instead of emitting
one unicast packet per subscriber.

TCP connections remain per subscriber for subscription, progress, repair, and disconnect events.
The multicast address and interface become deployment configuration. The publisher still retains
one history entry per sequence, and each subscriber repairs independently over TCP.

Important operational work includes multicast group management, interface selection, TTL or hop
limit, source filtering, and verifying that the target network does not suppress or rate-limit
multicast.

Success criterion: publisher work and egress bandwidth remain close to constant as subscriber
count increases, without regressing one-subscriber delivery latency.

## 5. Add zero-wait opportunistic batching

Do not hold a message for a timer in the latency path. Instead, when the publisher is ready to send
one message, it may also drain messages that are already available from its input queue.

The first implementation can extend `sendmmsg` batching across messages and subscribers:

- Mixed datagram sizes are allowed.
- Payloads remain in the existing repair-history slots and are referenced by iovecs.
- No additional payload copy is required.
- Message order within each destination is preserved.
- The batch is bounded and flushed immediately; it never waits for another message.

GSO can then optimize the subset of a ready batch containing equal-sized datagrams. It should be a
send-path optimization behind the same semantics, not a separate protocol mode.

This improves sustainable rate and protects latency when publisher CPU is close to saturation. It
does not solve an overloaded link and must not allow the publisher to build an unbounded queue.

Success criterion: lower publisher CPU and higher sustainable offered rate with unchanged
low-rate p50 and no increase in repair fraction.

## 6. Evaluate GRO separately

GRO reduces receive-side kernel work by combining several UDP datagrams into one larger receive
buffer. It was effective at increasing datagrams per receive operation in the benchmark, but it
may delay the first datagram until the current aggregation cycle completes.

Compare these combinations independently:

- No GSO and no GRO.
- GSO without GRO.
- GSO with GRO.
- GRO with ordinary sends.

Keep GRO optional unless it improves capacity without a measurable latency regression. The
subscriber already has a `recvmmsg` batch and showed substantial receive headroom in several runs.

## 7. Define overload behavior

Batching and GSO reduce CPU overhead but can make the publisher reach the link limit faster. For a
latency transport, building a large queue is usually worse than rejecting or dropping stale work.

Possible deployment policies are:

- The application guarantees an offered rate below provisioned capacity.
- A configured token bucket limits aggregate UDP egress.
- A small bounded queue absorbs short bursts, with an explicit stale-message policy.
- Publication reports backpressure so the caller decides whether to retry or drop.

This is congestion avoidance for a known deployment, not an attempt to implement general Internet
congestion control. Arbitrary congested WAN paths should continue to use TCP or QUIC.

Success criterion: short bursts do not cause a repair storm, and the transport never accumulates
unbounded latency internally.

## 8. Evaluate simple cross-message FEC

FEC is relevant to sparse-loss recovery, not clean-path p50. A useful first scheme is systematic
XOR parity across a small group of original datagrams:

- Original messages are sent immediately.
- One parity datagram follows every configured group of messages.
- One missing message can be reconstructed after the remaining group members and parity arrive.
- Multiple losses or unavailable parity fall back to TCP repair.

For example, one parity packet for eight data packets adds 12.5% bandwidth. At a high message rate,
the group can complete much sooner than a TCP repair round trip. Variable payload lengths,
partial groups, versioned wire metadata, and subscriber memory bounds must be specified explicitly.

FEC should only proceed after measuring real loss below path capacity. Under congestion, parity
traffic can worsen the condition it is intended to repair.

Success criterion: materially lower p99.9 or loss-recovery latency at an acceptable bandwidth and
CPU cost, with no delay added to original data packets.

## 9. Experiment with io_uring receive

The subscriber currently performs many empty nonblocking `recvmmsg` calls while spinning.
io_uring multishot `recvmsg` with provided buffers could leave one receive request active and
deliver completions as datagrams arrive.

Prototype this in the benchmark before changing the transport. Compare:

- Current `recvmmsg` spin.
- `recvmmsg` with socket or NAPI busy polling.
- io_uring multishot receive while polling the completion queue.
- io_uring multishot receive with NAPI busy polling.

Keep the publisher on synchronous `sendmmsg` initially. Async sends require history payloads and
metadata to remain pinned until completion, and one send submission per message does not remove
the need for batching.

Success criterion: lower receive latency or materially lower CPU at equal latency. A reduction in
empty syscalls alone does not justify replacing the current path.

## 10. Revisit AF_XDP only after profiling

AF_XDP should remain experimental until timestamping shows that the kernel UDP stack is a material
part of end-to-end latency or the required packet rate exceeds the socket path's sustainable
capacity.

A production design would need dedicated queue selection, correct routing and neighbour handling,
UMEM ownership, completion processing, wakeup management, and clean fallback when zero-copy mode
is unavailable. Sender-only AF_XDP is unlikely to transform end-to-end latency; meaningful gains
may require dedicated queues and AF_XDP at both endpoints.

Success criterion: a reproducible end-to-end latency or capacity improvement large enough to
justify the deployment and maintenance cost.

## Deliberately deferred

- **Timed batching:** improves syscall efficiency by spending the latency advantage.
- **UDP repair:** repair remains reliable and congestion-controlled over TCP.
- **`MSG_ZEROCOPY`:** page pinning and completion handling are unsuitable for small messages.
- **A full io_uring event-loop rewrite:** only justified after a focused receive experiment.
- **General WAN congestion control:** use TCP or QUIC instead of rebuilding it here.

## Recommended order

1. Land and validate the simple reliable-unicast implementation.
2. Improve the benchmark and tune the host/NIC path.
3. Add UDP-driven gap detection.
4. Add multicast if the target network supports it.
5. Add zero-wait batching and optional GSO, measuring GRO separately.
6. Define overload policy for the actual deployment.
7. Evaluate FEC, io_uring receive, and finally AF_XDP only when measurements justify them.
