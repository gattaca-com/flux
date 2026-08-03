# Flux production network checklist

Use this checklist for bare-metal Flux publishers and subscribers.

The approved data path is UDP sockets with GSO and GRO. Use multicast when the network supports
it. AF_XDP is experimental.

## Approved profile

| Item | Required value |
|---|---|
| Kernel | Same approved regular kernel on all hosts |
| Debug kernel | Not permitted |
| Kernel taint | 0 |
| CPU governor | `performance` |
| Network thread | One dedicated physical core |
| Network-thread sibling | Idle, if possible |
| NIC interrupts | Not on the network core or its sibling |
| Link | 10 Gb/s, full duplex, MTU 1500 |
| NIC queues | 12 combined queues on approved ixgbe hosts |
| Normal RSS | Queues 0 to 11 |
| Socket limits | 64 MiB send and receive maxima |
| Publisher | UDP GSO enabled |
| Subscriber | UDP GRO enabled |
| Scheduler | Normal scheduler with CPU affinity |

GSO means Generic Segmentation Offload. GRO means Generic Receive Offload. RSS means Receive Side
Scaling. NIC means Network Interface Card.

## 1. Provision the host

### Kernel and CPU

- [ ] Install the approved regular kernel.
- [ ] Use the same kernel build on all Flux hosts.
- [ ] Verify that the kernel name does not contain `debug`.
- [ ] Verify that `/proc/sys/kernel/tainted` is `0`.
- [ ] Keep the required CPUs online.
- [ ] Set the CPU governor to `performance`.
- [ ] Pin the Flux network thread to one physical core.
- [ ] Keep other services off that core.
- [ ] Keep its SMT sibling idle, if possible.

The approved test profile used SMT. Remove `nosmt` from the managed kernel command line if the
production profile also uses SMT.

### NIC and interrupts

- [ ] Verify the approved NIC driver and firmware.
- [ ] Verify 10 Gb/s and full-duplex mode.
- [ ] Verify the approved VLAN and MTU 1500.
- [ ] Configure 12 combined queues on approved ixgbe hosts.
- [ ] Configure normal RSS across queues 0 to 11.
- [ ] Select IRQ CPUs that are separate from the Flux core.
- [ ] Keep all data-NIC interrupts off the Flux core and its sibling.
- [ ] Configure `irqbalance` exclusions or install a managed IRQ-affinity service.
- [ ] Do not store IRQ numbers in static configuration.

```bash
sudo ethtool -L eno2 combined 12
sudo ethtool -X eno2 equal 12
```

### Socket and transport settings

- [ ] Install `/etc/sysctl.d/90-flux-network.conf`.
- [ ] Set `net.core.rmem_max` to `67108864`.
- [ ] Set `net.core.wmem_max` to `67108864`.
- [ ] Configure Flux sockets to request the required buffer sizes.
- [ ] Enable UDP GSO on publishers.
- [ ] Enable UDP GRO on subscribers.
- [ ] Keep all application send queues bounded.
- [ ] Use the normal scheduler.
- [ ] Do not enable `SCHED_FIFO` without separate approval.

```text
net.core.rmem_max = 67108864
net.core.wmem_max = 67108864
```

### Multicast hosts

- [ ] Assign the approved multicast group and UDP port.
- [ ] Configure the data VLAN and multicast interface.
- [ ] Configure an IGMP querier on the VLAN.
- [ ] Verify IGMP snooping on each switch.
- [ ] Permit the group, UDP port, and IGMP through the firewall.
- [ ] Permit the TCP control and repair ports.
- [ ] Limit firewall rules to the trusted data network.
- [ ] Monitor each subscriber's group membership.

## 2. Verify the host before service

Run these checks after each boot, NIC reset, or driver update:

```bash
uname -r
cat /proc/sys/kernel/tainted
lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE
cat /sys/devices/system/cpu/cpu2/cpufreq/scaling_governor
ethtool eno2
ethtool -i eno2
ethtool -l eno2
ethtool -x eno2
ethtool -k eno2
ip -details link show dev eno2
sysctl -n net.core.rmem_max net.core.wmem_max
awk '/eno2-TxRx/ {print}' /proc/interrupts
```

Do not put the host in service unless all checks pass:

- [ ] The approved regular kernel is active.
- [ ] The kernel taint value is 0.
- [ ] The CPU governor is `performance`.
- [ ] The Flux thread has the correct CPU affinity.
- [ ] No data-NIC interrupt uses the Flux core or its sibling.
- [ ] The link is 10 Gb/s and full duplex.
- [ ] The VLAN and MTU are correct.
- [ ] The NIC queue count and RSS table are correct.
- [ ] Both socket limits are 64 MiB.
- [ ] GSO and GRO are available and active.
- [ ] The clock is synchronized.
- [ ] TCP control and repair connections work.
- [ ] Multicast works, if the service uses multicast.
- [ ] All loss counters have a clean start value.

## 3. Monitor the host

Monitor these values:

- [ ] Flux publish and delivery rates.
- [ ] Active subscriber count.
- [ ] Sequence gaps and unavailable messages.
- [ ] Repair requests and repair deliveries.
- [ ] p50, p99, p99.9, and maximum latency.
- [ ] `Udp.RcvbufErrors`, `Udp.SndbufErrors`, and `Udp.MemErrors`.
- [ ] NIC errors, drops, missed packets, and TX timeouts.
- [ ] Link state and speed.
- [ ] Per-queue traffic and interrupt rates.
- [ ] CPU frequency, temperature, migration, and run-queue delay.
- [ ] IRQ, queue, RSS, governor, and offload configuration.
- [ ] Clock synchronization state and offset.
- [ ] Multicast group membership and IGMP querier state.

Send an alert when one of these events occurs:

- An application sequence gap continues for the alert interval.
- A UDP socket-buffer error occurs.
- The repair rate is more than the approved limit.
- A NIC error counter increases.
- The link changes state or speed.
- A NIC interrupt moves to the Flux core.
- A required host setting changes.
- Multicast membership fails.
- The host clock loses synchronization.

## 4. Reboot and rollback

After each reboot or upgrade:

1. Apply the CPU governor and affinity policy.
2. Apply the NIC queue and RSS configuration.
3. Apply the NIC interrupt policy.
4. Verify the sysctl values.
5. Verify GSO and GRO.
6. Verify multicast, if used.
7. Run all pre-service checks.
8. Send a limited amount of production traffic.
9. Return the host to service after the checks succeed.

Keep the previous approved kernel as a boot option. Drain the host before you change the kernel,
NIC, IRQ, RSS, or XDP configuration.

## 5. Why these settings matter

| Setting | Measured effect |
|---|---|
| Regular kernel | Reduced a five-message GSO send from 64-75 us to about 3.15 us |
| IRQ isolation | Increased Flux CPU access from 53-71 percent to 97-99 percent |
| 64 MiB socket limits | Removed about 50-53 receive-buffer losses per million messages |
| GSO and GRO | Increased 1232-byte multicast from about 414,000 to 920,000 messages/s |
| Multicast | Increased three-subscriber logical throughput from about 310,000 to 920,000 messages/s |
| Normal scheduler | Avoided the 13-50 ms stalls caused by real-time throttling |

## 6. AF_XDP experimental hosts

Do not apply this section to standard production hosts.

- [ ] Use a separate host group.
- [ ] Require native zero-copy mode.
- [ ] Reserve XSK queue 7.
- [ ] Restrict normal RX RSS to queues 0 to 5.
- [ ] Reapply RSS after XDP attachment.
- [ ] Configure 32,768 TX and completion entries.
- [ ] Configure 65,536 UMEM frames.
- [ ] Monitor ring, frame, kick, and completion counters.
- [ ] Fail readiness after any XDP ring or frame drop.
- [ ] Remove XDP and restore normal RSS during rollback.
- [ ] Keep socket UDP as the rollback path.

AF_XDP added about one percent to the clean 1232-byte multicast limit. It did not improve balanced
latency. Do not use AF_XDP copy mode for the low-latency path.

## 7. Settings that need separate approval

Do not apply these settings to all production hosts without separate tests:

- CPU boot isolation options.
- Disabled CPU C-states.
- New NUMA placement rules.
- New NIC interrupt-coalescing values.
- New NIC descriptor-ring sizes.
- RPS, RFS, XPS, or NAPI busy polling.
- `SO_BUSY_POLL`.
- io_uring receive.
- Jumbo frames.
- Real-time scheduling.
- PTP or NIC hardware timestamps.
- AF_XDP receivers.

Test one change at a time. Prepare a rollback procedure. Use limited production traffic before a
deployment to all hosts.
