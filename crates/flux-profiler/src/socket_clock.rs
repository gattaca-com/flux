use std::mem;

use flux_timing::{Duration, Nanos, SOCKET_SHIFT, TSC_MASK, read_tsc_and_node};

pub(crate) const MAX_NODES: usize = 1 << (64 - SOCKET_SHIFT);

#[derive(Clone, Copy)]
struct Node {
    tsc: u64,
    wall_ns: u64,
}

pub struct SocketClocks {
    nodes: [Node; MAX_NODES],
}

impl SocketClocks {
    fn single_socket() -> Self {
        let (wall_ns, tsc, _) = sample();
        Self { nodes: [Node { tsc, wall_ns }; MAX_NODES] }
    }

    /// Per-CPU calibration; needs Linux thread-affinity to pin onto each core.
    #[cfg(target_os = "linux")]
    pub fn calibrate() -> Self {
        // Do in a different thread to avoid changing the caller's CPU affinity
        std::thread::Builder::new()
            .name("socket-clock-calib".to_owned())
            .spawn(Self::calibrate_per_cpu)
            .map_or_else(
                |_| Self::single_socket(),
                |handle| handle.join().unwrap_or_else(|_| Self::single_socket()),
            )
    }

    /// Single-socket machines (e.g. macOS) have no cross-socket skew to
    /// correct.
    #[cfg(not(target_os = "linux"))]
    pub fn calibrate() -> Self {
        Self::single_socket()
    }

    #[cfg(test)]
    pub(crate) fn identity() -> Self {
        Self { nodes: [Node { tsc: 0, wall_ns: 0 }; MAX_NODES] }
    }

    #[cfg(target_os = "linux")]
    fn calibrate_per_cpu() -> Self {
        let (wall_ns, tsc, _) = sample();
        let mut nodes = [Node { tsc, wall_ns }; MAX_NODES];
        let mut max_node = 0;
        for cpu in allowed_cpus() {
            pin_to(cpu);
            let (wall_ns, tsc, node) = sample();
            max_node = max_node.max(node);
            nodes[node & (MAX_NODES - 1)] = Node { tsc, wall_ns };
        }
        if max_node >= MAX_NODES {
            tracing::warn!(
                numa_nodes = max_node + 1,
                tag_buckets = MAX_NODES,
                "more NUMA nodes than the profiler's socket tag can distinguish; marks from \
                 nodes that alias onto a different socket may carry inaccurate timestamps"
            );
        }
        Self { nodes }
    }

    /// ntpd corrects the wall clock but not the TSC, so an anchor drifts. One
    /// sample re-anchors every node: the gaps between sockets' TSCs are fixed
    /// at boot, so the drift is shared.
    pub fn recalibrate(&mut self) {
        let (wall_ns, tsc, node) = sample();
        let now = ((node as u64) << SOCKET_SHIFT) | tsc;
        let by = wall_ns as i64 - self.resolve_ns(now) as i64;
        for node in &mut self.nodes {
            node.wall_ns = node.wall_ns.saturating_add_signed(by);
        }
    }

    pub fn resolve_ns(&self, packed: u64) -> u64 {
        let node = self.nodes[(packed >> SOCKET_SHIFT) as usize & (MAX_NODES - 1)];
        let tsc = packed & TSC_MASK;
        let scale = |ticks: u64| Duration(ticks).as_nanos() as u64;
        if tsc >= node.tsc {
            node.wall_ns.saturating_add(scale(tsc - node.tsc))
        } else {
            node.wall_ns.saturating_sub(scale(node.tsc - tsc))
        }
    }
}

fn sample() -> (u64, u64, usize) {
    let t0 = Nanos::now().0;
    let (tsc, node) = read_tsc_and_node();
    let t1 = Nanos::now().0;
    (t0.midpoint(t1), tsc & TSC_MASK, node as usize)
}

/// CPUs from the current thread's affinity mask. Empty if the query fails.
#[cfg(target_os = "linux")]
fn allowed_cpus() -> impl Iterator<Item = usize> {
    let set = unsafe {
        let mut set: libc::cpu_set_t = mem::zeroed();
        libc::sched_getaffinity(0, mem::size_of::<libc::cpu_set_t>(), &raw mut set);
        set
    };
    (0..libc::CPU_SETSIZE as usize).filter(move |&c| unsafe { libc::CPU_ISSET(c, &set) })
}

#[cfg(target_os = "linux")]
fn pin_to(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &raw const set);
    }
}

#[cfg(test)]
mod tests {
    use flux_timing::{Nanos, SOCKET_SHIFT};

    use super::{MAX_NODES, Node, SocketClocks};

    const SECOND: u64 = 1_000_000_000;

    fn packed(node: u64, tsc: u64) -> u64 {
        (node << SOCKET_SHIFT) | tsc
    }

    #[test]
    fn identity() {
        let c = SocketClocks::identity();
        assert_eq!(c.resolve_ns(packed(0, 0)), 0);
        assert!(c.resolve_ns(packed(0, 10_000)) > 0);
        assert_eq!(c.resolve_ns(packed(3, 10_000)), c.resolve_ns(packed(0, 10_000)));
    }

    #[test]
    fn per_node_clocks() {
        let mut nodes = [Node { tsc: 0, wall_ns: 0 }; MAX_NODES];
        nodes[0] = Node { tsc: 1_000_000, wall_ns: 5_000 };
        nodes[1] = Node { tsc: 9_000_000, wall_ns: 5_000 };
        let c = SocketClocks { nodes };

        assert_eq!(c.resolve_ns(packed(0, 1_000_000)), 5_000);
        assert_eq!(c.resolve_ns(packed(1, 9_000_000)), 5_000);
        assert!(c.resolve_ns(packed(1, 8_000_000)) < 5_000);
        let adv0 = c.resolve_ns(packed(0, 1_003_000)) - 5_000;
        let adv1 = c.resolve_ns(packed(1, 9_003_000)) - 5_000;
        assert_eq!(adv0, adv1);
    }

    /// The whole point: a clock that has drifted must come back to the wall
    /// clock. An earlier version cancelled its own correction and this caught
    /// it.
    #[test]
    fn recalibrate_corrects_a_fast_clock() {
        let (wall_ns, tsc, node) = super::sample();
        let mut c = SocketClocks { nodes: [Node { tsc, wall_ns: wall_ns + SECOND }; MAX_NODES] };
        let now = packed(node as u64, tsc);
        assert!(c.resolve_ns(now).abs_diff(wall_ns) > SECOND / 2, "the clock starts wrong");

        c.recalibrate();

        let (wall_ns, tsc, node) = super::sample();
        let err = c.resolve_ns(packed(node as u64, tsc)).abs_diff(wall_ns);
        assert!(err < 1_000_000, "still {err}ns out after recalibrating");
    }

    /// Sockets differ by a fixed amount, so one sample moves them all together.
    #[test]
    fn recalibrate_keeps_the_gap_between_sockets() {
        let (wall_ns, tsc, _) = super::sample();
        let mut nodes = [Node { tsc, wall_ns: wall_ns + SECOND }; MAX_NODES];
        nodes[1].wall_ns += 500;
        let mut c = SocketClocks { nodes };

        c.recalibrate();

        assert_eq!(c.nodes[1].wall_ns - c.nodes[0].wall_ns, 500);
    }

    #[test]
    fn calibrate_wall_clock() {
        let before = Nanos::now().0;
        let c = SocketClocks::calibrate();
        let after = Nanos::now().0;
        assert!(c.nodes.iter().all(|n| before <= n.wall_ns && n.wall_ns <= after));
    }
}
