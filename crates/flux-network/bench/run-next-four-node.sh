#!/usr/bin/env bash
set -euo pipefail

# Stable approved entry point for the final AF_XDP publisher profile.
BENCH_RESULTS=bench-results/final-regular-profile-xdp-mcast-1232-850k \
BENCH_CASES='udp:1232:850000:1:0:60000:1:3:0:0:1:20:100:0' \
BENCH_REPETITIONS=1 \
BENCH_WARMUP_SECS=3 \
BENCH_DURATION_SECS=10 \
BENCH_DRAIN_SECS=3 \
BENCH_END_MARKERS=64 \
BENCH_UDP_MULTICAST=0 \
BENCH_UDP_XDP=1 \
BENCH_XDP_MODE=zero-copy \
BENCH_XDP_QUEUE=7 \
BENCH_XDP_RSS_QUEUES=6 \
BENCH_XDP_RING_SIZE=32768 \
BENCH_XDP_FRAME_COUNT=65536 \
BENCH_STABILIZE_IRQS=1 \
BENCH_PROFILE_TARGET=publisher \
BENCH_PROFILE_CAPTURE_SECS=5 \
exec bash crates/flux-network/bench/run-four-node.sh
