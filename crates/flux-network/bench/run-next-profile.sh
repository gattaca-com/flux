#!/usr/bin/env bash
set -euo pipefail

# Stable entry point for the targeted contiguous-vs-scatter publisher profile.
readonly hosts=(
  rocky@solana-node1-lat-fr
  rocky@solana-node2-lat-fr
  rocky@solana-node3-lat-fr
  rocky@solana-node4-lat-fr
)

cargo build --release -p flux-network --example transport_bench --features profiling
cargo build --release -p flux-profiler --bin flux-profiler
for host in "${hosts[@]}"; do
  scp target/release/examples/transport_bench \
    "$host:/home/rocky/flux-transport-bench-profile"
  ssh "$host" chmod 0755 /home/rocky/flux-transport-bench-profile
done
scp target/release/flux-profiler \
  rocky@solana-node1-lat-fr:/home/rocky/flux-profiler
ssh rocky@solana-node1-lat-fr chmod 0755 /home/rocky/flux-profiler

BENCH_UDP_SEND_BATCH_SIZE=4 \
BENCH_UDP_GSO=1 \
BENCH_UDP_GSO_COPY=0 \
BENCH_UDP_GRO=1 \
  bash crates/flux-network/bench/profile-four-node.sh \
    udp 4096 12000 46600 gso-scatter-4096-s3

BENCH_UDP_SEND_BATCH_SIZE=4 \
BENCH_UDP_GSO=1 \
BENCH_UDP_GSO_COPY=1 \
BENCH_UDP_GRO=1 \
  bash crates/flux-network/bench/profile-four-node.sh \
    udp 4096 12000 46610 gso-contiguous-4096-s3
