#!/usr/bin/env bash
set -euo pipefail

readonly publisher_host=rocky@solana-node1-lat-fr
readonly subscriber_hosts=(
  rocky@solana-node2-lat-fr
  rocky@solana-node3-lat-fr
  rocky@solana-node4-lat-fr
)
readonly subscriber_ips=(10.9.0.2 10.9.0.3 10.9.0.4)
readonly remote_probe=/home/rocky/flux-multicast-probe.py
readonly group=239.255.42.42
readonly port=46800
readonly count=50000
readonly rate=20000
readonly datagram_bytes=1400
readonly results=bench-results/multicast-viability1

mkdir -p "$results"
for host in "$publisher_host" "${subscriber_hosts[@]}"; do
  scp crates/flux-network/bench/multicast_probe.py "$host:$remote_probe"
done

subscriber_pids=()
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  for pid in "${subscriber_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  exit "$status"
}
trap cleanup EXIT INT TERM

for index in 0 1 2; do
  ssh "${subscriber_hosts[index]}" \
    timeout 15s taskset -c 2 python3 "$remote_probe" subscriber \
      --group "$group" --port "$port" --interface-ip "${subscriber_ips[index]}" \
      --count "$count" --timeout 10 \
    >"$results/subscriber-$((index + 1)).log" 2>&1 &
  subscriber_pids+=("$!")
done

for _attempt in {1..100}; do
  ready=0
  for index in 1 2 3; do
    grep -q '^READY ' "$results/subscriber-$index.log" 2>/dev/null && ready=$((ready + 1))
  done
  ((ready == 3)) && break
  sleep 0.05
done
if ((ready != 3)); then
  echo "multicast subscribers did not become ready" >&2
  exit 1
fi

ssh "$publisher_host" \
  timeout 15s taskset -c 2 python3 "$remote_probe" publisher \
    --group "$group" --port "$port" --interface-ip 10.9.0.1 \
    --count "$count" --rate "$rate" --datagram-bytes "$datagram_bytes" \
  >"$results/publisher.log" 2>&1

status=0
for pid in "${subscriber_pids[@]}"; do wait "$pid" || status=$?; done
subscriber_pids=()
for log in "$results"/*.log; do
  grep -E '^(READY|RESULT) ' "$log" || status=1
done
exit "$status"
