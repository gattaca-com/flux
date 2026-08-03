#!/usr/bin/env bash
set -euo pipefail

# Independent bulk-TCP check of the same private links used by the transport
# benchmark. This script is intentionally locked to the four authorized hosts.
readonly PROBE=crates/flux-network/bench/stream_probe.py
readonly REMOTE_PROBE=/home/rocky/flux-stream-probe.py
readonly RESULTS=bench-results/private-link-stream2
readonly SSH_OPTIONS=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)

mkdir -p "$RESULTS"
for host in \
  rocky@solana-node1-lat-fr \
  rocky@solana-node2-lat-fr \
  rocky@solana-node3-lat-fr \
  rocky@solana-node4-lat-fr
do
  scp "${SSH_OPTIONS[@]}" "$PROBE" "$host:$REMOTE_PROBE"
done

run_one() {
  local name=$1
  local server_host=$2
  local server_ip=$3
  local client_host=$4
  local port=$5
  local cpu=${6:-2}
  local server_pid

  ssh "${SSH_OPTIONS[@]}" "$server_host" \
    timeout 20s taskset -c "$cpu" python3 "$REMOTE_PROBE" server \
      --bind "$server_ip" --port "$port" \
    >"$RESULTS/$name-server.log" 2>&1 &
  server_pid=$!
  for _ in {1..100}; do
    if grep -q '^READY ' "$RESULTS/$name-server.log" 2>/dev/null; then break; fi
    sleep 0.05
  done
  grep -q '^READY ' "$RESULTS/$name-server.log"

  ssh "${SSH_OPTIONS[@]}" "$client_host" \
    timeout 20s taskset -c "$cpu" python3 "$REMOTE_PROBE" client \
      --target "$server_ip" --port "$port" --duration 10 \
    >"$RESULTS/$name-client.log" 2>&1
  wait "$server_pid"
  grep '^RESULT ' "$RESULTS/$name-client.log"
  grep '^RESULT ' "$RESULTS/$name-server.log"
}

run_parallel_four() {
  local -a cpus=(0 1 3 5)
  local -a server_pids=()
  local -a client_pids=()
  local index
  local port

  for index in 0 1 2 3; do
    port=$((52110 + index))
    ssh "${SSH_OPTIONS[@]}" rocky@solana-node2-lat-fr \
      timeout 20s taskset -c "${cpus[index]}" python3 "$REMOTE_PROBE" server \
        --bind 10.9.0.2 --port "$port" \
      >"$RESULTS/parallel-$index-server.log" 2>&1 &
    server_pids+=("$!")
  done
  for _ in {1..100}; do
    local ready=0
    for index in 0 1 2 3; do
      grep -q '^READY ' "$RESULTS/parallel-$index-server.log" 2>/dev/null && ready=$((ready + 1))
    done
    if ((ready == 4)); then break; fi
    sleep 0.05
  done
  ((ready == 4))

  for index in 0 1 2 3; do
    port=$((52110 + index))
    ssh "${SSH_OPTIONS[@]}" rocky@solana-node1-lat-fr \
      timeout 20s taskset -c "${cpus[index]}" python3 "$REMOTE_PROBE" client \
        --target 10.9.0.2 --port "$port" --duration 10 \
      >"$RESULTS/parallel-$index-client.log" 2>&1 &
    client_pids+=("$!")
  done
  for index in 0 1 2 3; do wait "${client_pids[index]}"; done
  for index in 0 1 2 3; do wait "${server_pids[index]}"; done
  grep '^RESULT ' "$RESULTS"/parallel-*.log
}

run_one node1-to-node2-cpu0 rocky@solana-node2-lat-fr 10.9.0.2 rocky@solana-node1-lat-fr 52100 0
run_one node1-to-node2-cpu3 rocky@solana-node2-lat-fr 10.9.0.2 rocky@solana-node1-lat-fr 52102 3
run_parallel_four
