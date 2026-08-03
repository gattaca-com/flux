#!/usr/bin/env bash
set -euo pipefail

# Targeted Flux Profiler capture for the fixed four-node lab. By default the
# publisher is profiled; BENCH_PROFILE_TARGET=subscriber-1 selects node2 (and
# likewise for subscriber-2/node3 and subscriber-3/node4). The other processes
# remain enabled but unprofiled.

if (($# != 5)); then
  echo "usage: $0 <tcp|udp> <payload-bytes> <offered-rate> <data-port> <case-name>" >&2
  exit 2
fi

transport=$1
payload=$2
rate=$3
data_port=$4
case_name=$5

publisher_host=rocky@solana-node1-lat-fr
publisher_ip=10.9.0.1
subscriber_hosts=(
  rocky@solana-node2-lat-fr
  rocky@solana-node3-lat-fr
  rocky@solana-node4-lat-fr
)
subscriber_ips=(10.9.0.2 10.9.0.3 10.9.0.4)
remote_bench=/home/rocky/flux-transport-bench-profile
remote_profiler=/home/rocky/flux-profiler
remote_trace=/home/rocky/flux-profile-${case_name}.fxt
remote_log=/home/rocky/flux-profile-${case_name}-publisher.log
results=${BENCH_PROFILE_RESULTS:-bench-results/profiles}
capture_secs=${BENCH_PROFILE_CAPTURE_SECS:-8}
warmup_secs=${BENCH_PROFILE_WARMUP_SECS:-3}
duration_secs=${BENCH_PROFILE_DURATION_SECS:-10}
drain_secs=${BENCH_PROFILE_DRAIN_SECS:-1}
cpu=${BENCH_CPU:-2}
udp_batch_size=${BENCH_UDP_SEND_BATCH_SIZE:-1}
udp_batch_delay_us=${BENCH_UDP_BATCH_DELAY_US:-50}
udp_gso=${BENCH_UDP_GSO:-0}
udp_gso_copy=${BENCH_UDP_GSO_COPY:-0}
udp_gro=${BENCH_UDP_GRO:-0}
udp_multicast=${BENCH_UDP_MULTICAST:-0}
udp_multicast_group=${BENCH_UDP_MULTICAST_GROUP:-239.255.42.42}
profile_target=${BENCH_PROFILE_TARGET:-publisher}

case "$profile_target" in
  publisher|subscriber-1|subscriber-2|subscriber-3) ;;
  *) echo "BENCH_PROFILE_TARGET must be publisher or subscriber-1..3" >&2; exit 2 ;;
esac

case "$transport" in tcp|udp) ;; *) echo "transport must be tcp or udp" >&2; exit 2 ;; esac
[[ $udp_multicast =~ ^[01]$ ]] || { echo "BENCH_UDP_MULTICAST must be 0 or 1" >&2; exit 2; }
if [[ $transport != udp && $udp_multicast == 1 ]]; then
  echo "multicast profiling requires UDP" >&2
  exit 2
fi
[[ $payload =~ ^[0-9]+$ && $rate =~ ^[0-9]+$ && $data_port =~ ^[0-9]+$ ]] || {
  echo "payload, rate and port must be integers" >&2
  exit 2
}

case_dir=$results/$case_name
mkdir -p "$case_dir"

printf -v bench_command '%q ' \
  timeout 60s taskset -c "$cpu" "$remote_bench" publisher \
  --transport "$transport" --bind "0.0.0.0:$data_port" --subscribers 3 \
  --payload-bytes "$payload" --rate "$rate" --burst 1 \
  --udp-send-batch-size "$udp_batch_size" \
  --udp-send-batch-delay-us "$udp_batch_delay_us" \
  --warmup-secs "$warmup_secs" --duration-secs "$duration_secs" \
  --drain-secs "$drain_secs" --connect-timeout-secs 30 \
  --interface eno2.2135 --label solana-node1-lat-fr
if [[ $udp_gso == 1 ]]; then
  bench_command+='--udp-gso '
fi
if [[ $udp_gso_copy == 1 ]]; then
  bench_command+='--udp-gso-copy '
fi
if [[ $udp_multicast == 1 ]]; then
  printf -v multicast_options '%q ' \
    --udp-multicast-group "$udp_multicast_group:$data_port" \
    --udp-multicast-interface "$publisher_ip"
  bench_command+="$multicast_options"
fi

if [[ $profile_target == publisher ]]; then
  bench_command+="--profile-name flux-$case_name "
  # `timeout` remains the shell child while taskset execs the benchmark beneath
  # it, so its pid is not the profiler producer pid. This host has exactly one
  # enabled producer during a capture; let the profiler resolve it by app name.
  remote_script="set -u; $bench_command > $remote_log 2>&1 & publisher_pid=\$!; profile_status=1; for attempt in \$(seq 1 100); do if $remote_profiler --duration ${capture_secs}s --filter-short-frames 2us --summary --out $remote_trace; then profile_status=0; break; fi; kill -0 \$publisher_pid 2>/dev/null || break; sleep 0.05; done; wait \$publisher_pid; publisher_status=\$?; cat $remote_log; if ((publisher_status != 0 || profile_status != 0)); then exit 1; fi"
  printf -v publisher_command 'bash -lc %q' "$remote_script"
else
  publisher_command=$bench_command
fi

ssh "$publisher_host" "$publisher_command" >"$case_dir/publisher-and-profiler.log" 2>&1 &
publisher_ssh_pid=$!
sleep 0.5

subscriber_pids=()
subscriber_index=0
for host in "${subscriber_hosts[@]}"; do
  subscriber_index=$((subscriber_index + 1))
  label=${host#*@}
  printf -v subscriber_command '%q ' \
    timeout 60s taskset -c "$cpu" "$remote_bench" subscriber \
    --transport "$transport" --publisher "$publisher_ip:$data_port" \
    --timeout-secs 60 --drain-secs "$drain_secs" \
    --interface eno2.2135 --label "$label"
  if [[ $udp_gro == 1 ]]; then
    subscriber_command+='--udp-gro '
  fi
  if [[ $udp_multicast == 1 ]]; then
    printf -v multicast_options '%q ' \
      --udp-multicast-group "$udp_multicast_group:$data_port" \
      --udp-multicast-interface "${subscriber_ips[subscriber_index - 1]}"
    subscriber_command+="$multicast_options"
  fi
  if [[ $profile_target == subscriber-$subscriber_index ]]; then
    subscriber_command+="--profile-name flux-$case_name "
    remote_log=/home/rocky/flux-profile-${case_name}-subscriber.log
    profile_delay=${BENCH_PROFILE_START_DELAY_SECS:-3}
    remote_script="set -u; $subscriber_command > $remote_log 2>&1 & subscriber_pid=\$!; sleep $profile_delay; profile_status=1; for attempt in \$(seq 1 100); do if $remote_profiler --duration ${capture_secs}s --filter-short-frames 2us --summary --out $remote_trace; then profile_status=0; break; fi; kill -0 \$subscriber_pid 2>/dev/null || break; sleep 0.05; done; wait \$subscriber_pid; subscriber_status=\$?; cat $remote_log; if ((subscriber_status != 0 || profile_status != 0)); then exit 1; fi"
    printf -v subscriber_command 'bash -lc %q' "$remote_script"
  fi
  ssh "$host" "$subscriber_command" >"$case_dir/subscriber-$subscriber_index.log" 2>&1 &
  subscriber_pids+=("$!")
done

status=0
wait "$publisher_ssh_pid" || status=$?
for pid in "${subscriber_pids[@]}"; do
  wait "$pid" || status=$?
done
if ((status != 0)); then
  echo "profile case $case_name failed; see $case_dir" >&2
  exit "$status"
fi

trace_host=$publisher_host
if [[ $profile_target == subscriber-1 ]]; then trace_host=${subscriber_hosts[0]}; fi
if [[ $profile_target == subscriber-2 ]]; then trace_host=${subscriber_hosts[1]}; fi
if [[ $profile_target == subscriber-3 ]]; then trace_host=${subscriber_hosts[2]}; fi
scp "$trace_host:$remote_trace" "$case_dir/trace.fxt"
grep -E '^(PROFILE_|RESULT )' "$case_dir"/*.log || true
