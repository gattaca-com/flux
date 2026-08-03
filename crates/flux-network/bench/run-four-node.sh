#!/usr/bin/env bash
set -euo pipefail

# This runner is intentionally locked to the four authorized benchmark hosts.
readonly PUBLISHER_HOST="rocky@solana-node1-lat-fr"
readonly SUBSCRIBER_HOSTS=(
  "rocky@solana-node2-lat-fr"
  "rocky@solana-node3-lat-fr"
  "rocky@solana-node4-lat-fr"
)
readonly SUBSCRIBER_IPS=("10.9.0.2" "10.9.0.3" "10.9.0.4")
readonly ALL_HOSTS=("$PUBLISHER_HOST" "${SUBSCRIBER_HOSTS[@]}")
readonly SSH_OPTIONS=(
  -o BatchMode=yes
  -o ConnectTimeout=10
  -o ServerAliveInterval=15
  -o StrictHostKeyChecking=accept-new
)

readonly BENCH_BINARY="${BENCH_BINARY:-target/release/examples/transport_bench}"
readonly REMOTE_BINARY="/home/rocky/flux-transport-bench-current"
readonly BENCH_CPU="${BENCH_CPU:-2}"
readonly BENCH_SOCKET_BUFFER_BYTES="${BENCH_SOCKET_BUFFER_BYTES:-67108864}"
readonly BENCH_REALTIME_PRIORITY="${BENCH_REALTIME_PRIORITY:-0}"
readonly BENCH_PAYLOADS="${BENCH_PAYLOADS:-64 1232 4096}"
readonly BENCH_RATES="${BENCH_RATES:-50000 100000 200000 300000}"
readonly BENCH_BURSTS="${BENCH_BURSTS:-1 32}"
readonly BENCH_TRANSPORTS="${BENCH_TRANSPORTS:-tcp udp}"
readonly BENCH_UDP_BATCH_SIZES="${BENCH_UDP_BATCH_SIZES:-1}"
readonly BENCH_UDP_GSO_MODES="${BENCH_UDP_GSO_MODES:-0}"
# Optional whitespace-separated batch:gso pairs, for example "1:0 4:1".
# When set, this replaces the batch-size/GSO cross product.
readonly BENCH_UDP_CONFIGS="${BENCH_UDP_CONFIGS:-}"
# Optional explicit cases. Entries may be payload:rate:batch:gso (UDP),
# transport:payload:rate:batch:gso, or the latter plus :repair-delay-ms,
# :gro (0 or 1), :subscribers (1 to 3), :adaptive-batching (0 or 1),
# :gso-copy (0 or 1), :multicast (0 or 1), :batch-delay-us, and
# :control-poll-interval-us, and :data-poll-interval-us. A zero control
# interval preserves the combined per-iteration polling path. A nonzero data
# interval paces empty recvmmsg probes while the subscriber remains busy.
# When set, this replaces the payload/rate matrix while retaining repetitions
# and order.
readonly BENCH_CASES="${BENCH_CASES:-}"
readonly BENCH_UDP_BATCH_DELAY_US="${BENCH_UDP_BATCH_DELAY_US:-50}"
readonly BENCH_UDP_REPAIR_DELAY_MS="${BENCH_UDP_REPAIR_DELAY_MS:-1}"
readonly BENCH_UDP_GRO="${BENCH_UDP_GRO:-0}"
readonly BENCH_SUBSCRIBERS="${BENCH_SUBSCRIBERS:-3}"
readonly BENCH_UDP_ADAPTIVE_BATCHING="${BENCH_UDP_ADAPTIVE_BATCHING:-0}"
readonly BENCH_UDP_GSO_COPY="${BENCH_UDP_GSO_COPY:-0}"
readonly BENCH_UDP_MULTICAST="${BENCH_UDP_MULTICAST:-0}"
readonly BENCH_UDP_MULTICAST_GROUP="${BENCH_UDP_MULTICAST_GROUP:-239.255.42.42}"
readonly BENCH_UDP_XDP="${BENCH_UDP_XDP:-0}"
readonly BENCH_XDP_MODE="${BENCH_XDP_MODE:-copy}"
readonly BENCH_ALLOW_DEBUG_XDP="${BENCH_ALLOW_DEBUG_XDP:-0}"
readonly BENCH_XDP_INTERFACE="${BENCH_XDP_INTERFACE:-eno2}"
readonly BENCH_XDP_QUEUE="${BENCH_XDP_QUEUE:-2}"
readonly BENCH_XDP_VLAN_ID="${BENCH_XDP_VLAN_ID:-2135}"
readonly BENCH_XDP_RING_SIZE="${BENCH_XDP_RING_SIZE:-4096}"
readonly BENCH_XDP_FRAME_COUNT="${BENCH_XDP_FRAME_COUNT:-8192}"
readonly BENCH_XDP_RSS_QUEUES="${BENCH_XDP_RSS_QUEUES:-0}"
readonly BENCH_PUBLISHER_LOOP="${BENCH_PUBLISHER_LOOP:-spin-poll}"
readonly BENCH_IO_POLL_EVERY="${BENCH_IO_POLL_EVERY:-64}"
readonly BENCH_CONTROL_POLL_INTERVAL_US="${BENCH_CONTROL_POLL_INTERVAL_US:-0}"
readonly BENCH_DATA_POLL_INTERVAL_US="${BENCH_DATA_POLL_INTERVAL_US:-0}"
readonly BENCH_LATENCY_SAMPLE_EVERY="${BENCH_LATENCY_SAMPLE_EVERY:-100}"
readonly BENCH_REPETITIONS="${BENCH_REPETITIONS:-3}"
readonly BENCH_WARMUP_SECS="${BENCH_WARMUP_SECS:-5}"
readonly BENCH_DURATION_SECS="${BENCH_DURATION_SECS:-20}"
readonly BENCH_DRAIN_SECS="${BENCH_DRAIN_SECS:-5}"
readonly BENCH_END_MARKERS="${BENCH_END_MARKERS:-1}"
readonly BENCH_START_PORT="${BENCH_START_PORT:-20000}"
readonly BENCH_TIMEOUT_SLACK_SECS="${BENCH_TIMEOUT_SLACK_SECS:-46}"
readonly BENCH_RESULTS="${BENCH_RESULTS:-bench-results/$(date -u +%Y%m%dT%H%M%SZ)}"
readonly BENCH_INTERFACE="${BENCH_INTERFACE:-eno2.2135}"
readonly BENCH_STABILIZE_IRQS="${BENCH_STABILIZE_IRQS:-0}"
readonly BENCH_IRQ_CPUS="${BENCH_IRQ_CPUS:-4 5}"
readonly BENCH_PROFILE_TARGET="${BENCH_PROFILE_TARGET:-none}"
readonly BENCH_PROFILE_CAPTURE_SECS="${BENCH_PROFILE_CAPTURE_SECS:-5}"
readonly BENCH_PROFILE_FILTER="${BENCH_PROFILE_FILTER:-100ns}"
readonly REMOTE_PROFILER="/home/rocky/flux-profiler"

if [[ ! $BENCH_SUBSCRIBERS =~ ^[1-3]$ || ! $BENCH_UDP_ADAPTIVE_BATCHING =~ ^[01]$ \
  || ! $BENCH_UDP_GSO_COPY =~ ^[01]$ || ! $BENCH_UDP_MULTICAST =~ ^[01]$ \
  || ! $BENCH_UDP_XDP =~ ^[01]$ ]]; then
  echo "BENCH_SUBSCRIBERS must be 1 to 3; adaptive batching, GSO copy, multicast, and XDP must be 0 or 1" >&2
  exit 2
fi
if [[ ! $BENCH_XDP_MODE =~ ^(copy|zero-copy)$ ]]; then
  echo "BENCH_XDP_MODE must be copy or zero-copy" >&2
  exit 2
fi
if [[ ! $BENCH_ALLOW_DEBUG_XDP =~ ^[01]$ ]]; then
  echo "BENCH_ALLOW_DEBUG_XDP must be 0 or 1" >&2
  exit 2
fi
if [[ ! $BENCH_XDP_QUEUE =~ ^[0-9]+$ || ! $BENCH_XDP_VLAN_ID =~ ^[0-9]+$ \
  || ! $BENCH_XDP_RSS_QUEUES =~ ^[0-9]+$ \
  || ! $BENCH_XDP_RING_SIZE =~ ^[1-9][0-9]*$ || ! $BENCH_XDP_FRAME_COUNT =~ ^[1-9][0-9]*$ ]]; then
  echo "XDP queue, VLAN, ring size, and frame count must be nonnegative integers" >&2
  exit 2
fi
if [[ ! $BENCH_REALTIME_PRIORITY =~ ^([0-9]|[1-9][0-9])$ ]]; then
  echo "BENCH_REALTIME_PRIORITY must be 0 (disabled) or 1 to 99" >&2
  exit 2
fi
if [[ ! $BENCH_LATENCY_SAMPLE_EVERY =~ ^[0-9]+$ \
  || ! $BENCH_CONTROL_POLL_INTERVAL_US =~ ^[0-9]+$ \
  || ! $BENCH_DATA_POLL_INTERVAL_US =~ ^[0-9]+$ \
  || ! $BENCH_END_MARKERS =~ ^[1-9][0-9]*$ ]]; then
  echo "latency sampling and control poll interval must be nonnegative integers" >&2
  exit 2
fi
if [[ ! $BENCH_SOCKET_BUFFER_BYTES =~ ^[1-9][0-9]*$ ]]; then
  echo "BENCH_SOCKET_BUFFER_BYTES must be a positive integer" >&2
  exit 2
fi
if [[ ! $BENCH_PROFILE_TARGET =~ ^(none|publisher|subscriber-[1-3])$ ]]; then
  echo "BENCH_PROFILE_TARGET must be none, publisher, or subscriber-1..3" >&2
  exit 2
fi
if [[ ! $BENCH_PROFILE_CAPTURE_SECS =~ ^[1-9][0-9]*$ ]]; then
  echo "BENCH_PROFILE_CAPTURE_SECS must be a positive integer" >&2
  exit 2
fi

mkdir -p "$BENCH_RESULTS/environment" "$BENCH_RESULTS/runs"

for host in "${ALL_HOSTS[@]}"; do
  ssh "${SSH_OPTIONS[@]}" "$host" true
  read -r host_rmem_max host_wmem_max < <(
    ssh "${SSH_OPTIONS[@]}" "$host" \
      "/usr/sbin/sysctl -n net.core.rmem_max net.core.wmem_max" | xargs
  )
  if ((host_rmem_max < BENCH_SOCKET_BUFFER_BYTES || host_wmem_max < BENCH_SOCKET_BUFFER_BYTES)); then
    echo "$host socket limits rmem=$host_rmem_max wmem=$host_wmem_max are below BENCH_SOCKET_BUFFER_BYTES=$BENCH_SOCKET_BUFFER_BYTES" >&2
    echo "Raise net.core.rmem_max and net.core.wmem_max before benchmarking; the kernel otherwise silently clamps the socket buffers" >&2
    exit 2
  fi
done

if [[ $BENCH_UDP_XDP == 1 && $BENCH_XDP_MODE == zero-copy ]]; then
  publisher_kernel=$(ssh "${SSH_OPTIONS[@]}" "$PUBLISHER_HOST" uname -r)
  if [[ $publisher_kernel == *debug* && $BENCH_ALLOW_DEBUG_XDP != 1 ]]; then
    echo "Refusing zero-copy XDP on debug kernel $publisher_kernel; ixgbe hit a KASAN slab-out-of-bounds during XSK bind" >&2
    exit 2
  fi
fi

irq_state_tag="flux-bench-irqs-$$"
stabilized_hosts=()
active_run=0

cleanup_remote_benchmarks() {
  [[ $active_run == 1 ]] || return 0
  for host in "${ALL_HOSTS[@]}"; do
    ssh "${SSH_OPTIONS[@]}" "$host" bash -s <<'REMOTE_CLEANUP' || true
set -u
mapfile -t pids < <(pgrep -f '^/home/rocky/flux-transport-bench-current (publisher|subscriber) ' || true)
((${#pids[@]} == 0)) || kill "${pids[@]}"
REMOTE_CLEANUP
  done
  active_run=0
}

restore_irqs() {
  local status=$?
  trap - EXIT INT TERM
  cleanup_remote_benchmarks
  for host in "${stabilized_hosts[@]}"; do
    ssh "${SSH_OPTIONS[@]}" "$host" bash -s -- "$irq_state_tag" <<'REMOTE_RESTORE' || true
set -u
state=/tmp/$1
[[ -r $state ]] || exit 0
while read -r kind first second fourth; do
  case "$kind" in
    irq)
      irq=$first
      if [[ ! -e /proc/irq/$irq/smp_affinity_list && -n ${fourth:-} ]]; then
        irq=$(awk -v label="$fourth" '$NF == label {sub(":", "", $1); print $1; exit}' /proc/interrupts)
      fi
      [[ -n $irq && -e /proc/irq/$irq/smp_affinity_list ]] && \
        printf '%s\n' "$second" | sudo -n tee "/proc/irq/$irq/smp_affinity_list" >/dev/null
      ;;
    irqbalance) [[ $first == active ]] && sudo -n systemctl start irqbalance ;;
  esac
done <"$state"
rm -f "$state"
REMOTE_RESTORE
  done
  exit "$status"
}
trap restore_irqs EXIT INT TERM

if [[ $BENCH_STABILIZE_IRQS == 1 ]]; then
  for host in "${ALL_HOSTS[@]}"; do
    stabilized_hosts+=("$host")
    ssh "${SSH_OPTIONS[@]}" "$host" bash -s -- \
      "$irq_state_tag" "$BENCH_CPU" "$BENCH_IRQ_CPUS" <<'REMOTE_PREPARE'
set -euo pipefail
state=/tmp/$1
bench_cpu=$2
read -r -a target_cpus <<<"$3"
sudo -n true
: >"$state"
irqbalance_state=$(systemctl is-active irqbalance 2>/dev/null || true)
printf 'irqbalance %s -\n' "$irqbalance_state" >>"$state"
if [[ $irqbalance_state == active ]]; then sudo -n systemctl stop irqbalance; fi
mapfile -t irqs < <(awk '/eno2-TxRx/ {sub(":", "", $1); print $1}' /proc/interrupts)
for index in "${!irqs[@]}"; do
  irq=${irqs[index]}
  original=$(<"/proc/irq/$irq/smp_affinity_list")
  label=$(awk -v irq="$irq:" '$1 == irq {print $NF; exit}' /proc/interrupts)
  printf 'irq %s %s %s\n' "$irq" "$original" "$label" >>"$state"
  target=${target_cpus[index % ${#target_cpus[@]}]}
  if [[ $target == "$bench_cpu" ]]; then
    echo "BENCH_IRQ_CPUS must not include BENCH_CPU ($bench_cpu)" >&2
    exit 2
  fi
  printf '%s\n' "$target" | sudo -n tee "/proc/irq/$irq/smp_affinity_list" >/dev/null
done
REMOTE_PREPARE
  done
fi

repin_host_irqs() {
  local host=$1
  ssh "${SSH_OPTIONS[@]}" "$host" bash -s -- "$BENCH_CPU" "$BENCH_IRQ_CPUS" <<'REMOTE_REPIN'
set -euo pipefail
bench_cpu=$1
read -r -a target_cpus <<<"$2"
mapfile -t irqs < <(awk '/eno2-TxRx/ {sub(":", "", $1); print $1}' /proc/interrupts)
if ((${#irqs[@]} == 0)); then
  echo "no eno2 TxRx IRQs found after XDP activation" >&2
  exit 1
fi
for index in "${!irqs[@]}"; do
  irq=${irqs[index]}
  target=${target_cpus[index % ${#target_cpus[@]}]}
  if [[ $target == "$bench_cpu" ]]; then
    echo "BENCH_IRQ_CPUS must not include BENCH_CPU ($bench_cpu)" >&2
    exit 2
  fi
  printf '%s\n' "$target" | sudo -n tee "/proc/irq/$irq/smp_affinity_list" >/dev/null
done
REMOTE_REPIN
}

if [[ $BENCH_PROFILE_TARGET == none ]]; then
  cargo build --release -p flux-network --example transport_bench
else
  cargo build --release -p flux-network --example transport_bench --features profiling
  cargo build --release -p flux-profiler --bin flux-profiler
fi
for host in "${ALL_HOSTS[@]}"; do
  scp "${SSH_OPTIONS[@]}" "$BENCH_BINARY" "$host:$REMOTE_BINARY"
  ssh "${SSH_OPTIONS[@]}" "$host" chmod 0755 "$REMOTE_BINARY"
  if [[ $BENCH_PROFILE_TARGET != none ]]; then
    scp "${SSH_OPTIONS[@]}" target/release/flux-profiler "$host:$REMOTE_PROFILER"
    ssh "${SSH_OPTIONS[@]}" "$host" chmod 0755 "$REMOTE_PROFILER"
  fi
done

publisher_ip=${BENCH_PUBLISHER_IP:-$(ssh "${SSH_OPTIONS[@]}" "$PUBLISHER_HOST" \
  "hostname -I | tr ' ' '\n' | awk '/^10\\.9\\.0\\./ {print; exit}'")}
if [[ ! "$publisher_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Could not determine a usable publisher IPv4 address: $publisher_ip" >&2
  exit 1
fi

xdp_interface_index=""
xdp_source_mac=""
subscriber_xdp_macs=()
if [[ $BENCH_UDP_XDP == 1 ]]; then
  xdp_interface_index=$(ssh "${SSH_OPTIONS[@]}" "$PUBLISHER_HOST" \
    "cat /sys/class/net/$BENCH_XDP_INTERFACE/ifindex")
  xdp_source_mac=$(ssh "${SSH_OPTIONS[@]}" "$PUBLISHER_HOST" \
    "cat /sys/class/net/$BENCH_XDP_INTERFACE/address")
  if [[ ! $xdp_interface_index =~ ^[1-9][0-9]*$ \
    || ! $xdp_source_mac =~ ^([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}$ ]]; then
    echo "Could not resolve AF_XDP interface metadata for $BENCH_XDP_INTERFACE" >&2
    exit 1
  fi
  ssh "${SSH_OPTIONS[@]}" "$PUBLISHER_HOST" \
    "test -d /sys/class/net/$BENCH_XDP_INTERFACE/queues/tx-$BENCH_XDP_QUEUE"
  for host in "${SUBSCRIBER_HOSTS[@]}"; do
    destination_mac=$(ssh "${SSH_OPTIONS[@]}" "$host" \
      "cat /sys/class/net/$BENCH_XDP_INTERFACE/address")
    if [[ ! $destination_mac =~ ^([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}$ ]]; then
      echo "Could not resolve AF_XDP destination MAC for $host" >&2
      exit 1
    fi
    subscriber_xdp_macs+=("$destination_mac")
  done
fi

declare -A node_interfaces
for host in "${ALL_HOSTS[@]}"; do
  node_interfaces["$host"]=$BENCH_INTERFACE
done

for host in "${ALL_HOSTS[@]}"; do
  safe_name=${host#*@}
  interface=${node_interfaces[$host]}
  ssh "${SSH_OPTIONS[@]}" "$host" bash -s -- "$interface" >"$BENCH_RESULTS/environment/$safe_name.txt" <<'REMOTE_ENV'
set -u
interface=$1
hostname
uname -a
lscpu
hostname -I
cat /proc/net/route
/usr/sbin/ip -brief address 2>&1 || true
/usr/sbin/ip -details -statistics link show dev "$interface" 2>&1 || true
if command -v ethtool >/dev/null 2>&1; then
  ethtool -i "$interface" 2>&1 || true
  ethtool -k "$interface" 2>&1 || true
  ethtool -g "$interface" 2>&1 || true
fi
/usr/sbin/tc -s qdisc show dev "$interface" 2>&1 || true
/usr/sbin/sysctl net.core.rmem_max net.core.wmem_max net.core.netdev_max_backlog \
  net.ipv4.udp_rmem_min net.ipv4.udp_wmem_min \
  net.ipv4.tcp_rmem net.ipv4.tcp_wmem 2>&1 || true
chronyc tracking 2>&1 || timedatectl status 2>&1 || true
cat /proc/interrupts
REMOTE_ENV
done

run_number=0
run_one() {
  local transport=$1
  local payload=$2
  local rate=$3
  local burst=$4
  local repetition=$5
  local udp_batch_size=$6
  local udp_gso=$7
  local udp_repair_delay_ms=$8
  local udp_gro=$9
  local subscriber_count=${10}
  local udp_adaptive_batching=${11}
  local udp_gso_copy=${12}
  local udp_multicast=${13}
  local udp_batch_delay_us=${14}
  local control_poll_interval_us=${15}
  local data_poll_interval_us=${16}
  local use_xdp=0
  if [[ $transport == udp && $BENCH_UDP_XDP == 1 ]]; then
    use_xdp=1
  fi
  local data_port=$((BENCH_START_PORT + run_number * 2))
  local multicast_endpoint="$BENCH_UDP_MULTICAST_GROUP:$data_port"
  local run_name
  local run_dir
  local publisher_interface=${node_interfaces[$PUBLISHER_HOST]}
  if [[ $use_xdp == 1 ]]; then
    publisher_interface=$BENCH_XDP_INTERFACE
  fi
  local total_timeout
  local publisher_command
  local publisher_pid
  local publisher_status
  local subscriber_status=0
  local profile_pid=""
  local profile_status=0
  local profile_host=""
  local profile_remote_trace=""
  local -a subscriber_pids=()

  run_name=$(printf 'r%02d-%s-p%s-rate%s-b%s-loop%s-cp%s-dp%s-rt%s-ub%s-bd%s-gso%s-copy%s-gro%s-mcast%s-xdp%s-%s-s%s-adapt%s-rd%s-rep%s' \
    "$run_number" "$transport" "$payload" "$rate" "$burst" \
    "$BENCH_PUBLISHER_LOOP" "$control_poll_interval_us" "$data_poll_interval_us" \
    "$BENCH_REALTIME_PRIORITY" "$udp_batch_size" \
    "$udp_batch_delay_us" "$udp_gso" \
    "$udp_gso_copy" "$udp_gro" "$udp_multicast" "$use_xdp" "$BENCH_XDP_MODE" "$subscriber_count" \
    "$udp_adaptive_batching" "$udp_repair_delay_ms" "$repetition")
  run_dir="$BENCH_RESULTS/runs/$run_name"
  mkdir -p "$run_dir"
  total_timeout=$(awk -v warmup="$BENCH_WARMUP_SECS" -v duration="$BENCH_DURATION_SECS" \
    -v drain="$BENCH_DRAIN_SECS" -v slack="$BENCH_TIMEOUT_SLACK_SECS" \
    'BEGIN {printf "%d", warmup + duration + drain + slack}')

  local -a scheduling_prefix=(timeout "${total_timeout}s")
  if [[ $BENCH_REALTIME_PRIORITY != 0 ]]; then
    scheduling_prefix+=(sudo -n chrt -f "$BENCH_REALTIME_PRIORITY")
  fi
  local -a publisher_scheduling_prefix=("${scheduling_prefix[@]}")
  if [[ $use_xdp == 1 && $BENCH_REALTIME_PRIORITY == 0 ]]; then
    publisher_scheduling_prefix+=(sudo -n)
  fi
  printf -v publisher_command '%q ' \
    "${publisher_scheduling_prefix[@]}" taskset -c "$BENCH_CPU" "$REMOTE_BINARY" publisher \
    --transport "$transport" --bind "0.0.0.0:$data_port" --subscribers "$subscriber_count" \
    --socket-buffer-bytes "$BENCH_SOCKET_BUFFER_BYTES" \
    --payload-bytes "$payload" --rate "$rate" --burst "$burst" \
    --publisher-loop "$BENCH_PUBLISHER_LOOP" \
    --io-poll-every "$BENCH_IO_POLL_EVERY" \
    --control-poll-interval-us "$control_poll_interval_us" \
    --latency-sample-every "$BENCH_LATENCY_SAMPLE_EVERY" \
    --udp-send-batch-size "$udp_batch_size" \
    --udp-send-batch-delay-us "$udp_batch_delay_us" \
    --warmup-secs "$BENCH_WARMUP_SECS" --duration-secs "$BENCH_DURATION_SECS" \
    --drain-secs "$BENCH_DRAIN_SECS" --end-markers "$BENCH_END_MARKERS" \
    --connect-timeout-secs 60 \
    --interface "$publisher_interface" --label solana-node1-lat-fr
  if [[ $BENCH_PROFILE_TARGET == publisher ]]; then
    local publisher_profile_command
    printf -v publisher_profile_command '%q ' --profile-name "flux-$run_name"
    publisher_command+="$publisher_profile_command"
  fi
  if [[ $udp_gso == 1 ]]; then
    publisher_command+='--udp-gso '
  fi
  if [[ $udp_gso_copy == 1 ]]; then
    publisher_command+='--udp-gso-copy '
  fi
  if [[ $udp_adaptive_batching == 1 ]]; then
    publisher_command+='--udp-adaptive-batching '
  fi
  if [[ $udp_multicast == 1 ]]; then
    local publisher_multicast_command
    printf -v publisher_multicast_command '%q ' \
      --udp-multicast-group "$multicast_endpoint" \
      --udp-multicast-interface "$publisher_ip"
    publisher_command+="$publisher_multicast_command"
  fi
  if [[ $use_xdp == 1 ]]; then
    local publisher_xdp_command
    printf -v publisher_xdp_command '%q ' \
      --udp-xdp-interface-index "$xdp_interface_index" \
      --udp-xdp-source-mac "$xdp_source_mac" \
      --udp-xdp-queue "$BENCH_XDP_QUEUE" \
      --udp-xdp-vlan-id "$BENCH_XDP_VLAN_ID" \
      --udp-xdp-ring-size "$BENCH_XDP_RING_SIZE" \
      --udp-xdp-frame-count "$BENCH_XDP_FRAME_COUNT" \
      --udp-xdp-no-fallback
    publisher_command+="$publisher_xdp_command"
    if [[ $udp_multicast == 0 ]]; then
      local publisher_xdp_unicast_command
      printf -v publisher_xdp_unicast_command '%q ' --udp-xdp-source-ip "$publisher_ip"
      publisher_command+="$publisher_xdp_unicast_command"
      local destination_index
      for ((destination_index = 0; destination_index < subscriber_count; destination_index++)); do
        printf -v publisher_xdp_unicast_command '%q ' \
          --udp-xdp-unicast-destination \
          "${SUBSCRIBER_IPS[destination_index]}:$data_port@${subscriber_xdp_macs[destination_index]}"
        publisher_command+="$publisher_xdp_unicast_command"
      done
    fi
    if [[ $BENCH_XDP_MODE == zero-copy ]]; then
      publisher_command+='--udp-xdp-zero-copy --udp-xdp-attach-pass '
    fi
  fi

  echo "Starting $run_name on port $data_port"
  ssh "${SSH_OPTIONS[@]}" "$PUBLISHER_HOST" "$publisher_command" \
    >"$run_dir/publisher.log" 2>&1 &
  publisher_pid=$!
  active_run=1
  if [[ $use_xdp == 1 ]]; then
    local ready_deadline=$((SECONDS + 30))
    while ! grep -q '^READY ' "$run_dir/publisher.log"; do
      if ! kill -0 "$publisher_pid" 2>/dev/null; then
        echo "$run_name publisher exited before AF_XDP setup completed" >&2
        return 1
      fi
      if ((SECONDS >= ready_deadline)); then
        echo "$run_name timed out waiting for AF_XDP publisher readiness" >&2
        return 1
      fi
      sleep 0.05
    done
    # Native ixgbe XDP attachment recreates the queues and drops link for
    # roughly five seconds. Do not race subscriber TCP control connections
    # against that reconfiguration when XDP is activated at publisher start.
    sleep 12
    if ((BENCH_XDP_RSS_QUEUES > 0)); then
      # ixgbe resets the RSS table while attaching native XDP and can rewrite
      # it again as link comes back. After link settles, keep ordinary RX
      # flows off the TX-only XSK queue, which intentionally has no fill
      # buffers, before any subscriber control connection is created.
      ssh "${SSH_OPTIONS[@]}" "$PUBLISHER_HOST" \
        "sudo -n ethtool -X $BENCH_XDP_INTERFACE equal $BENCH_XDP_RSS_QUEUES"
    fi
  else
    sleep 0.5
  fi

  local subscriber_index
  local host
  for ((subscriber_index = 1; subscriber_index <= subscriber_count; subscriber_index++)); do
    host=${SUBSCRIBER_HOSTS[subscriber_index - 1]}
    local subscriber_interface=${node_interfaces[$host]}
    local subscriber_label=${host#*@}
    local subscriber_command
    printf -v subscriber_command '%q ' \
      "${scheduling_prefix[@]}" taskset -c "$BENCH_CPU" "$REMOTE_BINARY" subscriber \
      --transport "$transport" --publisher "$publisher_ip:$data_port" \
      --timeout-secs "$total_timeout" --drain-secs "$BENCH_DRAIN_SECS" \
      --socket-buffer-bytes "$BENCH_SOCKET_BUFFER_BYTES" \
      --udp-repair-delay-ms "$udp_repair_delay_ms" \
      --control-poll-interval-us "$control_poll_interval_us" \
      --data-poll-interval-us "$data_poll_interval_us" \
      --interface "$subscriber_interface" --label "$subscriber_label"
    if [[ $use_xdp == 1 && $udp_multicast == 0 ]]; then
      local subscriber_bind_command
      printf -v subscriber_bind_command '%q ' \
        --bind "${SUBSCRIBER_IPS[subscriber_index - 1]}:$data_port"
      subscriber_command+="$subscriber_bind_command"
    fi
    if [[ $udp_gro == 1 ]]; then
      subscriber_command+='--udp-gro '
    fi
    if [[ $udp_multicast == 1 ]]; then
      local subscriber_multicast_command
      printf -v subscriber_multicast_command '%q ' \
        --udp-multicast-group "$multicast_endpoint" \
        --udp-multicast-interface "${SUBSCRIBER_IPS[subscriber_index - 1]}"
      subscriber_command+="$subscriber_multicast_command"
    fi
    if [[ $BENCH_PROFILE_TARGET == subscriber-$subscriber_index ]]; then
      local subscriber_profile_command
      printf -v subscriber_profile_command '%q ' --profile-name "flux-$run_name"
      subscriber_command+="$subscriber_profile_command"
    fi
    ssh "${SSH_OPTIONS[@]}" "$host" "$subscriber_command" \
      >"$run_dir/subscriber-$subscriber_index.log" 2>&1 &
    subscriber_pids+=("$!")
  done

  # Wait for every transport and clock-calibration connection, then pin the
  # replacement ixgbe IRQs during warmup. The publisher allows 60 seconds for
  # setup, including the link-settle delay above, so use the same outer bound.
  if [[ ($use_xdp == 1 && $BENCH_STABILIZE_IRQS == 1) || $BENCH_PROFILE_TARGET != none ]]; then
    local activation_deadline=$((SECONDS + 60))
    while ! grep -q '^ACTIVATED ' "$run_dir/publisher.log"; do
      if ! kill -0 "$publisher_pid" 2>/dev/null; then
        echo "$run_name publisher exited before AF_XDP activation" >&2
        return 1
      fi
      for pid in "${subscriber_pids[@]}"; do
        if ! kill -0 "$pid" 2>/dev/null; then
          echo "$run_name subscriber exited before AF_XDP activation" >&2
          return 1
        fi
      done
      if ((SECONDS >= activation_deadline)); then
        echo "$run_name timed out waiting for AF_XDP activation" >&2
        return 1
      fi
      sleep 0.05
    done
    if [[ $use_xdp == 1 && $BENCH_STABILIZE_IRQS == 1 ]]; then
      repin_host_irqs "$PUBLISHER_HOST"
    fi
  fi

  if [[ $BENCH_PROFILE_TARGET != none ]]; then
    profile_host=$PUBLISHER_HOST
    if [[ $BENCH_PROFILE_TARGET == subscriber-* ]]; then
      profile_host=${SUBSCRIBER_HOSTS[${BENCH_PROFILE_TARGET#subscriber-} - 1]}
    fi
    profile_remote_trace="/tmp/flux-profile-$run_name.fxt"
    local profile_command
    local -a profile_prefix=()
    if [[ $BENCH_PROFILE_TARGET == publisher && $use_xdp == 1 ]]; then
      profile_prefix=(sudo -n)
    fi
    printf -v profile_command '%q ' \
      "${profile_prefix[@]}" "$REMOTE_PROFILER" \
      --duration "${BENCH_PROFILE_CAPTURE_SECS}s" \
      --filter-short-frames "$BENCH_PROFILE_FILTER" \
      --summary --out "$profile_remote_trace"
    if ((${#profile_prefix[@]} != 0)); then
      profile_command+="&& sudo -n chmod 0644 $(printf '%q' "$profile_remote_trace")"
    fi
    ssh "${SSH_OPTIONS[@]}" "$profile_host" "$profile_command" \
      >"$run_dir/profile.log" 2>&1 &
    profile_pid=$!
  fi

  set +e
  wait "$publisher_pid"
  publisher_status=$?
  for pid in "${subscriber_pids[@]}"; do
    wait "$pid" || subscriber_status=$?
  done
  if [[ -n $profile_pid ]]; then
    wait "$profile_pid" || profile_status=$?
  fi
  set -e
  active_run=0

  if [[ -n $profile_remote_trace && $profile_status == 0 ]]; then
    scp "${SSH_OPTIONS[@]}" "$profile_host:$profile_remote_trace" "$run_dir/trace.fxt"
  fi

  if ((publisher_status != 0 || subscriber_status != 0 || profile_status != 0)); then
    echo "$run_name failed: publisher=$publisher_status subscriber=$subscriber_status profiler=$profile_status" >&2
    return 1
  fi
  if ! grep -q '^RESULT ' "$run_dir/publisher.log"; then
    echo "$run_name has no publisher result" >&2
    return 1
  fi
  for ((subscriber_index = 1; subscriber_index <= subscriber_count; subscriber_index++)); do
    if ! grep -q '^RESULT ' "$run_dir/subscriber-$subscriber_index.log"; then
      echo "$run_name has no subscriber-$subscriber_index result" >&2
      return 1
    fi
  done
  run_number=$((run_number + 1))
  sleep 1
}

if [[ -n $BENCH_CASES ]]; then
  for ((repetition = 1; repetition <= BENCH_REPETITIONS; repetition++)); do
    read -r -a cases <<<"$BENCH_CASES"
    if ((repetition % 2 == 0)); then
      reversed=()
      for ((index = ${#cases[@]} - 1; index >= 0; index--)); do
        reversed+=("${cases[index]}")
      done
      cases=("${reversed[@]}")
    fi
    for bench_case in "${cases[@]}"; do
      IFS=: read -r -a case_fields <<<"$bench_case"
      subscriber_count=$BENCH_SUBSCRIBERS
      udp_adaptive_batching=$BENCH_UDP_ADAPTIVE_BATCHING
      udp_gso_copy=$BENCH_UDP_GSO_COPY
      udp_multicast=$BENCH_UDP_MULTICAST
      udp_batch_delay_us=$BENCH_UDP_BATCH_DELAY_US
      control_poll_interval_us=$BENCH_CONTROL_POLL_INTERVAL_US
      data_poll_interval_us=$BENCH_DATA_POLL_INTERVAL_US
      if ((${#case_fields[@]} == 4)); then
        transport=udp
        payload=${case_fields[0]}
        rate=${case_fields[1]}
        udp_batch_size=${case_fields[2]}
        udp_gso=${case_fields[3]}
        udp_repair_delay_ms=$BENCH_UDP_REPAIR_DELAY_MS
        udp_gro=$BENCH_UDP_GRO
      elif ((${#case_fields[@]} == 5)); then
        transport=${case_fields[0]}
        payload=${case_fields[1]}
        rate=${case_fields[2]}
        udp_batch_size=${case_fields[3]}
        udp_gso=${case_fields[4]}
        udp_repair_delay_ms=$BENCH_UDP_REPAIR_DELAY_MS
        udp_gro=$BENCH_UDP_GRO
      elif ((${#case_fields[@]} == 6)); then
        transport=${case_fields[0]}
        payload=${case_fields[1]}
        rate=${case_fields[2]}
        udp_batch_size=${case_fields[3]}
        udp_gso=${case_fields[4]}
        udp_repair_delay_ms=${case_fields[5]}
        udp_gro=$BENCH_UDP_GRO
      elif ((${#case_fields[@]} == 7)); then
        transport=${case_fields[0]}
        payload=${case_fields[1]}
        rate=${case_fields[2]}
        udp_batch_size=${case_fields[3]}
        udp_gso=${case_fields[4]}
        udp_repair_delay_ms=${case_fields[5]}
        udp_gro=${case_fields[6]}
      elif ((${#case_fields[@]} == 8)); then
        transport=${case_fields[0]}
        payload=${case_fields[1]}
        rate=${case_fields[2]}
        udp_batch_size=${case_fields[3]}
        udp_gso=${case_fields[4]}
        udp_repair_delay_ms=${case_fields[5]}
        udp_gro=${case_fields[6]}
        subscriber_count=${case_fields[7]}
      elif ((${#case_fields[@]} == 9)); then
        transport=${case_fields[0]}
        payload=${case_fields[1]}
        rate=${case_fields[2]}
        udp_batch_size=${case_fields[3]}
        udp_gso=${case_fields[4]}
        udp_repair_delay_ms=${case_fields[5]}
        udp_gro=${case_fields[6]}
        subscriber_count=${case_fields[7]}
        udp_adaptive_batching=${case_fields[8]}
      elif ((${#case_fields[@]} == 10)); then
        transport=${case_fields[0]}
        payload=${case_fields[1]}
        rate=${case_fields[2]}
        udp_batch_size=${case_fields[3]}
        udp_gso=${case_fields[4]}
        udp_repair_delay_ms=${case_fields[5]}
        udp_gro=${case_fields[6]}
        subscriber_count=${case_fields[7]}
        udp_adaptive_batching=${case_fields[8]}
        udp_gso_copy=${case_fields[9]}
      elif ((${#case_fields[@]} == 11)); then
        transport=${case_fields[0]}
        payload=${case_fields[1]}
        rate=${case_fields[2]}
        udp_batch_size=${case_fields[3]}
        udp_gso=${case_fields[4]}
        udp_repair_delay_ms=${case_fields[5]}
        udp_gro=${case_fields[6]}
        subscriber_count=${case_fields[7]}
        udp_adaptive_batching=${case_fields[8]}
        udp_gso_copy=${case_fields[9]}
        udp_multicast=${case_fields[10]}
      elif ((${#case_fields[@]} == 12)); then
        transport=${case_fields[0]}
        payload=${case_fields[1]}
        rate=${case_fields[2]}
        udp_batch_size=${case_fields[3]}
        udp_gso=${case_fields[4]}
        udp_repair_delay_ms=${case_fields[5]}
        udp_gro=${case_fields[6]}
        subscriber_count=${case_fields[7]}
        udp_adaptive_batching=${case_fields[8]}
        udp_gso_copy=${case_fields[9]}
        udp_multicast=${case_fields[10]}
        udp_batch_delay_us=${case_fields[11]}
      elif ((${#case_fields[@]} == 13)); then
        transport=${case_fields[0]}
        payload=${case_fields[1]}
        rate=${case_fields[2]}
        udp_batch_size=${case_fields[3]}
        udp_gso=${case_fields[4]}
        udp_repair_delay_ms=${case_fields[5]}
        udp_gro=${case_fields[6]}
        subscriber_count=${case_fields[7]}
        udp_adaptive_batching=${case_fields[8]}
        udp_gso_copy=${case_fields[9]}
        udp_multicast=${case_fields[10]}
        udp_batch_delay_us=${case_fields[11]}
        control_poll_interval_us=${case_fields[12]}
      elif ((${#case_fields[@]} == 14)); then
        transport=${case_fields[0]}
        payload=${case_fields[1]}
        rate=${case_fields[2]}
        udp_batch_size=${case_fields[3]}
        udp_gso=${case_fields[4]}
        udp_repair_delay_ms=${case_fields[5]}
        udp_gro=${case_fields[6]}
        subscriber_count=${case_fields[7]}
        udp_adaptive_batching=${case_fields[8]}
        udp_gso_copy=${case_fields[9]}
        udp_multicast=${case_fields[10]}
        udp_batch_delay_us=${case_fields[11]}
        control_poll_interval_us=${case_fields[12]}
        data_poll_interval_us=${case_fields[13]}
      else
        echo "invalid BENCH_CASES entry: $bench_case" >&2
        exit 2
      fi
      if [[ $transport == tcp ]]; then
        udp_multicast=0
      fi
      if [[ ! $transport =~ ^(tcp|udp)$ || ! $payload =~ ^[0-9]+$ || ! $rate =~ ^[0-9]+$ \
        || ! $udp_batch_size =~ ^[0-9]+$ || ! $udp_gso =~ ^[01]$ \
        || ! $udp_repair_delay_ms =~ ^[1-9][0-9]*$ || ! $udp_gro =~ ^[01]$ \
        || ! $subscriber_count =~ ^[1-3]$ || ! $udp_adaptive_batching =~ ^[01]$ \
        || ! $udp_gso_copy =~ ^[01]$ || ! $udp_multicast =~ ^[01]$ \
        || ! $udp_batch_delay_us =~ ^[1-9][0-9]*$ \
        || ! $control_poll_interval_us =~ ^[0-9]+$ \
        || ! $data_poll_interval_us =~ ^[0-9]+$ ]]; then
        echo "invalid BENCH_CASES entry: $bench_case" >&2
        exit 2
      fi
      run_one "$transport" "$payload" "$rate" 1 "$repetition" "$udp_batch_size" "$udp_gso" \
        "$udp_repair_delay_ms" "$udp_gro" "$subscriber_count" "$udp_adaptive_batching" \
        "$udp_gso_copy" "$udp_multicast" "$udp_batch_delay_us" "$control_poll_interval_us" \
        "$data_poll_interval_us"
    done
  done
  echo "Benchmark complete: $BENCH_RESULTS"
  exit 0
fi

for ((repetition = 1; repetition <= BENCH_REPETITIONS; repetition++)); do
  read -r -a transports <<<"$BENCH_TRANSPORTS"
  if ((repetition % 2 == 1)); then
    :
  else
    reversed=()
    for ((index = ${#transports[@]} - 1; index >= 0; index--)); do
      reversed+=("${transports[index]}")
    done
    transports=("${reversed[@]}")
  fi
  for payload in $BENCH_PAYLOADS; do
    for rate in $BENCH_RATES; do
      for burst in $BENCH_BURSTS; do
        for transport in "${transports[@]}"; do
          if [[ $transport == udp ]]; then
            if [[ -n $BENCH_UDP_CONFIGS ]]; then
              for udp_config in $BENCH_UDP_CONFIGS; do
                IFS=: read -r udp_batch_size udp_gso <<<"$udp_config"
                if [[ ! $udp_batch_size =~ ^[0-9]+$ || ! $udp_gso =~ ^[01]$ ]]; then
                  echo "invalid BENCH_UDP_CONFIGS entry: $udp_config" >&2
                  exit 2
                fi
                run_one "$transport" "$payload" "$rate" "$burst" "$repetition" \
                  "$udp_batch_size" "$udp_gso" "$BENCH_UDP_REPAIR_DELAY_MS" "$BENCH_UDP_GRO" \
                  "$BENCH_SUBSCRIBERS" "$BENCH_UDP_ADAPTIVE_BATCHING" "$BENCH_UDP_GSO_COPY" \
                  "$BENCH_UDP_MULTICAST" "$BENCH_UDP_BATCH_DELAY_US" \
                  "$BENCH_CONTROL_POLL_INTERVAL_US" "$BENCH_DATA_POLL_INTERVAL_US"
              done
            else
              for udp_batch_size in $BENCH_UDP_BATCH_SIZES; do
                for udp_gso in $BENCH_UDP_GSO_MODES; do
                  run_one "$transport" "$payload" "$rate" "$burst" "$repetition" \
                    "$udp_batch_size" "$udp_gso" "$BENCH_UDP_REPAIR_DELAY_MS" "$BENCH_UDP_GRO" \
                    "$BENCH_SUBSCRIBERS" "$BENCH_UDP_ADAPTIVE_BATCHING" "$BENCH_UDP_GSO_COPY" \
                    "$BENCH_UDP_MULTICAST" "$BENCH_UDP_BATCH_DELAY_US" \
                    "$BENCH_CONTROL_POLL_INTERVAL_US" "$BENCH_DATA_POLL_INTERVAL_US"
                done
              done
            fi
          else
            run_one "$transport" "$payload" "$rate" "$burst" "$repetition" 1 0 \
              "$BENCH_UDP_REPAIR_DELAY_MS" "$BENCH_UDP_GRO" "$BENCH_SUBSCRIBERS" \
              "$BENCH_UDP_ADAPTIVE_BATCHING" "$BENCH_UDP_GSO_COPY" 0 \
              "$BENCH_UDP_BATCH_DELAY_US" "$BENCH_CONTROL_POLL_INTERVAL_US" \
              "$BENCH_DATA_POLL_INTERVAL_US"
          fi
        done
      done
    done
  done
done

echo "Benchmark complete: $BENCH_RESULTS"
