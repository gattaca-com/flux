#!/usr/bin/env python3
"""Summarize RESULT lines produced by run-four-node.sh as one CSV row per run."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from pathlib import Path
from typing import Any


FIELDS = [
    "run",
    "status",
    "transport",
    "payload_bytes",
    "target_rate_pps",
    "burst",
    "subscribers_requested",
    "publisher_loop",
    "control_poll_interval_us",
    "data_poll_interval_us",
    "publisher_io_poll_calls",
    "udp_repair_delay_ms",
    "udp_gro",
    "udp_send_batch_size",
    "udp_send_batch_delay_us",
    "udp_adaptive_batching",
    "udp_gso",
    "udp_gso_copy",
    "udp_multicast",
    "udp_multicast_group",
    "udp_xdp_requested",
    "udp_xdp_active",
    "udp_xdp_queue",
    "udp_xdp_vlan_id",
    "udp_xdp_zero_copy",
    "udp_xdp_attach_pass",
    "publisher_pps",
    "publisher_cpu_core_fraction",
    "publisher_core_irq_fraction",
    "publisher_process_plus_irq_fraction",
    "publisher_softirq_timer",
    "publisher_softirq_net_tx",
    "publisher_softirq_net_rx",
    "publisher_softirq_sched",
    "publisher_softirq_rcu",
    "publisher_min_active",
    "publisher_late_bursts",
    "subscriber_min_pps",
    "subscriber_mean_pps",
    "subscriber_max_loss_fraction",
    "subscriber_total_missing",
    "subscriber_max_out_of_order",
    "subscriber_max_cpu_seconds_including_drain",
    "subscriber_max_softirq_timer_including_drain",
    "subscriber_max_softirq_net_tx_including_drain",
    "subscriber_max_softirq_net_rx_including_drain",
    "subscriber_max_softirq_sched_including_drain",
    "subscriber_max_softirq_rcu_including_drain",
    "latency_max_p50_us",
    "latency_max_p99_us",
    "latency_max_p99_9_us",
    "clock_max_uncertainty_us",
    "kernel_drop_total",
    "tcp_retransmits_publisher",
    "udp_publication_flushes",
    "udp_max_publications_per_flush",
    "udp_immediate_flushes",
    "udp_adaptive_immediate_flushes",
    "udp_adaptive_batch_activations",
    "udp_adaptive_idle_resets",
    "udp_full_batch_flushes",
    "udp_deadline_flushes",
    "udp_explicit_flushes",
    "udp_mean_batch_dwell_us",
    "udp_max_batch_dwell_us",
    "udp_sendmmsg_calls",
    "udp_mean_sendmmsg_us",
    "udp_max_sendmmsg_us",
    "udp_sendmmsg_would_block",
    "udp_send_entries",
    "udp_wire_datagrams",
    "udp_xdp_enqueued_datagrams",
    "udp_xdp_completed_datagrams",
    "udp_xdp_ring_full_drops",
    "udp_xdp_frame_exhaustion_drops",
    "udp_xdp_kick_calls",
    "udp_xdp_kick_errors",
    "udp_xdp_wire_bytes",
    "udp_subscriber_min_datagrams",
    "udp_subscriber_min_socket_messages",
    "udp_subscriber_mean_datagrams_per_nonempty_recvmmsg",
    "udp_subscriber_max_recvmmsg_calls",
    "udp_subscriber_max_empty_recvmmsg_fraction",
    "udp_subscriber_max_combined_poll_calls",
    "udp_subscriber_max_control_poll_calls",
    "udp_subscriber_max_data_poll_calls",
    "udp_subscriber_max_recvmmsg_batch",
    "udp_subscriber_total_gro_packets",
    "udp_subscriber_total_gro_segments",
    "udp_subscriber_max_gro_segments",
    "udp_subscriber_max_repair_requests",
    "udp_subscriber_max_repair_deliveries",
    "udp_subscriber_total_repair_deliveries",
]

COMPACT_FIELDS = [
    "run",
    "status",
    "payload_bytes",
    "target_rate_pps",
    "subscribers_requested",
    "publisher_loop",
    "control_poll_interval_us",
    "data_poll_interval_us",
    "publisher_io_poll_calls",
    "udp_repair_delay_ms",
    "udp_gro",
    "udp_send_batch_size",
    "udp_adaptive_batching",
    "udp_gso",
    "udp_gso_copy",
    "udp_multicast",
    "publisher_pps",
    "publisher_cpu_core_fraction",
    "publisher_core_irq_fraction",
    "publisher_process_plus_irq_fraction",
    "publisher_softirq_net_tx",
    "publisher_softirq_net_rx",
    "subscriber_min_pps",
    "subscriber_max_loss_fraction",
    "subscriber_max_out_of_order",
    "latency_max_p50_us",
    "latency_max_p99_us",
    "subscriber_max_softirq_net_rx_including_drain",
    "udp_max_publications_per_flush",
    "udp_full_batch_flushes",
    "udp_deadline_flushes",
    "udp_mean_sendmmsg_us",
    "udp_max_sendmmsg_us",
    "udp_mean_batch_dwell_us",
    "udp_max_batch_dwell_us",
    "udp_subscriber_mean_datagrams_per_nonempty_recvmmsg",
    "udp_subscriber_max_recvmmsg_calls",
    "udp_subscriber_max_empty_recvmmsg_fraction",
    "udp_subscriber_max_control_poll_calls",
    "udp_subscriber_max_gro_segments",
    "udp_subscriber_max_repair_deliveries",
]

AGGREGATE_FIELDS = [
    "configuration",
    "transport",
    "payload_bytes",
    "target_rate_pps",
    "repetitions",
    "clean_repetitions",
    "publisher_min_pps",
    "subscriber_min_pps",
    "latency_p50_median_us",
    "latency_p50_worst_us",
    "latency_p99_median_us",
    "latency_p99_worst_us",
    "latency_p99_9_median_us",
    "latency_p99_9_worst_us",
    "clock_uncertainty_worst_us",
    "max_loss_fraction",
    "max_repair_deliveries",
]

DROP_KEYS = {
    "Nic.rx_dropped",
    "Nic.rx_errors",
    "Nic.tx_dropped",
    "Nic.tx_errors",
    "Udp.InErrors",
    "Udp.RcvbufErrors",
    "Udp.SndbufErrors",
}


def parse_result(path: Path) -> dict[str, Any] | None:
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return None
    results = []
    for line in lines:
        if line.startswith("RESULT "):
            try:
                results.append(json.loads(line.removeprefix("RESULT ")))
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid RESULT JSON in {path}: {error}") from error
    return results[-1] if results else None


def maximum(items: list[dict[str, Any]], path: tuple[str, ...], default: float = 0.0) -> float:
    values: list[float] = []
    for item in items:
        value: Any = item
        for component in path:
            value = value.get(component) if isinstance(value, dict) else None
        if value is not None:
            values.append(float(value))
    return max(values, default=default)


def summarize(run_dir: Path) -> dict[str, Any]:
    publisher = parse_result(run_dir / "publisher.log")
    subscribers = [
        result
        for path in sorted(run_dir.glob("subscriber-*.log"))
        if (result := parse_result(path)) is not None
    ]
    problems: list[str] = []
    if publisher is None:
        problems.append("no_publisher_result")
        publisher = {}
    subscribers_requested = int(publisher.get("subscribers_requested", len(subscribers)))
    if len(subscribers) != subscribers_requested:
        problems.append(f"subscriber_results_{len(subscribers)}_of_{subscribers_requested}")

    if publisher.get("min_active_during_measurement") != subscribers_requested:
        problems.append("publisher_disconnected")
    target_rate = float(publisher.get("target_rate_pps", 0))
    publisher_pps = float(publisher.get("achieved_publish_calls_per_sec", 0))
    publisher_cpu_fraction = float(publisher.get("process_cpu_utilization_one_core", 0))
    publisher_under_target = bool(target_rate and publisher_pps < target_rate * 0.99)
    if publisher_under_target:
        problems.append("publisher_under_target")
    cpu_core_delta = publisher.get("cpu_core_delta", {})
    cpu_total_ticks = int(cpu_core_delta.get("total_ticks", 0))
    cpu_irq_ticks = int(cpu_core_delta.get("irq_ticks", 0)) + int(
        cpu_core_delta.get("softirq_ticks", 0)
    )
    publisher_core_irq_fraction = cpu_irq_ticks / cpu_total_ticks if cpu_total_ticks else 0.0
    publisher_process_plus_irq_fraction = publisher_cpu_fraction + publisher_core_irq_fraction
    publisher_softirqs = publisher.get("softirq_delta") or {}
    udp_multicast = bool(publisher.get("udp_multicast", False))
    if publisher_under_target and publisher_cpu_fraction < 0.90:
        if udp_multicast and publisher_process_plus_irq_fraction >= 0.90:
            problems.append("publisher_core_saturated")
        else:
            problems.append("publisher_cpu_starved")
    if publisher_core_irq_fraction > 0.05 and not udp_multicast:
        problems.append("benchmark_cpu_irq_busy")
    if int(cpu_core_delta.get("steal_ticks", 0)):
        problems.append("benchmark_cpu_stolen")
    if any(result.get("timed_out", True) or not result.get("end_seen", False) for result in subscribers):
        problems.append("subscriber_incomplete")
    if any(int(result.get("missing_messages", 0)) != 0 for result in subscribers):
        problems.append("application_loss")

    all_results = [publisher, *subscribers]
    kernel_drop_total = sum(
        int(result.get("kernel_delta", {}).get(key, 0))
        for result in all_results
        for key in DROP_KEYS
    )
    if kernel_drop_total:
        problems.append("kernel_or_nic_drop")

    delivered_rates = [float(result.get("delivered_messages_per_sec", 0)) for result in subscribers]
    loss_fractions = [float(result.get("loss_fraction_after_drain", 0)) for result in subscribers]
    subscriber_stats = [result.get("transport_stats", {}) for result in subscribers]
    received_datagrams = [int(stats.get("datagrams_received", 0)) for stats in subscriber_stats]
    received_socket_messages = [
        int(stats.get("socket_messages_received", stats.get("datagrams_received", 0)))
        for stats in subscriber_stats
    ]
    received_batches = [int(stats.get("nonempty_recvmmsg_calls", 0)) for stats in subscriber_stats]
    recvmmsg_calls = [int(stats.get("recvmmsg_calls", 0)) for stats in subscriber_stats]
    datagrams_per_nonempty_call = [
        datagrams / batches if batches else 0.0
        for datagrams, batches in zip(received_datagrams, received_batches, strict=True)
    ]
    repair_requests = [int(stats.get("repair_requests", 0)) for stats in subscriber_stats]
    repair_deliveries = [
        int(stats.get("repair_messages_delivered", 0)) for stats in subscriber_stats
    ]
    if any(repair_deliveries):
        problems.append("udp_repair")
    publisher_transport_stats = publisher.get("transport_stats", {})
    if publisher.get("udp_xdp_requested") and not publisher_transport_stats.get("using_xdp"):
        problems.append("xdp_fallback")
    if int(publisher_transport_stats.get("xdp_ring_full_drops", 0)):
        problems.append("xdp_ring_full")
    if int(publisher_transport_stats.get("xdp_frame_exhaustion_drops", 0)):
        problems.append("xdp_frame_exhaustion")
    if int(publisher_transport_stats.get("xdp_kick_errors", 0)):
        problems.append("xdp_kick_error")
    publication_flushes = int(publisher_transport_stats.get("publication_flushes", 0))
    total_batch_dwell_ns = int(publisher_transport_stats.get("total_batch_dwell_ns", 0))
    row = {
        "run": run_dir.name,
        "status": "ok" if not problems else "+".join(problems),
        "transport": publisher.get("transport", subscribers[0].get("transport", "") if subscribers else ""),
        "payload_bytes": publisher.get("payload_bytes", ""),
        "target_rate_pps": publisher.get("target_rate_pps", ""),
        "burst": publisher.get("burst", ""),
        "subscribers_requested": subscribers_requested,
        "publisher_loop": publisher.get("publisher_loop", ""),
        "control_poll_interval_us": publisher.get("control_poll_interval_us", ""),
        "data_poll_interval_us": (
            subscribers[0].get("data_poll_interval_us", "") if subscribers else ""
        ),
        "publisher_io_poll_calls": publisher.get("io_poll_calls", ""),
        "udp_repair_delay_ms": (
            subscribers[0].get("udp_repair_delay_ms", "") if subscribers else ""
        ),
        "udp_gro": subscribers[0].get("udp_gro", "") if subscribers else "",
        "udp_send_batch_size": publisher.get("udp_send_batch_size", ""),
        "udp_send_batch_delay_us": publisher.get("udp_send_batch_delay_us", ""),
        "udp_adaptive_batching": publisher.get("udp_adaptive_batching", ""),
        "udp_gso": publisher.get("udp_gso", ""),
        "udp_gso_copy": publisher.get("udp_gso_copy", ""),
        "udp_multicast": publisher.get("udp_multicast", ""),
        "udp_multicast_group": publisher.get("udp_multicast_group", ""),
        "udp_xdp_requested": publisher.get("udp_xdp_requested", False),
        "udp_xdp_active": publisher_transport_stats.get("using_xdp", False),
        "udp_xdp_queue": publisher.get("udp_xdp_queue", ""),
        "udp_xdp_vlan_id": publisher.get("udp_xdp_vlan_id", ""),
        "udp_xdp_zero_copy": publisher.get("udp_xdp_zero_copy", False),
        "udp_xdp_attach_pass": publisher.get("udp_xdp_attach_pass", False),
        "publisher_pps": publisher_pps,
        "publisher_cpu_core_fraction": publisher_cpu_fraction,
        "publisher_core_irq_fraction": publisher_core_irq_fraction,
        "publisher_process_plus_irq_fraction": publisher_process_plus_irq_fraction,
        "publisher_softirq_timer": publisher_softirqs.get("TIMER", ""),
        "publisher_softirq_net_tx": publisher_softirqs.get("NET_TX", ""),
        "publisher_softirq_net_rx": publisher_softirqs.get("NET_RX", ""),
        "publisher_softirq_sched": publisher_softirqs.get("SCHED", ""),
        "publisher_softirq_rcu": publisher_softirqs.get("RCU", ""),
        "publisher_min_active": publisher.get("min_active_during_measurement", ""),
        "publisher_late_bursts": publisher.get("late_bursts_over_50us", ""),
        "subscriber_min_pps": min(delivered_rates, default=0.0),
        "subscriber_mean_pps": statistics.fmean(delivered_rates) if delivered_rates else 0.0,
        "subscriber_max_loss_fraction": max(loss_fractions, default=0.0),
        "subscriber_total_missing": sum(int(result.get("missing_messages", 0)) for result in subscribers),
        "subscriber_max_out_of_order": max(
            (int(result.get("out_of_order_messages", 0)) for result in subscribers), default=0
        ),
        "subscriber_max_cpu_seconds_including_drain": maximum(
            subscribers, ("process_cpu_ns_including_drain",)
        )
        / 1e9,
        "subscriber_max_softirq_timer_including_drain": maximum(
            subscribers, ("softirq_delta_including_drain", "TIMER")
        ),
        "subscriber_max_softirq_net_tx_including_drain": maximum(
            subscribers, ("softirq_delta_including_drain", "NET_TX")
        ),
        "subscriber_max_softirq_net_rx_including_drain": maximum(
            subscribers, ("softirq_delta_including_drain", "NET_RX")
        ),
        "subscriber_max_softirq_sched_including_drain": maximum(
            subscribers, ("softirq_delta_including_drain", "SCHED")
        ),
        "subscriber_max_softirq_rcu_including_drain": maximum(
            subscribers, ("softirq_delta_including_drain", "RCU")
        ),
        "latency_max_p50_us": maximum(subscribers, ("latency", "p50_us")),
        "latency_max_p99_us": maximum(subscribers, ("latency", "p99_us")),
        "latency_max_p99_9_us": maximum(subscribers, ("latency", "p99_9_us")),
        "clock_max_uncertainty_us": maximum(
            subscribers, ("clock_one_way_uncertainty_bound_ns",)
        )
        / 1_000.0,
        "kernel_drop_total": kernel_drop_total,
        "tcp_retransmits_publisher": publisher.get("kernel_delta", {}).get("Tcp.RetransSegs", 0),
        "udp_publication_flushes": publisher_transport_stats.get("publication_flushes", ""),
        "udp_max_publications_per_flush": publisher_transport_stats.get(
            "max_publications_per_flush", ""
        ),
        "udp_immediate_flushes": publisher_transport_stats.get("immediate_flushes", ""),
        "udp_adaptive_immediate_flushes": publisher_transport_stats.get(
            "adaptive_immediate_flushes", ""
        ),
        "udp_adaptive_batch_activations": publisher_transport_stats.get(
            "adaptive_batch_activations", ""
        ),
        "udp_adaptive_idle_resets": publisher_transport_stats.get("adaptive_idle_resets", ""),
        "udp_full_batch_flushes": publisher_transport_stats.get("full_batch_flushes", ""),
        "udp_deadline_flushes": publisher_transport_stats.get("deadline_flushes", ""),
        "udp_explicit_flushes": publisher_transport_stats.get("explicit_flushes", ""),
        "udp_mean_batch_dwell_us": (
            total_batch_dwell_ns / publication_flushes / 1_000 if publication_flushes else 0.0
        ),
        "udp_max_batch_dwell_us": publisher_transport_stats.get("max_batch_dwell_ns", 0) / 1_000,
        "udp_sendmmsg_calls": publisher_transport_stats.get("sendmmsg_calls", ""),
        "udp_mean_sendmmsg_us": (
            int(publisher_transport_stats.get("total_sendmmsg_ns", 0))
            / int(publisher_transport_stats.get("sendmmsg_calls", 1) or 1)
            / 1_000
        ),
        "udp_max_sendmmsg_us": publisher_transport_stats.get("max_sendmmsg_ns", 0) / 1_000,
        "udp_sendmmsg_would_block": publisher_transport_stats.get("sendmmsg_would_block", ""),
        "udp_send_entries": publisher_transport_stats.get("send_entries", ""),
        "udp_wire_datagrams": publisher_transport_stats.get("wire_datagrams", ""),
        "udp_xdp_enqueued_datagrams": publisher_transport_stats.get(
            "xdp_enqueued_datagrams", ""
        ),
        "udp_xdp_completed_datagrams": publisher_transport_stats.get(
            "xdp_completed_datagrams", ""
        ),
        "udp_xdp_ring_full_drops": publisher_transport_stats.get("xdp_ring_full_drops", ""),
        "udp_xdp_frame_exhaustion_drops": publisher_transport_stats.get(
            "xdp_frame_exhaustion_drops", ""
        ),
        "udp_xdp_kick_calls": publisher_transport_stats.get("xdp_kick_calls", ""),
        "udp_xdp_kick_errors": publisher_transport_stats.get("xdp_kick_errors", ""),
        "udp_xdp_wire_bytes": publisher_transport_stats.get("xdp_wire_bytes", ""),
        "udp_subscriber_min_datagrams": min(received_datagrams, default=0),
        "udp_subscriber_min_socket_messages": min(received_socket_messages, default=0),
        "udp_subscriber_mean_datagrams_per_nonempty_recvmmsg": (
            statistics.fmean(datagrams_per_nonempty_call) if datagrams_per_nonempty_call else 0.0
        ),
        "udp_subscriber_max_recvmmsg_calls": max(recvmmsg_calls, default=0),
        "udp_subscriber_max_empty_recvmmsg_fraction": max(
            (
                (calls - nonempty) / calls if calls else 0.0
                for calls, nonempty in zip(recvmmsg_calls, received_batches, strict=True)
            ),
            default=0.0,
        ),
        "udp_subscriber_max_combined_poll_calls": maximum(
            subscribers, ("combined_poll_calls_including_drain",)
        ),
        "udp_subscriber_max_control_poll_calls": maximum(
            subscribers, ("control_poll_calls_including_drain",)
        ),
        "udp_subscriber_max_data_poll_calls": maximum(
            subscribers, ("data_poll_calls_including_drain",)
        ),
        "udp_subscriber_max_recvmmsg_batch": max(
            (int(stats.get("max_datagrams_per_recvmmsg", 0)) for stats in subscriber_stats),
            default=0,
        ),
        "udp_subscriber_total_gro_packets": sum(
            int(stats.get("gro_packets_received", 0)) for stats in subscriber_stats
        ),
        "udp_subscriber_total_gro_segments": sum(
            int(stats.get("gro_segments_received", 0)) for stats in subscriber_stats
        ),
        "udp_subscriber_max_gro_segments": max(
            (int(stats.get("max_gro_segments", 0)) for stats in subscriber_stats), default=0
        ),
        "udp_subscriber_max_repair_requests": max(repair_requests, default=0),
        "udp_subscriber_max_repair_deliveries": max(repair_deliveries, default=0),
        "udp_subscriber_total_repair_deliveries": sum(repair_deliveries),
    }
    return row


def aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = (
            row["transport"],
            row["payload_bytes"],
            row["target_rate_pps"],
            row["udp_send_batch_size"],
            row["udp_send_batch_delay_us"],
            row["udp_gso"],
            row["udp_gro"],
            row["udp_multicast"],
            row["udp_xdp_active"],
            row["udp_xdp_zero_copy"],
            row["control_poll_interval_us"],
            row["data_poll_interval_us"],
        )
        groups.setdefault(key, []).append(row)

    aggregated = []
    for key, members in groups.items():
        (
            transport,
            payload,
            rate,
            batch,
            delay,
            gso,
            gro,
            multicast,
            xdp,
            zero_copy,
            control_poll,
            data_poll,
        ) = key
        if transport == "tcp":
            configuration = "tcp"
        else:
            mode = "multicast" if multicast else "unicast"
            backend = "xdp-zero-copy" if zero_copy else ("xdp-copy" if xdp else "socket")
            configuration = (
                f"udp-{mode}-{backend}-b{batch}-d{delay}-gso{int(bool(gso))}"
                f"-gro{int(bool(gro))}-cp{control_poll}-dp{data_poll}"
            )

        def values(field: str) -> list[float]:
            return [float(member[field]) for member in members]

        aggregated.append(
            {
                "configuration": configuration,
                "transport": transport,
                "payload_bytes": payload,
                "target_rate_pps": rate,
                "repetitions": len(members),
                "clean_repetitions": sum(member["status"] == "ok" for member in members),
                "publisher_min_pps": min(values("publisher_pps")),
                "subscriber_min_pps": min(values("subscriber_min_pps")),
                "latency_p50_median_us": statistics.median(values("latency_max_p50_us")),
                "latency_p50_worst_us": max(values("latency_max_p50_us")),
                "latency_p99_median_us": statistics.median(values("latency_max_p99_us")),
                "latency_p99_worst_us": max(values("latency_max_p99_us")),
                "latency_p99_9_median_us": statistics.median(values("latency_max_p99_9_us")),
                "latency_p99_9_worst_us": max(values("latency_max_p99_9_us")),
                "clock_uncertainty_worst_us": max(values("clock_max_uncertainty_us")),
                "max_loss_fraction": max(values("subscriber_max_loss_fraction")),
                "max_repair_deliveries": max(values("udp_subscriber_max_repair_deliveries")),
            }
        )
    return sorted(
        aggregated,
        key=lambda row: (int(row["payload_bytes"]), str(row["configuration"])),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("results", type=Path, help="bench-results/<timestamp> directory")
    parser.add_argument(
        "--compact", action="store_true", help="print only the primary classification columns"
    )
    parser.add_argument(
        "--aggregate", action="store_true", help="aggregate repeated configurations"
    )
    args = parser.parse_args()
    runs_dir = args.results / "runs"
    if not runs_dir.is_dir():
        parser.error(f"missing runs directory: {runs_dir}")
    rows = [summarize(path) for path in sorted(runs_dir.iterdir()) if path.is_dir()]
    if args.aggregate:
        rows = aggregate(rows)
        fields = AGGREGATE_FIELDS
    else:
        fields = COMPACT_FIELDS if args.compact else FIELDS
    writer = csv.DictWriter(sys.stdout, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
