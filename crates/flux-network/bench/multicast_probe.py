#!/usr/bin/env python3
"""Small stdlib-only IPv4 multicast delivery probe for the four-node lab."""

from __future__ import annotations

import argparse
import json
import socket
import struct
import time


MAGIC = b"FLXMCAST"
HEADER = struct.Struct("!8sQ")


def receive(args: argparse.Namespace) -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024 * 1024)
    sock.bind(("", args.port))
    membership = socket.inet_aton(args.group) + socket.inet_aton(args.interface_ip)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
    sock.settimeout(0.25)
    print(
        "READY "
        + json.dumps(
            {"role": "subscriber", "group": args.group, "port": args.port}, sort_keys=True
        ),
        flush=True,
    )

    seen = bytearray(args.count)
    unique = duplicates = invalid = 0
    first_ns = last_ns = 0
    deadline = time.monotonic() + args.timeout
    while unique < args.count and time.monotonic() < deadline:
        try:
            payload, _source = sock.recvfrom(65_535)
        except TimeoutError:
            continue
        now_ns = time.monotonic_ns()
        if len(payload) < HEADER.size:
            invalid += 1
            continue
        magic, sequence = HEADER.unpack_from(payload)
        if magic != MAGIC or sequence >= args.count:
            invalid += 1
            continue
        if seen[sequence]:
            duplicates += 1
            continue
        seen[sequence] = 1
        unique += 1
        first_ns = first_ns or now_ns
        last_ns = now_ns

    sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, membership)
    print(
        "RESULT "
        + json.dumps(
            {
                "role": "subscriber",
                "expected": args.count,
                "unique": unique,
                "missing": args.count - unique,
                "duplicates": duplicates,
                "invalid": invalid,
                "receive_span_ns": max(0, last_ns - first_ns),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return int(unique != args.count or invalid != 0)


def publish(args: argparse.Namespace) -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 64 * 1024 * 1024)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(args.interface_ip))
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 0)
    sock.bind((args.interface_ip, 0))
    padding = bytes(args.datagram_bytes - HEADER.size)
    destination = (args.group, args.port)
    started_ns = time.monotonic_ns()
    for sequence in range(args.count):
        target_ns = started_ns + sequence * 1_000_000_000 // args.rate
        while time.monotonic_ns() < target_ns:
            pass
        sock.sendto(HEADER.pack(MAGIC, sequence) + padding, destination)
    elapsed_ns = time.monotonic_ns() - started_ns
    print(
        "RESULT "
        + json.dumps(
            {
                "role": "publisher",
                "count": args.count,
                "datagram_bytes": args.datagram_bytes,
                "target_rate": args.rate,
                "achieved_rate": args.count * 1e9 / elapsed_ns,
                "elapsed_ns": elapsed_ns,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="role", required=True)
    for role in ("publisher", "subscriber"):
        sub = subparsers.add_parser(role)
        sub.add_argument("--group", required=True)
        sub.add_argument("--port", type=int, required=True)
        sub.add_argument("--interface-ip", required=True)
        sub.add_argument("--count", type=int, required=True)
        if role == "publisher":
            sub.add_argument("--rate", type=int, required=True)
            sub.add_argument("--datagram-bytes", type=int, required=True)
        else:
            sub.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    if args.role == "publisher":
        if args.datagram_bytes < HEADER.size or args.datagram_bytes > 1_400:
            parser.error("datagram bytes must be between 16 and 1400")
        return publish(args)
    return receive(args)


if __name__ == "__main__":
    raise SystemExit(main())
