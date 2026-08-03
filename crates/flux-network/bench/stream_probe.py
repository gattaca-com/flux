#!/usr/bin/env python3
"""Minimal bulk TCP probe for the authorized private benchmark network."""

from __future__ import annotations

import argparse
import json
import socket
import time


BUFFER_BYTES = 1024 * 1024
SOCKET_BUFFER_BYTES = 64 * 1024 * 1024


def server(bind: str, port: int) -> None:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUFFER_BYTES)
    listener.bind((bind, port))
    listener.listen(1)
    print(f"READY {json.dumps({'role': 'server', 'bind': bind, 'port': port})}", flush=True)
    connection, peer = listener.accept()
    connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUFFER_BYTES)
    buffer = bytearray(BUFFER_BYTES)
    view = memoryview(buffer)
    received = 0
    started_ns: int | None = None
    ended_ns: int | None = None
    while True:
        count = connection.recv_into(view)
        now_ns = time.monotonic_ns()
        if count == 0:
            break
        if started_ns is None:
            started_ns = now_ns
        ended_ns = now_ns
        received += count
    connection.close()
    listener.close()
    elapsed_ns = max(1, (ended_ns or time.monotonic_ns()) - (started_ns or time.monotonic_ns()))
    print(
        "RESULT "
        + json.dumps(
            {
                "role": "server",
                "peer": peer[0],
                "bytes": received,
                "elapsed_ns": elapsed_ns,
                "bits_per_second": received * 8e9 / elapsed_ns,
            }
        ),
        flush=True,
    )


def client(target: str, port: int, duration: float) -> None:
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUFFER_BYTES)
    connection.connect((target, port))
    payload = bytes(BUFFER_BYTES)
    view = memoryview(payload)
    sent = 0
    started_ns = time.monotonic_ns()
    deadline_ns = started_ns + int(duration * 1e9)
    while time.monotonic_ns() < deadline_ns:
        sent += connection.send(view)
    connection.shutdown(socket.SHUT_WR)
    ended_ns = time.monotonic_ns()
    connection.close()
    elapsed_ns = max(1, ended_ns - started_ns)
    print(
        "RESULT "
        + json.dumps(
            {
                "role": "client",
                "target": target,
                "bytes": sent,
                "elapsed_ns": elapsed_ns,
                "bits_per_second": sent * 8e9 / elapsed_ns,
            }
        ),
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    server_parser = subparsers.add_parser("server")
    server_parser.add_argument("--bind", required=True)
    server_parser.add_argument("--port", type=int, required=True)
    client_parser = subparsers.add_parser("client")
    client_parser.add_argument("--target", required=True)
    client_parser.add_argument("--port", type=int, required=True)
    client_parser.add_argument("--duration", type=float, default=10.0)
    args = parser.parse_args()
    if args.command == "server":
        server(args.bind, args.port)
    else:
        client(args.target, args.port, args.duration)


if __name__ == "__main__":
    main()
