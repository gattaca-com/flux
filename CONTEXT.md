# Flux

Flux runs latency-sensitive applications as pinned-core worker loops that exchange
messages over shared memory. This glossary fixes the words used across the workspace;
implementation detail belongs in code and ADRs, not here.

## Language

### Execution

**Tile**:
One worker loop pinned to a core, with an init, a loop body, and a teardown.
_Avoid_: thread, worker, actor

**Spine**:
The shared-memory queue fabric that connects tiles within a process.
_Avoid_: bus, channel

**Signal**:
The process-wide sticky wake counter that idle tiles park on and that producers
increment.
_Avoid_: work signal, event, notify

**Waker**:
The `mio::Waker` a tile registers with the Signal so that spine work interrupts the
tile's blocking poll.
_Avoid_: unparker, notifier

### Networking

**Stream**:
An ordered byte channel with one peer at each end — a TCP or Unix-domain connection — and the
unit a stream network drives. A response held open for appended writes is not a stream in this
sense.
_Avoid_: socket (the OS handle, not the channel), pipe

**ConnectionGroup**:
The connections — inbound and outbound, TCP or Unix-domain — that share one configuration
(framing, socket options, backlog and connection caps) and one owner, together with the listeners
and outbound endpoints that produce them. Owned by one Service, or by the caller as an unclaimed
group. A Service requires one; you never use one bare.
_Avoid_: pool, channel, transport, stream group

**Service**:
A stateful server, client, or both, for an application-layer protocol. It owns one ConnectionGroup
inside a shared network and is scheduled by that network. An HTTP server or client is a Service
(`HttpService`).
_Avoid_: tenant, handler, protocol

**Unclaimed ConnectionGroup**:
A ConnectionGroup no Service has claimed; the caller is its protocol layer and receives its events
inline through the closure passed to `drive`. Claiming is a group-level decision made when a
Service is constructed and undone by `close`, never a per-connection state.
_Avoid_: raw group, unmanaged group, bare group

**Owned poll**:
A network that holds its own poll and drives it, with whatever timeout the caller passes.
_Avoid_: standalone, embedded

**External poll**:
A network built over a poll the caller holds; the caller delivers readiness events and
drives timers, and may register its own sources alongside.
_Avoid_: injected, shared, hosted

**Endpoint**:
The address a listener binds or an outbound connection targets: a TCP socket address or
a Unix-domain socket path.
_Avoid_: bind, addr, address, target

**Peer**:
The identity of the remote end of an accepted connection: a TCP socket address, or
anonymous for a Unix-domain socket.
_Avoid_: client, remote

**Deadline**:
The per-request timer on an outbound connection; expiry fails the request and closes the
connection.
_Avoid_: timeout (which names the idle sweep), TTL

**Draining**:
The closing state of a connection whose request stream was fully consumed: the
connection closes as soon as its queued bytes are written.
_Avoid_: flushing, closing

**Lingering**:
The closing state of a connection whose request stream was not fully consumed: after the
response is written the write side shuts, inbound bytes are read and discarded under
idle and total caps, then the connection closes.
_Avoid_: half-close, graceful close, linger-close

**Refused**:
An accepted connection dropped immediately, without registration or bytes, because its
ConnectionGroup is at its connection cap.
_Avoid_: rejected, throttled
