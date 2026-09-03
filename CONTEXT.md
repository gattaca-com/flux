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
and outbound endpoints that produce them. Created by a network and owned by exactly one Service
chain: moving the group into its Service is the claim, and a group cannot be scheduled bare.
_Avoid_: pool, channel, transport, stream group, unclaimed group

**Service**:
A stateful server, client, or both, for an application-layer protocol. It implements the static
scheduling contract (`Service`), owns one ConnectionGroup — directly, or through a lower Service
it contains — and is scheduled by the network that created the group; of a composed chain, only
the outermost Service is scheduled. An HTTP server or client is a Service (`HttpService`).
_Avoid_: tenant, handler, protocol

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
connection. In scheduling (`next_deadline`), the earliest instant a tick of a Service could
progress work, with work already due reported at the instant it became due; work a Service
exposes upward for its caller to pull rides the did-work report, never the deadline. The fold
is asked and answered for one instant, which the deadline carries (see Witness).
_Avoid_: timeout (which names the idle sweep), TTL

**Witness**:
A value only a ConnectionGroup constructs — `ReadinessOutcome`, `TickOutcome`, `Deadline` —
that carries proof of a scheduling obligation up a Service chain: a composer can pass it
through, widen its work or bring its deadline forward, and nothing else. The tick and deadline
witnesses also carry the instant they answer for, and reading one for another instant fails
under debug assertions (ADR 0004).
_Avoid_: outcome type, token, receipt, proof object

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
