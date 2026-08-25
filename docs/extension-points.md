# Extension points

Directions the networking design keeps open without deciding, in two tiers. An **eventual
feature** is wanted and unscheduled: the current design must accommodate it cheaply, so its
shape is worked out here in enough detail to check that nothing in the current stack
forecloses it. A **speculative feature** may never happen: the current design must merely not
make it impossible, and it does not drive any current decision, so it is recorded in a few
sentences. A section becomes an ADR only when it is decided, and may be dropped at any time.

## Eventual features

### Streamed responses

**Why.** Two workloads need a response held open and appended to: an event stream
(server-sent events — a long-lived, mostly idle connection with small appended writes) and a
body too large to render at once (hundreds of megabytes of JSON produced in slices). The state
machine is the costly part to retrofit, so its shape is fixed here; the framing is not.

**Shape.** The `Responder` carried by `HttpEvent::Request` gains `begin_stream(status,
headers)`; once a connection is in the **Streaming** state its `Responder` — carried by
`HttpEvent::Writable` — offers `stream(bytes)`, `stream_with(FnOnce(&mut Vec<u8>))`,
`end_stream()` and `abort_stream()`. Each is valid only in the state that admits it; a misuse
returns `false`, like a second `respond`. The same operations exist deferred by token. A
producer writes its first slices inline from the request that opened the stream, until the
connection reports full, and continues from each `Writable`. A Streaming connection stops
parsing inbound bytes as pipelined requests (they are read and discarded, as in Lingering) and
is exempt from the idle sweep, which would otherwise disconnect exactly the subscribers
behaving correctly; the exemption is per connection, so ordinary requests on the same listener
keep their timeout. The state ends when the tenant ends or aborts the stream, when the
network disconnects the peer (backlog cap or `send_timeout`), or when the peer disconnects.

**Framing.** The caller supplies only the status and its own headers; the tenant writes the
message delimiting, chosen per stream, so the delimiting can change without touching callers.
A bulk body is chunked, because the terminal chunk is what lets a client tell a complete body
from a truncated one; after `end_stream` the connection returns to keep-alive, and a request
the client sends after the stream is served normally. An event stream is intended to be
close-delimited (no `Content-Length`, no `Transfer-Encoding`, `Connection: close`; `end_stream`
drains and closes), pending a per-client check of the event-stream consumers, with chunked as
the alternative. An HTTP/1.0 requester always gets close-delimited.

**Backpressure to the producer.** Watermarks are Group queue policy and the network enforces
them: `StreamGroupConfig` carries a low and a high watermark below `max_backlog_bytes`; a
write that leaves a connection's backlog at or above the high watermark is reported as full,
and when a backlog that was reported full drains below the low watermark the network emits one
`StreamEvent::Writable` for that connection — an edge, never repeated while the backlog stays
low. Raw groups receive it through `raw_handler`; the HTTP tenant records it in `on_event`
and, only for a connection whose producer was refused, queues an `HttpEvent::Writable` to be
pulled. The producer keeps its own cursor and writes the next slice on each `Writable`, so
per-connection memory stays near one watermark whatever the body size, and the work done per
iteration is bounded by the slice — which is what lets a bulk render share a tile with
latency-sensitive tenants. The hard cap still disconnects a peer that stops draining;
`send_timeout`, also Group policy enforced by the network for every connection in the group,
disconnects a peer whose queue makes no progress for that long — the peer that drains too
slowly to trip the cap.

**Abort is not end.** `abort_stream()` closes the connection without the terminal chunk, so the
client observes a truncated body. A producer whose source is invalidated mid-stream — a state
snapshot superseded while a body is half sent — cannot restart the body on the wire, and ending
the stream would forge completion. An event stream has nothing to abort; a bulk body does.

**Consequences.** Slow event-stream consumers are disconnected by the backlog cap and reconnect
with `Last-Event-ID`; the tenant adds no stream-specific buffering. A half-open peer with an
empty backlog is invisible to both the backlog cap and `TCP_USER_TIMEOUT`, which only act with
data queued, so event-stream producers emit a periodic SSE comment line at an interval of
their choosing; the tenant does not do this for them. A client that gives up on a large body
mid-stream is the ordinary case, not an error — many consumers cap a whole request at a few
seconds — and the existing `Disconnected` handling covers it; the producer drops its cursor.
Un-pulled `Writable` events persist across iterations like any other readiness, so a tile's
per-iteration work cap applies to streams too.

**What the current design must preserve.** The tenant, not the caller, writes
`Content-Length` and rejects caller-supplied `Content-Length` and `Transfer-Encoding`, so
delimiting stays the tenant's to choose. The `Responder` is scoped to the connection of the
event being pulled and can grow operations gated on connection state. The accepted-connection
state is an enum that can gain a variant, and the idle sweep decides per connection state.
Watermarks and `send_timeout` are `StreamGroupConfig` fields the network enforces, beside the
existing backlog cap and the send queue's age tracking; `StreamEvent` can gain a `Writable`
variant. None of this is built until the feature is scheduled; the compile-check prototype of
the `Responder` borrows covers the stream operations as well as `respond`.

## Speculative features

### TLS 1.3

Would be a per-group option on `StreamGroupConfig`, transparent to every tenant: the network
decrypts before emitting `Message` and encrypts inside its write path, emits `Accepted` and
`Connected` once the handshake completes, and sends `close_notify` before a write-side shutdown.
A sans-IO implementation behind a cargo feature; the crate is chosen if and when the work is
scheduled. Terminating TLS at a reverse proxy in front of a plain listener remains the
zero-cost alternative. Kept open by two properties the current design already has: `Message`
payloads are opaque byte chunks, and all writes go through one closure-based path.

### Unix socket file mode

Would be an explicit mode, and possibly owner and group, on a Unix `Endpoint`, applied at
bind instead of inherited from the process umask (ADR 0003). The umask is process-wide, so
changing it around a bind is not thread-safe; the race-free shapes are a mode applied after
bind, accepting a brief window at umask mode, or binding inside a private directory and
renaming into place. Kept open because the bind path is one function inside `StreamNetwork`
and `Endpoint::Unix` can grow options without disturbing callers that pass only a path.

### QUIC

Would be a sibling network beside `StreamNetwork`, sharing the poll through the same tenant
contract, never an `Endpoint` variant: QUIC multiplexes many connections over one UDP socket
and many streams per connection, which does not fit a network whose unit is an accepted byte
stream (ADR 0002). A flux application already runs a sans-IO QUIC transport in a tile of its
own to a latency standard the HTTP tenant does not need to meet; a flux `QuicNetwork` would
generalise that shape, and only if a second user needs it. Kept open by one discipline: a
token identifies the channel a request arrives on, not a TCP connection, so a multiplexing
transport can key the same router-facing events by request stream.
