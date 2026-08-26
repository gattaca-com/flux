---
status: accepted
---

# Poll ownership is a network mode; services are protocol layers the network schedules

A tile that hosts sockets must own exactly one `mio::Poll`, so that under `flux/park` it can
register one `Waker` with the Signal and block in `poll` with a non-zero timeout — two polls
in one tile means neither may ever block. `StreamNetwork` therefore chooses at construction who
holds the poll: **Owned poll** (the network creates and drives its own poll, and hands out a
`Waker` on a reserved token) or **External poll** (the network is built over a `Registry`
cloned from the caller's poll and a token base, and never polls). Protocol layers do not own a
network: each is a **Service** owning one `ConnectionGroup` inside a shared `StreamNetwork`, and
the network — not the caller — schedules them. The shape is general — a service is any
protocol layer the network schedules — and `HttpService` is the first. Under Owned poll, one
call per iteration, `drive(max_timeout, services, unclaimed_handler)`, folds every deadline, polls
once, routes each event to the service owning its group, runs network maintenance and ticks each
service. Under External poll the caller makes only the three calls a caller-held poll inherently
requires — `next_deadline(services)` to fold into its own timeout,
`handle_event(&event, services, unclaimed_handler) -> bool` per readiness event (false: not ours,
route to your own sources), and `tick(services)` once per iteration — and reconstructs nothing
else. Services expose their protocol events by pull (`next_event(&mut net)`). The scheduling
hooks — group, `on_event`, `tick`, `next_deadline` — live on a trait private to flux that only
the network calls; a service hands the network an opaque `ServiceRef<'_>` (`beacon.as_service()`),
so `drive(max_timeout, &mut [beacon.as_service(), engine.as_service()], unclaimed_handler)` is the
whole public contract, and nothing outside flux can implement or invoke a hook. ConnectionGroups
no service claims are **unclaimed groups**: their events reach `unclaimed_handler` synchronously,
lending the payload for the duration of the call.

## Considered options

- **Owned poll only, services inside it.** Rejected: users with their own mio sources need a
  caller-held poll; a foreign-source API on an owned poll would duplicate External poll with
  a worse contract.
- **Per-service token ranges.** Rejected: a service's token demand (accepted connections) is
  unknowable up front. One network is one contiguous token space from its base; the caller
  reserves its own tokens below it.
- **Type-state `StreamNetwork<Owned>` / `StreamNetwork<External>`.** Rejected: the parameter
  would infect every service signature to catch a misuse that fires on the first poll in any
  test. Polling an External-poll network panics with a clear message instead.
- **Individually driven services** — the caller folds deadlines, chains `on_event` calls and
  orders ticks. Rejected: every tile becomes a slightly different implementation of the
  network scheduler, and the scheduling invariants live nowhere.
- **A public `Service` trait as the carrier (`&mut [&mut dyn Service]`), sealed or not.**
  Rejected: an openly implementable trait commits flux to far more than four methods before
  third-party services are product scope, and sealing does not help — sealing prevents
  implementation, not invocation. On a trait object, supertrait methods resolve as if they were
  inherent even when the supertrait is unnameable, so hooks placed on a private supertrait are
  still callable by every holder of the object. Only an opaque carrier over a private trait
  keeps the hooks the network's alone.
- **Releasing a claim from the network side (`release_group`), or permanent claims.**
  Rejected: the former leaves a stopped service's connections delivering HTTP-framed bytes to
  `unclaimed_handler` with nothing to parse them; the latter forces every service to live as long as
  its network for no gain. A `Drop`-time check was rejected because it fires spuriously at
  process teardown.
- **Pull-based delivery for unclaimed groups too.** Rejected: an unclaimed group's `Message` lends
  its payload for the callback only; queueing it for a later pull would force a copy on a path
  that is zero-copy today.

## Consequences

- One iteration is, in order: capture `now`; run network maintenance due at `now` (reconnect
  attempts, pending disconnects); poll (Owned) or receive the caller's events (External);
  route each event to its service or to `unclaimed_handler`; deliver the lifecycle events those
  operations produced; tick each service once in slice order, passing `now`; return so the
  caller pulls protocol events. Transport events produced during maintenance reach a service
  before that service's tick, so protocol state never lags transport state by an iteration.
  Slice order may affect fairness and must never affect correctness.
- `drive`, `next_deadline` and `tick` first validate the supplied services against the groups
  the network knows to be service-owned: every such group appears exactly once, or the call
  panics before anything else happens — a deterministic configuration error at the first call,
  never a timing-dependent one (an omitted service with a request deadline in flight would
  otherwise never have that deadline folded). A service's claim is released only by
  `close(self, &mut StreamNetwork)`, which consumes the service, hard-closes its group's
  connections and listeners, discards its un-pulled events and returns the group to unclaimed
  status, empty, with its handle still valid. Dropping a service without closing it while the
  network is still driven is a programming error, and the next driver call reports it through that
  same validation, naming the group; at teardown, dropping services and network together is
  harmless because nothing is driven afterwards. Omission is therefore never a lifecycle state.
  Routing is then a linear lookup by group.
- Deadlines are folded by the network from its own timers and every service's
  `next_deadline()`; under External poll the caller folds only its own timers against the result.
- Response capability is offered only where a response is possible: `Request` and `Writable`
  carry a `Responder` scoped to that connection, writing the body straight into the send
  buffer; a request borrowed from the connection buffer is never copied to make a response
  possible. Answering later by token remains available; a request is answered exactly once
  and that is connection state, not caller choreography.
- A pulled event borrows the service and the network for as long as it lives, so a handler
  reaches the network only through the `Responder` it was handed, and events cannot be stored
  or cloned. Parsed request metadata is kept as byte ranges owned by the service so that an
  event can outlive the parse. Dropping a `Responder` without responding defers the response
  to the by-token path; a request never answered is closed by the idle sweep.
- Dynamic dispatch touches only the control path — routing, ticks, deadlines. Parsing, byte
  handling and response generation stay concrete, and `next_event` runs in the same iteration
  as the tick that made the connection ready.
- Readiness is service state, not per-iteration scratch: a caller may stop pulling after any
  number of events and resume in a later iteration with nothing lost, which is what gives a
  tile a per-iteration work cap. A service's `tick` returns `true` while it has pullable protocol
  events — created by this tick or left un-pulled by a caller that stopped early — and
  `drive`, `handle_event` and the network's `tick` fold that with their own actions into one
  did-work result, so a tile can honour the park contract without inspecting service internals
  and never parks on outstanding work.
- Configuration ownership follows the layers: stream transport and queue policy (socket
  options, reconnect interval, framing, backlog caps, connection cap) live on the ConnectionGroup;
  HTTP parsing and HTTP connection-state policy (head, body and header limits, idle timeout,
  linger caps, request deadline) live on the service; nothing varies per operation until a
  consumer demonstrates the need. A service claims a caller-created group and adds no transport
  settings of its own, so two services with different caps coexist because they own different
  groups.
- The park contract this serves: a tile registers the network's (Owned) or its own
  (External) `Waker` via `SpineAdapter::register_waker`, after which the tile runner stops
  parking on the Signal and the tile blocks in its poll; the Signal wakes the poll on spine
  work, socket readiness wakes it on I/O.
