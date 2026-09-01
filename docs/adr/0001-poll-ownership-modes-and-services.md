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
network: each is a **Service** owning one `ConnectionGroup`, and the network — not the caller —
schedules them. The group is the transport state itself — listeners, outbound endpoints,
connections and byte queues — created by `add_group` and moved into its service: ownership is
the claim, the compiler keeps a group to one owner, and the network keeps only what every group
shares (the registry, one contiguous token space, and each group's identity). `Service` is a
public, statically dispatched trait of four methods — `group_id`, `handle_event`, `tick`,
`next_deadline` — that downstream crates implement; the driver is generic over the service
slice (with a blanket `impl Service for &mut S`), and a tile hosting several service types
wraps them in one enum of its own. `HttpService` is the first service. Under Owned poll, one
call per iteration, `drive(max_timeout, services)`, folds every deadline, polls once, routes
each event to the service owning its group and ticks each service once. Under External poll the
caller makes only the three calls a caller-held poll inherently requires —
`next_deadline(services)` to fold into its own timeout, `handle_event(&event, services) ->
bool` per readiness event (false: not ours, route to your own sources), and `tick(services)`
once per iteration — and reconstructs nothing else. Services expose their protocol events by
pull (`next_event()`). The scheduling contract is enforced without hiding it: readiness is
truthful by construction, because `handle_event` returns a `ReadinessOutcome` that only
`ConnectionGroup::handle_event` can produce, and the two obligations the type cannot carry —
run the group's maintenance inside every tick, fold the group's deadline into every fold — are
audited through the group's identity at runtime, panicking on the first omission.

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
- **An opaque carrier over a private trait** — network-held group state, `as_service()`
  handles, and a closure for groups no service claims. Rejected: downstream services are
  product scope, and a contract nothing outside flux can implement forces every protocol into
  flux. Group state held by the network also reduces a service's identity to an integer index
  that validates against any network, so a service built on one network is silently driven by
  another; with the group owned by its service, identity is the network behind it, and the
  driver rejects a foreign service before any state changes. The hook-leak argument against a
  public trait applied to `dyn` carriers — on a trait object, supertrait methods resolve as if
  inherent, so sealing prevents implementation, not invocation — and static dispatch avoids the
  trait object rather than the trait: the hooks are harmless to expose because each operates on
  the service that implements it, and the outcome type keeps readiness the group's alone.
- **Unclaimed groups, delivered through a closure.** Rejected: with transport state owned by
  services there is no network-side state to deliver from, and a trivial leaf service replaces
  the closure. The zero-copy path survives the move: `ConnectionGroup::handle_event` and
  `maintain` lend each payload to the owning service's callback for the duration of the call,
  exactly as the closure was lent to.
- **Releasing a claim from the network side (`release_group`), or permanent claims.**
  Rejected: a released group's connections would go on delivering protocol-framed bytes with
  nothing left to parse them, and permanent claims force every service to live as long as its
  network for no gain. Closing consumes instead: `close(self, &mut net)` consumes the service
  and `remove_group(group)` consumes the group, hard-closing its sockets and closing its slot.
  A `Drop`-time check was rejected because it fires spuriously at process teardown.

## Consequences

- One iteration is, in order: validate the service set; capture `now`; fold `max_timeout` with
  every service's deadline; poll (Owned) or receive the caller's events (External); route each
  event to its service in slice order, with the disconnects that handling produced drained
  inside the owning group before the next event; tick each service once in slice order, passing
  the time the poll wait ended — the wait is where an iteration spends its time, so a timer a
  tick starts runs from the end of the wait, and one that expired during it is due in the same
  iteration; return so the caller pulls protocol events. A service runs its group's due
  transport work (reconnect attempts, pending disconnects) at the start of its tick and routes
  what that produces before its own timers, so protocol state never lags transport state by an
  iteration. Slice order may affect fairness and must never affect correctness.
- `drive`, `next_deadline` and `tick` first validate the supplied services against the groups
  the network opened: every open group appears exactly once and each belongs to this network,
  or the call panics before anything else happens — a deterministic configuration error at the
  first call, never a timing-dependent one (an omitted service with a request deadline in
  flight would otherwise never have that deadline folded). Dropping a service without closing
  it while the network is still driven is a programming error, and the next driver call reports
  it through that same validation, naming the group; at teardown, dropping services and network
  together is harmless because nothing is driven afterwards. Omission is therefore never a
  lifecycle state. Routing is a linear offer by group.
- A deadline is the earliest instant a tick of the scheduled service could progress work, and
  work already due reports the instant it *became* due — a queued disconnect its queue instant,
  a composer's undrained leftovers the tick instant the composer already held — never a fresh
  clock read, so a due deadline folds to a zero poll timeout (mio rounds a nonzero
  sub-millisecond timeout up to a whole millisecond). Work a service exposes upward for its
  caller to pull is the caller's to schedule and rides the did-work report, never the deadline.
  Under External poll the caller folds only its own timers against the result.
- A service may own a lower service instead of the group directly; only the outermost one is
  scheduled, and each level delegates readiness, the tick and the deadline along the chain —
  the audits hold the root to the leaf's obligations. A composer consumes the lower service's
  lending events through a bounded drain whose result — events remain undrained, `#[must_use]`
  — is what it folds into its deadline, at the tick instant that left them.
- Response capability is offered only where a response is possible: `Request` and `Writable`
  carry a `Responder` scoped to that connection, writing the body straight into the send
  buffer; a request borrowed from the connection buffer is never copied to make a response
  possible. Answering later by token remains available; a request is answered exactly once
  and that is connection state, not caller choreography.
- A pulled event borrows the service for as long as it lives, so a handler reaches the
  connection only through the `Responder` it was handed, and events cannot be stored or
  cloned. Parsed request metadata is kept as byte ranges owned by the service so that an
  event can outlive the parse. Dropping a `Responder` without responding defers the response
  to the by-token path; a request never answered is closed by the idle sweep.
- The whole control path is statically dispatched: no trait objects, no boxing, and the
  compiler sees every service concretely. Parsing, byte handling and response generation stay
  concrete too, and `next_event` runs in the same iteration as the tick that made the
  connection ready.
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
  consumer demonstrates the need. A service takes ownership of a caller-created group and adds
  no transport settings of its own, so two services with different caps coexist because they
  own different groups.
- The park contract this serves: a tile registers the network's (Owned) or its own
  (External) `Waker` via `SpineAdapter::register_waker`, after which the tile runner stops
  parking on the Signal and the tile blocks in its poll; the Signal wakes the poll on spine
  work, socket readiness wakes it on I/O.
