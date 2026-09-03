---
status: accepted
---

# The scheduling contract is three laws, carried by stamped witnesses

A `Service` keeps three laws toward the network that schedules it. **Tick is fresh**: every
`tick` reaches the group on that invocation — a leaf calls `ConnectionGroup::maintain` with the
`now` it was given, a composer ticks its lower service with that same `now`. **Work is
monotone**: a tick's work is the lower work or the service's own; work reported below is never
erased. **Deadline is fresh and monotone**: every `next_deadline` consults the group, or the
lower service, on that invocation with the `now` it was given, and reports no later than what it
was told; `None` is infinity. The laws are carried by the two witness types the group alone
constructs, `TickOutcome` and `Deadline`, and the types stay: the monotone halves and "reached
the group" are compile-time facts (`or_worked` only widens, `earliest` only brings forward and
treats `None` as infinity, there is no other constructor), and the fresh halves are **stamps**.
Each witness carries the instant it was produced for — the group stamps the outcome of
`maintain` with the tick's `now` and the deadline with the fold's `now`, which
`Service::next_deadline` therefore takes as a parameter and hands down unchanged — and reading a
witness names the instant being answered: `worked(&self, now)` and `instant(&self, now)` compare
stamps under debug assertions and panic on a mismatch, so an answer kept from an earlier
invocation and returned for a later one fails at its first read. The check is debug-only by
decision: authors of services outside flux must exercise them in debug-mode tests, flux tests its
own services, and flux makes no promise about an untested implementation in a release build. The
stamps are a debug-time contract detector, not a complete correctness guarantee: they certify
that delegation was fresh, not the service's own bookkeeping.

## Considered options

- **Drop the witness types** for `bool` and `Option<Instant>`, with a free `earliest` helper
  and a client-side probe test as the documented check. Rejected: the probe is needed in both
  worlds, because replay escapes the types either way, while the compile-time half is free; and
  "we can mock a Service" is not a benefit, since a fabricated `true` or `None` proves nothing
  about the obligations and real composers hold concrete lower types, so a `Service` mock is
  uninjectable without test-only generics. Dropping also reopens the `Option::min` trap, where
  `None < Some` silently discards the transport deadline exactly when a composer has no timer.
- **Keep the types as they were**, with replay a documented hazard. Rejected in favour of stamps,
  but preferred to dropping: an independent review ranked keep-and-stamp above keep-as-is above
  drop.
- **The runtime audit** this branch replaced: flags in the group identity armed by `maintain` and
  the fold, observed and asserted by the driver. Rejected: cross-call state behind `&self` that
  had to be `!Sync` after a concurrency failure, and a check that is correct only if every writer
  of the flags is known. Stamps have no shared state; their coverage overlaps the audit's without
  equalling it — a stamp proves some group produced the witness for this instant, not which
  group (the completeness validation closes that in practice), and cannot see a replay under an
  identical instant (an accepted missed detection; no iteration counter).
- **Lifetime-tied witnesses** borrowing the service, which make stashing a compile error.
  Rejected on ergonomics: `HttpService` holds its tick outcome while it ticks its protocol
  against the group, and a borrowing outcome conflicts with that `&mut` access.
- **Tuples in place of the newtypes.** Rejected: a public constructor makes the stamp forgeable
  (`(false, now)` is a fresh-looking outcome that never reached the group), the monotone folds
  are lost, and the stamp becomes readable so a stale value's own instant can be passed back.
- **A release `assert!`.** Rejected on the responsibility split above; the compare would be two
  per service per iteration and never scale with events or connections, so the level is a
  decision about who owns a user service's correctness, not about cost. Layout is one
  representation across profiles unless a measurement says otherwise.
- **Caller-supplied instants in External mode.** Rejected: the network is the start of every
  delegation chain in both modes, so `next_deadline(&services)` and `tick(&services)` mint their
  instants internally, as `drive` does, and a caller can neither handle a scheduler instant nor
  tick with a stale one. Nothing is lost for tests: a synthetic clock is a property of driving a
  `Service` directly.
- **A `FoldInstant` newtype** for the fold's `now`, and **consuming read accessors**. Rejected:
  the newtype adds a concept without adding protection, and a consuming read outlaws a composer
  that branches on a lower outcome and still passes it up.

## Consequences

- `Service::next_deadline(&self, now: Instant) -> Deadline` and
  `ConnectionGroup::next_deadline(&self, now: Instant) -> Deadline`; `TickOutcome::worked(&self,
  now: Instant) -> bool` and `Deadline::instant(&self, now: Instant) -> Option<Instant>`. Every
  service implementation hands `now` down unchanged; substituting a fresh clock read violates the
  contract and is reported by the same check. `now` is a consistency token, not a filter: an
  instant already in the past is still reported. The Owned `drive` and the three External calls
  keep their signatures.
- The driver mints one instant per delegation chain: one before the fold, and a fresh one after
  the poll wait for the tick, because the wait may have been a whole timeout. Both chains are
  read for the instant they were asked with.
- Freshness is testable without sockets. The minimal probe is two quiet iterations with distinct
  instants, each reading the fold and the tick for the instant it asked with; any replay from the
  first iteration carries the first instant and fails the second read. Replay can be gated on
  state — a cache filled only after a handshake, or only while no readiness arrives between tick
  and fold — so the probe runs quiet and is repeated in every protocol state the service can
  reach. The value half of the laws, that an overdue reconnect is attempted and the deadline
  advances, is a separate test with an outbound endpoint nobody listens on. The `Service`
  documentation carries the recipe.
- Not detected by stamps, and left to a service's own behavioural tests: delegation to the wrong
  group with the correct `now`; a stale timer of the service's own folded onto a fresh deadline;
  a wrong pending report; events dropped after `maintain`; and a replayed `ReadinessOutcome`,
  which has the same banking hazard and stays unstamped as an accepted limitation shared by
  every option considered, readiness being the hotter path. A lifetime tied to the offered event
  may close it later and is evaluated separately.
- `TickOutcome` grows from 1 byte to 16 and `Deadline` from 16 to 24, alignment included; both
  live on the stack in the driver's loops and are never stored. The staleness demonstrations in
  `exp_QA3_staleness` become should-panic pins under debug assertions and keep pinning the
  hazard's shape in release.
- ADR 0001 stands with the contract paragraph read through this one: where it names the runtime
  audit, the witnesses and their stamps have taken its place.
