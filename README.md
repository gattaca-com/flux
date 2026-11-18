# flux

flux is a lightweight Rust framework for building deterministic, high-performance, distributed systems where latency, throughput, and predictability are key constraints.

The framework is built around pinned workers assigned to physical cores, and deterministic dataflow through queues. Large data is copied into a data cache before publication, and messages with offsets are communicated through queues rather than the allocated objects directly. This naturally pushes the architecture towards allocation-free hot paths. Timing and latency tracking are integrated directly into the communication layer.

## where to look

- **[`tile/mod.rs`](crates/flux/src/tile/mod.rs)** — core execution unit. One per physical core, pinned with affinity and optionally assigned a Linux nice value while retaining the default scheduler.
- **[`spine/mod.rs`](crates/flux/src/spine/mod.rs)** — wiring & dataflow layer. Single struct holding all queues and shared data. Tiles read and write through a SpineAdapter that tracks work and stamps timing metadata automatically.
- **[`queue/mod.rs`](crates/flux-communication/src/queue/mod.rs)** — seqlock-based broadcast queue. SPMC or MPMC. Shared-memory backed. No coordination between consumers — they track their own position and recover if lapped by the producers.
- **[`seqlock.rs`](crates/flux-communication/src/seqlock.rs)** — seqlock impl that handles synchronisation.
- **[`flux-timing`](crates/flux-timing/)** — custom time types. Instant wraps rdtsc, Nanos is wall clock, IngestionTime links the two at message arrival.
- **[`spine-derive`](crates/spine-derive/src/lib.rs)** — generates consumer/producer structs, trait wiring, and persistence plumbing from a struct definition.
- **[`flux-profiler`](crates/flux-profiler/README.md)** — cross-process flamegraph profiler. Annotate functions with `#[timed]`, attach the CLI, and open the trace in magic-trace or Perfetto.

## creating a version tag

Change only `workspace.package.version` in the root `Cargo.toml`. Refresh
`Cargo.lock` with `cargo check --workspace --all-features`, and merge through a
pull request.

The `release` workflow then validates the version, runs the tests and the semver
check, and pushes an annotated tag such as `v0.2.0`. It creates only the Git tag,
not a GitHub Release or binaries. If a run fails, fix the cause and re-run the
workflow by hand.
