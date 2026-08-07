# flux

flux is a lightweight Rust framework for building deterministic, high-performance, distributed systems where latency, throughput, and predictability are key constraints.

The framework is built around pinned workers assigned to physical cores, and deterministic dataflow through queues. Large data is copied into a data cache before publication, and messages with offsets are communicated through queues rather than the allocated objects directly. This naturally pushes the architecture towards allocation-free hot paths. Timing and latency tracking are integrated directly into the communication layer.

## where to look

- **[`tile/mod.rs`](crates/flux/src/tile/mod.rs)** — core execution unit. One per physical core, pinned with affinity and optionally assigned a Linux nice value while retaining the default scheduler.
- **[`spine/mod.rs`](crates/flux/src/spine/mod.rs)** — wiring & dataflow layer. Single struct holding all queues and shared data. Tiles read and write through a SpineAdapter that tracks work and stamps timing metadata automatically.
- **[`queue.rs`](crates/flux-communication/src/queue.rs)** — seqlock-based broadcast queue. SPMC or MPMC. Shared-memory backed. No coordination between consumers — they track their own position and recover if lapped by the producers.
- **[`seqlock.rs`](crates/flux-communication/src/seqlock.rs)** — seqlock impl that handles synchronisation.
- **[`flux-timing`](crates/flux-timing/)** — custom time types. Instant wraps rdtsc, Nanos is wall clock, IngestionTime links the two at message arrival.
- **[`spine-derive`](crates/spine-derive/src/lib.rs)** — generates consumer/producer structs, trait wiring, and persistence plumbing from a struct definition.
- **[`flux-profiler`](crates/flux-profiler/README.md)** — cross-process flamegraph profiler. Annotate functions with `#[timed]`, attach the CLI, and open the trace in magic-trace or Perfetto.

## creating a version tag

Version tags are created locally because the GitHub organization IP allow list
prevents standard GitHub-hosted runners from pushing them. Prepare the version
by changing only `workspace.package.version` in the root `Cargo.toml`, refresh
`Cargo.lock` with `cargo check --workspace --all-features`, run
`cargo test --workspace --all-features --locked`, and merge those changes through
a pull request. Then:

1. Use a machine whose public IP is in the organization allow list and whose Git
   credentials can push tags to the repository.
2. Update local `main` and make sure the worktree is clean:

   ```console
   git switch main
   git pull --ff-only origin main
   ```

3. Create and push the tag:

   ```console
   just release
   ```

The command reads `workspace.package.version` from `Cargo.toml`, validates it
against the existing version tags, and pushes an annotated tag such as `v0.2.0`.
It refuses to run from another branch, with uncommitted changes, or when local
`main` differs from `origin/main`. It creates only the Git tag; it does not create
a GitHub Release or upload binaries.
