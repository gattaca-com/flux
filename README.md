# flux

flux is a lightweight Rust framework for building deterministic, high-performance systems where latency, throughput, and predictability are key constraints.

The system is built around pinned workers assigned to physical cores and deterministic dataflow through queues. Messages are copied into shared memory before publication and queues move offsets rather than heap-allocated objects, which naturally pushes the architecture toward allocation-free hot paths. Timing and latency tracking are integrated into the execution model.

## where to look

- **[`tile/mod.rs`](crates/flux/src/tile/mod.rs)** — core execution unit. One per physical core, pinned with affinity + SCHED_FIFO.
- **[`spine/mod.rs`](crates/flux/src/spine/mod.rs)** — wiring layer. Single struct holding all queues and shared data. Tiles read and write through a SpineAdapter that tracks work and stamps timing metadata automatically.
- **[`queue.rs`](crates/flux-communication/src/queue.rs)** — seqlock-based broadcast queue. SPMC or MPMC. Shared-memory backed. No coordination between consumers — they track their own position and recover if lapped.
- **[`seqlock.rs`](crates/flux-communication/src/seqlock.rs)** — seq lock impl that handles synchronisation in the queues.
- **[`flux-timing`](crates/flux-timing/)** — custom time types. Instant wraps rdtsc, Nanos is wall clock, IngestionTime links the two at message arrival.
- **[`spine-derive`](crates/spine-derive/src/lib.rs)** — generates consumer/producer structs, trait wiring, and persistence plumbing from a struct definition.
