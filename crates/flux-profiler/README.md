# flux-profiler

A cross-process tracing profiler for flux. Annotate functions with `#[timed]`,
run your app, then attach the `flux-profiler` CLI to capture a trace you can open
in [magic-trace](https://magic-trace.org) or [Perfetto](https://ui.perfetto.dev).

Marks are written to per-thread shared-memory rings, so the profiler reads them
from a **separate process** with zero involvement from your app after startup.

## Install

Build and install the `flux-profiler` CLI binary:

```bash
cargo install --git https://github.com/gattaca-com/flux flux-profiler
```

## Quick start

### 1. Instrument your app

```rust
use flux_profiler::{enable_profiler, timed};

#[timed]
fn do_work() {
    // ...
}

fn main() {
    enable_profiler("my-app"); // publishes the shmem rings under this app name
    loop {
        do_work();
    }
}
```

- `#[timed]` records an open/close frame on every exit path (return, `?`, panic).
  Frames are named `crate::module::fn` by default; pass a literal to override:
  `#[timed("custom_name")]`.
- `enable_profiler(app)` must be called once at startup before any `#[timed]`
  function runs.

### 2. Run your app, then attach the profiler

```bash
cargo run -p my-app
```

Then attach the profiler in a second terminal. If exactly one instrumented app
is live it attaches automatically; otherwise pass `--pid <pid>`:

```
flux-profiler [--pid <pid>] [--out <path.fxt>] [--duration <30s|5m|1h>] [--max-mem <512MB|2GB>]
```

Press **Ctrl-C** (or let the app exit) to write the `.fxt` trace. The default
output is `<app>-trace-<pid>.fxt`.

To stop automatically instead of waiting for Ctrl-C, cap the run by wall-clock
time (`--duration`, e.g. `30s`/`5m`/`1h`) or by the reader's retained-event
footprint (`--max-mem`, e.g. `512MB`/`2GB`). Whichever fires first — that,
Ctrl-C, or the app exiting — stops the capture and exports the trace.

`--max-mem` defaults to `1GB` so a forgotten profiler can't grow unbounded;
raise it for longer captures.

### 3. Open the trace

Drag the `.fxt` file into <https://magic-trace.org> or <https://ui.perfetto.dev>.

## Try it end to end

A minimal producer example is included:

```bash
# terminal 1
cargo run -p flux-profiler --example timed_producer

# terminal 2
flux-profiler
```

## Optional features

| Feature | What it adds |
|---|---|
| `disable-profiling` | Compiles every `#[timed]` out to a plain function call — zero overhead, no guard, no atomic load. |
| `perf` | Per-call hardware counters (instructions, cycles, branch/cache misses) via rdpmc. Requires `kernel.perf_event_paranoid <= 2` at runtime (`<= 1` to include kernel-mode work). |
| `alloc-profile` | Per-thread allocated/freed byte counts recorded alongside each `#[timed]` mark. Wraps the global allocator. |
| `unpinned-threads` | Tags every mark with its socket (rdtscp) so timestamps stay aligned on machines with drifted per-socket TSCs (Linux only). See [Multi-socket machines](#multi-socket-machines). |

### Zero overhead when not profiling

`#[timed]` is near-free until you call `enable_profiler` (one atomic load per
call). To strip it out entirely, build with `disable-profiling` — every
`#[timed]` collapses to just the function body, so annotations can stay in the
source and vanish from production builds:

```bash
cargo run -p my-app --release --features flux-profiler/disable-profiling
```

`perf` works out of the box — enable it and the counters ride every `#[timed]`
mark:

```bash
cargo run -p flux-profiler --example timed_producer --features perf
```

`alloc-profile` additionally requires you to install the counting allocator as
your app's global allocator, so it can tally bytes. Gate it behind your own
feature flag (chained to `flux-profiler/alloc-profile`) so normal builds keep
the plain allocator untouched:

```rust
use std::alloc::System;
use flux_profiler::allocator::CountingAllocator;

#[cfg(not(feature = "alloc-profile"))]
#[global_allocator]
static GLOBAL: System = System;

#[cfg(feature = "alloc-profile")]
#[global_allocator]
static GLOBAL: CountingAllocator<System> = CountingAllocator(System);
```

`CountingAllocator` wraps any base allocator — swap `System` for `MiMalloc/Jemalloc` or
whatever you run in production.

### Multi-socket machines

Mark timestamps are raw TSC reads. On a single socket, or when TSCs are
synchronized across sockets (the common case), that's all you need: the
default build adds no per-mark overhead, and per-thread durations are exact on
any machine.

On multi-socket machines with *unsynchronized* TSCs, marks from different
sockets land on shifted timelines. Build with `unpinned-threads` to tag every
mark with its socket (one rdtscp per mark, in place of rdtsc); the reader then
calibrates a per-socket clock and places all marks on one wall-clock timeline,
correct even as threads migrate between sockets mid-capture.

Per-socket calibration is **Linux-only** — it pins the sampling thread to each
core via thread-affinity APIs that have no portable equivalent (e.g. macOS).
Other platforms fall back to a single-socket clock, which is correct for
single-socket machines (all Macs) but does not correct cross-socket TSC skew.

## License

Apache-2.0 AND MIT
