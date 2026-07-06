# flux-profiler

A cross-process flamegraph profiler for flux. Annotate functions with `#[timed]`,
run your app, then attach the `flux-profiler` CLI to capture a trace you can open
in [magic-trace](https://magic-trace.org) or [Perfetto](https://ui.perfetto.dev).

Marks are written to per-thread shared-memory rings, so the profiler reads them
from a **separate process** with zero involvement from your app after startup.

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
# terminal 1 — your app (prints its pid)
cargo run -p my-app

# terminal 2 — attach, then Ctrl-C to stop and export the trace
cargo run -p flux-profiler --bin flux-profiler
```

If exactly one instrumented app is live, the profiler attaches automatically.
Otherwise pass `--pid <pid>`:

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
cargo run -p flux-profiler --bin flux-profiler
```

## Optional features

| Feature | What it adds |
|---|---|
| `perf` | Per-call hardware counters (instructions, cycles, branch/cache misses) via rdpmc. Requires `kernel.perf_event_paranoid <= 2` at runtime (`<= 1` to include kernel-mode work). |
| `alloc-profile` | Per-thread allocated/freed byte counts recorded alongside each `#[timed]` mark. Wraps the global allocator. |

```bash
cargo run -p flux-profiler --example timed_producer --features perf
```

## License

Apache-2.0 AND MIT
