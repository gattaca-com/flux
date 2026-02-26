# flux-ctl — Shared Memory Discovery & TUI

## What is this?

`flux-ctl` is a CLI/TUI tool for the `flux` Rust workspace that provides
**filesystem-based discovery** of shared memory segments and a ratatui-based
terminal UI for observing queues, data segments, and seqlock arrays.

## Architecture

### Discovery (`crates/flux-ctl/src/discovery/`)

Segments are discovered by walking `<base_dir>/<app>/shmem/{queues,data,arrays}/<TypeName>`.
No global shared-memory registry — the filesystem **is** the source of truth.

- **registry.rs** — `scan_base_dir()` walks the FS, opens each flink to read
  header metadata (elem_size, capacity), returns `Vec<DiscoveredEntry>`.
  `DiscoveredEntry` is a plain owned struct (not `repr(C)`, not in shmem).
- **inspect.rs** — `PoisonInfo`, `QueueStats`, `PidInfo`, `backing_file_size()`,
  `format_bytes()`, `read_shmem_os_id()`.
- **cli.rs** — `list_all()`, `list_json()`, `stats()`, `inspect()`, `clean()`
  CLI implementations, all driven by `scan_base_dir()`.

### Utilities kept in flux-communication (`crates/flux-communication/src/registry.rs`)

Only thin helpers remain — no `ShmemRegistry`, no `ShmemEntry`, no `PidSet`:

- `ShmemKind` enum (Queue, Data, SeqlockArray)
- `cleanup_flink(path)` — unlink shmem backing + remove flink file
- `cleanup_shmem(root)` — walk a tree, cleanup all flinks, remove dir
- `is_pid_alive(pid)` — `/proc/<pid>` existence check

### Data plane (`crates/flux-communication/src/`)

`shmem_queue()`, `shmem_array()`, `ShmemData::open_or_init()` create/open
shared memory segments with **no side effects** — no auto-registration,
no global singleton. They just create the flink file under the well-known
directory convention and return the handle.

### CLI (`crates/flux-ctl/src/main.rs`)

- `flux-ctl` (bare) — launches TUI (defaults to `watch`)
- `flux-ctl list [--json] [--app NAME]` — list segments
- `flux-ctl inspect [APP] [SEGMENT]` — detailed segment info
- `flux-ctl clean [--force] [--app NAME]` — remove stale flinks
- `flux-ctl scan [--json] [--app NAME]` — scan + display results
- `flux-ctl stats [--verbose]` — segment statistics

### TUI (`crates/flux-ctl/src/tui/`)

- **app.rs** — `App` state: `View::List` / `View::Detail`, refresh via
  `scan_base_dir()`, msgs/s tracking via write-count deltas
- **render.rs** — ratatui rendering: list table, detail view, help popup
- **mod.rs** — event loop with `handle_key()`

## File Map

```
crates/flux-communication/src/registry.rs   — ShmemKind, cleanup_flink, cleanup_shmem (~80 lines)
crates/flux-ctl/src/main.rs                 — clap CLI definition
crates/flux-ctl/src/discovery/registry.rs   — DiscoveredEntry, scan_base_dir, flink_reachable
crates/flux-ctl/src/discovery/cli.rs        — CLI command implementations
crates/flux-ctl/src/discovery/inspect.rs    — shmem inspection helpers
crates/flux-ctl/src/tui/app.rs              — App state, refresh, navigation
crates/flux-ctl/src/tui/render.rs           — ratatui rendering
crates/flux-ctl/tests/tui_tests.rs          — TUI + discovery tests
crates/flux-ctl/examples/demo.rs            — smoke test demo
crates/flux-ctl/examples/live.rs            — live demo with --workers, --poison
```

## Constraints

- Workspace edition 2024, rust-version 1.91.0
- **Read [STYLE.md](STYLE.md)** before writing any code
- No `anyhow` — use `thiserror` or `Box<dyn std::error::Error>`
- No `libc` — use `/proc` filesystem or `std` APIs
- `flux_timing::Instant` uses `elapsed_since()`, `Duration::as_secs()` returns `f64`
- Do NOT modify `crates/spine-derive/` without explicit approval
- Favour methods on types over free-standing functions
