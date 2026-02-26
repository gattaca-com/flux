# flux-ctl — Shared Memory Registry & TUI

## What is this?

`flux-ctl` is a CLI/TUI tool for the `flux` Rust workspace that provides a **single global shared memory registry** and a ratatui-based terminal UI for managing and observing shared memory queues, data segments, and seqlock arrays.

## Branch & Status

- **Branch:** `feat/flux-ctl-improvements` (based on `feat/flux-ctl-shmem-registry`)
- **Tests:** 119+ workspace-wide, 0 failures, 0 clippy warnings
- **Code:** ~5,900 lines across flux-ctl + registry

## Architecture

### Registry (`crates/flux-communication/src/registry.rs`)
- Lives in shared memory at `<base_dir>/flux/_shmem_registry`
- Single global registry shared by ALL apps (not per-app)
- `ShmemRegistry` uses `ShmemConf` directly (not `ShmemData`) to avoid circular registration
- Lock-free slot claiming via `fetch_add`, CAS-based `PidSet` (up to 256 PIDs per entry)
- `#[repr(C, align(64))]` layout, `REGISTRY_VERSION = 4`, `MAX_REGISTRY_ENTRIES = 4096`
- `ShmemEntry` (1472 bytes): kind, pids, app_name, type_name, flink, type_hash, elem_size, capacity, created_at_nanos
- Auto-registration from wrapper functions: `shmem_queue_with_base_dir()`, `open_or_init_with_base_dir()`, `shmem_array_with_base_dir()`
- Filesystem discovery: `populate_from_fs()` scans `<base_dir>/*/shmem/{queues,data,arrays}/*` with 5s cooldown
- Spin-lock protected `compact()` and `defragment()`, plus `health_check()` diagnostics

### CLI (`crates/flux-ctl/src/main.rs`)
- `flux-ctl` (bare) — launches TUI (defaults to `watch`)
- `flux-ctl list [--json] [--app NAME]` — list segments
- `flux-ctl inspect [--app NAME]` — detailed segment info
- `flux-ctl clean [--force] [--app NAME]` — remove dead segments
- `flux-ctl scan` — discover pre-existing shmem from filesystem
- `flux-ctl stats [--verbose]` — registry statistics + health diagnostics
- `flux-ctl --clean` — shorthand for `clean --force`

### TUI (`crates/flux-ctl/src/tui/`)
- **app.rs** — state machine: `View::List` / `View::Detail(DetailState)`, refresh logic, msgs/s tracking
- **render.rs** — ratatui rendering: list table, detail view, help popup, status bar
- **mod.rs** — event loop with extracted `handle_key()` function

#### List view columns
| Column | Content |
|--------|---------|
| Name | Segment type name |
| Kind | Queue / Data / SeqlockArray |
| Details | cap, elem size, msg/s rate |
| Conns | Number of connected processes |
| Status | 🟢 alive / 💀 dead / ☠ poisoned |

#### Key bindings
| Key | Action |
|-----|--------|
| j/k, ↑/↓ | Navigate |
| Enter | Open detail view |
| Esc | Back to list |
| / | Filter mode (search) |
| s | Cycle sort: Name → Kind → Status |
| g / G | First / last row |
| Home/End/PgUp/PgDn | Navigation |
| d | Destroy dead segments (Enter to confirm) |
| ? | Help popup |
| q | Quit |

#### Detail view shows
- Segment metadata (kind, app, flink, elem size, capacity, backing file size, OS ID)
- Write count + msgs/s rate (cyan)
- Write position bar (20-char, cyan)
- Poison info if detected
- PID table with process name, command, start time, status

### Discovery (`crates/flux-ctl/src/discovery/`)
- **registry.rs** — `open_registry()`, `scan()`, `app_names()`, `flink_reachable()`, `entry_visible()`
- **inspect.rs** — `check_poison()`, `read_queue_stats()`, `pid_info()`, `read_shmem_os_id()`, `backing_file_size()`
- **cli.rs** — `list()`, `inspect()`, `clean()`, `stats()` CLI implementations

## Key Design Decisions

1. **Observer PID filtering**: `populate_from_fs()` uses `PidSet::default()` (empty) so flux-ctl doesn't count itself as connected. `register()` skips `attach` when creator_pid is 0.
2. **Stale entry filtering**: `flink_reachable()` checks file metadata (exists + non-empty) instead of opening shmem — avoids mmap leaks.
3. **Cleanup safety**: Two-step confirm (d then Enter), stashed `pending_cleanup_flinks` prevents state drift between presses. `cleanup_flink()` returns `Result`.
4. **Init race fix**: `open_or_create` stores magic+version BEFORE `std::mem::forget(shmem)` with Release ordering.
5. **Msgs/s**: Tracked via `HashMap<flink, (prev_writes, prev_timestamp)>` in App, computed on each ~1s refresh.
6. **Poison detection**: Observation-only, reads raw `AtomicU64` at seqlock slots. `Producer::poison_next()` gated behind `#[cfg(feature = "poison-test")]`.

## Constraints

- Workspace edition 2024, rust-version 1.91.0
- No `anyhow` — use `Box<dyn std::error::Error>`
- No `libc` — use `std::process::id()`, `/proc` filesystem
- Do NOT modify `crates/spine-derive/`
- `flux_timing::Instant` uses `elapsed_since()` not `duration_since()`, `Duration::as_secs()` returns `f64`

## File Map

```
crates/flux-communication/src/registry.rs   — ShmemRegistry, ShmemEntry, PidSet, populate_from_fs (1125 lines)
crates/flux-ctl/src/main.rs                 — clap CLI definition (92 lines)
crates/flux-ctl/src/lib.rs                  — crate doc (14 lines)
crates/flux-ctl/src/tui/app.rs              — App state, refresh, navigation (594 lines)
crates/flux-ctl/src/tui/render.rs           — ratatui rendering (688 lines)
crates/flux-ctl/src/tui/mod.rs              — event loop, handle_key (139 lines)
crates/flux-ctl/src/discovery/cli.rs        — CLI command implementations (547 lines)
crates/flux-ctl/src/discovery/inspect.rs    — shmem inspection helpers (338 lines)
crates/flux-ctl/src/discovery/registry.rs   — registry access helpers (103 lines)
crates/flux-ctl/src/discovery/mod.rs        — re-exports (27 lines)
crates/flux-ctl/tests/tui_tests.rs          — 63 tests (2223 lines)
crates/flux-ctl/examples/demo.rs            — simple smoke test demo
crates/flux-ctl/examples/live.rs            — live demo with --workers, --poison flags
crates/flux-ctl/CHANGELOG.md                — feature summary
```

## Running

```bash
# TUI (default)
cargo run -p flux-ctl

# Live demo in another terminal
cargo run -p flux-ctl --example live -- --workers 3

# With poison testing
cargo run -p flux-ctl --example live --features poison-test -- --workers 3 --poison

# Tests
cargo test -p flux-ctl                # 66 tests
cargo test --workspace                # 119+ tests
```

## Potential Next Steps

- `TypeHash` bounds for layout mismatch detection (currently `type_hash: 0`)
- Move registration into low-level constructors for guaranteed coverage
- Per-queue latency stats
- Consider `serde` workspace deps for JSON output improvements
- Merge `feat/flux-ctl-improvements` → `feat/flux-ctl-shmem-registry` → main
