# flux-ctl

Command-line tool and interactive terminal UI for managing and observing **flux** shared memory segments.

`flux-ctl` opens the global `ShmemRegistry` and provides a live dashboard of all registered queues, seqlock arrays, and data segments — enriched with queue write counts, fill levels, poison detection, and per-PID process info.

## Installation

```bash
cargo install --path crates/flux-ctl
```

Or build from the workspace root:

```bash
cargo build -p flux-ctl --release
```

## CLI Usage

```
flux-ctl [OPTIONS] [COMMAND]
```

### Global Options

| Option | Description |
|---|---|
| `--base-dir <PATH>` | Override the base directory (default: `~/.local/share`) |
| `--clean` | Clean up all dead/stale segments and exit (shorthand for `clean --force`) |

### Commands

If no command is given, `flux-ctl` launches the **TUI monitor** (`watch`).

#### `watch` (default)

```bash
flux-ctl                   # launch TUI
flux-ctl watch --app myapp # filter to a single app
```

Interactive terminal UI that auto-refreshes every second. See [TUI Keybindings](#tui-keybindings) below.

#### `list`

```bash
flux-ctl list              # table of all segments
flux-ctl list --verbose    # include flink paths and queue internals
flux-ctl list --json       # JSON output (pipe to jq, etc.)
flux-ctl list --app myapp  # filter by app name
```

Lists all active shared memory segments grouped by application. Shows segment kind, type name, element size, capacity, attached PIDs, and status (alive / dead / poisoned).

#### `inspect`

```bash
flux-ctl inspect                     # inspect all segments
flux-ctl inspect myapp               # filter by app name
flux-ctl inspect myapp PriceUpdate   # filter by app and type name
```

Detailed per-segment view matching the TUI detail panel: kind, status, element size, capacity, type hash, creation timestamp, flink path, backing file size, write count (queues), poison info, and a table of attached processes with PID, status, process name, and command line.

#### `stats`

```bash
flux-ctl stats             # summary across all apps
flux-ctl stats --app myapp # summary for one app
```

Aggregate statistics: segment count, alive/dead/poisoned breakdown, kind distribution, total slot count, and estimated memory footprint.

#### `scan`

```bash
flux-ctl scan
```

Creates the registry if needed, walks the base directory for pre-existing shared memory segments not yet registered, and removes stale flinks whose backing file no longer exists.

#### `clean`

```bash
flux-ctl clean             # dry run: show what would be removed
flux-ctl clean --force     # actually unlink dead segments
flux-ctl clean --app myapp # scope to one app
```

Finds segments with no alive PIDs and (with `--force`) unlinks their backing files and compacts the registry.

## TUI Keybindings

### List View

| Key | Action |
|---|---|
| `↑` / `k` | Move selection up |
| `↓` / `j` | Move selection down |
| `Home` | Jump to first row |
| `End` | Jump to last row |
| `PgUp` | Page up (10 rows) |
| `PgDn` | Page down (10 rows) |
| `Enter` | Open segment detail / toggle app group expand/collapse |
| `Esc` | Clear active filter, or quit |
| `/` | Enter filter mode — type to filter segments by name |
| `s` | Cycle sort order: name → kind → status |
| `d` | Destroy selected dead segment (with confirmation) |
| `D` | Destroy **all** dead segments (with confirmation) |
| `r` | Force refresh |
| `?` | Toggle help popup |
| `q` | Quit |

### Detail View

| Key | Action |
|---|---|
| `↑` / `k` | Select previous PID in process table |
| `↓` / `j` | Select next PID |
| `Home` / `End` | Jump to first / last PID |
| `PgUp` / `PgDn` | Page through PID table |
| `Esc` / `Backspace` | Return to list view |
| `d` | Destroy this dead segment (with confirmation) |
| `D` | Destroy all dead segments (with confirmation) |
| `r` | Force refresh |
| `?` | Toggle help popup |
| `q` | Quit |

### Filter Mode

| Key | Action |
|---|---|
| _any character_ | Append to filter string (matches type name, app name, or kind) |
| `Backspace` | Delete last character |
| `Enter` | Confirm filter and return to normal navigation |
| `Esc` | Clear filter and return to normal navigation |

## Architecture

```
crates/flux-ctl/
├── src/
│   ├── main.rs              # CLI entry point (clap argument parsing, command dispatch)
│   ├── lib.rs               # Crate root, re-exports `discovery` and `tui` modules
│   ├── discovery/           # All non-UI logic for registry access and segment inspection
│   │   ├── mod.rs           # Module root with re-exports
│   │   ├── registry.rs      # open_registry, scan, app_names, entry_visible, flink_reachable
│   │   ├── inspect.rs       # PidInfo, PoisonInfo, QueueStats, check_poison, backing_file_size
│   │   └── cli.rs           # list_all, list_json, stats, inspect, clean commands
│   └── tui/                 # Interactive terminal UI
│       ├── mod.rs           # Event loop, key handling, terminal setup/teardown
│       ├── app.rs           # App state: groups, segments, selection, refresh, navigation
│       └── render.rs        # ratatui widget rendering (list view, detail view, popups)
├── tests/
│   └── tui_tests.rs         # Integration tests (synthetic + real registry)
├── examples/
│   ├── demo.rs
│   └── live.rs
├── Cargo.toml
└── README.md                # This file
```

### Module Responsibilities

**`discovery::registry`** — Opens the global `ShmemRegistry`, scans the filesystem to discover pre-existing segments, determines entry visibility (alive PIDs or reachable flink), and provides PID formatting helpers.

**`discovery::inspect`** — Low-level segment inspection: reads `/proc` to build `PidInfo` structs, scans seqlock buffers for poisoned slots (`PoisonInfo`), reads queue statistics (`QueueStats`) from shared memory headers, and reports backing file sizes.

**`discovery::cli`** — High-level CLI command implementations that combine registry and inspect primitives with terminal-aware formatting (colored output, column alignment, JSON serialization).

**`tui::app`** — Application state for the interactive monitor: `AppGroup`/`SegmentInfo` data model, 1-second refresh cycle, selection/navigation, sort/filter, and cleanup workflows with confirmation flow.

**`tui::render`** — Pure rendering functions: list view table, detail view (segment info panel + PID table), help popup, cleanup confirmation dialogs, and status bar.

### Data Flow

```
ShmemRegistry (lock-free shared memory)
       │
       ▼
discovery::registry::open_registry()   ← sweep dead PIDs, populate from fs
       │
       ├──► discovery::inspect::*      ← enrich with PidInfo, PoisonInfo, QueueStats
       │
       ├──► discovery::cli::*          ← CLI commands (list, inspect, stats, clean)
       │
       └──► tui::app::App::refresh()   ← TUI data model (SegmentInfo, AppGroup)
                    │
                    ▼
            tui::render::render()       ← ratatui terminal UI
```

## Dependencies

| Crate | Purpose |
|---|---|
| `flux-communication` | `ShmemRegistry`, `QueueHeader`, `ArrayHeader`, `ShmemEntry` |
| `flux-timing` | Low-overhead `Instant` / `Duration` for refresh intervals |
| `flux-utils` | `directories::local_share_dir()` default base path |
| `clap` | CLI argument parsing |
| `crossterm` | Terminal I/O, raw mode, styled output |
| `ratatui` | TUI widget framework |
| `shared_memory` | POSIX shared memory access |
| `serde` / `serde_json` | JSON serialization for `list --json` |
| `humantime` | RFC 3339 timestamp formatting |

## License

Apache-2.0 AND MIT
