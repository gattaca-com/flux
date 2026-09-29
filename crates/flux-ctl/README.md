# flux-ctl

Command-line tool and interactive terminal UI for managing and observing **flux** shared memory segments.

`flux-ctl` scans the base directory for shared memory segments and provides a live dashboard of all queues, seqlock arrays, and data segments — enriched with queue write counts, fill levels, poison detection, and per-PID process info.

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
flux-ctl watch myapp       # filter to a single app
```

Interactive terminal UI that auto-refreshes every second. See [TUI Keybindings](#tui-keybindings) below.

#### `list`

```bash
flux-ctl list              # table of all segments
flux-ctl list --verbose    # include flink paths and queue internals
flux-ctl list --json       # JSON output (pipe to jq, etc.)
flux-ctl list --app myapp  # filter by app name
```

Lists all active shared memory segments grouped by application. Shows segment kind, type name, element size, capacity, and status (alive / dead / poisoned).

#### `inspect`

```bash
flux-ctl inspect                     # inspect all segments
flux-ctl inspect myapp               # filter by app name
flux-ctl inspect myapp PriceUpdate   # filter by app and type name
```

Detailed per-segment view: kind, status, element size, capacity, flink path, backing file size, write count (queues), and poison info.

#### `stats`

```bash
flux-ctl stats             # summary across all apps
flux-ctl stats --app myapp # summary for one app
```

Aggregate statistics: segment count, alive/dead/poisoned breakdown, kind distribution, total slot count, and estimated memory footprint.

#### `clean`

```bash
flux-ctl clean             # dry run: show what would be removed
flux-ctl clean --force     # actually unlink dead segments
flux-ctl clean myapp       # scope to one app
```

Finds segments whose backing shared memory can no longer be opened and (with `--force`) removes their flink files.

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
| `Esc` / `q` | Clear active filter, or quit |
| `/` | Enter filter mode — type to filter segments by name |
| `s` | Cycle sort order: name → kind → status → activity |
| `d` | Destroy selected dead segment (with confirmation) |
| `D` | Destroy **all** dead segments (with confirmation) |
| `r` | Force refresh |
| `?` | Toggle help popup |

### Detail View

| Key | Action |
|---|---|
| `↑` / `k` | Select previous row in the focused table (consumer groups or PIDs) |
| `↓` / `j` | Select next row |
| `Home` / `End` | Jump to first / last row |
| `PgUp` / `PgDn` | Page through the focused table |
| `Tab` | Switch focus between consumer groups and PIDs |
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

## License

Apache-2.0 AND MIT
