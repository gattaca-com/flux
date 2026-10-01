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

#### `record`

```bash
flux-ctl record myapp                          # 60 s into ./myapp-<UTC time>/
flux-ctl record myapp --out run --duration 0   # until ^C, SIGTERM or SIGHUP
flux-ctl record myapp --match Tile-Msg         # only queues whose name contains this
flux-ctl record myapp --cores 3               # keep the recorder off the app's cores
```

Reads every sample of an app's `timing-*`, `latency-*` and `tilemetrics-*` queues into Parquet, then prints a per-queue table. It attaches to the queues that exist when it starts; a queue created later is not read. The output directory holds one recording, and `record` refuses a directory that already holds one.

| Option | Description |
|---|---|
| `--out <DIR>` | Output directory (default: `<app>-<UTC time>`) |
| `--duration <S>` | Seconds to record; 0 records until a signal (default: 60) |
| `--rotate-secs <S>` | Seconds per file; 0 writes one file per table (default: 300) |
| `--max-mb <MB>` | Stop once the files reach this many MB, 10^6 bytes; the last poll can pass it by a row group (default: 0, no limit) |
| `--match <TEXT>` | Only queues whose file name contains this |
| `--cores <N,...>` | Pin the recorder to these cores; an error if it cannot |
| `--nice <N>` | Niceness, -20 to 19; an error if it cannot be set (default: 10, which only warns if it cannot be set) |

It writes:

| File | Contents |
|---|---|
| `timers-<n>.parquet` | One row per timer sample: `wall_ns` (the handler's start on the wall clock), `timer`, `kind` (`timing` or `latency`), `dur_ns` |
| `tiles-<n>.parquet` | One row per tile-metric window: `window_end_ns`, `tile`, `window_ns`, `busy_ns`, `busy_max_ns`, `busy_count`, `loop_count` |
| `record.json` | Per queue: `samples`, `lost` (overwritten before they were read) and `invalid`; and the `late` queues (created after attach) and `skipped` ones (matched but did not open) |

A Parquet file is readable only once it is closed, so a recording killed without the chance to close (SIGKILL, a crash) loses only the file it had open, and writes no `record.json`. `read_parquet('timers-*.parquet')` reads the rotated files as one table.

#### `summarize`

```bash
flux-ctl summarize run                                     # the whole recording
flux-ctl summarize run --from 1790000000000000000 --to 1790000060000000000
```

Prints per-queue statistics of a `record` directory as JSON: for a timer, its sample count and `dur` percentiles (p50, p90, p99, p99.9, max, to 3 significant figures); for a tile, its window count, `busy_ns`, `window_ns`, utilisation and `busy_max` percentiles. `lost` and `invalid` come from `record.json` and cover the whole recording.

`--from` and `--to` bound the window in nanoseconds since the Unix epoch (`date +%s%N`), `--to` exclusive; a timer sample falls in it by its start, a tile window by its end. A file left without a footer is skipped and named in `unfinished`, and `late` and `skipped` repeat what `record.json` holds.

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
