# Changelog

All notable changes to `flux-ctl` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased] — Round 4

### Added

- **Registry robustness** — `ShmemRegistry::defragment()` deduplicates entries
  with identical flinks (merges PID sets, zeroes duplicates, then compacts).
  `ShmemRegistry::health_check()` returns diagnostic messages covering
  duplicates, stub entries, dead-unreachable segments, and capacity warnings.
- **Compact spin-lock** — `compact()` now uses a CAS on an `AtomicBool`
  (`compacting` field) to prevent concurrent compaction races. Returns 0 if
  another process holds the lock. Registry version bumped from 3 → 4.
- **Bounds-checked slot scanning** — `scan_seqlock_slots()` validates each
  slot offset against the remaining mapped region before reading, preventing
  out-of-bounds reads on corrupted or truncated segments.
- **`PopulateResult::skipped` field** — `populate_from_fs()` now tracks
  entries that could not be registered because the registry was full.
  `scan` and `list` commands print the skipped count.
- **`stats --verbose` / `stats -v`** — runs `health_check()` and appends
  diagnostic bullet points below the summary statistics.
- **Documentation** — doc comments added to all public functions and types
  in `discovery::cli`, `discovery::inspect`, and `discovery::registry`.

### Changed

- `REGISTRY_VERSION` bumped to 4 (auto-recreates v3 registries on next open).
- `ShmemRegistry::_pad: u32` replaced with `compacting: AtomicBool` + `_pad: [u8; 3]`
  (byte-compatible, no change in struct size).

### Fixed

- **Safety audit (A4)** — verified that no TUI code calls `open_shared()` or
  `std::mem::forget(shmem)`; all shmem opens go through read-only discovery
  helpers that properly `drop(shmem)` after use.
- `flink_reachable()` uses `std::fs::metadata` instead of `ShmemConf::open()`
  to avoid leaking mmaps.

## [0.3.0] — Round 3

### Added

- **Discovery module split** — `discovery.rs` refactored into submodules:
  `discovery::cli`, `discovery::inspect`, `discovery::registry`.
- **`flux-ctl inspect`** — detailed single-segment view with kind, status,
  element size, capacity, type hash, creation time, flink, backing file size,
  write count, poison status, and attached-process table.
- **`flux-ctl stats`** — summary statistics: alive/dead/poisoned counts,
  kind breakdown, total slots, estimated memory.
- **`flux-ctl list --json`** — machine-readable JSON output of all segments.
- **`flux-ctl clean`** — dry-run and `--force` modes for stale segment removal.
- **`flux-ctl scan`** — filesystem scan to register pre-existing shmem and
  remove stale flinks.
- **Queue stats** — `read_queue_stats()` reads `QueueHeader` fields (writes,
  fill, capacity) without leaking the mapping.
- **Backing file size** — `backing_file_size()` returns the shmem file size.
- **Byte formatting** — `format_bytes()` for human-readable KiB/MiB/GiB.
- Integration and unit tests for `populate_from_fs`, `scan`, `truncate_str`.
- Doc comments for `registry.rs`, `discovery.rs`, `lib.rs`.

### Changed

- Alignment and dead-code fixes across the codebase.
- `populate_from_fs` cached with 5-second cooldown.

## [0.2.0] — Round 2

### Added

- **TUI enhancements** — segment detail view with PID table and cleanup
  confirmation dialog; `?` help popup; `d` to clean dead segments;
  `D` (shift) to destroy all stale segments at once.
- **Poison detection** — `check_poison()` scans seqlock buffers for stuck
  (odd-version) slots in both Queue and SeqlockArray segments.
- **Multi-PID support** — `PidSet` with 256 slots per entry; dead PIDs
  swept on register and discovery open.
- **Registry header** — magic + version to detect layout mismatches.
- **`MAX_REGISTRY_ENTRIES`** bumped from 512 to 4096.
- **`--clean` flag** on the root command for quick stale cleanup.
- Auto-register `SeqlockArray` in the shmem registry.
- `populate_from_fs` for discovering pre-existing shmem on disk.

### Fixed

- Keys pass through when help popup is open.
- Return to list when the viewed segment is cleaned up.
- Box-drawing alignment in the live demo banner.

## [0.1.0] — Round 1

### Added

- **Shared memory registry** (`ShmemRegistry`) with `open_or_create`, `open`,
  `register`, `attach`, `detach_pid`, `compact`, `entries`, `find_by_flink`.
- **CLI** — `flux-ctl list`, `flux-ctl watch` (default), basic `--app` filter.
- **TUI** — ratatui-based live monitor with segment list, auto-refresh, and
  terminal colour support.
- **Discovery** — `open_registry`, `entry_visible`, `flink_reachable`,
  `app_names`, `format_pids`.
