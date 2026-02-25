//! **flux-ctl** — CLI tool for managing and observing flux shared memory.
//!
//! Provides a ratatui TUI (`watch` command, default) and CLI commands (`list`,
//! `inspect`, `clean`, `scan`) for inspecting the global `ShmemRegistry`,
//! viewing per-segment stats (queue writes, poison status, attached PIDs),
//! and cleaning up stale/dead shared memory segments.
//!
//! # Modules
//!
//! - [`discovery`] — registry access, segment inspection, cleanup logic
//! - [`tui`] — interactive terminal UI (app state, rendering, event loop)

pub mod discovery;
pub mod tui;
