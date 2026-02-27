//! **flux-ctl** — CLI tool for managing and observing flux shared memory.
//!
//! Provides a ratatui TUI (`watch` command, default) and CLI commands (`list`,
//! `inspect`, `clean`, `scan`) for discovering shared memory segments via
//! filesystem scanning, viewing per-segment stats (queue writes, poison
//! status), and cleaning up stale segments.
//!
//! # Modules
//!
//! - [`discovery`] — filesystem-based segment discovery, inspection, cleanup
//! - [`tui`] — interactive terminal UI (app state, rendering, event loop)

pub mod discovery;
pub mod tui;
