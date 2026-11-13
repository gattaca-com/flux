//! **flux-ctl** — CLI tool for managing and observing flux shared memory.
//!
//! Provides a ratatui TUI (`watch` command, default) and CLI commands (`list`,
//! `inspect`, `clean`, `scan`) for discovering shared memory segments via
//! filesystem scanning, viewing per-segment stats (queue writes, poison
//! status), and cleaning up stale segments.
//! `record` writes an app's timer and tile-metric samples to Parquet.
//!
//! # Modules
//!
//! - [`discovery`] — filesystem-based segment discovery, inspection, cleanup
//! - [`record`] — headless recording of timer and tile-metric queues
//! - [`tui`] — interactive terminal UI (app state, rendering, event loop)

pub mod discovery;
pub mod record;
pub mod tui;

use std::borrow::Cow;

/// `s` with control characters escaped, for printing names and labels read
/// from the filesystem, shared memory or `/proc`: otherwise an escape sequence
/// in one would be interpreted by the operator's terminal. Display only, never
/// for a value used as a path or identifier.
pub(crate) fn printable(s: &str) -> Cow<'_, str> {
    if !s.chars().any(char::is_control) {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_control() {
            out.extend(c.escape_default());
        } else {
            out.push(c);
        }
    }
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::printable;

    #[test]
    fn printable_escapes_control_characters() {
        assert_eq!(printable("a\x1b]0;x\x07b\n\u{9b}"), "a\\u{1b}]0;x\\u{7}b\\n\\u{9b}");
    }
}
