use flux_communication::registry::{
    ShmemKind, ShmemRegistry, REGISTRY_FLINK_NAME, data_entry, queue_entry,
};
use flux_ctl::discovery;
use flux_ctl::tui::app::{App, AppGroup, SegmentInfo, SortMode, View};
use flux_ctl::tui::render;
use ratatui::{Terminal, backend::TestBackend, buffer::Buffer};
use shared_memory::ShmemConf;
use std::path::Path;

/// Helper: render an App into a TestBackend buffer and return the buffer.
fn render_to_buffer(app: &mut App, width: u16, height: u16) -> Buffer {
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.draw(|frame| render::render(frame, app)).unwrap();
    terminal.backend().buffer().clone()
}

/// Helper: extract all text from a Buffer as a single string (rows joined by newlines).
fn buffer_text(buf: &Buffer) -> String {
    let area = buf.area;
    let mut lines = Vec::new();
    for y in area.y..area.y + area.height {
        let mut line = String::new();
        for x in area.x..area.x + area.width {
            let cell = &buf[(x, y)];
            line.push_str(cell.symbol());
        }
        lines.push(line.trim_end().to_string());
    }
    lines.join("\n")
}

/// RAII guard that cleans up shmem when dropped.
struct ShmemGuard {
    base_dir: std::path::PathBuf,
}

impl Drop for ShmemGuard {
    fn drop(&mut self) {
        ShmemRegistry::destroy(&self.base_dir);
    }
}

/// Create a tempdir with an RAII guard that cleans up shmem on drop.
fn guarded_tmpdir() -> (tempfile::TempDir, ShmemGuard) {
    let tmpdir = tempfile::tempdir().unwrap();
    let guard = ShmemGuard {
        base_dir: tmpdir.path().to_path_buf(),
    };
    (tmpdir, guard)
}

// ─── Unit tests: buffer rendering with synthetic data ───────────────────────

#[test]
fn help_hint_visible_by_default() {
    let mut app = App::with_groups(vec![]);
    let buf = render_to_buffer(&mut app, 80, 20);
    let text = buffer_text(&buf);
    assert!(
        text.contains("? help"),
        "help hint should appear in status bar:\n{text}"
    );
}

#[test]
fn help_popup_toggle() {
    let mut app = App::with_groups(vec![]);
    assert!(!app.show_help);

    // Show help
    app.toggle_help();
    assert!(app.show_help);
    let buf = render_to_buffer(&mut app, 80, 24);
    let text = buffer_text(&buf);
    assert!(text.contains("Keybindings"), "popup should show title:\n{text}");
    assert!(text.contains("Move up"), "popup should list keys:\n{text}");
    assert!(text.contains("Quit"), "popup should show quit:\n{text}");

    // Dismiss
    app.toggle_help();
    assert!(!app.show_help);
}

#[test]
fn render_empty_app() {
    let mut app = App::with_groups(vec![]);
    let buf = render_to_buffer(&mut app, 80, 20);
    let text = buffer_text(&buf);
    assert!(
        text.contains("No segments found"),
        "should show empty message:\n{text}"
    );
}

#[test]
fn render_single_app_expanded() {
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![
            SegmentInfo {
                entry: queue_entry("myapp", "PriceUpdate", "/dev/shm/q1", 24, 1024),
                alive: true,
                pid_count: 1,
                queue_writes: Some(42), queue_fill: None, queue_capacity: None, poison: None,
            },
            SegmentInfo {
                entry: data_entry("myapp", "Config", "/dev/shm/d1", 128),
                alive: true,
                pid_count: 1,
                queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
            },
        ],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    assert_eq!(app.total_rows, 3); // 1 header + 2 segments

    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);

    assert!(text.contains("myapp"), "app name in TUI:\n{text}");
    assert!(text.contains("PriceUpdate"), "queue segment:\n{text}");
    assert!(text.contains("Config"), "data segment:\n{text}");
    assert!(text.contains("writes=42"), "write count:\n{text}");
    assert!(text.contains("Queue"), "queue kind:\n{text}");
    assert!(text.contains("Data"), "data kind:\n{text}");
    assert!(text.contains("alive"), "alive status:\n{text}");
}

#[test]
fn render_collapsed_app_hides_segments() {
    let groups = vec![AppGroup {
        name: "hidden".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("hidden", "Msg", "/dev/shm/q", 8, 16),
            alive: true,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: false,
    }];

    let mut app = App::with_groups(groups);
    assert_eq!(app.total_rows, 1); // just the header

    let buf = render_to_buffer(&mut app, 100, 15);
    let text = buffer_text(&buf);

    assert!(text.contains("hidden"), "app name:\n{text}");
    assert!(!text.contains("Msg"), "segment should be hidden:\n{text}");
    assert!(text.contains("▶"), "collapsed icon:\n{text}");
}

#[test]
fn render_multiple_apps() {
    let groups = vec![
        AppGroup {
            name: "alpha".into(),
            segments: vec![SegmentInfo {
                entry: queue_entry("alpha", "MsgA", "/dev/shm/a", 8, 16),
                alive: true,
                pid_count: 1,
                queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
            }],
            expanded: true,
        },
        AppGroup {
            name: "beta".into(),
            segments: vec![SegmentInfo {
                entry: data_entry("beta", "State", "/dev/shm/b", 64),
                alive: false,
                pid_count: 1,
                queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
            }],
            expanded: true,
        },
    ];

    let mut app = App::with_groups(groups);
    let buf = render_to_buffer(&mut app, 100, 20);
    let text = buffer_text(&buf);

    assert!(text.contains("alpha"), "first app:\n{text}");
    assert!(text.contains("beta"), "second app:\n{text}");
    assert!(text.contains("MsgA"), "first segment:\n{text}");
    assert!(text.contains("State"), "second segment:\n{text}");
}

#[test]
fn navigation_next_previous() {
    let groups = vec![
        AppGroup {
            name: "a".into(),
            segments: vec![SegmentInfo {
                entry: queue_entry("a", "Q1", "/dev/shm/q1", 8, 16),
                alive: true,
                pid_count: 1,
                queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
            }],
            expanded: true,
        },
        AppGroup {
            name: "b".into(),
            segments: vec![],
            expanded: true,
        },
    ];

    let mut app = App::with_groups(groups);
    assert_eq!(app.total_rows, 3); // a header, a/Q1, b header
    assert_eq!(app.selected, 0);

    app.next();
    assert_eq!(app.selected, 1);
    app.next();
    assert_eq!(app.selected, 2);
    app.next();
    assert_eq!(app.selected, 2, "should clamp at end");

    app.previous();
    assert_eq!(app.selected, 1);
    app.previous();
    assert_eq!(app.selected, 0);
    app.previous();
    assert_eq!(app.selected, 0, "should clamp at start");
}

#[test]
fn toggle_expand_collapse() {
    let groups = vec![AppGroup {
        name: "app".into(),
        segments: vec![
            SegmentInfo {
                entry: queue_entry("app", "Q1", "/dev/shm/q1", 8, 16),
                alive: true,
                pid_count: 1,
                queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
            },
            SegmentInfo {
                entry: queue_entry("app", "Q2", "/dev/shm/q2", 8, 16),
                alive: true,
                pid_count: 1,
                queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
            },
        ],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    assert_eq!(app.total_rows, 3); // 1 header + 2 segments

    // Collapse
    app.selected = 0;
    app.enter();
    assert_eq!(app.total_rows, 1);
    assert!(!app.groups[0].expanded);

    // Verify segments are hidden in render
    let buf = render_to_buffer(&mut app, 100, 15);
    let text = buffer_text(&buf);
    assert!(!text.contains("Q1"), "Q1 should be hidden:\n{text}");
    assert!(text.contains("▶"), "collapsed icon:\n{text}");

    // Expand again
    app.enter();
    assert_eq!(app.total_rows, 3);
    assert!(app.groups[0].expanded);

    let buf = render_to_buffer(&mut app, 100, 15);
    let text = buffer_text(&buf);
    assert!(text.contains("Q1"), "Q1 should be visible:\n{text}");
    assert!(text.contains("▼"), "expanded icon:\n{text}");
}

// ─── Integration tests: real shmem registry with cleanup ────────────────────

#[test]
fn registry_roundtrip_and_discovery() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path();

    let registry = ShmemRegistry::open_or_create(base);
    assert_eq!(registry.entry_count(), 0);

    registry.register(queue_entry("app1", "Msg", "/dev/shm/test_rr_q", 8, 16));
    registry.register(data_entry("app1", "Config", "/dev/shm/test_rr_d", 64));
    registry.register(queue_entry("app2", "Event", "/dev/shm/test_rr_e", 32, 256));

    assert_eq!(registry.entry_count(), 3);

    // Open from the flink path (as the CLI would)
    let registry_path = base.join(REGISTRY_FLINK_NAME);
    let reopened = ShmemRegistry::open(&registry_path).expect("should open existing registry");
    assert_eq!(reopened.entry_count(), 3);

    let names = discovery::app_names(reopened);
    assert_eq!(names, vec!["app1", "app2"]);
}

#[test]
fn app_from_real_registry() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path();

    let registry = ShmemRegistry::open_or_create(base);
    registry.register(queue_entry("demo", "PriceUpdate", "/dev/shm/test_afr_q", 24, 1024));
    registry.register(data_entry("demo", "AppState", "/dev/shm/test_afr_d", 1024));

    // Build App from the real base_dir
    let mut app = App::new(base, None);
    assert_eq!(app.groups.len(), 1);
    assert_eq!(app.groups[0].name, "demo");
    assert_eq!(app.groups[0].segments.len(), 2);
    assert_eq!(app.total_rows, 3); // 1 header + 2 segments

    // Render and verify
    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);
    assert!(text.contains("demo"), "app name in TUI:\n{text}");
    assert!(
        text.contains("PriceUpdate"),
        "queue segment in TUI:\n{text}"
    );
    assert!(text.contains("AppState"), "data segment in TUI:\n{text}");
}

#[test]
fn app_filter_limits_to_one_app() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path();

    let registry = ShmemRegistry::open_or_create(base);
    registry.register(queue_entry("alpha", "Msg", "/dev/shm/test_af_a", 8, 16));
    registry.register(queue_entry("beta", "Msg", "/dev/shm/test_af_b", 8, 16));

    let app = App::new(base, Some("alpha"));
    assert_eq!(app.groups.len(), 1);
    assert_eq!(app.groups[0].name, "alpha");
}

#[test]
fn clean_detects_stale_pids() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path();

    let registry = ShmemRegistry::open_or_create(base);

    // PID 1 (init, always alive) and PID 99999999 (almost certainly dead)
    let alive_entry = queue_entry("app", "Alive", "/dev/shm/test_cd_alive", 8, 16);
    // Attach PID 1 as an additional PID (creator is current process, also alive)
    let dead_entry = queue_entry("app", "Dead", "/dev/shm/test_cd_dead", 8, 16);

    registry.register(alive_entry);
    registry.register(dead_entry);

    // Attach PID 1 to the alive entry, and replace the dead entry's only PID
    // with 99999999 by detaching the creator and attaching the dead PID.
    let entries = registry.entries();
    entries[0].pids.attach(1);
    assert!(entries[0].pids.any_alive());

    let my_pid = std::process::id();
    entries[1].pids.detach(my_pid);
    entries[1].pids.attach(99999999);
    assert!(!entries[1].pids.any_alive());
}

#[test]
fn dead_segment_renders_skull() {
    let dead_entry = data_entry("ghost", "OldData", "/dev/shm/test_ds_old", 64);
    // Replace creator PID with a dead one
    dead_entry.pids.detach(std::process::id());
    dead_entry.pids.attach(99999999);

    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: dead_entry,
            alive: false,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    let buf = render_to_buffer(&mut app, 100, 15);
    let text = buffer_text(&buf);

    assert!(text.contains("dead"), "dead status should show:\n{text}");
    assert!(
        text.contains("OldData"),
        "segment name should show:\n{text}"
    );
}

// ─── PidSet tests ───────────────────────────────────────────────────────────

#[test]
fn multi_pid_attach_and_liveness() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path();

    let registry = ShmemRegistry::open_or_create(base);
    registry.register(queue_entry("multi", "Msg", "/dev/shm/test_mp", 8, 16));

    let entries = registry.entries();
    let entry = &entries[0];

    // Creator PID (ourselves) is alive
    assert_eq!(entry.pids.count(), 1);
    assert!(entry.pids.any_alive());

    // Attach a second (dead) PID
    entry.pids.attach(99999999);
    assert_eq!(entry.pids.count(), 2);
    // Still alive because our PID is there
    assert!(entry.pids.any_alive());

    // Detach ourselves — now only dead PID remains
    let my_pid = std::process::id();
    entry.pids.detach(my_pid);
    assert_eq!(entry.pids.count(), 1);
    assert!(!entry.pids.any_alive());
}

#[test]
fn register_same_flink_attaches_pid() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path();

    let registry = ShmemRegistry::open_or_create(base);
    registry.register(queue_entry("app", "Msg", "/dev/shm/test_rsf", 8, 16));
    assert_eq!(registry.entry_count(), 1);

    // Register again with same flink — should attach, not create new entry
    registry.register(queue_entry("app", "Msg", "/dev/shm/test_rsf", 8, 16));
    assert_eq!(registry.entry_count(), 1, "should reuse existing entry");

    // The entry should still have 1 PID (same process registered twice)
    assert_eq!(registry.entries()[0].pids.count(), 1);
}

#[test]
fn attach_detach_by_flink() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path();

    let registry = ShmemRegistry::open_or_create(base);
    registry.register(queue_entry("app", "Msg", "/dev/shm/test_ad", 8, 16));

    // Attach a fake PID via the registry method
    assert!(registry.attach_pid("/dev/shm/test_ad", 12345));
    assert_eq!(registry.entries()[0].pids.count(), 2);

    // Detach it
    assert!(registry.detach_pid("/dev/shm/test_ad", 12345));
    assert_eq!(registry.entries()[0].pids.count(), 1);

    // Detach non-existent returns false
    assert!(!registry.detach_pid("/dev/shm/test_ad", 12345));

    // Attach to non-existent flink returns false
    assert!(!registry.attach_pid("/dev/shm/nonexistent", 12345));
}

#[test]
fn multi_pid_renders_count() {
    let entry = queue_entry("multi", "Msg", "/dev/shm/q", 8, 16);
    entry.pids.attach(12345);
    entry.pids.attach(67890);

    let groups = vec![AppGroup {
        name: "multi".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 3,
            queue_writes: Some(100), queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    let buf = render_to_buffer(&mut app, 120, 15);
    let text = buffer_text(&buf);

    // Should show "×3" for 3 attached PIDs
    assert!(text.contains("×3"), "should show multi-pid count:\n{text}");
}

#[test]
fn sweep_dead_pids_removes_stale() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path();

    let registry = ShmemRegistry::open_or_create(base);
    registry.register(queue_entry("app", "Msg", "/dev/shm/test_sweep", 8, 16));

    // Attach several dead PIDs
    let entry = &registry.entries()[0];
    entry.pids.attach(99999990);
    entry.pids.attach(99999991);
    entry.pids.attach(99999992);
    assert_eq!(entry.pids.count(), 4); // self + 3 dead

    // Sweep at entry level
    let removed = entry.pids.sweep_dead();
    assert_eq!(removed, 3);
    assert_eq!(entry.pids.count(), 1); // only self remains

    // Re-add dead PIDs and test registry-level sweep
    entry.pids.attach(99999993);
    entry.pids.attach(99999994);
    assert_eq!(entry.pids.count(), 3);
    let total = registry.sweep_dead_pids();
    assert_eq!(total, 2);
    assert_eq!(entry.pids.count(), 1);
}

#[test]
fn register_reattach_sweeps_dead() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path();

    let registry = ShmemRegistry::open_or_create(base);
    registry.register(queue_entry("app", "Msg", "/dev/shm/test_rsweep", 8, 16));

    // Simulate dead PIDs from previous runs
    let entry = &registry.entries()[0];
    entry.pids.attach(99999990);
    entry.pids.attach(99999991);
    assert_eq!(entry.pids.count(), 3);

    // Re-register same flink — should sweep dead PIDs before attaching
    registry.register(queue_entry("app", "Msg", "/dev/shm/test_rsweep", 8, 16));
    assert_eq!(registry.entry_count(), 1, "should still be 1 entry");
    assert_eq!(entry.pids.count(), 1, "dead PIDs should have been swept");
}

// ─── Detail view tests ──────────────────────────────────────────────────────

#[test]
fn enter_on_segment_opens_detail() {
    let entry = queue_entry("myapp", "Quote", "/dev/shm/test_esd", 24, 64);
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: Some(42), queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    // Row 0 = app header, Row 1 = segment
    app.selected = 1;
    app.enter();

    assert!(
        matches!(app.view, View::Detail(_)),
        "should switch to detail view"
    );
}

#[test]
fn detail_view_renders_segment_info() {
    let entry = queue_entry("myapp", "Quote", "/dev/shm/test_dvr", 24, 64);
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: Some(42), queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.enter();

    let buf = render_to_buffer(&mut app, 120, 30);
    let text = buffer_text(&buf);

    assert!(text.contains("Quote"), "type name:\n{text}");
    assert!(text.contains("myapp"), "app name:\n{text}");
    assert!(text.contains("Queue"), "kind:\n{text}");
    assert!(text.contains("24 bytes"), "elem size:\n{text}");
    assert!(text.contains("64"), "capacity:\n{text}");
    assert!(text.contains("Flink"), "flink label:\n{text}");
    assert!(
        text.contains("Attached Processes"),
        "PID table header:\n{text}"
    );
}

#[test]
fn detail_view_shows_pid_info() {
    let entry = queue_entry("myapp", "Quote", "/dev/shm/test_dvp", 24, 64);
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.enter();

    let buf = render_to_buffer(&mut app, 120, 30);
    let text = buffer_text(&buf);

    // Our own PID should be shown and marked alive
    let our_pid = std::process::id().to_string();
    assert!(text.contains(&our_pid), "our PID should show:\n{text}");
    assert!(text.contains("alive"), "should show alive:\n{text}");
}

#[test]
fn detail_view_back_returns_to_list() {
    let entry = queue_entry("myapp", "Quote", "/dev/shm/test_dvb", 24, 64);
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.enter();
    assert!(matches!(app.view, View::Detail(_)));

    app.back();
    assert!(matches!(app.view, View::List));
}

#[test]
fn detail_cleanup_blocked_for_alive() {
    let entry = queue_entry("myapp", "Quote", "/dev/shm/test_dcb", 24, 64);
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.enter();

    // Trying to clean a live segment should be blocked
    app.request_cleanup();
    assert!(
        app.status_msg.is_some(),
        "should show error message for live segment"
    );
    if let View::Detail(ref d) = app.view {
        assert!(!d.confirm_cleanup, "should not show confirm for live segment");
    }
}

#[test]
fn detail_cleanup_shows_confirm_for_dead() {
    let dead_entry = data_entry("ghost", "OldData", "/dev/shm/test_dcc", 64);
    dead_entry.pids.detach(std::process::id());
    dead_entry.pids.attach(99999999);

    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: dead_entry,
            alive: false,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.enter();

    // First press: should show confirm prompt
    app.request_cleanup();
    if let View::Detail(ref d) = app.view {
        assert!(d.confirm_cleanup, "should show confirm prompt");
    } else {
        panic!("should still be in detail view");
    }

    // Render and check confirm popup appears
    let buf = render_to_buffer(&mut app, 120, 30);
    let text = buffer_text(&buf);
    assert!(
        text.contains("Clean up this segment"),
        "confirm popup should appear:\n{text}"
    );
}

#[test]
fn detail_cancel_cleanup_hides_confirm() {
    let dead_entry = data_entry("ghost", "OldData", "/dev/shm/test_dch", 64);
    dead_entry.pids.detach(std::process::id());
    dead_entry.pids.attach(99999999);

    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: dead_entry,
            alive: false,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.enter();

    app.request_cleanup(); // show confirm
    app.cancel_cleanup();  // cancel

    if let View::Detail(ref d) = app.view {
        assert!(!d.confirm_cleanup, "confirm should be cancelled");
    }
}

#[test]
fn detail_status_bar_shows_cleanup_hint_for_dead() {
    let dead_entry = data_entry("ghost", "OldData", "/dev/shm/test_dsh", 64);
    dead_entry.pids.detach(std::process::id());
    dead_entry.pids.attach(99999999);

    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: dead_entry,
            alive: false,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.enter();

    let buf = render_to_buffer(&mut app, 120, 30);
    let text = buffer_text(&buf);

    // Status bar should hint about cleanup for dead segments
    assert!(
        text.contains("d destroy"),
        "status bar should show destroy hint for dead segment:\n{text}"
    );
}

#[test]
fn enter_on_app_header_still_toggles() {
    let entry = queue_entry("myapp", "Msg", "/dev/shm/test_eoah", 8, 16);
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 0; // app header row
    app.enter();

    // Should toggle collapse, not enter detail
    assert!(matches!(app.view, View::List));
    assert!(!app.groups[0].expanded);
}

// ─── List-view cleanup tests ────────────────────────────────────────────────

#[test]
fn list_cleanup_blocked_for_alive_segment() {
    let entry = queue_entry("myapp", "Msg", "/dev/shm/test_lcba", 8, 16);
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1; // segment row
    app.request_cleanup();

    assert!(!app.confirm_cleanup, "should not confirm for live segment");
    assert!(app.status_msg.is_some(), "should show error message");
}

#[test]
fn list_cleanup_blocked_on_app_header() {
    let entry = queue_entry("myapp", "Msg", "/dev/shm/test_lcbh", 8, 16);
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 0; // app header row
    app.request_cleanup();

    assert!(!app.confirm_cleanup);
    assert!(app.status_msg.is_some(), "should say 'select a segment'");
}

#[test]
fn list_cleanup_shows_confirm_for_dead() {
    let dead_entry = data_entry("ghost", "OldData", "/dev/shm/test_lcsc", 64);
    dead_entry.pids.detach(std::process::id());
    dead_entry.pids.attach(99999999);

    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: dead_entry,
            alive: false,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.request_cleanup();

    assert!(app.confirm_cleanup, "first press should show confirm");

    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);
    assert!(text.contains("Clean up this segment"), "confirm popup:\n{text}");
}

#[test]
fn list_cancel_cleanup_hides_confirm() {
    let dead_entry = data_entry("ghost", "OldData", "/dev/shm/test_lcch", 64);
    dead_entry.pids.detach(std::process::id());
    dead_entry.pids.attach(99999999);

    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: dead_entry,
            alive: false,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.request_cleanup();
    assert!(app.confirm_cleanup);

    app.cancel_cleanup();
    assert!(!app.confirm_cleanup);
}

#[test]
fn list_status_bar_shows_cleanup_hint_for_dead() {
    let dead_entry = data_entry("ghost", "OldData", "/dev/shm/test_lsbh", 64);
    dead_entry.pids.detach(std::process::id());
    dead_entry.pids.attach(99999999);

    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: dead_entry,
            alive: false,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1; // dead segment row

    let buf = render_to_buffer(&mut app, 120, 15);
    let text = buffer_text(&buf);

    assert!(
        text.contains("d destroy"),
        "status bar should show destroy hint:\n{text}"
    );
}

// ─── Poison detection tests ─────────────────────────────────────────────────

#[test]
fn poisoned_segment_renders_skull_crossbones() {
    let entry = queue_entry("myapp", "Quote", "/dev/shm/test_poison_render", 24, 64);
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: Some(100),
            queue_fill: None,
            queue_capacity: None,
            poison: Some(discovery::PoisonInfo {
                n_poisoned: 2,
                first_slot: 5,
                total_slots: 64,
            }),
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    let buf = render_to_buffer(&mut app, 120, 15);
    let text = buffer_text(&buf);
    assert!(text.contains("poisoned"), "list should show poisoned status:\n{text}");
}

#[test]
fn poisoned_detail_view_shows_poison_info() {
    let entry = queue_entry("myapp", "Quote", "/dev/shm/test_poison_detail", 24, 64);
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: Some(100),
            queue_fill: None,
            queue_capacity: None,
            poison: Some(discovery::PoisonInfo {
                n_poisoned: 3,
                first_slot: 7,
                total_slots: 64,
            }),
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.enter();

    let buf = render_to_buffer(&mut app, 120, 30);
    let text = buffer_text(&buf);

    assert!(text.contains("poisoned"), "detail status should show poisoned:\n{text}");
    assert!(text.contains("3/64"), "should show n_poisoned/total:\n{text}");
    assert!(text.contains("slot 7"), "should show first slot:\n{text}");
}

#[test]
fn healthy_segment_has_no_poison() {
    let entry = queue_entry("myapp", "Quote", "/dev/shm/test_no_poison", 24, 64);
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: Some(42),
            queue_fill: None,
            queue_capacity: None,
            poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    let buf = render_to_buffer(&mut app, 120, 15);
    let text = buffer_text(&buf);

    assert!(!text.contains("poisoned"), "healthy segment should not show poisoned:\n{text}");
    assert!(text.contains("alive"), "should show alive:\n{text}");
}

// ─── Destroy-all tests ──────────────────────────────────────────────────────

#[test]
fn destroy_all_shows_confirm_when_dead_exist() {
    let dead = data_entry("ghost", "Old", "/dev/shm/test_da_confirm", 64);
    dead.pids.detach(std::process::id());
    dead.pids.attach(99999999);

    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: dead,
            alive: false,
            pid_count: 1,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.request_cleanup_all();
    assert!(app.confirm_cleanup_all);

    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);
    assert!(text.contains("Destroy all"), "confirm popup:\n{text}");
    assert!(text.contains("1 stale"), "should show count:\n{text}");
}

#[test]
fn destroy_all_noop_when_all_alive() {
    let entry = queue_entry("myapp", "Msg", "/dev/shm/test_da_alive", 8, 16);
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.request_cleanup_all();
    assert!(!app.confirm_cleanup_all);
    assert!(app.status_msg.is_some(), "should say no stale segments");
}

#[test]
fn destroy_all_cancel_hides_confirm() {
    let dead = data_entry("ghost", "Old", "/dev/shm/test_da_cancel", 64);
    dead.pids.detach(std::process::id());
    dead.pids.attach(99999999);

    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: dead,
            alive: false,
            pid_count: 1,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.request_cleanup_all();
    assert!(app.confirm_cleanup_all);
    app.cancel_cleanup();
    assert!(!app.confirm_cleanup_all);
}

#[test]
fn status_bar_shows_destroy_all_hint() {
    let dead = data_entry("ghost", "Old", "/dev/shm/test_da_hint", 64);
    dead.pids.detach(std::process::id());
    dead.pids.attach(99999999);

    let alive_entry = queue_entry("myapp", "Msg", "/dev/shm/test_da_hint2", 8, 16);

    let groups = vec![
        AppGroup {
            name: "ghost".into(),
            segments: vec![SegmentInfo {
                entry: dead,
                alive: false,
                pid_count: 1,
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
            }],
            expanded: true,
        },
        AppGroup {
            name: "myapp".into(),
            segments: vec![SegmentInfo {
                entry: alive_entry,
                alive: true,
                pid_count: 1,
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
            }],
            expanded: true,
        },
    ];

    // Cursor on alive segment — should still show "D destroy all" since dead exist
    let mut app = App::with_groups(groups);
    app.selected = 3; // myapp > Msg (alive)

    let buf = render_to_buffer(&mut app, 120, 15);
    let text = buffer_text(&buf);
    assert!(
        text.contains("D destroy all"),
        "status bar should show D hint when dead segments exist:\n{text}"
    );
}

// ─── populate_from_fs / scan tests ──────────────────────────────────────────

/// Helper: create a real shmem segment via ShmemConf (bypassing the registry)
/// and place the flink under `base_dir/<app>/shmem/<subdir>/<name>`.
fn create_raw_shmem(base_dir: &Path, app: &str, subdir: &str, name: &str, size: usize) {
    let flink_dir = base_dir.join(app).join("shmem").join(subdir);
    std::fs::create_dir_all(&flink_dir).unwrap();
    let flink_path = flink_dir.join(name);
    let mut shmem = ShmemConf::new()
        .size(size)
        .flink(&flink_path)
        .create()
        .unwrap();
    // Disown so drop doesn't unlink the backing file.
    shmem.set_owner(false);
    drop(shmem);
}

/// Helper: clean up a raw shmem segment created by `create_raw_shmem`.
fn cleanup_raw_shmem(base_dir: &Path, app: &str, subdir: &str, name: &str) {
    let flink_path = base_dir.join(app).join("shmem").join(subdir).join(name);
    if let Ok(mut shmem) = ShmemConf::new().flink(&flink_path).open() {
        shmem.set_owner(true);
        // drop unlinks the backing
    }
    let _ = std::fs::remove_file(&flink_path);
}

#[test]
fn populate_from_fs_discovers_preexisting_shmem() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    // Create raw shmem segments (no registry involvement)
    create_raw_shmem(base, "testapp", "data", "MyStruct", 256);

    // Create and populate the registry
    let reg = ShmemRegistry::open_or_create(base);
    // Reset cooldown so populate actually runs
    reg.last_populated_nanos
        .store(0, std::sync::atomic::Ordering::Relaxed);
    let result = reg.populate_from_fs(base);

    assert_eq!(result.registered, 1, "should discover 1 pre-existing segment");
    assert_eq!(result.stale_removed, 0);

    // Verify the entry is in the registry
    let entries = reg.entries();
    let found = entries.iter().any(|e| {
        e.app_name.as_str() == "testapp" && e.type_name.as_str() == "MyStruct"
    });
    assert!(found, "pre-existing segment should appear in registry entries");

    // Second call should skip (already known)
    reg.last_populated_nanos
        .store(0, std::sync::atomic::Ordering::Relaxed);
    let result2 = reg.populate_from_fs(base);
    assert_eq!(result2.already_known, 1);
    assert_eq!(result2.registered, 0);

    // Cleanup
    cleanup_raw_shmem(base, "testapp", "data", "MyStruct");
    ShmemRegistry::destroy(base);
}

#[test]
fn populate_from_fs_removes_stale_flinks() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    // Create a stale flink (file exists but no backing shmem)
    let flink_dir = base.join("staleapp").join("shmem").join("queues");
    std::fs::create_dir_all(&flink_dir).unwrap();
    let stale_flink = flink_dir.join("GhostQueue");
    std::fs::write(&stale_flink, "bogus_os_id").unwrap();

    // Set the flink file's mtime to >60s ago so it is not treated as a
    // recent file by the mtime fast-path in populate_from_fs.
    let old_time = std::time::SystemTime::now() - std::time::Duration::from_secs(120);
    let times = std::fs::FileTimes::new()
        .set_modified(old_time)
        .set_accessed(old_time);
    std::fs::File::options()
        .write(true)
        .open(&stale_flink)
        .unwrap()
        .set_times(times)
        .unwrap();

    let reg = ShmemRegistry::open_or_create(base);
    reg.last_populated_nanos
        .store(0, std::sync::atomic::Ordering::Relaxed);
    let result = reg.populate_from_fs(base);

    assert_eq!(result.stale_removed, 1, "should remove the stale flink");
    assert!(!stale_flink.exists(), "stale flink file should be deleted");

    ShmemRegistry::destroy(base);
}

#[test]
fn scan_on_empty_base_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    // scan() should succeed on an empty directory
    let result = discovery::scan(base);
    assert!(result.is_ok(), "scan on empty dir should not error");

    ShmemRegistry::destroy(base);
}

#[test]
fn populate_cooldown_skips_repeated_scans() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    create_raw_shmem(base, "coolapp", "data", "CoolData", 128);

    let reg = ShmemRegistry::open_or_create(base);
    // First scan
    let r1 = reg.populate_from_fs(base);
    assert_eq!(r1.registered, 1);

    // Immediate second scan — should be skipped due to cooldown
    let r2 = reg.populate_from_fs(base);
    assert_eq!(r2.registered, 0);
    assert_eq!(r2.already_known, 0); // didn't even run the scan
    assert_eq!(r2.stale_removed, 0);

    // Cleanup
    cleanup_raw_shmem(base, "coolapp", "data", "CoolData");
    ShmemRegistry::destroy(base);
}

// ─── Phase 6: New tests for filter, sort, navigation, stress, compact, entry_by_index ──

#[test]
fn filter_narrows_visible_segments() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path();

    let registry = ShmemRegistry::open_or_create(base);
    registry.register(queue_entry("myapp", "Quote", "/dev/shm/test_fn_q", 24, 64));
    registry.register(queue_entry("myapp", "Trade", "/dev/shm/test_fn_t", 24, 64));
    registry.register(data_entry("myapp", "Order", "/dev/shm/test_fn_o", 128));

    // No filter: all three segments should appear
    let mut app = App::new(base, None);
    assert_eq!(app.groups.len(), 1);
    assert_eq!(app.groups[0].segments.len(), 3);

    // Set filter_text and refresh — only "Quote" should survive
    app.filter_text = "quo".into();
    app.refresh();
    assert_eq!(app.groups.len(), 1, "app group should still exist");
    assert_eq!(
        app.groups[0].segments.len(),
        1,
        "only matching segment should remain"
    );
    assert_eq!(app.groups[0].segments[0].entry.type_name.as_str(), "Quote");

    // Filter that matches nothing: groups should be empty
    app.filter_text = "nonexistent".into();
    app.refresh();
    assert_eq!(app.groups.len(), 0, "no groups when filter matches nothing");

    // Empty filter restores all segments
    app.filter_text.clear();
    app.refresh();
    assert_eq!(app.groups[0].segments.len(), 3);
}

#[test]
fn sort_mode_cycles_through_all_variants() {
    // Verify the enum cycling: Name → Kind → Status → Name
    assert_eq!(SortMode::Name.next(), SortMode::Kind);
    assert_eq!(SortMode::Kind.next(), SortMode::Status);
    assert_eq!(SortMode::Status.next(), SortMode::Name);

    // Verify labels
    assert_eq!(SortMode::Name.label(), "name");
    assert_eq!(SortMode::Kind.label(), "kind");
    assert_eq!(SortMode::Status.label(), "status");

    // Verify toggle_sort() on a synthetic App updates sort_mode
    // (refresh() will be a no-op since base_dir is empty)
    let mut app = App::with_groups(vec![]);
    assert_eq!(app.sort_mode, SortMode::Name);
    app.sort_mode = app.sort_mode.next();
    assert_eq!(app.sort_mode, SortMode::Kind);
    app.sort_mode = app.sort_mode.next();
    assert_eq!(app.sort_mode, SortMode::Status);
    app.sort_mode = app.sort_mode.next();
    assert_eq!(app.sort_mode, SortMode::Name);
}

#[test]
fn home_end_navigation() {
    let groups = vec![
        AppGroup {
            name: "a".into(),
            segments: vec![
                SegmentInfo {
                    entry: queue_entry("a", "S1", "/dev/shm/he1", 8, 16),
                    alive: true,
                    pid_count: 1,
                    queue_writes: None,
                    queue_fill: None,
                    queue_capacity: None,
                    poison: None,
                },
                SegmentInfo {
                    entry: queue_entry("a", "S2", "/dev/shm/he2", 8, 16),
                    alive: true,
                    pid_count: 1,
                    queue_writes: None,
                    queue_fill: None,
                    queue_capacity: None,
                    poison: None,
                },
            ],
            expanded: true,
        },
        AppGroup {
            name: "b".into(),
            segments: vec![SegmentInfo {
                entry: data_entry("b", "S3", "/dev/shm/he3", 64),
                alive: true,
                pid_count: 1,
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
            }],
            expanded: true,
        },
    ];

    let mut app = App::with_groups(groups);
    // total_rows = 2 headers + 3 segments = 5
    assert_eq!(app.total_rows, 5);
    assert_eq!(app.selected, 0);

    // End → last row
    app.end();
    assert_eq!(app.selected, 4);

    // Home → first row
    app.home();
    assert_eq!(app.selected, 0);

    // End on empty app is safe
    let mut empty = App::with_groups(vec![]);
    empty.end();
    assert_eq!(empty.selected, 0);
    empty.home();
    assert_eq!(empty.selected, 0);
}

#[test]
fn page_up_page_down_navigation() {
    // Build 25 expanded groups with 1 segment each → 50 total rows
    let groups: Vec<AppGroup> = (0..25)
        .map(|i| AppGroup {
            name: format!("app{i:02}"),
            segments: vec![SegmentInfo {
                entry: queue_entry(
                    &format!("app{i:02}"),
                    &format!("Seg{i:02}"),
                    &format!("/dev/shm/test_pp_{i}"),
                    8,
                    16,
                ),
                alive: true,
                pid_count: 1,
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
            }],
            expanded: true,
        })
        .collect();

    let mut app = App::with_groups(groups);
    assert_eq!(app.total_rows, 50); // 25 headers + 25 segments
    assert_eq!(app.selected, 0);

    // PageDown jumps by 10
    app.page_down();
    assert_eq!(app.selected, 10);

    app.page_down();
    assert_eq!(app.selected, 20);

    // PageUp jumps back by 10
    app.page_up();
    assert_eq!(app.selected, 10);

    app.page_up();
    assert_eq!(app.selected, 0);

    // PageUp at 0 stays at 0 (no underflow)
    app.page_up();
    assert_eq!(app.selected, 0);

    // Jump to end, then PageDown should clamp
    app.end();
    assert_eq!(app.selected, 49);
    app.page_down();
    assert_eq!(app.selected, 49);
}

#[test]
fn stress_test_many_entries() {
    // 50 groups × 5 segments each = 300 total rows (50 headers + 250 segments)
    let groups: Vec<AppGroup> = (0..50)
        .map(|gi| AppGroup {
            name: format!("app{gi:03}"),
            segments: (0..5)
                .map(|si| SegmentInfo {
                    entry: queue_entry(
                        &format!("app{gi:03}"),
                        &format!("Seg{si}"),
                        &format!("/dev/shm/test_stress_{gi}_{si}"),
                        8,
                        16,
                    ),
                    alive: true,
                    pid_count: 1,
                    queue_writes: Some((gi * 5 + si) as usize),
                    queue_fill: None,
                    queue_capacity: None,
                    poison: None,
                })
                .collect(),
            expanded: true,
        })
        .collect();

    let mut app = App::with_groups(groups);
    assert_eq!(app.total_rows, 300);

    // End navigates to last row
    app.end();
    assert_eq!(app.selected, 299);

    // Home navigates back to first
    app.home();
    assert_eq!(app.selected, 0);

    // Render should not panic even with many rows
    let buf = render_to_buffer(&mut app, 120, 40);
    let text = buffer_text(&buf);
    assert!(text.contains("app000"), "first app should render:\n{text}");

    // Navigate to end and render
    app.end();
    let buf = render_to_buffer(&mut app, 120, 40);
    let _text = buffer_text(&buf);
    // Just verify no panic — the last group may or may not be visible
    // depending on scroll, but it should not crash.
}

#[test]
fn registry_compact_removes_empty_slots() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path();

    let registry = ShmemRegistry::open_or_create(base);
    registry.register(queue_entry("a", "First", "/dev/shm/test_compact_1", 8, 16));
    registry.register(queue_entry("b", "Middle", "/dev/shm/test_compact_2", 8, 16));
    registry.register(queue_entry("c", "Last", "/dev/shm/test_compact_3", 8, 16));

    assert_eq!(registry.entry_count(), 3);

    // Corrupt the middle entry by setting its kind to ShmemKind::Unknown (value 0).
    // ShmemEntry is repr(C, align(64)) and `kind` (ShmemKind, repr(u8)) is the first field.
    // We use addr_of! on the entries slice element to avoid going through &mut.
    let entries = registry.entries();
    let kind_ptr = std::ptr::addr_of!(entries[1].kind) as *mut u8;
    unsafe {
        kind_ptr.write_volatile(ShmemKind::Unknown as u8);
    }
    // Sanity check: the middle entry should now appear empty.
    assert!(entries[1].is_empty(), "middle entry should be marked empty");

    // Compact should remove the empty slot and shift entries.
    let removed = registry.compact();
    assert_eq!(removed, 1, "one empty entry should have been removed");
    assert_eq!(registry.entry_count(), 2);

    // Verify remaining entries are the first and last.
    let entries = registry.entries();
    assert_eq!(entries[0].type_name.as_str(), "First");
    assert_eq!(entries[1].type_name.as_str(), "Last");
}

#[test]
fn registry_entry_by_index_bounds_check() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path();

    let registry = ShmemRegistry::open_or_create(base);
    registry.register(queue_entry("x", "A", "/dev/shm/test_ebi_a", 8, 16));
    registry.register(data_entry("x", "B", "/dev/shm/test_ebi_b", 64));

    assert_eq!(registry.entry_count(), 2);

    // Valid indices
    let e0 = registry.entry_by_index(0);
    assert!(e0.is_some(), "index 0 should return Some");
    assert_eq!(e0.unwrap().type_name.as_str(), "A");

    let e1 = registry.entry_by_index(1);
    assert!(e1.is_some(), "index 1 should return Some");
    assert_eq!(e1.unwrap().type_name.as_str(), "B");

    // Out-of-bounds indices
    assert!(
        registry.entry_by_index(2).is_none(),
        "index 2 should be None"
    );
    assert!(
        registry.entry_by_index(u32::MAX).is_none(),
        "u32::MAX should be None"
    );

    // After compacting (no-op here since no empty), bounds still hold
    let removed = registry.compact();
    assert_eq!(removed, 0);
    assert!(registry.entry_by_index(0).is_some());
    assert!(registry.entry_by_index(2).is_none());
}

#[test]
fn filter_mode_input_and_clear() {
    // Test that filter_mode, filter_text state management works correctly.
    let mut app = App::with_groups(vec![
        AppGroup {
            name: "testapp".into(),
            segments: vec![
                SegmentInfo {
                    entry: queue_entry("testapp", "Alpha", "/dev/shm/test_fm_a", 8, 16),
                    alive: true,
                    pid_count: 1,
                    queue_writes: None,
                    queue_fill: None,
                    queue_capacity: None,
                    poison: None,
                },
                SegmentInfo {
                    entry: data_entry("testapp", "Beta", "/dev/shm/test_fm_b", 64),
                    alive: true,
                    pid_count: 1,
                    queue_writes: None,
                    queue_fill: None,
                    queue_capacity: None,
                    poison: None,
                },
            ],
            expanded: true,
        },
    ]);

    // Initially: no filter
    assert!(!app.filter_mode);
    assert!(app.filter_text.is_empty());
    assert_eq!(app.sort_mode, SortMode::Name);

    // Activate filter mode
    app.filter_mode = true;
    assert!(app.filter_mode);

    // Type characters
    app.filter_text.push('a');
    app.filter_text.push('l');
    assert_eq!(app.filter_text, "al");

    // Backspace
    app.filter_text.pop();
    assert_eq!(app.filter_text, "a");

    // Clear filter via Esc (simulated)
    app.filter_mode = false;
    app.filter_text.clear();
    assert!(!app.filter_mode);
    assert!(app.filter_text.is_empty());

    // Render should still work fine after filter manipulations
    let buf = render_to_buffer(&mut app, 100, 15);
    let text = buffer_text(&buf);
    assert!(
        text.contains("Alpha"),
        "Alpha should be visible after clearing filter:\n{text}"
    );
    assert!(
        text.contains("Beta"),
        "Beta should be visible after clearing filter:\n{text}"
    );
}

// ─── Phase 7: TUI polish tests (items 4-7) ─────────────────────────────────

#[test]
fn title_bar_shows_segment_and_app_counts() {
    let groups = vec![
        AppGroup {
            name: "alpha".into(),
            segments: vec![
                SegmentInfo {
                    entry: queue_entry("alpha", "Q1", "/dev/shm/tb1", 8, 16),
                    alive: true,
                    pid_count: 1,
                    queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
                },
                SegmentInfo {
                    entry: data_entry("alpha", "D1", "/dev/shm/tb2", 64),
                    alive: true,
                    pid_count: 1,
                    queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
                },
            ],
            expanded: true,
        },
        AppGroup {
            name: "beta".into(),
            segments: vec![SegmentInfo {
                entry: queue_entry("beta", "Q2", "/dev/shm/tb3", 8, 16),
                alive: true,
                pid_count: 1,
                queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
            }],
            expanded: true,
        },
    ];

    let mut app = App::with_groups(groups);
    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);

    assert!(
        text.contains("3 segments"),
        "title bar should show segment count:\n{text}"
    );
    assert!(
        text.contains("2 apps"),
        "title bar should show app count:\n{text}"
    );
}

#[test]
fn title_bar_shows_zero_counts_when_empty() {
    let mut app = App::with_groups(vec![]);
    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);

    assert!(
        text.contains("0 segments"),
        "empty state should show 0 segments:\n{text}"
    );
    assert!(
        text.contains("0 apps"),
        "empty state should show 0 apps:\n{text}"
    );
}

#[test]
fn empty_with_filter_shows_filter_message() {
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Quote", "/dev/shm/ef1", 8, 16),
            alive: true,
            pid_count: 1,
            queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);

    // Set a filter that matches nothing, then manually clear groups to simulate
    app.filter_text = "nonexistent".into();
    app.groups.clear();
    app.recount_rows();

    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);

    assert!(
        text.contains("No segments match filter"),
        "should show filter-specific empty message:\n{text}"
    );
    assert!(
        text.contains("nonexistent"),
        "should include the filter text:\n{text}"
    );
}

#[test]
fn empty_without_filter_shows_default_message() {
    let mut app = App::with_groups(vec![]);
    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);

    assert!(
        text.contains("No segments found"),
        "should show default empty message:\n{text}"
    );
    // Ensure it does NOT show the filter-specific message
    assert!(
        !text.contains("No segments match filter"),
        "should not show filter message when no filter is active:\n{text}"
    );
}

#[test]
fn detail_view_shows_fill_level_bar() {
    let entry = queue_entry("myapp", "Quote", "/dev/shm/fill1", 24, 64);
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: Some(100),
            queue_fill: Some(32),
            queue_capacity: Some(64),
            poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.enter();

    let buf = render_to_buffer(&mut app, 120, 30);
    let text = buffer_text(&buf);

    assert!(
        text.contains("Fill:"),
        "detail view should show Fill label:\n{text}"
    );
    assert!(
        text.contains("50%"),
        "should show 50% fill (32/64):\n{text}"
    );
    assert!(
        text.contains("32/64"),
        "should show count/capacity:\n{text}"
    );
    // Should contain block chars for the bar
    assert!(
        text.contains('█'),
        "should contain filled bar chars:\n{text}"
    );
    assert!(
        text.contains('░'),
        "should contain empty bar chars:\n{text}"
    );
}

#[test]
fn auto_scroll_shows_last_group_at_end() {
    // Build enough groups that the list exceeds the visible area.
    // With height=20 and 3 chrome rows (title + status bar + header row),
    // about 15 rows are visible. Create 30 groups to overflow.
    let groups: Vec<AppGroup> = (0..30)
        .map(|i| AppGroup {
            name: format!("app{i:02}"),
            segments: vec![SegmentInfo {
                entry: queue_entry(
                    &format!("app{i:02}"),
                    &format!("Seg{i:02}"),
                    &format!("/dev/shm/test_scroll_{i}"),
                    8,
                    16,
                ),
                alive: true,
                pid_count: 1,
                queue_writes: None, queue_fill: None, queue_capacity: None, poison: None,
            }],
            expanded: true,
        })
        .collect();

    let mut app = App::with_groups(groups);
    assert_eq!(app.total_rows, 60); // 30 headers + 30 segments

    // Navigate to the very last row
    app.end();
    assert_eq!(app.selected, 59);

    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);

    // The last group (app29) should be visible when scrolled to the end
    assert!(
        text.contains("app29"),
        "last group should be visible after scrolling to end:\n{text}"
    );

    // The first group should NOT be visible (scrolled off)
    assert!(
        !text.contains("app00"),
        "first group should be scrolled off screen:\n{text}"
    );
}

// ─── Section D: Testing hardening ───────────────────────────────────────────

/// D11: Concurrent register() with 4 threads, each using a different flink.
/// All 4 entries must appear in the registry after all threads join.
#[test]
fn d11_concurrent_register_different_flinks() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path().to_path_buf();

    let registry = ShmemRegistry::open_or_create(&base);

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let barrier = barrier.clone();
            let base = base.clone();
            std::thread::spawn(move || {
                let reg = ShmemRegistry::open_or_create(&base);
                barrier.wait();
                let flink = format!("/dev/shm/test_d11_thread_{i}");
                let entry = queue_entry(
                    &format!("app_d11_{i}"),
                    &format!("Msg{i}"),
                    &flink,
                    8,
                    16,
                );
                let result = reg.register(entry);
                assert!(result.is_some(), "thread {i} register should succeed");
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread should not panic");
    }

    // All 4 distinct flinks should be registered.
    assert_eq!(
        registry.entry_count(),
        4,
        "4 entries from 4 threads with different flinks"
    );

    // Verify each flink is present.
    for i in 0..4 {
        let flink = format!("/dev/shm/test_d11_thread_{i}");
        assert!(
            registry.find_by_flink(&flink).is_some(),
            "flink {flink} should exist in registry"
        );
    }
}

/// D12: Concurrent register() with same flink from 2 threads.
/// Should result in exactly 1 entry, and the entry's PID set should contain
/// the current process PID (both threads share the same PID since they are
/// in the same process).
#[test]
fn d12_concurrent_register_same_flink() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path().to_path_buf();

    let registry = ShmemRegistry::open_or_create(&base);

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let handles: Vec<_> = (0..2)
        .map(|_i| {
            let barrier = barrier.clone();
            let base = base.clone();
            std::thread::spawn(move || {
                let reg = ShmemRegistry::open_or_create(&base);
                barrier.wait();
                let entry = queue_entry("shared_app", "SharedMsg", "/dev/shm/test_d12_same", 8, 16);
                let result = reg.register(entry);
                assert!(result.is_some(), "register should succeed");
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread should not panic");
    }

    // The dedup logic in register() should ensure only 1 logical entry for that flink.
    // There may be an abandoned empty slot from the second thread's fetch_add, but
    // find_by_flink should find exactly one non-empty entry with this flink.
    let flink = "/dev/shm/test_d12_same";
    let idx = registry.find_by_flink(flink);
    assert!(idx.is_some(), "entry for shared flink must exist");

    let entry = registry.entry_by_index(idx.unwrap()).unwrap();
    assert_eq!(entry.type_name.as_str(), "SharedMsg");
    assert_eq!(entry.app_name.as_str(), "shared_app");

    // Both threads run in the same process, so the PID set should contain our PID.
    let our_pid = std::process::id();
    let pids = entry.pids.active_pids();
    assert!(
        pids.contains(&our_pid),
        "PID set should contain our PID {our_pid}, got {pids:?}"
    );

    // There should be exactly 1 PID (same process registered twice → dedup).
    assert_eq!(
        entry.pids.count(),
        1,
        "same process registering twice should result in 1 PID"
    );

    // Count non-empty entries with this flink — should be exactly 1.
    let entries = registry.entries();
    let matching: Vec<_> = entries
        .iter()
        .filter(|e| !e.is_empty() && e.flink.as_str() == flink)
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "exactly 1 non-empty entry should have this flink"
    );
}

/// D13: compact() running concurrently with entries() reads.
/// Neither operation should panic or produce undefined behavior.
#[test]
fn d13_compact_under_concurrent_reads() {
    let (tmpdir, _guard) = guarded_tmpdir();
    let base = tmpdir.path().to_path_buf();

    let registry = ShmemRegistry::open_or_create(&base);

    // Pre-populate with some entries, including gaps for compact to work on.
    for i in 0..20 {
        let flink = format!("/dev/shm/test_d13_{i}");
        registry.register(queue_entry("d13app", &format!("Msg{i}"), &flink, 8, 16));
    }
    assert_eq!(registry.entry_count(), 20);

    // Create some empty gaps by zeroing out every other entry's kind.
    let entries = registry.entries();
    for i in (1..20).step_by(2) {
        let kind_ptr = std::ptr::addr_of!(entries[i].kind) as *mut u8;
        unsafe {
            kind_ptr.write_volatile(ShmemKind::Unknown as u8);
        }
    }

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));

    // Thread 1: call entries() in a loop, reading type names.
    let reader_barrier = barrier.clone();
    let reader_base = base.clone();
    let reader = std::thread::spawn(move || {
        let reg = ShmemRegistry::open_or_create(&reader_base);
        reader_barrier.wait();
        let mut total_reads = 0usize;
        for _ in 0..1000 {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let es = reg.entries();
                let mut count = 0usize;
                for e in es {
                    if !e.is_empty() {
                        // Access type_name to exercise the read path.
                        let _ = e.type_name.as_str();
                        count += 1;
                    }
                }
                count
            }));
            assert!(result.is_ok(), "entries() reader should not panic");
            total_reads += 1;
        }
        total_reads
    });

    // Thread 2: call compact() in a loop.
    let compacter_barrier = barrier.clone();
    let compacter_base = base.clone();
    let compacter = std::thread::spawn(move || {
        let reg = ShmemRegistry::open_or_create(&compacter_base);
        compacter_barrier.wait();
        let mut total_compacted = 0usize;
        for _ in 0..50 {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                reg.compact()
            }));
            assert!(result.is_ok(), "compact() should not panic");
            if let Ok(removed) = result {
                total_compacted += removed;
            }
        }
        total_compacted
    });

    let reads = reader.join().expect("reader thread should not panic");
    let _compacted = compacter.join().expect("compacter thread should not panic");

    assert_eq!(reads, 1000, "reader should complete all 1000 iterations");

    // After all compactions, no panics occurred — that's the main assertion.
    // The entry count should be ≤ original (some empty slots were compacted).
    assert!(
        registry.entry_count() <= 20,
        "entry count should not exceed original"
    );
}

/// D14: TUI render with 100+ character app names and type names.
/// The renderer must not panic on very long strings.
#[test]
fn d14_render_long_names() {
    let long_app_name = "a".repeat(120);
    let long_type_name = "b".repeat(120);
    let long_flink = format!("/dev/shm/{}", "c".repeat(110));

    let entry = queue_entry(&long_app_name, &long_type_name, &long_flink, 24, 1024);

    let groups = vec![AppGroup {
        name: long_app_name.clone(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: Some(42),
            queue_fill: Some(500),
            queue_capacity: Some(1024),
            poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    assert_eq!(app.total_rows, 2); // 1 header + 1 segment

    // Render at a normal width — names should be truncated, not panic.
    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);

    // The long name should appear (possibly truncated) without crash.
    // ArrayStr<64> truncates at 63 chars, but AppGroup.name is a String.
    assert!(
        text.contains(&"a".repeat(20)),
        "at least part of the long app name should render:\n{text}"
    );

    // Enter detail view on the segment with long names.
    app.selected = 1;
    app.enter();
    assert!(matches!(app.view, View::Detail(_)));

    let buf = render_to_buffer(&mut app, 120, 30);
    let text = buffer_text(&buf);

    // Detail view should render without panic.
    assert!(
        text.contains("Queue"),
        "detail view should show kind:\n{text}"
    );

    // Also test at very narrow width.
    app.back();
    let buf_narrow = render_to_buffer(&mut app, 40, 10);
    let _text_narrow = buffer_text(&buf_narrow);
    // Just verify no panic at narrow width with long names.

    // And very wide width.
    let buf_wide = render_to_buffer(&mut app, 300, 50);
    let text_wide = buffer_text(&buf_wide);
    assert!(
        text_wide.contains(&"a".repeat(20)),
        "wide render should show long name:\n{text_wide}"
    );
}

/// D15: TUI with exactly 1 segment (edge case for off-by-one).
/// Verifies total_rows, render, navigation, and detail view all work.
#[test]
fn d15_single_segment() {
    let entry = queue_entry("solo", "OnlyMsg", "/dev/shm/test_d15_solo", 16, 32);

    let groups = vec![AppGroup {
        name: "solo".into(),
        segments: vec![SegmentInfo {
            entry,
            alive: true,
            pid_count: 1,
            queue_writes: Some(7),
            queue_fill: Some(3),
            queue_capacity: Some(32),
            poison: None,
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);

    // Exactly 1 app header + 1 segment = 2 rows.
    assert_eq!(app.total_rows, 2);
    assert_eq!(app.groups.len(), 1);
    assert_eq!(app.groups[0].segments.len(), 1);

    // Render the list view.
    let buf = render_to_buffer(&mut app, 100, 15);
    let text = buffer_text(&buf);
    assert!(text.contains("solo"), "app name should appear:\n{text}");
    assert!(text.contains("OnlyMsg"), "segment type should appear:\n{text}");
    assert!(text.contains("▼"), "expanded icon should appear:\n{text}");

    // Navigation: next should go to segment, then clamp.
    assert_eq!(app.selected, 0);
    app.next();
    assert_eq!(app.selected, 1);
    app.next();
    assert_eq!(app.selected, 1, "should clamp at end with 1 segment");

    // Previous back to header.
    app.previous();
    assert_eq!(app.selected, 0);
    app.previous();
    assert_eq!(app.selected, 0, "should clamp at start");

    // Home and End.
    app.end();
    assert_eq!(app.selected, 1);
    app.home();
    assert_eq!(app.selected, 0);

    // Enter detail view on the single segment.
    app.selected = 1;
    app.enter();
    assert!(
        matches!(app.view, View::Detail(_)),
        "should enter detail view"
    );

    let buf = render_to_buffer(&mut app, 120, 30);
    let text = buffer_text(&buf);
    assert!(text.contains("OnlyMsg"), "detail should show type:\n{text}");
    assert!(text.contains("solo"), "detail should show app:\n{text}");
    assert!(text.contains("Queue"), "detail should show kind:\n{text}");

    // Back to list.
    app.back();
    assert!(matches!(app.view, View::List));

    // Collapse the single group.
    app.selected = 0;
    app.enter();
    assert_eq!(app.total_rows, 1, "collapsed: only header row");
    assert!(!app.groups[0].expanded);

    let buf = render_to_buffer(&mut app, 100, 15);
    let text = buffer_text(&buf);
    assert!(text.contains("▶"), "collapsed icon:\n{text}");
    assert!(!text.contains("OnlyMsg"), "segment should be hidden:\n{text}");

    // Re-expand.
    app.enter();
    assert_eq!(app.total_rows, 2);
    assert!(app.groups[0].expanded);
}
