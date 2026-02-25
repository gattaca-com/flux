use flux_communication::registry::{
    ShmemRegistry, REGISTRY_FLINK_NAME, data_entry, queue_entry,
};
use flux_ctl::discovery;
use flux_ctl::tui::app::{App, AppGroup, SegmentInfo};
use flux_ctl::tui::render;
use ratatui::{Terminal, backend::TestBackend, buffer::Buffer};

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
        text.contains("Press ? for help"),
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
                queue_writes: Some(42),
            },
            SegmentInfo {
                entry: data_entry("myapp", "Config", "/dev/shm/d1", 128),
                alive: true,
                pid_count: 1,
                queue_writes: None,
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
            queue_writes: None,
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
                queue_writes: None,
            }],
            expanded: true,
        },
        AppGroup {
            name: "beta".into(),
            segments: vec![SegmentInfo {
                entry: data_entry("beta", "State", "/dev/shm/b", 64),
                alive: false,
                pid_count: 1,
                queue_writes: None,
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
                queue_writes: None,
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
                queue_writes: None,
            },
            SegmentInfo {
                entry: queue_entry("app", "Q2", "/dev/shm/q2", 8, 16),
                alive: true,
                pid_count: 1,
                queue_writes: None,
            },
        ],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    assert_eq!(app.total_rows, 3); // 1 header + 2 segments

    // Collapse
    app.selected = 0;
    app.toggle_expand();
    assert_eq!(app.total_rows, 1);
    assert!(!app.groups[0].expanded);

    // Verify segments are hidden in render
    let buf = render_to_buffer(&mut app, 100, 15);
    let text = buffer_text(&buf);
    assert!(!text.contains("Q1"), "Q1 should be hidden:\n{text}");
    assert!(text.contains("▶"), "collapsed icon:\n{text}");

    // Expand again
    app.toggle_expand();
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
            queue_writes: None,
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
            queue_writes: Some(100),
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
