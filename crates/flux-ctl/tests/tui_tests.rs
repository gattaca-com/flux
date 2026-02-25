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
    let guard = ShmemGuard { base_dir: tmpdir.path().to_path_buf() };
    (tmpdir, guard)
}

// ─── Unit tests: render with synthetic data (no real shmem) ─────────────────

#[test]
fn render_empty_app() {
    let mut app = App::with_groups(vec![]);
    let buf = render_to_buffer(&mut app, 100, 20);
    let text = buffer_text(&buf);

    assert!(
        text.contains("flux-ctl"),
        "title should be visible:\n{text}"
    );
    assert!(
        text.contains("No segments found"),
        "empty state message:\n{text}"
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
                queue_writes: Some(42),
            },
            SegmentInfo {
                entry: data_entry("myapp", "Config", "/dev/shm/d1", 128),
                alive: true,
                queue_writes: None,
            },
        ],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    assert_eq!(app.total_rows, 3); // 1 header + 2 segments

    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);

    assert!(text.contains("myapp"), "app name:\n{text}");
    assert!(text.contains("2 segments"), "segment count:\n{text}");
    assert!(text.contains("PriceUpdate"), "queue name:\n{text}");
    assert!(text.contains("Config"), "data name:\n{text}");
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
                queue_writes: None,
            }],
            expanded: true,
        },
        AppGroup {
            name: "beta".into(),
            segments: vec![SegmentInfo {
                entry: data_entry("beta", "State", "/dev/shm/b", 64),
                alive: false,
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
                queue_writes: None,
            },
            SegmentInfo {
                entry: queue_entry("app", "Q2", "/dev/shm/q2", 8, 16),
                alive: true,
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
    let mut alive_entry = queue_entry("app", "Alive", "/dev/shm/test_cd_alive", 8, 16);
    alive_entry.pid = 1;
    let mut dead_entry = queue_entry("app", "Dead", "/dev/shm/test_cd_dead", 8, 16);
    dead_entry.pid = 99999999;

    registry.register(alive_entry);
    registry.register(dead_entry);

    let entries = registry.entries();
    assert!(discovery::is_pid_alive(entries[0].pid));
    assert!(!discovery::is_pid_alive(entries[1].pid));
}

#[test]
fn dead_segment_renders_skull() {
    let mut dead_entry = data_entry("ghost", "OldData", "/dev/shm/test_ds_old", 64);
    dead_entry.pid = 99999999;

    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: dead_entry,
            alive: false,
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
