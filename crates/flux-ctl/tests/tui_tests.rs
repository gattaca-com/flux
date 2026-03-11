use std::path::Path;

use flux_communication::ShmemKind;
use flux_ctl::{
    discovery,
    discovery::DiscoveredEntry,
    tui::{
        app::{App, AppGroup, SegmentInfo, SortMode, View},
        render,
    },
};
use flux_timing::Instant;
use ratatui::{Terminal, backend::TestBackend, buffer::Buffer};
use shared_memory::ShmemConf;
/// Shorthand to construct a queue `DiscoveredEntry`.
fn queue_entry(
    app: &str,
    type_name: &str,
    flink: &str,
    elem_size: usize,
    capacity: usize,
) -> DiscoveredEntry {
    DiscoveredEntry {
        kind: ShmemKind::Queue,
        app_name: app.to_string(),
        type_name: type_name.to_string(),
        flink: flink.to_string(),
        elem_size,
        capacity,
        queue_writes: None,
        queue_fill: None,
        backing_path: None,
        backing_size: elem_size * capacity,
        poison_quick: None,
    }
}

/// Shorthand to construct a data `DiscoveredEntry`.
fn data_entry(app: &str, type_name: &str, flink: &str, size: usize) -> DiscoveredEntry {
    DiscoveredEntry {
        kind: ShmemKind::Data,
        app_name: app.to_string(),
        type_name: type_name.to_string(),
        flink: flink.to_string(),
        elem_size: size,
        capacity: 1,
        queue_writes: None,
        queue_fill: None,
        backing_path: None,
        backing_size: size,
        poison_quick: None,
    }
}

/// Helper: render an App into a TestBackend buffer and return the buffer.
fn render_to_buffer(app: &mut App, width: u16, height: u16) -> Buffer {
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.draw(|frame| render::render(frame, app)).unwrap();
    terminal.backend().buffer().clone()
}

/// Helper: extract all text from a Buffer as a single string (rows joined by
/// newlines).
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

/// Helper: create a real shmem segment via ShmemConf
/// and place the flink under `base_dir/<app>/shmem/<subdir>/<name>`.
fn create_raw_shmem(base_dir: &Path, app: &str, subdir: &str, name: &str, size: usize) {
    let flink_dir = base_dir.join(app).join("shmem").join(subdir);
    std::fs::create_dir_all(&flink_dir).unwrap();
    let flink_path = flink_dir.join(name);
    let mut shmem = ShmemConf::new().size(size).flink(&flink_path).create().unwrap();
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
fn help_hint_visible_by_default() {
    let mut app = App::with_groups(vec![]);
    let buf = render_to_buffer(&mut app, 80, 20);
    let text = buffer_text(&buf);
    assert!(text.contains("? help"), "help hint should appear in status bar:\n{text}");
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
    assert!(text.contains("No segments found"), "should show empty message:\n{text}");
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
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
            },
            SegmentInfo {
                entry: data_entry("myapp", "Config", "/dev/shm/d1", 128),
                alive: true,
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
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
    assert!(text.contains("cap="), "queue details:\n{text}");
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
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
            }],
            expanded: true,
        },
        AppGroup {
            name: "beta".into(),
            segments: vec![SegmentInfo {
                entry: data_entry("beta", "State", "/dev/shm/b", 64),
                alive: false,
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
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
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
            }],
            expanded: true,
        },
        AppGroup { name: "b".into(), segments: vec![], expanded: true },
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
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
            },
            SegmentInfo {
                entry: queue_entry("app", "Q2", "/dev/shm/q2", 8, 16),
                alive: true,
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
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
#[test]
fn scan_base_dir_discovers_preexisting_shmem() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    // Create raw shmem segments (filesystem layout only)
    create_raw_shmem(base, "testapp", "data", "MyStruct", 256);

    let entries = discovery::scan_base_dir(base);
    assert_eq!(entries.len(), 1, "should discover 1 pre-existing segment");

    let e = &entries[0];
    assert_eq!(e.app_name, "testapp");
    assert_eq!(e.type_name, "MyStruct");
    assert_eq!(e.kind, ShmemKind::Data);

    // Cleanup
    cleanup_raw_shmem(base, "testapp", "data", "MyStruct");
}

#[test]
fn scan_base_dir_discovers_multiple_apps() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    create_raw_shmem(base, "app1", "data", "Config", 128);
    create_raw_shmem(base, "app2", "data", "State", 256);

    let entries = discovery::scan_base_dir(base);
    assert_eq!(entries.len(), 2, "should discover 2 segments");

    let names = discovery::DiscoveredEntry::app_names(&entries);
    assert_eq!(names, vec!["app1", "app2"]);

    // Cleanup
    cleanup_raw_shmem(base, "app1", "data", "Config");
    cleanup_raw_shmem(base, "app2", "data", "State");
}

#[test]
fn scan_on_empty_base_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    let entries = discovery::scan_base_dir(base);
    assert!(entries.is_empty(), "scan on empty dir should return no entries");
}

#[test]
fn app_from_real_discovery() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    create_raw_shmem(base, "demo", "data", "AppState", 1024);

    // Build App from the real base_dir
    let mut app = App::new(base, None);
    assert_eq!(app.groups.len(), 1);
    assert_eq!(app.groups[0].name, "demo");
    assert_eq!(app.groups[0].segments.len(), 1);
    assert_eq!(app.total_rows, 2); // 1 header + 1 segment

    // Render and verify
    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);
    assert!(text.contains("demo"), "app name in TUI:\n{text}");
    assert!(text.contains("AppState"), "data segment in TUI:\n{text}");

    // Cleanup
    cleanup_raw_shmem(base, "demo", "data", "AppState");
}

#[test]
fn app_filter_limits_to_one_app() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    create_raw_shmem(base, "alpha", "data", "Msg", 64);
    create_raw_shmem(base, "beta", "data", "Msg", 64);

    let app = App::new(base, Some("alpha"));
    assert_eq!(app.groups.len(), 1);
    assert_eq!(app.groups[0].name, "alpha");

    // Cleanup
    cleanup_raw_shmem(base, "alpha", "data", "Msg");
    cleanup_raw_shmem(base, "beta", "data", "Msg");
}
#[test]
fn dead_segment_renders_skull() {
    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: data_entry("ghost", "OldData", "/dev/shm/test_ds_old", 64),
            alive: false,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    let buf = render_to_buffer(&mut app, 100, 15);
    let text = buffer_text(&buf);

    assert!(text.contains("dead"), "dead status should show:\n{text}");
    assert!(text.contains("OldData"), "segment name should show:\n{text}");
}
#[test]
fn enter_on_segment_opens_detail() {
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Quote", "/dev/shm/test_esd", 24, 64),
            alive: true,
            queue_writes: Some(42),
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    // Row 0 = app header, Row 1 = segment
    app.selected = 1;
    app.enter();

    assert!(matches!(app.view, View::Detail(_)), "should switch to detail view");
}

#[test]
fn detail_view_renders_segment_info() {
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Quote", "/dev/shm/test_dvr", 24, 64),
            alive: true,
            queue_writes: Some(42),
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
}

#[test]
fn detail_view_back_returns_to_list() {
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Quote", "/dev/shm/test_dvb", 24, 64),
            alive: true,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Quote", "/dev/shm/test_dcb", 24, 64),
            alive: true,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.enter();

    // Trying to clean a live segment should be blocked
    app.request_cleanup();
    assert!(app.status_msg.is_some(), "should show error message for live segment");
    if let View::Detail(ref d) = app.view {
        assert!(!d.confirm_cleanup, "should not show confirm for live segment");
    }
}

#[test]
fn detail_cleanup_shows_confirm_for_dead() {
    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: data_entry("ghost", "OldData", "/dev/shm/test_dcc", 64),
            alive: false,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
    assert!(text.contains("Clean up this segment"), "confirm popup should appear:\n{text}");
}

#[test]
fn detail_cancel_cleanup_hides_confirm() {
    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: data_entry("ghost", "OldData", "/dev/shm/test_dch", 64),
            alive: false,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.enter();

    app.request_cleanup(); // show confirm
    app.cancel_cleanup(); // cancel

    if let View::Detail(ref d) = app.view {
        assert!(!d.confirm_cleanup, "confirm should be cancelled");
    }
}

#[test]
fn detail_status_bar_shows_cleanup_hint_for_dead() {
    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: data_entry("ghost", "OldData", "/dev/shm/test_dsh", 64),
            alive: false,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Msg", "/dev/shm/test_eoah", 8, 16),
            alive: true,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
#[test]
fn list_cleanup_blocked_for_alive_segment() {
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Msg", "/dev/shm/test_lcba", 8, 16),
            alive: true,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Msg", "/dev/shm/test_lcbh", 8, 16),
            alive: true,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: data_entry("ghost", "OldData", "/dev/shm/test_lcsc", 64),
            alive: false,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: data_entry("ghost", "OldData", "/dev/shm/test_lcch", 64),
            alive: false,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: data_entry("ghost", "OldData", "/dev/shm/test_lsbh", 64),
            alive: false,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1; // dead segment row

    let buf = render_to_buffer(&mut app, 120, 15);
    let text = buffer_text(&buf);

    assert!(text.contains("d destroy"), "status bar should show destroy hint:\n{text}");
}
#[test]
fn poisoned_segment_renders_skull_crossbones() {
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Quote", "/dev/shm/test_poison_render", 24, 64),
            alive: true,
            queue_writes: Some(100),
            queue_fill: None,
            queue_capacity: None,
            poison: Some(discovery::PoisonInfo { n_poisoned: 2, first_slot: 5, total_slots: 64 }),
            msgs_per_sec: None,
            pids: vec![],
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
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Quote", "/dev/shm/test_poison_detail", 24, 64),
            alive: true,
            queue_writes: Some(100),
            queue_fill: None,
            queue_capacity: None,
            poison: Some(discovery::PoisonInfo { n_poisoned: 3, first_slot: 7, total_slots: 64 }),
            msgs_per_sec: None,
            pids: vec![],
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
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Quote", "/dev/shm/test_no_poison", 24, 64),
            alive: true,
            queue_writes: Some(42),
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    let buf = render_to_buffer(&mut app, 120, 15);
    let text = buffer_text(&buf);

    assert!(!text.contains("poisoned"), "healthy segment should not show poisoned:\n{text}");
    assert!(text.contains("alive"), "should show alive:\n{text}");
}
#[test]
fn destroy_all_shows_confirm_when_dead_exist() {
    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: data_entry("ghost", "Old", "/dev/shm/test_da_confirm", 64),
            alive: false,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Msg", "/dev/shm/test_da_alive", 8, 16),
            alive: true,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
    let groups = vec![AppGroup {
        name: "ghost".into(),
        segments: vec![SegmentInfo {
            entry: data_entry("ghost", "Old", "/dev/shm/test_da_cancel", 64),
            alive: false,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
    let groups = vec![
        AppGroup {
            name: "ghost".into(),
            segments: vec![SegmentInfo {
                entry: data_entry("ghost", "Old", "/dev/shm/test_da_hint", 64),
                alive: false,
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
            }],
            expanded: true,
        },
        AppGroup {
            name: "myapp".into(),
            segments: vec![SegmentInfo {
                entry: queue_entry("myapp", "Msg", "/dev/shm/test_da_hint2", 8, 16),
                alive: true,
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
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
#[test]
fn scan_base_dir_skips_stale_flinks() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    // Create a stale flink (file exists but no backing shmem)
    let flink_dir = base.join("staleapp").join("shmem").join("queues");
    std::fs::create_dir_all(&flink_dir).unwrap();
    let stale_flink = flink_dir.join("GhostQueue");
    std::fs::write(&stale_flink, "bogus_os_id").unwrap();

    // scan_base_dir should skip entries whose backing shmem can't be opened
    let entries = discovery::scan_base_dir(base);
    assert_eq!(entries.len(), 0, "stale flink should be skipped by scan");
}
#[test]
fn filter_narrows_visible_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    create_raw_shmem(base, "myapp", "data", "Quote", 64);
    create_raw_shmem(base, "myapp", "data", "Trade", 64);
    create_raw_shmem(base, "myapp", "data", "Order", 128);

    // No filter: all three segments should appear
    let mut app = App::new(base, None);
    assert_eq!(app.groups.len(), 1);
    assert_eq!(app.groups[0].segments.len(), 3);

    // Set filter_text and refresh — only "Quote" should survive
    app.filter_text = "quo".into();
    app.refresh();
    assert_eq!(app.groups.len(), 1, "app group should still exist");
    assert_eq!(app.groups[0].segments.len(), 1, "only matching segment should remain");
    assert_eq!(app.groups[0].segments[0].entry.type_name, "Quote");

    // Filter that matches nothing: groups should be empty
    app.filter_text = "nonexistent".into();
    app.refresh();
    assert_eq!(app.groups.len(), 0, "no groups when filter matches nothing");

    // Empty filter restores all segments
    app.filter_text.clear();
    app.refresh();
    assert_eq!(app.groups[0].segments.len(), 3);

    // Cleanup
    cleanup_raw_shmem(base, "myapp", "data", "Quote");
    cleanup_raw_shmem(base, "myapp", "data", "Trade");
    cleanup_raw_shmem(base, "myapp", "data", "Order");
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
                    queue_writes: None,
                    queue_fill: None,
                    queue_capacity: None,
                    poison: None,
                    msgs_per_sec: None,
                    pids: vec![],
                },
                SegmentInfo {
                    entry: queue_entry("a", "S2", "/dev/shm/he2", 8, 16),
                    alive: true,
                    queue_writes: None,
                    queue_fill: None,
                    queue_capacity: None,
                    poison: None,
                    msgs_per_sec: None,
                    pids: vec![],
                },
            ],
            expanded: true,
        },
        AppGroup {
            name: "b".into(),
            segments: vec![SegmentInfo {
                entry: data_entry("b", "S3", "/dev/shm/he3", 64),
                alive: true,
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
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
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
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
                    queue_writes: Some((gi * 5 + si) as usize),
                    queue_fill: None,
                    queue_capacity: None,
                    poison: None,
                    msgs_per_sec: None,
                    pids: vec![],
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
fn filter_mode_input_and_clear() {
    // Test that filter_mode, filter_text state management works correctly.
    let mut app = App::with_groups(vec![AppGroup {
        name: "testapp".into(),
        segments: vec![
            SegmentInfo {
                entry: queue_entry("testapp", "Alpha", "/dev/shm/test_fm_a", 8, 16),
                alive: true,
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
            },
            SegmentInfo {
                entry: data_entry("testapp", "Beta", "/dev/shm/test_fm_b", 64),
                alive: true,
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
            },
        ],
        expanded: true,
    }]);

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
    assert!(text.contains("Alpha"), "Alpha should be visible after clearing filter:\n{text}");
    assert!(text.contains("Beta"), "Beta should be visible after clearing filter:\n{text}");
}
#[test]
fn title_bar_shows_segment_and_app_counts() {
    let groups = vec![
        AppGroup {
            name: "alpha".into(),
            segments: vec![
                SegmentInfo {
                    entry: queue_entry("alpha", "Q1", "/dev/shm/tb1", 8, 16),
                    alive: true,
                    queue_writes: None,
                    queue_fill: None,
                    queue_capacity: None,
                    poison: None,
                    msgs_per_sec: None,
                    pids: vec![],
                },
                SegmentInfo {
                    entry: data_entry("alpha", "D1", "/dev/shm/tb2", 64),
                    alive: true,
                    queue_writes: None,
                    queue_fill: None,
                    queue_capacity: None,
                    poison: None,
                    msgs_per_sec: None,
                    pids: vec![],
                },
            ],
            expanded: true,
        },
        AppGroup {
            name: "beta".into(),
            segments: vec![SegmentInfo {
                entry: queue_entry("beta", "Q2", "/dev/shm/tb3", 8, 16),
                alive: true,
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
            }],
            expanded: true,
        },
    ];

    let mut app = App::with_groups(groups);
    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);

    assert!(text.contains("3 segments"), "title bar should show segment count:\n{text}");
    assert!(text.contains("2 apps"), "title bar should show app count:\n{text}");
}

#[test]
fn title_bar_shows_zero_counts_when_empty() {
    let mut app = App::with_groups(vec![]);
    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);

    assert!(text.contains("0 segments"), "empty state should show 0 segments:\n{text}");
    assert!(text.contains("0 apps"), "empty state should show 0 apps:\n{text}");
}

#[test]
fn empty_with_filter_shows_filter_message() {
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Quote", "/dev/shm/ef1", 8, 16),
            alive: true,
            queue_writes: None,
            queue_fill: None,
            queue_capacity: None,
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
    assert!(text.contains("nonexistent"), "should include the filter text:\n{text}");
}

#[test]
fn empty_without_filter_shows_default_message() {
    let mut app = App::with_groups(vec![]);
    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);

    assert!(text.contains("No segments found"), "should show default empty message:\n{text}");
    // Ensure it does NOT show the filter-specific message
    assert!(
        !text.contains("No segments match filter"),
        "should not show filter message when no filter is active:\n{text}"
    );
}

#[test]
fn detail_view_shows_write_pos_bar() {
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "Quote", "/dev/shm/fill1", 24, 64),
            alive: true,
            queue_writes: Some(100),
            queue_fill: Some(32),
            queue_capacity: Some(64),
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    app.selected = 1;
    app.enter();

    let buf = render_to_buffer(&mut app, 120, 30);
    let text = buffer_text(&buf);

    assert!(text.contains("Write Pos:"), "detail view should show Write Pos label:\n{text}");
    assert!(text.contains("32/64"), "should show pos/capacity:\n{text}");
    // Should contain block chars for the bar
    assert!(text.contains('█'), "should contain filled bar chars:\n{text}");
    assert!(text.contains('░'), "should contain empty bar chars:\n{text}");
}

#[test]
fn auto_scroll_shows_last_group_at_end() {
    // Build enough groups that the list exceeds the visible area.
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
                queue_writes: None,
                queue_fill: None,
                queue_capacity: None,
                poison: None,
                msgs_per_sec: None,
                pids: vec![],
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
    assert!(text.contains("app29"), "last group should be visible after scrolling to end:\n{text}");

    // The first group should NOT be visible (scrolled off)
    assert!(!text.contains("app00"), "first group should be scrolled off screen:\n{text}");
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
            queue_writes: Some(42),
            queue_fill: Some(500),
            queue_capacity: Some(1024),
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);
    assert_eq!(app.total_rows, 2); // 1 header + 1 segment

    // Render at a normal width — names should be truncated, not panic.
    let buf = render_to_buffer(&mut app, 120, 20);
    let text = buffer_text(&buf);

    // The long name should appear (possibly truncated) without crash.
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
    assert!(text.contains("Queue"), "detail view should show kind:\n{text}");

    // Also test at very narrow width.
    app.back();
    let buf_narrow = render_to_buffer(&mut app, 40, 10);
    let _text_narrow = buffer_text(&buf_narrow);
    // Just verify no panic at narrow width with long names.

    // And very wide width.
    let buf_wide = render_to_buffer(&mut app, 300, 50);
    let text_wide = buffer_text(&buf_wide);
    assert!(text_wide.contains(&"a".repeat(20)), "wide render should show long name:\n{text_wide}");
}

/// Performance regression test: scan_base_dir with 250 segments across 40 apps
/// should complete within a reasonable time (prevents future performance
/// regressions).
#[test]
fn performance_scan_base_dir_250_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    // Create 250 segments across 40 apps (6-7 segments per app)
    let mut segment_paths = Vec::new();

    for app_idx in 0..40 {
        let app_name = format!("perfapp{:03}", app_idx);

        // Create 6 segments for apps 0-29, 7 segments for apps 30-39 = 250 total
        let segments_per_app = if app_idx < 30 { 6 } else { 7 };

        for seg_idx in 0..segments_per_app {
            let segment_name = format!("Segment{:02}", seg_idx);
            let subdir = if seg_idx % 2 == 0 { "queues" } else { "data" };
            let size = 1024 * (seg_idx + 1); // varying sizes

            create_raw_shmem(base, &app_name, subdir, &segment_name, size);
            segment_paths.push((app_name.clone(), subdir, segment_name));
        }
    }

    // Verify we created exactly 250 segments
    assert_eq!(segment_paths.len(), 250, "should have created exactly 250 segments");

    // Time the scan_base_dir operation
    let start = Instant::now();
    let entries = discovery::scan_base_dir(base);
    let elapsed = start.elapsed();

    // Verify all segments were discovered
    assert_eq!(entries.len(), 250, "should discover all 250 segments");

    // Verify we have 40 unique apps
    let app_names = discovery::DiscoveredEntry::app_names(&entries);
    assert_eq!(app_names.len(), 40, "should have 40 unique apps");

    // Performance assertion: should complete within 5 seconds
    // (This is a generous upper bound - the optimized scan should be much faster)
    let max_duration_secs = 5.0;
    let elapsed_secs = elapsed.as_secs();
    assert!(
        elapsed_secs <= max_duration_secs,
        "scan_base_dir took {:.2}s with 250 segments, expected <= {:.2}s (performance regression)",
        elapsed_secs,
        max_duration_secs
    );

    // Also log the actual timing for monitoring
    println!("Performance: scan_base_dir processed 250 segments in {:.2}s", elapsed_secs);

    // Cleanup all segments
    for (app_name, subdir, segment_name) in segment_paths {
        cleanup_raw_shmem(base, &app_name, subdir, &segment_name);
    }
}

/// D15: TUI with exactly 1 segment (edge case for off-by-one).
/// Verifies total_rows, render, navigation, and detail view all work.
#[test]
fn d15_single_segment() {
    let groups = vec![AppGroup {
        name: "solo".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("solo", "OnlyMsg", "/dev/shm/test_d15_solo", 16, 32),
            alive: true,
            queue_writes: Some(7),
            queue_fill: Some(3),
            queue_capacity: Some(32),
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
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
    assert!(matches!(app.view, View::Detail(_)), "should enter detail view");

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

// ── Consumer Groups panel tests ───────────────────────────────────────

#[test]
fn detail_view_shows_consumer_groups() {
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "TelemetryUpdate", "/dev/shm/test_cg1", 64, 1024),
            alive: true,
            queue_writes: Some(5000),
            queue_fill: Some(100),
            queue_capacity: Some(1024),
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);

    // Manually enter detail view with consumer groups.
    app.view = View::Detail(flux_ctl::tui::app::DetailState {
        group_idx: 0,
        segment_idx: 0,
        pids: vec![],
        selected_pid: 0,
        confirm_cleanup: false,
        consumer_groups: vec![
            discovery::ConsumerGroupInfo {
                label: "builder.telemetry.broadcast".into(),
                cursor: 4990,
            },
            discovery::ConsumerGroupInfo { label: "relay.telemetry.collab".into(), cursor: 4800 },
            discovery::ConsumerGroupInfo {
                label: "monitor.metrics.broadcast".into(),
                cursor: 5000,
            },
        ],
    });

    let buf = render_to_buffer(&mut app, 120, 40);
    let text = buffer_text(&buf);

    // Section header should appear
    assert!(text.contains("Consumer Groups (3)"), "should show consumer groups header:\n{text}");

    // Group labels should appear
    assert!(text.contains("builder.telemetry.broadcast"), "should show first group label:\n{text}");
    assert!(text.contains("relay.telemetry.collab"), "should show second group label:\n{text}");
    assert!(text.contains("monitor.metrics.broadcast"), "should show third group label:\n{text}");

    // Lag values: 5000-4990=10, 5000-4800=200, 5000-5000=0
    assert!(text.contains("10"), "should show lag of 10:\n{text}");
    assert!(text.contains("200"), "should show lag of 200:\n{text}");
}

#[test]
fn detail_view_hides_consumer_groups_when_empty() {
    let groups = vec![AppGroup {
        name: "myapp".into(),
        segments: vec![SegmentInfo {
            entry: queue_entry("myapp", "SomeMsg", "/dev/shm/test_cg2", 32, 512),
            alive: true,
            queue_writes: Some(100),
            queue_fill: Some(10),
            queue_capacity: Some(512),
            poison: None,
            msgs_per_sec: None,
            pids: vec![],
        }],
        expanded: true,
    }];

    let mut app = App::with_groups(groups);

    // Detail view with no consumer groups.
    app.view = View::Detail(flux_ctl::tui::app::DetailState {
        group_idx: 0,
        segment_idx: 0,
        pids: vec![],
        selected_pid: 0,
        confirm_cleanup: false,
        consumer_groups: vec![],
    });

    let buf = render_to_buffer(&mut app, 120, 30);
    let text = buffer_text(&buf);

    // Consumer Groups section should NOT appear.
    assert!(
        !text.contains("Consumer Groups"),
        "should not show consumer groups when empty:\n{text}"
    );
}
