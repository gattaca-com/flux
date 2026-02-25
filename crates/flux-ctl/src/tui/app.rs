use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Instant;

use flux_communication::queue::QueueHeader;
use flux_communication::registry::{ShmemEntry, ShmemKind, cleanup_flink};

use crate::discovery;

#[derive(Clone)]
pub struct SegmentInfo {
    pub entry: ShmemEntry,
    pub alive: bool,
    pub pid_count: usize,
    pub queue_writes: Option<usize>,
}

pub struct AppGroup {
    pub name: String,
    pub segments: Vec<SegmentInfo>,
    pub expanded: bool,
}

/// Which view is currently active.
#[derive(Clone, Debug)]
pub enum View {
    /// Main list of app groups + segments.
    List,
    /// Detail view for a specific segment.
    Detail(DetailState),
}

/// State for the detail view.
#[derive(Clone, Debug)]
pub struct DetailState {
    /// Index into `App::groups`.
    pub group_idx: usize,
    /// Index into `AppGroup::segments`.
    pub segment_idx: usize,
    /// Per-PID info (refreshed along with the rest).
    pub pids: Vec<discovery::PidInfo>,
    /// Selected row in the PID table (for scrolling).
    pub selected_pid: usize,
    /// Whether a confirm-cleanup prompt is visible.
    pub confirm_cleanup: bool,
}

pub struct App {
    pub groups: Vec<AppGroup>,
    pub selected: usize,
    pub total_rows: usize,
    pub base_dir: PathBuf,
    pub app_filter: Option<String>,
    pub last_refresh: Instant,
    pub show_help: bool,
    pub view: View,
    /// Ephemeral status message shown in the status bar.
    pub status_msg: Option<(String, Instant)>,
}

impl App {
    pub fn new(base_dir: &Path, app_filter: Option<&str>) -> Self {
        let mut app = Self {
            groups: Vec::new(),
            selected: 0,
            total_rows: 0,
            base_dir: base_dir.to_path_buf(),
            app_filter: app_filter.map(String::from),
            last_refresh: Instant::now(),
            show_help: false,
            view: View::List,
            status_msg: None,
        };
        app.refresh();
        app
    }

    /// Construct an App directly from pre-built groups (for testing).
    pub fn with_groups(groups: Vec<AppGroup>) -> Self {
        let mut app = Self {
            groups,
            selected: 0,
            total_rows: 0,
            base_dir: PathBuf::new(),
            app_filter: None,
            last_refresh: Instant::now(),
            show_help: false,
            view: View::List,
            status_msg: None,
        };
        app.recount_rows();
        app
    }

    pub fn refresh(&mut self) {
        self.groups.clear();
        let Some(registry) = discovery::open_registry(&self.base_dir) else {
            return;
        };
        let entries = registry.entries();
        let app_names = discovery::app_names(registry);

        for name in app_names {
            if let Some(ref filter) = self.app_filter {
                if &name != filter {
                    continue;
                }
            }
            let segments: Vec<SegmentInfo> = entries
                .iter()
                .filter(|e| e.app_name.as_str() == name)
                .map(|e| {
                    let alive = e.pids.any_alive();
                    let pid_count = e.pids.count();
                    let queue_writes = if e.kind == ShmemKind::Queue {
                        QueueHeader::open_shared(e.flink.as_str())
                            .ok()
                            .map(|h| h.count.load(Ordering::Relaxed))
                    } else {
                        None
                    };
                    SegmentInfo { entry: e.clone(), alive, pid_count, queue_writes }
                })
                .collect();
            self.groups.push(AppGroup { name, segments, expanded: true });
        }

        self.recount_rows();
        self.refresh_detail_pids();
        self.last_refresh = Instant::now();
    }

    fn recount_rows(&mut self) {
        self.total_rows = self
            .groups
            .iter()
            .map(|g| 1 + if g.expanded { g.segments.len() } else { 0 })
            .sum();
        if self.selected >= self.total_rows && self.total_rows > 0 {
            self.selected = self.total_rows - 1;
        }
    }

    /// Refresh the PID info for the detail view (if active).
    fn refresh_detail_pids(&mut self) {
        if let View::Detail(ref mut detail) = self.view {
            if let Some(seg) = self
                .groups
                .get(detail.group_idx)
                .and_then(|g| g.segments.get(detail.segment_idx))
            {
                detail.pids = discovery::pids_info(&seg.entry);
            }
        }
    }

    pub fn tick(&mut self) {
        // Expire status messages after 3 seconds
        if let Some((_, created)) = &self.status_msg {
            if created.elapsed() >= std::time::Duration::from_secs(3) {
                self.status_msg = None;
            }
        }

        if self.last_refresh.elapsed() >= std::time::Duration::from_secs(1) {
            self.refresh();
        }
    }

    pub fn next(&mut self) {
        match &mut self.view {
            View::List => {
                if self.total_rows > 0 {
                    self.selected =
                        (self.selected + 1).min(self.total_rows.saturating_sub(1));
                }
            }
            View::Detail(detail) => {
                if !detail.pids.is_empty() {
                    detail.selected_pid =
                        (detail.selected_pid + 1).min(detail.pids.len().saturating_sub(1));
                }
            }
        }
    }

    pub fn previous(&mut self) {
        match &mut self.view {
            View::List => {
                self.selected = self.selected.saturating_sub(1);
            }
            View::Detail(detail) => {
                detail.selected_pid = detail.selected_pid.saturating_sub(1);
            }
        }
    }

    pub fn toggle_help(&mut self) {
        self.show_help = !self.show_help;
    }

    /// Called on Enter key.
    pub fn enter(&mut self) {
        match &self.view {
            View::List => {
                // Figure out what the cursor is pointing at.
                let mut row = 0;
                for (gi, group) in self.groups.iter_mut().enumerate() {
                    if row == self.selected {
                        // Cursor is on a group header → toggle expand.
                        group.expanded = !group.expanded;
                        self.recount_rows();
                        return;
                    }
                    row += 1;
                    if group.expanded {
                        for si in 0..group.segments.len() {
                            if row == self.selected {
                                // Cursor is on a segment → open detail view.
                                let pids =
                                    discovery::pids_info(&group.segments[si].entry);
                                self.view = View::Detail(DetailState {
                                    group_idx: gi,
                                    segment_idx: si,
                                    pids,
                                    selected_pid: 0,
                                    confirm_cleanup: false,
                                });
                                return;
                            }
                            row += 1;
                        }
                    }
                }
            }
            View::Detail(_) => {} // Enter does nothing in detail view
        }
    }

    /// Go back from detail to list.
    pub fn back(&mut self) {
        if let View::Detail(_) = &self.view {
            self.view = View::List;
        }
    }

    /// Request cleanup of the currently-viewed segment.
    /// First call shows a confirmation prompt, second call executes.
    pub fn request_cleanup(&mut self) {
        let View::Detail(ref mut detail) = self.view else { return };

        let seg = match self
            .groups
            .get(detail.group_idx)
            .and_then(|g| g.segments.get(detail.segment_idx))
        {
            Some(s) => s,
            None => return,
        };

        // Only allow cleanup if no PIDs are alive.
        if seg.alive {
            self.status_msg =
                Some(("Cannot clean: segment still has live processes".into(), Instant::now()));
            return;
        }

        if detail.confirm_cleanup {
            // Second press → actually clean up.
            let flink = seg.entry.flink.as_str().to_string();
            cleanup_flink(Path::new(&flink));
            self.status_msg =
                Some((format!("Cleaned up {}", flink), Instant::now()));
            detail.confirm_cleanup = false;
            self.view = View::List;
            self.refresh();
        } else {
            detail.confirm_cleanup = true;
        }
    }

    /// Cancel the cleanup confirmation prompt.
    pub fn cancel_cleanup(&mut self) {
        if let View::Detail(ref mut detail) = self.view {
            detail.confirm_cleanup = false;
        }
    }

    /// Get the segment info for the current detail view, if any.
    pub fn detail_segment(&self) -> Option<&SegmentInfo> {
        if let View::Detail(ref detail) = self.view {
            self.groups
                .get(detail.group_idx)
                .and_then(|g| g.segments.get(detail.segment_idx))
        } else {
            None
        }
    }

    // Keep the old method name working for the expand/collapse test.
    pub fn toggle_expand(&mut self) {
        self.enter();
    }
}
