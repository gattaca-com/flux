use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use flux_timing::{Duration, Instant};

use flux_communication::queue::QueueHeader;
use flux_communication::registry::{ShmemEntry, ShmemKind, cleanup_flink};

use crate::discovery;

#[derive(Clone)]
pub struct SegmentInfo {
    pub entry: ShmemEntry,
    pub alive: bool,
    pub pid_count: usize,
    pub queue_writes: Option<usize>,
    pub poison: Option<discovery::PoisonInfo>,
}

pub struct AppGroup {
    pub name: String,
    pub segments: Vec<SegmentInfo>,
    pub expanded: bool,
}

#[derive(Clone, Debug)]
pub enum View {
    List,
    Detail(DetailState),
}

#[derive(Clone, Debug)]
pub struct DetailState {
    pub group_idx: usize,
    pub segment_idx: usize,
    pub pids: Vec<discovery::PidInfo>,
    pub selected_pid: usize,
    pub confirm_cleanup: bool,
}

/// What the cursor is pointing at in the list view.
pub enum SelectedItem<'a> {
    AppHeader(usize),
    Segment(usize, usize, &'a SegmentInfo),
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
    pub status_msg: Option<(String, Instant)>,
    pub confirm_cleanup: bool,
    pub confirm_cleanup_all: bool,
    /// Dead flinks captured on the first press of "cleanup all", used on confirm.
    pub pending_cleanup_flinks: Option<Vec<String>>,
}

fn status_msg_duration() -> Duration {
    Duration::from_secs(3)
}

fn refresh_interval() -> Duration {
    Duration::from_secs(1)
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
            confirm_cleanup: false,
            confirm_cleanup_all: false,
            pending_cleanup_flinks: None,
        };
        app.refresh();
        app
    }

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
            confirm_cleanup: false,
            confirm_cleanup_all: false,
            pending_cleanup_flinks: None,
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
            if let Some(ref filter) = self.app_filter
                && &name != filter
            {
                continue;
            }
            let segments: Vec<SegmentInfo> = entries
                .iter()
                .filter(|e| e.app_name.as_str() == name)
                .filter(|e| discovery::entry_visible(e))
                .map(|e| {
                    let alive = e.pids.any_alive();
                    let pid_count = e.pids.count();
                    let queue_writes = if alive && e.kind == ShmemKind::Queue {
                        QueueHeader::open_shared(e.flink.as_str())
                            .ok()
                            .map(|h| h.count.load(Ordering::Relaxed))
                    } else {
                        None
                    };
                    let poison = if alive { discovery::check_poison(e) } else { None };
                    SegmentInfo { entry: e.clone(), alive, pid_count, queue_writes, poison }
                })
                .collect();
            if segments.is_empty() {
                continue;
            }
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

    fn refresh_detail_pids(&mut self) {
        if let View::Detail(ref mut detail) = self.view {
            let seg = self
                .groups
                .get(detail.group_idx)
                .and_then(|g| g.segments.get(detail.segment_idx));
            match seg {
                Some(s) => detail.pids = discovery::pids_info(&s.entry),
                None => self.view = View::List,
            }
        }
    }

    pub fn tick(&mut self) {
        if let Some((_, ts)) = &self.status_msg
            && ts.elapsed() >= status_msg_duration()
        {
            self.status_msg = None;
        }
        if self.last_refresh.elapsed() >= refresh_interval() {
            self.refresh();
        }
    }

    pub fn next(&mut self) {
        match &mut self.view {
            View::List => {
                if self.total_rows > 0 {
                    self.selected = (self.selected + 1).min(self.total_rows.saturating_sub(1));
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

    pub fn enter(&mut self) {
        match &self.view {
            View::List => {
                let mut row = 0;
                for (gi, group) in self.groups.iter_mut().enumerate() {
                    if row == self.selected {
                        group.expanded = !group.expanded;
                        self.recount_rows();
                        return;
                    }
                    row += 1;
                    if group.expanded {
                        for si in 0..group.segments.len() {
                            if row == self.selected {
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
            View::Detail(_) => {}
        }
    }

    pub fn back(&mut self) {
        if let View::Detail(_) = &self.view {
            self.view = View::List;
        }
    }

    pub fn selected_item(&self) -> Option<SelectedItem<'_>> {
        let mut row = 0;
        for (gi, group) in self.groups.iter().enumerate() {
            if row == self.selected {
                return Some(SelectedItem::AppHeader(gi));
            }
            row += 1;
            if group.expanded {
                for (si, seg) in group.segments.iter().enumerate() {
                    if row == self.selected {
                        return Some(SelectedItem::Segment(gi, si, seg));
                    }
                    row += 1;
                }
            }
        }
        None
    }

    pub fn request_cleanup(&mut self) {
        match &self.view {
            View::List => self.request_cleanup_list(),
            View::Detail(_) => self.request_cleanup_detail(),
        }
    }

    fn request_cleanup_list(&mut self) {
        let (alive, flink) = match self.selected_item() {
            Some(SelectedItem::Segment(_, _, seg)) => {
                (seg.alive, seg.entry.flink.as_str().to_string())
            }
            _ => {
                self.status_msg = Some(("Select a segment first".into(), Instant::now()));
                return;
            }
        };

        if alive {
            self.status_msg =
                Some(("Cannot clean: segment still has live processes".into(), Instant::now()));
            return;
        }

        if self.confirm_cleanup {
            match cleanup_flink(Path::new(&flink)) {
                Ok(()) => {
                    self.status_msg = Some((format!("Cleaned up {}", flink), Instant::now()));
                }
                Err(e) => {
                    self.status_msg = Some((format!("Cleanup failed: {e}"), Instant::now()));
                }
            }
            self.confirm_cleanup = false;
            self.refresh();
        } else {
            self.confirm_cleanup = true;
        }
    }

    fn request_cleanup_detail(&mut self) {
        let View::Detail(ref mut detail) = self.view else { return };

        let seg = match self
            .groups
            .get(detail.group_idx)
            .and_then(|g| g.segments.get(detail.segment_idx))
        {
            Some(s) => s,
            None => return,
        };

        if seg.alive {
            self.status_msg =
                Some(("Cannot clean: segment still has live processes".into(), Instant::now()));
            return;
        }

        if detail.confirm_cleanup {
            let flink = seg.entry.flink.as_str().to_string();
            match cleanup_flink(Path::new(&flink)) {
                Ok(()) => {
                    self.status_msg = Some((format!("Cleaned up {}", flink), Instant::now()));
                }
                Err(e) => {
                    self.status_msg = Some((format!("Cleanup failed: {e}"), Instant::now()));
                }
            }
            detail.confirm_cleanup = false;
            self.view = View::List;
            self.refresh();
        } else {
            detail.confirm_cleanup = true;
        }
    }

    pub fn request_cleanup_all(&mut self) {
        if self.confirm_cleanup_all {
            // Second press — use the stashed list so a refresh between presses
            // cannot change what gets cleaned.
            if let Some(flinks) = self.pending_cleanup_flinks.take() {
                let n = flinks.len();
                let mut errors = Vec::new();
                for flink in &flinks {
                    if let Err(e) = cleanup_flink(Path::new(flink)) {
                        errors.push(e);
                    }
                }
                if errors.is_empty() {
                    self.status_msg =
                        Some((format!("Cleaned up {n} stale segments"), Instant::now()));
                } else {
                    self.status_msg = Some((
                        format!(
                            "Cleaned {n} segments, {} errors: {}",
                            errors.len(),
                            errors.join("; ")
                        ),
                        Instant::now(),
                    ));
                }
            }
            self.confirm_cleanup_all = false;
            self.view = View::List;
            self.refresh();
        } else {
            // First press — snapshot dead flinks and ask for confirmation.
            let dead_flinks: Vec<String> = self
                .groups
                .iter()
                .flat_map(|g| &g.segments)
                .filter(|s| !s.alive)
                .map(|s| s.entry.flink.as_str().to_string())
                .collect();

            if dead_flinks.is_empty() {
                self.status_msg =
                    Some(("No stale segments to clean".into(), Instant::now()));
                return;
            }

            self.pending_cleanup_flinks = Some(dead_flinks);
            self.confirm_cleanup_all = true;
        }
    }

    pub fn cancel_cleanup(&mut self) {
        self.confirm_cleanup = false;
        self.confirm_cleanup_all = false;
        self.pending_cleanup_flinks = None;
        if let View::Detail(ref mut detail) = self.view {
            detail.confirm_cleanup = false;
        }
    }

    pub fn detail_segment(&self) -> Option<&SegmentInfo> {
        if let View::Detail(ref detail) = self.view {
            self.groups
                .get(detail.group_idx)
                .and_then(|g| g.segments.get(detail.segment_idx))
        } else {
            None
        }
    }
}
