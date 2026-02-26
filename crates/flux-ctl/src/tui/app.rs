use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind};
use flux_communication::registry::{ShmemKind, cleanup_flink};
use flux_timing::{Duration, Instant};

use crate::{discovery, discovery::registry::DiscoveredEntry};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortMode {
    Name,
    Kind,
    Status,
}

impl SortMode {
    pub fn next(self) -> Self {
        match self {
            SortMode::Name => SortMode::Kind,
            SortMode::Kind => SortMode::Status,
            SortMode::Status => SortMode::Name,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            SortMode::Name => "name",
            SortMode::Kind => "kind",
            SortMode::Status => "status",
        }
    }
}

#[derive(Clone)]
pub struct SegmentInfo {
    pub entry: DiscoveredEntry,
    pub alive: bool,
    pub queue_writes: Option<usize>,
    /// Current write position within the ring buffer (`count & mask`).
    pub queue_fill: Option<usize>,
    /// Ring buffer capacity (`mask + 1`).
    pub queue_capacity: Option<usize>,
    pub poison: Option<discovery::PoisonInfo>,
    /// Messages per second computed from delta writes / delta time.
    pub msgs_per_sec: Option<f64>,
    /// PIDs attached to this segment's backing shmem (from `/proc` scan).
    pub pids: Vec<u32>,
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
    /// Dead flinks captured on the first press of "cleanup all", used on
    /// confirm.
    pub pending_cleanup_flinks: Option<Vec<String>>,
    /// Whether the interactive filter input is active.
    pub filter_mode: bool,
    /// Current filter text typed by the user.
    pub filter_text: String,
    /// Current sort order for segments within each group.
    pub sort_mode: SortMode,
    /// Tracks (last_writes, timestamp) per flink for msgs/s calculation.
    prev_writes: HashMap<String, (usize, Instant)>,
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
            filter_mode: false,
            filter_text: String::new(),
            sort_mode: SortMode::Name,
            prev_writes: HashMap::new(),
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
            filter_mode: false,
            filter_text: String::new(),
            sort_mode: SortMode::Name,
            prev_writes: HashMap::new(),
        };
        app.recount_rows();
        app
    }

    pub fn refresh(&mut self) {
        if let Err(e) = self.try_refresh() {
            self.status_msg = Some((format!("Refresh error: {e}"), Instant::now()));
        }
    }

    /// Inner refresh that returns errors instead of panicking, so the TUI
    /// stays alive and shows a status message on failure.
    fn try_refresh(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Preserve collapsed state across refreshes.
        let prev_expanded: HashMap<String, bool> =
            self.groups.iter().map(|g| (g.name.clone(), g.expanded)).collect();

        self.groups.clear();
        let all_entries = discovery::scan_base_dir(&self.base_dir);
        let app_names = discovery::app_names(&all_entries);
        let proc_map = discovery::scan_proc_fds();

        for name in app_names {
            if let Some(ref filter) = self.app_filter &&
                &name != filter
            {
                continue;
            }
            let filter_lower = self.filter_text.to_lowercase();
            let segments: Vec<SegmentInfo> = all_entries
                .iter()
                .filter(|e| e.app_name == name)
                .filter(|e| discovery::entry_visible(e))
                .filter(|e| {
                    filter_lower.is_empty() ||
                        e.type_name.to_lowercase().contains(&filter_lower) ||
                        e.app_name.to_lowercase().contains(&filter_lower) ||
                        format!("{}", e.kind).to_lowercase().contains(&filter_lower)
                })
                .map(|e| {
                    // Without PID tracking, we consider a segment alive if its
                    // backing flink is reachable.
                    let alive = discovery::flink_reachable(&e.flink);
                    let (queue_writes, queue_fill, queue_capacity) =
                        if alive && e.kind == ShmemKind::Queue {
                            match discovery::QueueStats::read(&e.flink) {
                                Some(stats) => {
                                    (Some(stats.writes), Some(stats.fill), Some(stats.capacity))
                                }
                                None => (None, None, None),
                            }
                        } else {
                            (None, None, None)
                        };
                    let poison = if alive { discovery::PoisonInfo::check(e) } else { None };
                    let msgs_per_sec = queue_writes.and_then(|w| {
                        let flink = e.flink.clone();
                        let now = Instant::now();
                        let rate = self.prev_writes.get(&flink).and_then(|(prev_w, prev_t)| {
                            let dt = now.elapsed_since(*prev_t).as_secs();
                            if dt > 0.1 && w >= *prev_w {
                                Some((w - *prev_w) as f64 / dt)
                            } else {
                                None
                            }
                        });
                        self.prev_writes.insert(flink, (w, now));
                        rate
                    });
                    let pids = e.pids(&proc_map);
                    SegmentInfo {
                        entry: e.clone(),
                        alive,
                        queue_writes,
                        queue_fill,
                        queue_capacity,
                        poison,
                        msgs_per_sec,
                        pids,
                    }
                })
                .collect();
            if segments.is_empty() {
                continue;
            }
            let expanded = prev_expanded.get(&name).copied().unwrap_or(true);
            self.groups.push(AppGroup { name, segments, expanded });
        }

        // Sort segments within each group according to the current sort mode.
        let sort_mode = self.sort_mode;
        for group in &mut self.groups {
            match sort_mode {
                SortMode::Name => {
                    group.segments.sort_by(|a, b| a.entry.type_name.cmp(&b.entry.type_name));
                }
                SortMode::Kind => {
                    group.segments.sort_by(|a, b| {
                        format!("{}", a.entry.kind).cmp(&format!("{}", b.entry.kind))
                    });
                }
                SortMode::Status => {
                    group.segments.sort_by(|a, b| {
                        // alive first, then dead, then poisoned
                        fn rank(s: &SegmentInfo) -> u8 {
                            if s.poison.is_some() {
                                2
                            } else if s.alive {
                                0
                            } else {
                                1
                            }
                        }
                        rank(a).cmp(&rank(b))
                    });
                }
            }
        }

        self.recount_rows();
        self.refresh_detail_pids();
        self.last_refresh = Instant::now();
        Ok(())
    }

    pub fn recount_rows(&mut self) {
        self.total_rows =
            self.groups.iter().map(|g| 1 + if g.expanded { g.segments.len() } else { 0 }).sum();
        if self.selected >= self.total_rows && self.total_rows > 0 {
            self.selected = self.total_rows - 1;
        }
    }

    fn refresh_detail_pids(&mut self) {
        if let View::Detail(ref mut detail) = self.view {
            let seg =
                self.groups.get(detail.group_idx).and_then(|g| g.segments.get(detail.segment_idx));
            match seg {
                Some(seg) => detail.pids = discovery::PidInfo::for_pids(&seg.pids),
                None => self.view = View::List,
            }
        }
    }

    pub fn tick(&mut self) {
        if let Some((_, ts)) = &self.status_msg &&
            ts.elapsed() >= status_msg_duration()
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

    pub fn home(&mut self) {
        match &mut self.view {
            View::List => self.selected = 0,
            View::Detail(detail) => detail.selected_pid = 0,
        }
    }

    pub fn end(&mut self) {
        match &mut self.view {
            View::List => {
                if self.total_rows > 0 {
                    self.selected = self.total_rows - 1;
                }
            }
            View::Detail(detail) => {
                if !detail.pids.is_empty() {
                    detail.selected_pid = detail.pids.len() - 1;
                }
            }
        }
    }

    pub fn page_up(&mut self) {
        match &mut self.view {
            View::List => {
                self.selected = self.selected.saturating_sub(10);
            }
            View::Detail(detail) => {
                detail.selected_pid = detail.selected_pid.saturating_sub(10);
            }
        }
    }

    pub fn page_down(&mut self) {
        match &mut self.view {
            View::List => {
                if self.total_rows > 0 {
                    self.selected = (self.selected + 10).min(self.total_rows.saturating_sub(1));
                }
            }
            View::Detail(detail) => {
                if !detail.pids.is_empty() {
                    detail.selected_pid =
                        (detail.selected_pid + 10).min(detail.pids.len().saturating_sub(1));
                }
            }
        }
    }

    pub fn toggle_sort(&mut self) {
        self.sort_mode = self.sort_mode.next();
        self.refresh();
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
                                let pids = discovery::PidInfo::for_pids(&group.segments[si].pids);
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
            Some(SelectedItem::Segment(_, _, seg)) => (seg.alive, seg.entry.flink.clone()),
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
            let flink = seg.entry.flink.clone();
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
                .map(|s| s.entry.flink.clone())
                .collect();

            if dead_flinks.is_empty() {
                self.status_msg = Some(("No stale segments to clean".into(), Instant::now()));
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
            self.groups.get(detail.group_idx).and_then(|g| g.segments.get(detail.segment_idx))
        } else {
            None
        }
    }

    /// Process a key event, returning `true` if the application should quit.
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if key.kind != KeyEventKind::Press {
            return false;
        }

        if self.show_help {
            self.show_help = false;
            return false;
        }

        // ── Filter mode: capture typed characters ──────────────────────
        if self.filter_mode {
            match key.code {
                KeyCode::Esc => {
                    self.filter_mode = false;
                    self.filter_text.clear();
                    self.refresh();
                }
                KeyCode::Enter => {
                    self.filter_mode = false;
                }
                KeyCode::Backspace => {
                    self.filter_text.pop();
                    self.refresh();
                }
                KeyCode::Char(c) => {
                    self.filter_text.push(c);
                    self.refresh();
                }
                _ => {}
            }
            return false;
        }

        let confirming = match &self.view {
            View::List => self.confirm_cleanup || self.confirm_cleanup_all,
            View::Detail(d) => d.confirm_cleanup,
        };
        if confirming {
            match key.code {
                KeyCode::Enter => {
                    if self.confirm_cleanup_all {
                        self.request_cleanup_all();
                    } else {
                        self.request_cleanup();
                    }
                }
                _ => self.cancel_cleanup(),
            }
            return false;
        }

        match &self.view {
            View::List => match key.code {
                KeyCode::Char('q') | KeyCode::Esc => {
                    if !self.filter_text.is_empty() {
                        self.filter_text.clear();
                        self.refresh();
                    } else {
                        return true;
                    }
                }
                KeyCode::Char('?') => self.toggle_help(),
                KeyCode::Up | KeyCode::Char('k') => self.previous(),
                KeyCode::Down | KeyCode::Char('j') => self.next(),
                KeyCode::Home | KeyCode::Char('g') => self.home(),
                KeyCode::End | KeyCode::Char('G') => self.end(),
                KeyCode::PageUp => self.page_up(),
                KeyCode::PageDown => self.page_down(),
                KeyCode::Enter => self.enter(),
                KeyCode::Char('d') => self.request_cleanup(),
                KeyCode::Char('D') => self.request_cleanup_all(),
                KeyCode::Char('r') => self.refresh(),
                KeyCode::Char('/') => {
                    self.filter_mode = true;
                }
                KeyCode::Char('s') => self.toggle_sort(),
                _ => {}
            },
            View::Detail(_) => match key.code {
                KeyCode::Char('q') => return true,
                KeyCode::Esc | KeyCode::Backspace => self.back(),
                KeyCode::Char('?') => self.toggle_help(),
                KeyCode::Char('d') => self.request_cleanup(),
                KeyCode::Char('D') => self.request_cleanup_all(),
                KeyCode::Char('r') => self.refresh(),
                KeyCode::Up | KeyCode::Char('k') => self.previous(),
                KeyCode::Down | KeyCode::Char('j') => self.next(),
                KeyCode::Home | KeyCode::Char('g') => self.home(),
                KeyCode::End | KeyCode::Char('G') => self.end(),
                KeyCode::PageUp => self.page_up(),
                KeyCode::PageDown => self.page_down(),
                _ => {}
            },
        }

        false
    }
}
