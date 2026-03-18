use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind};
use flux_communication::{ShmemKind, cleanup_flink};
use flux_timing::{Duration, Instant};
use ratatui::widgets::TableState;

use crate::{
    discovery,
    discovery::{DiscoveredEntry, ShmemCache},
};

/// Rolling window (in seconds) over which msgs/s samples are averaged.
const RATE_SMOOTHING_WINDOW_SECS: f64 = 30.0;

/// Averages raw per-tick msgs/s samples over a [`RATE_SMOOTHING_WINDOW_SECS`]
/// rolling window so the displayed rate is less jittery.
struct RateSmoother {
    /// Per-segment ring of `(timestamp, rate)` samples, keyed by flink path.
    samples: HashMap<String, VecDeque<(Instant, f64)>>,
}

impl RateSmoother {
    fn new() -> Self {
        Self { samples: HashMap::new() }
    }

    /// Record a raw msgs/s sample for the segment identified by `key` and
    /// return the smoothed (averaged) rate over the rolling window.
    fn record(&mut self, key: &str, rate: f64) -> f64 {
        let now = Instant::now();
        let ring = self.samples.entry(key.to_owned()).or_default();
        ring.push_back((now, rate));
        // Evict samples older than the window.
        while ring
            .front()
            .is_some_and(|(t, _)| t.elapsed().as_secs() > RATE_SMOOTHING_WINDOW_SECS)
        {
            ring.pop_front();
        }
        let sum: f64 = ring.iter().map(|(_, r)| r).sum();
        sum / ring.len() as f64
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortMode {
    Name,
    Kind,
    Status,
    Activity,
}

impl SortMode {
    pub fn next(self) -> Self {
        match self {
            SortMode::Name => SortMode::Kind,
            SortMode::Kind => SortMode::Status,
            SortMode::Status => SortMode::Activity,
            SortMode::Activity => SortMode::Name,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            SortMode::Name => "name",
            SortMode::Kind => "kind",
            SortMode::Status => "status",
            SortMode::Activity => "activity",
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
    /// Longest consumer-group lag as a percentage of queue capacity.
    ///
    /// Computed as `(writes - min_cursor) / capacity * 100` across all
    /// registered consumer groups.  Only set for alive Queue segments with
    /// at least one consumer group.
    pub max_lagger_pct: Option<f64>,
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
    pub consumer_groups: Vec<discovery::ConsumerGroupInfo>,
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
    /// Whether to hide dead (stale) segments from the list view.
    pub hide_dead: bool,
    /// Current sort order for segments within each group.
    pub sort_mode: SortMode,
    /// Rolling (writes, timestamp) history per flink for raw msgs/s
    /// computation over a 10-second window.
    write_history: HashMap<String, VecDeque<(usize, Instant)>>,
    /// Smooths raw per-tick msgs/s over a 30-second rolling window.
    rate_smoother: RateSmoother,
    /// Cached result of scan_proc_fds() with TTL.
    cached_proc_map: HashMap<PathBuf, Vec<u32>>,
    /// Last time proc_map was scanned.
    proc_map_last_scan: Option<Instant>,
    /// Persistent shmem cache — segments stay mapped across ticks so reading
    /// headers is a plain pointer dereference (zero syscalls).
    shmem_cache: ShmemCache,
    /// Persistent table state so the viewport offset survives across renders.
    pub table_state: TableState,
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
            hide_dead: true,
            sort_mode: SortMode::Name,
            write_history: HashMap::new(),
            rate_smoother: RateSmoother::new(),
            cached_proc_map: HashMap::new(),
            proc_map_last_scan: None,
            shmem_cache: ShmemCache::new(),
            table_state: TableState::default().with_selected(0),
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
            hide_dead: true,
            sort_mode: SortMode::Name,
            write_history: HashMap::new(),
            rate_smoother: RateSmoother::new(),
            cached_proc_map: HashMap::new(),
            proc_map_last_scan: None,
            shmem_cache: ShmemCache::new(),
            table_state: TableState::default().with_selected(0),
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

        // Periodically rescan the filesystem for new/removed segments (10s
        // TTL).  Between rescans we just re-read headers from the
        // already-mapped memory — zero syscalls.
        self.shmem_cache.refresh(&self.base_dir);

        // Read live stats from cached mappings (pure pointer reads).
        let all_entries = self.shmem_cache.read_entries();
        let app_names = DiscoveredEntry::app_names(&all_entries);

        // Cache proc scan with 5-second TTL
        let now = Instant::now();
        let should_rescan_proc = self
            .proc_map_last_scan
            .is_none_or(|last| now.elapsed_since(last) >= Duration::from_secs(5));

        if should_rescan_proc {
            self.cached_proc_map = discovery::scan_proc_fds();
            self.proc_map_last_scan = Some(now);
        }
        let own_pid = std::process::id();

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
                .filter(|e| {
                    filter_lower.is_empty() ||
                        e.type_name.to_lowercase().contains(&filter_lower) ||
                        e.app_name.to_lowercase().contains(&filter_lower) ||
                        e.kind.as_str_lowercase().contains(&filter_lower)
                })
                .map(|e| {
                    let pids = e.pids(&self.cached_proc_map);
                    let alive = pids.iter().any(|&p| p != own_pid);
                    let (queue_writes, queue_fill, queue_capacity) =
                        if alive && e.kind == ShmemKind::Queue {
                            (e.queue_writes, e.queue_fill, Some(e.capacity))
                        } else {
                            (None, None, None)
                        };
                    // Use the poison result that was already read from the
                    // cached mapping — no extra mmap.
                    let poison = if alive {
                        match e.poison_quick {
                            Some(true) => Some(discovery::PoisonInfo {
                                n_poisoned: 1,
                                first_slot: 0,
                                total_slots: e.capacity,
                            }),
                            _ => None,
                        }
                    } else {
                        None
                    };
                    let raw_rate = queue_writes.and_then(|w| {
                        let flink = e.flink.clone();
                        let now = Instant::now();
                        let history = self.write_history.entry(flink).or_default();
                        history.push_back((w, now));
                        // Drop entries older than 10 seconds.
                        while let Some(&(_, t)) = history.front() {
                            if now.elapsed_since(t).as_secs() > 10.0 {
                                history.pop_front();
                            } else {
                                break;
                            }
                        }
                        if history.len() >= 2 {
                            let &(oldest_w, oldest_t) = history.front().unwrap();
                            let &(newest_w, _) = history.back().unwrap();
                            let dt = now.elapsed_since(oldest_t).as_secs();
                            if dt > 0.1 && newest_w >= oldest_w {
                                Some((newest_w - oldest_w) as f64 / dt)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    });
                    // Smooth the rate over a 30-second rolling window.
                    // When a segment stops reporting (raw_rate is None) we
                    // still record 0 so stale non-zero samples are pushed
                    // out of the window.
                    let msgs_per_sec = if alive && e.kind == ShmemKind::Queue {
                        let rate = raw_rate.unwrap_or(0.0);
                        Some(self.rate_smoother.record(&e.flink, rate))
                    } else {
                        raw_rate
                    };
                    // Longest consumer-group lag as % of capacity.
                    let max_lagger_pct = queue_writes.and_then(|writes| {
                        let capacity = queue_capacity?;
                        if capacity == 0 {
                            return None;
                        }
                        let groups = self.shmem_cache.consumer_groups(&e.flink);
                        if groups.is_empty() {
                            return None;
                        }
                        let max_lag = groups
                            .iter()
                            .map(|g| writes.saturating_sub(g.cursor))
                            .max()
                            .unwrap_or(0);
                        Some(max_lag as f64 / capacity as f64 * 100.0)
                    });
                    let pids: Vec<u32> = pids.into_iter().filter(|&p| p != own_pid).collect();
                    SegmentInfo {
                        entry: e.clone(),
                        alive,
                        queue_writes,
                        queue_fill,
                        queue_capacity,
                        poison,
                        msgs_per_sec,
                        max_lagger_pct,
                        pids,
                    }
                })
                .filter(|s| !self.hide_dead || s.alive)
                .collect();
            if segments.is_empty() {
                continue;
            }
            let expanded = prev_expanded.get(&name).copied().unwrap_or(false);
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
                    group.segments.sort_by(|a, b| (a.entry.kind as u8).cmp(&(b.entry.kind as u8)));
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
                SortMode::Activity => {
                    group.segments.sort_by(|a, b| {
                        let a_rate = a.msgs_per_sec.unwrap_or(-1.0);
                        let b_rate = b.msgs_per_sec.unwrap_or(-1.0);
                        b_rate.partial_cmp(&a_rate).unwrap_or(std::cmp::Ordering::Equal)
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
            let seg_opt = self
                .groups
                .get_mut(detail.group_idx)
                .and_then(|g| g.segments.get_mut(detail.segment_idx));
            match seg_opt {
                Some(seg) => {
                    detail.pids = discovery::PidInfo::for_pids(&seg.pids);
                    // Refresh full poison scan for detail view
                    if !seg.pids.is_empty() {
                        seg.poison = discovery::PoisonInfo::check(&seg.entry);
                    }
                    // Refresh consumer groups (cursors move in real-time)
                    detail.consumer_groups = self.shmem_cache.consumer_groups(&seg.entry.flink);
                }
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

                                // Perform full poison scan for detail view
                                let segment = &mut self.groups[gi].segments[si];
                                if !segment.pids.is_empty() {
                                    segment.poison = discovery::PoisonInfo::check(&segment.entry);
                                }

                                let consumer_groups =
                                    self.shmem_cache.consumer_groups(&segment.entry.flink);

                                self.view = View::Detail(DetailState {
                                    group_idx: gi,
                                    segment_idx: si,
                                    pids,
                                    selected_pid: 0,
                                    confirm_cleanup: false,
                                    consumer_groups,
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
                KeyCode::Char('a') => {
                    self.hide_dead = !self.hide_dead;
                    self.refresh();
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_smoother_single_sample() {
        let mut s = RateSmoother::new();
        let rate = s.record("seg_a", 100.0);
        assert!((rate - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn rate_smoother_averages_multiple_samples() {
        let mut s = RateSmoother::new();
        s.record("seg_a", 100.0);
        s.record("seg_a", 200.0);
        let avg = s.record("seg_a", 300.0);
        // (100 + 200 + 300) / 3 = 200
        assert!((avg - 200.0).abs() < f64::EPSILON);
    }

    #[test]
    fn rate_smoother_independent_keys() {
        let mut s = RateSmoother::new();
        s.record("seg_a", 100.0);
        s.record("seg_b", 500.0);
        let a = s.record("seg_a", 200.0);
        let b = s.record("seg_b", 600.0);
        // seg_a: (100 + 200) / 2 = 150
        assert!((a - 150.0).abs() < f64::EPSILON);
        // seg_b: (500 + 600) / 2 = 550
        assert!((b - 550.0).abs() < f64::EPSILON);
    }

    #[test]
    fn rate_smoother_zero_decay() {
        // Recording 0s should pull the average down.
        let mut s = RateSmoother::new();
        s.record("seg_a", 1000.0);
        s.record("seg_a", 0.0);
        let avg = s.record("seg_a", 0.0);
        // (1000 + 0 + 0) / 3 ≈ 333.33
        assert!((avg - 1000.0 / 3.0).abs() < 0.01);
    }
}
