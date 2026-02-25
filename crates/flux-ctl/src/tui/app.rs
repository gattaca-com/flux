use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Instant;

use flux_communication::queue::QueueHeader;
use flux_communication::registry::{ShmemEntry, ShmemKind};

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

pub struct App {
    pub groups: Vec<AppGroup>,
    pub selected: usize,
    pub total_rows: usize,
    pub base_dir: PathBuf,
    pub app_filter: Option<String>,
    pub last_refresh: Instant,
    pub show_help: bool,
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

    pub fn tick(&mut self) {
        if self.last_refresh.elapsed() >= std::time::Duration::from_secs(1) {
            self.refresh();
        }
    }

    pub fn next(&mut self) {
        if self.total_rows > 0 {
            self.selected = (self.selected + 1).min(self.total_rows.saturating_sub(1));
        }
    }

    pub fn previous(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    pub fn toggle_help(&mut self) {
        self.show_help = !self.show_help;
    }

    pub fn toggle_expand(&mut self) {
        let mut row = 0;
        for group in &mut self.groups {
            if row == self.selected {
                group.expanded = !group.expanded;
                break;
            }
            row += 1;
            if group.expanded {
                row += group.segments.len();
            }
        }
        self.recount_rows();
    }
}
