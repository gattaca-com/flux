//! Headless recording of an app's `timing-*`, `latency-*` and `tilemetrics-*`
//! queues: every sample to Parquet on the wall clock, and a count of the
//! samples the reader missed.

mod summarize;
mod table;

use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Display},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration as StdDuration, Instant as StdInstant},
};

use flux::tile::metrics::TileSample;
use flux_communication::{
    QueueError, ReadError, ShmemKind, TimingMessage,
    queue::{Consumer, Queue},
};
use flux_timing::{Duration, IngestionTime, Instant, Nanos, TSC_MASK};
use flux_utils::directories::shmem_dir_with_base;
use parquet::{data_type::ByteArray, errors::ParquetError};
use serde::{Deserialize, Serialize};
pub use summarize::{Percentiles, Report, TileStats, TimerStats, summarize};
use table::{Table, TileRows, TimerRows, is_file_of};
use tracing::{info, warn};

use crate::discovery::scan_base_dir_with_dirs;

/// A timer's ring has 8192 slots and a tile's 4096. At 10 ms a reader keeps up
/// with any producer below ~400 k samples/s.
const POLL: StdDuration = StdDuration::from_millis(10);

/// File stems of the timer and the tile table.
const STEMS: [&str; 2] = ["timers", "tiles"];
/// Each queue's totals, written when a recording finishes.
const TOTALS: &str = "record.json";
/// Version of `record.json`.
const TOTALS_FORMAT: u32 = 1;
/// Version of `summarize`'s output. 2 nests a timer's percentiles under `dur`.
const REPORT_FORMAT: u32 = 2;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
#[serde(into = "&'static str", try_from = "String")]
/// Which kind of queue a sample came from.
pub enum Kind {
    /// `[handler start, handler end]`, or a duration a timer accumulated.
    Timing,
    /// `[ingestion, handler start]`.
    Latency,
    /// A tile's metrics over one window of its loop.
    Tile,
}

impl Kind {
    const ALL: [Self; 3] = [Self::Timing, Self::Latency, Self::Tile];

    /// Its name in the `kind` column, `record.json` and `summarize`'s keys.
    pub fn label(self) -> &'static str {
        match self {
            Self::Timing => "timing",
            Self::Latency => "latency",
            Self::Tile => "tile",
        }
    }
}

impl From<Kind> for &'static str {
    fn from(kind: Kind) -> Self {
        kind.label()
    }
}

impl TryFrom<&str> for Kind {
    type Error = String;

    fn try_from(label: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|k| k.label() == label)
            .ok_or_else(|| format!("no kind {label:?}"))
    }
}

impl TryFrom<String> for Kind {
    type Error = String;

    fn try_from(label: String) -> Result<Self, String> {
        Self::try_from(label.as_str())
    }
}

fn classify(file: &str) -> Option<(Kind, &str)> {
    [("timing-", Kind::Timing), ("latency-", Kind::Latency), ("tilemetrics-", Kind::Tile)]
        .into_iter()
        .find_map(|(prefix, kind)| file.strip_prefix(prefix).map(|name| (kind, name)))
}

/// The timer and tile queues in `shmem_dir` whose file name contains `filter`,
/// as `(kind, name, shmem file)`.
fn queues(app: &str, shmem_dir: &Path, filter: Option<&str>) -> Vec<(Kind, String, PathBuf)> {
    scan_base_dir_with_dirs(&[(app.to_owned(), shmem_dir.to_owned())])
        .into_iter()
        .filter(|e| e.kind == ShmemKind::Queue && filter.is_none_or(|f| e.type_name.contains(f)))
        .filter_map(|e| {
            let (kind, name) = classify(&e.type_name)?;
            Some((kind, name.to_owned(), PathBuf::from(&e.flink)))
        })
        .collect()
}

/// Wall-clock nanoseconds of a TSC reading, from `origin`'s pair of reads.
/// Valid on the machine that took both. The error grows with the distance
/// from `origin`: the tick rate is calibrated once, and NTP slews the wall
/// clock, so `origin` should be recent.
fn wall_ns(origin: IngestionTime, t: Instant) -> u64 {
    let (at, t) = (origin.internal().0 & TSC_MASK, t.0 & TSC_MASK);
    let real = origin.real().0;
    if at <= t {
        real + Nanos::from(Duration(t - at)).0
    } else {
        real.saturating_sub(Nanos::from(Duration(at - t)).0)
    }
}

/// Reads every sample `consumer` holds. A lapped reader resumes at its own
/// slot in the producer's current lap rather than at the head, which keeps
/// what the producer wrote since it passed that slot.
fn drain<T: 'static + Copy>(consumer: &mut Consumer<T>, mut f: impl FnMut(&T)) {
    loop {
        match consumer.try_consume_with_epoch() {
            Ok((m, _, _)) => f(m),
            Err(ReadError::Empty) => return,
            Err(ReadError::SpedPast) => {
                if consumer.resync().is_none() {
                    consumer.recover_after_error();
                }
            }
        }
    }
}

struct Tracked<T: 'static + Copy> {
    name: String,
    kind: Kind,
    /// `name` and `kind` as Parquet values, shared by every row.
    name_value: ByteArray,
    kind_value: ByteArray,
    consumer: Consumer<T>,
    writes_at_attach: usize,
    read: usize,
    invalid: usize,
}

impl<T: 'static + Copy> Tracked<T> {
    fn open(name: &str, kind: Kind, flink: &Path) -> Result<Self, QueueError> {
        let queue = Queue::<T>::try_open_shared(flink)?;
        let writes_at_attach = queue.count();
        let mut consumer = Consumer::new(queue, "flux-ctl-record").without_log();
        // Places the cursor at the producer's head; a sample written between
        // the two reads of the count is counted as lost.
        consumer.subscribe_broadcast();
        Ok(Self {
            name: name.to_owned(),
            kind,
            name_value: ByteArray::from(name),
            kind_value: ByteArray::from(kind.label()),
            consumer,
            writes_at_attach,
            read: 0,
            invalid: 0,
        })
    }

    fn writes(&self) -> usize {
        self.consumer.queue_message_count()
    }

    /// `writes` is the queue's count taken before the last drain, so a sample
    /// written after that drain is not counted as lost.
    fn totals(&self, writes: usize) -> QueueTotals {
        QueueTotals {
            kind: self.kind,
            name: self.name.clone(),
            samples: self.read - self.invalid,
            lost: (writes - self.writes_at_attach).saturating_sub(self.read),
            invalid: self.invalid,
        }
    }
}

/// One queue's counts at the end of a recording. The samples themselves are
/// in the Parquet files; [`summarize`] reads them back.
#[derive(Debug, Serialize, Deserialize)]
pub struct QueueTotals {
    /// The queue's kind.
    pub kind: Kind,
    /// The queue's file name without its kind's prefix, e.g. `Tile-Msg`.
    pub name: String,
    /// Rows written, one per valid sample.
    pub samples: usize,
    /// Samples the producer overwrote before they were read.
    pub lost: usize,
    /// Timer samples read but not written, because they failed `is_valid`.
    pub invalid: usize,
}

/// What [`Recorder::finish`] wrote.
#[derive(Debug)]
pub struct Summary {
    /// Every file written, the Parquet files then `record.json`.
    pub files: Vec<PathBuf>,
    /// Sorted by kind, then name.
    pub queues: Vec<QueueTotals>,
    /// `<kind>/<name>` of each queue created after attach, and so not read;
    /// sorted.
    pub late: Vec<String>,
}

/// `record.json`.
#[derive(Serialize, Deserialize)]
struct Totals {
    format: u32,
    queues: Vec<QueueTotals>,
    /// `<kind>/<name>` of each queue created after attach, and so not read.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    late: Vec<String>,
    /// `<kind>/<name>` of each queue that matched but did not open.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    skipped: Vec<String>,
}

/// Reads every timer and tile queue of one app into `<out>/timers-<n>.parquet`
/// and `<out>/tiles-<n>.parquet`.
pub struct Recorder {
    app: String,
    shmem_dir: PathBuf,
    filter: Option<String>,
    /// `<kind>/<name>` of each queue that matched but did not open.
    skipped: Vec<String>,
    out: PathBuf,
    timers: Vec<Tracked<TimingMessage>>,
    tiles: Vec<Tracked<TileSample>>,
    timer_table: Table<TimerRows>,
    tile_table: Table<TileRows>,
}

impl Recorder {
    /// Attaches to the queues that exist now; a queue created later is not
    /// read. `filter` keeps only queues whose file name contains it, and
    /// `rotate` starts a new file per period.
    pub fn attach(
        base_dir: &Path,
        app: &str,
        out: &Path,
        rotate: Option<StdDuration>,
        filter: Option<&str>,
    ) -> Result<Self, Box<dyn Error>> {
        let shmem_dir = shmem_dir_with_base(base_dir, app);
        let mut timers = Vec::new();
        let mut tiles = Vec::new();
        let mut skipped = Vec::new();
        for (kind, name, flink) in queues(app, &shmem_dir, filter) {
            let opened = match kind {
                Kind::Tile => Tracked::open(&name, kind, &flink).map(|t| tiles.push(t)),
                _ => Tracked::open(&name, kind, &flink).map(|t| timers.push(t)),
            };
            if let Err(e) = opened {
                let queue = format!("{}/{name}", kind.label());
                warn!("not reading {queue}: {e}");
                skipped.push(queue);
            }
        }
        if timers.is_empty() && tiles.is_empty() {
            return Err(format!("no timer or tile queue in {}", shmem_dir.display()).into());
        }
        let in_out = |e: std::io::Error| format!("{}: {e}", out.display());
        std::fs::create_dir_all(out).map_err(in_out)?;
        // One recording per directory: `timers-*.parquet` reads every file in it.
        let earlier = std::fs::read_dir(out).map_err(in_out)?.flatten().find(|e| {
            e.file_name().to_str().is_some_and(|n| STEMS.iter().any(|s| is_file_of(s, n)))
        });
        if let Some(file) = earlier {
            return Err(format!(
                "{} already holds a recording ({}); remove it or record elsewhere",
                out.display(),
                file.file_name().to_string_lossy()
            )
            .into());
        }
        info!(
            "reading {} timer and {} tile queues of {app} into {}",
            timers.len(),
            tiles.len(),
            out.display()
        );
        Ok(Self {
            app: app.to_owned(),
            shmem_dir,
            filter: filter.map(str::to_owned),
            skipped,
            out: out.to_owned(),
            timers,
            tiles,
            timer_table: Table::new(out, STEMS[0], rotate)?,
            tile_table: Table::new(out, STEMS[1], rotate)?,
        })
    }

    /// Timer queues and tile queues attached.
    pub fn queue_counts(&self) -> (usize, usize) {
        (self.timers.len(), self.tiles.len())
    }

    /// Polls until `stop` is set or a limit in `until` is reached, then
    /// finishes and reads the recording back with [`summarize`].
    pub fn run(mut self, until: Until, stop: &AtomicBool) -> Result<Recording, Box<dyn Error>> {
        let started = StdInstant::now();
        let stopped = loop {
            if stop.load(Ordering::Relaxed) {
                break Stop::Requested;
            }
            if until.duration.is_some_and(|d| started.elapsed() >= d) {
                break Stop::Duration;
            }
            if let Err(e) = self.poll() {
                // Close what still closes, so the files so far stay readable.
                let _ = self.finish();
                return Err(e.into());
            }
            std::thread::sleep(POLL);
        };
        let (app, out) = (self.app.clone(), self.out.clone());
        let summary = self.finish()?;
        let report = summarize(&out, None, None)?;
        Ok(Recording { app, out, elapsed: started.elapsed(), stopped, summary, report })
    }

    /// Drains every queue, and writes out what a table has buffered once it
    /// holds a row group or its file's period has ended.
    pub fn poll(&mut self) -> Result<(), ParquetError> {
        let Self { timers, tiles, timer_table, tile_table, .. } = self;
        // Taken each poll, so a sample is never more than a ring's age from it.
        let origin = IngestionTime::now();
        let polled_at = origin.real().0;
        let rows = &mut timer_table.rows;
        for t in timers.iter_mut() {
            drain(&mut t.consumer, |m| {
                t.read += 1;
                if !m.is_valid() {
                    t.invalid += 1;
                    return;
                }
                let dur = Nanos::from(m.elapsed()).0;
                // An accumulated duration is written as `Instant(0)..d`, so it
                // has no start on the clock; the read stands in for it.
                let wall = if m.start_t.0 & TSC_MASK == 0 {
                    polled_at
                } else {
                    wall_ns(origin, m.start_t)
                };
                rows.wall_ns.push(wall as i64);
                rows.timer.push(t.name_value.clone());
                rows.kind.push(t.kind_value.clone());
                rows.dur_ns.push(dur as i64);
            });
        }
        let rows = &mut tile_table.rows;
        for t in tiles.iter_mut() {
            // `TileSample` already holds nanoseconds, its `busy_ticks` too.
            drain(&mut t.consumer, |s| {
                t.read += 1;
                rows.window_end_ns.push(s.window_end.0 as i64);
                rows.tile.push(t.name_value.clone());
                rows.window_ns.push(s.total_ticks() as i64);
                rows.busy_ns.push(s.busy_ticks as i64);
                rows.busy_max_ns.push(s.busy_max as i64);
                rows.busy_count.push(s.busy_count as i32);
                rows.loop_count.push(s.loop_count as i32);
            });
        }
        timer_table.tick()?;
        tile_table.tick()
    }

    /// Takes each queue's write count, drains once more, closes the files,
    /// and writes each queue's totals to `record.json`. Each table is closed
    /// even when the drain or the other table fails, so the files keep their
    /// footers where they can.
    pub fn finish(mut self) -> Result<Summary, Box<dyn Error>> {
        let timer_writes: Vec<_> = self.timers.iter().map(Tracked::writes).collect();
        let tile_writes: Vec<_> = self.tiles.iter().map(Tracked::writes).collect();
        let polled = self.poll();
        let timer_files = self.timer_table.finish();
        let tile_files = self.tile_table.finish();
        polled?;
        let mut files = timer_files?;
        files.extend(tile_files?);

        let mut queues = BTreeMap::new();
        for (t, writes) in self.timers.iter().zip(timer_writes) {
            queues.insert((t.kind, t.name.clone()), t.totals(writes));
        }
        for (t, writes) in self.tiles.iter().zip(tile_writes) {
            queues.insert((t.kind, t.name.clone()), t.totals(writes));
        }
        let mut late: Vec<_> = self::queues(&self.app, &self.shmem_dir, self.filter.as_deref())
            .into_iter()
            .filter(|(kind, name, _)| !queues.contains_key(&(*kind, name.clone())))
            .map(|(kind, name, _)| format!("{}/{name}", kind.label()))
            .filter(|key| !self.skipped.contains(key))
            .collect();
        late.sort();
        if !late.is_empty() {
            warn!("created after attach, not recorded: {}", late.join(" "));
        }
        let totals = Totals {
            format: TOTALS_FORMAT,
            queues: queues.into_values().collect(),
            late,
            skipped: self.skipped,
        };
        let path = self.out.join(TOTALS);
        let json = serde_json::to_string_pretty(&totals)?;
        std::fs::write(&path, json + "\n").map_err(|e| format!("{}: {e}", path.display()))?;
        files.push(path);
        Ok(Summary { files, queues: totals.queues, late: totals.late })
    }
}

/// When [`Recorder::run`] stops, besides its `stop` flag. `None` is no limit.
#[derive(Clone, Copy, Debug, Default)]
pub struct Until {
    /// Time since [`Recorder::run`] started.
    pub duration: Option<StdDuration>,
}

/// What ended a [`Recorder::run`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stop {
    /// The `stop` flag.
    Requested,
    /// [`Until::duration`].
    Duration,
}

/// A finished recording: what [`Recorder::finish`] wrote, and its
/// [`summarize`] report over the whole run. `Display` prints it as a table.
pub struct Recording {
    /// The app recorded.
    pub app: String,
    /// The output directory.
    pub out: PathBuf,
    /// Time from [`Recorder::run`] starting to it stopping polling.
    pub elapsed: StdDuration,
    /// What ended it.
    pub stopped: Stop,
    /// What [`Recorder::finish`] wrote.
    pub summary: Summary,
    /// [`summarize`] over the whole recording.
    pub report: Report,
}

impl Display for Recording {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { app, out, elapsed, stopped, summary, report } = self;
        let why = match stopped {
            Stop::Requested => "stopped",
            Stop::Duration => "the duration ended",
        };
        let secs = elapsed.as_secs_f64();
        writeln!(f, "{secs:.1} s of {app}, until {why}; tile rows are busy_max per window")?;
        let names: Vec<_> = summary.files.iter().filter_map(|f| f.file_name()?.to_str()).collect();
        writeln!(f, "wrote {} in {}", names.join(" "), out.display())?;
        row(f, [
            &"queue",
            &"samples",
            &"lost",
            &"invalid",
            &"p50_us",
            &"p90_us",
            &"p99_us",
            &"p99.9_us",
            &"max_us",
        ])?;
        let us = |p: Option<Percentiles>| -> Vec<String> {
            p.map_or_else(
                || vec!["-".to_owned(); 5],
                |p| p.values().iter().map(|&ns| format!("{:.1}", ns as f64 / 1e3)).collect(),
            )
        };
        let or_dash = |n: Option<usize>| n.map_or_else(|| "-".to_owned(), |n| n.to_string());
        for (key, t) in &report.timers {
            let us = us(t.dur);
            row(f, [
                key,
                &t.samples,
                &or_dash(t.lost),
                &or_dash(t.invalid),
                &us[0],
                &us[1],
                &us[2],
                &us[3],
                &us[4],
            ])?;
        }
        for (name, t) in &report.tiles {
            let us = us(t.busy_max);
            row(f, [
                &format!("tile/{name}"),
                &t.windows,
                &or_dash(t.lost),
                &"-",
                &us[0],
                &us[1],
                &us[2],
                &us[3],
                &us[4],
            ])?;
        }
        Ok(())
    }
}

fn row(f: &mut fmt::Formatter<'_>, cells: [&dyn Display; 9]) -> fmt::Result {
    let [queue, samples, lost, invalid, p50, p90, p99, p999, max] = cells;
    writeln!(
        f,
        "{queue:<52} {samples:>9} {lost:>7} {invalid:>7} {p50:>9} {p90:>9} {p99:>9} {p999:>9} {max:>9}"
    )
}
