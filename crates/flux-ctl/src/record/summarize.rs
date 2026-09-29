//! Per-queue statistics of a recording, read back from its Parquet files, over
//! a window of wall-clock time.

use std::{
    collections::BTreeMap,
    error::Error,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use hdrhistogram::Histogram;
use parquet::{
    column::reader::get_typed_column_reader,
    data_type::{ByteArray, ByteArrayType, DataType, Int64Type},
    file::reader::{FileReader, RowGroupReader, SerializedFileReader},
};
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::{
    Kind, QueueTotals, REPORT_FORMAT, STEMS, TOTALS, TOTALS_FORMAT, Totals, table::is_file_of,
};

/// Nearest-rank percentiles of a set of nanosecond values (`p99_ns` is the
/// `ceil(0.99 * n)`-th smallest), to 3 significant figures: each, `max_ns`
/// too, is at most 0.1% above the exact value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Percentiles {
    /// The median.
    pub p50_ns: u64,
    /// The 90th percentile.
    pub p90_ns: u64,
    /// The 99th percentile.
    pub p99_ns: u64,
    /// The 99.9th percentile.
    pub p999_ns: u64,
    /// The largest value.
    pub max_ns: u64,
}

impl Percentiles {
    /// `None` when there are no values.
    fn of(values: &Values) -> Option<Self> {
        let at = |q| values.0.value_at_quantile(q);
        (values.len() > 0).then(|| Self {
            p50_ns: at(0.5),
            p90_ns: at(0.9),
            p99_ns: at(0.99),
            p999_ns: at(0.999),
            max_ns: values.0.max(),
        })
    }

    /// p50, p90, p99, p99.9 and max, in that order.
    pub fn values(&self) -> [u64; 5] {
        [self.p50_ns, self.p90_ns, self.p99_ns, self.p999_ns, self.max_ns]
    }
}

/// One queue's values as a histogram, so memory does not grow with their
/// number: ~100-200 KB for nanoseconds up to seconds.
struct Values(Histogram<u64>);

impl Default for Values {
    fn default() -> Self {
        Self(Histogram::new(3).expect("3 significant figures are valid"))
    }
}

impl Values {
    fn push(&mut self, value: u64) {
        // `record` grows the histogram; `saturating_record` would clamp to its
        // current bound. Only a value past `i64::MAX / 2` fails, and is clamped.
        if self.0.record(value).is_err() {
            self.0.saturating_record(value);
        }
    }

    fn len(&self) -> usize {
        self.0.len() as usize
    }
}

/// A timer queue's samples inside the window.
#[derive(Debug, Serialize, Deserialize)]
pub struct TimerStats {
    /// Samples inside the window.
    pub samples: usize,
    /// Over the whole recording, from `record.json`; absent without one.
    pub lost: Option<usize>,
    /// Like `lost`.
    pub invalid: Option<usize>,
    /// Percentiles of the samples' durations; `None` without samples.
    pub dur: Option<Percentiles>,
}

/// A tile's windows that ended inside the window.
#[derive(Debug, Serialize, Deserialize)]
pub struct TileStats {
    /// Windows that ended inside the window.
    pub windows: usize,
    /// Over the whole recording, from `record.json`; absent without one.
    pub lost: Option<usize>,
    /// Time the tile spent busy, summed over the windows.
    pub busy_ns: u64,
    /// The windows' lengths, summed.
    pub window_ns: u64,
    /// `busy_ns / window_ns`.
    pub utilisation: f64,
    /// Percentiles of each window's longest busy loop iteration; `None`
    /// without windows.
    pub busy_max: Option<Percentiles>,
}

/// What `summarize` prints.
#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    /// Version of this report's layout.
    pub format: u32,
    /// The window, in nanoseconds since the Unix epoch; open ends are absent.
    pub from_ns: Option<u64>,
    /// Exclusive.
    pub to_ns: Option<u64>,
    /// Keyed `<kind>/<name>`, e.g. `timing/Tile-Msg`; each recorded queue has
    /// a key, with or without samples in the window.
    pub timers: BTreeMap<String, TimerStats>,
    /// Keyed by tile name.
    pub tiles: BTreeMap<String, TileStats>,
    /// Files left without a footer by a recording killed before it closed
    /// them, and so not read: their period is missing from the numbers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unfinished: Vec<String>,
    /// From `record.json`: `<kind>/<name>` of each queue created after the
    /// recording attached, and so not in the numbers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub late: Vec<String>,
    /// From `record.json`: `<kind>/<name>` of each queue that matched but did
    /// not open, and so not in the numbers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<String>,
}

/// Reads `<dir>`'s `timers-*.parquet`, `tiles-*.parquet` and `record.json`,
/// keeping the rows whose time falls in `[from_ns, to_ns)`: a timer sample's
/// start, a tile window's end.
///
/// A file with no footer is skipped and named in `unfinished`; any other
/// unreadable file is an error.
pub fn summarize(
    dir: &Path,
    from_ns: Option<u64>,
    to_ns: Option<u64>,
) -> Result<Report, Box<dyn Error>> {
    let inside = |t: i64| {
        let t = t as u64;
        from_ns.is_none_or(|f| f <= t) && to_ns.is_none_or(|e| t < e)
    };
    let path = dir.join(TOTALS);
    let (mut late, mut skipped) = (Vec::new(), Vec::new());
    let totals: BTreeMap<(Kind, String), QueueTotals> = match std::fs::read_to_string(&path) {
        Ok(json) => {
            let totals: Totals = serde_json::from_str(&json)?;
            if totals.format != TOTALS_FORMAT {
                let format = totals.format;
                return Err(
                    format!("{}: format {format}, not {TOTALS_FORMAT}", path.display()).into()
                );
            }
            (late, skipped) = (totals.late, totals.skipped);
            totals.queues.into_iter().map(|q| ((q.kind, q.name.clone()), q)).collect()
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
        Err(e) => return Err(format!("{}: {e}", path.display()).into()),
    };

    // Every recorded queue gets a key, whether or not it has rows in the window.
    let recorded =
        |tile: bool| totals.keys().filter(move |(kind, _)| (*kind == Kind::Tile) == tile).cloned();
    let mut durations: BTreeMap<(Kind, String), Values> =
        recorded(false).map(|key| (key, Values::default())).collect();
    let mut tiles: BTreeMap<String, (TileSums, Values)> =
        recorded(true).map(|(_, name)| (name, Default::default())).collect();
    let mut unfinished = Vec::new();

    each_row_group(dir, STEMS[0], &mut unfinished, |group, rows| {
        let wall = column::<Int64Type>(group, 0, rows)?;
        let timer = column::<ByteArrayType>(group, 1, rows)?;
        let kind = column::<ByteArrayType>(group, 2, rows)?;
        let dur = column::<Int64Type>(group, 3, rows)?;
        for i in (0..rows).filter(|&i| inside(wall[i])) {
            let key = (Kind::try_from(text(&kind[i])?)?, text(&timer[i])?.to_owned());
            durations.entry(key).or_default().push(dur[i] as u64);
        }
        Ok(())
    })?;
    each_row_group(dir, STEMS[1], &mut unfinished, |group, rows| {
        let end = column::<Int64Type>(group, 0, rows)?;
        let tile = column::<ByteArrayType>(group, 1, rows)?;
        let window = column::<Int64Type>(group, 2, rows)?;
        let busy = column::<Int64Type>(group, 3, rows)?;
        let max = column::<Int64Type>(group, 4, rows)?;
        for i in (0..rows).filter(|&i| inside(end[i])) {
            let (sums, busy_max) = tiles.entry(text(&tile[i])?.to_owned()).or_default();
            sums.windows += 1;
            sums.busy_ns += busy[i] as u64;
            sums.window_ns += window[i] as u64;
            busy_max.push(max[i] as u64);
        }
        Ok(())
    })?;

    let timers = durations
        .into_iter()
        .map(|((kind, name), values)| {
            let recorded = totals.get(&(kind, name.clone()));
            let stats = TimerStats {
                samples: values.len(),
                lost: recorded.map(|q| q.lost),
                invalid: recorded.map(|q| q.invalid),
                dur: Percentiles::of(&values),
            };
            (format!("{}/{name}", kind.label()), stats)
        })
        .collect();
    let tiles = tiles
        .into_iter()
        .map(|(name, (TileSums { windows, busy_ns, window_ns }, values))| {
            let utilisation = if window_ns == 0 { 0.0 } else { busy_ns as f64 / window_ns as f64 };
            let stats = TileStats {
                windows,
                lost: totals.get(&(Kind::Tile, name.clone())).map(|q| q.lost),
                busy_ns,
                window_ns,
                utilisation,
                busy_max: Percentiles::of(&values),
            };
            (name, stats)
        })
        .collect();
    Ok(Report { format: REPORT_FORMAT, from_ns, to_ns, timers, tiles, unfinished, late, skipped })
}

/// A tile's windows inside the window, and their sums.
#[derive(Default)]
struct TileSums {
    windows: usize,
    busy_ns: u64,
    window_ns: u64,
}

fn text(value: &ByteArray) -> Result<&str, Box<dyn Error>> {
    Ok(value.as_utf8()?)
}

/// Calls `f` with every row group of `<dir>/<stem>-*.parquet` and its row
/// count, in file order, adding to `unfinished` the name of each file that has
/// no footer.
fn each_row_group(
    dir: &Path,
    stem: &str,
    unfinished: &mut Vec<String>,
    mut f: impl FnMut(&dyn RowGroupReader, usize) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| format!("{}: {e}", dir.display()))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| is_file_of(stem, n)))
        .collect();
    files.sort();
    for path in files {
        let mut file = File::open(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        if !has_footer(&mut file)? {
            warn!(file = %path.display(), "no footer, so not read: the recording was killed");
            unfinished.push(path.file_name().unwrap_or_default().to_string_lossy().into_owned());
            continue;
        }
        let reader =
            SerializedFileReader::new(file).map_err(|e| format!("{}: {e}", path.display()))?;
        for i in 0..reader.num_row_groups() {
            let group = reader.get_row_group(i)?;
            f(group.as_ref(), group.metadata().num_rows() as usize)?;
        }
    }
    Ok(())
}

/// Whether `file` ends in the magic `PAR1` that closes a Parquet file's footer.
/// A writer that is killed leaves its row groups on disk but never writes it.
fn has_footer(file: &mut File) -> std::io::Result<bool> {
    let mut magic = [0; 4];
    if file.metadata()?.len() < 8 {
        return Ok(false);
    }
    file.seek(SeekFrom::End(-4))?;
    file.read_exact(&mut magic)?;
    file.rewind()?;
    Ok(&magic == b"PAR1")
}

/// One column of a row group, all `rows` of it.
fn column<T: DataType>(
    group: &dyn RowGroupReader,
    i: usize,
    rows: usize,
) -> Result<Vec<T::T>, Box<dyn Error>> {
    let mut reader = get_typed_column_reader::<T>(group.get_column_reader(i)?);
    let mut values = Vec::with_capacity(rows);
    while values.len() < rows {
        let (read, _, _) = reader.read_records(rows - values.len(), None, None, &mut values)?;
        if read == 0 {
            break;
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_within_a_thousandth() {
        let mut values = Values::default();
        (1..=100_000).for_each(|v| values.push(v));
        let p = Percentiles::of(&values).unwrap();
        for (got, exact) in p.values().into_iter().zip([50_000, 90_000, 99_000, 99_900, 100_000]) {
            assert!((exact..=exact + exact / 1000).contains(&got), "{got} for {exact}");
        }
        assert_eq!(Percentiles::of(&Values::default()), None);
    }
}
