//! A recording as Parquet: `<stem>-000.parquet`, `<stem>-001.parquet`, one per
//! `rotate` period. A Parquet file is unreadable until its footer is written at
//! close, so a process killed without the chance to close (SIGKILL, a crash)
//! loses only the current period. `read_parquet('<stem>-*.parquet')` reads
//! them as one table.

use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use parquet::{
    basic::{Compression, Encoding, ZstdLevel},
    data_type::{ByteArray, ByteArrayType, DataType, Int32Type, Int64Type},
    errors::Result,
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedRowGroupWriter},
    },
    schema::{
        parser::parse_message_type,
        types::{ColumnPath, Type},
    },
};

/// Rows kept in memory before they go out as a row group. The reader writes
/// the group between two drains: 64 k rows take ~4 ms, 1 M rows ~46 ms, long
/// enough for a timer at ~145 k samples/s to lap its 8192-slot ring.
const ROW_GROUP: usize = 1 << 16;

/// Whether `file_name` is one of `stem`'s files.
pub fn is_file_of(stem: &str, file_name: &str) -> bool {
    file_name
        .strip_prefix(stem)
        .and_then(|n| n.strip_prefix('-'))
        .is_some_and(|n| n.ends_with(".parquet"))
}

/// A table's rows, column by column, and how to write them.
pub trait Columns: Default {
    const SCHEMA: &'static str;
    /// Near-monotone columns, delta-encoded.
    const DELTA: &'static [&'static str];
    fn len(&self) -> usize;
    fn write(&self, group: &mut SerializedRowGroupWriter<'_, File>) -> Result<()>;
}

pub struct Table<C: Columns> {
    dir: PathBuf,
    stem: &'static str,
    rotate: Option<Duration>,
    schema: Arc<Type>,
    props: Arc<WriterProperties>,
    file: Option<SerializedFileWriter<File>>,
    period_start: Instant,
    written: Vec<PathBuf>,
    /// Bytes of the files already closed.
    closed_bytes: u64,
    pub rows: C,
}

impl<C: Columns> Table<C> {
    pub fn new(dir: &Path, stem: &'static str, rotate: Option<Duration>) -> Result<Self> {
        let mut props =
            WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?));
        for column in C::DELTA {
            let path = ColumnPath::from(*column);
            props = props
                .set_column_dictionary_enabled(path.clone(), false)
                .set_column_encoding(path, Encoding::DELTA_BINARY_PACKED);
        }
        Ok(Self {
            dir: dir.to_owned(),
            stem,
            rotate,
            schema: Arc::new(parse_message_type(C::SCHEMA)?),
            props: Arc::new(props.build()),
            file: None,
            period_start: Instant::now(),
            written: Vec::new(),
            closed_bytes: 0,
            rows: C::default(),
        })
    }

    /// Writes the buffered rows once there are a row group's worth, and closes
    /// the file at the end of its period.
    pub fn tick(&mut self) -> Result<()> {
        if self.rows.len() >= ROW_GROUP {
            self.flush()?;
        }
        if self.rotate.is_some_and(|r| self.period_start.elapsed() >= r) {
            self.close_file()?;
        }
        Ok(())
    }

    /// Bytes on disk: the closed files, and what the open one has written.
    /// Rows still buffered, at most a row group, are not counted.
    pub fn bytes(&self) -> u64 {
        self.closed_bytes + self.file.as_ref().map_or(0, |f| f.bytes_written() as u64)
    }

    /// Closes the last file and returns every file written. A table that got no
    /// rows still gets one, empty.
    pub fn finish(mut self) -> Result<Vec<PathBuf>> {
        self.close_file()?;
        if self.written.is_empty() {
            self.open()?;
            self.close_file()?;
        }
        Ok(self.written)
    }

    fn open(&mut self) -> Result<&mut SerializedFileWriter<File>> {
        if self.file.is_none() {
            let path = self.dir.join(format!("{}-{:03}.parquet", self.stem, self.written.len()));
            let file = File::create(&path)?;
            self.written.push(path);
            self.file = Some(SerializedFileWriter::new(
                file,
                Arc::clone(&self.schema),
                Arc::clone(&self.props),
            )?);
        }
        Ok(self.file.as_mut().unwrap())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows.len() == 0 {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        let mut group = self.open()?.next_row_group()?;
        rows.write(&mut group)?;
        group.close()?;
        Ok(())
    }

    fn close_file(&mut self) -> Result<()> {
        self.flush()?;
        if let Some(file) = self.file.take() {
            file.close()?;
            let path = self.written.last().expect("an open file was pushed to `written`");
            self.closed_bytes += std::fs::metadata(path)?.len();
        }
        self.period_start = Instant::now();
        Ok(())
    }
}

/// Writes the row group's next column. Unsigned values go in the signed
/// physical type by their bits, as each column's `INTEGER(n, false)` says.
pub fn column<T: DataType>(
    group: &mut SerializedRowGroupWriter<'_, File>,
    values: &[T::T],
) -> Result<()> {
    let mut column = group.next_column()?.expect("a column per schema field");
    column.typed::<T>().write_batch(values, None, None)?;
    column.close()
}

#[derive(Default)]
pub struct TimerRows {
    pub wall_ns: Vec<i64>,
    pub timer: Vec<ByteArray>,
    pub kind: Vec<ByteArray>,
    pub dur_ns: Vec<i64>,
}

impl Columns for TimerRows {
    const SCHEMA: &'static str = "message timers {
        required int64 wall_ns (INTEGER(64, false));
        required binary timer (STRING);
        required binary kind (STRING);
        required int64 dur_ns (INTEGER(64, false));
    }";
    const DELTA: &'static [&'static str] = &["wall_ns"];

    fn len(&self) -> usize {
        self.wall_ns.len()
    }

    fn write(&self, group: &mut SerializedRowGroupWriter<'_, File>) -> Result<()> {
        column::<Int64Type>(group, &self.wall_ns)?;
        column::<ByteArrayType>(group, &self.timer)?;
        column::<ByteArrayType>(group, &self.kind)?;
        column::<Int64Type>(group, &self.dur_ns)
    }
}

#[derive(Default)]
pub struct TileRows {
    pub window_end_ns: Vec<i64>,
    pub tile: Vec<ByteArray>,
    pub window_ns: Vec<i64>,
    pub busy_ns: Vec<i64>,
    pub busy_max_ns: Vec<i64>,
    pub busy_count: Vec<i32>,
    pub loop_count: Vec<i32>,
}

impl Columns for TileRows {
    const SCHEMA: &'static str = "message tiles {
        required int64 window_end_ns (INTEGER(64, false));
        required binary tile (STRING);
        required int64 window_ns (INTEGER(64, false));
        required int64 busy_ns (INTEGER(64, false));
        required int64 busy_max_ns (INTEGER(64, false));
        required int32 busy_count (INTEGER(32, false));
        required int32 loop_count (INTEGER(32, false));
    }";
    const DELTA: &'static [&'static str] = &["window_end_ns"];

    fn len(&self) -> usize {
        self.window_end_ns.len()
    }

    fn write(&self, group: &mut SerializedRowGroupWriter<'_, File>) -> Result<()> {
        column::<Int64Type>(group, &self.window_end_ns)?;
        column::<ByteArrayType>(group, &self.tile)?;
        column::<Int64Type>(group, &self.window_ns)?;
        column::<Int64Type>(group, &self.busy_ns)?;
        column::<Int64Type>(group, &self.busy_max_ns)?;
        column::<Int32Type>(group, &self.busy_count)?;
        column::<Int32Type>(group, &self.loop_count)
    }
}

#[cfg(test)]
mod tests {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::*;

    fn rows_in(path: &Path) -> i64 {
        let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
        reader.metadata().file_metadata().num_rows()
    }

    #[test]
    fn rotates_into_readable_files_and_writes_an_empty_table() {
        let dir = tempfile::tempdir().unwrap();

        let mut timers =
            Table::<TimerRows>::new(dir.path(), "timers", Some(Duration::ZERO)).unwrap();
        for n in 0..3 {
            for k in 0..=n {
                timers.rows.wall_ns.push(k);
                timers.rows.timer.push(ByteArray::from("Tile-Msg"));
                timers.rows.kind.push(ByteArray::from("timing"));
                timers.rows.dur_ns.push(u64::MAX as i64);
            }
            timers.tick().unwrap();
        }
        let files = timers.finish().unwrap();
        assert_eq!(files.iter().map(|f| rows_in(f)).collect::<Vec<_>>(), [1, 2, 3]);

        let tiles = Table::<TileRows>::new(dir.path(), "tiles", None).unwrap();
        let files = tiles.finish().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(rows_in(&files[0]), 0);
    }
}
