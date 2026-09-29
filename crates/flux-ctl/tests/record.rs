use std::{fs::File, path::Path, sync::atomic::AtomicBool};

use flux::tile::metrics::TileMetrics;
use flux_communication::{
    Timer, cleanup_shmem,
    queue::{Queue, QueueType},
};
use flux_ctl::record::{Kind, Recorder, Stop, Until, summarize};
use flux_timing::{Duration, IngestionTime, Nanos};
use flux_utils::directories::shmem_dir_queues_with_base;
use parquet::file::reader::{FileReader, SerializedFileReader};
use tempfile::tempdir;

/// A timer's ring size.
const RING: usize = 8192;

fn rows_in(path: &Path) -> i64 {
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    reader.metadata().file_metadata().num_rows()
}

fn emit(timer: &mut Timer, n: usize) {
    for _ in 0..n {
        timer.start();
        timer.record_processing();
    }
}

#[test]
fn records_every_sample_and_counts_what_it_missed() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();
    let out = base.join("run");
    let mut timer = Timer::new_with_base_dir(base, "testapp", "Tile-Msg");
    emit(&mut timer, 5);

    let mut recorder = Recorder::attach(base, "testapp", &out, None, None).unwrap();
    assert_eq!(recorder.queue_counts(), (2, 0));
    emit(&mut timer, 10);
    recorder.poll().unwrap();
    // Laps the reader by 100: it resumes at its own slot in the current lap,
    // so it reads those 100 and loses a ring.
    emit(&mut timer, RING + 100);
    let summary = recorder.finish().unwrap();

    let [timing, latency] = &summary.queues[..] else { panic!("{:?}", summary.queues) };
    assert_eq!((latency.kind, latency.name.as_str()), (Kind::Latency, "Tile-Msg"));
    assert_eq!((latency.samples, latency.lost), (0, 0));

    assert_eq!((timing.kind, timing.name.as_str()), (Kind::Timing, "Tile-Msg"));
    assert_eq!((timing.samples, timing.lost), (10 + 100, RING), "{timing:?}");
    assert_eq!(timing.invalid, 0);

    let parquet = summary.files.iter().filter(|f| f.extension().is_some_and(|e| e == "parquet"));
    let rows: Vec<_> = parquet.map(|f| rows_in(f)).collect();
    assert_eq!(rows, [timing.samples as i64, 0]);
    cleanup_shmem(base);
}

#[test]
fn names_the_queues_created_after_attach() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();
    let _timer = Timer::new_with_base_dir(base, "testapp", "Tile-Msg");
    let recorder = Recorder::attach(base, "testapp", &base.join("run"), None, None).unwrap();
    let _late = Timer::new_with_base_dir(base, "testapp", "Late-Msg");

    let summary = recorder.finish().unwrap();
    assert_eq!(summary.late, ["latency/Late-Msg", "timing/Late-Msg"]);
    assert_eq!(summary.queues.len(), 2);
    let report = summarize(&base.join("run"), None, None).unwrap();
    assert_eq!(report.late, summary.late, "record.json keeps them for summarize");
    assert!(report.skipped.is_empty());
    cleanup_shmem(base);
}

#[test]
fn names_a_queue_it_cannot_read_as_skipped() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();
    let _timer = Timer::new_with_base_dir(base, "testapp", "Tile-Msg");
    // A `timing-` name on a queue of a larger element (a `Seqlock` pads to a line).
    let file = shmem_dir_queues_with_base(base, "testapp").join("timing-Bytes");
    let _bytes = Queue::<[u8; 256]>::create_or_open_shared(file, 64, QueueType::MPMC);

    let recorder = Recorder::attach(base, "testapp", &base.join("run"), None, None).unwrap();
    assert_eq!(recorder.queue_counts(), (2, 0));
    assert!(recorder.finish().unwrap().late.is_empty(), "skipped is not late");
    let report = summarize(&base.join("run"), None, None).unwrap();
    assert_eq!(report.skipped, ["timing/Bytes"]);
    cleanup_shmem(base);
}

#[test]
fn keeps_only_matching_queues_and_refuses_an_app_with_none() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();
    let _timer = Timer::new_with_base_dir(base, "testapp", "Tile-Msg");

    let recorder =
        Recorder::attach(base, "testapp", &base.join("a"), None, Some("timing-")).unwrap();
    assert_eq!(recorder.queue_counts(), (1, 0));
    assert!(Recorder::attach(base, "testapp", &base.join("b"), None, Some("nothing")).is_err());
    assert!(Recorder::attach(base, "other", &base.join("c"), None, None).is_err());
    cleanup_shmem(base);
}

#[test]
fn refuses_a_directory_that_holds_a_recording() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();
    let out = base.join("run");
    let _timer = Timer::new_with_base_dir(base, "testapp", "Tile-Msg");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("notes.txt"), "").unwrap();

    Recorder::attach(base, "testapp", &out, None, None).unwrap().finish().unwrap();
    let err = Recorder::attach(base, "testapp", &out, None, None).err().unwrap();
    assert!(err.to_string().contains("already holds a recording"), "{err}");
    cleanup_shmem(base);
}

#[test]
fn summarize_reads_back_what_record_wrote_inside_a_window() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();
    let out = base.join("run");
    let mut timer = Timer::new_with_base_dir(base, "testapp", "Tile-Msg");
    let recorder = Recorder::attach(base, "testapp", &out, None, None).unwrap();
    // Sample times map from TSC ticks to the wall clock, so leave margin.
    let margin = std::time::Duration::from_millis(50);
    let start = Nanos::now().0;
    std::thread::sleep(margin);
    emit(&mut timer, 10);
    std::thread::sleep(margin);
    let end = Nanos::now().0;
    let summary = recorder.finish().unwrap();
    assert!(summary.files.iter().any(|f| f.ends_with("record.json")));

    let all = summarize(&out, None, None).unwrap();
    let timing = &all.timers["timing/Tile-Msg"];
    assert_eq!((timing.samples, timing.lost, timing.invalid), (10, Some(0), Some(0)));
    assert!(timing.dur.is_some());
    let latency = &all.timers["latency/Tile-Msg"];
    assert_eq!((latency.samples, latency.dur), (0, None));
    assert!(all.tiles.is_empty());

    let inside = summarize(&out, Some(start), Some(end)).unwrap();
    assert_eq!(inside.timers["timing/Tile-Msg"].samples, 10);
    let after = summarize(&out, Some(end), None).unwrap();
    assert_eq!(after.timers["timing/Tile-Msg"].samples, 0);
    let before = summarize(&out, None, Some(start)).unwrap();
    assert_eq!(before.timers["timing/Tile-Msg"].samples, 0);
    cleanup_shmem(base);
}

#[test]
fn run_stops_on_the_flag_and_prints_each_queue() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();
    let mut timer = Timer::new_with_base_dir(base, "testapp", "Tile-Msg");
    let recorder = Recorder::attach(base, "testapp", &base.join("run"), None, None).unwrap();
    emit(&mut timer, 3);

    let recording = recorder.run(Until::default(), &AtomicBool::new(true)).unwrap();
    assert_eq!(recording.stopped, Stop::Requested);
    assert_eq!(recording.report.timers["timing/Tile-Msg"].samples, 3);
    let table = recording.to_string();
    let row = table.lines().find(|l| l.starts_with("timing/Tile-Msg")).expect(&table);
    assert_eq!(row.split_whitespace().nth(1), Some("3"), "{table}");
    cleanup_shmem(base);
}

#[test]
fn summarize_refuses_a_record_json_of_another_format() {
    let tmp = tempdir().unwrap();
    std::fs::write(tmp.path().join("record.json"), r#"{"format": 99, "queues": []}"#).unwrap();
    let err = summarize(tmp.path(), None, None).unwrap_err();
    assert!(err.to_string().contains("format 99"), "{err}");
}

#[test]
fn records_a_tile_window_per_sample() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();
    let mut tile = TileMetrics::new(base, "testapp", "SomeTile");
    let recorder = Recorder::attach(base, "testapp", &base.join("run"), None, None).unwrap();
    assert_eq!(recorder.queue_counts(), (0, 1));
    // A tile emits one sample per 1024 loops.
    for _ in 0..2 * 1024 {
        tile.begin(IngestionTime::now());
        tile.end(true);
    }
    let summary = recorder.finish().unwrap();

    let [q] = &summary.queues[..] else { panic!("{:?}", summary.queues) };
    assert_eq!((q.kind, q.name.as_str(), q.samples, q.lost), (Kind::Tile, "SomeTile", 2, 0));
    let report = summarize(&base.join("run"), None, None).unwrap();
    let stats = &report.tiles["SomeTile"];
    assert_eq!((stats.windows, stats.lost), (2, Some(0)));
    assert!(stats.busy_ns > 0 && stats.busy_ns <= stats.window_ns, "{stats:?}");
    assert!(stats.busy_max.is_some_and(|p| p.max_ns <= stats.busy_ns), "{stats:?}");
    cleanup_shmem(base);
}

#[test]
fn an_accumulated_duration_is_placed_at_its_read() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();
    let out = base.join("run");
    let mut timer = Timer::new_with_base_dir(base, "testapp", "Tile-Msg");
    let recorder = Recorder::attach(base, "testapp", &out, None, None).unwrap();
    timer.start_accumulate();
    timer.accumulate(Duration::from(Nanos(5_000)));
    timer.emit_accumulated_processing();
    let before_read = Nanos::now().0;
    recorder.finish().unwrap();

    let at_read = summarize(&out, Some(before_read), None).unwrap();
    let timing = &at_read.timers["timing/Tile-Msg"];
    assert_eq!(timing.samples, 1);
    let dur = timing.dur.unwrap().max_ns;
    assert!((4_990..=5_010).contains(&dur), "{dur}");
    assert_eq!(
        summarize(&out, None, Some(before_read)).unwrap().timers["timing/Tile-Msg"].samples,
        0
    );
    cleanup_shmem(base);
}

#[test]
fn rotates_a_file_per_period_and_summarizes_them_as_one() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();
    let out = base.join("run");
    let mut timer = Timer::new_with_base_dir(base, "testapp", "Tile-Msg");
    let mut recorder =
        Recorder::attach(base, "testapp", &out, Some(std::time::Duration::ZERO), None).unwrap();
    emit(&mut timer, 3);
    recorder.poll().unwrap();
    emit(&mut timer, 4);
    recorder.poll().unwrap();
    let summary = recorder.finish().unwrap();

    let names: Vec<_> =
        summary.files.iter().map(|f| f.file_name().unwrap().to_str().unwrap()).collect();
    assert_eq!(names, [
        "timers-000.parquet",
        "timers-001.parquet",
        "tiles-000.parquet",
        "record.json"
    ]);
    assert_eq!(rows_in(&summary.files[0]), 3);
    assert_eq!(rows_in(&summary.files[1]), 4);
    assert_eq!(summarize(&out, None, None).unwrap().timers["timing/Tile-Msg"].samples, 7);
    cleanup_shmem(base);
}

#[test]
fn summarize_skips_and_names_a_file_a_killed_recording_left_open() {
    let tmp = tempdir().unwrap();
    let base = tmp.path();
    let out = base.join("run");
    let mut timer = Timer::new_with_base_dir(base, "testapp", "Tile-Msg");
    let mut recorder =
        Recorder::attach(base, "testapp", &out, Some(std::time::Duration::ZERO), None).unwrap();
    emit(&mut timer, 3);
    recorder.poll().unwrap();
    emit(&mut timer, 4);
    recorder.poll().unwrap();
    let summary = recorder.finish().unwrap();
    // A killed writer leaves its row groups but not the footer after them.
    let open = &summary.files[1];
    let len = std::fs::metadata(open).unwrap().len();
    File::options().write(true).open(open).unwrap().set_len(len - 8).unwrap();

    let report = summarize(&out, None, None).unwrap();
    assert_eq!(report.timers["timing/Tile-Msg"].samples, 3);
    assert_eq!(report.unfinished, ["timers-001.parquet"]);
    cleanup_shmem(base);
}
