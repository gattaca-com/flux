//! Encode the retained `#[timed]` marks as a Fuchsia FXT trace — magic-trace's
//! native format, the one Perfetto reads with per-slice wall-clock time.
//!
//! Timestamps are absolute wall-clock ns and koids come from ids the drainer
//! holds for its whole life, so consecutive dumps of one reader share a
//! timeline and a set of tracks: concatenating their files is a valid trace.
//!
//! A string table, then per producing thread a process/thread kernel-object
//! pair and a thread record, followed by that thread's duration begin/end
//! events. See the Fuchsia Trace Format spec for the records.

use std::{borrow::Cow, io};

use rustc_hash::FxHashMap;

use super::{
    drainer::{FlamegraphMeta, ThreadEvents},
    socket_clock::SocketClocks,
};

/// Strip the `__TimedTy` method-marker plumbing from a resolved frame name,
/// keeping the full path verbatim.
#[allow(clippy::option_if_let_else)] // the match reads clearer than map_or here
fn untimed(qualified: &str) -> Cow<'_, str> {
    const PLUMBING: &str = "::__TimedTy"; // sits in front of the `<Receiver>`
    match qualified.find(PLUMBING) {
        None => Cow::Borrowed(qualified),
        Some(at) => Cow::Owned([&qualified[..at], &qualified[at + PLUMBING.len()..]].concat()),
    }
}

/// Perfetto sniffs these 8 bytes to pick its Fuchsia importer; `FxT` sits
/// between the record's fixed framing bytes.
const MAGIC_NUMBER_RECORD: &[u8] = b"\x10\x00\x04FxT\x16\x00";
const OBJ_PROCESS: u64 = 1; // zx_obj_type PROCESS
const OBJ_THREAD: u64 = 2; // zx_obj_type THREAD
const COUNTER: u64 = 1; // fuchsia event type Counter — Perfetto draws it as a line graph
const DURATION_BEGIN: u64 = 2;
const DURATION_END: u64 = 3;
const ARG_UINT64: u64 = 4;
const ARG_KOID: u64 = 8;

pub(super) fn trace<'a>(
    threads: impl Iterator<Item = ThreadEvents<'a>>,
    meta: &FlamegraphMeta,
    clocks: &SocketClocks,
) -> Vec<u8> {
    let mut buf = Vec::new();
    write(threads, meta, clocks, &mut buf).expect("a Vec sink never fails");
    buf
}

pub(super) fn write<'a>(
    threads: impl Iterator<Item = ThreadEvents<'a>>,
    meta: &FlamegraphMeta,
    clocks: &SocketClocks,
    out: impl io::Write,
) -> io::Result<()> {
    let FlamegraphMeta { names, schema } = meta;
    let mut threads: Vec<_> = threads.collect();
    threads.sort_by(|a, b| a.name.cmp(b.name).then(a.tid.cmp(&b.tid)));
    debug_assert!(threads.len() < 256, "thread ref is 8-bit");

    let mut fxt = Fxt::new(out);
    fxt.buf.extend_from_slice(MAGIC_NUMBER_RECORD);
    fxt.init();
    let process_arg = fxt.intern("process");

    for (i, t) in threads.iter().enumerate() {
        // Each thread is its own FXT process so its counters (which the
        // Fuchsia importer can only scope to a process, never a thread) group
        // under the same collapsible node as its timer track. Both koids come
        // from the thread's own id rather than its position here, which a
        // thread idle over one dump would shift.
        let process_koid = t.id;
        let thread_koid = if t.tid != 0 { t.tid } else { t.id };
        let index = i as u64 + 1; // 1-based thread-table index
        let name = fxt.intern(t.name);
        fxt.kernel_object(OBJ_PROCESS, process_koid, name, None);
        fxt.kernel_object(OBJ_THREAD, thread_koid, name, Some((process_arg, process_koid)));
        fxt.thread_record(index, process_koid, thread_koid);

        let memory_track = (!t.alloc.is_empty()).then(|| {
            (fxt.intern("memory"), fxt.intern("live"), fxt.intern("allocated"), fxt.intern("freed"))
        });
        let perf_tracks: Option<(u16, Vec<u16>)> = (!t.perf.is_empty())
            .then(|| (fxt.intern("perf"), schema.iter().map(|e| fxt.intern(&e.label)).collect()));

        for (j, mark) in t.marks.iter().enumerate() {
            let ty = if mark.is_open() { DURATION_BEGIN } else { DURATION_END };
            let name = fxt.intern_frame(mark.id, names);
            let ts = clocks.resolve_ns(mark.ts).max(t.last_written_ns);
            fxt.event(ty, index, name, ts);

            if let Some(&a) = t.alloc.get(j) &&
                let Some((track, live, allocated, freed)) = memory_track &&
                should_emit(t.alloc, j)
            {
                fxt.counter(index, track, ts, &[
                    (live, a.live()),
                    (allocated, a.allocated),
                    (freed, a.freed),
                ]);
            }
            if let Some(&sample) = t.perf.get(j) &&
                let Some((track, labels)) = &perf_tracks &&
                should_emit(t.perf, j)
            {
                let mut args = sample.vals.map(|value| (0u16, value));
                for (arg, &label) in args.iter_mut().zip(labels) {
                    arg.0 = label;
                }
                fxt.counter(index, *track, ts, &args[..labels.len()]);
            }
            if fxt.buf.len() >= FLUSH_BYTES {
                fxt.flush()?;
            }
        }
    }
    fxt.flush()
}

fn should_emit<T: PartialEq>(samples: &[T], j: usize) -> bool {
    let changed = j == 0 || samples[j - 1] != samples[j];
    let precedes_change = samples.get(j + 1).is_some_and(|next| *next != samples[j]);
    changed || precedes_change
}

const FLUSH_BYTES: usize = 64 << 10;

struct Fxt<W: io::Write> {
    out: W,
    buf: Vec<u8>,
    strings: FxHashMap<String, u16>,
    frames: FxHashMap<u64, u16>,
}

impl<W: io::Write> Fxt<W> {
    fn new(out: W) -> Self {
        Self { out, buf: Vec::new(), strings: FxHashMap::default(), frames: FxHashMap::default() }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.out.write_all(&self.buf)?;
        self.buf.clear();
        Ok(())
    }

    fn word(&mut self, w: u64) {
        self.buf.extend_from_slice(&w.to_le_bytes());
    }

    fn string_bytes(&mut self, s: &[u8]) {
        self.buf.extend_from_slice(s);
        self.buf.resize(self.buf.len() + (8 - s.len() % 8) % 8, 0);
    }

    /// Index of `s` in the string table, emitting its record on first use so it
    /// precedes any reference. Indices are 1-based (0 means the empty string).
    fn intern(&mut self, s: &str) -> u16 {
        if let Some(&i) = self.strings.get(s) {
            return i;
        }
        let index = self.strings.len() as u16 + 1;
        debug_assert!(index < 0x8000, "string ref is 15-bit");
        self.strings.insert(s.to_owned(), index);
        let size = 1 + s.len().div_ceil(8) as u64;
        // type 2, index [16:31), length [32:47).
        self.word(2 | (size << 4) | (u64::from(index) << 16) | ((s.len() as u64) << 32));
        self.string_bytes(s.as_bytes());
        index
    }

    fn intern_frame(&mut self, id: u64, names: &FxHashMap<u64, String>) -> u16 {
        if let Some(&index) = self.frames.get(&id) {
            return index;
        }
        let raw = names.get(&id).map_or("unknown", String::as_str);
        let index = self.intern(&untimed(raw));
        self.frames.insert(id, index);
        index
    }

    /// Init record: the tick rate the event timestamps are already in.
    fn init(&mut self) {
        self.word(1 | (2 << 4));
        self.word(1_000_000_000); // ticks_per_second → ns
    }

    fn kernel_object(&mut self, obj_type: u64, koid: u64, name: u16, process: Option<(u16, u64)>) {
        let size = 2 + 2 * process.is_some() as u64; // header, koid, optional koid arg
        // type 7, obj type [16:24), name ref [24:40), arg count [40:44).
        self.word(
            7 | (size << 4) |
                (obj_type << 16) |
                (u64::from(name) << 24) |
                ((process.is_some() as u64) << 40),
        );
        self.word(koid);
        if let Some((arg_name, koid_val)) = process {
            // koid argument: type 8, size 2, name ref [16:32), then the koid.
            self.word(ARG_KOID | (2 << 4) | (u64::from(arg_name) << 16));
            self.word(koid_val);
        }
    }

    fn thread_record(&mut self, index: u64, process_koid: u64, thread_koid: u64) {
        self.word(3 | (3 << 4) | (index << 16)); // type 3, thread index [16:24)
        self.word(process_koid);
        self.word(thread_koid);
    }

    fn event(&mut self, event_type: u64, thread_index: u64, name: u16, ts: u64) {
        // type 4, event type [16:20), thread ref [24:32), empty category, name
        // ref [48:64); header + timestamp, both refs indexed so no inline data.
        self.word(
            4 | (2 << 4) | (event_type << 16) | (thread_index << 24) | (u64::from(name) << 48),
        );
        self.word(ts);
    }

    /// Counter event: header + timestamp, a uint64 argument per series, then
    /// the trailing counter id. Perfetto keys the (process-scoped) track by
    /// `(name, counter_id)` and plots each argument as a line, labelling it
    /// `name:arg:counter_id`.
    fn counter(&mut self, thread_index: u64, name: u16, ts: u64, args: &[(u16, u64)]) {
        let n_args = args.len() as u64;
        let size = 2 + 2 * n_args + 1; // header + ts, two words per arg, counter id
        self.word(
            4 | (size << 4) |
                (COUNTER << 16) |
                (n_args << 20) |
                (thread_index << 24) |
                (u64::from(name) << 48),
        );
        self.word(ts);
        for &(arg_name, val) in args {
            // uint64 argument: type 4, size 2 words, name ref [16:32), then value.
            self.word(ARG_UINT64 | (2 << 4) | (u64::from(arg_name) << 16));
            self.word(val);
        }
        self.word(0); // counter id
    }
}

#[cfg(test)]
mod tests {
    use rustc_hash::FxHashMap;

    use super::{COUNTER, DURATION_BEGIN, DURATION_END, MAGIC_NUMBER_RECORD, OBJ_PROCESS, trace};
    use crate::{
        FlamegraphMeta, Loss, ThreadEvents,
        allocator::AllocSample,
        mark::Mark,
        perf::{PerfSample, Schema},
        socket_clock::SocketClocks,
    };

    fn names() -> FxHashMap<u64, String> {
        FxHashMap::from_iter([(7u64, "work".to_owned()), (8, "other".to_owned())])
    }

    fn frames() -> [Mark; 2] {
        [Mark::from_parts(7, 0, true), Mark::from_parts(7, 100, false)]
    }

    fn frames4() -> [Mark; 4] {
        [
            Mark::from_parts(7, 0, true),
            Mark::from_parts(7, 10, true),
            Mark::from_parts(7, 20, false),
            Mark::from_parts(7, 30, false),
        ]
    }

    fn alloc(n: u64) -> AllocSample {
        AllocSample { allocated: n, freed: 0 }
    }

    fn render(
        marks: &[Mark],
        alloc: &[AllocSample],
        perf: &[PerfSample],
        schema: Schema,
    ) -> Vec<u8> {
        let thread = ThreadEvents {
            name: "t",
            tid: 0,
            id: 1,
            marks,
            alloc,
            perf,
            loss: Loss::default(),
            last_written_ns: 0,
        };
        trace(
            [thread].into_iter(),
            &FlamegraphMeta { names: names(), schema },
            &SocketClocks::identity(),
        )
    }

    /// A new clock sample can move time back a little. Each thread keeps its
    /// own last written time, so a busy thread cannot drag a quiet one forward.
    #[test]
    fn marks_never_go_before_last_written() {
        let last_written_ns = 5_000;
        let quiet = ThreadEvents {
            name: "quiet",
            tid: 0,
            id: 1,
            marks: &frames(),
            alloc: &[],
            perf: &[],
            loss: Loss::default(),
            last_written_ns,
        };
        // A busy thread far ahead in time must not drag the quiet one forward.
        let busy =
            ThreadEvents { name: "busy", tid: 0, id: 2, last_written_ns: 9_000_000, ..quiet };
        let buf = trace(
            [quiet, busy].into_iter(),
            &FlamegraphMeta { names: names(), schema: Schema::empty() },
            &SocketClocks::identity(),
        );

        let ts = event_timestamps(&buf);
        assert!(!ts.is_empty());
        let quiet_range = last_written_ns..9_000_000;
        assert!(ts.iter().any(|&t| quiet_range.contains(&t)), "{ts:?} all dragged forward");
        assert!(ts.iter().all(|&t| t >= last_written_ns), "{ts:?} went before the last written");
    }

    #[test]
    fn alloc_emits_memory_counter() {
        let buf = render(&frames(), &[alloc(0), alloc(4096)], &[], Schema::empty());
        assert!(contains(&buf, b"memory") && contains(&buf, b"live"));
        assert!(!counter_samples(&buf).is_empty());
    }

    #[test]
    fn perf_emits_counter_per_event() {
        let perf =
            [PerfSample::default(), PerfSample { vals: [1_000_000_000, 500, 0, 0, 0, 0, 0, 0] }];
        let buf = render(&frames(), &[], &perf, Schema::parse("instructions,cache-misses"));
        assert!(contains(&buf, b"instructions") && contains(&buf, b"cache-misses"));
        assert!(!counter_samples(&buf).is_empty());
    }

    #[test]
    fn repeated_samples_emitted_once() {
        let buf = render(&frames4(), &[alloc(64); 4], &[], Schema::empty());
        assert_eq!(counter_samples(&buf).len(), 1);
    }

    #[test]
    fn keeps_sample_before_change() {
        let allocs = [alloc(0), alloc(0), alloc(0), alloc(100)];
        let samples = counter_samples(&render(&frames4(), &allocs, &[], Schema::empty()));

        let values: Vec<_> = samples.iter().map(|&(_ts, value)| value).collect();
        let times: Vec<_> = samples.iter().map(|&(ts, _value)| ts).collect();
        assert_eq!(values, [0, 0, 100]);
        assert!(times[0] < times[1] && times[1] < times[2], "re-anchor sits between the edges");
    }

    #[test]
    fn no_counter_without_samples() {
        let buf = render(&frames(), &[], &[], Schema::empty());
        assert!(!contains(&buf, b"memory") && !contains(&buf, b"live"));
    }

    /// What lets a run's dumps concatenate: a thread is keyed by its own id and
    /// timed by the clock, never by where it fell in this dump.
    #[test]
    fn koids_come_from_thread_ids_and_timestamps_are_absolute() {
        let marks = [Mark::from_parts(7, 5_000, true), Mark::from_parts(7, 9_000, false)];
        let thread = |name, id| ThreadEvents {
            name,
            tid: 0,
            id,
            marks: &marks,
            alloc: &[],
            perf: &[],
            last_written_ns: 0,
            loss: Loss::default(),
        };
        let buf = trace(
            [thread("b", 4), thread("a", 7)].into_iter(),
            &FlamegraphMeta { names: names(), schema: Schema::empty() },
            &SocketClocks::identity(),
        );

        assert_eq!(process_koids(&buf), [7, 4], "sorted a before b, each keyed by its own id");
        assert!(
            event_timestamps(&buf).iter().all(|&ts| ts > 0),
            "the dump's earliest mark is not rebased to zero"
        );
    }

    /// Merging a run's dumps is concatenating their bytes, so every dump has to
    /// stand alone: a reader that resolves each one against only the records it
    /// emitted itself must still see the right names — and see the thread the
    /// dumps share stay on one track.
    #[test]
    fn dumps_concatenate_into_one_trace() {
        let dump = |marks: &[Mark]| render(marks, &[], &[], Schema::empty());
        let frame =
            |id, at| [Mark::from_parts(id, at, true), Mark::from_parts(id, at + 10_000, false)];

        let mut merged = dump(&frame(7, 0));
        merged.extend_from_slice(&dump(&[frame(7, 100_000), frame(8, 200_000)].concat()));

        assert_eq!(framed_len(&merged), merged.len(), "every record frames the next one");
        let dumps = segments(&merged);
        assert_eq!(dumps.len(), 2, "each dump carries its own magic record");
        assert_eq!(frames_of(&dumps[0]), ["work", "work"]);
        assert_eq!(
            frames_of(&dumps[1]),
            ["work", "work", "other", "other"],
            "a dump re-interns even a name an earlier dump already emitted"
        );
        assert_eq!(dumps[0].koids, dumps[1].koids, "the shared thread keeps one track");
        assert!(
            first_ts(&dumps[1]) > last_ts(&dumps[0]),
            "the later dump continues the timeline instead of restarting it"
        );
    }

    /// One dump, decoded against nothing but its own records.
    #[derive(Default)]
    struct Segment {
        /// Duration events as (resolved name, timestamp).
        events: Vec<(String, u64)>,
        /// Thread records as (process koid, thread koid).
        koids: Vec<(u64, u64)>,
    }

    /// Split a trace at its magic records and decode each dump in isolation:
    /// the string table resets at every boundary, so a reference to a name
    /// another dump interned resolves to `<unresolved>` instead of silently
    /// reading the wrong dump's table.
    fn segments(buf: &[u8]) -> Vec<Segment> {
        let mut dumps: Vec<Segment> = Vec::new();
        let mut strings: FxHashMap<u16, String> = FxHashMap::default();
        for (header, off) in records(buf) {
            if &buf[off..off + 8] == MAGIC_NUMBER_RECORD {
                strings.clear();
                dumps.push(Segment::default());
                continue;
            }
            let Some(dump) = dumps.last_mut() else { continue };
            match header & 0xf {
                2 => {
                    let len = ((header >> 32) & 0x7fff) as usize;
                    let name = String::from_utf8_lossy(&buf[off + 8..off + 8 + len]);
                    strings.insert((header >> 16) as u16, name.into_owned());
                }
                3 => dump.koids.push((word(buf, off + 8), word(buf, off + 16))),
                4 if matches!((header >> 16) & 0xf, DURATION_BEGIN | DURATION_END) => {
                    let name = strings.get(&((header >> 48) as u16));
                    let name = name.map_or("<unresolved>", String::as_str);
                    dump.events.push((name.to_owned(), word(buf, off + 8)));
                }
                _ => {}
            }
        }
        dumps
    }

    fn frames_of(dump: &Segment) -> Vec<&str> {
        dump.events.iter().map(|(name, _)| name.as_str()).collect()
    }

    fn first_ts(dump: &Segment) -> u64 {
        dump.events.first().expect("the dump has events").1
    }

    fn last_ts(dump: &Segment) -> u64 {
        dump.events.last().expect("the dump has events").1
    }

    /// Where the last record the framing reaches ends — `buf.len()` only when
    /// every record's size field lands exactly on the next record's header.
    fn framed_len(buf: &[u8]) -> usize {
        records(buf).last().map_or(0, |(header, off)| off + ((header >> 4) & 0xfff) as usize * 8)
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    fn process_koids(buf: &[u8]) -> Vec<u64> {
        records(buf)
            .filter(|&(header, _)| header & 0xf == 7 && (header >> 16) & 0xff == OBJ_PROCESS)
            .map(|(_, off)| word(buf, off + 8))
            .collect()
    }

    fn event_timestamps(buf: &[u8]) -> Vec<u64> {
        records(buf)
            .filter(|&(header, _)| header & 0xf == 4)
            .map(|(_, off)| word(buf, off + 8))
            .collect()
    }

    fn word(buf: &[u8], off: usize) -> u64 {
        u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
    }

    /// Every record as (header, offset), walked by the header's size field.
    fn records(buf: &[u8]) -> impl Iterator<Item = (u64, usize)> {
        let mut off = 0;
        std::iter::from_fn(move || {
            let header = (off + 8 <= buf.len()).then(|| word(buf, off))?;
            let size = ((header >> 4) & 0xfff) as usize;
            (size > 0).then(|| {
                let at = off;
                off += size * 8;
                (header, at)
            })
        })
    }

    /// Every counter event as (timestamp, first argument value).
    fn counter_samples(buf: &[u8]) -> Vec<(u64, u64)> {
        records(buf)
            .filter(|&(header, _)| header & 0xf == 4 && (header >> 16) & 0xf == COUNTER)
            .map(|(_, off)| (word(buf, off + 8), word(buf, off + 24)))
            .collect()
    }
}
