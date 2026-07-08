//! Encode the retained `#[timed]` marks as a Fuchsia FXT trace — magic-trace's
//! native format, the one Perfetto reads with per-slice wall-clock time. The
//! init record's realtime anchor (wall-clock at tick 0) drives the UI's
//! "Absolute time".
//!
//! A string table, then per producing thread a process/thread kernel-object
//! pair and a thread record, followed by that thread's duration begin/end
//! events. See the Fuchsia Trace Format spec for the records.

use std::borrow::Cow;

use flux_timing::{Duration, IngestionTime};
use rustc_hash::FxHashMap;

use super::drainer::{FlamegraphMeta, ThreadEvents};

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

/// `Instant`'s top 2 bits hold the socket id, low 62 the rdtscp counter.
const TSC_MASK: u64 = 0x3fff_ffff_ffff_ffff;
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
) -> Vec<u8> {
    let FlamegraphMeta { names, schema } = meta;
    let mut threads: Vec<_> = threads.collect();
    threads.sort_by(|a, b| a.name.cmp(b.name).then(a.tid.cmp(&b.tid)));
    debug_assert!(threads.len() < 256, "thread ref is 8-bit");
    let now = IngestionTime::now();
    let now_tsc = now.internal().0 & TSC_MASK;
    let now_epoch_ns = now.real().0;
    let base_tsc = threads
        .iter()
        .filter_map(|t| t.marks.first().map(|m| m.ts & TSC_MASK))
        .min()
        .unwrap_or(now_tsc);
    // Wall-clock of tick 0 (the earliest mark): now minus how long ago it was.
    let anchor_ns =
        now_epoch_ns.saturating_sub(Duration(now_tsc.saturating_sub(base_tsc)).as_nanos() as u64);
    let ns = |tsc: u64| Duration((tsc & TSC_MASK).saturating_sub(base_tsc)).as_nanos() as u64;

    let mut fxt = Fxt::default();
    fxt.buf.extend_from_slice(MAGIC_NUMBER_RECORD);
    fxt.init(anchor_ns);
    let process_arg = fxt.intern("process");

    for (i, t) in threads.iter().enumerate() {
        // Each thread is its own FXT process so its counters (which the
        // Fuchsia importer can only scope to a process, never a thread) group
        // under the same collapsible node as its timer track.
        let process_koid = i as u64 + 1;
        let thread_koid = if t.tid != 0 { t.tid } else { threads.len() as u64 + i as u64 + 1 };
        let index = i as u64 + 1; // 1-based thread-table index
        let name = fxt.intern(t.name);
        fxt.kernel_object(OBJ_PROCESS, process_koid, name, None);
        fxt.kernel_object(OBJ_THREAD, thread_koid, name, Some((process_arg, process_koid)));
        fxt.thread_record(index, process_koid, thread_koid);

        let memory_track = (!t.alloc.is_empty()).then(|| {
            (fxt.intern("memory"), fxt.intern("live"), fxt.intern("allocated"), fxt.intern("freed"))
        });
        let perf_tracks: Option<Vec<_>> =
            (!t.perf.is_empty()).then(|| schema.iter().map(|e| fxt.intern(&e.label)).collect());

        for (j, mark) in t.marks.iter().enumerate() {
            let ty = if mark.is_open() { DURATION_BEGIN } else { DURATION_END };
            let raw = names.get(&mark.id).map_or("unknown", String::as_str);
            let name = fxt.intern(&untimed(raw));
            fxt.event(ty, index, name, ns(mark.ts));

            if let Some(&a) = t.alloc.get(j) &&
                let Some((track, live, allocated, freed)) = memory_track &&
                should_emit(t.alloc, j)
            {
                fxt.counter(index, track, ns(mark.ts), &[
                    (live, a.live()),
                    (allocated, a.allocated),
                    (freed, a.freed),
                ]);
            }
            if let Some(&sample) = t.perf.get(j) &&
                let Some(tracks) = &perf_tracks &&
                should_emit(t.perf, j)
            {
                for (slot, &track) in tracks.iter().enumerate() {
                    fxt.counter(index, track, ns(mark.ts), &[(track, sample.vals[slot])]);
                }
            }
        }
    }
    fxt.buf
}

fn should_emit<T: PartialEq>(samples: &[T], j: usize) -> bool {
    let changed = j == 0 || samples[j - 1] != samples[j];
    let precedes_change = samples.get(j + 1).is_some_and(|next| *next != samples[j]);
    changed || precedes_change
}

#[derive(Default)]
struct Fxt {
    buf: Vec<u8>,
    strings: FxHashMap<String, u16>,
}

impl Fxt {
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

    /// Init record (4 words): ns timebase, base tick 0, realtime at base tick.
    fn init(&mut self, anchor_ns: u64) {
        self.word(1 | (4 << 4));
        self.word(1_000_000_000); // ticks_per_second → ns
        self.word(0); // base tick
        self.word(anchor_ns); // wall-clock at the base tick
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

    use super::{COUNTER, trace};
    use crate::{
        FlamegraphMeta, Loss, ThreadEvents,
        allocator::AllocSample,
        mark::Mark,
        perf::{PerfSample, Schema},
    };

    fn names() -> FxHashMap<u64, String> {
        FxHashMap::from_iter([(7u64, "work".to_owned())])
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
        let thread = ThreadEvents { name: "t", tid: 0, marks, alloc, perf, loss: Loss::default() };
        trace([thread].into_iter(), &FlamegraphMeta { names: names(), schema })
    }

    #[test]
    fn alloc_emits_memory_counter() {
        let buf = render(&frames(), &[alloc(0), alloc(4096)], &[], Schema::empty());
        assert!(contains(&buf, b"memory") && contains(&buf, b"live"));
        assert!(count_counter_events(&buf) > 0);
    }

    #[test]
    fn perf_emits_counter_per_event() {
        let perf =
            [PerfSample::default(), PerfSample { vals: [1_000_000_000, 500, 0, 0, 0, 0, 0, 0] }];
        let buf = render(&frames(), &[], &perf, Schema::parse("instructions,cache-misses"));
        assert!(contains(&buf, b"instructions") && contains(&buf, b"cache-misses"));
        assert!(count_counter_events(&buf) > 0);
    }

    #[test]
    fn repeated_samples_emitted_once() {
        let buf = render(&frames4(), &[alloc(64); 4], &[], Schema::empty());
        assert_eq!(count_counter_events(&buf), 1);
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

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    fn count_counter_events(buf: &[u8]) -> usize {
        let mut off = 0;
        let mut n = 0;
        while off + 8 <= buf.len() {
            let word = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
            let size = ((word >> 4) & 0xfff) as usize;
            if size == 0 {
                break;
            }
            if word & 0xf == 4 && (word >> 16) & 0xf == COUNTER {
                n += 1;
            }
            off += size * 8;
        }
        n
    }

    fn counter_samples(buf: &[u8]) -> Vec<(u64, u64)> {
        let word = |off: usize| u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
        let mut off = 0;
        let mut out = Vec::new();
        while off + 8 <= buf.len() {
            let header = word(off);
            let size = ((header >> 4) & 0xfff) as usize;
            if size == 0 {
                break;
            }
            if header & 0xf == 4 && (header >> 16) & 0xf == COUNTER {
                out.push((word(off + 8), word(off + 24)));
            }
            off += size * 8;
        }
        out
    }
}
