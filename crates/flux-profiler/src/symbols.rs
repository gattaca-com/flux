//! Turns a frame id back into its `#[timed]` name. An id is only an address in
//! the *producing* process, so resolution is process-relative: an in-process
//! resolver dereferences it directly, a cross-process one reads it out of
//! another process's on-disk binary.

use std::{fs, os::unix::fs::FileExt, path::PathBuf};

/// `len` is the name's exact byte length (from the `Frame::Open` fat pointer),
/// so resolution reads a fixed span and never scans for a terminator.
pub(super) trait FrameResolver {
    fn resolve(&self, id: u64, len: u16) -> Option<String>;
}

pub(super) struct InProcessSymbolsResolver;

impl FrameResolver for InProcessSymbolsResolver {
    fn resolve(&self, id: u64, len: u16) -> Option<String> {
        // SAFETY: in-process ids are `&'static str` pointers from `#[timed]`
        // names; the string lives in `.rodata` for the whole run.
        let bytes = unsafe { std::slice::from_raw_parts(id as *const u8, len as usize) };
        std::str::from_utf8(bytes).ok().map(str::to_owned)
    }
}

/// Resolves marks from another process. The id is an address over there, but
/// `#[timed]` names live in file-backed `.rodata`, so the bytes are in its
/// on-disk binary. `/proc/<pid>/maps` gives the segment→file translation
/// (covering PIE/ASLR), and reading the file needs only `PTRACE_MODE_READ`,
/// which yama leaves open to same-uid readers — unlike the `PTRACE_MODE_ATTACH`
/// that `process_vm_readv` would require.
pub(super) struct CrossProcessSymbolsResolver {
    segments: Vec<Segment>,
}

/// `path` is `/proc/<pid>/exe` for the main binary so a rebuilt-then-deleted
/// file (overlapping restart) still reads the live inode.
struct Segment {
    start: u64,
    end: u64,
    offset: u64,
    path: PathBuf,
}

impl CrossProcessSymbolsResolver {
    pub(super) fn new(pid: u32) -> Self {
        Self { segments: parse_maps(pid) }
    }
}

impl FrameResolver for CrossProcessSymbolsResolver {
    fn resolve(&self, id: u64, len: u16) -> Option<String> {
        let seg = self.segments.iter().find(|s| (s.start..s.end).contains(&id))?;
        let mut buf = vec![0u8; len as usize];
        fs::File::open(&seg.path)
            .ok()?
            .read_exact_at(&mut buf, id - seg.start + seg.offset)
            .ok()?;
        String::from_utf8(buf).ok()
    }
}

fn parse_maps(pid: u32) -> Vec<Segment> {
    let exe = format!("/proc/{pid}/exe");
    let exe_target = fs::read_link(&exe).ok().map(|p| p.to_string_lossy().into_owned());
    let exe_target = exe_target.as_deref().map(strip_deleted);

    let Ok(maps) = fs::read_to_string(format!("/proc/{pid}/maps")) else {
        return Vec::new();
    };
    maps.lines()
        .filter_map(|line| {
            let toks: Vec<&str> = line.split_whitespace().collect();
            // `range perms offset dev inode path` — anonymous maps have no path.
            if toks.len() < 6 || !toks[1].starts_with('r') {
                return None;
            }
            let path = strip_deleted(&toks[5..].join(" ")).to_owned();
            if !path.starts_with('/') {
                return None;
            }
            let (start, end) = parse_range(toks[0])?;
            let offset = u64::from_str_radix(toks[2], 16).ok()?;
            let read_path = if Some(path.as_str()) == exe_target {
                PathBuf::from(&exe)
            } else {
                PathBuf::from(path)
            };
            Some(Segment { start, end, offset, path: read_path })
        })
        .collect()
}

fn parse_range(range: &str) -> Option<(u64, u64)> {
    let (start, end) = range.split_once('-')?;
    Some((u64::from_str_radix(start, 16).ok()?, u64::from_str_radix(end, 16).ok()?))
}

fn strip_deleted(path: &str) -> &str {
    path.strip_suffix(" (deleted)").unwrap_or(path)
}
