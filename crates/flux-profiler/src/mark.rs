use flux_timing::Instant;

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct Mark {
    // id - pointer to the function name in .rodata
    pub id: u64,
    pub ts: u64,
    // High bit flags an open; the low 15 bits carry the name's byte length (opens
    // only), letting a resolver read that many bytes of the name from .rodata.
    len_and_open: u16,
}

const OPEN_BIT: u16 = 1 << 15;

/// Frame id of the synthetic span covering a ring hole. Real ids are `.rodata`
/// pointers, never null.
pub(crate) const MISSED_ID: u64 = 0;

impl Mark {
    pub(crate) fn open(name: &'static str) -> Self {
        debug_assert!(name.len() < OPEN_BIT as usize, "timed name exceeds 15-bit length");
        Self {
            id: name.as_ptr() as u64,
            ts: Instant::now().0,
            len_and_open: name.len() as u16 | OPEN_BIT,
        }
    }

    pub(crate) fn close(name: &'static str) -> Self {
        Self { id: name.as_ptr() as u64, ts: Instant::now().0, len_and_open: 0 }
    }

    pub fn is_open(&self) -> bool {
        self.len_and_open & OPEN_BIT != 0
    }

    pub(crate) fn name_len(&self) -> u16 {
        self.len_and_open & !OPEN_BIT
    }

    pub const fn from_parts(id: u64, ts: u64, open: bool) -> Self {
        Self { id, ts, len_and_open: if open { OPEN_BIT } else { 0 } }
    }
}
