use std::{alloc::{self, Layout}, array, ptr::addr_of, slice, sync::atomic::{AtomicU64, Ordering}, u64};


const MAGIC: [u8; 4] = [0xDE, 0xAD, 0xEA, 0x51];
const MAX_CONSUMERS: usize = 16;
const ALIGN: usize = size_of::<Slot>();

pub struct TCache {
    head: TCacheHead,
    len: u32,
    consumers: usize,
    data: Box<[u8]>,
}

#[derive(Debug)]
pub struct Producer {
    cache: *const TCache,
    seq: u64,
    space: u32,
}

unsafe impl Send for Producer {}
unsafe impl Sync for Producer {}

impl Producer {
    /// Return requested buffer space, if available. 
    /// If None is returned, caller should retry. 
    pub fn reserve(&mut self, len: usize) -> Option<&mut [u8]> {
        let tcache = unsafe { &*self.cache };
        match tcache.reserve(self, len as u32) {
            Some(buffer) => Some(buffer),
            None => {
                // reset available space.
                // TODO kick out slow consumers
                self.space = tcache.space(self.seq);
                None
            },
        }
    }

    pub fn commit(&mut self) {
        let tcache = unsafe { &*self.cache };
        let idx = tcache.index(self.seq);

        let slot: &mut Slot = unsafe {
            let mut_ptr = tcache.data[idx..idx + size_of::<Slot>()].as_ptr() as *mut u8;
            slice::from_raw_parts_mut(mut_ptr, size_of::<Slot>()).into()
        };

        slot.seq = AtomicU64::new(self.seq);
        self.seq += slot.reservation_len as u64;
        self.space -= slot.reservation_len;
    }

    pub fn consumer(&mut self) -> Consumer {
        let tcache = unsafe { &mut *(self.cache as *mut TCache)};
        let index = tcache.consumers;
        tcache.consumers += 1;

        // find start seq
        let seq = tcache.head.seq.load(Ordering::Acquire);
        // Push as tail 
        tcache.head.tails[index].store(seq, Ordering::Release);
        
        Consumer { cache: self.cache, index, seq }
    }
}

pub enum Error {
    NoMagic,
    WrongSeq {
        expected: u64,
        slot: u64,
    }
}

#[derive(Debug)]
pub struct Consumer {
    cache: *const TCache,
    index: usize,
    seq: u64,
}

impl Consumer {
    /// Read next data in the buffer.
    pub fn read(&mut self) -> Result<&[u8], Error> {
        let tcache = unsafe { &*self.cache };
        tcache.read(self.seq).map(|(data, inc)| {
            self.seq += inc;
            data
        })
    }

    /// Release all data read so far.
    pub fn free(&self) {
        let tcache = unsafe { &*self.cache };
        tcache.head.tails[self.index].store(self.seq, Ordering::Release);
    }
}

impl TCache {
    pub fn new(n: usize) -> Producer {
        assert!(n.is_power_of_two() && n.is_multiple_of(ALIGN));
        let layout = Layout::from_size_align(n, ALIGN).unwrap();
        let tcache = unsafe {
            let ptr = alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                alloc::handle_alloc_error(layout);
            }
            let data = Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, n));
            Box::new(Self {
                head: TCacheHead { seq: AtomicU64::new(0), tails: array::from_fn(|_| AtomicU64::new(u64::MAX)) },
                len: data.len() as u32,
                consumers: 0,
                data,
            })
        };
        let space = tcache.len;
        let producer = Producer {
            cache: Box::into_raw(tcache),
            seq: 0,
            space,
        };
        producer
    }

    fn index(&self, seq: u64) -> usize {
        (seq & (self.len - 1) as u64) as usize
    }

    fn space(&self, head_seq: u64) -> u32 {
        self.head.seq.store(head_seq, Ordering::Release);
        let min_tail = self.min_tail(head_seq);
        debug_assert!(head_seq - min_tail <= self.len as u64, "{head_seq} - {min_tail} > {}", self.len);
        self.len - ((head_seq - min_tail) as u32)
    }

    fn min_tail(&self, seq: u64) -> u64 {
        let mut min = seq;
        for tail in &self.head.tails {
            min = min.min(tail.load(Ordering::Acquire));
        }
        min
    }

    fn read(&self, seq: u64) -> Result<(&[u8], u64), Error> {
        let idx = self.index(seq);
        let slot: &Slot = (&self.data[idx..]).into();
        if slot.magic != MAGIC {
            return Err(Error::NoMagic); 
        }
        
        let slot_seq = slot.seq.load(Ordering::Acquire);
        if slot_seq != seq {
            return Err(Error::WrongSeq { expected: seq, slot: slot_seq }); 
        }
        
        Ok((&self.data[slot.data_start..slot.data_end], slot.reservation_len as u64))
    }

    fn reserve(&self, producer: &mut Producer, len: u32) -> Option<&mut [u8]> {
        let mut data_len = len as usize + size_of::<Slot>();

        let reserve_seq = producer.seq;
        let idx = self.index(reserve_seq);

        let data_offset = if idx + data_len > self.len as usize {
            // would wrap, allocate at start of buffer to return contiguous slice
            let offset =  self.len as usize - idx;
            data_len += offset;
            offset
        } else {
            size_of::<Slot>()
        };

        // Aligned so that contiguous `size_of::<Slot>()` always available
        let reserve_len = align::<ALIGN>(data_len);

        (producer.space >= reserve_len as u32).then(|| {
            let start = self.index((idx + data_offset) as u64);
            let end = start + len as usize;

            let slot: &mut Slot = unsafe {
                let mut_ptr = self.data[idx..idx + size_of::<Slot>()].as_ptr() as *mut u8;
                slice::from_raw_parts_mut(mut_ptr, size_of::<Slot>()).into()
            };

            slot.seq = AtomicU64::new(u64::MAX);
            slot.reservation_len = reserve_len as u32;
            slot.data_start = start;
            slot.data_end = end;
            slot.magic = MAGIC;

            unsafe {
                let mut_ptr = self.data[start..end].as_ptr() as *mut u8;
                slice::from_raw_parts_mut(mut_ptr, len as usize)
            }
        })
    }
}

struct TCacheHead {
    seq: AtomicU64,
    tails: [AtomicU64; MAX_CONSUMERS],
}

#[repr(C)]
struct Slot {
    seq: AtomicU64,
    data_start: usize,
    data_end: usize,
    reservation_len: u32,
    magic: [u8; 4],
}

impl Default for Slot {
    fn default() -> Self {
        Self { seq: AtomicU64::new(0), data_start: 0, data_end: 0, reservation_len: 0, magic: MAGIC }
    }
}

impl Clone for Slot {
    fn clone(&self) -> Self {
        Self { seq: AtomicU64::new(self.seq.load(Ordering::Relaxed)), data_start: self.data_start, data_end: self.data_end, reservation_len: self.reservation_len, magic: MAGIC }
    }
}

impl AsRef<[u8]> for Slot {
    fn as_ref(&self) -> &[u8] {
        let ptr = addr_of!(*self) as *const u8;
        unsafe { slice::from_raw_parts(ptr, size_of::<Slot>()) }
    }
}

impl From<&[u8]> for &Slot {
    fn from(value: &[u8]) -> Self {
        let slot = value.as_ptr() as *const Slot;
        unsafe { &*slot }
    }
}

impl From<&mut [u8]> for &mut Slot {
    fn from(value: &mut [u8]) -> Self {
        let slot = value.as_mut_ptr() as *mut Slot;
        unsafe { &mut *slot }
    }
}

#[inline]
fn align<const A: usize>(val: usize) -> usize {
    debug_assert!(A.is_power_of_two());
    let d = val & (A - 1);
    if d == 0 { val } else { val + A - d }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn produce_consume() {
        let mut producer = TCache::new(2 << 14);
        let mut consumer = producer.consumer();

        let prod = std::thread::spawn(move || {
            let mut rng = rand::rng();
            let mut total = 0;
            while total < (2 << 32) {
                if let Some(buffer) = producer.reserve(rng.random_range(512..8192)) {
                    total += buffer.len();
                    producer.commit();
                } 
                std::thread::yield_now();
            }
            total
        });

        let mut total = consumer.seq as usize;
        while total < (2 << 32) {
            let seq_was = consumer.seq;
            match consumer.read() {
                Ok(buf) => {
                    total += buf.len();
                    assert!(buf.len() > 0);
                    assert!(consumer.seq > seq_was);
                }
                _ => {}
            } 
            consumer.free();
        }

        let prod_total = prod.join().unwrap();
        assert_eq!(prod_total, total);
    }
}
