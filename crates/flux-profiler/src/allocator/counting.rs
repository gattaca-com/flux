//! A [`GlobalAlloc`] wrapper that tallies per-thread allocated/freed bytes and
//! delegates to the real allocator. The tally is a thread-local integer add —
//! no lock, no backtrace.

use std::{
    alloc::{GlobalAlloc, Layout},
    cell::Cell,
};

use super::AllocSample;

thread_local! {
    // `const` init keeps these on the non-lazy TLS path: reading or bumping
    // them allocates nothing, so they are safe to touch from inside the global
    // allocator without reentering it.
    static ALLOCATED: Cell<u64> = const { Cell::new(0) };
    static FREED: Cell<u64> = const { Cell::new(0) };
}

#[inline]
fn on_alloc(bytes: usize) {
    ALLOCATED.with(|c| c.set(c.get().wrapping_add(bytes as u64)));
}

#[inline]
fn on_free(bytes: usize) {
    FREED.with(|c| c.set(c.get().wrapping_add(bytes as u64)));
}

#[inline]
pub(crate) fn read() -> AllocSample {
    AllocSample { allocated: ALLOCATED.with(Cell::get), freed: FREED.with(Cell::get) }
}

pub struct CountingAllocator<A>(pub A);

unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAllocator<A> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.0.alloc(layout) };
        if !ptr.is_null() {
            on_alloc(layout.size());
        }
        ptr
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.0.dealloc(ptr, layout) };
        on_free(layout.size());
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.0.alloc_zeroed(layout) };
        if !ptr.is_null() {
            on_alloc(layout.size());
        }
        ptr
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { self.0.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            // Count the old block freed and the new size allocated, so
            // `allocated - freed` tracks resident bytes across a moving realloc.
            on_free(layout.size());
            on_alloc(new_size);
        }
        new
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::{GlobalAlloc, Layout, System};

    use super::{CountingAllocator, read};

    // The test binary's global allocator is not `CountingAllocator`, so only the
    // calls through `a` below bump the counters; before/after deltas isolate them
    // from incidental allocations on the same thread.

    #[test]
    fn counts_alloc_and_free() {
        let a = CountingAllocator(System);
        let layout = Layout::from_size_align(4096, 8).unwrap();
        let before = read();
        unsafe {
            let p = a.alloc(layout);
            assert!(!p.is_null());
            a.dealloc(p, layout);
        }
        let after = read();
        assert_eq!(after.allocated - before.allocated, 4096);
        assert_eq!(after.freed - before.freed, 4096);
    }

    #[test]
    fn realloc_counts_both_legs() {
        let a = CountingAllocator(System);
        let small = Layout::from_size_align(64, 8).unwrap();
        let big = Layout::from_size_align(256, 8).unwrap();
        let before = read();
        unsafe {
            let p = a.alloc(small);
            let p = a.realloc(p, small, 256);
            assert!(!p.is_null());
            a.dealloc(p, big);
        }
        let after = read();
        // allocated: 64 (alloc) + 256 (realloc's new size); freed: 64 (realloc's
        // old block) + 256 (dealloc). `live` over the pair nets to zero.
        assert_eq!(after.allocated - before.allocated, 64 + 256);
        assert_eq!(after.freed - before.freed, 64 + 256);
    }
}
