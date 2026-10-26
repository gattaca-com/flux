use std::{
    collections::VecDeque,
    ops::{Deref, DerefMut, RangeBounds},
    vec,
};

/// How many lengths a [`Default`] buffer remembers: ten slots when it is
/// cleared once per slot.
pub const DEFAULT_HISTORY: usize = 10;

/// A reused buffer that gives back a burst's allocation.
///
/// Each [`clear`](Self::clear) records the length it cleared. Once capacity
/// exceeds twice the upper median of the last `history` lengths, it shrinks to
/// that median; the slack stops a buffer that swings around its median from
/// reallocating on every fill.
///
/// Only `clear` records a length. The slice is exposed mutably but the `Vec`
/// is not, so refills go through `clear`; `truncate` and `drain` trim within a
/// fill.
#[derive(Clone, Debug)]
pub struct TruncatingVec<T> {
    vec: Vec<T>,
    lens: VecDeque<usize>,
    history: usize,
}

impl<T> TruncatingVec<T> {
    pub fn new(history: usize) -> Self {
        assert!(history > 0, "TruncatingVec history must be non-zero");
        Self { vec: Vec::new(), lens: VecDeque::new(), history }
    }

    pub fn clear(&mut self) {
        if self.lens.len() == self.history {
            self.lens.pop_front();
        }
        self.lens.push_back(self.vec.len());
        self.vec.clear();
        // Counting instead of sorting a copy needs no scratch buffer.
        let mid = self.lens.len() / 2;
        let median = self
            .lens
            .iter()
            .copied()
            .find(|&len| {
                let below = self.lens.iter().filter(|&&l| l < len).count();
                below <= mid && mid < below + self.lens.iter().filter(|&&l| l == len).count()
            })
            .expect("a length was just recorded");
        if self.vec.capacity() > 2 * median {
            self.vec.shrink_to(median);
        }
    }

    #[inline]
    pub fn push(&mut self, value: T) {
        self.vec.push(value);
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.vec.reserve(additional);
    }

    #[inline]
    pub fn resize(&mut self, new_len: usize, value: T)
    where
        T: Clone,
    {
        self.vec.resize(new_len, value);
    }

    #[inline]
    pub fn truncate(&mut self, len: usize) {
        self.vec.truncate(len);
    }

    #[inline]
    pub fn drain<R: RangeBounds<usize>>(&mut self, range: R) -> vec::Drain<'_, T> {
        self.vec.drain(range)
    }

    pub fn shrink_to_fit(&mut self) {
        self.vec.shrink_to_fit();
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.vec.capacity()
    }
}

impl<T> Default for TruncatingVec<T> {
    fn default() -> Self {
        Self::new(DEFAULT_HISTORY)
    }
}

impl<T> Deref for TruncatingVec<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        &self.vec
    }
}

impl<T> DerefMut for TruncatingVec<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.vec
    }
}

impl<T> Extend<T> for TruncatingVec<T> {
    #[inline]
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.vec.extend(iter);
    }
}

impl<'a, T: Copy + 'a> Extend<&'a T> for TruncatingVec<T> {
    #[inline]
    fn extend<I: IntoIterator<Item = &'a T>>(&mut self, iter: I) {
        self.vec.extend(iter);
    }
}

impl<'a, T> IntoIterator for &'a TruncatingVec<T> {
    type IntoIter = std::slice::Iter<'a, T>;
    type Item = &'a T;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.vec.iter()
    }
}
