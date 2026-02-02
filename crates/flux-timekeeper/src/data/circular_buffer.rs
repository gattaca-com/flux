use serde::{Deserialize, Serialize};

/// Basically a ringbuffer to be used with data that gets streamed in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircularBuffer<T> {
    data: Vec<T>,
    mask: usize,
    // Keep track of which pos is current begin
    filled: bool,
    // Which slot to fill NEXT, i.e. id of the first element in buffer
    pos: usize,
}

impl<T: Default + Clone> CircularBuffer<T> {
    pub fn new(size: usize) -> Self {
        let realsize = size.next_power_of_two();
        Self { data: vec![T::default(); realsize], mask: realsize - 1, filled: false, pos: 0 }
    }
}
impl<T> CircularBuffer<T> {
    /// If the ringbuffer is full this returns the first element, i.e. that will
    /// be discarded
    #[inline]
    pub fn push(&mut self, v: T) -> Option<T> {
        let o = if self.filled {
            Some(std::mem::replace(unsafe { self.data.get_unchecked_mut(self.pos) }, v))
        } else {
            unsafe { *self.data.get_unchecked_mut(self.pos) = v };
            None
        };
        self.pos = (self.pos + 1) & self.mask;
        self.filled |= self.pos == 0;
        o
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, T> {
        Iter::new(self)
    }

    #[inline]
    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut::new(self)
    }

    #[inline]
    pub fn len(&self) -> usize {
        if self.filled { self.data.len() } else { self.pos }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.pos = 0;
        self.filled = false;
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn last(&self) -> Option<&T> {
        (!self.is_empty()).then(|| {
            let pos = if self.pos > 0 { self.pos - 1 } else { self.mask };
            unsafe { self.data.get_unchecked(pos) }
        })
    }

    #[inline]
    pub fn last_mut(&mut self) -> Option<&mut T> {
        (!self.is_empty()).then(|| {
            let pos = if self.pos > 0 { self.pos - 1 } else { self.mask };
            unsafe { self.data.get_unchecked_mut(pos) }
        })
    }

    #[inline]
    pub fn first(&self) -> Option<&T> {
        (!self.is_empty()).then(|| {
            let pos = if self.filled { self.pos } else { 0 };
            unsafe { self.data.get_unchecked(pos) }
        })
    }

    #[inline]
    pub fn first_mut(&mut self) -> Option<&mut T> {
        (!self.is_empty()).then(|| {
            let pos = if self.filled { self.pos } else { 0 };
            unsafe { self.data.get_unchecked_mut(pos) }
        })
    }

    fn first_pos(&self) -> usize {
        if self.filled { self.pos } else { 0 }
    }

    fn last_pos(&self) -> usize {
        if self.pos > 0 { self.pos - 1 } else { self.mask }
    }

    #[inline]
    pub fn nth_back(&self, id: usize) -> &T {
        if id == 0 {
            return self.last().unwrap();
        } else if id >= self.len() {
            panic!("out of bounds");
        }
        let last_filled_id = if self.pos == 0 { self.mask } else { self.pos - 1 };
        if last_filled_id < id {
            &self.data[self.len() - (id - last_filled_id)]
        } else {
            &self.data[last_filled_id - id]
        }
    }

    #[inline]
    pub fn nth_mut(&mut self, id: usize) -> Option<&mut T> {
        if id == 0 {
            return self.first_mut();
        } else if id >= self.len() {
            return None;
        }
        let pos = if self.filled { (self.pos + id) & self.mask } else { id };
        Some(&mut self.data[pos])
    }

    #[inline]
    pub fn nth(&mut self, id: usize) -> Option<&T> {
        if id == 0 {
            return self.first();
        } else if id >= self.len() {
            return None;
        }
        let pos = if self.filled { (self.pos + id) & self.mask } else { id };
        Some(&self.data[pos])
    }

    #[inline]
    pub fn filled(&self) -> bool {
        self.filled
    }

    pub fn n_slots(&self) -> usize {
        self.data.len()
    }
}

impl<T: Ord + Clone + Default> CircularBuffer<T> {
    #[inline]
    pub fn percentile(&mut self, p: f64) -> T {
        let l = self.len();
        if l == 0 {
            return T::default();
        }

        let mut tmp = Vec::with_capacity(l);
        for v in self.iter() {
            tmp.push(v.clone());
        }

        tmp.sort_unstable();

        let idx = ((p * l as f64).ceil() as usize).saturating_sub(1);
        tmp[idx.min(l - 1)].clone()
    }
}
#[derive(Clone, Debug)]
pub struct Iter<'a, T> {
    // This guy goes around
    pos: usize,
    count: usize,
    back: usize,
    // How many
    buffer: &'a CircularBuffer<T>,
}

impl<'a, T> Iter<'a, T> {
    fn new(buffer: &'a CircularBuffer<T>) -> Self {
        let back = buffer.last_pos();
        Self { pos: buffer.first_pos(), count: 0, buffer, back }
    }
}

#[derive(Debug)]
pub struct IterMut<'a, T> {
    pos: usize,
    count: usize,
    back: usize,
    // How many
    buffer: &'a mut CircularBuffer<T>,
}

impl<'a, T> IterMut<'a, T> {
    fn new(buffer: &'a mut CircularBuffer<T>) -> Self {
        let back = buffer.last_pos();
        Self { pos: buffer.first_pos(), count: 0, buffer, back }
    }
}

impl<'a, T: Copy> IntoIterator for &'a CircularBuffer<T> {
    type IntoIter = Iter<'a, T>;
    type Item = &'a T;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: Copy> IntoIterator for &'a mut CircularBuffer<T> {
    type IntoIter = IterMut<'a, T>;
    type Item = &'a mut T;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}
impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count == self.buffer.len() {
            return None;
        }
        let out = unsafe { self.buffer.data.get_unchecked(self.pos & self.buffer.mask) };
        self.pos = (self.pos + 1) & self.buffer.mask;
        self.count += 1;
        Some(out)
    }
}
impl<T> ExactSizeIterator for Iter<'_, T> {
    fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl<T> DoubleEndedIterator for Iter<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() || self.count == self.buffer.len() {
            None
        } else {
            let out = unsafe { self.buffer.data.get_unchecked(self.back & self.buffer.mask) };
            self.back = if self.back == 0 { self.buffer.mask } else { self.back - 1 };
            self.count += 1;
            Some(out)
        }
    }
}

impl<'a, T> Iterator for IterMut<'a, T>
where
    T: 'a,
{
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count == self.buffer.len() {
            return None;
        }

        let out = unsafe { self.buffer.data.get_unchecked_mut(self.pos & self.buffer.mask) };
        self.pos = (self.pos + 1) & self.buffer.mask;
        self.count += 1;
        unsafe { Some(&mut *(out as *mut T)) }
    }
}

impl<T> ExactSizeIterator for IterMut<'_, T> {
    fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl<T> DoubleEndedIterator for IterMut<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() || self.count == self.buffer.len() {
            None
        } else {
            let out = unsafe { self.buffer.data.get_unchecked_mut(self.back & self.buffer.mask) };
            self.back = if self.back == 0 { self.buffer.mask } else { self.back - 1 };
            self.count += 1;
            unsafe { Some(&mut *(out as *mut T)) }
        }
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test() {
        let mut buf = CircularBuffer::new(32);
        let mut tot = 0;
        for i in 0..30 {
            buf.push(i);
            tot += i;
        }

        assert_eq!(tot, buf.iter().sum::<i32>());

        let mut buf = CircularBuffer::new(32);
        let mut tot = 0;
        for i in 1..36 {
            buf.push(i);
            if i > 3 {
                tot += i;
            }
        }
        assert_eq!(tot, buf.iter().sum::<i32>());
    }

    #[test]
    fn nth_back() {
        let mut buf = CircularBuffer::new(32);
        buf.push(1u64);
        assert_eq!(*buf.nth_back(0), 1u64);
        buf.push(2u64);
        assert_eq!(*buf.nth_back(0), 2u64);
        assert_eq!(*buf.nth_back(1), 1u64);
    }

    #[test]
    fn nth_mut() {
        let mut buf = CircularBuffer::new(4);
        buf.push(1u64);
        assert_eq!(*buf.nth_mut(0).unwrap(), 1u64);
        buf.push(2u64);
        assert_eq!(*buf.nth_mut(0).unwrap(), 1u64);
        assert_eq!(*buf.nth_mut(1).unwrap(), 2u64);
        buf.push(3u64);
        buf.push(4u64);
        assert_eq!(*buf.nth_mut(2).unwrap(), 3u64);
        assert_eq!(*buf.nth_mut(3).unwrap(), 4u64);
        buf.push(5u64);
        assert_eq!(*buf.nth_mut(0).unwrap(), 2u64);
    }

    #[test]
    fn circular_iterators() {
        let mut buf = CircularBuffer::new(32);
        buf.push(1u64);

        assert_eq!(buf.iter().sum::<u64>(), 1u64);
        buf.push(2u64);
        assert_eq!(buf.iter().sum::<u64>(), 3u64);

        buf.iter_mut().for_each(|v| *v = 2);
        assert_eq!(buf.iter().sum::<u64>(), 4u64);
    }
}
