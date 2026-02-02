use core::{
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
};
use std::ops::{Index, IndexMut, RangeFull};

/// Creates an [`ArrayVec`] with the given elements.
///
/// `array_vec![a, b, c]` creates an `ArrayVec` containing `a`, `b`, `c`.
/// `array_vec![x; N]` creates an `ArrayVec` with `N` copies of `x`.
#[macro_export]
macro_rules! array_vec {
    () => {
        $crate::ArrayVec::new()
    };
    ($elem:expr; $n:expr) => {{
        let mut v = $crate::ArrayVec::new();
        v.resize($n, $elem);
        v
    }};
    ($($elem:expr),+ $(,)?) => {{
        let mut v = $crate::ArrayVec::new();
        $(v.push($elem);)+
        v
    }};
}

/// A fixed-capacity, stack-allocated vector whose elements are
/// `Copy`, and which is itself `Copy`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ArrayVec<T: Copy, const N: usize> {
    len: usize,
    data: [MaybeUninit<T>; N],
}

const MAX_SIZE: usize = u16::MAX as usize;

impl<T: Copy, const N: usize> ArrayVec<T, N> {
    #[inline(always)]
    pub const fn new() -> Self {
        assert!(N < MAX_SIZE);
        Self { len: 0, data: [MaybeUninit::uninit(); N] }
    }

    #[inline(always)]
    pub const fn capacity(&self) -> usize {
        N
    }

    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub const fn is_full(&self) -> bool {
        self.len() == N
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        // No drop needed because `T: Copy`
        self.len = 0;
    }

    /// Panics if full.
    #[inline(always)]
    pub fn push(&mut self, value: T) {
        let len = self.len();
        assert!(len < N, "push capacity overflow");
        unsafe {
            self.data.get_unchecked_mut(len).write(value);
        }
        self.len = len + 1;
    }

    /// Tries to place an element onto the end of the vec.
    /// Returns back the element if the capacity is exhausted,
    /// otherwise returns None.
    #[inline]
    pub fn try_push(&mut self, value: T) -> Option<T> {
        let len = self.len();
        if len == N {
            return Some(value);
        }
        unsafe {
            self.data.get_unchecked_mut(len).write(value);
        }
        self.len = len + 1;
        None
    }

    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        let len = self.len();
        if len == 0 {
            return None;
        }
        let new_len = len - 1;
        self.len = new_len;
        Some(unsafe { self.data.get_unchecked(new_len).assume_init_read() })
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len() {
            return None;
        }
        Some(unsafe { self.data.get_unchecked(index).assume_init_ref() })
    }

    #[inline]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        if index >= self.len() {
            return None;
        }
        Some(unsafe { self.data.get_unchecked_mut(index).assume_init_mut() })
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        let len = self.len();
        unsafe { core::slice::from_raw_parts(self.data.as_ptr().cast::<T>(), len) }
    }

    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        let len = self.len();
        unsafe { core::slice::from_raw_parts_mut(self.data.as_mut_ptr().cast::<T>(), len) }
    }

    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.as_slice().iter()
    }

    #[inline]
    pub fn resize_with<F: FnMut() -> T>(&mut self, new_len: usize, mut func: F) {
        match new_len.checked_sub(self.len) {
            None => self.truncate(new_len),
            Some(new_elements) => {
                for _ in 0..new_elements {
                    self.push(func());
                }
            }
        }
    }

    #[inline]
    pub fn resize(&mut self, new_len: usize, value: T) {
        self.resize_with(new_len, || value);
    }

    #[inline]
    pub fn truncate(&mut self, new_len: usize) {
        if new_len >= self.len {
            return;
        }
        self.len = new_len;
    }
}

impl<T: Copy, const N: usize> Default for ArrayVec<T, N> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Copy, const N: usize> Deref for ArrayVec<T, N> {
    type Target = [T];
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T: Copy, const N: usize> DerefMut for ArrayVec<T, N> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<T: Copy, const N: usize> core::fmt::Debug for ArrayVec<T, N>
where
    T: core::fmt::Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_list().entries(self.as_slice().iter()).finish()
    }
}

impl<T: Copy, const N: usize> From<[T; N]> for ArrayVec<T, N> {
    #[inline]
    fn from(arr: [T; N]) -> Self {
        let data = arr.map(MaybeUninit::new);

        Self { len: N, data }
    }
}

impl<T: Copy, const N: usize> Index<usize> for ArrayVec<T, N> {
    type Output = T;

    #[inline(always)]
    fn index(&self, idx: usize) -> &Self::Output {
        assert!(idx < self.len());
        unsafe { self.data.get_unchecked(idx).assume_init_ref() }
    }
}

impl<T: Copy, const N: usize> Index<RangeFull> for ArrayVec<T, N> {
    type Output = [T];

    #[inline]
    fn index(&self, _: RangeFull) -> &Self::Output {
        self.as_slice()
    }
}

impl<T: Copy, const N: usize> IndexMut<usize> for ArrayVec<T, N> {
    #[inline(always)]
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        assert!(idx < self.len());
        unsafe { self.data.get_unchecked_mut(idx).assume_init_mut() }
    }
}

impl<T: Copy, const N: usize> IndexMut<RangeFull> for ArrayVec<T, N> {
    #[inline]
    fn index_mut(&mut self, _: RangeFull) -> &mut Self::Output {
        self.as_mut_slice()
    }
}

impl<T: Copy + PartialEq, const N: usize> PartialEq for ArrayVec<T, N> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: Copy + Eq, const N: usize> Eq for ArrayVec<T, N> {}

impl<'a, T: Copy, const N: usize> IntoIterator for &'a ArrayVec<T, N> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: Copy, const N: usize> IntoIterator for &'a mut ArrayVec<T, N> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

pub struct IntoIter<T: Copy, const N: usize> {
    vec: ArrayVec<T, N>,
    idx: usize,
}

impl<T: Copy, const N: usize> Iterator for IntoIter<T, N> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.vec.len {
            return None;
        }
        let item = unsafe { self.vec.data.get_unchecked(self.idx).assume_init_read() };
        self.idx += 1;
        Some(item)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.vec.len - self.idx;
        (rem, Some(rem))
    }
}

impl<T: Copy, const N: usize> IntoIterator for ArrayVec<T, N> {
    type Item = T;
    type IntoIter = IntoIter<T, N>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        IntoIter { vec: self, idx: 0 }
    }
}

impl<T: Copy, const N: usize> Extend<T> for ArrayVec<T, N> {
    #[inline]
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for t in iter {
            self.push(t)
        }
    }
}

impl<T: Copy, const N: usize> FromIterator<T> for ArrayVec<T, N> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut a = ArrayVec::new();
        for t in iter {
            a.push(t)
        }
        a
    }
}

/// Fixed-capacity UTF-8 string.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(C)]
pub struct ArrayStr<const N: usize> {
    buf: ArrayVec<u8, N>,
}

impl<const N: usize> ArrayStr<N> {
    #[inline]
    pub const fn new() -> Self {
        Self { buf: ArrayVec::new() }
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        // Safety: we only ever write valid UTF-8 into buf.
        unsafe { core::str::from_utf8_unchecked(self.buf.as_slice()) }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    #[inline]
    pub const fn is_full(&self) -> bool {
        self.buf.is_full()
    }

    /// Push a single ASCII byte. Panics if full or non-ASCII.
    #[inline]
    pub(crate) fn push_byte(&mut self, b: u8) {
        debug_assert!(b.is_ascii());
        self.buf.push(b);
    }

    /// Push str, truncating at char boundary if exceeds capacity.
    #[inline]
    pub fn push_str_truncate(&mut self, s: &str) {
        let avail = N - self.buf.len();
        let mut take = s.len().min(avail);
        // Truncate at char boundary to preserve UTF-8 validity.
        while take > 0 && !s.is_char_boundary(take) {
            take -= 1;
        }
        for &b in &s.as_bytes()[..take] {
            self.buf.push(b);
        }
    }

    /// Create from str, truncating at char boundary if exceeds capacity.
    #[inline]
    pub fn from_str_truncate(s: &str) -> Self {
        let mut out = Self::new();
        out.push_str_truncate(s);
        out
    }
}

/// Error returned when string exceeds ArrayStr capacity.
#[derive(Debug, Clone, Copy)]
pub struct ArrayStrTooLong {
    pub len: usize,
    pub capacity: usize,
}

impl core::fmt::Display for ArrayStrTooLong {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "string length {} exceeds capacity {}; use from_str_truncate() to truncate",
            self.len, self.capacity
        )
    }
}

impl std::error::Error for ArrayStrTooLong {}

impl<const N: usize> TryFrom<&str> for ArrayStr<N> {
    type Error = ArrayStrTooLong;

    #[inline]
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.len() > N {
            return Err(ArrayStrTooLong { len: s.len(), capacity: N });
        }
        Ok(Self::from_str_truncate(s))
    }
}

impl<const N: usize> Deref for ArrayStr<N> {
    type Target = str;
    #[inline]
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl<const N: usize> AsRef<str> for ArrayStr<N> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<const N: usize> core::fmt::Display for ArrayStr<N> {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<const N: usize> core::fmt::Debug for ArrayStr<N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "\"{}\"", self.as_str())
    }
}

mod serde_impl {
    use core::fmt;

    use serde::{
        Deserialize, Deserializer, Serialize, Serializer,
        de::{Error as DeError, SeqAccess, Visitor},
    };

    use super::ArrayVec;

    impl<T, const N: usize> Serialize for ArrayVec<T, N>
    where
        T: Copy + Serialize,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.as_slice().serialize(serializer)
        }
    }

    impl<'de, T, const N: usize> Deserialize<'de> for ArrayVec<T, N>
    where
        T: Copy + Deserialize<'de>,
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct ArrayVecVisitor<T, const N: usize>(core::marker::PhantomData<T>);

            impl<'de, T, const N: usize> Visitor<'de> for ArrayVecVisitor<T, N>
            where
                T: Copy + Deserialize<'de>,
            {
                type Value = ArrayVec<T, N>;

                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    write!(f, "a sequence with at most {} elements", N)
                }

                fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where
                    A: SeqAccess<'de>,
                {
                    let mut out = ArrayVec::<T, N>::new();
                    while let Some(elem) = seq.next_element::<T>()? {
                        if out.try_push(elem).is_some() {
                            return Err(A::Error::custom("ArrayVec capacity exceeded"));
                        }
                    }
                    Ok(out)
                }
            }

            deserializer.deserialize_seq(ArrayVecVisitor::<T, N>(core::marker::PhantomData))
        }
    }
}

#[cfg(feature = "wincode")]
mod wincode_impl {
    use std::mem::{self, MaybeUninit};

    use wincode::{TypeMeta, io::Writer, len::SeqLen};

    use super::ArrayVec;
    type BatchWireLen = wincode::len::BincodeLen;

    fn is_pod<T>() -> bool
    where
        T: wincode::SchemaWrite<Src = T>,
    {
        matches!(T::TYPE_META, TypeMeta::Static { size: _, zero_copy: true })
    }

    impl<T: Copy, const N: usize> wincode::SchemaWrite for ArrayVec<T, N>
    where
        T: wincode::SchemaWrite<Src = T>,
    {
        type Src = Self;

        #[inline]
        fn size_of(src: &Self::Src) -> wincode::WriteResult<usize> {
            let len = src.len();
            let len_bytes = BatchWireLen::write_bytes_needed(len)?;

            if is_pod::<T>() {
                return Ok(len_bytes + len * mem::size_of::<T>());
            }

            let mut total = len_bytes;
            for x in src.as_slice() {
                total += <T as wincode::SchemaWrite>::size_of(x)?;
            }
            Ok(total)
        }

        #[inline]
        fn write(
            writer: &mut impl wincode::io::Writer,
            src: &Self::Src,
        ) -> wincode::WriteResult<()> {
            let total_bytes = Self::size_of(src)?;
            let mut w = unsafe { writer.as_trusted_for(total_bytes)? };
            BatchWireLen::write(&mut w, src.len())?;

            if is_pod::<T>() {
                let slice = src.as_slice();
                unsafe {
                    w.write_slice_t(slice)?;
                }
            } else {
                for item in src.as_slice() {
                    <T as wincode::SchemaWrite>::write(&mut w, item)?;
                }
            }
            w.finish()?;
            Ok(())
        }
    }

    impl<'de, T: Copy, const N: usize> wincode::SchemaRead<'de> for ArrayVec<T, N>
    where
        T: wincode::SchemaRead<'de, Dst = T> + wincode::SchemaWrite<Src = T>,
    {
        type Dst = Self;

        fn read(
            reader: &mut impl wincode::io::Reader<'de>,
            dst: &mut MaybeUninit<Self::Dst>,
        ) -> wincode::ReadResult<()> {
            let len = BatchWireLen::read::<T>(reader)?;
            if len > N {
                return Err(wincode::ReadError::LengthEncodingOverflow("too many values"));
            }

            let mut arr = ArrayVec::<T, N>::new();
            if is_pod::<T>() {
                unsafe {
                    let dst_slice: &mut [MaybeUninit<T>] =
                        core::slice::from_raw_parts_mut(arr.data.as_mut_ptr(), len);
                    reader.copy_into_slice_t(dst_slice)?;
                }
                arr.len = len;
            } else {
                for _ in 0..len {
                    let mut tmp = MaybeUninit::<T>::uninit();
                    <T as wincode::SchemaRead>::read(reader, &mut tmp)?;
                    arr.push(unsafe { tmp.assume_init() });
                }
            }

            dst.write(arr);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let mut v: ArrayVec<u32, 4> = ArrayVec::new();
        assert!(v.is_empty());
        v.push(1);
        v.push(2);
        assert_eq!(v.as_slice(), &[1, 2]);
        assert_eq!(v.pop(), Some(2));
        assert_eq!(v.pop(), Some(1));
        assert_eq!(v.pop(), None);
        for i in 1..5 {
            v.push(i);
        }
        assert!(v.try_push(5).is_some());
    }

    #[test]
    fn copy() {
        let mut a: ArrayVec<u16, 4> = ArrayVec::new();
        a.push(10);
        a.push(20);

        let b = a;
        assert_eq!(a.as_slice(), b.as_slice());
    }

    #[test]
    fn macro_list() {
        let v: ArrayVec<u32, 4> = array_vec![1, 2, 3];
        assert_eq!(v.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn macro_repeat() {
        let v: ArrayVec<u32, 4> = array_vec![42; 3];
        assert_eq!(v.as_slice(), &[42, 42, 42]);
    }

    #[test]
    fn macro_empty() {
        let v: ArrayVec<u32, 4> = array_vec![];
        assert!(v.is_empty());
    }

    #[test]
    fn macro_trailing_comma() {
        let v: ArrayVec<u32, 4> = array_vec![1, 2,];
        assert_eq!(v.as_slice(), &[1, 2]);
    }

    mod arraystr {
        use super::*;
        use crate::short_typename;

        struct MyTile;
        struct GenericTile<T>(std::marker::PhantomData<T>);

        #[test]
        fn short_typename_simple() {
            let name = short_typename::<MyTile>();
            assert_eq!(name.as_str(), "MyTile");
        }

        #[test]
        fn short_typename_generic() {
            let name = short_typename::<GenericTile<u32>>();
            assert_eq!(name.as_str(), "GenericTile<u32>");
        }

        #[test]
        fn from_str_truncate() {
            let s = ArrayStr::<16>::from_str_truncate("hello");
            assert_eq!(s.as_str(), "hello");
            assert_eq!(s.len(), 5);
        }

        #[test]
        fn display() {
            let s = ArrayStr::<16>::from_str_truncate("test");
            let formatted = format!("{}", s);
            assert_eq!(formatted, "test");
        }

        #[test]
        fn copy_semantics() {
            let a = ArrayStr::<16>::from_str_truncate("abc");
            let b = a;
            assert_eq!(a.as_str(), b.as_str());
        }
    }
}

#[cfg(all(test, feature = "wincode"))]
mod wincode_tests {
    use super::*;

    #[test]
    fn simple() {
        const N: usize = 40;
        let mut v1: ArrayVec<u32, N> = ArrayVec::new();
        for i in 0..N as u32 {
            v1.push(i);
        }
        let bytes = wincode::serialize(&v1).unwrap();
        let v2 = wincode::deserialize::<ArrayVec<u32, N>>(bytes.as_slice()).unwrap();
        assert_eq!(v1.as_slice(), v2.as_slice());
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    #[test]
    fn non_pod() {
        let data = vec![42; 100];
        let mut v: ArrayVec<&[u8], 1> = ArrayVec::new();
        v.push(&data);

        let bytes = wincode::serialize(&v).unwrap();
        assert!(
            contains(&bytes, &data),
            "serialized output should contain slice payload bytes; got: {bytes:?}"
        );
    }
}
