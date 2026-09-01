//! The serialiser's view of one frame while a batch is staged.

use std::{
    io::{self, Write},
    ops::{Deref, DerefMut},
};

/// The payload of one outgoing frame while a serialiser fills it.
///
/// Frames are staged back to back in one send buffer, so a serialiser must
/// not be able to reach the bytes of frames staged before its own. This
/// wrapper exposes only the payload region: every length, index, and
/// truncation is relative to the start of the payload, and the frame header
/// and earlier frames stay out of reach.
pub struct PayloadBuf<'a> {
    bytes: &'a mut Vec<u8>,
    start: usize,
}

impl<'a> PayloadBuf<'a> {
    pub(super) fn new(bytes: &'a mut Vec<u8>) -> Self {
        let start = bytes.len();
        Self { bytes, start }
    }

    /// Bytes serialised into this payload so far.
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len() - self.start
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reserves room for at least `additional` more payload bytes.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.bytes.reserve(additional);
    }

    #[inline]
    pub fn push(&mut self, byte: u8) {
        self.bytes.push(byte);
    }

    #[inline]
    pub fn extend_from_slice(&mut self, other: &[u8]) {
        self.bytes.extend_from_slice(other);
    }

    /// Resizes the payload to `len` bytes, filling new bytes with `value`.
    ///
    /// # Panics
    ///
    /// Panics if the payload cannot fit in memory, as `Vec::resize` does.
    #[inline]
    pub fn resize(&mut self, len: usize, value: u8) {
        let end = self.start.checked_add(len).expect("payload length overflows usize");
        self.bytes.resize(end, value);
    }

    /// Shortens the payload to `len` bytes; no-op if already shorter.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        // Clamping keeps `start + len` from wrapping into earlier frames.
        let len = len.min(self.len());
        self.bytes.truncate(self.start + len);
    }

    /// Removes every payload byte serialised so far.
    #[inline]
    pub fn clear(&mut self) {
        self.bytes.truncate(self.start);
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[self.start..]
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes[self.start..]
    }
}

impl Deref for PayloadBuf<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl DerefMut for PayloadBuf<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl Extend<u8> for PayloadBuf<'_> {
    #[inline]
    fn extend<I: IntoIterator<Item = u8>>(&mut self, iter: I) {
        self.bytes.extend(iter);
    }
}

impl<'b> Extend<&'b u8> for PayloadBuf<'_> {
    #[inline]
    fn extend<I: IntoIterator<Item = &'b u8>>(&mut self, iter: I) {
        self.bytes.extend(iter);
    }
}

#[cfg(feature = "wincode")]
impl wincode::io::Writer for PayloadBuf<'_> {
    #[inline]
    fn write(&mut self, src: &[u8]) -> Result<(), wincode::io::WriteError> {
        self.bytes.extend_from_slice(src);
        Ok(())
    }

    #[inline]
    unsafe fn as_trusted_for(
        &mut self,
        n_bytes: usize,
    ) -> Result<impl wincode::io::Writer, wincode::io::WriteError> {
        // SAFETY: the caller upholds the `as_trusted_for` contract, and the
        // `Vec<u8>` writer only ever appends, so the payload start stays valid.
        unsafe { wincode::io::Writer::as_trusted_for(&mut *self.bytes, n_bytes) }
    }
}

impl Write for PayloadBuf<'_> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.bytes.extend_from_slice(buf);
        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::PayloadBuf;

    #[test]
    fn payload_buf_truncate_clamps_to_its_own_frame() {
        let mut bytes = b"earlier".to_vec();
        let mut payload = PayloadBuf::new(&mut bytes);
        payload.extend_from_slice(b"payload");

        payload.truncate(usize::MAX);
        assert_eq!(payload.as_slice(), b"payload");
        payload.truncate(3);
        assert_eq!(payload.as_slice(), b"pay");
        payload.truncate(0);
        assert!(payload.is_empty());
        assert_eq!(bytes, b"earlier");
    }

    #[test]
    fn payload_buf_resize_and_clear_stay_relative() {
        let mut bytes = b"earlier".to_vec();
        let mut payload = PayloadBuf::new(&mut bytes);
        payload.resize(3, 7);
        assert_eq!(payload.as_slice(), &[7, 7, 7]);
        payload.resize(1, 0);
        assert_eq!(payload.as_slice(), &[7]);
        payload.clear();
        assert!(payload.is_empty());
        assert_eq!(bytes, b"earlier");
    }

    #[test]
    #[should_panic(expected = "payload length overflows usize")]
    fn payload_buf_resize_rejects_overflow() {
        let mut bytes = b"earlier".to_vec();
        let mut payload = PayloadBuf::new(&mut bytes);
        payload.resize(usize::MAX, 0);
    }
}
