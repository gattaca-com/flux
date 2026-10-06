//! Batched datagram syscalls. Linux uses `sendmmsg`/`recvmmsg`; elsewhere the
//! same API loops over `sendmsg`/`recvmsg`. Both are non-blocking and send or
//! receive whatever is available right now: there is no accumulation delay.

use std::{
    io, mem,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    os::fd::RawFd,
    ptr, slice,
};

use super::wire::MAX_DATAGRAM_SIZE;

/// Receive entries per syscall.
pub(crate) const BATCH: usize = 32;
/// Datagrams per send batch.
pub(crate) const SEND_BATCH: usize = 256;

#[cfg(target_os = "linux")]
type MMsgHdr = libc::mmsghdr;

#[cfg(not(target_os = "linux"))]
#[repr(C)]
#[derive(Clone, Copy)]
struct MMsgHdr {
    msg_hdr: libc::msghdr,
    msg_len: libc::c_uint,
}

/// A `SocketAddr` in the kernel's layout.
#[derive(Clone, Copy)]
pub(crate) struct SockAddr {
    storage: libc::sockaddr_storage,
    len: libc::socklen_t,
}

impl SockAddr {
    pub(crate) fn new(addr: SocketAddr) -> Self {
        let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
        let len = match addr {
            SocketAddr::V4(a) => {
                let encoded = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: a.port().to_be(),
                    sin_addr: libc::in_addr { s_addr: u32::from_ne_bytes(a.ip().octets()) },
                    sin_zero: [0; 8],
                };
                unsafe { ptr::write(ptr::from_mut(&mut storage).cast(), encoded) };
                mem::size_of::<libc::sockaddr_in>()
            }
            SocketAddr::V6(a) => {
                let encoded = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: a.port().to_be(),
                    sin6_flowinfo: a.flowinfo().to_be(),
                    sin6_addr: libc::in6_addr { s6_addr: a.ip().octets() },
                    sin6_scope_id: a.scope_id(),
                };
                unsafe { ptr::write(ptr::from_mut(&mut storage).cast(), encoded) };
                mem::size_of::<libc::sockaddr_in6>()
            }
        };
        Self { storage, len: len as libc::socklen_t }
    }

    fn decode(storage: &libc::sockaddr_storage, len: libc::socklen_t) -> Option<SocketAddr> {
        match libc::c_int::from(storage.ss_family) {
            libc::AF_INET if len as usize >= mem::size_of::<libc::sockaddr_in>() => {
                let a: &libc::sockaddr_in = unsafe { &*ptr::from_ref(storage).cast() };
                Some(SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::from(a.sin_addr.s_addr.to_ne_bytes()),
                    u16::from_be(a.sin_port),
                )))
            }
            libc::AF_INET6 if len as usize >= mem::size_of::<libc::sockaddr_in6>() => {
                let a: &libc::sockaddr_in6 = unsafe { &*ptr::from_ref(storage).cast() };
                Some(SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(a.sin6_addr.s6_addr),
                    u16::from_be(a.sin6_port),
                    u32::from_be(a.sin6_flowinfo),
                    a.sin6_scope_id,
                )))
            }
            _ => None,
        }
    }

    /// Encoded bytes; `new` zero-fills `storage`, so equal addresses are
    /// byte-equal over `len`.
    #[inline]
    fn bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(ptr::from_ref(&self.storage).cast(), self.len as usize) }
    }
}

impl PartialEq for SockAddr {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.bytes() == other.bytes()
    }
}

/// Read-only buffer for sending.
#[inline]
fn iovec(bytes: &[u8]) -> libc::iovec {
    libc::iovec { iov_base: bytes.as_ptr().cast_mut().cast(), iov_len: bytes.len() }
}

/// Writable buffer for receiving.
#[inline]
fn iovec_mut(bytes: &mut [u8]) -> libc::iovec {
    libc::iovec { iov_base: bytes.as_mut_ptr().cast(), iov_len: bytes.len() }
}

/// Entries the kernel accepted, or -1 with `errno` when it refused the first.
#[cfg(target_os = "linux")]
#[inline]
fn sendmmsg(fd: RawFd, hdrs: &mut [MMsgHdr]) -> libc::c_int {
    unsafe { libc::sendmmsg(fd, hdrs.as_mut_ptr(), hdrs.len() as libc::c_uint, libc::MSG_DONTWAIT) }
}

/// Shortest run of equal-size datagrams to one destination sent as one
/// `UDP_SEGMENT` entry; shorter runs go out as plain datagrams.
const GSO_MIN_SEGMENTS: usize = 4;
/// `UDP_MAX_SEGMENTS` of the first kernels with `UDP_SEGMENT`.
const GSO_MAX_SEGMENTS: usize = 64;
/// Most iovecs one `sendmsg` accepts.
const UIO_MAXIOV: usize = 1024;
/// Payloads shorter than this are copied next to their header, so a
/// datagram of small records is one iovec; longer ones are borrowed.
const COPY_THRESHOLD: usize = 256;

#[cfg(target_os = "linux")]
const SEGMENT_CONTROL_SPACE: usize =
    unsafe { libc::CMSG_SPACE(mem::size_of::<u16>() as _) as usize };

#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Clone, Copy)]
struct SegmentControl {
    header: libc::cmsghdr,
    size: u16,
    padding: [u8; SEGMENT_CONTROL_SPACE - mem::size_of::<libc::cmsghdr>() - 2],
}

#[cfg(target_os = "linux")]
const SEGMENT_CONTROL: SegmentControl = SegmentControl {
    header: libc::cmsghdr {
        cmsg_len: unsafe { libc::CMSG_LEN(mem::size_of::<u16>() as _) as usize },
        cmsg_level: libc::SOL_UDP,
        cmsg_type: libc::UDP_SEGMENT,
    },
    size: 0,
    padding: [0; SEGMENT_CONTROL_SPACE - mem::size_of::<libc::cmsghdr>() - 2],
};

#[derive(Clone, Copy)]
struct Datagram {
    iov_start: u32,
    iov_len: u32,
    bytes: u32,
    addr: u32,
}

/// One `sendmmsg` entry: a run of datagrams to one destination.
#[derive(Clone, Copy)]
struct Entry {
    first: u32,
    count: u32,
    iov_len: u32,
    segmented: bool,
}

/// Up to [`SEND_BATCH`] outgoing datagrams built piece by piece.
///
/// Headers and small payloads are copied into an arena the batch owns; a
/// payload of [`COPY_THRESHOLD`] bytes or more is borrowed and must stay
/// alive and unmodified until [`Self::send`] returns.
pub(crate) struct SendBatch {
    datagram_size: usize,
    arena: Box<[u8]>,
    arena_len: usize,
    iovs: Vec<libc::iovec>,
    /// Whether the last iovec is the arena's tail and can be extended.
    arena_tail: bool,
    datagrams: Vec<Datagram>,
    addrs: Vec<SockAddr>,
    /// Bytes of the datagram being built.
    building: usize,
    entries: Vec<Entry>,
    hdrs: Vec<MMsgHdr>,
    /// Kernel accepts `UDP_SEGMENT`; see [`Self::enable_gso`].
    #[cfg(target_os = "linux")]
    gso: bool,
    #[cfg(target_os = "linux")]
    controls: Vec<SegmentControl>,
}

// SAFETY: the raw pointers inside are written right before each syscall and
// only read by it; nothing is shared or dereferenced across threads.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for SendBatch {}

impl SendBatch {
    /// `datagram_size` bounds every datagram built.
    pub(crate) fn new(datagram_size: usize) -> Self {
        // Two iovecs per borrowed payload plus one for the arena run in
        // between, and a borrowed payload needs COPY_THRESHOLD bytes.
        let iovs_per_datagram = 1 + 2 * datagram_size.div_ceil(COPY_THRESHOLD);
        Self {
            datagram_size,
            arena: vec![0; SEND_BATCH * datagram_size].into_boxed_slice(),
            arena_len: 0,
            iovs: Vec::with_capacity(SEND_BATCH * iovs_per_datagram),
            arena_tail: false,
            datagrams: Vec::with_capacity(SEND_BATCH),
            addrs: Vec::with_capacity(SEND_BATCH),
            building: 0,
            entries: Vec::with_capacity(SEND_BATCH),
            hdrs: Vec::with_capacity(SEND_BATCH),
            #[cfg(target_os = "linux")]
            gso: false,
            #[cfg(target_os = "linux")]
            controls: Vec::with_capacity(SEND_BATCH),
        }
    }

    /// Turns on segmentation offload for `send` if the kernel supports it.
    /// Kernels before 4.18 ignore an unknown `SOL_UDP` cmsg and coalesce the
    /// batch into one datagram, so support has to be probed rather than
    /// detected from a send error. `fd` is left unchanged (segment size 0).
    #[cfg(target_os = "linux")]
    pub(crate) fn enable_gso(&mut self, fd: RawFd) -> io::Result<()> {
        let size: libc::c_int = 0;
        let result = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_UDP,
                libc::UDP_SEGMENT,
                ptr::from_ref(&size).cast(),
                mem::size_of_val(&size) as _,
            )
        };
        self.gso = result == 0;
        if result < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
    }

    #[inline]
    pub(crate) fn is_full(&self) -> bool {
        self.datagrams.len() == SEND_BATCH
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.datagrams.is_empty()
    }

    /// Starts a datagram to `to`; the batch must not be full.
    pub(crate) fn open(&mut self, to: &SockAddr) {
        debug_assert!(!self.is_full());
        match self.addrs.last() {
            Some(last) if last == to => {}
            _ => self.addrs.push(*to),
        }
        self.datagrams.push(Datagram {
            iov_start: self.iovs.len() as u32,
            iov_len: 0,
            bytes: 0,
            addr: (self.addrs.len() - 1) as u32,
        });
        self.arena_tail = false;
        self.building = 0;
    }

    /// Appends bytes the batch copies; headers always go this way.
    pub(crate) fn copy(&mut self, bytes: &[u8]) {
        let start = self.arena_len;
        self.arena[start..start + bytes.len()].copy_from_slice(bytes);
        self.arena_len += bytes.len();
        self.building += bytes.len();
        if self.arena_tail {
            self.iovs.last_mut().unwrap().iov_len += bytes.len();
        } else {
            self.iovs.push(iovec(&self.arena[start..self.arena_len]));
            self.arena_tail = true;
        }
    }

    /// Appends a payload, borrowing it when it is long enough to be worth
    /// an iovec of its own.
    #[inline]
    pub(crate) fn payload(&mut self, bytes: &[u8]) {
        if bytes.len() < COPY_THRESHOLD {
            self.copy(bytes);
        } else {
            self.iovs.push(iovec(bytes));
            self.arena_tail = false;
            self.building += bytes.len();
        }
    }

    /// Finishes the datagram being built. Returns its size.
    pub(crate) fn close(&mut self) -> usize {
        debug_assert!(self.building <= self.datagram_size);
        let datagram = self.datagrams.last_mut().unwrap();
        datagram.iov_len = self.iovs.len() as u32 - datagram.iov_start;
        datagram.bytes = self.building as u32;
        self.building
    }

    /// Groups the datagrams from `from` on into entries. With `gso`, a run of
    /// at least [`GSO_MIN_SEGMENTS`] equal-size datagrams to one destination
    /// (the last may be shorter) becomes segmented entries of near-equal
    /// length, so capping a long run never strands a short plain tail.
    fn build_entries(&mut self, from: usize, gso: bool) {
        self.entries.clear();
        let max_segments = GSO_MAX_SEGMENTS.min(MAX_DATAGRAM_SIZE / self.datagram_size).max(1);
        let mut s = from;
        while s < self.datagrams.len() {
            let first = self.datagrams[s];
            let mut e = s + 1;
            let mut iov_len = first.iov_len;
            while gso && e < self.datagrams.len() {
                let next = self.datagrams[e];
                if next.addr != first.addr ||
                    self.datagrams[e - 1].bytes != first.bytes ||
                    iov_len + next.iov_len > UIO_MAXIOV as u32
                {
                    break;
                }
                iov_len += next.iov_len;
                e += 1;
            }
            if e - s >= GSO_MIN_SEGMENTS {
                let parts = (e - s).div_ceil(max_segments);
                let per = (e - s).div_ceil(parts);
                for k in (s..e).step_by(per) {
                    let part = &self.datagrams[k..(k + per).min(e)];
                    self.entries.push(Entry {
                        first: k as u32,
                        count: part.len() as u32,
                        iov_len: part.iter().map(|d| d.iov_len).sum(),
                        segmented: true,
                    });
                }
            } else {
                for (k, datagram) in self.datagrams[s..e].iter().enumerate() {
                    self.entries.push(Entry {
                        first: (s + k) as u32,
                        count: 1,
                        iov_len: datagram.iov_len,
                        segmented: false,
                    });
                }
            }
            s = e;
        }
    }

    /// Points one `msghdr` at each entry.
    fn stage(&mut self) {
        let n = self.entries.len();
        self.hdrs.clear();
        self.hdrs.resize(n, unsafe { mem::zeroed() });
        #[cfg(target_os = "linux")]
        {
            self.controls.clear();
            self.controls.resize(n, SEGMENT_CONTROL);
        }
        for (e, entry) in self.entries.iter().enumerate() {
            let first = self.datagrams[entry.first as usize];
            let addr = &mut self.addrs[first.addr as usize];
            let hdr = &mut self.hdrs[e].msg_hdr;
            hdr.msg_iov = self.iovs[first.iov_start as usize..].as_mut_ptr();
            hdr.msg_iovlen = entry.iov_len as _;
            hdr.msg_name = ptr::from_mut(&mut addr.storage).cast();
            hdr.msg_namelen = addr.len;
            #[cfg(target_os = "linux")]
            if entry.segmented {
                self.controls[e].size = first.bytes as u16;
                hdr.msg_control = ptr::from_mut(&mut self.controls[e]).cast();
                hdr.msg_controllen = SEGMENT_CONTROL_SPACE;
            }
        }
    }

    /// Datagrams inside the first `entries` entries.
    #[inline]
    fn datagrams_in(&self, entries: usize) -> usize {
        self.entries[..entries].iter().map(|e| e.count as usize).sum()
    }

    /// Sends the closed datagrams and empties the batch. Returns how many
    /// the kernel accepted, in order; `WouldBlock` only when it accepted
    /// none.
    pub(crate) fn send(&mut self, fd: RawFd) -> io::Result<usize> {
        let result = self.send_datagrams(fd);
        self.datagrams.clear();
        self.addrs.clear();
        self.iovs.clear();
        self.arena_len = 0;
        result
    }

    #[cfg(target_os = "linux")]
    fn send_datagrams(&mut self, fd: RawFd) -> io::Result<usize> {
        self.build_entries(0, self.gso);
        self.stage();
        let sent = sendmmsg(fd, &mut self.hdrs);
        let err = (sent < 0).then(io::Error::last_os_error);
        // The kernel stops at the first entry it refuses. UDP sends are
        // atomic, so every accepted entry delivered all its datagrams.
        let accepted = usize::try_from(sent).unwrap_or(0);
        let datagrams = self.datagrams_in(accepted);
        // Route MTU, SG support, checksum offload and memory pressure all
        // refuse a segmented entry; a full socket buffer refuses plain
        // datagrams just the same.
        let retry = self.entries.get(accepted).is_some_and(|entry| entry.segmented) &&
            err.as_ref().is_none_or(|err| err.kind() != io::ErrorKind::WouldBlock);
        if !retry {
            return err.map_or(Ok(datagrams), Err);
        }
        // Retry the rest as plain datagrams, which reports the same errno if
        // it persists.
        let resume = self.entries[accepted].first as usize;
        self.build_entries(resume, false);
        self.stage();
        match sendmmsg(fd, &mut self.hdrs) {
            sent @ 0.. => Ok(datagrams + sent as usize),
            _ if datagrams != 0 => Ok(datagrams),
            _ => Err(io::Error::last_os_error()),
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn send_datagrams(&mut self, fd: RawFd) -> io::Result<usize> {
        self.build_entries(0, false);
        self.stage();
        for i in 0..self.entries.len() {
            let r = unsafe { libc::sendmsg(fd, &self.hdrs[i].msg_hdr, libc::MSG_DONTWAIT) };
            if r < 0 {
                let err = io::Error::last_os_error();
                return if i == 0 { Err(err) } else { Ok(i) };
            }
        }
        Ok(self.entries.len())
    }
}

#[cfg(target_os = "linux")]
const GRO_CONTROL_SPACE: usize =
    unsafe { libc::CMSG_SPACE(mem::size_of::<libc::c_int>() as _) as usize };

#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Clone, Copy)]
struct GroControl {
    header: libc::cmsghdr,
    size: libc::c_int,
    padding:
        [u8; GRO_CONTROL_SPACE - mem::size_of::<libc::cmsghdr>() - mem::size_of::<libc::c_int>()],
}

/// Up to [`BATCH`] receive entries, each possibly holding GRO segments.
pub(crate) struct RecvBatch {
    bufs: Vec<u8>,
    stride: usize,
    datagram_size: usize,
    #[cfg(target_os = "linux")]
    controls: [GroControl; BATCH],
    addrs: [libc::sockaddr_storage; BATCH],
    iovs: [libc::iovec; BATCH],
    hdrs: [MMsgHdr; BATCH],
    len: usize,
}

// SAFETY: as for `SendBatch`; every pointer is re-established inside `recv`.
unsafe impl Send for RecvBatch {}

impl RecvBatch {
    /// `datagram_size` bounds each wire datagram, including GRO segments.
    pub(crate) fn new(datagram_size: usize) -> Self {
        #[cfg(target_os = "linux")]
        let stride = datagram_size.saturating_mul(64).min(65_535);
        #[cfg(not(target_os = "linux"))]
        let stride = datagram_size;
        Self {
            bufs: vec![0; BATCH * stride],
            stride,
            datagram_size,
            #[cfg(target_os = "linux")]
            controls: unsafe { mem::zeroed() },
            addrs: unsafe { mem::zeroed() },
            iovs: unsafe { mem::zeroed() },
            hdrs: unsafe { mem::zeroed() },
            len: 0,
        }
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn enable_gro(fd: RawFd) -> io::Result<()> {
        let enabled: libc::c_int = 1;
        let result = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_UDP,
                libc::UDP_GRO,
                ptr::from_ref(&enabled).cast(),
                mem::size_of_val(&enabled) as _,
            )
        };
        if result < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
    }

    /// Receives whatever is queued, up to [`BATCH`] entries.
    pub(crate) fn recv(&mut self, fd: RawFd) -> io::Result<usize> {
        self.len = 0;
        for i in 0..BATCH {
            self.iovs[i] = iovec_mut(&mut self.bufs[i * self.stride..(i + 1) * self.stride]);
            let hdr = &mut self.hdrs[i].msg_hdr;
            hdr.msg_name = ptr::from_mut(&mut self.addrs[i]).cast();
            hdr.msg_namelen = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            hdr.msg_iov = ptr::from_mut(&mut self.iovs[i]);
            hdr.msg_iovlen = 1;
            hdr.msg_flags = 0;
            #[cfg(target_os = "linux")]
            {
                hdr.msg_control = ptr::from_mut(&mut self.controls[i]).cast();
                hdr.msg_controllen = GRO_CONTROL_SPACE;
            }
        }
        #[cfg(target_os = "linux")]
        {
            let n = unsafe {
                libc::recvmmsg(
                    fd,
                    self.hdrs.as_mut_ptr(),
                    BATCH as libc::c_uint,
                    libc::MSG_DONTWAIT,
                    ptr::null_mut(),
                )
            };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            self.len = n as usize;
        }
        #[cfg(not(target_os = "linux"))]
        {
            for i in 0..BATCH {
                let r = unsafe { libc::recvmsg(fd, &mut self.hdrs[i].msg_hdr, libc::MSG_DONTWAIT) };
                if r < 0 {
                    let err = io::Error::last_os_error();
                    if i == 0 {
                        return Err(err);
                    }
                    break;
                }
                self.hdrs[i].msg_len = r as libc::c_uint;
                self.len = i + 1;
            }
        }
        Ok(self.len)
    }

    /// Wire datagrams in receive entry `i`; rejects truncated or oversized
    /// entries.
    pub(crate) fn datagrams(&self, i: usize) -> Option<(std::slice::Chunks<'_, u8>, SocketAddr)> {
        debug_assert!(i < self.len);
        let hdr = &self.hdrs[i];
        if hdr.msg_hdr.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
            return None;
        }
        let len = hdr.msg_len as usize;
        #[cfg(not(target_os = "linux"))]
        let segment_size = len;
        #[cfg(target_os = "linux")]
        let segment_size = if hdr.msg_hdr.msg_controllen != 0 {
            let control = &self.controls[i];
            let control_len =
                unsafe { libc::CMSG_LEN(mem::size_of::<libc::c_int>() as _) as usize };
            if hdr.msg_hdr.msg_controllen < control_len ||
                control.header.cmsg_len != control_len ||
                control.header.cmsg_level != libc::SOL_UDP ||
                control.header.cmsg_type != libc::UDP_GRO
            {
                return None;
            }
            usize::try_from(control.size).ok()?
        } else {
            len
        };
        if segment_size == 0 || segment_size > self.datagram_size {
            return None;
        }
        let from = SockAddr::decode(&self.addrs[i], hdr.msg_hdr.msg_namelen)?;
        let start = i * self.stride;
        Some((self.bufs[start..start + len].chunks(segment_size), from))
    }
}

#[cfg(test)]
mod tests {
    use std::{net::UdpSocket, os::fd::AsRawFd, time::Duration};

    use super::*;

    #[cfg(target_os = "linux")]
    fn offload_available(result: io::Result<()>) -> bool {
        match result {
            Ok(()) => true,
            Err(err)
                if matches!(err.raw_os_error(), Some(libc::ENOPROTOOPT | libc::EOPNOTSUPP)) =>
            {
                false
            }
            Err(err) => panic!("offload setup failed: {err}"),
        }
    }

    fn pair() -> (UdpSocket, UdpSocket, SockAddr) {
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let to = SockAddr::new(receiver.local_addr().unwrap());
        (sender, receiver, to)
    }

    /// Builds a datagram of `small` copied pieces followed by one borrowed
    /// payload of `big` bytes.
    fn build(batch: &mut SendBatch, to: &SockAddr, small: &[&[u8]], big: &[u8]) -> Vec<u8> {
        batch.open(to);
        let mut expect = Vec::new();
        for piece in small {
            batch.copy(piece);
            expect.extend_from_slice(piece);
        }
        if !big.is_empty() {
            batch.payload(big);
            expect.extend_from_slice(big);
        }
        batch.close();
        expect
    }

    #[test]
    fn datagrams_keep_their_bytes_and_order() {
        let (sender, receiver, to) = pair();
        let mut batch = SendBatch::new(1200);
        #[cfg(target_os = "linux")]
        offload_available(batch.enable_gso(sender.as_raw_fd()));
        let big = vec![7; 900];
        let mut expected = Vec::new();
        for i in 0..6u8 {
            let (head, tail) = ([i; 25], [i; 200]);
            let small = [head.as_slice(), b"abc", tail.as_slice()];
            let big_len = if i == 5 { 100 } else { big.len() };
            expected.push(build(&mut batch, &to, &small, &big[..big_len]));
        }
        assert_eq!(batch.send(sender.as_raw_fd()).unwrap(), 6);
        assert!(batch.is_empty());
        let mut buf = [0; 2048];
        for expect in expected {
            let n = receiver.recv(&mut buf).unwrap();
            assert_eq!(&buf[..n], &expect[..]);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn segmentation_falls_back_without_checksums() {
        let (sender, receiver, to) = pair();
        let no_check: libc::c_int = 1;
        assert_eq!(
            unsafe {
                libc::setsockopt(
                    sender.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_NO_CHECK,
                    ptr::from_ref(&no_check).cast(),
                    mem::size_of_val(&no_check) as _,
                )
            },
            0
        );
        let mut batch = SendBatch::new(64);
        if !offload_available(batch.enable_gso(sender.as_raw_fd())) {
            return;
        }
        for _ in 0..5 {
            build(&mut batch, &to, &[b"headerpayload"], &[]);
        }
        assert_eq!(batch.send(sender.as_raw_fd()).unwrap(), 5);
        // Per-socket EIO is not a kernel capability; offload stays enabled.
        assert!(batch.gso);
        let mut buf = [0; 32];
        for _ in 0..5 {
            let n = receiver.recv(&mut buf).unwrap();
            assert_eq!(&buf[..n], b"headerpayload");
        }
    }

    #[test]
    fn mixed_destinations_keep_their_datagrams() {
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receivers =
            [UdpSocket::bind("127.0.0.1:0").unwrap(), UdpSocket::bind("127.0.0.1:0").unwrap()];
        for receiver in &receivers {
            receiver.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        }
        let to = receivers.each_ref().map(|r| SockAddr::new(r.local_addr().unwrap()));
        let mut batch = SendBatch::new(1200);
        let payloads: [&[u8]; 4] = [b"a", b"bbbb", b"cc", b"ddd"];
        for (i, payload) in payloads.iter().enumerate() {
            build(&mut batch, &to[i % 2], &[b"h", payload], &[]);
        }
        assert_eq!(batch.send(sender.as_raw_fd()).unwrap(), 4);
        let mut buf = [0; 32];
        for (i, payload) in payloads.iter().enumerate() {
            let n = receivers[i % 2].recv(&mut buf).unwrap();
            assert_eq!(buf[0], b'h');
            assert_eq!(&buf[1..n], *payload);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn gro_splits_segments_and_then_receives_plain_datagrams() {
        for bind in ["127.0.0.1:0", "[::1]:0"] {
            let sender = match UdpSocket::bind(bind) {
                Ok(socket) => socket,
                Err(_) if bind.starts_with('[') => continue,
                Err(err) => panic!("IPv4 bind failed: {err}"),
            };
            let receiver = UdpSocket::bind(bind).unwrap();
            if !offload_available(RecvBatch::enable_gro(receiver.as_raw_fd())) {
                return;
            }
            let to = SockAddr::new(receiver.local_addr().unwrap());
            let mut tx = SendBatch::new(1200);
            if !offload_available(tx.enable_gso(sender.as_raw_fd())) {
                return;
            }
            // Three full datagrams and a short one: one GSO run.
            let payload = [0x5a; 1200 - 29];
            let len = |i: usize| if i == 3 { 17 } else { payload.len() };
            for i in 0..4u8 {
                build(&mut tx, &to, &[&[i + 1; 29]], &payload[..len(i as usize)]);
            }
            assert_eq!(tx.send(sender.as_raw_fd()).unwrap(), 4);
            let mut rx = RecvBatch::new(1200);
            let n = rx.recv(receiver.as_raw_fd()).unwrap();
            assert_eq!(n, 1, "expected a combined GRO entry");
            assert_eq!(rx.controls[0].header.cmsg_type, libc::UDP_GRO);
            let mut seen = 0;
            for i in 0..n {
                let (datagrams, from) = rx.datagrams(i).unwrap();
                assert_eq!(from, sender.local_addr().unwrap());
                for bytes in datagrams {
                    assert_eq!(&bytes[..29], &[seen as u8 + 1; 29]);
                    assert_eq!(&bytes[29..], &payload[..len(seen)]);
                    seen += 1;
                }
            }
            assert_eq!(seen, 4);
            sender.send_to(b"plain", receiver.local_addr().unwrap()).unwrap();
            assert_eq!(rx.recv(receiver.as_raw_fd()).unwrap(), 1);
            let (mut datagrams, _) = rx.datagrams(0).unwrap();
            assert_eq!(datagrams.next(), Some(b"plain".as_slice()));
            assert!(datagrams.next().is_none());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn gro_rejects_bad_metadata_and_oversized_datagrams() {
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        let mut rx = RecvBatch::new(1200);
        sender.send_to(&[0; 1201], receiver.local_addr().unwrap()).unwrap();
        assert_eq!(rx.recv(receiver.as_raw_fd()).unwrap(), 1);
        assert!(rx.datagrams(0).is_none());
        rx.hdrs[0].msg_hdr.msg_controllen = GRO_CONTROL_SPACE;
        rx.controls[0].header = libc::cmsghdr {
            cmsg_len: unsafe { libc::CMSG_LEN(mem::size_of::<libc::c_int>() as _) as usize },
            cmsg_level: libc::SOL_UDP,
            cmsg_type: libc::UDP_GRO,
        };
        for size in [0, -1, 1201] {
            rx.controls[0].size = size;
            assert!(rx.datagrams(0).is_none());
        }
        rx.controls[0].size = 1200;
        for flag in [libc::MSG_TRUNC, libc::MSG_CTRUNC] {
            rx.hdrs[0].msg_hdr.msg_flags = flag;
            assert!(rx.datagrams(0).is_none());
        }
    }

    #[test]
    fn sockaddr_roundtrip() {
        for addr in ["127.0.0.1:4242", "[::1]:9"] {
            let addr: SocketAddr = addr.parse().unwrap();
            let native = SockAddr::new(addr);
            assert_eq!(SockAddr::decode(&native.storage, native.len), Some(addr));
        }
    }
}
