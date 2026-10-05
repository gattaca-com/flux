//! Batched datagram syscalls. Linux uses `sendmmsg`/`recvmmsg`; elsewhere the
//! same API loops over `sendmsg`/`recvmsg`. Both are non-blocking and send or
//! receive whatever is available right now: there is no accumulation delay.

use std::{
    io, mem,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    os::fd::RawFd,
    ptr, slice,
};

#[cfg(target_os = "linux")]
use super::wire::MAX_DATAGRAM_SIZE;
use super::wire::{PACKET_HEADER_SIZE, Packet};

/// Receive entries per syscall.
pub(crate) const BATCH: usize = 32;
/// Records per send batch.
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

/// Shortest run of packets to one peer sent as one `UDP_SEGMENT` entry;
/// shorter runs go out as plain datagrams.
const GSO_MIN_SEGMENTS: usize = 4;
/// `UDP_MAX_SEGMENTS` of the first kernels with `UDP_SEGMENT`.
const GSO_MAX_SEGMENTS: usize = 64;
/// Most iovecs one `sendmsg` accepts.
const UIO_MAXIOV: usize = 1024;

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

/// A record pushed for sending: header plus payload to one destination.
#[derive(Clone, Copy)]
struct Record {
    header: libc::iovec,
    payload: libc::iovec,
    stream: u32,
}

/// One wire datagram: a run of iovecs holding a packet header and record
/// pieces, or one bare record.
#[derive(Clone, Copy)]
struct Segment {
    iov_start: u32,
    iov_len: u32,
    stream: u32,
    /// Records whose last byte lies in this segment.
    ends: u32,
    len: u32,
}

/// One `sendmmsg` entry: a run of segments to one destination.
#[derive(Clone, Copy)]
struct Entry {
    seg_start: u32,
    iov_len: u32,
    ends: u32,
    segmented: bool,
}

/// A packet under construction.
#[derive(Clone, Copy)]
struct Open {
    /// Its header's slot in `packets`.
    slot: usize,
    iov_start: usize,
    used: usize,
    first: Option<usize>,
    ends: u32,
}

/// Up to [`SEND_BATCH`] outgoing records, each a header plus a payload slice.
///
/// Destinations are copied in; the slices passed to [`Self::push`] must stay
/// alive and unmodified until [`Self::send`] returns. Consecutive records to
/// one destination form a stream that `send` lays out back to back and cuts
/// every `segment_size` bytes into packets (see [`super::wire`]), so a record
/// may straddle two consecutive packets. A record that no packet could share
/// goes out bare, exactly as it would without packing.
pub(crate) struct SendBatch {
    segment_size: usize,
    records: Vec<Record>,
    /// One per stream.
    addrs: Vec<SockAddr>,
    iovs: Vec<libc::iovec>,
    /// Packet headers by segment index; iovecs point here, so it never moves.
    packets: Box<[[u8; PACKET_HEADER_SIZE]]>,
    segments: Vec<Segment>,
    entries: Vec<Entry>,
    hdrs: Vec<MMsgHdr>,
    next_packet: u32,
    /// Kernel accepts `UDP_SEGMENT`; see [`Self::enable_gso`].
    #[cfg(target_os = "linux")]
    gso: bool,
    /// One `UDP_SEGMENT` cmsg per entry; only segmented ones point at theirs.
    #[cfg(target_os = "linux")]
    controls: Vec<SegmentControl>,
}

/// A segment ends at most once inside each record and once more per bare
/// record or stream, so a batch never has more segments than this.
const MAX_SEGMENTS: usize = 3 * SEND_BATCH;
/// Two pieces per record plus one packet header and one straddle cut per
/// segment keep every entry under the kernel's iovec limit.
const _: () = assert!(2 * SEND_BATCH + 2 * GSO_MAX_SEGMENTS <= UIO_MAXIOV);

// SAFETY: the raw pointers inside are written right before each syscall and
// only read by it; nothing is shared or dereferenced across threads.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for SendBatch {}

impl SendBatch {
    /// `segment_size` is the wire datagram size, packet header included.
    pub(crate) fn new(segment_size: usize) -> Self {
        assert!(segment_size > PACKET_HEADER_SIZE);
        Self {
            segment_size,
            records: Vec::with_capacity(SEND_BATCH),
            addrs: Vec::with_capacity(SEND_BATCH),
            iovs: Vec::with_capacity(4 * SEND_BATCH),
            packets: vec![[0; PACKET_HEADER_SIZE]; MAX_SEGMENTS].into_boxed_slice(),
            segments: Vec::with_capacity(MAX_SEGMENTS),
            entries: Vec::with_capacity(MAX_SEGMENTS),
            hdrs: Vec::with_capacity(MAX_SEGMENTS),
            next_packet: 0,
            #[cfg(target_os = "linux")]
            gso: false,
            #[cfg(target_os = "linux")]
            controls: Vec::with_capacity(MAX_SEGMENTS),
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
    pub(crate) fn push(&mut self, header: &[u8], payload: &[u8], to: &SockAddr) {
        let stream = match self.addrs.last() {
            Some(last) if last == to => self.addrs.len() - 1,
            _ => {
                self.addrs.push(*to);
                self.addrs.len() - 1
            }
        };
        self.records.push(Record {
            header: iovec(header),
            payload: iovec(payload),
            stream: stream as u32,
        });
    }

    fn open(&mut self) -> Open {
        let (slot, iov_start) = (self.segments.len(), self.iovs.len());
        self.iovs.push(iovec(&self.packets[slot]));
        Open { slot, iov_start, used: PACKET_HEADER_SIZE, first: None, ends: 0 }
    }

    fn close(&mut self, open: Open, stream: u32) {
        Packet { seq: self.next_packet, first: open.first.unwrap_or(open.used) as u16 }
            .encode(&mut self.packets[open.slot]);
        self.next_packet = self.next_packet.wrapping_add(1);
        self.segments.push(Segment {
            iov_start: open.iov_start as u32,
            iov_len: (self.iovs.len() - open.iov_start) as u32,
            stream,
            ends: open.ends,
            len: open.used as u32,
        });
    }

    /// Cuts every stream into segments. A record too large to share a packet
    /// is its own bare segment, so the fragments of a large message stay one
    /// per datagram with no packing overhead.
    fn layout(&mut self) {
        self.iovs.clear();
        self.segments.clear();
        let size = self.segment_size;
        let mut i = 0;
        while i < self.records.len() {
            let stream = self.records[i].stream;
            let mut open: Option<Open> = None;
            while i < self.records.len() && self.records[i].stream == stream {
                let record = self.records[i];
                i += 1;
                let len = record.header.iov_len + record.payload.iov_len;
                if len + PACKET_HEADER_SIZE > size {
                    if let Some(o) = open.take() {
                        self.close(o, stream);
                    }
                    let iov_start = self.iovs.len() as u32;
                    self.iovs.push(record.header);
                    self.iovs.push(record.payload);
                    self.segments.push(Segment {
                        iov_start,
                        iov_len: 2,
                        stream,
                        ends: 1,
                        len: len as u32,
                    });
                    continue;
                }
                let o = open.get_or_insert_with(|| self.open());
                o.first.get_or_insert(o.used);
                for (piece, last) in [(record.header, false), (record.payload, true)] {
                    let mut off = 0;
                    while off < piece.iov_len {
                        let o = open.get_or_insert_with(|| self.open());
                        let take = (size - o.used).min(piece.iov_len - off);
                        self.iovs.push(libc::iovec {
                            iov_base: piece.iov_base.cast::<u8>().wrapping_add(off).cast(),
                            iov_len: take,
                        });
                        o.used += take;
                        off += take;
                        if last && off == piece.iov_len {
                            o.ends += 1;
                        }
                        if o.used == size {
                            self.close(open.take().unwrap(), stream);
                        }
                    }
                }
            }
            if let Some(o) = open {
                self.close(o, stream);
            }
        }
    }

    /// Groups the segments from `from` on into entries. With `gso`, a run of
    /// at least [`GSO_MIN_SEGMENTS`] full segments to one destination (the
    /// last may be shorter) becomes segmented entries of near-equal length,
    /// so capping a long run never strands a short plain tail.
    fn build_entries(&mut self, from: usize, gso: bool) {
        self.entries.clear();
        let size = self.segment_size as u32;
        #[cfg(target_os = "linux")]
        let max_segments = GSO_MAX_SEGMENTS.min(MAX_DATAGRAM_SIZE / self.segment_size).max(1);
        #[cfg(not(target_os = "linux"))]
        let max_segments = 1;
        let mut s = from;
        while s < self.segments.len() {
            let first = self.segments[s];
            let mut e = s + 1;
            while gso &&
                e < self.segments.len() &&
                self.segments[e].stream == first.stream &&
                self.segments[e - 1].len == size
            {
                e += 1;
            }
            if e - s >= GSO_MIN_SEGMENTS {
                let parts = (e - s).div_ceil(max_segments);
                let per = (e - s).div_ceil(parts);
                for k in (s..e).step_by(per) {
                    let part = &self.segments[k..(k + per).min(e)];
                    self.entries.push(Entry {
                        seg_start: k as u32,
                        iov_len: part.iter().map(|g| g.iov_len).sum(),
                        ends: part.iter().map(|g| g.ends).sum(),
                        segmented: true,
                    });
                }
            } else {
                for (k, segment) in self.segments[s..e].iter().enumerate() {
                    self.entries.push(Entry {
                        seg_start: (s + k) as u32,
                        iov_len: segment.iov_len,
                        ends: segment.ends,
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
            let first = self.segments[entry.seg_start as usize];
            let addr = &mut self.addrs[first.stream as usize];
            let hdr = &mut self.hdrs[e].msg_hdr;
            hdr.msg_iov = self.iovs[first.iov_start as usize..].as_mut_ptr();
            hdr.msg_iovlen = entry.iov_len as _;
            hdr.msg_name = ptr::from_mut(&mut addr.storage).cast();
            hdr.msg_namelen = addr.len;
            #[cfg(target_os = "linux")]
            if entry.segmented {
                self.controls[e].size = self.segment_size as u16;
                hdr.msg_control = ptr::from_mut(&mut self.controls[e]).cast();
                hdr.msg_controllen = SEGMENT_CONTROL_SPACE;
            }
        }
    }

    /// Records that ended inside the first `entries` entries.
    #[inline]
    fn records_in(&self, entries: usize) -> usize {
        self.entries[..entries].iter().map(|e| e.ends as usize).sum()
    }

    /// Sends what was pushed and empties the batch. Returns how many records
    /// the kernel accepted whole; `WouldBlock` only when it accepted none. A
    /// record cut off by a refused entry counts as not sent even though its
    /// head went out: the receiver drops that head.
    pub(crate) fn send(&mut self, fd: RawFd) -> io::Result<usize> {
        self.layout();
        let result = self.send_segments(fd);
        self.records.clear();
        self.addrs.clear();
        result
    }

    #[cfg(target_os = "linux")]
    fn send_segments(&mut self, fd: RawFd) -> io::Result<usize> {
        self.build_entries(0, self.gso);
        self.stage();
        let sent = sendmmsg(fd, &mut self.hdrs);
        let err = (sent < 0).then(io::Error::last_os_error);
        // The kernel stops at the first entry it refuses. UDP sends are
        // atomic, so every accepted entry delivered all its segments.
        let accepted = usize::try_from(sent).unwrap_or(0);
        let records = self.records_in(accepted);
        // Route MTU, SG support, checksum offload and memory pressure all
        // refuse a segmented entry; a full socket buffer refuses plain
        // datagrams just the same.
        let retry = self.entries.get(accepted).is_some_and(|entry| entry.segmented) &&
            err.as_ref().is_none_or(|err| err.kind() != io::ErrorKind::WouldBlock);
        if !retry {
            return err.map_or(Ok(records), Err);
        }
        // Retry the rest as plain datagrams, which reports the same errno if
        // it persists.
        let resume = self.entries[accepted].seg_start as usize;
        self.build_entries(resume, false);
        self.stage();
        match sendmmsg(fd, &mut self.hdrs) {
            sent @ 0.. => Ok(records + self.records_in(sent as usize)),
            _ if records != 0 => Ok(records),
            _ => Err(io::Error::last_os_error()),
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn send_segments(&mut self, fd: RawFd) -> io::Result<usize> {
        self.build_entries(0, false);
        self.stage();
        let mut records = 0;
        for (i, entry) in self.entries.iter().enumerate() {
            let r = unsafe { libc::sendmsg(fd, &self.hdrs[i].msg_hdr, libc::MSG_DONTWAIT) };
            if r < 0 {
                let err = io::Error::last_os_error();
                return if i == 0 { Err(err) } else { Ok(records) };
            }
            records += entry.ends as usize;
        }
        Ok(records)
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

    /// `records` laid back to back, and where each one starts.
    fn stream(records: &[(&[u8], &[u8])]) -> (Vec<u8>, Vec<usize>) {
        let mut bytes = Vec::new();
        let mut starts = Vec::new();
        for (header, payload) in records {
            starts.push(bytes.len());
            bytes.extend_from_slice(header);
            bytes.extend_from_slice(payload);
        }
        (bytes, starts)
    }

    /// `first` of the packet carrying stream bytes `from..to`.
    fn expected_first(starts: &[usize], from: usize, to: usize) -> usize {
        let p = starts.iter().copied().find(|&p| p >= from && p < to).unwrap_or(to);
        PACKET_HEADER_SIZE + p - from
    }

    /// `count` datagrams with consecutive packet sequences, as `(first, body)`.
    fn recv_packets(receiver: &UdpSocket, count: usize) -> Vec<(usize, Vec<u8>)> {
        let mut buf = [0; 2048];
        let mut out = Vec::new();
        let mut seq = None;
        for _ in 0..count {
            let n = receiver.recv(&mut buf).unwrap();
            let packet = Packet::decode(&buf[..n]).expect("packet header");
            assert_eq!(packet.seq, *seq.get_or_insert(packet.seq));
            seq = Some(packet.seq + 1);
            out.push((usize::from(packet.first), buf[PACKET_HEADER_SIZE..n].to_vec()));
        }
        out
    }

    #[test]
    fn packets_cut_the_record_stream() {
        let (sender, receiver, to) = pair();
        let mut batch = SendBatch::new(1200);
        #[cfg(target_os = "linux")]
        offload_available(batch.enable_gso(sender.as_raw_fd()));
        let headers: Vec<[u8; 29]> = (0..50).map(|i| [i as u8; 29]).collect();
        let payloads: Vec<Vec<u8>> = (0..50).map(|i| vec![0x80 | i as u8; 64 + i % 37]).collect();
        for (header, payload) in headers.iter().zip(&payloads) {
            batch.push(header, payload, &to);
        }
        assert_eq!(batch.send(sender.as_raw_fd()).unwrap(), 50);
        let records: Vec<(&[u8], &[u8])> =
            headers.iter().zip(&payloads).map(|(h, p)| (&h[..], &p[..])).collect();
        let (bytes, starts) = stream(&records);
        let room = 1200 - PACKET_HEADER_SIZE;
        let packets = recv_packets(&receiver, bytes.len().div_ceil(room));
        assert!(packets.len() >= GSO_MIN_SEGMENTS);
        for (k, (first, body)) in packets.iter().enumerate() {
            let from = k * room;
            let to = (from + room).min(bytes.len());
            assert_eq!(body, &bytes[from..to], "packet {k}");
            assert_eq!(*first, expected_first(&starts, from, to), "packet {k}");
        }
    }

    #[test]
    fn full_records_travel_bare() {
        let (sender, receiver, to) = pair();
        let mut batch = SendBatch::new(1200);
        let full = vec![4; 1200 - 29];
        batch.push(&[1; 29], &[2; 10], &to);
        batch.push(&[3; 29], &full, &to);
        batch.push(&[5; 29], &[6; 10], &to);
        assert_eq!(batch.send(sender.as_raw_fd()).unwrap(), 3);
        let mut buf = [0; 2048];
        let (first, body) = recv_packets(&receiver, 1).pop().unwrap();
        assert_eq!((first, body.len()), (PACKET_HEADER_SIZE, 39));
        let n = receiver.recv(&mut buf).unwrap();
        assert_eq!(n, 1200);
        assert_eq!(&buf[..29], &[3; 29]);
        assert_eq!(&buf[29..n], &full[..]);
        let (first, body) = recv_packets(&receiver, 1).pop().unwrap();
        assert_eq!((first, body.len()), (PACKET_HEADER_SIZE, 39));
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
        // Six 13-byte records over 23-byte packet bodies: four packets.
        let mut batch = SendBatch::new(32);
        if !offload_available(batch.enable_gso(sender.as_raw_fd())) {
            return;
        }
        for _ in 0..6 {
            batch.push(b"header", b"payload", &to);
        }
        assert_eq!(batch.send(sender.as_raw_fd()).unwrap(), 6);
        // Per-socket EIO is not a kernel capability; offload stays enabled.
        assert!(batch.gso);
        let packets = recv_packets(&receiver, 4);
        let body: Vec<u8> = packets.into_iter().flat_map(|(_, body)| body).collect();
        assert_eq!(body, b"headerpayload".repeat(6));
    }

    #[test]
    fn mixed_batch_preserves_destinations_and_lengths() {
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receivers =
            [UdpSocket::bind("127.0.0.1:0").unwrap(), UdpSocket::bind("127.0.0.1:0").unwrap()];
        for receiver in &receivers {
            receiver.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        }
        let to = receivers.each_ref().map(|r| SockAddr::new(r.local_addr().unwrap()));
        let mut batch = SendBatch::new(1200);
        #[cfg(target_os = "linux")]
        offload_available(batch.enable_gso(sender.as_raw_fd()));
        let payloads: [&[u8]; 4] = [b"a", b"bbbb", b"cc", b"ddd"];
        for (i, payload) in payloads.iter().enumerate() {
            batch.push(b"h", payload, &to[i % 2]);
        }
        assert_eq!(batch.send(sender.as_raw_fd()).unwrap(), 4);
        for (i, payload) in payloads.iter().enumerate() {
            let (first, body) = recv_packets(&receivers[i % 2], 1).pop().unwrap();
            assert_eq!(first, PACKET_HEADER_SIZE);
            assert_eq!(body[0], b'h');
            assert_eq!(&body[1..], *payload);
        }
        for payload in &payloads {
            batch.push(b"h", payload, &to[0]);
        }
        assert_eq!(batch.send(sender.as_raw_fd()).unwrap(), 4);
        let (_, body) = recv_packets(&receivers[0], 1).pop().unwrap();
        assert_eq!(body, b"hahbbbbhcchddd");
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
            // Three bare full fragments and a short tail packed into a
            // packet: one GSO run, the last segment shorter.
            let headers = [[1; 29], [2; 29], [3; 29], [4; 29]];
            let payload = [0x5a; 1200 - 29];
            let len = |i: usize| if i == 3 { 17 } else { payload.len() };
            for (i, header) in headers.iter().enumerate() {
                tx.push(header, &payload[..len(i)], &to);
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
                    let packet = Packet::decode(bytes);
                    assert_eq!(packet.is_some(), seen == 3);
                    if let Some(packet) = packet {
                        assert_eq!(usize::from(packet.first), PACKET_HEADER_SIZE);
                    }
                    let body = &bytes[if packet.is_some() { PACKET_HEADER_SIZE } else { 0 }..];
                    assert_eq!(&body[..29], &headers[seen]);
                    assert_eq!(&body[29..], &payload[..len(seen)]);
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
