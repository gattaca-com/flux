//! Datagram I/O with an optional Linux completion backend.
//! Batched syscalls use `sendmmsg`/`recvmmsg` on Linux; elsewhere the
//! same API loops over `sendmsg`/`recvmsg`. Both are non-blocking and send or
//! receive whatever is available right now: there is no accumulation delay.

use std::{
    io, mem,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    os::fd::RawFd,
    ptr, slice,
};

#[cfg(target_os = "linux")]
pub(crate) mod uring;

/// Datagram socket and its optional completion backend.
pub(crate) struct UdpSocket {
    #[cfg(target_os = "linux")]
    // Retire kernel requests before closing the socket below.
    pub(crate) ring: Option<std::cell::RefCell<uring::Ring>>,
    socket: mio::net::UdpSocket,
}

impl UdpSocket {
    pub(crate) fn bind(addr: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            socket: mio::net::UdpSocket::bind(addr)?,
            #[cfg(target_os = "linux")]
            ring: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Panics unless the socket was opened with [`crate::udp::UdpIo::Uring`].
    #[cfg(target_os = "linux")]
    pub(crate) fn ring(&self) -> std::cell::RefMut<'_, uring::Ring> {
        self.ring.as_ref().expect("io_uring socket").borrow_mut()
    }

    pub(crate) fn send_to(&self, bytes: &[u8], addr: SocketAddr) -> io::Result<usize> {
        #[cfg(target_os = "linux")]
        if let Some(ring) = &self.ring {
            return ring.borrow_mut().send(bytes, SockAddr::new(addr));
        }
        self.socket.send_to(bytes, addr)
    }

    pub(crate) fn send_batch(&self, batch: &mut SendBatch) -> io::Result<usize> {
        #[cfg(target_os = "linux")]
        if let Some(ring) = &self.ring {
            return ring.borrow_mut().send_batch(batch);
        }
        batch.send(std::os::fd::AsRawFd::as_raw_fd(self))
    }
}

impl std::os::fd::AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.socket.as_raw_fd()
    }
}

impl mio::event::Source for UdpSocket {
    fn register(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        if self.ring.is_some() {
            return Ok(());
        }
        self.socket.register(registry, token, interests)
    }
    fn reregister(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        if self.ring.is_some() {
            return Ok(());
        }
        self.socket.reregister(registry, token, interests)
    }
    fn deregister(&mut self, registry: &mio::Registry) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        if self.ring.is_some() {
            return Ok(());
        }
        self.socket.deregister(registry)
    }
}

/// Datagrams per syscall.
pub(crate) const BATCH: usize = 32;

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
    #[cfg(target_os = "linux")]
    #[inline]
    fn bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(ptr::from_ref(&self.storage).cast(), self.len as usize) }
    }
}

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
const SEGMENT_CONTROL_SPACE: usize =
    unsafe { libc::CMSG_SPACE(mem::size_of::<u16>() as _) as usize };

#[cfg(target_os = "linux")]
#[repr(C)]
struct SegmentControl {
    header: libc::cmsghdr,
    size: u16,
    padding: [u8; SEGMENT_CONTROL_SPACE - mem::size_of::<libc::cmsghdr>() - 2],
}

/// Up to [`BATCH`] outgoing datagrams, each a header plus a payload slice.
///
/// Destinations are copied in; the slices passed to [`Self::push`] must stay
/// alive and unmodified until [`Self::send`] returns.
pub(crate) struct SendBatch {
    hdrs: [MMsgHdr; BATCH],
    iovs: [[libc::iovec; 2]; BATCH],
    addrs: [SockAddr; BATCH],
    len: usize,
    /// Kernel accepts `UDP_SEGMENT`; see [`Self::enable_gso`].
    #[cfg(target_os = "linux")]
    gso: bool,
}

// SAFETY: the raw pointers inside are written right before each syscall and
// only read by it; nothing is shared or dereferenced across threads.
unsafe impl Send for SendBatch {}

impl SendBatch {
    pub(crate) fn new() -> Self {
        Self {
            hdrs: unsafe { mem::zeroed() },
            iovs: unsafe { mem::zeroed() },
            addrs: [SockAddr { storage: unsafe { mem::zeroed() }, len: 0 }; BATCH],
            len: 0,
            #[cfg(target_os = "linux")]
            gso: false,
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
        let i = self.len;
        self.iovs[i] = [iovec(header), iovec(payload)];
        self.addrs[i] = *to;
        self.hdrs[i].msg_hdr.msg_iovlen = 2;
        self.len += 1;
    }

    /// Sends what was pushed and empties the batch. Returns how many datagrams
    /// the kernel accepted; `WouldBlock` only when it accepted none.
    pub(crate) fn send(&mut self, fd: RawFd) -> io::Result<usize> {
        let n = mem::take(&mut self.len);
        #[cfg(target_os = "linux")]
        if self.gso && n >= 4 {
            let size = self.iovs[0][0].iov_len + self.iovs[0][1].iov_len;
            let can_segment = size != 0 &&
                size * n <= super::wire::MAX_DATAGRAM_SIZE &&
                (1..n).all(|i| {
                    let len = self.iovs[i][0].iov_len + self.iovs[i][1].iov_len;
                    (len == size || (i == n - 1 && len != 0 && len < size)) &&
                        self.addrs[i] == self.addrs[0]
                });
            if can_segment {
                let mut control = SegmentControl {
                    header: libc::cmsghdr {
                        cmsg_len: unsafe { libc::CMSG_LEN(mem::size_of::<u16>() as _) as usize },
                        cmsg_level: libc::SOL_UDP,
                        cmsg_type: libc::UDP_SEGMENT,
                    },
                    size: size as u16,
                    padding: [0; SEGMENT_CONTROL_SPACE - mem::size_of::<libc::cmsghdr>() - 2],
                };
                let mut hdr: libc::msghdr = unsafe { mem::zeroed() };
                hdr.msg_iov = self.iovs.as_mut_ptr().cast();
                hdr.msg_iovlen = 2 * n;
                hdr.msg_name = ptr::from_mut(&mut self.addrs[0].storage).cast();
                hdr.msg_namelen = self.addrs[0].len;
                hdr.msg_control = ptr::from_mut(&mut control).cast();
                hdr.msg_controllen = SEGMENT_CONTROL_SPACE;
                let sent = unsafe { libc::sendmsg(fd, ptr::from_ref(&hdr), libc::MSG_DONTWAIT) };
                if sent >= 0 {
                    // UDP sends are atomic; report wire datagrams, not GSO packets.
                    return Ok(n);
                }
                // Route MTU, SG support, checksum offload and memory pressure
                // all fail the whole GSO send; sendmmsg keeps per-datagram
                // accounting and reports the same errno if it persists.
            }
        }
        for i in 0..n {
            let hdr = &mut self.hdrs[i].msg_hdr;
            hdr.msg_iov = self.iovs[i].as_mut_ptr();
            hdr.msg_name = ptr::from_mut(&mut self.addrs[i].storage).cast();
            hdr.msg_namelen = self.addrs[i].len;
        }
        #[cfg(target_os = "linux")]
        {
            let sent = unsafe {
                libc::sendmmsg(fd, self.hdrs.as_mut_ptr(), n as libc::c_uint, libc::MSG_DONTWAIT)
            };
            if sent < 0 { Err(io::Error::last_os_error()) } else { Ok(sent as usize) }
        }
        #[cfg(not(target_os = "linux"))]
        {
            for i in 0..n {
                let r = unsafe { libc::sendmsg(fd, &self.hdrs[i].msg_hdr, libc::MSG_DONTWAIT) };
                if r < 0 {
                    let err = io::Error::last_os_error();
                    return if i == 0 { Err(err) } else { Ok(i) };
                }
            }
            Ok(n)
        }
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
        let start = i * self.stride;
        decode_datagrams(
            &self.hdrs[i],
            &self.addrs[i],
            #[cfg(target_os = "linux")]
            &self.controls[i],
            &self.bufs[start..start + self.stride],
            self.datagram_size,
        )
    }
}

fn decode_datagrams<'a>(
    hdr: &MMsgHdr,
    addr: &libc::sockaddr_storage,
    #[cfg(target_os = "linux")] control: &GroControl,
    bytes: &'a [u8],
    datagram_size: usize,
) -> Option<(std::slice::Chunks<'a, u8>, SocketAddr)> {
    if hdr.msg_hdr.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
        return None;
    }
    let len = hdr.msg_len as usize;
    #[cfg(not(target_os = "linux"))]
    let segment_size = len;
    #[cfg(target_os = "linux")]
    let segment_size = if hdr.msg_hdr.msg_controllen != 0 {
        let control_len = unsafe { libc::CMSG_LEN(mem::size_of::<libc::c_int>() as _) as usize };
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
    if segment_size == 0 || segment_size > datagram_size {
        return None;
    }
    let from = SockAddr::decode(addr, hdr.msg_hdr.msg_namelen)?;
    Some((bytes[..len].chunks(segment_size), from))
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
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

    #[cfg(target_os = "linux")]
    #[test]
    fn segmented_batch_preserves_datagrams() {
        for bind in ["127.0.0.1:0", "[::1]:0"] {
            // Hosts without IPv6 loopback only run the v4 case.
            let sender = match UdpSocket::bind(bind) {
                Ok(socket) => socket,
                Err(_) if bind.starts_with('[') => continue,
                Err(err) => panic!("IPv4 bind failed: {err}"),
            };
            let receiver = UdpSocket::bind(bind).unwrap();
            receiver.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
            let to = SockAddr::new(receiver.local_addr().unwrap());
            let mut batch = SendBatch::new();
            offload_available(batch.enable_gso(sender.as_raw_fd()));
            let headers: [[u8; 29]; BATCH] = std::array::from_fn(|i| [i as u8; 29]);
            let payload = [0x5a; 1171];
            for (i, header) in headers.iter().enumerate() {
                let len = if i == BATCH - 1 { 17 } else { payload.len() };
                batch.push(header, &payload[..len], &to);
            }
            assert_eq!(batch.send(sender.as_raw_fd()).unwrap(), BATCH);
            let mut buf = [0; 2048];
            for (i, header) in headers.iter().enumerate() {
                let len = if i == BATCH - 1 { 17 } else { payload.len() };
                let n = receiver.recv(&mut buf).unwrap();
                assert_eq!(n, header.len() + len);
                assert_eq!(&buf[..header.len()], header);
                assert_eq!(&buf[header.len()..n], &payload[..len]);
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn segmentation_falls_back_without_checksums() {
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
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
        let to = SockAddr::new(receiver.local_addr().unwrap());
        let mut batch = SendBatch::new();
        if !offload_available(batch.enable_gso(sender.as_raw_fd())) {
            return;
        }
        for _ in 0..4 {
            batch.push(b"header", b"payload", &to);
        }
        assert_eq!(batch.send(sender.as_raw_fd()).unwrap(), 4);
        // Per-socket EIO is not a kernel capability; offload stays enabled.
        assert!(batch.gso);
        let mut buf = [0; 32];
        for _ in 0..4 {
            let n = receiver.recv(&mut buf).unwrap();
            assert_eq!(&buf[..n], b"headerpayload");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mixed_batch_preserves_destinations_and_lengths() {
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receivers =
            [UdpSocket::bind("127.0.0.1:0").unwrap(), UdpSocket::bind("127.0.0.1:0").unwrap()];
        for receiver in &receivers {
            receiver.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        }
        let to = receivers.each_ref().map(|r| SockAddr::new(r.local_addr().unwrap()));
        for mixed_destinations in [false, true] {
            let mut batch = SendBatch::new();
            offload_available(batch.enable_gso(sender.as_raw_fd()));
            let payloads: [&[u8]; 4] = if mixed_destinations {
                [b"aaa", b"bbb", b"ccc", b"ddd"]
            } else {
                [b"a", b"bbbb", b"cc", b"ddd"]
            };
            let destination = |i: usize| if mixed_destinations { i % 2 } else { 0 };
            for (i, payload) in payloads.iter().enumerate() {
                batch.push(b"h", payload, &to[destination(i)]);
            }
            assert_eq!(batch.send(sender.as_raw_fd()).unwrap(), 4);
            let mut buf = [0; 32];
            for (i, payload) in payloads.iter().enumerate() {
                let n = receivers[destination(i)].recv(&mut buf).unwrap();
                assert_eq!(buf[0], b'h');
                assert_eq!(&buf[1..n], *payload);
            }
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
            let mut tx = SendBatch::new();
            if !offload_available(tx.enable_gso(sender.as_raw_fd())) {
                return;
            }
            let headers = [[1; 29], [2; 29], [3; 29], [4; 29]];
            let payload = [0x5a; 1171];
            for (i, header) in headers.iter().enumerate() {
                tx.push(header, &payload[..if i == 3 { 17 } else { 1171 }], &to);
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
                    assert_eq!(&bytes[..29], &headers[seen]);
                    assert_eq!(&bytes[29..], &payload[..if seen == 3 { 17 } else { 1171 }]);
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
