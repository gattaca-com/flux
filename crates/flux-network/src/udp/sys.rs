//! Batched datagram syscalls. Linux uses `sendmmsg`/`recvmmsg`; elsewhere the
//! same API loops over `sendmsg`/`recvmsg`. Both are non-blocking and send or
//! receive whatever is available right now: there is no accumulation delay.

use std::{
    io, mem,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    os::fd::RawFd,
    ptr,
};

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
            gso: true,
        }
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
        // A single GSO send is atomic: its result still counts wire datagrams.
        // Mixed destinations and sizes retain sendmmsg's accepted-prefix path.
        if self.gso && n >= 4 {
            let size = self.iovs[0][0].iov_len + self.iovs[0][1].iov_len;
            let addr = SockAddr::decode(&self.addrs[0].storage, self.addrs[0].len);
            let can_segment = size != 0 &&
                size * n <= 65_507 &&
                (1..n).all(|i| {
                    let len = self.iovs[i][0].iov_len + self.iovs[i][1].iov_len;
                    (len == size || (i == n - 1 && len != 0 && len < size)) &&
                        SockAddr::decode(&self.addrs[i].storage, self.addrs[i].len) == addr
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
                    return Ok(n);
                }
                let err = io::Error::last_os_error();
                if !matches!(
                    err.raw_os_error(),
                    Some(
                        libc::EINVAL |
                            libc::EIO |
                            libc::ENOPROTOOPT |
                            libc::EOPNOTSUPP |
                            libc::EMSGSIZE
                    )
                ) {
                    return Err(err);
                }
                // Retry this untouched batch normally, and stop probing GSO.
                self.gso = false;
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

/// Up to [`BATCH`] incoming datagrams with their source addresses.
pub(crate) struct RecvBatch {
    bufs: Vec<u8>,
    stride: usize,
    addrs: [libc::sockaddr_storage; BATCH],
    iovs: [libc::iovec; BATCH],
    hdrs: [MMsgHdr; BATCH],
    len: usize,
}

// SAFETY: as for `SendBatch`; every pointer is re-established inside `recv`.
unsafe impl Send for RecvBatch {}

impl RecvBatch {
    /// `datagram_size` bounds each datagram; longer ones are flagged truncated
    /// and dropped by [`Self::datagram`].
    pub(crate) fn new(datagram_size: usize) -> Self {
        Self {
            bufs: vec![0; BATCH * datagram_size],
            stride: datagram_size,
            addrs: unsafe { mem::zeroed() },
            iovs: unsafe { mem::zeroed() },
            hdrs: unsafe { mem::zeroed() },
            len: 0,
        }
    }

    /// Receives whatever is queued, up to [`BATCH`] datagrams.
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

    /// Datagram `i` of the last receive. `None` if it was truncated or its
    /// source address is not IP.
    pub(crate) fn datagram(&self, i: usize) -> Option<(&[u8], SocketAddr)> {
        debug_assert!(i < self.len);
        let hdr = &self.hdrs[i];
        if hdr.msg_hdr.msg_flags & libc::MSG_TRUNC != 0 {
            return None;
        }
        let from = SockAddr::decode(&self.addrs[i], hdr.msg_hdr.msg_namelen)?;
        let start = i * self.stride;
        Some((&self.bufs[start..start + hdr.msg_len as usize], from))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn segmented_batch_preserves_datagrams() {
        use std::{net::UdpSocket, os::fd::AsRawFd, time::Duration};

        for bind in ["127.0.0.1:0", "[::1]:0"] {
            let sender = UdpSocket::bind(bind).unwrap();
            let receiver = UdpSocket::bind(bind).unwrap();
            receiver.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
            let to = SockAddr::new(receiver.local_addr().unwrap());
            let mut batch = SendBatch::new();
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
        use std::{net::UdpSocket, os::fd::AsRawFd, time::Duration};

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
        for _ in 0..4 {
            batch.push(b"header", b"payload", &to);
        }
        assert_eq!(batch.send(sender.as_raw_fd()).unwrap(), 4);
        assert!(!batch.gso);
        let mut buf = [0; 32];
        for _ in 0..4 {
            let n = receiver.recv(&mut buf).unwrap();
            assert_eq!(&buf[..n], b"headerpayload");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mixed_batch_preserves_destinations_and_lengths() {
        use std::{net::UdpSocket, os::fd::AsRawFd, time::Duration};

        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receivers =
            [UdpSocket::bind("127.0.0.1:0").unwrap(), UdpSocket::bind("127.0.0.1:0").unwrap()];
        for receiver in &receivers {
            receiver.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        }
        let to = receivers.each_ref().map(|r| SockAddr::new(r.local_addr().unwrap()));
        let mut batch = SendBatch::new();
        let payloads: [&[u8]; 4] = [b"a", b"bbbb", b"cc", b"ddd"];
        for (i, payload) in payloads.iter().enumerate() {
            batch.push(b"h", payload, &to[i % 2]);
        }
        assert_eq!(batch.send(sender.as_raw_fd()).unwrap(), 4);
        let mut buf = [0; 32];
        for (i, payload) in payloads.iter().enumerate() {
            let n = receivers[i % 2].recv(&mut buf).unwrap();
            assert_eq!(buf[0], b'h');
            assert_eq!(&buf[1..n], *payload);
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
