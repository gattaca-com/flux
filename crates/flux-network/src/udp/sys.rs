//! Batched datagram syscalls. Linux uses `sendmmsg`/`recvmmsg`; elsewhere the
//! same API loops over `sendmsg`/`recvmsg`. Both are non-blocking and send or
//! receive whatever is available right now: there is no accumulation delay.
//!
//! The `msghdr` pointers are (re)established immediately before each syscall,
//! so neither batch is self-referential and both may be moved freely.

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

/// Up to [`BATCH`] outgoing datagrams, each a header plus a payload slice.
///
/// Destinations are copied in; the slices passed to [`Self::push`] must stay
/// alive and unmodified until [`Self::send`] returns.
pub(crate) struct SendBatch {
    hdrs: [MMsgHdr; BATCH],
    iovs: [[libc::iovec; 2]; BATCH],
    addrs: [SockAddr; BATCH],
    len: usize,
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

    #[test]
    fn sockaddr_roundtrip() {
        for addr in ["127.0.0.1:4242", "[::1]:9"] {
            let addr: SocketAddr = addr.parse().unwrap();
            let native = SockAddr::new(addr);
            assert_eq!(SockAddr::decode(&native.storage, native.len), Some(addr));
        }
    }

    #[test]
    fn batches_roundtrip_over_loopback() {
        use std::{net::UdpSocket, os::fd::AsRawFd};
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        let tx = UdpSocket::bind("127.0.0.1:0").unwrap();
        rx.set_nonblocking(true).unwrap();
        let to = SockAddr::new(rx.local_addr().unwrap());
        let headers: Vec<[u8; 2]> = (0..5u8).map(|i| [i, i]).collect();
        let payload = b"payload";
        let mut batch = SendBatch::new();
        for h in &headers {
            batch.push(h, payload, &to);
        }
        assert_eq!(batch.send(tx.as_raw_fd()).unwrap(), 5);
        assert_eq!(batch.len, 0);

        let mut recv = RecvBatch::new(64);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut got = Vec::new();
        while got.len() < 5 && std::time::Instant::now() < deadline {
            let Ok(n) = recv.recv(rx.as_raw_fd()) else { continue };
            for i in 0..n {
                let (bytes, from) = recv.datagram(i).unwrap();
                assert_eq!(from, tx.local_addr().unwrap());
                assert_eq!(&bytes[2..], payload);
                got.push(bytes[0]);
            }
        }
        got.sort_unstable();
        assert_eq!(got, [0, 1, 2, 3, 4]);
    }
}
