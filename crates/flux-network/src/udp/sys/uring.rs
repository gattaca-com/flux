//! Bounded completion I/O. Kernel requests own pooled buffers, never peer
//! state.

use std::{collections::VecDeque, io, mem, os::fd::RawFd, ptr};

use io_uring::{IoUring, opcode, types};
use tracing::{debug, warn};

use super::{
    GRO_CONTROL_SPACE, GroControl, MMsgHdr, SEGMENT_CONTROL_SPACE, SegmentControl, SendBatch,
    SockAddr, decode_datagrams, iovec_mut,
};
use crate::udp::UringConfig;

const RX_BIT: u64 = 1 << 63;
const BUFFER_SIZE: usize = 65_535;

const NAME_SIZE: usize = mem::size_of::<libc::sockaddr_storage>();
const RX_SIZE: usize = BUFFER_SIZE + 16 + NAME_SIZE + GRO_CONTROL_SPACE;

struct Rx {
    bytes: Box<[u8]>,
    len: usize,
}

impl Rx {
    fn new() -> Self {
        Self { bytes: vec![0; RX_SIZE].into_boxed_slice(), len: 0 }
    }
}

struct Provided {
    base: std::ptr::NonNull<types::BufRingEntry>,
    layout: std::alloc::Layout,
    tail: u16,
    mask: u16,
}

impl Provided {
    fn new(entries: u16) -> Self {
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        assert!(page > 0);
        let layout = std::alloc::Layout::from_size_align(
            usize::from(entries) * mem::size_of::<types::BufRingEntry>(),
            page as usize,
        )
        .unwrap();
        let base = std::ptr::NonNull::new(unsafe { std::alloc::alloc_zeroed(layout) }.cast())
            .unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        Self { base, layout, tail: 0, mask: entries - 1 }
    }

    fn provide(&mut self, index: usize, bytes: &mut [u8]) {
        // Only consumed entries are reused. Publish the buffer after its
        // descriptor, and do not touch its bytes until the receive CQE.
        unsafe {
            let entry = &mut *self.base.as_ptr().add(usize::from(self.tail & self.mask));
            entry.set_addr(bytes.as_mut_ptr() as u64);
            entry.set_len(bytes.len() as u32);
            entry.set_bid(index as u16);
            self.tail = self.tail.wrapping_add(1);
            let tail = types::BufRingEntry::tail(self.base.as_ptr())
                .cast::<std::sync::atomic::AtomicU16>();
            (*tail).store(self.tail, std::sync::atomic::Ordering::Release);
        }
    }
}

impl Drop for Provided {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.base.as_ptr().cast(), self.layout);
        }
    }
}

fn receive_header() -> libc::msghdr {
    let mut header: libc::msghdr = unsafe { mem::zeroed() };
    header.msg_namelen = NAME_SIZE as _;
    header.msg_controllen = GRO_CONTROL_SPACE;
    header
}

struct Tx {
    header: libc::msghdr,
    addr: SockAddr,
    control: SegmentControl,
    iov: libc::iovec,
    bytes: Box<[u8]>,
    len: usize,
    segment: usize,
    offset: usize,
    fallback: bool,
}

impl Tx {
    fn new() -> Box<Self> {
        Box::new(Self {
            header: unsafe { mem::zeroed() },
            addr: SockAddr::new("0.0.0.0:0".parse().unwrap()),
            control: unsafe { mem::zeroed() },
            iov: unsafe { mem::zeroed() },
            bytes: vec![0; BUFFER_SIZE].into_boxed_slice(),
            len: 0,
            segment: 0,
            offset: 0,
            fallback: false,
        })
    }

    fn prepare(&mut self, fd: RawFd, index: usize) -> io_uring::squeue::Entry {
        let end = if self.fallback { (self.offset + self.segment).min(self.len) } else { self.len };
        self.iov = iovec_mut(&mut self.bytes[self.offset..end]);
        self.header.msg_name = ptr::from_mut(&mut self.addr.storage).cast();
        self.header.msg_namelen = self.addr.len;
        self.header.msg_iov = ptr::from_mut(&mut self.iov);
        self.header.msg_iovlen = 1;
        self.header.msg_control = ptr::null_mut();
        self.header.msg_controllen = 0;
        if self.segment != 0 && !self.fallback {
            self.control.header = libc::cmsghdr {
                cmsg_len: unsafe { libc::CMSG_LEN(mem::size_of::<u16>() as _) as usize },
                cmsg_level: libc::SOL_UDP,
                cmsg_type: libc::UDP_SEGMENT,
            };
            self.control.size = self.segment as u16;
            self.header.msg_control = ptr::from_mut(&mut self.control).cast();
            self.header.msg_controllen = SEGMENT_CONTROL_SPACE;
        }
        opcode::SendMsg::new(types::Fd(fd), ptr::from_ref(&self.header))
            .flags(libc::MSG_DONTWAIT as _)
            .build()
            .user_data(index as u64)
    }
}

pub(crate) struct Received {
    index: usize,
    slot: Rx,
}

impl Received {
    pub(crate) fn datagrams(
        &self,
        max_size: usize,
    ) -> Option<(std::slice::Chunks<'_, u8>, std::net::SocketAddr)> {
        let rx = &self.slot;
        let out = types::RecvMsgOut::parse(&rx.bytes[..rx.len], &receive_header()).ok()?;
        if out.is_name_data_truncated() ||
            out.is_control_data_truncated() ||
            out.is_payload_truncated()
        {
            return None;
        }
        let mut addr: libc::sockaddr_storage = unsafe { mem::zeroed() };
        let mut control: GroControl = unsafe { mem::zeroed() };
        // The parser bounds these slices by the configured metadata capacities.
        unsafe {
            ptr::copy_nonoverlapping(
                out.name_data().as_ptr(),
                ptr::from_mut(&mut addr).cast(),
                out.name_data().len(),
            );
            ptr::copy_nonoverlapping(
                out.control_data().as_ptr(),
                ptr::from_mut(&mut control).cast(),
                out.control_data().len(),
            );
        }
        let mut hdr: MMsgHdr = unsafe { mem::zeroed() };
        hdr.msg_len = out.payload_data().len() as u32;
        hdr.msg_hdr.msg_namelen = out.incoming_name_len();
        hdr.msg_hdr.msg_controllen = out.control_data().len();
        // Return a slice of the owned buffer, independent of the parser's borrow.
        let offset = out.payload_data().as_ptr() as usize - rx.bytes.as_ptr() as usize;
        decode_datagrams(
            &hdr,
            &addr,
            &control,
            &rx.bytes[offset..offset + hdr.msg_len as usize],
            max_size,
        )
    }
}

pub(crate) struct Ring {
    io: IoUring,
    fd: RawFd,
    owner: std::thread::ThreadId,
    // Boxes keep msghdr, iovec and ancillary pointers stable across moves.
    rx: Vec<Option<Rx>>,
    provided: Option<Provided>,
    receive_header: Box<libc::msghdr>,
    rx_active: bool,
    available: usize,
    #[allow(clippy::vec_box)]
    tx: Vec<Box<Tx>>,
    free_tx: Vec<usize>,
    ready: VecDeque<usize>,
    completions: Vec<(u64, i32, u32)>,
}

// SAFETY: requests only access their boxed buffers. Moving the owner does not
// move the buffers; RefCell on the socket excludes concurrent access.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for Ring {}

impl Ring {
    pub(crate) fn new(fd: RawFd, config: UringConfig) -> io::Result<Self> {
        let count = u32::from(config.send_entries) + u32::from(config.recv_entries);
        let ring = IoUring::builder()
            .setup_coop_taskrun()
            .setup_taskrun_flag()
            .build(count.next_power_of_two())?;
        // Require the teardown primitive before posting any borrowed pointers.
        cancel(&ring)?;
        let provided = Provided::new(config.recv_entries);
        // SAFETY: the aligned descriptor ring outlives its registration.
        unsafe {
            ring.submitter().register_buf_ring_with_flags(
                provided.base.as_ptr() as u64,
                config.recv_entries,
                0,
                0,
            )?;
        }
        let mut this = Self {
            io: ring,
            fd,
            owner: std::thread::current().id(),
            rx: (0..config.recv_entries).map(|_| Some(Rx::new())).collect(),
            provided: Some(provided),
            receive_header: Box::new(receive_header()),
            rx_active: false,
            available: config.recv_entries.into(),
            tx: (0..config.send_entries).map(|_| Tx::new()).collect(),
            free_tx: (0..usize::from(config.send_entries)).rev().collect(),
            ready: VecDeque::with_capacity(config.recv_entries.into()),
            completions: Vec::with_capacity(count as usize + 1),
        };
        for i in 0..this.rx.len() {
            this.provided.as_mut().unwrap().provide(i, &mut this.rx[i].as_mut().unwrap().bytes);
        }
        this.arm_receive();
        this.io.submit()?;
        Ok(this)
    }

    fn push(&mut self, entry: &io_uring::squeue::Entry) {
        // At most one SQE per slot. Ring capacity covers every TX and RX slot.
        // SAFETY: slots remain allocated and unchanged until their CQE.
        unsafe {
            self.io.submission().push(entry).expect("UDP operation slots exceed SQ capacity");
        };
    }

    fn arm_receive(&mut self) {
        if self.rx_active || self.available == 0 {
            return;
        }
        let entry =
            opcode::RecvMsgMulti::new(types::Fd(self.fd), ptr::from_ref(&*self.receive_header), 0)
                .build()
                .user_data(RX_BIT);
        self.push(&entry);
        self.rx_active = true;
    }

    fn arm_send(&mut self, index: usize) {
        let entry = self.tx[index].prepare(self.fd, index);
        self.push(&entry);
    }

    /// A successful enqueue owns the bytes, like acceptance into a socket
    /// send buffer. Later errors are packet loss, recovered by the protocol.
    pub(crate) fn send(&mut self, bytes: &[u8], to: SockAddr) -> io::Result<usize> {
        if bytes.len() > BUFFER_SIZE {
            return Err(io::Error::from_raw_os_error(libc::EMSGSIZE));
        }
        let index = self.free_tx.pop().ok_or(io::ErrorKind::WouldBlock)?;
        let tx = &mut self.tx[index];
        tx.bytes[..bytes.len()].copy_from_slice(bytes);
        tx.len = bytes.len();
        tx.addr = to;
        tx.segment = 0;
        tx.offset = 0;
        tx.fallback = false;
        self.arm_send(index);
        Ok(bytes.len())
    }

    pub(crate) fn send_batch(&mut self, batch: &mut SendBatch) -> io::Result<usize> {
        if self.free_tx.is_empty() {
            self.poll();
        }
        let n = mem::take(&mut batch.len);
        let mut accepted = 0;
        while accepted < n {
            let Some(index) = self.free_tx.pop() else { break };
            // A batch may straddle peers or message tails. Segment each
            // compatible run instead of losing GSO for the entire batch.
            let size = batch.iovs[accepted].iter().map(|iov| iov.iov_len).sum::<usize>();
            let mut end = accepted + 1;
            if batch.gso && size != 0 {
                while end < n && batch.addrs[end] == batch.addrs[accepted] {
                    let len = batch.iovs[end].iter().map(|iov| iov.iov_len).sum::<usize>();
                    if len == 0 || len > size || (end - accepted) * size + len > BUFFER_SIZE {
                        break;
                    }
                    end += 1;
                    if len < size {
                        break;
                    }
                }
            }
            let segment = if end > accepted + 1 { size } else { 0 };
            let tx = &mut self.tx[index];
            tx.len = 0;
            tx.addr = batch.addrs[accepted];
            tx.segment = segment;
            tx.offset = 0;
            tx.fallback = false;
            for i in accepted..end {
                for iov in &batch.iovs[i] {
                    // SAFETY: SendBatch's slices remain live throughout this call.
                    let bytes = unsafe {
                        std::slice::from_raw_parts(iov.iov_base.cast::<u8>(), iov.iov_len)
                    };
                    tx.bytes[tx.len..tx.len + bytes.len()].copy_from_slice(bytes);
                    tx.len += bytes.len();
                }
            }
            self.arm_send(index);
            accepted = end;
        }
        if accepted == 0 && n != 0 { Err(io::ErrorKind::WouldBlock.into()) } else { Ok(accepted) }
    }

    pub(crate) fn submit(&mut self) {
        let owner = std::thread::current().id();
        if self.owner != owner {
            // Receive task work belongs to the submitting thread. Retire those
            // requests before a moved driver starts polling on another thread.
            cancel(&self.io).expect("couldn't transfer UDP io_uring to this thread");
            self.owner = owner;
        }
        let needs_enter = {
            let sq = self.io.submission();
            !sq.is_empty() || sq.taskrun()
        };
        if needs_enter {
            if let Err(err) = self.io.submit() {
                debug!(?err, "UDP io_uring submit failed");
            }
        }
    }

    pub(crate) fn poll(&mut self) -> bool {
        self.submit();
        self.completions.clear();
        self.completions
            .extend(self.io.completion().map(|c| (c.user_data(), c.result(), c.flags())));
        for i in 0..self.completions.len() {
            let (id, result, flags) = self.completions[i];
            if id & RX_BIT != 0 {
                if !io_uring::cqueue::more(flags) {
                    self.rx_active = false;
                }
                if let Some(index) = io_uring::cqueue::buffer_select(flags) {
                    let index = usize::from(index);
                    self.available -= 1;
                    if result >= 0 {
                        self.rx[index].as_mut().unwrap().len = result as usize;
                        self.ready.push_back(index);
                    } else {
                        self.provided
                            .as_mut()
                            .unwrap()
                            .provide(index, &mut self.rx[index].as_mut().unwrap().bytes);
                        self.available += 1;
                    }
                }
                if result < 0 && result != -libc::ENOBUFS && result != -libc::ECANCELED {
                    debug!(result, "UDP io_uring receive failed");
                }
            } else {
                let index = id as usize;
                let tx = &mut self.tx[index];
                if result < 0 && tx.segment != 0 && !tx.fallback {
                    // GSO errors reject the whole message. Retry its original
                    // datagrams without offload, retaining the same buffer.
                    tx.fallback = true;
                    self.arm_send(index);
                } else if tx.fallback && result >= 0 && tx.offset + tx.iov.iov_len < tx.len {
                    tx.offset += tx.iov.iov_len;
                    self.arm_send(index);
                } else {
                    if result < 0 {
                        debug!(result, "UDP io_uring send failed");
                    }
                    self.free_tx.push(index);
                }
            }
        }
        self.arm_receive();
        !self.completions.is_empty()
    }

    pub(crate) fn receive(&mut self) -> Option<Received> {
        let index = self.ready.pop_front()?;
        Some(Received { index, slot: self.rx[index].take().unwrap() })
    }

    pub(crate) fn recycle(&mut self, received: Received) {
        let index = received.index;
        self.rx[index] = Some(received.slot);
        self.provided.as_mut().unwrap().provide(index, &mut self.rx[index].as_mut().unwrap().bytes);
        self.available += 1;
        self.arm_receive();
    }
}

fn cancel(ring: &IoUring) -> io::Result<()> {
    loop {
        match ring.submitter().register_sync_cancel(None, types::CancelBuilder::any()) {
            Ok(()) => return Ok(()),
            Err(err) if err.raw_os_error() == Some(libc::ENOENT) => return Ok(()),
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
}

impl Drop for Ring {
    fn drop(&mut self) {
        // Unsubmitted SQEs are never submitted again. Cancel all kernel-owned
        // operations synchronously before releasing their memory or socket.
        match cancel(&self.io) {
            Ok(()) => {
                if let Err(err) = self.io.submitter().unregister_buf_ring(0) {
                    warn!(?err, "UDP buffer ring unregister failed; retaining receive buffers");
                    mem::forget(mem::take(&mut self.rx));
                    mem::forget(self.provided.take());
                }
            }
            Err(err) => {
                // A failed cancellation cannot justify freeing kernel buffers.
                warn!(?err, "UDP io_uring cancellation failed; retaining operation buffers");
                mem::forget(mem::take(&mut self.rx));
                mem::forget(mem::take(&mut self.tx));
                mem::forget(self.provided.take());
                // The multishot request also borrows this header.
                mem::forget(mem::replace(&mut self.receive_header, Box::new(receive_header())));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::UdpSocket,
        os::fd::AsRawFd,
        time::{Duration, Instant},
    };

    use super::*;

    fn config() -> UringConfig {
        UringConfig { send_entries: 2, recv_entries: 2 }
    }

    fn receive(ring: &mut Ring) -> Received {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            ring.poll();
            if let Some(rx) = ring.receive() {
                return rx;
            }
            assert!(Instant::now() < deadline, "receive completion timed out");
        }
    }

    #[test]
    fn owned_sends_backpressure_and_receive_recycling() {
        for bind in ["127.0.0.1:0", "[::1]:0"] {
            let socket = UdpSocket::bind(bind).unwrap();
            let remote = UdpSocket::bind(bind).unwrap();
            remote.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let mut ring = Ring::new(socket.as_raw_fd(), config()).unwrap();
            let to = SockAddr::new(remote.local_addr().unwrap());
            let mut payload = vec![1; 100];
            assert_eq!(ring.send(&payload, to).unwrap(), 100);
            payload.fill(2);
            assert_eq!(ring.send(&payload, to).unwrap(), 100);
            payload.fill(3);
            assert_eq!(ring.send(&payload, to).unwrap_err().kind(), io::ErrorKind::WouldBlock);
            ring.poll();
            let mut buf = [0; 1200];
            let mut seen = Vec::new();
            for _ in 0..2 {
                let n = remote.recv(&mut buf).unwrap();
                assert_eq!(n, 100);
                assert!(buf[..n].iter().all(|b| *b == buf[0]));
                seen.push(buf[0]);
            }
            seen.sort_unstable();
            assert_eq!(seen, [1, 2]);
            for i in 0..16 {
                remote.send_to(&[i; 10], socket.local_addr().unwrap()).unwrap();
                let rx = receive(&mut ring);
                let (mut datagrams, from) = rx.datagrams(1200).unwrap();
                assert_eq!(from, remote.local_addr().unwrap());
                assert_eq!(datagrams.next().unwrap(), &[i; 10]);
                assert!(datagrams.next().is_none());
                ring.recycle(rx);
            }
            // Drop with receives both submitted and queued for rearming.
            drop(ring);
            drop(socket);
        }
    }

    #[test]
    fn gso_and_fallback_preserve_boundaries() {
        for fallback in [false, true] {
            let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
            let remote = UdpSocket::bind("127.0.0.1:0").unwrap();
            remote.set_nonblocking(true).unwrap();
            if fallback {
                let enabled: libc::c_int = 1;
                assert_eq!(
                    unsafe {
                        libc::setsockopt(
                            socket.as_raw_fd(),
                            libc::SOL_SOCKET,
                            libc::SO_NO_CHECK,
                            ptr::from_ref(&enabled).cast(),
                            mem::size_of_val(&enabled) as _,
                        )
                    },
                    0
                );
            }
            let mut ring = Ring::new(socket.as_raw_fd(), config()).unwrap();
            let mut batch = SendBatch::new();
            batch.enable_gso(socket.as_raw_fd()).unwrap();
            let to = SockAddr::new(remote.local_addr().unwrap());
            let headers = [[1; 29], [2; 29], [3; 29], [4; 29]];
            let payload = [0x5a; 1171];
            for (i, header) in headers.iter().enumerate() {
                batch.push(header, &payload[..if i == 3 { 17 } else { 1171 }], &to);
            }
            assert_eq!(ring.send_batch(&mut batch).unwrap(), 4);
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut seen = [false; 4];
            let mut count = 0;
            let mut buf = [0; 2048];
            while count < 4 {
                ring.poll();
                if let Ok(n) = remote.recv(&mut buf) {
                    let i = usize::from(buf[0] - 1);
                    assert!(!seen[i]);
                    seen[i] = true;
                    assert_eq!(&buf[..29], &headers[i]);
                    assert_eq!(&buf[29..n], &payload[..if i == 3 { 17 } else { 1171 }]);
                    count += 1;
                }
                assert!(Instant::now() < deadline, "GSO/fallback timed out");
            }
        }
    }

    #[test]
    fn exhausted_receive_pool_rearms_and_tail_wraps() {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = UdpSocket::bind("127.0.0.1:0").unwrap();
        let mut ring = Ring::new(socket.as_raw_fd(), config()).unwrap();
        for i in 0..8 {
            remote.send_to(&[i], socket.local_addr().unwrap()).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while ring.rx_active || ring.available != 0 {
            ring.poll();
            assert!(Instant::now() < deadline, "buffer exhaustion CQE missing");
        }
        let mut seen = [false; 8];
        for _ in 0..8 {
            let rx = receive(&mut ring);
            let (mut packets, _) = rx.datagrams(1200).unwrap();
            let i = usize::from(packets.next().unwrap()[0]);
            assert!(!seen[i]);
            seen[i] = true;
            ring.recycle(rx);
        }
        assert!(seen.into_iter().all(|v| v));
        for _ in 0..65_536 {
            remote.send_to(b"wrap", socket.local_addr().unwrap()).unwrap();
            let rx = receive(&mut ring);
            assert_eq!(rx.datagrams(1200).unwrap().0.next().unwrap(), b"wrap");
            ring.recycle(rx);
        }
    }

    #[test]
    fn driver_moves_after_submitting_receives() {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = UdpSocket::bind("127.0.0.1:0").unwrap();
        let mut ring = Ring::new(socket.as_raw_fd(), config()).unwrap();
        // The original issuer exits while the socket and ring move onward.
        let (mut ring, socket) = std::thread::spawn(move || {
            ring.poll();
            (ring, socket)
        })
        .join()
        .unwrap();
        remote.send_to(b"moved", socket.local_addr().unwrap()).unwrap();
        let rx = receive(&mut ring);
        assert_eq!(rx.datagrams(1200).unwrap().0.next().unwrap(), b"moved");
        ring.recycle(rx);
    }

    #[test]
    fn mixed_gso_runs_keep_their_destinations_and_lengths() {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let peers =
            [UdpSocket::bind("127.0.0.1:0").unwrap(), UdpSocket::bind("127.0.0.1:0").unwrap()];
        let to = peers.each_ref().map(|p| SockAddr::new(p.local_addr().unwrap()));
        for p in &peers {
            p.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        }
        let mut ring =
            Ring::new(socket.as_raw_fd(), UringConfig { send_entries: 8, recv_entries: 2 })
                .unwrap();
        let mut batch = SendBatch::new();
        batch.enable_gso(socket.as_raw_fd()).unwrap();
        let headers: [[u8; 29]; 8] = std::array::from_fn(|i| [i as u8; 29]);
        let payload = [0x5a; 1171];
        let lengths = [1171, 17, 1171, 1171, 1171, 3, 1171, 1171];
        for i in 0..8 {
            batch.push(&headers[i], &payload[..lengths[i]], &to[i / 4]);
        }
        assert_eq!(ring.send_batch(&mut batch).unwrap(), 8);
        ring.poll();
        let mut seen = [false; 8];
        for (peer_index, peer) in peers.iter().enumerate() {
            for _ in 0..4 {
                let mut buf = [0; 2048];
                let len = peer.recv(&mut buf).unwrap();
                let i = usize::from(buf[0]);
                assert_eq!(i / 4, peer_index);
                assert!(!seen[i]);
                seen[i] = true;
                assert_eq!(&buf[..29], &headers[i]);
                assert_eq!(&buf[29..len], &payload[..lengths[i]]);
            }
        }
    }

    #[test]
    fn gro_and_oversized_receives() {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = UdpSocket::bind("127.0.0.1:0").unwrap();
        super::super::RecvBatch::enable_gro(socket.as_raw_fd()).unwrap();
        let mut ring = Ring::new(socket.as_raw_fd(), config()).unwrap();
        let mut batch = SendBatch::new();
        batch.enable_gso(remote.as_raw_fd()).unwrap();
        let to = SockAddr::new(socket.local_addr().unwrap());
        for _ in 0..4 {
            batch.push(&[1; 29], &[2; 1171], &to);
        }
        assert_eq!(batch.send(remote.as_raw_fd()).unwrap(), 4);
        let rx = receive(&mut ring);
        let (datagrams, _) = rx.datagrams(1200).unwrap();
        assert_eq!(datagrams.count(), 4);
        ring.recycle(rx);
        remote.send_to(&[0; 1201], socket.local_addr().unwrap()).unwrap();
        let rx = receive(&mut ring);
        assert!(rx.datagrams(1200).is_none());
        ring.recycle(rx);
    }
}
