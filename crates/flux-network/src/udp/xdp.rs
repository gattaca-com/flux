use std::{
    io,
    mem::align_of,
    net::{Ipv4Addr, SocketAddrV4},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    ptr::NonNull,
    sync::atomic::{AtomicU32, Ordering},
};

use super::{
    UdpMulticastConfig, UdpXdpConfig, UdpXdpMode,
    wire::{FragmentHeader, UDP_HEADER_SIZE, encode_fragments},
};

const XDP_MMAP_OFFSETS: libc::c_int = 1;
const XDP_TX_RING: libc::c_int = 3;
const XDP_UMEM_REG: libc::c_int = 4;
const XDP_UMEM_FILL_RING: libc::c_int = 5;
const XDP_UMEM_COMPLETION_RING: libc::c_int = 6;

const XDP_COPY: u16 = 1 << 1;
const XDP_ZEROCOPY: u16 = 1 << 2;
const XDP_USE_NEED_WAKEUP: u16 = 1 << 3;
const XDP_RING_NEED_WAKEUP: u32 = 1;
const XDP_FLAGS_DRV_MODE: u32 = 1 << 2;

const BPF_PROG_LOAD: libc::c_uint = 5;
const BPF_LINK_CREATE: libc::c_uint = 28;
const BPF_PROG_TYPE_XDP: u32 = 6;
const BPF_XDP: u32 = 37;
const XDP_PASS: i32 = 2;

const XDP_PGOFF_TX_RING: libc::off_t = 0x8000_0000;
const XDP_UMEM_PGOFF_FILL_RING: libc::off_t = 0x1_0000_0000;
const XDP_UMEM_PGOFF_COMPLETION_RING: libc::off_t = 0x1_8000_0000;

const ETHERTYPE_IPV4: u16 = 0x0800;
const ETHERTYPE_VLAN: u16 = 0x8100;
const IPV4_HEADER_SIZE: usize = 20;
const UDP_HEADER_SIZE_BYTES: usize = 8;

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct SockAddrXdp {
    family: u16,
    flags: u16,
    interface_index: u32,
    queue_id: u32,
    shared_umem_fd: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct XdpUmemReg {
    addr: u64,
    len: u64,
    chunk_size: u32,
    headroom: u32,
    flags: u32,
    tx_metadata_len: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct XdpRingOffset {
    producer: u64,
    consumer: u64,
    desc: u64,
    flags: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct XdpMmapOffsets {
    rx: XdpRingOffset,
    tx: XdpRingOffset,
    fill: XdpRingOffset,
    completion: XdpRingOffset,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct XdpDesc {
    addr: u64,
    len: u32,
    options: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct BpfInsn {
    code: u8,
    registers: u8,
    offset: i16,
    immediate: i32,
}

#[repr(C, align(8))]
struct BpfProgLoadAttr {
    prog_type: u32,
    insn_count: u32,
    instructions: u64,
    license: u64,
    log_level: u32,
    log_size: u32,
    log_buffer: u64,
    kernel_version: u32,
    prog_flags: u32,
    prog_name: [u8; 16],
    prog_ifindex: u32,
    expected_attach_type: u32,
    padding: [u8; 80],
}

#[repr(C, align(8))]
struct BpfLinkCreateAttr {
    prog_fd: u32,
    target_ifindex: u32,
    attach_type: u32,
    flags: u32,
}

struct MmapRegion {
    ptr: NonNull<u8>,
    len: usize,
}

impl MmapRegion {
    fn anonymous(len: usize) -> io::Result<Self> {
        let ptr = unsafe {
            libc::mmap(
                core::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_POPULATE,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { ptr: NonNull::new(ptr.cast()).expect("mmap returned a non-null mapping"), len })
    }

    fn shared(fd: &OwnedFd, len: usize, offset: libc::off_t) -> io::Result<Self> {
        let ptr = unsafe {
            libc::mmap(
                core::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                offset,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { ptr: NonNull::new(ptr.cast()).expect("mmap returned a non-null mapping"), len })
    }

    fn at<T>(&self, offset: u64) -> io::Result<NonNull<T>> {
        let offset = usize::try_from(offset).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "AF_XDP ring offset does not fit usize")
        })?;
        let end = offset.checked_add(size_of::<T>()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "AF_XDP ring offset overflow")
        })?;
        if end > self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "AF_XDP ring field lies outside its mapping",
            ));
        }
        let ptr = unsafe { self.ptr.as_ptr().add(offset).cast::<T>() };
        if !ptr.is_aligned() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("AF_XDP ring field is not aligned to {} bytes", align_of::<T>()),
            ));
        }
        NonNull::new(ptr).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "AF_XDP returned a null ring field")
        })
    }
}

impl Drop for MmapRegion {
    fn drop(&mut self) {
        let result = unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
        debug_assert_eq!(result, 0);
    }
}

struct ProducerRing<T> {
    _mapping: MmapRegion,
    producer: NonNull<AtomicU32>,
    consumer: NonNull<AtomicU32>,
    flags: NonNull<AtomicU32>,
    descriptors: NonNull<T>,
    size: u32,
    mask: u32,
    cached_producer: u32,
    cached_consumer: u32,
}

impl<T> ProducerRing<T> {
    fn map(
        fd: &OwnedFd,
        offset: libc::off_t,
        ring_offset: XdpRingOffset,
        size: u32,
    ) -> io::Result<Self> {
        let mapping_len = ring_mapping_len::<T>(ring_offset, size)?;
        let mapping = MmapRegion::shared(fd, mapping_len, offset)?;
        let producer = mapping.at(ring_offset.producer)?;
        let consumer = mapping.at(ring_offset.consumer)?;
        let flags = mapping.at(ring_offset.flags)?;
        let descriptors = mapping.at(ring_offset.desc)?;
        Ok(Self {
            _mapping: mapping,
            producer,
            consumer,
            flags,
            descriptors,
            size,
            mask: size - 1,
            cached_producer: 0,
            cached_consumer: 0,
        })
    }

    fn available(&mut self, requested: u32) -> u32 {
        let mut available =
            self.size.saturating_sub(self.cached_producer.wrapping_sub(self.cached_consumer));
        if available < requested {
            self.cached_consumer = unsafe { self.consumer.as_ref() }.load(Ordering::Acquire);
            available =
                self.size.saturating_sub(self.cached_producer.wrapping_sub(self.cached_consumer));
        }
        available
    }

    fn push(&mut self, descriptor: T) {
        debug_assert!(self.cached_producer.wrapping_sub(self.cached_consumer) < self.size);
        let index = self.cached_producer & self.mask;
        unsafe { self.descriptors.as_ptr().add(index as usize).write(descriptor) };
        self.cached_producer = self.cached_producer.wrapping_add(1);
    }

    fn publish(&self) {
        unsafe { self.producer.as_ref() }.store(self.cached_producer, Ordering::Release);
    }

    fn needs_wakeup(&self) -> bool {
        unsafe { self.flags.as_ref() }.load(Ordering::Acquire) & XDP_RING_NEED_WAKEUP != 0
    }

    fn is_empty(&self) -> bool {
        let consumer = unsafe { self.consumer.as_ref() }.load(Ordering::Acquire);
        self.cached_producer == consumer
    }

    fn indices(&self) -> (u32, u32) {
        (
            unsafe { self.producer.as_ref() }.load(Ordering::Acquire),
            unsafe { self.consumer.as_ref() }.load(Ordering::Acquire),
        )
    }
}

struct ConsumerRing<T> {
    _mapping: MmapRegion,
    producer: NonNull<AtomicU32>,
    consumer: NonNull<AtomicU32>,
    descriptors: NonNull<T>,
    mask: u32,
    cached_producer: u32,
    cached_consumer: u32,
}

impl<T: Copy> ConsumerRing<T> {
    fn map(
        fd: &OwnedFd,
        offset: libc::off_t,
        ring_offset: XdpRingOffset,
        size: u32,
    ) -> io::Result<Self> {
        let mapping_len = ring_mapping_len::<T>(ring_offset, size)?;
        let mapping = MmapRegion::shared(fd, mapping_len, offset)?;
        let producer = mapping.at(ring_offset.producer)?;
        let consumer = mapping.at(ring_offset.consumer)?;
        let descriptors = mapping.at(ring_offset.desc)?;
        Ok(Self {
            _mapping: mapping,
            producer,
            consumer,
            descriptors,
            mask: size - 1,
            cached_producer: 0,
            cached_consumer: 0,
        })
    }

    fn pop(&mut self) -> Option<T> {
        if self.cached_consumer == self.cached_producer {
            self.cached_producer = unsafe { self.producer.as_ref() }.load(Ordering::Acquire);
            if self.cached_consumer == self.cached_producer {
                return None;
            }
        }
        let index = self.cached_consumer & self.mask;
        let descriptor = unsafe { self.descriptors.as_ptr().add(index as usize).read() };
        self.cached_consumer = self.cached_consumer.wrapping_add(1);
        Some(descriptor)
    }

    fn release(&self) {
        unsafe { self.consumer.as_ref() }.store(self.cached_consumer, Ordering::Release);
    }

    fn indices(&self) -> (u32, u32) {
        (
            unsafe { self.producer.as_ref() }.load(Ordering::Acquire),
            unsafe { self.consumer.as_ref() }.load(Ordering::Acquire),
        )
    }
}

fn ring_mapping_len<T>(offset: XdpRingOffset, size: u32) -> io::Result<usize> {
    let descriptors_len = u64::from(size)
        .checked_mul(size_of::<T>() as u64)
        .and_then(|len| offset.desc.checked_add(len))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "AF_XDP ring size overflow"))?;
    let scalar_end = |field: u64| field.saturating_add(size_of::<u32>() as u64);
    let len = descriptors_len
        .max(scalar_end(offset.producer))
        .max(scalar_end(offset.consumer))
        .max(scalar_end(offset.flags));
    usize::try_from(len).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "AF_XDP mapping length does not fit usize")
    })
}

#[derive(Clone, Copy)]
struct FrameTemplate {
    source_mac: [u8; 6],
    destination_mac: [u8; 6],
    source_ip: Ipv4Addr,
    destination_ip: Ipv4Addr,
    source_port: u16,
    destination_port: u16,
    ttl: u8,
    vlan_id: Option<u16>,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct XdpBatchResult {
    pub(crate) enqueued_datagrams: u64,
    pub(crate) completed_datagrams: u64,
    pub(crate) dropped_ring_full: u64,
    pub(crate) dropped_frame_exhaustion: u64,
    pub(crate) kick_calls: u64,
    pub(crate) kick_errors: u64,
    pub(crate) wire_bytes: u64,
    pub(crate) tx_producer: u32,
    pub(crate) tx_consumer: u32,
    pub(crate) completion_producer: u32,
    pub(crate) completion_consumer: u32,
    pub(crate) tx_needs_wakeup: bool,
    pub(crate) free_frames: u32,
}

pub(crate) struct XdpTx {
    socket: OwnedFd,
    _xdp_link: Option<OwnedFd>,
    umem: MmapRegion,
    tx: ProducerRing<XdpDesc>,
    _fill: ProducerRing<u64>,
    completion: ConsumerRing<u64>,
    free_frames: Vec<u64>,
    frame_size: usize,
    frame_count: u32,
    next_ipv4_id: u16,
    templates: Vec<FrameTemplate>,
}

// SAFETY: `XdpTx` owns every file descriptor and mapping referenced by its raw
// pointers. Moving it to another thread does not move the mmap regions, and its
// mutable ring operations require exclusive access. It is intentionally not
// `Sync`.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for XdpTx {}

impl XdpTx {
    // Keep the kernel setup sequence together: ordering these UAPI operations
    // incorrectly can make bind succeed while the first TX wakeup fails.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn new(
        publisher: SocketAddrV4,
        multicast: Option<UdpMulticastConfig>,
        config: &UdpXdpConfig,
    ) -> io::Result<Self> {
        let templates = multicast.map_or_else(
            || {
                let source_ip =
                    config.source_ip.expect("AF_XDP unicast source address was validated");
                config
                    .unicast_destinations
                    .iter()
                    .flatten()
                    .map(|destination| FrameTemplate {
                        source_mac: config.source_mac,
                        destination_mac: destination.destination_mac,
                        source_ip,
                        destination_ip: *destination.address.ip(),
                        source_port: publisher.port(),
                        destination_port: destination.address.port(),
                        ttl: 64,
                        vlan_id: config.vlan_id,
                    })
                    .collect()
            },
            |multicast| {
                vec![FrameTemplate {
                    source_mac: config.source_mac,
                    destination_mac: multicast_mac(*multicast.group.ip()),
                    source_ip: multicast.interface,
                    destination_ip: *multicast.group.ip(),
                    source_port: publisher.port(),
                    destination_port: multicast.group.port(),
                    ttl: multicast.ttl,
                    vlan_id: config.vlan_id,
                }]
            },
        );
        let umem_len =
            (config.frame_size as usize).checked_mul(config.frame_count as usize).ok_or_else(
                || io::Error::new(io::ErrorKind::InvalidInput, "AF_XDP UMEM is too large"),
            )?;
        let umem = MmapRegion::anonymous(umem_len)?;

        let raw_fd = unsafe { libc::socket(libc::AF_XDP, libc::SOCK_RAW | libc::SOCK_NONBLOCK, 0) };
        if raw_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let socket = unsafe { OwnedFd::from_raw_fd(raw_fd) };

        let registration = XdpUmemReg {
            addr: umem.ptr.as_ptr() as u64,
            len: umem_len as u64,
            chunk_size: config.frame_size,
            headroom: 0,
            flags: 0,
            tx_metadata_len: 0,
        };
        set_xdp_option(&socket, XDP_UMEM_REG, "XDP_UMEM_REG", &registration)?;
        set_xdp_option(&socket, XDP_TX_RING, "XDP_TX_RING", &config.ring_size)?;
        set_xdp_option(&socket, XDP_UMEM_FILL_RING, "XDP_UMEM_FILL_RING", &config.ring_size)?;
        set_xdp_option(
            &socket,
            XDP_UMEM_COMPLETION_RING,
            "XDP_UMEM_COMPLETION_RING",
            &config.ring_size,
        )?;

        let mut offsets = XdpMmapOffsets::default();
        let mut offsets_len = size_of::<XdpMmapOffsets>() as libc::socklen_t;
        let result = unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                libc::SOL_XDP,
                XDP_MMAP_OFFSETS,
                core::ptr::from_mut(&mut offsets).cast(),
                &raw mut offsets_len,
            )
        };
        if result != 0 {
            return Err(last_xdp_error("getsockopt(XDP_MMAP_OFFSETS)"));
        }
        if offsets_len < size_of::<XdpMmapOffsets>() as libc::socklen_t {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "kernel returned an older AF_XDP ring-offset layout",
            ));
        }

        let tx = ProducerRing::map(&socket, XDP_PGOFF_TX_RING, offsets.tx, config.ring_size)
            .map_err(|error| contextual_error("mmap(XDP_TX_RING)", &error))?;
        // The first socket associated with a UMEM must create both UMEM
        // rings, even for TX-only operation. Leave the fill ring empty, as
        // recommended by the kernel AF_XDP documentation for TX-only sockets.
        let fill =
            ProducerRing::map(&socket, XDP_UMEM_PGOFF_FILL_RING, offsets.fill, config.ring_size)
                .map_err(|error| contextual_error("mmap(XDP_UMEM_FILL_RING)", &error))?;
        let completion = ConsumerRing::map(
            &socket,
            XDP_UMEM_PGOFF_COMPLETION_RING,
            offsets.completion,
            config.ring_size,
        )
        .map_err(|error| contextual_error("mmap(XDP_UMEM_COMPLETION_RING)", &error))?;

        let xdp_link =
            config.attach_xdp_pass.then(|| attach_xdp_pass(config.interface_index)).transpose()?;

        let address = SockAddrXdp {
            family: libc::AF_XDP as u16,
            flags: match config.mode {
                UdpXdpMode::Copy => XDP_COPY,
                UdpXdpMode::ZeroCopy => XDP_ZEROCOPY,
            } | XDP_USE_NEED_WAKEUP,
            interface_index: config.interface_index,
            queue_id: config.queue_id,
            shared_umem_fd: 0,
        };
        let result = unsafe {
            libc::bind(
                socket.as_raw_fd(),
                core::ptr::from_ref(&address).cast::<libc::sockaddr>(),
                size_of::<SockAddrXdp>() as libc::socklen_t,
            )
        };
        if result != 0 {
            let mode = match config.mode {
                UdpXdpMode::Copy => "XDP_COPY",
                UdpXdpMode::ZeroCopy => "XDP_ZEROCOPY",
            };
            return Err(last_xdp_error(&format!("bind(AF_XDP, {mode} | XDP_USE_NEED_WAKEUP)")));
        }
        let probe = unsafe {
            libc::sendto(
                socket.as_raw_fd(),
                core::ptr::null(),
                0,
                libc::MSG_DONTWAIT,
                core::ptr::null(),
                0,
            )
        };
        if probe < 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::WouldBlock {
                return Err(contextual_error("initial AF_XDP TX wakeup", &error));
            }
        }

        let mut free_frames = Vec::with_capacity(config.frame_count as usize);
        for index in (0..config.frame_count).rev() {
            free_frames.push(u64::from(index) * u64::from(config.frame_size));
        }

        Ok(Self {
            socket,
            _xdp_link: xdp_link,
            umem,
            tx,
            _fill: fill,
            completion,
            free_frames,
            frame_size: config.frame_size as usize,
            frame_count: config.frame_count,
            next_ipv4_id: 0,
            templates,
        })
    }

    #[cfg_attr(feature = "profiling", flux_profiler::timed("xdp.send_batch"))]
    pub(crate) fn send_batch(
        &mut self,
        max_datagram_size: usize,
        session_id: u32,
        sequences: &[u64],
        history: &[Vec<u8>],
        history_mask: u64,
    ) -> XdpBatchResult {
        let mut result =
            XdpBatchResult { completed_datagrams: self.reclaim(), ..XdpBatchResult::default() };

        let fragment_payload_size = max_datagram_size - UDP_HEADER_SIZE;
        let datagrams_per_destination = sequences
            .iter()
            .map(|sequence| {
                let len = history[(*sequence & history_mask) as usize].len();
                len.max(1).div_ceil(fragment_payload_size)
            })
            .sum::<usize>();
        let datagram_count = datagrams_per_destination.saturating_mul(self.templates.len());
        let requested = u32::try_from(datagram_count).unwrap_or(u32::MAX);
        let ring_available = self.tx.available(requested) as usize;
        let frame_available = self.free_frames.len();
        let capacity = ring_available.min(frame_available);
        let mut accepted_sequences = 0;
        let mut accepted_datagrams = 0;
        for &sequence in sequences {
            let len = history[(sequence & history_mask) as usize].len();
            let fragments =
                len.max(1).div_ceil(fragment_payload_size).saturating_mul(self.templates.len());
            if accepted_datagrams + fragments > capacity {
                break;
            }
            accepted_sequences += 1;
            accepted_datagrams += fragments;
        }
        let dropped_datagrams = datagram_count - accepted_datagrams;
        if ring_available <= frame_available {
            result.dropped_ring_full = dropped_datagrams as u64;
        } else {
            result.dropped_frame_exhaustion = dropped_datagrams as u64;
        }
        if accepted_sequences == 0 {
            self.kick_if_needed(&mut result);
            self.record_snapshot(&mut result);
            return result;
        }

        for &sequence in &sequences[..accepted_sequences] {
            let payload = &history[(sequence & history_mask) as usize];
            let encoded_all = encode_fragments(
                max_datagram_size,
                session_id,
                sequence,
                payload,
                |header, fragment| {
                    for template_index in 0..self.templates.len() {
                        let template = self.templates[template_index];
                        let frame_address =
                            self.free_frames.pop().expect("batch capacity was reserved");
                        let frame = unsafe {
                            core::slice::from_raw_parts_mut(
                                self.umem.ptr.as_ptr().add(frame_address as usize),
                                self.frame_size,
                            )
                        };
                        let frame_len =
                            encode_frame(frame, template, self.next_ipv4_id, header, fragment);
                        self.next_ipv4_id = self.next_ipv4_id.wrapping_add(1);
                        self.tx.push(XdpDesc {
                            addr: frame_address,
                            len: frame_len as u32,
                            options: 0,
                        });
                        result.enqueued_datagrams += 1;
                        result.wire_bytes += frame_len as u64;
                    }
                    true
                },
            );
            debug_assert!(encoded_all);
        }
        self.tx.publish();
        self.kick_if_needed(&mut result);
        self.record_snapshot(&mut result);
        result
    }

    fn record_snapshot(&self, result: &mut XdpBatchResult) {
        (result.tx_producer, result.tx_consumer) = self.tx.indices();
        (result.completion_producer, result.completion_consumer) = self.completion.indices();
        result.tx_needs_wakeup = self.tx.needs_wakeup();
        result.free_frames = self.free_frames.len() as u32;
    }

    #[cfg_attr(feature = "profiling", flux_profiler::timed("xdp.kick"))]
    fn kick_if_needed(&mut self, result: &mut XdpBatchResult) {
        if !self.tx.is_empty() && self.tx.needs_wakeup() {
            result.kick_calls += 1;
            let sent = unsafe {
                libc::sendto(
                    self.socket.as_raw_fd(),
                    core::ptr::null(),
                    0,
                    libc::MSG_DONTWAIT,
                    core::ptr::null(),
                    0,
                )
            };
            if sent < 0 {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::WouldBlock {
                    result.kick_errors = 1;
                }
            }
        }
    }

    #[cfg_attr(feature = "profiling", flux_profiler::timed("xdp.reclaim"))]
    fn reclaim(&mut self) -> u64 {
        let mut completed = 0;
        while let Some(address) = self.completion.pop() {
            if address % self.frame_size as u64 == 0 &&
                address / (self.frame_size as u64) < u64::from(self.frame_count)
            {
                self.free_frames.push(address);
                completed += 1;
            }
        }
        if completed != 0 {
            self.completion.release();
        }
        completed
    }
}

fn attach_xdp_pass(interface_index: u32) -> io::Result<OwnedFd> {
    const INSTRUCTIONS: [BpfInsn; 2] = [
        BpfInsn {
            // BPF_ALU64 | BPF_MOV | BPF_K: r0 = XDP_PASS.
            code: 0xb7,
            registers: 0,
            offset: 0,
            immediate: XDP_PASS,
        },
        BpfInsn {
            // BPF_JMP | BPF_EXIT.
            code: 0x95,
            registers: 0,
            offset: 0,
            immediate: 0,
        },
    ];
    const LICENSE: &[u8] = b"GPL\0";
    let mut verifier_log = vec![0_u8; 32 * 1024];
    let mut name = [0_u8; 16];
    name[..13].copy_from_slice(b"flux_xdp_pass");
    let program_attr = BpfProgLoadAttr {
        prog_type: BPF_PROG_TYPE_XDP,
        insn_count: INSTRUCTIONS.len() as u32,
        instructions: INSTRUCTIONS.as_ptr() as u64,
        license: LICENSE.as_ptr() as u64,
        log_level: 1,
        log_size: verifier_log.len() as u32,
        log_buffer: verifier_log.as_mut_ptr() as u64,
        kernel_version: 0,
        prog_flags: 0,
        prog_name: name,
        prog_ifindex: 0,
        expected_attach_type: 0,
        padding: [0; 80],
    };
    let program_fd = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            BPF_PROG_LOAD,
            core::ptr::from_ref(&program_attr),
            size_of::<BpfProgLoadAttr>(),
        )
    } as libc::c_int;
    if program_fd < 0 {
        let error = io::Error::last_os_error();
        let log_end = verifier_log.iter().position(|&byte| byte == 0).unwrap_or(verifier_log.len());
        let log = String::from_utf8_lossy(&verifier_log[..log_end]);
        return Err(io::Error::new(
            error.kind(),
            format!("bpf(BPF_PROG_LOAD XDP_PASS): {error}; verifier: {log}"),
        ));
    }
    let program = unsafe { OwnedFd::from_raw_fd(program_fd) };
    let link_attr = BpfLinkCreateAttr {
        prog_fd: program.as_raw_fd() as u32,
        target_ifindex: interface_index,
        attach_type: BPF_XDP,
        flags: XDP_FLAGS_DRV_MODE,
    };
    let link_fd = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            BPF_LINK_CREATE,
            core::ptr::from_ref(&link_attr),
            size_of::<BpfLinkCreateAttr>(),
        )
    } as libc::c_int;
    if link_fd < 0 {
        return Err(last_xdp_error("bpf(BPF_LINK_CREATE XDP_PASS, XDP_FLAGS_DRV_MODE)"));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(link_fd) })
}

fn set_xdp_option<T>(
    socket: &OwnedFd,
    option: libc::c_int,
    name: &str,
    value: &T,
) -> io::Result<()> {
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_XDP,
            option,
            core::ptr::from_ref(value).cast(),
            size_of::<T>() as libc::socklen_t,
        )
    };
    if result == 0 { Ok(()) } else { Err(last_xdp_error(&format!("setsockopt({name})"))) }
}

fn last_xdp_error(operation: &str) -> io::Error {
    contextual_error(operation, &io::Error::last_os_error())
}

fn contextual_error(operation: &str, error: &io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("{operation}: {error}"))
}

fn multicast_mac(ip: Ipv4Addr) -> [u8; 6] {
    let octets = ip.octets();
    [0x01, 0x00, 0x5e, octets[1] & 0x7f, octets[2], octets[3]]
}

fn encode_frame(
    frame: &mut [u8],
    template: FrameTemplate,
    ipv4_id: u16,
    flux_header: FragmentHeader,
    payload: &[u8],
) -> usize {
    let ethernet_header_size = if template.vlan_id.is_some() { 18 } else { 14 };
    let udp_payload_size = UDP_HEADER_SIZE + payload.len();
    let udp_size = UDP_HEADER_SIZE_BYTES + udp_payload_size;
    let ipv4_size = IPV4_HEADER_SIZE + udp_size;
    let frame_size = ethernet_header_size + ipv4_size;
    assert!(frame_size <= frame.len(), "AF_XDP frame exceeds configured frame size");

    frame[..6].copy_from_slice(&template.destination_mac);
    frame[6..12].copy_from_slice(&template.source_mac);
    let ip_offset = if let Some(vlan_id) = template.vlan_id {
        frame[12..14].copy_from_slice(&ETHERTYPE_VLAN.to_be_bytes());
        frame[14..16].copy_from_slice(&(vlan_id & 0x0fff).to_be_bytes());
        frame[16..18].copy_from_slice(&ETHERTYPE_IPV4.to_be_bytes());
        18
    } else {
        frame[12..14].copy_from_slice(&ETHERTYPE_IPV4.to_be_bytes());
        14
    };

    let ip = &mut frame[ip_offset..ip_offset + IPV4_HEADER_SIZE];
    ip.fill(0);
    ip[0] = 0x45;
    ip[2..4].copy_from_slice(&(ipv4_size as u16).to_be_bytes());
    ip[4..6].copy_from_slice(&ipv4_id.to_be_bytes());
    ip[6..8].copy_from_slice(&0x4000_u16.to_be_bytes());
    ip[8] = template.ttl;
    ip[9] = libc::IPPROTO_UDP as u8;
    ip[12..16].copy_from_slice(&template.source_ip.octets());
    ip[16..20].copy_from_slice(&template.destination_ip.octets());
    let checksum = ipv4_checksum(ip);
    ip[10..12].copy_from_slice(&checksum.to_be_bytes());

    let udp_offset = ip_offset + IPV4_HEADER_SIZE;
    let udp = &mut frame[udp_offset..udp_offset + UDP_HEADER_SIZE_BYTES];
    udp[0..2].copy_from_slice(&template.source_port.to_be_bytes());
    udp[2..4].copy_from_slice(&template.destination_port.to_be_bytes());
    udp[4..6].copy_from_slice(&(udp_size as u16).to_be_bytes());
    udp[6..8].fill(0); // Filled after the complete datagram has been encoded.

    let flux_offset = udp_offset + UDP_HEADER_SIZE_BYTES;
    let flux_bytes: &mut [u8; UDP_HEADER_SIZE] = (&mut frame
        [flux_offset..flux_offset + UDP_HEADER_SIZE])
        .try_into()
        .expect("sliced one Flux header");
    flux_header.encode(flux_bytes);
    frame[flux_offset + UDP_HEADER_SIZE..frame_size].copy_from_slice(payload);
    let checksum = udp_ipv4_checksum(
        template.source_ip,
        template.destination_ip,
        &frame[udp_offset..frame_size],
    );
    frame[udp_offset + 6..udp_offset + 8].copy_from_slice(&checksum.to_be_bytes());
    frame_size
}

fn ipv4_checksum(header: &[u8]) -> u16 {
    debug_assert_eq!(header.len(), IPV4_HEADER_SIZE);
    let mut sum = 0_u32;
    for word in header.chunks_exact(2) {
        sum += u32::from(u16::from_be_bytes([word[0], word[1]]));
    }
    while sum > u32::from(u16::MAX) {
        sum = (sum & u32::from(u16::MAX)) + (sum >> 16);
    }
    !(sum as u16)
}

fn udp_ipv4_checksum(source: Ipv4Addr, destination: Ipv4Addr, udp_datagram: &[u8]) -> u16 {
    let source = source.octets();
    let destination = destination.octets();
    let mut sum = u32::from(u16::from_be_bytes([source[0], source[1]])) +
        u32::from(u16::from_be_bytes([source[2], source[3]])) +
        u32::from(u16::from_be_bytes([destination[0], destination[1]])) +
        u32::from(u16::from_be_bytes([destination[2], destination[3]])) +
        libc::IPPROTO_UDP as u32 +
        udp_datagram.len() as u32;
    let mut words = udp_datagram.chunks_exact(2);
    for word in &mut words {
        sum += u32::from(u16::from_be_bytes([word[0], word[1]]));
    }
    if let Some(&last) = words.remainder().first() {
        sum += u32::from(last) << 8;
    }
    while sum > u32::from(u16::MAX) {
        sum = (sum & u32::from(u16::MAX)) + (sum >> 16);
    }
    let checksum = !(sum as u16);
    if checksum == 0 { u16::MAX } else { checksum }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>() {}

    #[test]
    fn xdp_tx_can_move_to_a_pinned_worker_thread() {
        assert_send::<XdpTx>();
    }

    #[test]
    fn producer_refreshes_consumer_before_wrapped_batch() {
        let raw_fd =
            unsafe { libc::memfd_create(c"flux-xdp-ring-test".as_ptr(), libc::MFD_CLOEXEC) };
        assert!(raw_fd >= 0, "memfd_create failed: {}", io::Error::last_os_error());
        let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
        let offsets = XdpRingOffset { producer: 0, consumer: 4, flags: 8, desc: 16 };
        let mapping_len = ring_mapping_len::<u64>(offsets, 8).unwrap();
        let truncate_result =
            unsafe { libc::ftruncate(fd.as_raw_fd(), mapping_len as libc::off_t) };
        assert_eq!(truncate_result, 0, "ftruncate failed: {}", io::Error::last_os_error());

        let mut ring = ProducerRing::<u64>::map(&fd, 0, offsets, 8).unwrap();
        ring.cached_producer = 6;
        ring.cached_consumer = 0;
        unsafe { ring.consumer.as_ref() }.store(4, Ordering::Release);

        assert_eq!(ring.available(4), 6);
        for descriptor in 100..104 {
            ring.push(descriptor);
        }
        ring.publish();

        assert_eq!(unsafe { ring.producer.as_ref() }.load(Ordering::Acquire), 10);
        assert_eq!(unsafe { *ring.descriptors.as_ptr().add(6) }, 100);
        assert_eq!(unsafe { *ring.descriptors.as_ptr().add(7) }, 101);
        assert_eq!(unsafe { *ring.descriptors.as_ptr() }, 102);
        assert_eq!(unsafe { *ring.descriptors.as_ptr().add(1) }, 103);
    }

    fn template(vlan_id: Option<u16>) -> FrameTemplate {
        FrameTemplate {
            source_mac: [0x02, 0, 0, 0, 0, 1],
            destination_mac: multicast_mac(Ipv4Addr::new(239, 10, 20, 30)),
            source_ip: Ipv4Addr::new(10, 9, 0, 1),
            destination_ip: Ipv4Addr::new(239, 10, 20, 30),
            source_port: 9000,
            destination_port: 9100,
            ttl: 1,
            vlan_id,
        }
    }

    #[test]
    fn derives_ipv4_multicast_mac() {
        assert_eq!(multicast_mac(Ipv4Addr::new(239, 10, 20, 30)), [
            0x01, 0x00, 0x5e, 0x0a, 0x14, 0x1e
        ]);
        assert_eq!(multicast_mac(Ipv4Addr::new(239, 138, 20, 30)), [
            0x01, 0x00, 0x5e, 0x0a, 0x14, 0x1e
        ]);
    }

    #[test]
    fn encodes_ethernet_ipv4_udp_and_flux_headers() {
        let mut frame = [0_u8; 2048];
        let header = FragmentHeader { session_id: 7, seq: 11, len: 3, offset: 0 };
        let len = encode_frame(&mut frame, template(None), 42, header, b"abc");

        assert_eq!(len, 14 + 20 + 8 + UDP_HEADER_SIZE + 3);
        assert_eq!(&frame[..6], &[0x01, 0x00, 0x5e, 0x0a, 0x14, 0x1e]);
        assert_eq!(&frame[6..12], &[0x02, 0, 0, 0, 0, 1]);
        assert_eq!(u16::from_be_bytes(frame[12..14].try_into().unwrap()), ETHERTYPE_IPV4);
        assert_eq!(ipv4_checksum(&frame[14..34]), 0);
        assert_eq!(&frame[26..30], &[10, 9, 0, 1]);
        assert_eq!(&frame[30..34], &[239, 10, 20, 30]);
        assert_eq!(u16::from_be_bytes(frame[34..36].try_into().unwrap()), 9000);
        assert_eq!(u16::from_be_bytes(frame[36..38].try_into().unwrap()), 9100);
        assert_eq!(u16::from_be_bytes(frame[38..40].try_into().unwrap()) as usize, 8 + 24 + 3);
        assert_ne!(u16::from_be_bytes(frame[40..42].try_into().unwrap()), 0);
        assert_eq!(
            udp_ipv4_checksum(
                template(None).source_ip,
                template(None).destination_ip,
                &frame[34..69]
            ),
            u16::MAX
        );
        assert_eq!(FragmentHeader::decode(&frame[42..66]).unwrap(), header);
        assert_eq!(&frame[66..69], b"abc");
    }

    #[test]
    fn encodes_vlan_header() {
        let mut frame = [0_u8; 2048];
        let header = FragmentHeader { session_id: 1, seq: 2, len: 0, offset: 0 };
        let len = encode_frame(&mut frame, template(Some(2135)), 0, header, b"");

        assert_eq!(len, 18 + 20 + 8 + UDP_HEADER_SIZE);
        assert_eq!(u16::from_be_bytes(frame[12..14].try_into().unwrap()), ETHERTYPE_VLAN);
        assert_eq!(u16::from_be_bytes(frame[14..16].try_into().unwrap()), 2135);
        assert_eq!(u16::from_be_bytes(frame[16..18].try_into().unwrap()), ETHERTYPE_IPV4);
        assert_eq!(ipv4_checksum(&frame[18..38]), 0);
    }
}
