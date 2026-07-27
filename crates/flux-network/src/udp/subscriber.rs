use std::{io, net::SocketAddr, os::fd::AsRawFd};

use flux_timing::Nanos;
use flux_utils::{safe_assert, safe_assert_eq};
use mio::Token;
use thiserror::Error;
use tracing::{debug, warn};

use super::{
    NativeSocketAddr, UdpConfig,
    control::{PublisherMessage, SubscriberMessage},
    wire::{DatagramError, Fragment, UDP_HEADER_SIZE},
};
use crate::tcp::{ClientEvent, SendBehavior, TcpClient, set_socket_buf_size};

/// Result of inserting fragment bytes into a reassembly buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InsertStatus {
    Duplicate,
    Incomplete,
    Complete,
}

#[derive(Debug, Error, PartialEq, Eq)]
enum InsertFragmentError {
    #[error("message length {message_length} exceeds configured maximum {maximum}")]
    MessageTooLarge { message_length: u32, maximum: usize },
    #[error(
        "inconsistent message length for sequence {sequence}: expected {expected}, got {actual}"
    )]
    InconsistentMessageLength { sequence: u64, expected: u32, actual: u32 },
}

/// Fixed-stride fragment reassembly for one UDP message sequence.
#[derive(Default)]
struct ReassemblyBuffer {
    bytes: Vec<u8>,
    received: Vec<u64>,
    fragment_count: usize,
    received_count: usize,
}

impl ReassemblyBuffer {
    fn reset(&mut self, message_length: u32, fragment_payload_size: usize) {
        safe_assert!(message_length != 0);
        safe_assert!(fragment_payload_size != 0);
        self.bytes.resize(message_length as usize, 0);
        self.fragment_count = self.bytes.len().div_ceil(fragment_payload_size);
        self.received.resize(self.fragment_count.div_ceil(u64::BITS as usize), 0);
        self.received.fill(0);
        self.received_count = 0;
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.received.clear();
        self.fragment_count = 0;
        self.received_count = 0;
    }

    fn is_initialized(&self) -> bool {
        self.fragment_count != 0
    }

    fn message_length(&self) -> u32 {
        self.bytes.len() as u32
    }

    fn is_complete(&self) -> bool {
        self.is_initialized() && self.received_count == self.fragment_count
    }

    fn payload(&self) -> Option<&[u8]> {
        self.is_complete().then_some(&self.bytes)
    }

    fn insert(&mut self, fragment_index: usize, offset: u32, payload: &[u8]) -> InsertStatus {
        safe_assert!(self.is_initialized());
        safe_assert!(fragment_index < self.fragment_count);
        safe_assert!(!payload.is_empty());
        let word = fragment_index / u64::BITS as usize;
        let bit = 1_u64 << (fragment_index % u64::BITS as usize);
        if self.received[word] & bit != 0 {
            return InsertStatus::Duplicate;
        }

        let start = offset as usize;
        let end = start + payload.len();
        safe_assert!(end <= self.bytes.len());
        self.bytes[start..end].copy_from_slice(payload);
        self.received[word] |= bit;
        self.received_count += 1;

        if self.is_complete() { InsertStatus::Complete } else { InsertStatus::Incomplete }
    }
}

const RECV_BATCH_SIZE: usize = 64;

struct RecvMetadata {
    sources: [libc::sockaddr_storage; RECV_BATCH_SIZE],
    iovecs: [libc::iovec; RECV_BATCH_SIZE],
    messages: [libc::mmsghdr; RECV_BATCH_SIZE],
}

/// Reusable `recvmmsg` storage for up to 64 already-queued datagrams per
/// syscall. `MSG_DONTWAIT` adds no batching delay. The payload and metadata are
/// heap-backed because the prewired message headers contain pointers into both
/// allocations, which must remain stable when the subscriber moves.
struct RecvBatch {
    buffer: Box<[u8]>,
    metadata: Box<RecvMetadata>,
    stride: usize,
    used: usize,
}

impl RecvBatch {
    fn new(stride: usize) -> Self {
        let mut buffer = vec![0; RECV_BATCH_SIZE * stride].into_boxed_slice();
        let mut metadata: Box<RecvMetadata> = Box::new(unsafe { core::mem::zeroed() });

        for index in 0..RECV_BATCH_SIZE {
            let start = index * stride;
            metadata.iovecs[index] = libc::iovec {
                iov_base: buffer[start..].as_mut_ptr().cast::<libc::c_void>(),
                iov_len: stride,
            };
            metadata.messages[index].msg_hdr.msg_name =
                core::ptr::from_mut(&mut metadata.sources[index]).cast::<libc::c_void>();
            metadata.messages[index].msg_hdr.msg_namelen =
                core::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            metadata.messages[index].msg_hdr.msg_iov =
                core::ptr::from_mut(&mut metadata.iovecs[index]);
            metadata.messages[index].msg_hdr.msg_iovlen = 1;
        }

        Self { buffer, metadata, stride, used: 0 }
    }

    fn receive(&mut self, socket: &mio::net::UdpSocket) -> io::Result<usize> {
        for message in &mut self.metadata.messages[..self.used] {
            message.msg_hdr.msg_namelen =
                core::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        }
        self.used = 0;

        let received = unsafe {
            libc::recvmmsg(
                socket.as_raw_fd(),
                self.metadata.messages.as_mut_ptr(),
                RECV_BATCH_SIZE as libc::c_uint,
                libc::MSG_DONTWAIT,
                core::ptr::null_mut(),
            )
        };
        if received < 0 {
            Err(io::Error::last_os_error())
        } else {
            self.used = received as usize;
            Ok(self.used)
        }
    }

    fn source_matches(&self, index: usize, expected: &NativeSocketAddr) -> bool {
        if self.metadata.messages[index].msg_hdr.msg_namelen != expected.address_length {
            return false;
        }

        unsafe {
            libc::memcmp(
                core::ptr::from_ref(&self.metadata.sources[index]).cast::<libc::c_void>(),
                core::ptr::from_ref(&expected.address).cast::<libc::c_void>(),
                expected.address_length as usize,
            ) == 0
        }
    }

    fn is_truncated(&self, index: usize) -> bool {
        self.metadata.messages[index].msg_hdr.msg_flags & libc::MSG_TRUNC != 0
    }

    fn datagram(&self, index: usize) -> &[u8] {
        let length = self.metadata.messages[index].msg_len as usize;
        safe_assert!(length <= self.stride);
        let start = index * self.stride;
        &self.buffer[start..start + length]
    }
}

pub enum SubscriberEvent<'a> {
    Connected {
        peer_addr: SocketAddr,
    },
    Disconnect {
        peer_addr: SocketAddr,
    },
    /// `ingest_ts` is when the first UDP fragment arrived, or when repair was
    /// first requested if the message was completely missed on UDP.
    Message {
        payload: &'a [u8],
        ingest_ts: Nanos,
    },
}

#[derive(Clone, Copy)]
enum SlotState {
    Empty,
    Pending { ingest_ts: Option<Nanos>, repair_after: Nanos, repair_requested: bool },
    Done,
}

struct SequenceSlot {
    sequence: u64,
    state: SlotState,
    buffer: ReassemblyBuffer,
}

impl SequenceSlot {
    fn new() -> Self {
        Self { sequence: 0, state: SlotState::Empty, buffer: ReassemblyBuffer::default() }
    }

    fn reset_pending(&mut self, sequence: u64, repair_after: Nanos) {
        self.sequence = sequence;
        self.state = SlotState::Pending { ingest_ts: None, repair_after, repair_requested: false };
        self.buffer.clear();
    }

    fn clear(&mut self) {
        self.state = SlotState::Empty;
        self.buffer.clear();
    }
}

struct SessionState {
    session_id: u32,
    fragment_payload_size: usize,
    max_message_size: usize,
    window_start: u64,
    watermark: u64,
    slots: Vec<SequenceSlot>,
    window_mask: u64,
    repair_cursor: usize,
    pending_count: usize,
    requested_count: usize,
    repair_delay: Nanos,
    next_repair_at: Option<Nanos>,
}

impl SessionState {
    fn new(
        session_id: u32,
        next_sequence: u64,
        sequence_window: usize,
        fragment_payload_size: usize,
        max_message_size: usize,
        repair_delay: Nanos,
    ) -> Self {
        Self {
            session_id,
            fragment_payload_size,
            max_message_size,
            window_start: next_sequence,
            watermark: next_sequence,
            slots: (0..sequence_window).map(|_| SequenceSlot::new()).collect(),
            window_mask: sequence_window as u64 - 1,
            repair_cursor: 0,
            pending_count: 0,
            requested_count: 0,
            repair_delay,
            next_repair_at: None,
        }
    }

    fn reset(&mut self, session_id: u32, next_sequence: u64) {
        for slot in &mut self.slots {
            slot.clear();
        }
        self.session_id = session_id;
        self.window_start = next_sequence;
        self.watermark = next_sequence;
        self.repair_cursor = 0;
        self.pending_count = 0;
        self.requested_count = 0;
        self.next_repair_at = None;
    }

    fn slot_index(&self, sequence: u64) -> usize {
        (sequence & self.window_mask) as usize
    }

    fn is_done(&self, sequence: u64) -> bool {
        if sequence < self.window_start {
            return true;
        }
        let slot = &self.slots[self.slot_index(sequence)];
        slot.sequence == sequence && matches!(slot.state, SlotState::Done)
    }

    fn clear_slot(&mut self, index: usize) -> Option<Nanos> {
        let slot = &mut self.slots[index];
        let state = core::mem::replace(&mut slot.state, SlotState::Empty);
        let ingest_ts = match state {
            SlotState::Empty => return None,
            SlotState::Done => None,
            SlotState::Pending { ingest_ts, repair_requested, .. } => {
                self.pending_count -= 1;
                if repair_requested {
                    self.requested_count -= 1;
                }
                ingest_ts
            }
        };
        slot.clear();
        ingest_ts
    }

    fn ensure_pending(&mut self, sequence: u64, observed_at: Nanos) -> usize {
        safe_assert!(sequence >= self.window_start);
        let index = self.slot_index(sequence);
        if self.slots[index].sequence == sequence {
            match self.slots[index].state {
                SlotState::Pending { .. } => return index,
                SlotState::Done => panic!("completed sequence cannot become pending"),
                SlotState::Empty => {}
            }
        }

        let _ = self.clear_slot(index);
        if self.pending_count == self.requested_count {
            self.repair_cursor = index;
        }
        let repair_after = Nanos(observed_at.0.saturating_add(self.repair_delay.0));
        self.slots[index].reset_pending(sequence, repair_after);
        self.pending_count += 1;
        self.next_repair_at =
            Some(self.next_repair_at.map_or(repair_after, |current| current.min(repair_after)));
        index
    }

    fn pending_mut(&mut self, sequence: u64, observed_at: Nanos) -> &mut SequenceSlot {
        let index = self.ensure_pending(sequence, observed_at);
        &mut self.slots[index]
    }

    fn finish(&mut self, sequence: u64) -> Option<Nanos> {
        safe_assert!(sequence >= self.window_start);
        let index = self.slot_index(sequence);
        let ingest_ts = if self.slots[index].sequence == sequence {
            self.clear_slot(index)
        } else {
            let _ = self.clear_slot(index);
            None
        };
        self.slots[index].sequence = sequence;
        self.slots[index].state = SlotState::Done;
        ingest_ts
    }

    fn mark_requested(&mut self, sequence: u64, request_ts: Nanos) {
        let index = self.slot_index(sequence);
        let slot = &mut self.slots[index];
        assert_eq!(slot.sequence, sequence, "repair slot changed before request was recorded");
        let SlotState::Pending { ingest_ts, repair_requested, .. } = &mut slot.state else {
            panic!("repair slot is not pending");
        };
        assert!(!*repair_requested, "repair was already requested");
        *repair_requested = true;
        ingest_ts.get_or_insert(request_ts);
        self.requested_count += 1;
    }

    fn reset_repair_requests(&mut self) {
        if self.requested_count == 0 {
            return;
        }
        let mut next_repair_at = None;
        for slot in &mut self.slots {
            if let SlotState::Pending { repair_after, repair_requested, .. } = &mut slot.state {
                *repair_requested = false;
                next_repair_at = Some(
                    next_repair_at
                        .map_or(*repair_after, |current: Nanos| current.min(*repair_after)),
                );
            }
        }
        self.requested_count = 0;
        self.next_repair_at = next_repair_at;
    }

    fn next_repair(&mut self, now: Nanos) -> Option<u64> {
        if self.pending_count == self.requested_count {
            self.next_repair_at = None;
            return None;
        }
        if self.next_repair_at.is_some_and(|next| now < next) {
            return None;
        }

        let mut next_repair_at = None;
        for _ in 0..self.slots.len() {
            let index = self.repair_cursor;
            self.repair_cursor += 1;
            if self.repair_cursor == self.slots.len() {
                self.repair_cursor = 0;
            }
            let slot = &self.slots[index];
            if let SlotState::Pending { repair_after, repair_requested: false, .. } = slot.state {
                if repair_after <= now {
                    return Some(slot.sequence);
                }
                next_repair_at = Some(
                    next_repair_at.map_or(repair_after, |current: Nanos| current.min(repair_after)),
                );
            }
        }
        self.next_repair_at = next_repair_at;
        None
    }

    fn advance_window(&mut self, new_start: u64) {
        if new_start <= self.window_start {
            return;
        }

        let window = self.slots.len() as u64;
        if new_start - self.window_start >= window {
            for index in 0..self.slots.len() {
                let _ = self.clear_slot(index);
            }
        } else {
            for sequence in self.window_start..new_start {
                let index = self.slot_index(sequence);
                if self.slots[index].sequence == sequence {
                    let _ = self.clear_slot(index);
                }
            }
        }
        self.window_start = new_start;
        self.watermark = self.watermark.max(new_start);
    }

    fn maybe_advance_window(&mut self, next_sequence: u64) {
        let new_start = next_sequence.saturating_sub(self.slots.len() as u64);
        self.advance_window(new_start);
    }

    fn observe_watermark(&mut self, next_sequence: u64, observed_at: Nanos) {
        if next_sequence <= self.watermark {
            return;
        }

        self.maybe_advance_window(next_sequence);
        for sequence in self.watermark..next_sequence {
            if !self.is_done(sequence) {
                self.ensure_pending(sequence, observed_at);
            }
        }
        self.watermark = next_sequence;
    }

    fn insert_fragment<F>(
        &mut self,
        fragment: Fragment<'_>,
        recv_ts: Nanos,
        handler: &mut F,
    ) -> Result<(), InsertFragmentError>
    where
        F: for<'a> FnMut(SubscriberEvent<'a>),
    {
        let sequence = fragment.header.seq;

        if fragment.header.len as usize > self.max_message_size {
            return Err(InsertFragmentError::MessageTooLarge {
                message_length: fragment.header.len,
                maximum: self.max_message_size,
            });
        }

        self.maybe_advance_window(sequence + 1);
        if self.is_done(sequence) {
            return Ok(());
        }

        if fragment.header.offset == 0 && fragment.payload.len() == fragment.header.len as usize {
            // fast path no reassembly
            let ingest_ts = self.finish(sequence).unwrap_or(recv_ts);
            handler(SubscriberEvent::Message { payload: fragment.payload, ingest_ts });
            return Ok(());
        }

        let fragment_payload_size = self.fragment_payload_size;
        let pending = self.pending_mut(sequence, recv_ts);
        let SlotState::Pending { ingest_ts, .. } = &mut pending.state else {
            unreachable!("ensured sequence slot is not pending")
        };
        ingest_ts.get_or_insert(recv_ts);
        if pending.buffer.is_initialized() {
            let expected = pending.buffer.message_length();
            if expected != fragment.header.len {
                return Err(InsertFragmentError::InconsistentMessageLength {
                    sequence,
                    expected,
                    actual: fragment.header.len,
                });
            }
        }
        if !pending.buffer.is_initialized() {
            pending.buffer.reset(fragment.header.len, fragment_payload_size);
        }

        match pending.buffer.insert(fragment.index, fragment.header.offset, fragment.payload) {
            InsertStatus::Complete => {
                let index = self.slot_index(sequence);
                let slot = &self.slots[index];
                safe_assert_eq!(slot.sequence, sequence);
                let SlotState::Pending { ingest_ts: Some(ingest_ts), .. } = slot.state else {
                    unreachable!("complete sequence slot has no ingest timestamp")
                };
                let payload = slot.buffer.payload().expect("complete payload is available");
                handler(SubscriberEvent::Message { payload, ingest_ts });
                self.finish(sequence);
                Ok(())
            }
            InsertStatus::Duplicate | InsertStatus::Incomplete => Ok(()),
        }
    }
}

pub struct UdpSubscriber {
    config: UdpConfig,
    publisher_addr: SocketAddr,
    publisher_native_addr: NativeSocketAddr,
    udp_socket: mio::net::UdpSocket,
    receive_batch: RecvBatch,
    repair: TcpClient,
    repair_token: Token,
    repair_ready: bool,
    session: Option<SessionState>,
}

impl UdpSubscriber {
    /// Make sure the same config is used for the publisher
    pub fn new_with_config(
        publisher_addr: SocketAddr,
        udp_bind_addr: SocketAddr,
        config: UdpConfig,
    ) -> io::Result<Self> {
        config.validate()?;

        let udp_socket = mio::net::UdpSocket::bind(udp_bind_addr)?;
        if let Some(size) = config.socket_buf_size {
            set_socket_buf_size(&udp_socket, size);
        }
        let udp_addr = udp_socket.local_addr()?;
        if udp_addr.port() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "subscriber UDP socket did not receive a port",
            ));
        }
        let mut subscribe = Vec::new();
        SubscriberMessage::Subscribe { udp_port: udp_addr.port() }.encode(&mut subscribe);
        let mut repair = TcpClient::default()
            .with_reconnect_interval(config.reconnect_interval.into())
            .with_on_connect_msg(subscribe)
            .with_drop_backlog_on_disconnect(true);
        if let Some(size) = config.socket_buf_size {
            repair = repair.with_socket_buf_size(size);
        }
        let repair_token = repair.connect(publisher_addr);

        Ok(Self {
            receive_batch: RecvBatch::new(config.max_datagram_size),
            config,
            publisher_addr,
            publisher_native_addr: NativeSocketAddr::encode(publisher_addr),
            udp_socket,
            repair,
            repair_token,
            repair_ready: false,
            session: None,
        })
    }

    pub fn poll_with<F>(&mut self, mut handler: F)
    where
        F: for<'a> FnMut(SubscriberEvent<'a>),
    {
        let max_message_size = self.config.max_message_size;
        let repair_delay = Nanos(self.config.repair_delay.as_nanos() as u64);
        let sequence_window = self.config.sequence_window;
        let fragment_payload_size = self.config.max_datagram_size - UDP_HEADER_SIZE;
        let can_repair = &mut self.repair_ready;
        let session = &mut self.session;
        let mut protocol_error = None;

        self.repair.poll_with(|event| {
            if protocol_error.is_some() {
                return;
            }

            match event {
                ClientEvent::Connected { token: _, peer_addr } => {
                    *can_repair = false;
                    if let Some(session) = session.as_mut() {
                        session.reset_repair_requests();
                    }
                    handler(SubscriberEvent::Connected { peer_addr });
                }
                ClientEvent::Disconnect { token: _, peer_addr } => {
                    *can_repair = false;
                    if let Some(session) = session.as_mut() {
                        session.reset_repair_requests();
                    }
                    handler(SubscriberEvent::Disconnect { peer_addr });
                }
                ClientEvent::Message { token: _, payload, .. } => {
                    let recv_ts = Nanos::now();
                    match PublisherMessage::decode(payload) {
                        Ok(PublisherMessage::State { session_id, next_sequence }) => {
                            match session {
                                Some(current) if current.session_id == session_id => {
                                    current.observe_watermark(next_sequence, recv_ts);
                                }
                                Some(current) => {
                                    current.reset(session_id, next_sequence);
                                }
                                None => {
                                    *session = Some(SessionState::new(
                                        session_id,
                                        next_sequence,
                                        sequence_window,
                                        fragment_payload_size,
                                        max_message_size,
                                        repair_delay,
                                    ));
                                }
                            }
                            *can_repair = true;
                        }
                        Ok(PublisherMessage::RepairData { session_id, sequence, payload }) => {
                            let Some(current) = session.as_mut() else { return };
                            if !*can_repair || current.session_id != session_id {
                                return;
                            }
                            if payload.len() > max_message_size {
                                protocol_error = Some(format!(
                                    "repair payload length {} exceeds configured maximum {}",
                                    payload.len(),
                                    max_message_size
                                ));
                                return;
                            }

                            if current.is_done(sequence) {
                                return;
                            }

                            let ingest_ts = current.finish(sequence).unwrap_or(recv_ts);
                            handler(SubscriberEvent::Message { payload, ingest_ts });
                        }
                        Ok(PublisherMessage::Unavailable { session_id, sequence }) => {
                            let Some(current) = session.as_mut() else { return };
                            if !*can_repair || current.session_id != session_id {
                                return;
                            }

                            if current.is_done(sequence) {
                                return;
                            }

                            current.finish(sequence);
                            debug!(session_id, sequence, "lost message");
                        }

                        Err(error) => protocol_error = Some(error.to_string()),
                    }
                }
            }
        });

        if let Some(error) = protocol_error {
            self.protocol_error(&error, &mut handler);
            return;
        }

        if let Some(error) = self.receive_datagrams(&mut handler) {
            self.protocol_error(&error, &mut handler);
            return;
        }

        self.request_repairs();
    }

    fn receive_datagrams<F>(&mut self, handler: &mut F) -> Option<String>
    where
        F: for<'a> FnMut(SubscriberEvent<'a>),
    {
        let mut protocol_error = None;
        let fragment_payload_size = self.config.max_datagram_size - UDP_HEADER_SIZE;

        loop {
            let received = match self.receive_batch.receive(&self.udp_socket) {
                Ok(0) => break,
                Ok(received) => received,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    warn!(%error, "UDP subscriber receive failed");
                    break;
                }
            };
            let recv_ts = Nanos::now();

            for index in 0..received {
                if !self.receive_batch.source_matches(index, &self.publisher_native_addr) {
                    continue;
                }
                let Some(session_id) = self.session.as_ref().map(|session| session.session_id)
                else {
                    continue;
                };
                if self.receive_batch.is_truncated(index) {
                    protocol_error =
                        Some("UDP datagram exceeds the configured maximum size".into());
                    break;
                }

                let datagram = self.receive_batch.datagram(index);
                let fragment = match Fragment::decode(datagram, session_id, fragment_payload_size) {
                    Ok(fragment) => fragment,
                    Err(DatagramError::UnexpectedSession { .. }) => continue,
                    Err(error) => {
                        protocol_error = Some(error.to_string());
                        break;
                    }
                };
                if let Err(error) = self
                    .session
                    .as_mut()
                    .expect("current session exists")
                    .insert_fragment(fragment, recv_ts, handler)
                {
                    warn!(%error, "UDP subscriber received invalid fragment");
                }
            }

            if protocol_error.is_some() {
                break;
            }
        }

        protocol_error
    }

    fn request_repairs(&mut self) {
        if !self.repair_ready {
            return;
        }

        let request_ts = Nanos::now();
        loop {
            let Some(session) = self.session.as_mut() else {
                return;
            };
            if session.requested_count >= self.config.max_inflight_repair_requests {
                return;
            }
            let Some(sequence) = session.next_repair(request_ts) else {
                return;
            };
            let session_id = session.session_id;

            let request = SubscriberMessage::Repair { session_id, sequence };
            self.repair.write_or_enqueue_with(SendBehavior::Single(self.repair_token), |output| {
                request.encode(output);
            });
            self.session
                .as_mut()
                .expect("repair request requires a session")
                .mark_requested(sequence, request_ts);
        }
    }

    fn protocol_error<F>(&mut self, error: &str, handler: &mut F)
    where
        F: for<'a> FnMut(SubscriberEvent<'a>),
    {
        warn!(%error, "UDP subscriber protocol error");
        self.repair_ready = false;
        if let Some(session) = &mut self.session {
            session.reset_repair_requests();
        }
        self.repair.disconnect(self.repair_token);
        handler(SubscriberEvent::Disconnect { peer_addr: self.publisher_addr });
    }
}
