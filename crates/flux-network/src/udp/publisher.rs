use std::{io, net::SocketAddr, os::fd::AsRawFd, time::Instant};

use flux_timing::Repeater;
use mio::Token;
use tracing::warn;

use super::{
    NativeSocketAddr, UdpConfig, UdpMulticastConfig, UdpSendBatchMode,
    control::{self, PublisherMessage, SubscriberMessage},
    wire::{FragmentHeader, UDP_HEADER_SIZE, encode_fragments},
    xdp::{XdpBatchResult, XdpTx},
};
use crate::tcp::{SendBehavior, ServerEvent, TcpServer, set_socket_buf_size};

pub enum PublisherEvent {
    // not necessarily subscribed yet
    Connected { addr: SocketAddr },
    Disconnect { addr: SocketAddr },
}

/// Publisher-side UDP send accounting. A GSO send entry may represent more
/// than one wire datagram, while a plain `sendmmsg` entry always represents
/// exactly one.
#[derive(Clone, Copy, Debug, Default)]
pub struct UdpPublisherStats {
    pub publication_flushes: u64,
    pub publications_flushed: u64,
    pub max_publications_per_flush: usize,
    pub immediate_flushes: u64,
    pub adaptive_immediate_flushes: u64,
    pub adaptive_batch_activations: u64,
    pub adaptive_idle_resets: u64,
    pub full_batch_flushes: u64,
    pub deadline_flushes: u64,
    pub explicit_flushes: u64,
    pub total_batch_dwell_ns: u64,
    pub max_batch_dwell_ns: u64,
    pub sendmmsg_calls: u64,
    pub total_sendmmsg_ns: u64,
    pub max_sendmmsg_ns: u64,
    pub sendmmsg_would_block: u64,
    pub send_entries: u64,
    pub wire_datagrams: u64,
    pub xdp_enqueued_datagrams: u64,
    pub xdp_completed_datagrams: u64,
    pub xdp_ring_full_drops: u64,
    pub xdp_frame_exhaustion_drops: u64,
    pub xdp_kick_calls: u64,
    pub xdp_kick_errors: u64,
    pub xdp_setup_fallbacks: u64,
    pub xdp_wire_bytes: u64,
    pub xdp_tx_producer: u32,
    pub xdp_tx_consumer: u32,
    pub xdp_completion_producer: u32,
    pub xdp_completion_consumer: u32,
    pub xdp_tx_needs_wakeup: bool,
    pub xdp_free_frames: u32,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FlushReason {
    AdaptiveImmediate,
    FullBatch,
    Deadline,
    Explicit,
}

/// Non-blocking directional publisher for reliable, unordered UDP messages.
pub struct UdpPublisher {
    config: UdpConfig,
    udp_socket: mio::net::UdpSocket,
    xdp_tx: Option<XdpTx>,
    session_id: u32,
    history: Vec<Vec<u8>>,
    history_mask: u64,
    next_sequence: u64,
    progress_repeater: Repeater,

    // TODO: remove this with new poll_with api
    pending_control: Vec<(Token, Result<SubscriberMessage, control::ControlError>)>,
    repair: TcpServer,
    subscribers: Subscribers,
    to_disconnect: Vec<Token>,
    pending_sequences: Vec<u64>,
    pending_since: Option<Instant>,
    last_publish_completed_at: Option<Instant>,
    adaptive_force_batch: bool,
    stats: UdpPublisherStats,
}

impl UdpPublisher {
    /// Make sure the same config is used for the subscriber
    pub fn new_with_config(addr: SocketAddr, config: UdpConfig) -> io::Result<Self> {
        config.validate()?;
        if config.multicast.is_some() && !addr.is_ipv4() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP multicast publisher address must be IPv4",
            ));
        }

        let socket = mio::net::UdpSocket::bind(addr)?;
        if let Some(size) = config.socket_buf_size {
            set_socket_buf_size(&socket, size);
        }
        if config.use_udp_segment {
            probe_udp_segment(&socket)?;
        }
        if let Some(multicast) = config.multicast {
            configure_multicast_sender(&socket, multicast)?;
            socket.connect(SocketAddr::V4(multicast.group))?;
        }
        let mut repair =
            TcpServer::default().with_nodelay(true).with_drop_backlog_on_disconnect(true);
        if let Some(size) = config.socket_buf_size {
            repair = repair.with_socket_buf_size(size);
        }
        repair.try_listen_at(addr)?;

        let mut stats = UdpPublisherStats::default();
        let xdp_tx = if let Some(xdp_config) = config.xdp {
            let publisher = match addr {
                SocketAddr::V4(publisher) => publisher,
                SocketAddr::V6(_) => unreachable!("AF_XDP validation requires IPv4"),
            };
            match XdpTx::new(publisher, config.multicast, &xdp_config) {
                Ok(xdp) => Some(xdp),
                Err(error) if xdp_config.fallback_to_socket => {
                    warn!(?error, "AF_XDP setup failed; falling back to kernel UDP");
                    stats.xdp_setup_fallbacks = 1;
                    None
                }
                Err(error) => {
                    return Err(io::Error::new(
                        error.kind(),
                        format!("AF_XDP publisher setup failed: {error}"),
                    ));
                }
            }
        } else {
            None
        };

        let mut history = Vec::with_capacity(config.sequence_window);
        history.resize_with(config.sequence_window, Vec::new);
        let mut progress_repeater = Repeater::every(config.progress_interval.into());
        progress_repeater.reset();
        let send_batch_size = config.send_batch_size;
        let history_mask = config.sequence_window as u64 - 1;
        let multicast_destination =
            config.multicast.map(|multicast| SocketAddr::V4(multicast.group));

        Ok(Self {
            config,
            udp_socket: socket,
            xdp_tx,
            repair,
            session_id: rand::random(),
            history,
            history_mask,
            next_sequence: 0,
            progress_repeater,
            to_disconnect: Vec::with_capacity(64),
            pending_control: Vec::with_capacity(64),
            subscribers: Subscribers::new(multicast_destination),
            pending_sequences: Vec::with_capacity(send_batch_size),
            pending_since: None,
            last_publish_completed_at: None,
            adaptive_force_batch: false,
            stats,
        })
    }

    pub fn active_subscribers(&self) -> usize {
        self.subscribers.len()
    }

    /// Returns true when publications are using the `AF_XDP` data path rather
    /// than its optional setup-time kernel UDP fallback.
    pub const fn using_xdp(&self) -> bool {
        self.xdp_tx.is_some()
    }

    pub const fn stats(&self) -> UdpPublisherStats {
        self.stats
    }

    pub fn reset_stats(&mut self) {
        self.stats = UdpPublisherStats::default();
    }

    #[cfg_attr(feature = "profiling", flux_profiler::timed("udp.publish"))]
    pub fn publish_with<F, G>(&mut self, mut handler: F, serialise: G)
    where
        F: FnMut(PublisherEvent),
        G: FnOnce(&mut Vec<u8>),
    {
        let sequence = self.next_sequence;
        let index = (sequence & self.history_mask) as usize;
        let payload = &mut self.history[index];
        payload.clear();
        serialise(payload);
        assert!(
            payload.len() <= self.config.max_message_size,
            "payload exceeds configured maximum UDP message size"
        );

        self.next_sequence += 1;
        if self.subscribers.is_empty() {
            return;
        }

        if self.config.send_batch_size == 1 && !self.config.use_udp_segment {
            self.stats.publication_flushes += 1;
            self.stats.publications_flushed += 1;
            self.stats.max_publications_per_flush = 1;
            self.stats.immediate_flushes += 1;
            if self.xdp_tx.is_some() {
                self.send_xdp_batch(core::slice::from_ref(&sequence));
            } else {
                self.subscribers.broadcast(
                    &self.udp_socket,
                    self.config.max_datagram_size,
                    self.session_id,
                    sequence,
                    payload,
                    &mut self.to_disconnect,
                    &mut self.stats,
                );
            }
            self.disconnect_pending(handler);
            return;
        }

        let now = Instant::now();
        let idle = self
            .last_publish_completed_at
            .map(|completed| now.saturating_duration_since(completed));
        if self.adaptive_force_batch &&
            idle.is_some_and(|idle| idle >= self.config.send_batch_max_delay.saturating_mul(2))
        {
            self.adaptive_force_batch = false;
            self.stats.adaptive_idle_resets += 1;
        }
        let adaptive_immediate = self.config.send_batch_mode == UdpSendBatchMode::Adaptive &&
            !self.adaptive_force_batch &&
            self.pending_sequences.is_empty() &&
            idle.is_none_or(|idle| idle >= self.config.send_batch_max_delay);
        self.pending_sequences.push(sequence);
        self.pending_since.get_or_insert(now);
        let flush_reason = if adaptive_immediate {
            Some(FlushReason::AdaptiveImmediate)
        } else if self.pending_sequences.len() >= self.config.send_batch_size {
            Some(FlushReason::FullBatch)
        } else if self.pending_deadline_elapsed() {
            Some(FlushReason::Deadline)
        } else {
            None
        };
        if let Some(reason) = flush_reason {
            let flush_started = Instant::now();
            self.flush_pending(&mut handler, reason);
            if self.config.send_batch_mode == UdpSendBatchMode::Adaptive &&
                (reason == FlushReason::FullBatch ||
                    (reason == FlushReason::AdaptiveImmediate &&
                        flush_started.elapsed() >= self.config.send_batch_max_delay))
            {
                self.stats.adaptive_batch_activations += u64::from(!self.adaptive_force_batch);
                self.adaptive_force_batch = true;
            }
        }
        self.last_publish_completed_at = Some(Instant::now());
    }

    /// Flushes any queued publisher batch without polling the repair channel.
    pub fn flush_with<F>(&mut self, mut handler: F)
    where
        F: FnMut(PublisherEvent),
    {
        self.flush_pending(&mut handler, FlushReason::Explicit);
    }

    #[cfg_attr(feature = "profiling", flux_profiler::timed("udp.poll"))]
    pub fn poll_with<F>(&mut self, mut handler: F)
    where
        F: FnMut(PublisherEvent),
    {
        self.poll_data_inner(&mut handler);
        self.poll_control_inner(&mut handler);
    }

    /// Polls publication batch deadlines without entering the TCP
    /// subscription and repair control plane.
    #[cfg_attr(feature = "profiling", flux_profiler::timed("udp.data_poll"))]
    pub fn poll_data_with<F>(&mut self, mut handler: F)
    where
        F: FnMut(PublisherEvent),
    {
        self.poll_data_inner(&mut handler);
    }

    /// Polls only TCP subscription, progress, and repair processing.
    #[cfg_attr(feature = "profiling", flux_profiler::timed("udp.control_poll"))]
    pub fn poll_control_with<F>(&mut self, mut handler: F)
    where
        F: FnMut(PublisherEvent),
    {
        self.poll_control_inner(&mut handler);
    }

    fn poll_data_inner<F>(&mut self, handler: &mut F)
    where
        F: FnMut(PublisherEvent),
    {
        if self.pending_deadline_elapsed() {
            self.flush_pending(handler, FlushReason::Deadline);
        }
    }

    fn poll_control_inner<F>(&mut self, handler: &mut F)
    where
        F: FnMut(PublisherEvent),
    {
        // TODO: owned api in poll_with so we can send immediately
        self.repair.poll_with(|event| match event {
            ServerEvent::Accept { stream, peer_addr, .. } => {
                self.subscribers.add_pending(stream, peer_addr);
                handler(PublisherEvent::Connected { addr: peer_addr });
            }
            ServerEvent::Disconnect { token } => {
                if let Some(addr) = self.subscribers.remove(token) {
                    handler(PublisherEvent::Disconnect { addr });
                }
            }
            ServerEvent::Message { token, payload, .. } => {
                self.pending_control.push((token, SubscriberMessage::decode(payload)));
            }
        });

        for (token, msg) in self.pending_control.drain(..) {
            match msg {
                Ok(msg) => match msg {
                    SubscriberMessage::Subscribe { udp_port } => {
                        if !self.subscribers.set_udp_port(token, udp_port) {
                            continue;
                        }
                        self.repair.write_or_enqueue_with(SendBehavior::Single(token), |buf| {
                            PublisherMessage::State {
                                session_id: self.session_id,
                                next_sequence: self.next_sequence,
                            }
                            .encode(buf);
                        });
                    }
                    SubscriberMessage::Repair { session_id, sequence } => {
                        if self.session_id != session_id {
                            push_unique(&mut self.to_disconnect, token);
                            continue;
                        }

                        let distance = self.next_sequence.saturating_sub(sequence);
                        if distance == 0 || distance as usize > self.history.len() {
                            self.repair.write_or_enqueue_with(SendBehavior::Single(token), |buf| {
                                PublisherMessage::Unavailable {
                                    session_id: self.session_id,
                                    sequence,
                                }
                                .encode(buf);
                            });
                            continue;
                        }

                        let index = (sequence & self.history_mask) as usize;
                        let payload = self.history[index].as_slice();

                        self.repair.write_or_enqueue_with(SendBehavior::Single(token), |buf| {
                            PublisherMessage::RepairData { session_id, sequence, payload }
                                .encode(buf);
                        });
                    }
                },
                Err(err) => {
                    warn!(?err, "udp protocol error");
                    push_unique(&mut self.to_disconnect, token);
                }
            }
        }

        if self.progress_repeater.fired() {
            self.repair.write_or_enqueue_with(SendBehavior::Broadcast, |buf| {
                PublisherMessage::State {
                    session_id: self.session_id,
                    next_sequence: self.next_sequence,
                }
                .encode(buf);
            });
        }

        self.disconnect_pending(handler);
    }

    #[cfg_attr(feature = "profiling", flux_profiler::timed("udp.batch_flush"))]
    fn pending_deadline_elapsed(&self) -> bool {
        self.pending_since
            .is_some_and(|started| started.elapsed() >= self.config.send_batch_max_delay)
    }

    fn flush_pending<F>(&mut self, handler: &mut F, reason: FlushReason)
    where
        F: FnMut(PublisherEvent),
    {
        if self.pending_sequences.is_empty() {
            return;
        }

        let publication_count = self.pending_sequences.len();
        self.stats.publication_flushes += 1;
        self.stats.publications_flushed += publication_count as u64;
        self.stats.max_publications_per_flush =
            self.stats.max_publications_per_flush.max(publication_count);
        match reason {
            FlushReason::AdaptiveImmediate => self.stats.adaptive_immediate_flushes += 1,
            FlushReason::FullBatch => self.stats.full_batch_flushes += 1,
            FlushReason::Deadline => self.stats.deadline_flushes += 1,
            FlushReason::Explicit => self.stats.explicit_flushes += 1,
        }
        let dwell_ns = self
            .pending_since
            .map_or(0, |started| u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        self.stats.total_batch_dwell_ns = self.stats.total_batch_dwell_ns.saturating_add(dwell_ns);
        self.stats.max_batch_dwell_ns = self.stats.max_batch_dwell_ns.max(dwell_ns);
        let adaptive_sequence = (reason == FlushReason::AdaptiveImmediate)
            .then(|| self.pending_sequences.first().copied())
            .flatten();
        let use_immediate_path = adaptive_sequence.is_some_and(|sequence| {
            !self.config.use_udp_segment ||
                self.history[(sequence & self.history_mask) as usize].len() <=
                    self.config.max_datagram_size - UDP_HEADER_SIZE
        });
        if self.xdp_tx.is_some() {
            let sequences = core::mem::take(&mut self.pending_sequences);
            self.send_xdp_batch(&sequences);
            self.pending_sequences = sequences;
        } else if use_immediate_path {
            let sequence = adaptive_sequence.expect("checked adaptive sequence");
            let payload = &self.history[(sequence & self.history_mask) as usize];
            self.subscribers.broadcast(
                &self.udp_socket,
                self.config.max_datagram_size,
                self.session_id,
                sequence,
                payload,
                &mut self.to_disconnect,
                &mut self.stats,
            );
        } else {
            self.subscribers.broadcast_batch(
                &self.udp_socket,
                self.config.max_datagram_size,
                self.session_id,
                &self.pending_sequences,
                &self.history,
                self.history_mask,
                self.config.use_udp_segment,
                self.config.copy_udp_segment_payloads,
                &mut self.to_disconnect,
                &mut self.stats,
            );
        }
        self.pending_sequences.clear();
        self.pending_since = None;
        self.disconnect_pending(handler);
    }

    #[cfg_attr(feature = "profiling", flux_profiler::timed("udp.xdp_send_batch"))]
    fn send_xdp_batch(&mut self, sequences: &[u64]) {
        let result = self.xdp_tx.as_mut().expect("checked AF_XDP backend").send_batch(
            self.config.max_datagram_size,
            self.session_id,
            sequences,
            &self.history,
            self.history_mask,
        );
        self.record_xdp_result(result);
    }

    fn record_xdp_result(&mut self, result: XdpBatchResult) {
        self.stats.xdp_enqueued_datagrams += result.enqueued_datagrams;
        self.stats.xdp_completed_datagrams += result.completed_datagrams;
        self.stats.xdp_ring_full_drops += result.dropped_ring_full;
        self.stats.xdp_frame_exhaustion_drops += result.dropped_frame_exhaustion;
        self.stats.xdp_kick_calls += result.kick_calls;
        self.stats.xdp_kick_errors += result.kick_errors;
        self.stats.xdp_wire_bytes += result.wire_bytes;
        self.stats.xdp_tx_producer = result.tx_producer;
        self.stats.xdp_tx_consumer = result.tx_consumer;
        self.stats.xdp_completion_producer = result.completion_producer;
        self.stats.xdp_completion_consumer = result.completion_consumer;
        self.stats.xdp_tx_needs_wakeup = result.tx_needs_wakeup;
        self.stats.xdp_free_frames = result.free_frames;
        self.stats.wire_datagrams += result.enqueued_datagrams;
    }

    fn disconnect_pending<F>(&mut self, mut handler: F)
    where
        F: FnMut(PublisherEvent),
    {
        if self.to_disconnect.is_empty() {
            return;
        }
        for token in self.to_disconnect.drain(..) {
            if token == MULTICAST_TOKEN {
                continue;
            }
            self.repair.disconnect(token);
            if let Some(addr) = self.subscribers.remove(token) {
                handler(PublisherEvent::Disconnect { addr });
            }
        }
    }
}

struct Subscriber {
    token: Token,
    peer_addr: SocketAddr,
    udp_destination: Option<NativeSocketAddr>,
}

impl Subscriber {
    fn new(token: Token, peer_addr: SocketAddr) -> Self {
        Self { token, peer_addr, udp_destination: None }
    }

    fn update_udp_port(&mut self, udp_port: u16) {
        let udp_addr = SocketAddr::new(self.peer_addr.ip(), udp_port);
        self.udp_destination = Some(NativeSocketAddr::encode(udp_addr));
    }
}

const SEND_BATCH_SIZE: usize = 64;
const MULTICAST_TOKEN: Token = Token(usize::MAX);
const UDP_SEGMENT: libc::c_int = 103;
const GSO_MAX_SEGMENTS: usize = 64;
const GSO_MAX_PAYLOAD: usize = 65_507;
const UDP_SEGMENT_CONTROL_SPACE: usize =
    unsafe { libc::CMSG_SPACE(size_of::<u16>() as _) as usize };

#[repr(C)]
struct UdpSegmentControl {
    header: libc::cmsghdr,
    segment_size: u16,
    padding: [u8; UDP_SEGMENT_CONTROL_SPACE - size_of::<libc::cmsghdr>() - size_of::<u16>()],
}

impl UdpSegmentControl {
    fn new(segment_size: u16) -> Self {
        Self {
            header: libc::cmsghdr {
                cmsg_len: unsafe { libc::CMSG_LEN(size_of::<u16>() as _) as usize },
                cmsg_level: libc::SOL_UDP,
                cmsg_type: UDP_SEGMENT,
            },
            segment_size,
            padding: [0; UDP_SEGMENT_CONTROL_SPACE - size_of::<libc::cmsghdr>() - size_of::<u16>()],
        }
    }
}

struct GsoGroup {
    bytes: Vec<u8>,
    segment_size: usize,
    segment_indices: Vec<usize>,
    iovec_start: usize,
}

struct GsoSegment {
    header: [u8; UDP_HEADER_SIZE],
    payload: *const u8,
    payload_len: usize,
    datagram_size: usize,
}

struct Subscribers {
    subs: Vec<Subscriber>,
    active_count: usize,
    multicast_destination: Option<NativeSocketAddr>,
    headers: [[u8; UDP_HEADER_SIZE]; SEND_BATCH_SIZE],
    iovecs: [[libc::iovec; 2]; SEND_BATCH_SIZE],
    messages: Vec<libc::mmsghdr>,
    message_tokens: [Token; SEND_BATCH_SIZE],
    expected_lengths: [usize; SEND_BATCH_SIZE],
    fragment_count: usize,
    batch_datagrams: Vec<Vec<u8>>,
    batch_datagram_count: usize,
    batch_iovecs: Vec<libc::iovec>,
    batch_messages: Vec<libc::mmsghdr>,
    batch_message_tokens: Vec<Token>,
    batch_expected_lengths: Vec<usize>,
    batch_wire_datagrams: Vec<usize>,
    gso_segments: Vec<GsoSegment>,
    gso_groups: Vec<GsoGroup>,
    gso_controls: Vec<UdpSegmentControl>,
}

impl Subscribers {
    fn new(multicast_destination: Option<SocketAddr>) -> Self {
        Self {
            subs: Vec::with_capacity(64),
            active_count: 0,
            multicast_destination: multicast_destination.map(NativeSocketAddr::encode),
            headers: [[0; UDP_HEADER_SIZE]; SEND_BATCH_SIZE],
            iovecs: core::array::from_fn(|_| {
                core::array::from_fn(|_| libc::iovec {
                    iov_base: core::ptr::null_mut(),
                    iov_len: 0,
                })
            }),
            messages: Vec::with_capacity(SEND_BATCH_SIZE),
            message_tokens: [Token(0); SEND_BATCH_SIZE],
            expected_lengths: [0; SEND_BATCH_SIZE],
            fragment_count: 0,
            batch_datagrams: Vec::new(),
            batch_datagram_count: 0,
            batch_iovecs: Vec::new(),
            batch_messages: Vec::new(),
            batch_message_tokens: Vec::new(),
            batch_expected_lengths: Vec::new(),
            batch_wire_datagrams: Vec::new(),
            gso_segments: Vec::new(),
            gso_groups: Vec::new(),
            gso_controls: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.active_count
    }

    fn is_empty(&self) -> bool {
        self.active_count == 0
    }

    fn add_pending(&mut self, token: Token, peer_addr: SocketAddr) {
        self.subs.push(Subscriber::new(token, peer_addr));
    }

    fn set_udp_port(&mut self, token: Token, udp_port: u16) -> bool {
        let Some(sub) = self.subs.iter_mut().find(|sub| sub.token == token) else {
            return false;
        };
        let was_pending = sub.udp_destination.is_none();
        sub.update_udp_port(udp_port);
        self.active_count += usize::from(was_pending);
        true
    }

    fn remove(&mut self, token: Token) -> Option<SocketAddr> {
        let index = self.subs.iter().position(|sub| sub.token == token)?;
        let sub = self.subs.swap_remove(index);
        self.active_count -= usize::from(sub.udp_destination.is_some());
        Some(sub.peer_addr)
    }

    /// Batches up to 64 UDP datagrams across fragments and subscribers per
    /// `sendmmsg`. Payloads are not copied; the fixed batch stores one
    /// `mmsghdr`, token, and expected length per datagram, plus one shared
    /// header/iovec pair per fragment.
    #[cfg_attr(feature = "profiling", flux_profiler::timed("udp.broadcast"))]
    #[allow(clippy::too_many_arguments)]
    fn broadcast(
        &mut self,
        socket: &mio::net::UdpSocket,
        max_datagram_size: usize,
        session_id: u32,
        sequence: u64,
        message: &[u8],
        failed_tokens: &mut Vec<Token>,
        stats: &mut UdpPublisherStats,
    ) {
        self.messages.clear();
        self.fragment_count = 0;

        let encoded_all = encode_fragments(
            max_datagram_size,
            session_id,
            sequence,
            message,
            |header, fragment| {
                self.enqueue_fragment(socket, header, fragment, failed_tokens, stats)
            },
        );
        if encoded_all {
            let _ = self.flush(socket, failed_tokens, stats);
        }
    }

    #[cfg_attr(feature = "profiling", flux_profiler::timed("udp.batch_broadcast"))]
    #[allow(clippy::too_many_arguments)]
    fn broadcast_batch(
        &mut self,
        socket: &mio::net::UdpSocket,
        max_datagram_size: usize,
        session_id: u32,
        sequences: &[u64],
        history: &[Vec<u8>],
        history_mask: u64,
        use_udp_segment: bool,
        copy_udp_segment_payloads: bool,
        failed_tokens: &mut Vec<Token>,
        stats: &mut UdpPublisherStats,
    ) {
        if use_udp_segment {
            if copy_udp_segment_payloads {
                self.encode_batch_datagrams(
                    max_datagram_size,
                    session_id,
                    sequences,
                    history,
                    history_mask,
                );
                self.prepare_copied_gso_messages();
            } else {
                self.encode_gso_segments(
                    max_datagram_size,
                    session_id,
                    sequences,
                    history,
                    history_mask,
                );
                self.prepare_gso_messages();
            }
        } else {
            self.encode_batch_datagrams(
                max_datagram_size,
                session_id,
                sequences,
                history,
                history_mask,
            );
            self.prepare_plain_batch_messages();
        }

        let _ = send_messages(
            socket,
            &mut self.batch_messages,
            &self.batch_message_tokens,
            &self.batch_expected_lengths,
            &self.batch_wire_datagrams,
            failed_tokens,
            stats,
        );
        self.clear_dynamic_batch();
    }

    #[cfg_attr(feature = "profiling", flux_profiler::timed("udp.batch_encode"))]
    fn encode_batch_datagrams(
        &mut self,
        max_datagram_size: usize,
        session_id: u32,
        sequences: &[u64],
        history: &[Vec<u8>],
        history_mask: u64,
    ) {
        let mut used = 0;
        for &sequence in sequences {
            let payload = &history[(sequence & history_mask) as usize];
            let encoded_all = encode_fragments(
                max_datagram_size,
                session_id,
                sequence,
                payload,
                |header, fragment| {
                    if used == self.batch_datagrams.len() {
                        self.batch_datagrams
                            .push(Vec::with_capacity(UDP_HEADER_SIZE + fragment.len()));
                    }
                    let datagram = &mut self.batch_datagrams[used];
                    datagram.clear();
                    datagram.resize(UDP_HEADER_SIZE, 0);
                    let header_bytes: &mut [u8; UDP_HEADER_SIZE] =
                        datagram.as_mut_slice().try_into().expect("resized to one UDP header");
                    header.encode(header_bytes);
                    datagram.extend_from_slice(fragment);
                    used += 1;
                    true
                },
            );
            debug_assert!(encoded_all);
        }
        self.batch_datagram_count = used;
    }

    /// Encodes only Flux headers for GSO. Payload iovecs point directly into
    /// publisher history, which remains stable until this synchronous send
    /// completes, so the GSO path does not copy publication payload bytes.
    #[cfg_attr(feature = "profiling", flux_profiler::timed("udp.gso_encode_headers"))]
    fn encode_gso_segments(
        &mut self,
        max_datagram_size: usize,
        session_id: u32,
        sequences: &[u64],
        history: &[Vec<u8>],
        history_mask: u64,
    ) {
        self.gso_segments.clear();
        for &sequence in sequences {
            let payload = &history[(sequence & history_mask) as usize];
            let encoded_all = encode_fragments(
                max_datagram_size,
                session_id,
                sequence,
                payload,
                |header, fragment| {
                    let mut encoded_header = [0; UDP_HEADER_SIZE];
                    header.encode(&mut encoded_header);
                    self.gso_segments.push(GsoSegment {
                        header: encoded_header,
                        payload: fragment.as_ptr(),
                        payload_len: fragment.len(),
                        datagram_size: UDP_HEADER_SIZE + fragment.len(),
                    });
                    true
                },
            );
            debug_assert!(encoded_all);
        }
    }

    fn prepare_plain_batch_messages(&mut self) {
        self.batch_iovecs.clear();
        self.batch_iovecs.reserve(self.batch_datagram_count);
        for datagram in &mut self.batch_datagrams[..self.batch_datagram_count] {
            self.batch_iovecs.push(libc::iovec {
                iov_base: datagram.as_mut_ptr().cast::<libc::c_void>(),
                iov_len: datagram.len(),
            });
        }

        let destination_count =
            if self.multicast_destination.is_some() { 1 } else { self.active_count };
        let message_count = self.batch_datagram_count * destination_count;
        self.reserve_dynamic_messages(message_count);
        for datagram_index in 0..self.batch_datagram_count {
            let expected_length = self.batch_datagrams[datagram_index].len();
            if self.multicast_destination.is_some() {
                let mut message: libc::mmsghdr = unsafe { core::mem::zeroed() };
                message.msg_hdr.msg_iov = &raw mut self.batch_iovecs[datagram_index];
                message.msg_hdr.msg_iovlen = 1;
                self.batch_messages.push(message);
                self.batch_message_tokens.push(MULTICAST_TOKEN);
                self.batch_expected_lengths.push(expected_length);
                self.batch_wire_datagrams.push(1);
                continue;
            }
            for subscriber in &self.subs {
                let Some(destination) = subscriber.udp_destination.as_ref() else { continue };
                let mut message: libc::mmsghdr = unsafe { core::mem::zeroed() };
                message.msg_hdr.msg_name =
                    core::ptr::from_ref(&destination.address).cast_mut().cast::<libc::c_void>();
                message.msg_hdr.msg_namelen = destination.address_length;
                message.msg_hdr.msg_iov = &raw mut self.batch_iovecs[datagram_index];
                message.msg_hdr.msg_iovlen = 1;
                self.batch_messages.push(message);
                self.batch_message_tokens.push(subscriber.token);
                self.batch_expected_lengths.push(expected_length);
                self.batch_wire_datagrams.push(1);
            }
        }
    }

    #[cfg_attr(feature = "profiling", flux_profiler::timed("udp.gso_coalesce"))]
    fn prepare_gso_messages(&mut self) {
        let mut group_count = 0;
        for segment_index in 0..self.gso_segments.len() {
            let segment_size = self.gso_segments[segment_index].datagram_size;
            let max_segments = GSO_MAX_SEGMENTS.min((GSO_MAX_PAYLOAD / segment_size).max(1));
            let existing = (0..group_count).rev().find(|&index| {
                let group = &self.gso_groups[index];
                group.segment_size == segment_size && group.segment_indices.len() < max_segments
            });
            let group_index = existing.unwrap_or_else(|| {
                if group_count == self.gso_groups.len() {
                    self.gso_groups.push(GsoGroup {
                        bytes: Vec::new(),
                        segment_size,
                        segment_indices: Vec::new(),
                        iovec_start: 0,
                    });
                }
                let index = group_count;
                let group = &mut self.gso_groups[index];
                group.segment_size = segment_size;
                group.segment_indices.clear();
                group.iovec_start = 0;
                group_count += 1;
                index
            });
            self.gso_groups[group_index].segment_indices.push(segment_index);
        }

        self.batch_iovecs.clear();
        self.batch_iovecs.reserve(self.gso_segments.len() * 2);
        self.gso_controls.clear();
        self.gso_controls.reserve(group_count);
        for group in &mut self.gso_groups[..group_count] {
            group.iovec_start = self.batch_iovecs.len();
            for &segment_index in &group.segment_indices {
                let segment = &mut self.gso_segments[segment_index];
                self.batch_iovecs.push(libc::iovec {
                    iov_base: segment.header.as_mut_ptr().cast::<libc::c_void>(),
                    iov_len: UDP_HEADER_SIZE,
                });
                self.batch_iovecs.push(libc::iovec {
                    iov_base: segment.payload.cast_mut().cast::<libc::c_void>(),
                    iov_len: segment.payload_len,
                });
            }
            self.gso_controls.push(UdpSegmentControl::new(
                group.segment_size.try_into().expect("validated UDP datagram size fits u16"),
            ));
        }

        let destination_count =
            if self.multicast_destination.is_some() { 1 } else { self.active_count };
        let message_count = group_count * destination_count;
        self.reserve_dynamic_messages(message_count);
        for group_index in 0..group_count {
            let group = &self.gso_groups[group_index];
            let segments = group.segment_indices.len();
            let expected_length = group.segment_size * segments;
            if self.multicast_destination.is_some() {
                let mut message: libc::mmsghdr = unsafe { core::mem::zeroed() };
                message.msg_hdr.msg_iov =
                    unsafe { self.batch_iovecs.as_mut_ptr().add(group.iovec_start) };
                message.msg_hdr.msg_iovlen = segments * 2;
                if segments > 1 {
                    message.msg_hdr.msg_control =
                        core::ptr::from_mut(&mut self.gso_controls[group_index])
                            .cast::<libc::c_void>();
                    message.msg_hdr.msg_controllen = UDP_SEGMENT_CONTROL_SPACE;
                }
                self.batch_messages.push(message);
                self.batch_message_tokens.push(MULTICAST_TOKEN);
                self.batch_expected_lengths.push(expected_length);
                self.batch_wire_datagrams.push(segments);
                continue;
            }
            for subscriber in &self.subs {
                let Some(destination) = subscriber.udp_destination.as_ref() else { continue };
                let mut message: libc::mmsghdr = unsafe { core::mem::zeroed() };
                message.msg_hdr.msg_name =
                    core::ptr::from_ref(&destination.address).cast_mut().cast::<libc::c_void>();
                message.msg_hdr.msg_namelen = destination.address_length;
                message.msg_hdr.msg_iov =
                    unsafe { self.batch_iovecs.as_mut_ptr().add(group.iovec_start) };
                message.msg_hdr.msg_iovlen = segments * 2;
                if segments > 1 {
                    message.msg_hdr.msg_control =
                        core::ptr::from_mut(&mut self.gso_controls[group_index])
                            .cast::<libc::c_void>();
                    message.msg_hdr.msg_controllen = UDP_SEGMENT_CONTROL_SPACE;
                }
                self.batch_messages.push(message);
                self.batch_message_tokens.push(subscriber.token);
                self.batch_expected_lengths.push(expected_length);
                self.batch_wire_datagrams.push(segments);
            }
        }
    }

    #[cfg_attr(feature = "profiling", flux_profiler::timed("udp.gso_copy_coalesce"))]
    fn prepare_copied_gso_messages(&mut self) {
        let mut group_count = 0;
        for datagram_index in 0..self.batch_datagram_count {
            let datagram = &self.batch_datagrams[datagram_index];
            let segment_size = datagram.len();
            let max_segments = GSO_MAX_SEGMENTS.min((GSO_MAX_PAYLOAD / segment_size).max(1));
            let existing = (0..group_count).rev().find(|&index| {
                let group = &self.gso_groups[index];
                group.segment_size == segment_size && group.segment_indices.len() < max_segments
            });
            let group_index = existing.unwrap_or_else(|| {
                if group_count == self.gso_groups.len() {
                    self.gso_groups.push(GsoGroup {
                        bytes: Vec::new(),
                        segment_size,
                        segment_indices: Vec::new(),
                        iovec_start: 0,
                    });
                }
                let index = group_count;
                let group = &mut self.gso_groups[index];
                group.bytes.clear();
                group.segment_size = segment_size;
                group.segment_indices.clear();
                group.iovec_start = 0;
                group_count += 1;
                index
            });
            let group = &mut self.gso_groups[group_index];
            group.bytes.extend_from_slice(datagram);
            group.segment_indices.push(datagram_index);
        }

        self.batch_iovecs.clear();
        self.batch_iovecs.reserve(group_count);
        self.gso_controls.clear();
        self.gso_controls.reserve(group_count);
        for group in &mut self.gso_groups[..group_count] {
            self.batch_iovecs.push(libc::iovec {
                iov_base: group.bytes.as_mut_ptr().cast::<libc::c_void>(),
                iov_len: group.bytes.len(),
            });
            self.gso_controls.push(UdpSegmentControl::new(
                group.segment_size.try_into().expect("validated UDP datagram size fits u16"),
            ));
        }

        let destination_count =
            if self.multicast_destination.is_some() { 1 } else { self.active_count };
        let message_count = group_count * destination_count;
        self.reserve_dynamic_messages(message_count);
        for group_index in 0..group_count {
            let group = &self.gso_groups[group_index];
            let segments = group.segment_indices.len();
            if self.multicast_destination.is_some() {
                let mut message: libc::mmsghdr = unsafe { core::mem::zeroed() };
                message.msg_hdr.msg_iov = &raw mut self.batch_iovecs[group_index];
                message.msg_hdr.msg_iovlen = 1;
                if segments > 1 {
                    message.msg_hdr.msg_control =
                        core::ptr::from_mut(&mut self.gso_controls[group_index])
                            .cast::<libc::c_void>();
                    message.msg_hdr.msg_controllen = UDP_SEGMENT_CONTROL_SPACE;
                }
                self.batch_messages.push(message);
                self.batch_message_tokens.push(MULTICAST_TOKEN);
                self.batch_expected_lengths.push(group.bytes.len());
                self.batch_wire_datagrams.push(segments);
                continue;
            }
            for subscriber in &self.subs {
                let Some(destination) = subscriber.udp_destination.as_ref() else {
                    continue;
                };
                let mut message: libc::mmsghdr = unsafe { core::mem::zeroed() };
                message.msg_hdr.msg_name =
                    core::ptr::from_ref(&destination.address).cast_mut().cast::<libc::c_void>();
                message.msg_hdr.msg_namelen = destination.address_length;
                message.msg_hdr.msg_iov = &raw mut self.batch_iovecs[group_index];
                message.msg_hdr.msg_iovlen = 1;
                if segments > 1 {
                    message.msg_hdr.msg_control =
                        core::ptr::from_mut(&mut self.gso_controls[group_index])
                            .cast::<libc::c_void>();
                    message.msg_hdr.msg_controllen = UDP_SEGMENT_CONTROL_SPACE;
                }
                self.batch_messages.push(message);
                self.batch_message_tokens.push(subscriber.token);
                self.batch_expected_lengths.push(group.bytes.len());
                self.batch_wire_datagrams.push(segments);
            }
        }
    }

    fn reserve_dynamic_messages(&mut self, message_count: usize) {
        self.batch_messages.clear();
        self.batch_message_tokens.clear();
        self.batch_expected_lengths.clear();
        self.batch_wire_datagrams.clear();
        self.batch_messages.reserve(message_count);
        self.batch_message_tokens.reserve(message_count);
        self.batch_expected_lengths.reserve(message_count);
        self.batch_wire_datagrams.reserve(message_count);
    }

    fn clear_dynamic_batch(&mut self) {
        self.batch_messages.clear();
        self.batch_message_tokens.clear();
        self.batch_expected_lengths.clear();
        self.batch_wire_datagrams.clear();
        self.batch_iovecs.clear();
        self.gso_controls.clear();
        self.gso_segments.clear();
        self.batch_datagram_count = 0;
    }

    #[cfg_attr(feature = "profiling", flux_profiler::timed("udp.fragment_fanout"))]
    fn enqueue_fragment(
        &mut self,
        socket: &mio::net::UdpSocket,
        header: FragmentHeader,
        payload: &[u8],
        failed_tokens: &mut Vec<Token>,
        stats: &mut UdpPublisherStats,
    ) -> bool {
        if self.multicast_destination.is_some() {
            if self.messages.len() == SEND_BATCH_SIZE && !self.flush(socket, failed_tokens, stats) {
                return false;
            }

            let iovec_index = self.fragment_count;
            header.encode(&mut self.headers[iovec_index]);
            self.iovecs[iovec_index][0] = libc::iovec {
                iov_base: self.headers[iovec_index].as_mut_ptr().cast::<libc::c_void>(),
                iov_len: UDP_HEADER_SIZE,
            };
            self.iovecs[iovec_index][1] = libc::iovec {
                iov_base: payload.as_ptr().cast_mut().cast::<libc::c_void>(),
                iov_len: payload.len(),
            };
            self.fragment_count += 1;

            let mut message: libc::mmsghdr = unsafe { core::mem::zeroed() };
            message.msg_hdr.msg_iov = self.iovecs[iovec_index].as_mut_ptr();
            message.msg_hdr.msg_iovlen = self.iovecs[iovec_index].len();

            let message_index = self.messages.len();
            self.message_tokens[message_index] = MULTICAST_TOKEN;
            self.expected_lengths[message_index] = UDP_HEADER_SIZE + payload.len();
            self.messages.push(message);
            return true;
        }

        let mut fragment_slot = None;
        for subscriber_index in 0..self.subs.len() {
            let token = self.subs[subscriber_index].token;
            if self.subs[subscriber_index].udp_destination.is_none() {
                continue;
            }
            if failed_tokens.contains(&token) {
                continue;
            }

            if self.messages.len() == SEND_BATCH_SIZE {
                if !self.flush(socket, failed_tokens, stats) {
                    return false;
                }
                fragment_slot = None;
            }

            let iovec_index = *fragment_slot.get_or_insert_with(|| {
                let index = self.fragment_count;
                header.encode(&mut self.headers[index]);
                self.iovecs[index][0] = libc::iovec {
                    iov_base: self.headers[index].as_mut_ptr().cast::<libc::c_void>(),
                    iov_len: UDP_HEADER_SIZE,
                };
                self.iovecs[index][1] = libc::iovec {
                    iov_base: payload.as_ptr().cast_mut().cast::<libc::c_void>(),
                    iov_len: payload.len(),
                };
                self.fragment_count += 1;
                index
            });

            let sub = &self.subs[subscriber_index];
            let destination =
                sub.udp_destination.as_ref().expect("subscribed subscriber has a UDP destination");
            let mut msg: libc::mmsghdr = unsafe { core::mem::zeroed() };
            msg.msg_hdr.msg_name =
                core::ptr::from_ref(&destination.address).cast_mut().cast::<libc::c_void>();
            msg.msg_hdr.msg_namelen = destination.address_length;
            msg.msg_hdr.msg_iov = self.iovecs[iovec_index].as_mut_ptr();
            msg.msg_hdr.msg_iovlen = self.iovecs[iovec_index].len();

            let message_index = self.messages.len();
            self.message_tokens[message_index] = token;
            self.expected_lengths[message_index] = UDP_HEADER_SIZE + payload.len();
            self.messages.push(msg);
        }
        true
    }

    fn flush(
        &mut self,
        socket: &mio::net::UdpSocket,
        failed_tokens: &mut Vec<Token>,
        stats: &mut UdpPublisherStats,
    ) -> bool {
        let wire_datagrams = [1; SEND_BATCH_SIZE];
        let sent_all = send_messages(
            socket,
            &mut self.messages,
            &self.message_tokens,
            &self.expected_lengths,
            &wire_datagrams,
            failed_tokens,
            stats,
        );
        self.clear_batch();
        sent_all
    }

    fn clear_batch(&mut self) {
        self.messages.clear();
        self.fragment_count = 0;
    }
}

#[cfg_attr(feature = "profiling", flux_profiler::timed("udp.sendmmsg"))]
fn send_messages(
    socket: &mio::net::UdpSocket,
    messages: &mut [libc::mmsghdr],
    message_tokens: &[Token],
    expected_lengths: &[usize],
    wire_datagrams: &[usize],
    failed_tokens: &mut Vec<Token>,
    stats: &mut UdpPublisherStats,
) -> bool {
    debug_assert!(message_tokens.len() >= messages.len());
    debug_assert!(expected_lengths.len() >= messages.len());
    debug_assert!(wire_datagrams.len() >= messages.len());

    let mut cursor = 0;
    while cursor < messages.len() {
        if failed_tokens.contains(&message_tokens[cursor]) {
            cursor += 1;
            continue;
        }

        let mut run_end = (cursor + SEND_BATCH_SIZE).min(messages.len());
        if let Some(failed_at) =
            (cursor..run_end).find(|&index| failed_tokens.contains(&message_tokens[index]))
        {
            run_end = failed_at;
        }
        if run_end == cursor {
            cursor += 1;
            continue;
        }

        stats.sendmmsg_calls += 1;
        let send_started = Instant::now();
        let sent = unsafe {
            libc::sendmmsg(
                socket.as_raw_fd(),
                messages.as_mut_ptr().add(cursor),
                (run_end - cursor) as libc::c_uint,
                libc::MSG_DONTWAIT,
            )
        };
        let send_elapsed_ns = u64::try_from(send_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        stats.total_sendmmsg_ns = stats.total_sendmmsg_ns.saturating_add(send_elapsed_ns);
        stats.max_sendmmsg_ns = stats.max_sendmmsg_ns.max(send_elapsed_ns);

        if sent > 0 {
            let sent = sent as usize;
            for index in cursor..cursor + sent {
                stats.send_entries += 1;
                stats.wire_datagrams += wire_datagrams[index] as u64;
                let written = messages[index].msg_len as usize;
                if written != expected_lengths[index] {
                    push_unique(failed_tokens, message_tokens[index]);
                }
            }
            cursor += sent;
            continue;
        }

        let error = if sent == 0 {
            io::Error::new(io::ErrorKind::WriteZero, "sendmmsg sent no datagrams")
        } else {
            io::Error::last_os_error()
        };
        if error.kind() == io::ErrorKind::WouldBlock {
            stats.sendmmsg_would_block += 1;
            return false;
        }
        push_unique(failed_tokens, message_tokens[cursor]);
        cursor += 1;
    }
    true
}

fn push_unique(tokens: &mut Vec<Token>, token: Token) {
    if !tokens.contains(&token) {
        tokens.push(token);
    }
}

fn probe_udp_segment(socket: &mio::net::UdpSocket) -> io::Result<()> {
    let disabled: libc::c_int = 0;
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_UDP,
            UDP_SEGMENT,
            core::ptr::from_ref(&disabled).cast::<libc::c_void>(),
            size_of_val(&disabled) as libc::socklen_t,
        )
    };
    if result == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

fn configure_multicast_sender(
    socket: &mio::net::UdpSocket,
    config: UdpMulticastConfig,
) -> io::Result<()> {
    let interface = libc::in_addr { s_addr: u32::from_ne_bytes(config.interface.octets()) };
    set_socket_option(socket, libc::IPPROTO_IP, libc::IP_MULTICAST_IF, &interface)?;
    set_socket_option(socket, libc::IPPROTO_IP, libc::IP_MULTICAST_TTL, &config.ttl)?;
    let loopback = u8::from(config.loopback);
    set_socket_option(socket, libc::IPPROTO_IP, libc::IP_MULTICAST_LOOP, &loopback)
}

fn set_socket_option<T>(
    socket: &mio::net::UdpSocket,
    level: libc::c_int,
    option: libc::c_int,
    value: &T,
) -> io::Result<()> {
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            level,
            option,
            core::ptr::from_ref(value).cast::<libc::c_void>(),
            size_of_val(value) as libc::socklen_t,
        )
    };
    if result == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}
