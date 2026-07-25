use std::os::fd::AsRawFd;
use std::{io, net::SocketAddr};

use flux_timing::Repeater;
use mio::Token;
use tracing::warn;

use super::{
    NativeSocketAddr, UdpConfig,
    control::{self, PublisherMessage, SubscriberMessage},
    wire::{FragmentHeader, UDP_HEADER_SIZE, encode_fragments},
};
use crate::tcp::{SendBehavior, ServerEvent, TcpServer, set_socket_buf_size};

pub enum PublisherEvent {
    // not necessarily subscribed yet
    Connected { addr: SocketAddr },
    Disconnect { addr: SocketAddr },
}

/// Non-blocking directional publisher for reliable, unordered UDP messages.
pub struct UdpPublisher {
    config: UdpConfig,
    udp_socket: mio::net::UdpSocket,
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
}

impl UdpPublisher {
    /// Make sure the same config is used for the subscriber
    pub fn new_with_config(addr: SocketAddr, config: UdpConfig) -> io::Result<Self> {
        config.validate()?;

        let socket = mio::net::UdpSocket::bind(addr)?;
        if let Some(size) = config.socket_buf_size {
            set_socket_buf_size(&socket, size);
        }
        let mut repair =
            TcpServer::default().with_nodelay(true).with_drop_backlog_on_disconnect(true);
        if let Some(size) = config.socket_buf_size {
            repair = repair.with_socket_buf_size(size);
        }
        repair.try_listen_at(addr)?;

        let mut history = Vec::with_capacity(config.sequence_window);
        history.resize_with(config.sequence_window, Vec::new);
        let mut progress_repeater = Repeater::every(config.progress_interval.into());
        progress_repeater.reset();

        Ok(Self {
            config,
            udp_socket: socket,
            repair,
            session_id: rand::random(),
            history,
            history_mask: config.sequence_window as u64 - 1,
            next_sequence: 0,
            progress_repeater,
            to_disconnect: Vec::with_capacity(64),
            pending_control: Vec::with_capacity(64),
            subscribers: Subscribers::new(),
        })
    }

    pub fn active_subscribers(&self) -> usize {
        self.subscribers.len()
    }

    pub fn publish_with<F, G>(&mut self, handler: F, serialise: G)
    where
        F: FnMut(PublisherEvent),
        G: FnOnce(&mut Vec<u8>),
    {
        let sequence = self.next_sequence;
        let index = (sequence & self.history_mask) as usize;
        let payload = &mut self.history[index];
        payload.clear();
        serialise(payload);
        assert!(payload.len() < u32::MAX as usize, "payload too big");

        self.next_sequence += 1;
        if self.subscribers.is_empty() {
            return;
        }

        self.subscribers.broadcast(
            &self.udp_socket,
            self.config.max_datagram_size,
            self.session_id,
            sequence,
            payload,
            &mut self.to_disconnect,
        );

        self.disconnect_pending(handler);
    }

    pub fn poll_with<F>(&mut self, mut handler: F)
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

    fn disconnect_pending<F>(&mut self, mut handler: F)
    where
        F: FnMut(PublisherEvent),
    {
        if self.to_disconnect.is_empty() {
            return;
        }
        for token in self.to_disconnect.drain(..) {
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

struct Subscribers {
    subs: Vec<Subscriber>,
    active_count: usize,
    headers: [[u8; UDP_HEADER_SIZE]; SEND_BATCH_SIZE],
    iovecs: [[libc::iovec; 2]; SEND_BATCH_SIZE],
    messages: Vec<libc::mmsghdr>,
    message_tokens: [Token; SEND_BATCH_SIZE],
    expected_lengths: [usize; SEND_BATCH_SIZE],
    fragment_count: usize,
}

impl Subscribers {
    fn new() -> Self {
        Self {
            subs: Vec::with_capacity(64),
            active_count: 0,
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

    /// Batches up to 64 UDP datagrams across fragments and subscribers per `sendmmsg`.
    /// Payloads are not copied; the fixed batch stores one `mmsghdr`, token, and
    /// expected length per datagram, plus one shared header/iovec pair per fragment.
    fn broadcast(
        &mut self,
        socket: &mio::net::UdpSocket,
        max_datagram_size: usize,
        session_id: u32,
        sequence: u64,
        message: &[u8],
        failed_tokens: &mut Vec<Token>,
    ) {
        self.messages.clear();
        self.fragment_count = 0;

        let encoded_all = encode_fragments(
            max_datagram_size,
            session_id,
            sequence,
            message,
            |header, fragment| self.enqueue_fragment(socket, header, fragment, failed_tokens),
        );
        if encoded_all {
            let _ = self.flush(socket, failed_tokens);
        }
    }

    fn enqueue_fragment(
        &mut self,
        socket: &mio::net::UdpSocket,
        header: FragmentHeader,
        payload: &[u8],
        failed_tokens: &mut Vec<Token>,
    ) -> bool {
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
                if !self.flush(socket, failed_tokens) {
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

    fn flush(&mut self, socket: &mio::net::UdpSocket, failed_tokens: &mut Vec<Token>) -> bool {
        let message_count = self.messages.len();
        let mut cursor = 0;
        while cursor < message_count {
            if failed_tokens.contains(&self.message_tokens[cursor]) {
                cursor += 1;
                continue;
            }

            let mut run_end = cursor + 1;
            while run_end < message_count && !failed_tokens.contains(&self.message_tokens[run_end])
            {
                run_end += 1;
            }

            let sent = unsafe {
                libc::sendmmsg(
                    socket.as_raw_fd(),
                    self.messages.as_mut_ptr().add(cursor),
                    (run_end - cursor) as libc::c_uint,
                    libc::MSG_DONTWAIT,
                )
            };

            if sent > 0 {
                let sent = sent as usize;
                for index in cursor..cursor + sent {
                    let written = self.messages[index].msg_len as usize;
                    if written != self.expected_lengths[index] {
                        push_unique(failed_tokens, self.message_tokens[index]);
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
                self.clear_batch();
                return false;
            }
            push_unique(failed_tokens, self.message_tokens[cursor]);
            cursor += 1;
        }

        self.clear_batch();
        true
    }

    fn clear_batch(&mut self) {
        self.messages.clear();
        self.fragment_count = 0;
    }
}

fn push_unique(tokens: &mut Vec<Token>, token: Token) {
    if !tokens.contains(&token) {
        tokens.push(token);
    }
}
