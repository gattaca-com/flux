use std::{
    collections::VecDeque,
    io,
    net::{Shutdown, SocketAddr},
};

use flux_timing::{Duration, Instant, Nanos, Repeater};
use flux_utils::safe_panic;
use mio::{Events, Interest, Poll, Token, event::Event};
use tracing::{debug, error, warn};

use crate::tcp::{
    ConnState, HANDSHAKE_ACCEPTED, HANDSHAKE_REJECTED, SendBehavior, TcpStream, TcpTelemetry,
    stream::{
        DEFAULT_TCP_USER_TIMEOUT_MS, FRAME_HEADER_SIZE, TcpTimers, build_frame_vec,
        set_socket_buf_size, set_user_timeout, write_frame_header,
    },
};

// Endpoints are few; keep the framed stream inline to avoid indirection on the
// connected IO path.
#[allow(clippy::large_enum_variant)]
enum EndpointState {
    Disconnected,
    Connecting(mio::net::TcpStream),
    WaitHandshakeAccept(crate::tcp::TcpStream),
    Connected(crate::tcp::TcpStream),
}

impl EndpointState {
    fn stream(&self) -> Option<&TcpStream> {
        match self {
            Self::Connected(stream) => Some(stream),
            Self::Disconnected | Self::Connecting(_) | Self::WaitHandshakeAccept(_) => None,
        }
    }

    fn stream_mut(&mut self) -> Option<&mut TcpStream> {
        match self {
            Self::Connected(stream) => Some(stream),
            Self::Disconnected | Self::Connecting(_) | Self::WaitHandshakeAccept(_) => None,
        }
    }
}

enum HandshakeResponse {
    Accepted,
    Rejected,
    Invalid,
}

struct Endpoint {
    token: Token,
    peer_addr: SocketAddr,
    state: EndpointState,
    timers: Option<TcpTimers>,
    send_backlog: VecDeque<Vec<u8>>,
    backlog_exceeded_since: Option<Instant>,
}

/// Event emitted by [`TcpClient::poll_with`].
pub enum ClientEvent<Payload> {
    /// A persistent endpoint established a usable connection. For a client
    /// configured with a handshake, this is emitted only after server
    /// acceptance.
    Connected { token: Token, peer_addr: SocketAddr },
    /// The server explicitly rejected this endpoint's handshake. The endpoint
    /// remains managed and will be retried.
    HandshakeRejected { token: Token, peer_addr: SocketAddr },
    /// An established connection was lost. The endpoint remains managed and
    /// will be retried until it is removed.
    Disconnect { token: Token, peer_addr: SocketAddr },
    /// A complete framed message was received.
    Message { token: Token, payload: Payload, send_ts: Nanos },
}

struct ClientManager {
    poll: Poll,
    endpoints: Vec<Endpoint>,
    reconnector: Repeater,
    handshake: Option<Vec<u8>>,
    on_connect_msg: Option<Vec<u8>>,
    telemetry: TcpTelemetry,
    socket_buf_size: Option<usize>,
    user_timeout_ms: u32,
    max_backlog: Option<(usize, Duration)>,
    drop_backlog_on_disconnect: bool,
    nodelay: bool,
    pending_disconnects: Vec<Token>,
    next_token: usize,
    bcast_header: [u8; FRAME_HEADER_SIZE],
    bcast_payload: Vec<u8>,
}

impl Default for ClientManager {
    fn default() -> Self {
        Self {
            poll: Poll::new().expect("couldn't set up a poll for tcp client"),
            endpoints: Vec::with_capacity(5),
            reconnector: Repeater::every(Duration::from_secs(2)),
            handshake: None,
            on_connect_msg: None,
            telemetry: TcpTelemetry::Disabled,
            socket_buf_size: None,
            user_timeout_ms: DEFAULT_TCP_USER_TIMEOUT_MS,
            max_backlog: None,
            drop_backlog_on_disconnect: false,
            nodelay: true,
            pending_disconnects: Vec::with_capacity(10),
            next_token: 0,
            bcast_header: [0; FRAME_HEADER_SIZE],
            bcast_payload: Vec::with_capacity(TcpStream::SEND_BUF_SIZE),
        }
    }
}

impl ClientManager {
    fn connect(&mut self, peer_addr: SocketAddr) -> Token {
        let token = Token(self.next_token);
        self.next_token += 1;
        let timers = TcpTimers::new_client(self.telemetry, token, peer_addr);
        self.endpoints.push(Endpoint {
            token,
            peer_addr,
            state: EndpointState::Disconnected,
            timers,
            send_backlog: VecDeque::with_capacity(64),
            backlog_exceeded_since: None,
        });
        self.start_connect(self.endpoints.len() - 1);
        token
    }

    fn start_connect(&mut self, index: usize) {
        let endpoint = &self.endpoints[index];
        debug_assert!(matches!(endpoint.state, EndpointState::Disconnected));
        let token = endpoint.token;
        let peer_addr = endpoint.peer_addr;

        let Ok(mut socket) = mio::net::TcpStream::connect(peer_addr)
            .inspect_err(|err| warn!(?err, %peer_addr, "couldn't start tcp connection"))
        else {
            return;
        };
        if let Some(size) = self.socket_buf_size {
            set_socket_buf_size(&socket, size);
        }
        if let Err(err) = self.poll.registry().register(&mut socket, token, Interest::WRITABLE) {
            error!(?err, %peer_addr, "couldn't register connecting tcp stream");
            return;
        }
        self.endpoints[index].state = EndpointState::Connecting(socket);
    }

    fn maybe_reconnect(&mut self) {
        if !self.reconnector.fired() {
            return;
        }
        for index in 0..self.endpoints.len() {
            if matches!(self.endpoints[index].state, EndpointState::Disconnected) {
                self.start_connect(index);
            }
        }
    }

    fn connect_complete(socket: &mio::net::TcpStream) -> io::Result<bool> {
        if let Some(err) = socket.take_error()? {
            return Err(err);
        }
        match socket.peer_addr() {
            Ok(_) => Ok(true),
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::NotConnected | io::ErrorKind::WouldBlock
                ) || matches!(err.raw_os_error(), Some(libc::EINPROGRESS | libc::EALREADY)) =>
            {
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    fn finish_connect<F>(&mut self, index: usize, handler: &mut F)
    where
        F: for<'a> FnMut(ClientEvent<&'a [u8]>),
    {
        let endpoint = &self.endpoints[index];
        let EndpointState::Connecting(socket) = &endpoint.state else { return };
        match Self::connect_complete(socket) {
            Ok(false) => return,
            Err(err) => {
                debug!(?err, peer_addr = %endpoint.peer_addr, "tcp connection attempt failed");
                self.disconnect_at_index(index);
                return;
            }
            Ok(true) => {}
        }

        let EndpointState::Connecting(mut socket) =
            std::mem::replace(&mut self.endpoints[index].state, EndpointState::Disconnected)
        else {
            unreachable!();
        };
        let token = self.endpoints[index].token;
        let peer_addr = self.endpoints[index].peer_addr;

        if self.nodelay &&
            let Err(err) = socket.set_nodelay(true)
        {
            warn!(?err, %peer_addr, "couldn't set nodelay on tcp client stream");
            let _ = self.poll.registry().deregister(&mut socket);
            let _ = socket.shutdown(Shutdown::Both);
            return;
        }
        set_user_timeout(&socket, self.user_timeout_ms);
        if let Err(err) = self.poll.registry().reregister(&mut socket, token, Interest::READABLE) {
            warn!(?err, %peer_addr, "couldn't register connected tcp client stream");
            let _ = socket.shutdown(Shutdown::Both);
            return;
        }

        let timers = self.endpoints[index].timers.take();
        let mut stream = TcpStream::from_client_stream(socket, token, peer_addr, timers);
        if let Some(handshake) = self.handshake.as_ref() {
            let state = stream.install_send_backlog(
                self.poll.registry(),
                VecDeque::new(),
                Some(handshake),
                None,
            );
            self.endpoints[index].state = EndpointState::WaitHandshakeAccept(stream);
            if state == ConnState::Disconnected {
                self.disconnect_at_index(index);
                return;
            }

            self.endpoints[index].backlog_exceeded_since = None;
            debug!(%peer_addr, "tcp client awaiting handshake response");
            return;
        }

        let backlog = std::mem::take(&mut self.endpoints[index].send_backlog);
        if stream.install_send_backlog(
            self.poll.registry(),
            backlog,
            None,
            self.on_connect_msg.as_ref(),
        ) == ConnState::Disconnected
        {
            self.endpoints[index].state = EndpointState::Connected(stream);
            self.disconnect_at_index(index);
            return;
        }

        self.endpoints[index].backlog_exceeded_since = None;
        self.endpoints[index].state = EndpointState::Connected(stream);
        debug!(%peer_addr, "tcp client connected");
        handler(ClientEvent::Connected { token, peer_addr });
    }

    fn handle_handshake_response<F>(&mut self, index: usize, event: &Event, handler: &mut F)
    where
        F: for<'a> FnMut(ClientEvent<&'a [u8]>),
    {
        let token = self.endpoints[index].token;
        let peer_addr = self.endpoints[index].peer_addr;
        let mut response = None;
        let state = {
            let EndpointState::WaitHandshakeAccept(stream) = &mut self.endpoints[index].state
            else {
                unreachable!();
            };
            stream.poll_handshake(self.poll.registry(), event, &mut |message| {
                response = Some(if message == HANDSHAKE_ACCEPTED {
                    HandshakeResponse::Accepted
                } else if message == HANDSHAKE_REJECTED {
                    HandshakeResponse::Rejected
                } else {
                    HandshakeResponse::Invalid
                });
            })
        };

        if state == ConnState::Disconnected {
            handler(ClientEvent::Disconnect { token, peer_addr });
            self.disconnect_at_index(index);
            return;
        }

        match response {
            None => {}
            Some(HandshakeResponse::Rejected) => {
                handler(ClientEvent::HandshakeRejected { token, peer_addr });
                self.disconnect_at_index(index);
            }
            Some(HandshakeResponse::Invalid) => {
                warn!(%peer_addr, "tcp client received invalid handshake response");
                handler(ClientEvent::Disconnect { token, peer_addr });
                self.disconnect_at_index(index);
            }
            Some(HandshakeResponse::Accepted) => {
                let EndpointState::WaitHandshakeAccept(mut stream) = std::mem::replace(
                    &mut self.endpoints[index].state,
                    EndpointState::Disconnected,
                ) else {
                    unreachable!();
                };
                let backlog = std::mem::take(&mut self.endpoints[index].send_backlog);
                let install_state = stream.install_send_backlog(
                    self.poll.registry(),
                    backlog,
                    None,
                    self.on_connect_msg.as_ref(),
                );
                self.endpoints[index].state = EndpointState::Connected(stream);
                self.endpoints[index].backlog_exceeded_since = None;
                if install_state == ConnState::Disconnected {
                    handler(ClientEvent::Disconnect { token, peer_addr });
                    self.disconnect_at_index(index);
                    return;
                }

                debug!(%peer_addr, "tcp client handshake accepted");
                handler(ClientEvent::Connected { token, peer_addr });

                let EndpointState::Connected(stream) = &mut self.endpoints[index].state else {
                    unreachable!();
                };
                if stream.poll_with(
                    self.poll.registry(),
                    event,
                    None,
                    &mut |token, payload, send_ts| {
                        handler(ClientEvent::Message { token, payload, send_ts });
                    },
                ) == ConnState::Disconnected
                {
                    handler(ClientEvent::Disconnect { token, peer_addr });
                    self.disconnect_at_index(index);
                }
            }
        }
    }

    fn handle_event<F>(&mut self, event: &Event, handler: &mut F)
    where
        F: for<'a> FnMut(ClientEvent<&'a [u8]>),
    {
        let token = event.token();
        let Some(index) = self.endpoints.iter().position(|endpoint| endpoint.token == token) else {
            safe_panic!("tcp client got event for unknown token");
            return;
        };

        match self.endpoints[index].state {
            EndpointState::Connecting(_) => self.finish_connect(index, handler),
            EndpointState::WaitHandshakeAccept(_) => {
                self.handle_handshake_response(index, event, handler);
            }
            EndpointState::Connected(_) => {
                let peer_addr = self.endpoints[index].peer_addr;
                let EndpointState::Connected(stream) = &mut self.endpoints[index].state else {
                    unreachable!();
                };
                if stream.poll_with(
                    self.poll.registry(),
                    event,
                    None,
                    &mut |token, payload, send_ts| {
                        handler(ClientEvent::Message { token, payload, send_ts });
                    },
                ) == ConnState::Disconnected
                {
                    handler(ClientEvent::Disconnect { token, peer_addr });
                    self.disconnect_at_index(index);
                }
            }
            EndpointState::Disconnected => {
                safe_panic!("tcp client got event for disconnected token");
            }
        }
    }

    fn disconnect_at_index(&mut self, index: usize) {
        let old_state =
            std::mem::replace(&mut self.endpoints[index].state, EndpointState::Disconnected);
        match old_state {
            EndpointState::Disconnected => {}
            EndpointState::Connecting(mut socket) => {
                let _ = self.poll.registry().deregister(&mut socket);
                let _ = socket.shutdown(Shutdown::Both);
            }
            EndpointState::WaitHandshakeAccept(mut stream) => {
                self.endpoints[index].timers = stream.take_timers();
                if self.drop_backlog_on_disconnect {
                    self.endpoints[index].send_backlog.clear();
                }
                stream.close(self.poll.registry());
            }
            EndpointState::Connected(mut stream) => {
                self.endpoints[index].timers = stream.take_timers();
                if self.drop_backlog_on_disconnect {
                    self.endpoints[index].send_backlog.clear();
                } else {
                    self.endpoints[index].send_backlog = stream.take_send_backlog();
                }
                stream.close(self.poll.registry());
            }
        }
        if self.drop_backlog_on_disconnect {
            self.endpoints[index].send_backlog.clear();
        }
        self.endpoints[index].backlog_exceeded_since = None;
    }

    fn disconnect_pending(&mut self, index: usize) {
        let token = self.endpoints[index].token;
        self.disconnect_at_index(index);
        self.pending_disconnects.push(token);
    }

    fn disconnect(&mut self, token: Token) {
        if let Some(index) = self.endpoints.iter().position(|endpoint| endpoint.token == token) {
            self.disconnect_at_index(index);
        }
    }

    fn disconnect_all(&mut self) {
        for index in 0..self.endpoints.len() {
            self.disconnect_at_index(index);
        }
    }

    fn remove(&mut self, token: Token) {
        let Some(index) = self.endpoints.iter().position(|endpoint| endpoint.token == token) else {
            return;
        };
        self.disconnect_at_index(index);
        self.endpoints.swap_remove(index);
        self.pending_disconnects.retain(|pending| *pending != token);
    }

    fn drain_pending_disconnects<F>(&mut self, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(ClientEvent<&'a [u8]>),
    {
        let worked = !self.pending_disconnects.is_empty();
        for token in self.pending_disconnects.drain(..) {
            if let Some(endpoint) = self.endpoints.iter().find(|endpoint| endpoint.token == token) {
                handler(ClientEvent::Disconnect { token, peer_addr: endpoint.peer_addr });
            }
        }
        worked
    }

    fn currently_disconnected(&self) -> impl Iterator<Item = Token> + '_ {
        self.endpoints.iter().filter_map(|endpoint| {
            (!matches!(endpoint.state, EndpointState::Connected(_))).then_some(endpoint.token)
        })
    }

    fn force_reconnect(&mut self) {
        self.reconnector.force_fire();
        self.maybe_reconnect();
    }

    fn backlog_len(endpoint: &Endpoint) -> usize {
        endpoint
            .state
            .stream()
            .map_or_else(|| endpoint.send_backlog.len(), |stream| stream.send_backlog.len())
    }

    fn backlog_exceeded(
        max_backlog: Option<(usize, Duration)>,
        endpoint: &mut Endpoint,
        additional_messages: usize,
    ) -> bool {
        let Some((max, timeout)) = max_backlog else { return false };
        if Self::backlog_len(endpoint).saturating_add(additional_messages) <= max {
            endpoint.backlog_exceeded_since = None;
            return false;
        }
        let now = Instant::now();
        let since = endpoint.backlog_exceeded_since.get_or_insert(now);
        now.saturating_sub(*since) >= timeout
    }

    fn push_disconnected_frame(
        max_backlog: Option<(usize, Duration)>,
        endpoint: &mut Endpoint,
        header: &[u8; FRAME_HEADER_SIZE],
        payload: &[u8],
    ) {
        if !Self::backlog_exceeded(max_backlog, endpoint, 1) {
            endpoint.send_backlog.push_back(build_frame_vec(header, payload));
        }
    }

    fn broadcast<F>(&mut self, serialise: &F)
    where
        F: Fn(&mut Vec<u8>),
    {
        self.bcast_payload.clear();
        serialise(&mut self.bcast_payload);
        if self.bcast_payload.is_empty() {
            return;
        }
        write_frame_header(&mut self.bcast_header, self.bcast_payload.len(), Nanos::now());

        for index in 0..self.endpoints.len() {
            if self.endpoints[index].state.stream().is_none() {
                if !self.drop_backlog_on_disconnect {
                    Self::push_disconnected_frame(
                        self.max_backlog,
                        &mut self.endpoints[index],
                        &self.bcast_header,
                        &self.bcast_payload,
                    );
                }
                continue;
            }

            let state = {
                let stream = self.endpoints[index].state.stream_mut().unwrap();
                stream.write_or_enqueue_shared(
                    self.poll.registry(),
                    &self.bcast_header,
                    &self.bcast_payload,
                )
            };
            let backlog_exceeded =
                Self::backlog_exceeded(self.max_backlog, &mut self.endpoints[index], 0);
            if state == ConnState::Disconnected {
                self.disconnect_pending(index);
                if !self.drop_backlog_on_disconnect {
                    Self::push_disconnected_frame(
                        self.max_backlog,
                        &mut self.endpoints[index],
                        &self.bcast_header,
                        &self.bcast_payload,
                    );
                }
            } else if backlog_exceeded {
                self.disconnect_pending(index);
            }
        }
    }

    fn write_or_enqueue_with<F>(&mut self, where_to: SendBehavior, serialise: F)
    where
        F: Fn(&mut Vec<u8>),
    {
        let SendBehavior::Single(token) = where_to else {
            self.broadcast(&serialise);
            return;
        };
        let Some(index) = self.endpoints.iter().position(|endpoint| endpoint.token == token) else {
            error!(?token, "tcp client send to unknown token");
            return;
        };

        self.bcast_payload.clear();
        serialise(&mut self.bcast_payload);
        if self.bcast_payload.is_empty() {
            return;
        }
        write_frame_header(&mut self.bcast_header, self.bcast_payload.len(), Nanos::now());

        if self.endpoints[index].state.stream().is_none() {
            if !self.drop_backlog_on_disconnect {
                Self::push_disconnected_frame(
                    self.max_backlog,
                    &mut self.endpoints[index],
                    &self.bcast_header,
                    &self.bcast_payload,
                );
            }
            return;
        }

        let state = {
            let stream = self.endpoints[index].state.stream_mut().unwrap();
            stream.write_or_enqueue_shared(
                self.poll.registry(),
                &self.bcast_header,
                &self.bcast_payload,
            )
        };
        let backlog_exceeded =
            Self::backlog_exceeded(self.max_backlog, &mut self.endpoints[index], 0);
        if state == ConnState::Disconnected {
            self.disconnect_pending(index);
            if !self.drop_backlog_on_disconnect {
                Self::push_disconnected_frame(
                    self.max_backlog,
                    &mut self.endpoints[index],
                    &self.bcast_header,
                    &self.bcast_payload,
                );
            }
        } else if backlog_exceeded {
            self.disconnect_pending(index);
        }
    }
}

/// Non-blocking manager for persistent outbound TCP endpoints.
///
/// Each [`Self::connect`] call creates one endpoint with a stable token. The
/// client attempts it immediately and retries from [`Self::poll_with`] until
/// [`Self::remove`] is called.
pub struct TcpClient {
    events: Events,
    manager: ClientManager,
}

impl Default for TcpClient {
    fn default() -> Self {
        Self { events: Events::with_capacity(128), manager: ClientManager::default() }
    }
}

impl TcpClient {
    pub fn with_reconnect_interval(mut self, interval: Duration) -> Self {
        self.manager.reconnector = Repeater::every(interval);
        self
    }

    /// Sends `message` as the first frame on every connection attempt.
    ///
    /// A [`TcpServer`] configured with
    /// [`TcpServer::with_handshake_handler`](super::TcpServer::with_handshake_handler)
    /// consumes this frame before admitting the connection. The handshake is
    /// sent before the on-connect message and any queued application messages.
    /// Application writes remain in the client backlog until the server
    /// accepts the handshake.
    /// [`ClientEvent::Connected`] is delayed until the server accepts it;
    /// rejection emits [`ClientEvent::HandshakeRejected`].
    pub fn with_handshake(mut self, message: Vec<u8>) -> Self {
        assert!(!message.is_empty(), "handshake message cannot be empty");
        assert!(
            message.len() <= TcpStream::SEND_BUF_SIZE,
            "handshake message exceeds send buffer size"
        );
        self.manager.handshake = Some(message);
        self
    }

    pub fn with_on_connect_msg(mut self, msg: Vec<u8>) -> Self {
        assert!(msg.len() <= TcpStream::SEND_BUF_SIZE, "on_connect_msg exceeds send buffer size");
        self.manager.on_connect_msg = Some(msg);
        self
    }

    pub fn with_telemetry(mut self, telemetry: TcpTelemetry) -> Self {
        self.manager.telemetry = telemetry;
        self
    }

    pub fn with_socket_buf_size(mut self, size: usize) -> Self {
        self.manager.socket_buf_size = Some(size);
        self
    }

    pub fn with_user_timeout(mut self, timeout_ms: u32) -> Self {
        self.manager.user_timeout_ms = timeout_ms;
        self
    }

    pub fn with_nodelay(mut self, nodelay: bool) -> Self {
        self.manager.nodelay = nodelay;
        self
    }

    pub fn with_max_backlog(mut self, max: usize, timeout: Duration) -> Self {
        self.manager.max_backlog = Some((max, timeout));
        self
    }

    pub fn with_drop_backlog_on_disconnect(mut self, enabled: bool) -> Self {
        self.manager.drop_backlog_on_disconnect = enabled;
        self
    }

    /// Adds a persistent outbound endpoint and returns its stable token.
    #[must_use = "the token identifies the persistent outbound endpoint"]
    pub fn connect(&mut self, peer_addr: SocketAddr) -> Token {
        self.manager.connect(peer_addr)
    }

    /// Polls sockets once and dispatches lifecycle and message events.
    pub fn poll_with<F>(&mut self, mut handler: F) -> bool
    where
        F: for<'a> FnMut(ClientEvent<&'a [u8]>),
    {
        let mut worked = self.manager.drain_pending_disconnects(&mut handler);
        self.manager.maybe_reconnect();
        if let Err(err) = self.manager.poll.poll(&mut self.events, Some(std::time::Duration::ZERO))
        {
            safe_panic!("tcp client poll failed: {err}");
            return false;
        }
        for event in &self.events {
            worked = true;
            self.manager.handle_event(event, &mut handler);
        }
        worked |= self.manager.drain_pending_disconnects(&mut handler);
        worked
    }

    pub fn write_or_enqueue_with<F>(&mut self, where_to: SendBehavior, serialise: F)
    where
        F: Fn(&mut Vec<u8>),
    {
        self.manager.write_or_enqueue_with(where_to, serialise);
    }

    /// Closes an endpoint's current socket and schedules it for reconnection.
    pub fn disconnect(&mut self, token: Token) {
        self.manager.disconnect(token);
    }

    /// Closes every current socket and schedules all endpoints for reconnect.
    pub fn disconnect_all(&mut self) {
        self.manager.disconnect_all();
    }

    /// Permanently removes an endpoint and its queued messages.
    pub fn remove(&mut self, token: Token) {
        self.manager.remove(token);
    }

    /// Returns endpoints that are disconnected or still connecting.
    pub fn currently_disconnected(&self) -> impl Iterator<Item = Token> + '_ {
        self.manager.currently_disconnected()
    }

    /// Immediately retries endpoints that are currently disconnected.
    pub fn force_reconnect(&mut self) {
        self.manager.force_reconnect();
    }
}
