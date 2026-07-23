use std::net::{Shutdown, SocketAddr};

use flux::spine::{SpineProducerWithDCache, SpineProducers};
use flux_timing::{Duration, Instant, Nanos};
use flux_utils::{DCachePtr, safe_panic};
use mio::{Events, Interest, Poll, Token, event::Event, net::TcpListener};
use tracing::{error, info, warn};

use crate::tcp::{
    ConnState, HANDSHAKE_ACCEPTED, HANDSHAKE_REJECTED, SendBehavior, TcpStream, TcpTelemetry,
    stream::{
        DEFAULT_TCP_USER_TIMEOUT_MS, FRAME_HEADER_SIZE, set_socket_buf_size, set_user_timeout,
        write_frame_header,
    },
};

/// Event emitted by [`TcpServer::poll_with`] and
/// [`TcpServer::poll_with_produce`].
pub enum ServerEvent<Payload> {
    /// A listener accepted a new connection.
    Accept { listener: Token, stream: Token, peer_addr: SocketAddr },
    /// An accepted connection was closed.
    Disconnect { token: Token },
    /// A complete framed message was received.
    Message { token: Token, payload: Payload, send_ts: Nanos },
}

type HandshakeHandler = Box<dyn FnMut(&[u8]) -> bool + Send>;

#[allow(clippy::large_enum_variant)]
enum EndpointState {
    WaitHandshake { stream: TcpStream, accepted_at: Instant },
    SendHandshakeReject { stream: TcpStream, accepted_at: Instant },
    Connected(TcpStream),
}

impl EndpointState {
    fn stream_mut(&mut self) -> &mut TcpStream {
        match self {
            Self::WaitHandshake { stream, .. } |
            Self::SendHandshakeReject { stream, .. } |
            Self::Connected(stream) => stream,
        }
    }

    fn pending_since(&self) -> Option<Instant> {
        match self {
            Self::WaitHandshake { accepted_at, .. } |
            Self::SendHandshakeReject { accepted_at, .. } => Some(*accepted_at),
            Self::Connected(_) => None,
        }
    }
}

struct Endpoint {
    listener: Token,
    peer_addr: SocketAddr,
    state: EndpointState,
}

struct ServerManager {
    poll: Poll,
    listeners: Vec<(Token, TcpListener)>,
    endpoints: Vec<(Token, Endpoint)>,
    oldest_pending_handshake: Option<Instant>,
    handshake_handler: Option<HandshakeHandler>,
    handshake_timeout: Duration,
    on_connect_msg: Option<Vec<u8>>,
    telemetry: TcpTelemetry,
    socket_buf_size: Option<usize>,
    user_timeout_ms: u32,
    dcache: Option<DCachePtr>,
    max_backlog: Option<(usize, Duration)>,
    nodelay: bool,
    pending_disconnects: Vec<Token>,
    next_token: usize,
    bcast_header: [u8; FRAME_HEADER_SIZE],
    bcast_payload: Vec<u8>,
}

impl Default for ServerManager {
    fn default() -> Self {
        Self {
            poll: Poll::new().expect("couldn't set up a poll for tcp server"),
            listeners: Vec::with_capacity(2),
            endpoints: Vec::with_capacity(8),
            oldest_pending_handshake: None,
            handshake_handler: None,
            handshake_timeout: Duration::ZERO,
            on_connect_msg: None,
            telemetry: TcpTelemetry::Disabled,
            socket_buf_size: None,
            user_timeout_ms: DEFAULT_TCP_USER_TIMEOUT_MS,
            dcache: None,
            max_backlog: None,
            nodelay: true,
            pending_disconnects: Vec::with_capacity(10),
            next_token: 0,
            bcast_header: [0; FRAME_HEADER_SIZE],
            bcast_payload: Vec::with_capacity(TcpStream::SEND_BUF_SIZE),
        }
    }
}

impl ServerManager {
    fn push_endpoint(&mut self, token: Token, endpoint: Endpoint) {
        if let Some(accepted_at) = endpoint.state.pending_since() {
            self.oldest_pending_handshake = Some(
                self.oldest_pending_handshake.map_or(accepted_at, |oldest| oldest.min(accepted_at)),
            );
        }
        debug_assert!(self.endpoint_index(token).is_none());
        self.endpoints.push((token, endpoint));
    }

    fn endpoint_index(&self, token: Token) -> Option<usize> {
        self.endpoints.iter().position(|(candidate, _)| *candidate == token)
    }

    fn listen_at(&mut self, addr: SocketAddr) -> Option<Token> {
        let mut listener = TcpListener::bind(addr)
            .inspect_err(|err| warn!(?err, %addr, "couldn't start tcp listener"))
            .ok()?;
        let token = Token(self.next_token);
        self.poll
            .registry()
            .register(&mut listener, token, Interest::READABLE)
            .inspect_err(|err| warn!(?err, %addr, "couldn't register tcp listener"))
            .ok()?;
        self.next_token += 1;
        self.listeners.push((token, listener));
        Some(token)
    }

    fn accept_connections<F>(&mut self, listener_index: usize, handler: &mut F)
    where
        F: for<'a> FnMut(ServerEvent<&'a [u8]>),
    {
        let listener_token = self.listeners[listener_index].0;
        loop {
            let Ok((mut socket, peer_addr)) = self.listeners[listener_index].1.accept() else {
                return;
            };
            info!(%peer_addr, "tcp server accepted connection");
            if let Some(size) = self.socket_buf_size {
                set_socket_buf_size(&socket, size);
            }
            let token = Token(self.next_token);
            if let Err(err) = self.poll.registry().register(&mut socket, token, Interest::READABLE)
            {
                error!(?err, %peer_addr, "couldn't register accepted tcp stream");
                let _ = socket.shutdown(Shutdown::Both);
                continue;
            }
            if self.nodelay &&
                let Err(err) = socket.set_nodelay(true)
            {
                error!(?err, %peer_addr, "couldn't set nodelay on accepted tcp stream");
                let _ = self.poll.registry().deregister(&mut socket);
                let _ = socket.shutdown(Shutdown::Both);
                continue;
            }
            set_user_timeout(&socket, self.user_timeout_ms);

            let mut stream = if self.handshake_handler.is_some() {
                TcpStream::from_stream_for_handshake(
                    socket,
                    token,
                    peer_addr,
                    self.telemetry,
                    self.dcache.is_some(),
                )
            } else {
                TcpStream::from_stream_with_telemetry(
                    socket,
                    token,
                    peer_addr,
                    self.telemetry,
                    self.dcache.is_some(),
                )
            };
            self.next_token += 1;
            if self.handshake_handler.is_some() {
                let accepted_at = Instant::now();
                self.push_endpoint(token, Endpoint {
                    listener: listener_token,
                    peer_addr,
                    state: EndpointState::WaitHandshake { stream, accepted_at },
                });
                continue;
            }

            if let Some(message) = &self.on_connect_msg &&
                stream.write_or_enqueue_with(self.poll.registry(), |buf| {
                    buf.extend_from_slice(message);
                }) == ConnState::Disconnected
            {
                stream.close(self.poll.registry());
                continue;
            }

            handler(ServerEvent::Accept { listener: listener_token, stream: token, peer_addr });
            self.push_endpoint(token, Endpoint {
                listener: listener_token,
                peer_addr,
                state: EndpointState::Connected(stream),
            });
        }
    }

    fn take_endpoint_at(&mut self, index: usize) -> Endpoint {
        let (_, endpoint) = self.endpoints.swap_remove(index);
        if endpoint
            .state
            .pending_since()
            .is_some_and(|accepted_at| self.oldest_pending_handshake == Some(accepted_at))
        {
            self.oldest_pending_handshake = self
                .endpoints
                .iter()
                .filter_map(|(_, endpoint)| endpoint.state.pending_since())
                .min();
        }
        endpoint
    }

    fn handle_pending_handshake<F>(&mut self, token: Token, event: &Event, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(ServerEvent<&'a [u8]>),
    {
        let Some(index) = self.endpoint_index(token) else {
            safe_panic!("tcp server got event for unknown token");
            return false;
        };
        if matches!(self.endpoints[index].1.state, EndpointState::SendHandshakeReject { .. }) {
            let (state, backlog_empty) = {
                let endpoint = &mut self.endpoints[index].1;
                let EndpointState::SendHandshakeReject { stream, .. } = &mut endpoint.state else {
                    unreachable!();
                };
                let state = stream.poll_writable(self.poll.registry(), event);
                (state, stream.send_backlog.is_empty())
            };
            if state == ConnState::Disconnected || backlog_empty {
                self.disconnect_index(index);
            }
            return false;
        }

        let mut accepted = None;
        let state = {
            let endpoint = &mut self.endpoints[index].1;
            let EndpointState::WaitHandshake { stream, .. } = &mut endpoint.state else {
                safe_panic!("tcp server handled handshake for connected endpoint");
                return true;
            };
            let handshake_handler =
                self.handshake_handler.as_mut().expect("pending handshake without handler");
            stream.poll_handshake(self.poll.registry(), event, &mut |message| {
                accepted = Some(handshake_handler(message));
            })
        };

        if accepted == Some(true) && state == ConnState::Alive {
            let mut endpoint = self.take_endpoint_at(index);
            let EndpointState::WaitHandshake { mut stream, .. } = endpoint.state else {
                unreachable!();
            };
            if stream.write_or_enqueue_with(self.poll.registry(), |buf| {
                buf.extend_from_slice(HANDSHAKE_ACCEPTED);
            }) == ConnState::Disconnected
            {
                stream.close(self.poll.registry());
                return false;
            }
            if let Some(message) = &self.on_connect_msg &&
                stream.write_or_enqueue_with(self.poll.registry(), |buf| {
                    buf.extend_from_slice(message);
                }) == ConnState::Disconnected
            {
                stream.close(self.poll.registry());
                return false;
            }
            if self.dcache.is_some() {
                stream.enable_dcache();
            }
            handler(ServerEvent::Accept {
                listener: endpoint.listener,
                stream: token,
                peer_addr: endpoint.peer_addr,
            });
            endpoint.state = EndpointState::Connected(stream);
            self.push_endpoint(token, endpoint);
            true
        } else if accepted == Some(false) {
            info!(
                peer_addr = %self.endpoints[index].1.peer_addr,
                "tcp server rejected handshake"
            );
            let (send_state, backlog_empty) = {
                let endpoint = &mut self.endpoints[index].1;
                let EndpointState::WaitHandshake { stream, .. } = &mut endpoint.state else {
                    unreachable!();
                };
                let send_state = stream.write_or_enqueue_with(self.poll.registry(), |buf| {
                    buf.extend_from_slice(HANDSHAKE_REJECTED);
                });
                (send_state, stream.send_backlog.is_empty())
            };
            if send_state == ConnState::Disconnected || backlog_empty {
                self.disconnect_index(index);
            } else {
                let mut endpoint = self.take_endpoint_at(index);
                let EndpointState::WaitHandshake { stream, accepted_at } = endpoint.state else {
                    unreachable!();
                };
                endpoint.state = EndpointState::SendHandshakeReject { stream, accepted_at };
                self.push_endpoint(token, endpoint);
            }
            false
        } else if state == ConnState::Disconnected {
            self.disconnect_index(index);
            false
        } else {
            false
        }
    }

    fn handle_event<F>(&mut self, event: &Event, handler: &mut F)
    where
        F: for<'a> FnMut(ServerEvent<&'a [u8]>),
    {
        let token = event.token();
        if let Some(listener_index) =
            self.listeners.iter().position(|(candidate, _)| *candidate == token)
        {
            self.accept_connections(listener_index, handler);
            return;
        }
        let Some(mut index) = self.endpoint_index(token) else {
            safe_panic!("tcp server got event for unknown token");
            return;
        };
        if !matches!(self.endpoints[index].1.state, EndpointState::Connected(_)) {
            if !self.handle_pending_handshake(token, event, handler) {
                return;
            }
            let Some(admitted_index) = self.endpoint_index(token) else {
                safe_panic!("tcp server got event for unknown token");
                return;
            };
            index = admitted_index;
        }
        let EndpointState::Connected(stream) = &mut self.endpoints[index].1.state else {
            safe_panic!("tcp server admitted endpoint did not transition to connected");
            return;
        };
        if stream.poll_with(
            self.poll.registry(),
            event,
            self.dcache.as_deref(),
            &mut |token, payload, send_ts| {
                handler(ServerEvent::Message { token, payload, send_ts });
            },
        ) == ConnState::Disconnected
        {
            handler(ServerEvent::Disconnect { token });
            self.disconnect_index(index);
        }
    }

    fn handle_event_produce<T, P, F>(&mut self, event: &Event, produce: &mut P, handler: &mut F)
    where
        T: 'static + Copy,
        P: SpineProducers + AsRef<SpineProducerWithDCache<T>>,
        F: for<'a> FnMut(ServerEvent<&'a [u8]>) -> Option<T>,
    {
        let token = event.token();
        if let Some(listener_index) =
            self.listeners.iter().position(|(candidate, _)| *candidate == token)
        {
            self.accept_connections(listener_index, &mut |event| {
                let _ = handler(event);
            });
            return;
        }
        let Some(mut index) = self.endpoint_index(token) else {
            safe_panic!("tcp server got event for unknown token");
            return;
        };
        if !matches!(self.endpoints[index].1.state, EndpointState::Connected(_)) {
            let admitted = self.handle_pending_handshake(token, event, &mut |event| {
                let _ = handler(event);
            });
            if !admitted {
                return;
            }
            let Some(admitted_index) = self.endpoint_index(token) else {
                safe_panic!("tcp server got event for unknown token");
                return;
            };
            index = admitted_index;
        }
        let dcache = self.dcache.as_deref().expect("dcache required for poll_with_produce");
        let EndpointState::Connected(stream) = &mut self.endpoints[index].1.state else {
            safe_panic!("tcp server admitted endpoint did not transition to connected");
            return;
        };
        if stream.poll_with_produce(
            self.poll.registry(),
            event,
            dcache,
            produce,
            &mut |token, payload, send_ts| {
                handler(ServerEvent::Message { token, payload, send_ts })
            },
        ) == ConnState::Disconnected
        {
            let _ = handler(ServerEvent::Disconnect { token });
            self.disconnect_index(index);
        }
    }

    fn disconnect_index(&mut self, index: usize) {
        let mut endpoint = self.take_endpoint_at(index);
        endpoint.state.stream_mut().close(self.poll.registry());
    }

    fn expire_pending_handshakes(&mut self) -> bool {
        let Some(oldest) = self.oldest_pending_handshake else {
            return false;
        };
        debug_assert!(self.handshake_timeout != Duration::ZERO);
        if oldest.elapsed() < self.handshake_timeout {
            return false;
        }

        let now = Instant::now();
        let mut expired = false;
        let mut index = self.endpoints.len();
        while index != 0 {
            index -= 1;
            let should_expire =
                self.endpoints[index].1.state.pending_since().is_some_and(|accepted_at| {
                    now.saturating_sub(accepted_at) >= self.handshake_timeout
                });
            if should_expire {
                let mut endpoint = self.take_endpoint_at(index);
                info!(
                    peer_addr = %endpoint.peer_addr,
                    timeout = %self.handshake_timeout,
                    "tcp server handshake timed out"
                );
                endpoint.state.stream_mut().close(self.poll.registry());
                expired = true;
            }
        }
        self.oldest_pending_handshake =
            self.endpoints.iter().filter_map(|(_, endpoint)| endpoint.state.pending_since()).min();
        expired
    }

    fn disconnect(&mut self, token: Token) {
        if let Some(index) = self.endpoint_index(token) &&
            matches!(self.endpoints[index].1.state, EndpointState::Connected(_))
        {
            self.disconnect_index(index);
        }
    }

    fn disconnect_all(&mut self) {
        while !self.endpoints.is_empty() {
            self.disconnect_index(self.endpoints.len() - 1);
        }
        self.oldest_pending_handshake = None;
    }

    fn remove_listener(&mut self, token: Token) {
        let Some(index) = self.listeners.iter().position(|(candidate, _)| *candidate == token)
        else {
            return;
        };
        let (_, mut listener) = self.listeners.swap_remove(index);
        let _ = self.poll.registry().deregister(&mut listener);
    }

    fn drain_pending_disconnects<F>(&mut self, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(ServerEvent<&'a [u8]>),
    {
        let worked = !self.pending_disconnects.is_empty();
        for token in self.pending_disconnects.drain(..) {
            handler(ServerEvent::Disconnect { token });
        }
        worked
    }

    fn backlog_exceeded(max_backlog: Option<(usize, Duration)>, stream: &mut TcpStream) -> bool {
        let Some((max, timeout)) = max_backlog else { return false };
        if stream.send_backlog.len() <= max {
            stream.backlog_exceeded_since = None;
            return false;
        }
        let now = Instant::now();
        let since = stream.backlog_exceeded_since.get_or_insert(now);
        now.saturating_sub(*since) >= timeout
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

        let mut index = self.endpoints.len();
        while index != 0 {
            index -= 1;
            let (state, backlog_exceeded) = {
                let endpoint = &mut self.endpoints[index].1;
                let EndpointState::Connected(stream) = &mut endpoint.state else {
                    continue;
                };
                let state = stream.write_or_enqueue_shared(
                    self.poll.registry(),
                    &self.bcast_header,
                    &self.bcast_payload,
                );
                let backlog_exceeded = Self::backlog_exceeded(self.max_backlog, stream);
                (state, backlog_exceeded)
            };
            if state == ConnState::Disconnected || backlog_exceeded {
                let token = self.endpoints[index].0;
                self.disconnect_index(index);
                self.pending_disconnects.push(token);
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
        let Some(index) = self.endpoint_index(token) else {
            error!(?token, "tcp server send to unknown stream");
            return;
        };
        let EndpointState::Connected(stream) = &mut self.endpoints[index].1.state else {
            error!(?token, "tcp server send to endpoint before handshake completed");
            return;
        };
        let disconnect = stream.write_or_enqueue_with(self.poll.registry(), serialise) ==
            ConnState::Disconnected ||
            Self::backlog_exceeded(self.max_backlog, stream);
        if disconnect {
            self.disconnect_index(index);
            self.pending_disconnects.push(token);
        }
    }
}

/// Non-blocking TCP listener and accepted-connection manager.
pub struct TcpServer {
    events: Events,
    manager: ServerManager,
}

impl Default for TcpServer {
    fn default() -> Self {
        Self { events: Events::with_capacity(128), manager: ServerManager::default() }
    }
}

impl TcpServer {
    /// Validates the first frame received from each accepted socket.
    ///
    /// Connections are not surfaced through [`ServerEvent::Accept`] until the
    /// handler returns `true`. The handshake frame is consumed internally and
    /// is never emitted as [`ServerEvent::Message`]. The result is returned to
    /// a handshake-enabled [`TcpClient`](super::TcpClient); returning `false`
    /// closes the socket without emitting a server event.
    pub fn with_handshake_handler<F>(mut self, handler: F) -> Self
    where
        F: FnMut(&[u8]) -> bool + Send + 'static,
    {
        if self.manager.handshake_timeout == Duration::ZERO {
            self.manager.handshake_timeout = Duration::from_secs(5);
        }
        self.manager.handshake_handler = Some(Box::new(handler));
        self
    }

    /// Sets the maximum time an accepted socket may spend waiting for its
    /// complete handshake. The default is five seconds.
    ///
    /// Expired sockets are closed without emitting a [`ServerEvent`].
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        assert!(timeout.0 != 0, "handshake timeout cannot be zero");
        self.manager.handshake_timeout = timeout;
        self
    }

    pub fn with_on_connect_msg(mut self, msg: Vec<u8>) -> Self {
        assert!(msg.len() <= TcpStream::SEND_BUF_SIZE, "on_connect_msg exceeds send buffer size");
        self.manager.on_connect_msg = Some(msg);
        self
    }

    pub fn with_dcache(mut self, dcache: DCachePtr) -> Self {
        self.manager.dcache = Some(dcache);
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

    pub fn listen_at(&mut self, addr: SocketAddr) -> Option<Token> {
        self.manager.listen_at(addr)
    }

    pub fn poll_with<F>(&mut self, mut handler: F) -> bool
    where
        F: for<'a> FnMut(ServerEvent<&'a [u8]>),
    {
        let mut worked = self.manager.expire_pending_handshakes();
        worked |= self.manager.drain_pending_disconnects(&mut handler);
        if let Err(err) = self.manager.poll.poll(&mut self.events, Some(std::time::Duration::ZERO))
        {
            safe_panic!("tcp server poll failed: {err}");
            return false;
        }
        for event in &self.events {
            worked = true;
            self.manager.handle_event(event, &mut handler);
        }
        worked |= self.manager.drain_pending_disconnects(&mut handler);
        worked
    }

    pub fn poll_with_produce<T, P, F>(&mut self, produce: &mut P, mut handler: F) -> bool
    where
        T: 'static + Copy,
        P: SpineProducers + AsRef<SpineProducerWithDCache<T>>,
        F: for<'a> FnMut(ServerEvent<&'a [u8]>) -> Option<T>,
    {
        let mut worked = self.manager.expire_pending_handshakes();
        worked |= self.manager.drain_pending_disconnects(&mut |event| {
            let _ = handler(event);
        });
        if let Err(err) = self.manager.poll.poll(&mut self.events, Some(std::time::Duration::ZERO))
        {
            safe_panic!("tcp server poll failed: {err}");
            return false;
        }
        for event in &self.events {
            worked = true;
            self.manager.handle_event_produce(event, produce, &mut handler);
        }
        worked |= self.manager.drain_pending_disconnects(&mut |event| {
            let _ = handler(event);
        });
        worked
    }

    pub fn write_or_enqueue_with<F>(&mut self, where_to: SendBehavior, serialise: F)
    where
        F: Fn(&mut Vec<u8>),
    {
        self.manager.write_or_enqueue_with(where_to, serialise);
    }

    /// Permanently closes an accepted connection.
    pub fn disconnect(&mut self, token: Token) {
        self.manager.disconnect(token);
    }

    /// Permanently closes every accepted connection, leaving listeners active.
    pub fn disconnect_all(&mut self) {
        self.manager.disconnect_all();
    }

    /// Stops and removes a listener without affecting accepted connections.
    pub fn remove_listener(&mut self, token: Token) {
        self.manager.remove_listener(token);
    }
}
