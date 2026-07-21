use std::net::{Shutdown, SocketAddr};

use flux::spine::{SpineProducerWithDCache, SpineProducers};
use flux_timing::{Duration, Instant, Nanos};
use flux_utils::{DCachePtr, safe_panic};
use mio::{Events, Interest, Poll, Token, event::Event, net::TcpListener};
use tracing::{error, info, warn};

use crate::tcp::{
    ConnState, SendBehavior, TcpStream, TcpTelemetry,
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

struct ServerManager {
    poll: Poll,
    listeners: Vec<(Token, TcpListener)>,
    streams: Vec<(Token, TcpStream)>,
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
            streams: Vec::with_capacity(8),
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

            let mut stream = TcpStream::from_stream_with_telemetry(
                socket,
                token,
                peer_addr,
                self.telemetry,
                self.dcache.is_some(),
            );
            if let Some(message) = &self.on_connect_msg &&
                stream.write_or_enqueue_with(self.poll.registry(), |buf| {
                    buf.extend_from_slice(message);
                }) == ConnState::Disconnected
            {
                stream.close(self.poll.registry());
                continue;
            }

            self.next_token += 1;
            handler(ServerEvent::Accept { listener: listener_token, stream: token, peer_addr });
            self.streams.push((token, stream));
        }
    }

    fn handle_event<F>(&mut self, event: &Event, handler: &mut F)
    where
        F: for<'a> FnMut(ServerEvent<&'a [u8]>),
    {
        let token = event.token();
        if let Some(index) = self.listeners.iter().position(|(candidate, _)| *candidate == token) {
            self.accept_connections(index, handler);
            return;
        }
        let Some(index) = self.streams.iter().position(|(candidate, _)| *candidate == token) else {
            safe_panic!("tcp server got event for unknown token");
            return;
        };
        if self.streams[index].1.poll_with(
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
        if let Some(index) = self.listeners.iter().position(|(candidate, _)| *candidate == token) {
            self.accept_connections(index, &mut |event| {
                let _ = handler(event);
            });
            return;
        }
        let Some(index) = self.streams.iter().position(|(candidate, _)| *candidate == token) else {
            safe_panic!("tcp server got event for unknown token");
            return;
        };
        let dcache = self.dcache.as_deref().expect("dcache required for poll_with_produce");
        if self.streams[index].1.poll_with_produce(
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
        let (_, mut stream) = self.streams.swap_remove(index);
        stream.close(self.poll.registry());
    }

    fn disconnect(&mut self, token: Token) {
        if let Some(index) = self.streams.iter().position(|(candidate, _)| *candidate == token) {
            self.disconnect_index(index);
        }
    }

    fn disconnect_all(&mut self) {
        while !self.streams.is_empty() {
            self.disconnect_index(self.streams.len() - 1);
        }
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

        let mut index = self.streams.len();
        while index != 0 {
            index -= 1;
            let state = self.streams[index].1.write_or_enqueue_shared(
                self.poll.registry(),
                &self.bcast_header,
                &self.bcast_payload,
            );
            let backlog_exceeded =
                Self::backlog_exceeded(self.max_backlog, &mut self.streams[index].1);
            if state == ConnState::Disconnected || backlog_exceeded {
                let token = self.streams[index].0;
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
        let Some(index) = self.streams.iter().position(|(candidate, _)| *candidate == token) else {
            error!(?token, "tcp server send to unknown stream");
            return;
        };
        if self.streams[index].1.write_or_enqueue_with(self.poll.registry(), serialise) ==
            ConnState::Disconnected ||
            Self::backlog_exceeded(self.max_backlog, &mut self.streams[index].1)
        {
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
        let mut worked = self.manager.drain_pending_disconnects(&mut handler);
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
        let mut worked = self.manager.drain_pending_disconnects(&mut |event| {
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
