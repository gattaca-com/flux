use std::net::SocketAddr;

use flux_timing::{Duration, Nanos, Repeater};
use flux_utils::safe_panic;
use mio::{Events, Interest, Poll, Token, event::Event, net::TcpListener};
use tracing::{debug, error, warn};

use crate::tcp::{ConnState, TcpStream, TcpTelemetry, stream::set_socket_buf_size};

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum SendBehavior {
    Broadcast,
    Single(Token),
}

// Outbound will try to reconnect, inbound not
#[repr(u8)]
pub enum ConnectionVariant {
    /// Connections that we initiated, will be reconnected
    Outbound(TcpStream),
    /// Connections that were initated from outside through one of
    /// the listeners
    Inbound(TcpStream),
    /// Listeners for new connections. When a new connection
    /// is made to one of the listeners, it will
    /// be turned into an Inbound
    Listener(TcpListener),
}

/// Event emitted by [`TcpConnector::poll_with`] for each notable IO occurrence.
pub enum PollEvent<'a> {
    /// A new connection was accepted from a listener.
    ///
    /// - `listener`: token of the listening socket that accepted
    /// - `stream`: token assigned to the new inbound stream
    /// - `peer_addr`: remote address
    ///
    /// Use the `stream` token with [`SendBehavior::Single`] to write back.
    Accept { listener: Token, stream: Token, peer_addr: SocketAddr },
    /// A connection was closed (by the remote or due to an IO error).
    Disconnect { token: Token },
    /// A complete framed message was received.
    Message { token: Token, payload: &'a [u8], send_ts: Nanos },
}

struct ConnectionManager {
    poll: Poll,
    conns: Vec<(Token, ConnectionVariant)>,
    reconnector: Repeater,
    on_connect_msg: Option<Vec<u8>>,
    telemetry: TcpTelemetry,
    socket_buf_size: Option<usize>,

    // Always only outbound/client side connection streams
    to_be_reconnected: Vec<(Token, SocketAddr)>,
    // Outbound connections that completed during maybe_reconnect, drained in poll_with.
    newly_connected: Vec<(Token, SocketAddr)>,
    next_token: usize,
}
impl Default for ConnectionManager {
    fn default() -> Self {
        Self {
            conns: Vec::with_capacity(5),
            reconnector: Repeater::every(Duration::from_secs(2)),
            on_connect_msg: None,
            telemetry: TcpTelemetry::Disabled,
            socket_buf_size: None,
            to_be_reconnected: Vec::with_capacity(10),
            newly_connected: Vec::with_capacity(10),
            poll: Poll::new().expect("couldn't set up a poll for tcp connector"),
            next_token: 0,
        }
    }
}
impl ConnectionManager {
    #[inline]
    fn disconnect_all_outbound(&mut self) {
        let mut i = self.conns.len();
        while i != 0 {
            i -= 1;
            if matches!(self.conns[i].1, ConnectionVariant::Outbound(_)) {
                self.disconnect_at_index(i);
            }
        }
    }

    fn disconnect_at_index(&mut self, index: usize) {
        let (token, stream) = self.conns.swap_remove(index);
        match stream {
            ConnectionVariant::Outbound(mut tcp_connection) => {
                let addr = tcp_connection.close(self.poll.registry());
                self.to_be_reconnected.push((token, addr));
            }
            ConnectionVariant::Inbound(mut tcp_connection) => {
                let _ = tcp_connection.close(self.poll.registry());
            }
            ConnectionVariant::Listener(mut tcp_listener) => {
                let _ = self.poll.registry().deregister(&mut tcp_listener);
            }
        }
    }

    fn disconnect_token(&mut self, token: Token) {
        if let Some(i) = self.conns.iter().position(|(t, _)| *t == token) {
            self.disconnect_at_index(i);
        }
    }

    #[inline]
    fn broadcast<F>(&mut self, serialise: &F)
    where
        F: Fn(&mut Vec<u8>),
    {
        let mut i = self.conns.len();
        while i != 0 {
            i -= 1;
            match &mut self.conns[i].1 {
                ConnectionVariant::Outbound(tcp_connection) |
                ConnectionVariant::Inbound(tcp_connection) => {
                    if tcp_connection.write_or_enqueue_with(self.poll.registry(), serialise) ==
                        ConnState::Disconnected
                    {
                        self.disconnect_at_index(i);
                    }
                }
                ConnectionVariant::Listener(_tcp_listener) => {}
            }
        }
    }

    #[inline]
    fn write_or_enqueue_with<F>(&mut self, serialise: F, where_to: SendBehavior)
    where
        F: Fn(&mut Vec<u8>),
    {
        match where_to {
            SendBehavior::Broadcast => self.broadcast(&serialise),
            SendBehavior::Single(token) => {
                if let Some(i) = self.conns.iter().position(|(t, _)| *t == token) {
                    match &mut self.conns[i].1 {
                        ConnectionVariant::Outbound(tcp_connection) |
                        ConnectionVariant::Inbound(tcp_connection) => {
                            if tcp_connection.write_or_enqueue_with(self.poll.registry(), serialise) ==
                                ConnState::Disconnected
                            {
                                tracing::warn!("issue when writing to {token:?} disconnecting");
                                self.disconnect_at_index(i);
                            }
                        }
                        ConnectionVariant::Listener(_tcp_listener) => error!(
                            "cannot write to listener bound to token {token:?}, what are you doing"
                        ),
                    }
                } else {
                    error!("tcp sending: unknown token {token:?}");
                }
            }
        }
    }

    fn connect(&mut self, addr: SocketAddr) -> Option<Token> {
        let o = Token(self.next_token);
        self.to_be_reconnected.push((o, addr));
        self.reconnector.force_fire();
        self.maybe_reconnect();
        if self.conns.iter().any(|(t, _)| t == &o) {
            self.next_token += 1;
            Some(o)
        } else {
            None
        }
    }

    // This will start listening on a given port, returning the token tied to that
    // port. When a connection comes in through that port, this token will be
    // communicated to the handling function so the handler can know what
    // endpoint it is receiving a connection for.
    fn listen_at(&mut self, addr: SocketAddr) -> Option<Token> {
        let mut listener = mio::net::TcpListener::bind(addr)
            .inspect_err(|e| warn!("couldn't start listening at {addr:?}: {e}"))
            .ok()?;
        let token = Token(self.next_token);
        self.poll
            .registry()
            .register(&mut listener, token, Interest::READABLE)
            .inspect_err(|err| warn!("Couldn't register listening addr {addr:?}: {err}"))
            .ok()?;
        self.conns.push((token, ConnectionVariant::Listener(listener)));
        self.next_token += 1;
        Some(token)
    }

    fn maybe_reconnect(&mut self) {
        if !self.reconnector.fired() {
            return;
        }

        let mut i = self.to_be_reconnected.len();

        while i != 0 {
            i -= 1;
            let (token, addr) = self.to_be_reconnected[i];
            let Ok(mut stream) = mio::net::TcpStream::connect(addr)
                .inspect_err(|e| warn!("couldn't connect to {addr}: {e}"))
            else {
                continue;
            };
            if let Some(size) = self.socket_buf_size {
                set_socket_buf_size(&stream, size);
            }
            let Ok(err) =
                stream.take_error().inspect_err(|e| error!("couldn't take error on stream: {e}"))
            else {
                continue;
            };
            if let Some(err) = err {
                warn!("got error while connecting to {addr}: {err}");
                continue;
            }
            if let Err(e) = self.poll.registry().register(&mut stream, token, Interest::READABLE) {
                error!("couldn't register tcp stream for {addr} with registry: {e}");
                continue;
            };
            let Ok(mut stream) =
                TcpStream::from_stream_with_telemetry(stream, addr, self.telemetry).inspect_err(
                    |e| error!("couldn't construct tcpconnection for {addr} with registry: {e}"),
                )
            else {
                continue;
            };
            if let Some(msg) = &self.on_connect_msg &&
                stream.write_or_enqueue_with(self.poll.registry(), |buf: &mut Vec<u8>| {
                    buf.extend_from_slice(msg);
                }) == ConnState::Disconnected
            {
                warn!(addr = ?addr, "on_connect_msg send failed");
                return;
            }

            self.newly_connected.push(self.to_be_reconnected.swap_remove(i));
            self.conns.push((token, ConnectionVariant::Outbound(stream)));
            debug!(?addr, "connected");
        }
    }

    #[inline]
    fn currently_disconnected(&self) -> impl Iterator<Item = Token> {
        self.to_be_reconnected.iter().map(|(t, _)| *t)
    }

    #[inline]
    fn force_reconnect(&mut self) {
        self.reconnector.reset();
        self.maybe_reconnect();
    }

    #[inline]
    fn handle_event<F>(&mut self, e: &Event, handler: &mut F)
    where
        F: for<'a> FnMut(PollEvent<'a>),
    {
        let event_token = e.token();
        let Some(stream_id) = self.conns.iter().position(|(t, _)| t == &event_token) else {
            safe_panic!("got event for unknown token");
            return;
        };

        loop {
            match &mut self.conns[stream_id].1 {
                ConnectionVariant::Outbound(tcp_connection) |
                ConnectionVariant::Inbound(tcp_connection) => {
                    if tcp_connection.poll_with(
                        self.poll.registry(),
                        e,
                        &mut |token, payload, send_ts| {
                            handler(PollEvent::Message { token, payload, send_ts });
                        },
                    ) == ConnState::Disconnected
                    {
                        handler(PollEvent::Disconnect { token: event_token });
                        self.disconnect_at_index(stream_id);
                    }
                    return;
                }
                ConnectionVariant::Listener(tcp_listener) => {
                    if let Ok((mut stream, addr)) = tcp_listener.accept() {
                        tracing::info!(?addr, "client connected");
                        if let Some(size) = self.socket_buf_size {
                            set_socket_buf_size(&stream, size);
                        }
                        let token = Token(self.next_token);
                        if let Err(e) =
                            self.poll.registry().register(&mut stream, token, Interest::READABLE)
                        {
                            error!("couldn't register client {e}");
                            let _ = stream.shutdown(std::net::Shutdown::Both);
                            continue;
                        };
                        let Ok(mut conn) =
                            TcpStream::from_stream_with_telemetry(stream, addr, self.telemetry)
                        else {
                            continue;
                        };

                        if let Some(msg) = &self.on_connect_msg &&
                            conn.write_or_enqueue_with(
                                self.poll.registry(),
                                |buf: &mut Vec<u8>| {
                                    buf.extend_from_slice(msg);
                                },
                            ) == ConnState::Disconnected
                        {
                            continue;
                        }
                        handler(PollEvent::Accept {
                            listener: event_token,
                            stream: token,
                            peer_addr: addr,
                        });
                        self.conns.push((token, ConnectionVariant::Inbound(conn)));
                        self.next_token += 1;
                    } else {
                        return;
                    }
                }
            }
        }
    }
}

/// Non-blocking TCP connector/acceptor built on `mio`.
///
/// Manages:
/// - **Outbound (client) connections** created via [`connect`]. These are
///   **auto-retried** on failure/disconnect based on the configured reconnect
///   interval.
/// - **Listeners** created via [`listen_at`] and **inbound (server)
///   connections** accepted from them. Inbound connections are **not**
///   reconnected.
///
/// Drive all IO by calling [`poll_with`] regularly (typically in your event
/// loop). Use [`write_or_enqueue_with`] to send to one connection or broadcast
/// to all.
///
/// ## Tokens
/// Every listener and stream is identified by a `mio::Token`.
/// - [`listen_at`] returns the listener token.
/// - Each accepted inbound stream receives a new token (reported via
///   [`ConnectionEvent`]).
/// - [`connect`] returns the token for the outbound stream if the connection is
///   established.
///
/// ## on-connect message
/// If configured via [`with_on_connect_msg`], the provided bytes are sent once
/// after a connection is established (both outbound and newly accepted
/// inbound).
pub struct TcpConnector {
    events: Events,
    conn_mgr: ConnectionManager,
}
impl Default for TcpConnector {
    /// Creates a new connector with the given telemetry.
    ///
    /// The default outbound reconnect interval is 2 seconds.
    fn default() -> Self {
        Self { events: Events::with_capacity(128), conn_mgr: ConnectionManager::default() }
    }
}
impl TcpConnector {
    /// Sets the interval used to retry disconnected/failed outbound
    /// connections.
    ///
    /// Reconnect attempts are performed from within [`poll_with`].
    pub fn with_reconnect_interval(mut self, interval: Duration) -> Self {
        self.conn_mgr.reconnector = Repeater::every(interval);
        self
    }

    /// Sends this message once immediately after a connection becomes usable.
    ///
    /// Applied to:
    /// - outbound connections after a successful (re)connect
    /// - inbound connections right after accept
    ///
    /// # Panics
    /// Panics if `msg.len() > TcpConnection::SEND_BUF_SIZE`.
    pub fn with_on_connect_msg(mut self, msg: Vec<u8>) -> Self {
        assert!(msg.len() <= TcpStream::SEND_BUF_SIZE, "on_connect_msg exceeds send buffer size");
        self.conn_mgr.on_connect_msg = Some(msg);
        self
    }

    /// Sets telemetry config for all streams created by this connector.
    pub fn with_telemetry(mut self, telemetry: TcpTelemetry) -> Self {
        self.conn_mgr.telemetry = telemetry;
        self
    }

    /// Sets kernel SO_SNDBUF and SO_RCVBUF on all sockets (outbound and
    /// accepted).
    pub fn with_socket_buf_size(mut self, size: usize) -> Self {
        self.conn_mgr.socket_buf_size = Some(size);
        self
    }

    /// Polls sockets once (non-blocking) and dispatches events via
    /// [`PollEvent`].
    ///
    /// This call:
    /// 1) attempts outbound reconnects if the interval fired
    /// 2) polls `mio` with a zero timeout
    /// 3) for each event calls `handler` with the appropriate [`PollEvent`]
    /// 4) returns whether any IO events were processed
    #[inline]
    pub fn poll_with<F>(&mut self, mut handler: F) -> bool
    where
        F: for<'a> FnMut(PollEvent<'a>),
    {
        self.conn_mgr.maybe_reconnect();
        for (token, peer_addr) in self.conn_mgr.newly_connected.drain(..) {
            handler(PollEvent::Accept { listener: token, stream: token, peer_addr });
        }
        if let Err(e) = self.conn_mgr.poll.poll(&mut self.events, Some(std::time::Duration::ZERO)) {
            safe_panic!("got error polling {e}");
            return false;
        }

        let mut o = false;
        for e in self.events.iter() {
            o = true;
            self.conn_mgr.handle_event(e, &mut handler);
        }
        o
    }

    /// Writes immediately or enqueues bytes for later sending.
    ///
    /// `serialise` is called with a mutable send buffer and must return the
    /// number of bytes written. Use [`SendBehavior::BroadCast`] to send to
    /// all active connections or [`SendBehavior::Single`] to target one
    /// token.
    #[inline]
    pub fn write_or_enqueue_with<F>(&mut self, where_to: SendBehavior, serialise: F)
    where
        F: Fn(&mut Vec<u8>),
    {
        self.conn_mgr.write_or_enqueue_with(serialise, where_to);
    }

    /// Disconnects all outbound connections and schedules them for
    /// reconnection.
    ///
    /// Inbound connections and listeners are left untouched.
    pub fn disconnect_outbound(&mut self) {
        self.conn_mgr.disconnect_all_outbound();
    }

    /// Disconnects a specific connection by token.
    ///
    /// If the token is an outbound connection, it will be scheduled for
    /// reconnection. If inbound, it's simply closed. No-op if token not found.
    pub fn disconnect(&mut self, token: Token) {
        self.conn_mgr.disconnect_token(token);
    }

    /// Initiates (or schedules) an outbound connection to `addr`.
    ///
    /// Returns the token for this connection if the connection becomes
    /// established; otherwise returns `None` (the connector may still retry
    /// later).
    ///
    /// Note: reconnect attempts are driven by [`poll_with`].
    #[inline]
    pub fn connect(&mut self, addr: SocketAddr) -> Option<Token> {
        self.conn_mgr.connect(addr)
    }

    /// Starts listening on `addr` and registers the listener for readable
    /// events.
    ///
    /// Returns the token associated with the listener socket. When a client
    /// connects, `poll_with` will accept it, allocate a new token for the
    /// inbound stream, and emit a [`ConnectionEvent`] through `on_accept`.
    pub fn listen_at(&mut self, addr: SocketAddr) -> Option<Token> {
        self.conn_mgr.listen_at(addr)
    }

    /// Returns an iterator over tokens that are currently pending reconnection
    /// (outbound only).
    #[inline]
    pub fn currently_disconnected(&self) -> impl Iterator<Item = Token> {
        self.conn_mgr.currently_disconnected()
    }

    /// Forces the reconnect timer to fire and immediately attempts
    /// reconnections.
    #[inline]
    pub fn force_reconnect(&mut self) {
        self.conn_mgr.force_reconnect();
    }
}
