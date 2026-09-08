//! TCP side of [`crate::Connector`]: mio streams and listeners with framed
//! messages, reconnect of outbound streams, and per-stream send backlogs.

use std::net::SocketAddr;

use flux::spine::{SpineProducerWithDCache, SpineProducers};
use flux_timing::{Duration, Instant, Nanos, Repeater};
use flux_utils::safe_panic;
use mio::{Events, Interest, Poll, Registry, Token, event::Event, net::TcpListener};
use tracing::{debug, error, warn};

use crate::{
    connector::{Config, PollEvent, SendBehavior},
    tcp::{
        ConnState, FRAME_HEADER_SIZE, TcpStream, set_keepalive, set_socket_buf_size,
        set_user_timeout, write_frame_header,
    },
};

// Outbound will try to reconnect, inbound not
enum Variant {
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

pub(crate) struct TcpManager {
    registry: Registry,
    conns: Vec<(Token, Variant)>,
    reconnector: Repeater,
    // Always only outbound/client side connection streams
    to_be_reconnected: Vec<(Token, Variant)>,
    // Outbound connections that completed during maybe_reconnect, drained in poll_with.
    reconnected_to: Vec<Token>,
    // Connections dropped outside event handling, drained in poll_with before reconnects.
    pending_disconnects: Vec<Token>,
    next_token: usize,
    /// Header of the frame currently being written; the payload is the
    /// caller's and identical for every recipient of a broadcast.
    header: [u8; FRAME_HEADER_SIZE],
}

impl TcpManager {
    pub(crate) fn new(registry: Registry) -> Self {
        Self {
            registry,
            conns: Vec::with_capacity(5),
            reconnector: Repeater::every(Duration::from_secs(2)),
            to_be_reconnected: Vec::with_capacity(10),
            reconnected_to: Vec::with_capacity(10),
            pending_disconnects: Vec::with_capacity(10),
            next_token: 0,
            header: [0; FRAME_HEADER_SIZE],
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.conns.is_empty() && self.to_be_reconnected.is_empty()
    }

    pub(crate) fn set_reconnect_interval(&mut self, interval: Duration) {
        self.reconnector = Repeater::every(interval);
    }

    pub(crate) fn disconnect_outbound(&mut self, cfg: &Config) {
        let mut i = self.conns.len();
        while i != 0 {
            i -= 1;
            if matches!(self.conns[i].1, Variant::Outbound(_)) {
                self.disconnect_at_index(cfg, i);
            }
        }
    }

    fn disconnect_at_index(&mut self, cfg: &Config, index: usize) {
        let (token, stream) = self.conns.swap_remove(index);
        match stream {
            Variant::Outbound(mut tcp_connection) => {
                tcp_connection.close(&self.registry);
                if cfg.drop_outbound_backlog_on_disconnect {
                    tcp_connection.clear_send_backlog();
                }
                self.to_be_reconnected.push((token, Variant::Outbound(tcp_connection)));
            }
            Variant::Inbound(mut tcp_connection) => {
                tcp_connection.close(&self.registry);
            }
            Variant::Listener(mut tcp_listener) => {
                let _ = self.registry.deregister(&mut tcp_listener);
            }
        }
    }

    fn disconnect_at_index_pending(&mut self, cfg: &Config, index: usize) {
        let token = self.conns[index].0;
        self.disconnect_at_index(cfg, index);
        self.pending_disconnects.push(token);
    }

    pub(crate) fn disconnect(&mut self, cfg: &Config, token: Token) {
        if let Some(i) = self.conns.iter().position(|(t, _)| *t == token) {
            self.disconnect_at_index(cfg, i);
        }
    }

    #[inline]
    fn broadcast(&mut self, cfg: &Config, payload: &[u8]) {
        let max_backlog = cfg.max_backlog;
        if !cfg.drop_outbound_backlog_on_disconnect {
            for (_, c) in &mut self.to_be_reconnected {
                let Variant::Outbound(tcp) = c else {
                    unreachable!("only outbound should be auto reconnected");
                };
                Self::push_reconnect_backlog_shared(max_backlog, tcp, &self.header, payload);
            }
        }

        let mut i = self.conns.len();
        while i != 0 {
            i -= 1;
            match &mut self.conns[i].1 {
                Variant::Outbound(tcp_connection) | Variant::Inbound(tcp_connection) => {
                    let state = tcp_connection.write_or_enqueue_shared(
                        &self.registry,
                        &self.header,
                        payload,
                    );
                    if state == ConnState::Disconnected {
                        let token = self.conns[i].0;
                        self.disconnect_at_index_pending(cfg, i);
                        if !cfg.drop_outbound_backlog_on_disconnect &&
                            let Some((_, Variant::Outbound(tcp))) =
                                self.to_be_reconnected.iter_mut().find(|(t, _)| *t == token)
                        {
                            Self::push_reconnect_backlog_shared(
                                max_backlog,
                                tcp,
                                &self.header,
                                payload,
                            );
                        }
                    } else if Self::active_backlog_exceeded(max_backlog, tcp_connection) {
                        self.disconnect_at_index_pending(cfg, i);
                    }
                }
                Variant::Listener(_tcp_listener) => {}
            }
        }
    }

    #[inline]
    fn backlog_exceeded(
        max_backlog: Option<(usize, Duration)>,
        tcp: &mut TcpStream,
        additional_messages: usize,
    ) -> bool {
        let Some((max, timeout)) = max_backlog else { return false };
        if tcp.send_backlog.len().saturating_add(additional_messages) <= max {
            tcp.backlog_exceeded_since = None;
            return false;
        }

        let now = Instant::now();
        let since = tcp.backlog_exceeded_since.get_or_insert(now);
        now.saturating_sub(*since) >= timeout
    }

    #[inline]
    fn reconnect_backlog_accepts(
        max_backlog: Option<(usize, Duration)>,
        tcp: &mut TcpStream,
    ) -> bool {
        !Self::backlog_exceeded(max_backlog, tcp, 1)
    }

    #[inline]
    fn active_backlog_exceeded(
        max_backlog: Option<(usize, Duration)>,
        tcp: &mut TcpStream,
    ) -> bool {
        Self::backlog_exceeded(max_backlog, tcp, 0)
    }

    #[inline]
    fn push_reconnect_backlog_shared(
        max_backlog: Option<(usize, Duration)>,
        tcp: &mut TcpStream,
        header: &[u8; FRAME_HEADER_SIZE],
        payload: &[u8],
    ) {
        if Self::reconnect_backlog_accepts(max_backlog, tcp) {
            tcp.backlog_push_shared(header, payload);
        }
    }

    /// Frames `payload` and writes it to one connection or all of them.
    #[inline]
    pub(crate) fn write(&mut self, cfg: &Config, where_to: SendBehavior, payload: &[u8]) {
        write_frame_header(&mut self.header, payload.len(), Nanos::now());
        match where_to {
            SendBehavior::Broadcast => self.broadcast(cfg, payload),
            SendBehavior::Single(token) => {
                if let Some(i) = self.conns.iter().position(|(t, _)| *t == token) {
                    match &mut self.conns[i].1 {
                        Variant::Outbound(tcp_connection) | Variant::Inbound(tcp_connection) => {
                            let state = tcp_connection.write_or_enqueue_shared(
                                &self.registry,
                                &self.header,
                                payload,
                            );
                            if state == ConnState::Disconnected {
                                tracing::warn!("issue when writing to {token:?} disconnecting");
                                self.disconnect_at_index_pending(cfg, i);
                                if !cfg.drop_outbound_backlog_on_disconnect &&
                                    let Some((_, Variant::Outbound(tcp))) = self
                                        .to_be_reconnected
                                        .iter_mut()
                                        .find(|(t, _)| *t == token)
                                {
                                    Self::push_reconnect_backlog_shared(
                                        cfg.max_backlog,
                                        tcp,
                                        &self.header,
                                        payload,
                                    );
                                }
                            } else if Self::active_backlog_exceeded(cfg.max_backlog, tcp_connection)
                            {
                                self.disconnect_at_index_pending(cfg, i);
                            }
                        }
                        Variant::Listener(_tcp_listener) => error!(
                            "cannot write to listener bound to token {token:?}, what are you doing"
                        ),
                    }
                } else if let Some((_, Variant::Outbound(tcp))) =
                    self.to_be_reconnected.iter_mut().find(|(t, _)| *t == token)
                {
                    if !cfg.drop_outbound_backlog_on_disconnect {
                        Self::push_reconnect_backlog_shared(
                            cfg.max_backlog,
                            tcp,
                            &self.header,
                            payload,
                        );
                    }
                } else {
                    error!("tcp sending: unknown token {token:?}");
                }
            }
        }
    }

    pub(crate) fn connect(&mut self, cfg: &Config, addr: SocketAddr) -> Option<Token> {
        let o = Token(self.next_token);
        if let Some(stream) = self.try_connect(cfg, o, addr) {
            let mut tcp_stream = TcpStream::from_stream_with_telemetry(
                stream,
                o,
                addr,
                cfg.telemetry,
                cfg.dcache.is_some(),
            );
            if let Some(msg) = &cfg.on_connect_msg &&
                tcp_stream.write_or_enqueue_with(&self.registry, |buf: &mut Vec<u8>| {
                    buf.extend_from_slice(msg);
                }) == ConnState::Disconnected
            {
                warn!(?addr, "on_connect_msg send failed");
                return None;
            }
            self.conns.push((o, Variant::Outbound(tcp_stream)));
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
    pub(crate) fn listen_at(&mut self, addr: SocketAddr) -> Option<Token> {
        let mut listener = mio::net::TcpListener::bind(addr)
            .inspect_err(|e| warn!("couldn't start listening at {addr:?}: {e}"))
            .ok()?;
        let token = Token(self.next_token);
        self.registry
            .register(&mut listener, token, Interest::READABLE)
            .inspect_err(|err| warn!("Couldn't register listening addr {addr:?}: {err}"))
            .ok()?;
        self.conns.push((token, Variant::Listener(listener)));
        self.next_token += 1;
        Some(token)
    }

    fn maybe_reconnect(&mut self, cfg: &Config) {
        if !self.reconnector.fired() {
            return;
        }

        let mut i = self.to_be_reconnected.len();
        while i != 0 {
            i -= 1;
            let (token, mut stream) = self.to_be_reconnected.swap_remove(i);
            if self.try_reconnect(cfg, token, &mut stream) {
                self.conns.push((token, stream));
                self.reconnected_to.push(token);
            } else {
                self.to_be_reconnected.push((token, stream));
            }
        }
    }

    fn try_connect(
        &self,
        cfg: &Config,
        token: Token,
        addr: SocketAddr,
    ) -> Option<mio::net::TcpStream> {
        let Ok(mut new_stream) = mio::net::TcpStream::connect(addr)
            .inspect_err(|e| warn!("couldn't connect to {addr}: {e}"))
        else {
            return None;
        };

        if let Some(size) = cfg.socket_buf_size {
            set_socket_buf_size(&new_stream, size);
        }
        let Ok(err) =
            new_stream.take_error().inspect_err(|e| error!("couldn't take error on stream: {e}"))
        else {
            return None;
        };
        if let Some(err) = err {
            warn!("got error while connecting to {addr}: {err}");
            return None;
        }

        if let Err(e) = self.registry.register(&mut new_stream, token, Interest::READABLE) {
            error!("couldn't register tcp stream for {addr} with registry: {e}");
            return None;
        }
        if cfg.nodelay {
            new_stream
                .set_nodelay(true)
                .inspect_err(|e| {
                    error!("couldn't setup nodelay for tcp stream for {addr}: {e}");
                })
                .ok()?;
        }
        if cfg.keepalive {
            set_keepalive(&new_stream)
                .inspect_err(|e| error!("couldn't setup keepalive for tcp stream for {addr}: {e}"))
                .ok()?;
        }
        set_user_timeout(&new_stream, cfg.user_timeout_ms);
        Some(new_stream)
    }

    fn try_reconnect(&self, cfg: &Config, token: Token, stream: &mut Variant) -> bool {
        let Variant::Outbound(stream) = stream else {
            panic!("Can only try to connect a Outbound connection");
        };
        let addr = stream.peer();

        let Some(new_stream) = self.try_connect(cfg, token, addr) else {
            return false;
        };

        if stream.reset_with_new_stream(&self.registry, new_stream, cfg.on_connect_msg.as_ref()) ==
            ConnState::Disconnected
        {
            warn!(addr = ?addr, "on_connect_msg send failed");
            return false;
        }

        debug!(?addr, "connected");

        true
    }

    #[inline]
    pub(crate) fn currently_disconnected(&self) -> impl Iterator<Item = Token> {
        self.to_be_reconnected.iter().map(|(t, _)| *t)
    }

    #[inline]
    pub(crate) fn force_reconnect(&mut self, cfg: &Config) {
        self.reconnector.reset();
        self.maybe_reconnect(cfg);
    }

    #[inline]
    fn drain_pending_disconnects<F>(&mut self, handler: &mut F) -> bool
    where
        F: for<'a> FnMut(PollEvent<&'a [u8]>),
    {
        let had_pending = !self.pending_disconnects.is_empty();
        for token in self.pending_disconnects.drain(..) {
            handler(PollEvent::Disconnect { token });
        }
        had_pending
    }

    /// Accepts every pending connection on the listener at `index`, emitting
    /// [`PollEvent::Accept`] for each.
    fn accept_all<F>(
        &mut self,
        cfg: &Config,
        index: usize,
        listener_token: Token,
        on_accept: &mut F,
    ) where
        F: FnMut(PollEvent<&[u8]>),
    {
        loop {
            let Variant::Listener(tcp_listener) = &mut self.conns[index].1 else { unreachable!() };
            let Ok((mut stream, addr)) = tcp_listener.accept() else { return };
            tracing::info!(?addr, "client connected");
            if let Some(size) = cfg.socket_buf_size {
                set_socket_buf_size(&stream, size);
            }
            let token = Token(self.next_token);
            if let Err(e) = self.registry.register(&mut stream, token, Interest::READABLE) {
                error!("couldn't register client {e}");
                let _ = stream.shutdown(std::net::Shutdown::Both);
                continue;
            }
            if cfg.nodelay {
                if let Err(e) = stream.set_nodelay(true) {
                    error!("couldn't set nodelay on stream to {addr}: {e}");
                    continue;
                }
            }
            if cfg.keepalive &&
                let Err(e) = set_keepalive(&stream)
            {
                error!("couldn't set keepalive on stream to {addr}: {e}");
                continue;
            }
            set_user_timeout(&stream, cfg.user_timeout_ms);
            let mut conn = TcpStream::from_stream_with_telemetry(
                stream,
                token,
                addr,
                cfg.telemetry,
                cfg.dcache.is_some(),
            );
            if let Some(msg) = &cfg.on_connect_msg &&
                conn.write_or_enqueue_with(&self.registry, |buf: &mut Vec<u8>| {
                    buf.extend_from_slice(msg);
                }) == ConnState::Disconnected
            {
                continue;
            }
            on_accept(PollEvent::Accept {
                listener: listener_token,
                stream: token,
                peer_addr: addr,
            });
            self.conns.push((token, Variant::Inbound(conn)));
            self.next_token += 1;
        }
    }

    #[inline]
    fn handle_event<F>(&mut self, cfg: &Config, e: &Event, handler: &mut F)
    where
        F: for<'a> FnMut(PollEvent<&'a [u8]>),
    {
        let event_token = e.token();
        let Some(stream_id) = self.conns.iter().position(|(t, _)| t == &event_token) else {
            safe_panic!("got event for unknown token");
            return;
        };

        match &mut self.conns[stream_id].1 {
            Variant::Outbound(tcp_connection) | Variant::Inbound(tcp_connection) => {
                if tcp_connection.poll_with(
                    &self.registry,
                    e,
                    cfg.dcache.as_deref(),
                    &mut |token, bytes, send_ts| {
                        handler(PollEvent::Message { token, payload: bytes, send_ts });
                    },
                ) == ConnState::Disconnected
                {
                    handler(PollEvent::Disconnect { token: event_token });
                    self.disconnect_at_index(cfg, stream_id);
                }
            }
            Variant::Listener(_) => self.accept_all(cfg, stream_id, event_token, handler),
        }
    }

    #[inline]
    fn handle_event_produce<T, P, F>(
        &mut self,
        cfg: &Config,
        e: &Event,
        produce: &mut P,
        on_msg: &mut F,
    ) where
        T: 'static + Copy,
        P: SpineProducers + AsRef<SpineProducerWithDCache<T>>,
        F: for<'a> FnMut(PollEvent<&'a [u8]>) -> Option<T>,
    {
        let event_token = e.token();
        let Some(stream_id) = self.conns.iter().position(|(t, _)| t == &event_token) else {
            safe_panic!("got event for unknown token");
            return;
        };

        match &mut self.conns[stream_id].1 {
            Variant::Outbound(tcp_connection) | Variant::Inbound(tcp_connection) => {
                let dcache = cfg.dcache.as_deref().expect("dcache required for poll_with_produce");
                if tcp_connection.poll_with_produce(
                    &self.registry,
                    e,
                    dcache,
                    produce,
                    &mut |token, bytes, send_ts| {
                        on_msg(PollEvent::Message { token, payload: bytes, send_ts })
                    },
                ) == ConnState::Disconnected
                {
                    let _ = on_msg(PollEvent::Disconnect { token: event_token });
                    self.disconnect_at_index(cfg, stream_id);
                }
            }
            Variant::Listener(_) => self.accept_all(cfg, stream_id, event_token, &mut |event| {
                let _ = on_msg(event);
            }),
        }
    }

    /// Delivers pending disconnects and reconnects, then polls once.
    #[inline]
    pub(crate) fn poll_with<F>(
        &mut self,
        cfg: &Config,
        poll: &mut Poll,
        events: &mut Events,
        mut handler: F,
    ) -> bool
    where
        F: for<'a> FnMut(PollEvent<&'a [u8]>),
    {
        let mut o = self.drain_pending_disconnects(&mut handler);
        self.maybe_reconnect(cfg);
        for token in self.reconnected_to.drain(..) {
            handler(PollEvent::Reconnect { token });
            o = true;
        }
        if let Err(e) = poll.poll(events, Some(std::time::Duration::ZERO)) {
            safe_panic!("got error polling {e}");
            return false;
        }
        for e in &*events {
            o = true;
            self.handle_event(cfg, e, &mut handler);
        }
        o |= self.drain_pending_disconnects(&mut handler);
        o
    }

    #[inline]
    pub(crate) fn poll_with_produce<T, P, F>(
        &mut self,
        cfg: &Config,
        poll: &mut Poll,
        events: &mut Events,
        produce: &mut P,
        mut on_msg: F,
    ) -> bool
    where
        T: 'static + Copy,
        P: SpineProducers + AsRef<SpineProducerWithDCache<T>>,
        F: for<'a> FnMut(PollEvent<&'a [u8]>) -> Option<T>,
    {
        let mut o = self.drain_pending_disconnects(&mut |event| {
            let _ = on_msg(event);
        });
        self.maybe_reconnect(cfg);
        for token in self.reconnected_to.drain(..) {
            let _ = on_msg(PollEvent::Reconnect { token });
            o = true;
        }
        if let Err(e) = poll.poll(events, Some(std::time::Duration::ZERO)) {
            safe_panic!("got error polling {e}");
            return false;
        }
        for e in &*events {
            o = true;
            self.handle_event_produce(cfg, e, produce, &mut on_msg);
        }
        o |= self.drain_pending_disconnects(&mut |event| {
            let _ = on_msg(event);
        });
        o
    }
}
