use std::{
    net::SocketAddr,
    os::fd::{AsRawFd, FromRawFd},
    sync::Arc,
    time::Duration,
};

use flux_timing::{Duration as FluxDuration, Nanos, Repeater};
use flux_utils::{DCache, safe_panic};
use io_uring::{IoUring, opcode, types};
use tracing::{debug, error, info, warn};

use crate::Token;
use crate::tcp::{
    ConnState, MessagePayload, TcpStream, TcpTelemetry,
    stream::{
        PHASE_ACCEPT, PHASE_HEADER, PHASE_PAYLOAD, HeaderCqeOutcome, IoState, PayloadCqeOutcome,
        decode_user_data, encode_user_data, set_socket_buf_size,
    },
};

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum SendBehavior {
    Broadcast,
    Single(Token),
}

#[repr(u8)]
pub enum ConnectionVariant {
    /// Connections that we initiated; will be reconnected on failure.
    Outbound(TcpStream),
    /// Connections accepted from a listener.
    Inbound(TcpStream),
    /// Listeners for new connections.
    Listener(std::net::TcpListener),
}

/// Event emitted by [`TcpConnector::poll_with`] for each notable IO occurrence.
pub enum PollEvent<'a> {
    /// A new connection was accepted from a listener.
    Accept { listener: Token, stream: Token, peer_addr: SocketAddr },
    /// Successfully reconnected an outbound stream.
    Reconnect { token: Token },
    /// A connection was closed (by the remote or due to an IO error).
    Disconnect { token: Token },
    /// A complete framed message was received.
    Message { token: Token, payload: MessagePayload<'a>, send_ts: Nanos },
}

/// io_uring SQ ring size. Must be a power of 2.
const RING_ENTRIES: u32 = 256;

struct ConnectionManager {
    ring: IoUring,
    conns: Vec<(Token, ConnectionVariant)>,
    reconnector: Repeater,
    on_connect_msg: Option<Vec<u8>>,
    telemetry: TcpTelemetry,
    socket_buf_size: Option<usize>,
    dcache: Option<Arc<DCache>>,
    /// Fixed buffer index for the DCache region, set after `register_buffers`.
    dc_buf_index: Option<u16>,

    to_be_reconnected: Vec<(Token, ConnectionVariant)>,
    reconnected_to: Vec<Token>,
    next_token: usize,
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self {
            ring: IoUring::new(RING_ENTRIES).expect("io_uring init failed"),
            conns: Vec::with_capacity(5),
            reconnector: Repeater::every(FluxDuration::from_secs(2)),
            on_connect_msg: None,
            telemetry: TcpTelemetry::Disabled,
            socket_buf_size: None,
            dcache: None,
            dc_buf_index: None,
            to_be_reconnected: Vec::with_capacity(10),
            reconnected_to: Vec::with_capacity(10),
            next_token: 0,
        }
    }
}

impl ConnectionManager {
    fn register_dcache(&mut self, dc: &DCache) {
        let iov = dc.as_iovec();
        match unsafe { self.ring.submitter().register_buffers(&[iov]) } {
            Ok(()) => {
                self.dc_buf_index = Some(0);
                debug!("dcache buffer registered with io_uring ({}B)", dc.capacity());
            }
            Err(e) => {
                warn!("io_uring register_buffers failed: {e}; falling back to plain RECV");
            }
        }
    }

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
        let (token, conn) = self.conns.swap_remove(index);
        match conn {
            ConnectionVariant::Outbound(mut s) => {
                s.close();
                self.to_be_reconnected.push((token, ConnectionVariant::Outbound(s)));
            }
            ConnectionVariant::Inbound(mut s) => {
                s.close();
            }
            ConnectionVariant::Listener(l) => drop(l),
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
                ConnectionVariant::Outbound(s) | ConnectionVariant::Inbound(s) => {
                    if s.write_or_enqueue_with(serialise) == ConnState::Disconnected {
                        self.disconnect_at_index(i);
                    }
                }
                ConnectionVariant::Listener(_) => {}
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
                        ConnectionVariant::Outbound(s) | ConnectionVariant::Inbound(s) => {
                            if s.write_or_enqueue_with(serialise) == ConnState::Disconnected {
                                warn!("issue writing to {token:?}, disconnecting");
                                self.disconnect_at_index(i);
                            }
                        }
                        ConnectionVariant::Listener(_) => {
                            error!("cannot write to listener token {token:?}");
                        }
                    }
                } else {
                    error!("tcp send: unknown token {token:?}");
                }
            }
        }
    }

    fn flush_backlogs(&mut self) {
        let mut i = self.conns.len();
        while i != 0 {
            i -= 1;
            let s = match &mut self.conns[i].1 {
                ConnectionVariant::Outbound(s) | ConnectionVariant::Inbound(s) => s,
                ConnectionVariant::Listener(_) => continue,
            };
            if s.has_backlog() && s.drain_backlog() == ConnState::Disconnected {
                self.disconnect_at_index(i);
            }
        }
    }

    fn connect(&mut self, addr: SocketAddr) -> Option<Token> {
        let token = Token(self.next_token);
        let stream = self.try_connect(addr)?;
        let mut tcp = TcpStream::new(stream, token, addr, self.telemetry, self.dcache.clone());
        if let Some(msg) = &self.on_connect_msg {
            if tcp.write_or_enqueue_with(|buf| buf.extend_from_slice(msg)) ==
                ConnState::Disconnected
            {
                warn!(?addr, "on_connect_msg send failed");
                return None;
            }
        }
        self.conns.push((token, ConnectionVariant::Outbound(tcp)));
        self.next_token += 1;
        // Post AFTER push so the SQE pointer targets the element's stable heap location.
        let last = self.conns.len() - 1;
        if let ConnectionVariant::Outbound(s) = &mut self.conns[last].1 {
            s.post_header_sqe(&mut self.ring.submission());
        }
        Some(token)
    }

    fn listen_at(&mut self, addr: SocketAddr) -> Option<Token> {
        let listener = std::net::TcpListener::bind(addr)
            .inspect_err(|e| warn!("couldn't listen at {addr}: {e}"))
            .ok()?;
        listener.set_nonblocking(true).ok()?;
        let token = Token(self.next_token);
        // Multishot ACCEPT: the kernel fires a CQE for every new connection
        // and automatically re-arms unless IORING_CQE_F_MORE is absent.
        let sqe = opcode::AcceptMulti::new(types::Fd(listener.as_raw_fd()))
            .build()
            .user_data(encode_user_data(token, PHASE_ACCEPT));
        // SAFETY: listener fd is owned and lives in conns after the push below.
        if unsafe { self.ring.submission().push(&sqe) }.is_err() {
            warn!("SQ full on listen_at {addr}");
            return None;
        }
        self.ring.submit().ok()?;
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
            let (token, mut variant) = self.to_be_reconnected.swap_remove(i);
            if self.try_reconnect(token, &mut variant) {
                self.conns.push((token, variant));
                self.reconnected_to.push(token);
            } else {
                self.to_be_reconnected.push((token, variant));
            }
        }
    }

    fn try_connect(&self, addr: SocketAddr) -> Option<std::net::TcpStream> {
        let stream = std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200))
            .inspect_err(|e| warn!("couldn't connect to {addr}: {e}"))
            .ok()?;
        stream
            .set_nonblocking(true)
            .inspect_err(|e| error!("set_nonblocking for {addr}: {e}"))
            .ok()?;
        stream
            .set_nodelay(true)
            .inspect_err(|e| error!("set_nodelay for {addr}: {e}"))
            .ok()?;
        if let Some(size) = self.socket_buf_size {
            set_socket_buf_size(&stream, size);
        }
        Some(stream)
    }

    fn try_reconnect(&self, _token: Token, variant: &mut ConnectionVariant) -> bool {
        let ConnectionVariant::Outbound(stream) = variant else {
            panic!("can only reconnect Outbound connections");
        };
        let addr = stream.peer();
        let Some(new_stream) = self.try_connect(addr) else { return false };
        if stream.reset_for_reconnect(new_stream, self.on_connect_msg.as_ref()) ==
            ConnState::Disconnected
        {
            warn!(?addr, "on_connect_msg send failed on reconnect");
            return false;
        }
        debug!(?addr, "reconnected");
        true
    }

    fn currently_disconnected(&self) -> impl Iterator<Item = Token> {
        self.to_be_reconnected.iter().map(|(t, _)| *t)
    }

    fn force_reconnect(&mut self) {
        self.reconnector.reset();
        self.maybe_reconnect();
    }

    /// Submit pending SQEs and drain all available CQEs, then re-arm Idle
    /// streams with a new header RECV SQE.
    fn poll_with<F>(&mut self, handler: &mut F)
    where
        F: for<'a> FnMut(PollEvent<'a>),
    {
        self.flush_backlogs();

        if let Err(e) = self.ring.submit_and_wait(0) {
            safe_panic!("io_uring submit_and_wait: {e}");
            return;
        }

        // Collect CQE data before processing to avoid split borrows when
        // pushing follow-up SQEs.
        let cqes: Vec<(u64, i32, u32)> = {
            let cq = self.ring.completion();
            cq.map(|e| (e.user_data(), e.result(), e.flags())).collect()
        };

        for (ud, result, flags) in cqes {
            let (token, phase) = decode_user_data(ud);
            match phase {
                PHASE_ACCEPT => self.handle_accept_cqe(token, result, flags, handler),
                PHASE_HEADER => self.handle_header_cqe(token, result, handler),
                PHASE_PAYLOAD => self.handle_payload_cqe(token, result, handler),
                _ => safe_panic!("io_uring: unknown phase in user_data {ud:#x}"),
            }
        }

        // Re-arm any stream that became Idle during CQE processing.
        let mut sq = self.ring.submission();
        for (_, variant) in &mut self.conns {
            let s = match variant {
                ConnectionVariant::Outbound(s) | ConnectionVariant::Inbound(s) => s,
                ConnectionVariant::Listener(_) => continue,
            };
            if matches!(s.io_state, IoState::Idle) {
                s.post_header_sqe(&mut sq);
            }
        }
        drop(sq);
    }

    fn handle_accept_cqe<F>(
        &mut self,
        listener_token: Token,
        result: i32,
        flags: u32,
        handler: &mut F,
    ) where
        F: for<'a> FnMut(PollEvent<'a>),
    {
        if result < 0 {
            warn!(token = ?listener_token, errno = -result, "accept CQE error");
            self.maybe_rearm_accept(listener_token, flags);
            return;
        }

        let client_fd = result;
        // SAFETY: kernel handed us ownership of this fd.
        let client_stream = unsafe { std::net::TcpStream::from_raw_fd(client_fd) };

        if client_stream.set_nonblocking(true).is_err() ||
            client_stream.set_nodelay(true).is_err()
        {
            error!("socket option set failed for accepted fd {client_fd}");
            return;
        }
        if let Some(size) = self.socket_buf_size {
            set_socket_buf_size(&client_stream, size);
        }

        let peer_addr = match client_stream.peer_addr() {
            Ok(a) => a,
            Err(e) => {
                error!("peer_addr for accepted fd {client_fd}: {e}");
                return;
            }
        };
        info!(?peer_addr, "client connected");

        let stream_token = Token(self.next_token);
        self.next_token += 1;

        let mut tcp = TcpStream::new(
            client_stream,
            stream_token,
            peer_addr,
            self.telemetry,
            self.dcache.clone(),
        );
        if let Some(msg) = &self.on_connect_msg {
            if tcp.write_or_enqueue_with(|buf| buf.extend_from_slice(msg)) ==
                ConnState::Disconnected
            {
                return;
            }
        }
        handler(PollEvent::Accept { listener: listener_token, stream: stream_token, peer_addr });
        self.conns.push((stream_token, ConnectionVariant::Inbound(tcp)));
        // Post AFTER push so the SQE pointer targets the element's stable heap location.
        let last = self.conns.len() - 1;
        if let ConnectionVariant::Inbound(s) = &mut self.conns[last].1 {
            s.post_header_sqe(&mut self.ring.submission());
        }

        self.maybe_rearm_accept(listener_token, flags);
    }

    /// Re-post the multishot ACCEPT SQE if the kernel cancelled it
    /// (IORING_CQE_F_MORE absent).
    fn maybe_rearm_accept(&mut self, listener_token: Token, flags: u32) {
        if io_uring::cqueue::more(flags) {
            return;
        }
        let Some(i) = self.conns.iter().position(|(t, _)| *t == listener_token) else { return };
        if let ConnectionVariant::Listener(l) = &self.conns[i].1 {
            let sqe = opcode::AcceptMulti::new(types::Fd(l.as_raw_fd()))
                .build()
                .user_data(encode_user_data(listener_token, PHASE_ACCEPT));
            let _ = unsafe { self.ring.submission().push(&sqe) };
        }
    }

    fn handle_header_cqe<F>(&mut self, token: Token, result: i32, handler: &mut F)
    where
        F: for<'a> FnMut(PollEvent<'a>),
    {
        let Some(i) = self.conns.iter().position(|(t, _)| *t == token) else {
            safe_panic!("header CQE for unknown token {token:?}");
            return;
        };
        let s = match &mut self.conns[i].1 {
            ConnectionVariant::Outbound(s) | ConnectionVariant::Inbound(s) => s,
            ConnectionVariant::Listener(_) => {
                safe_panic!("header CQE for listener token {token:?}");
                return;
            }
        };
        match s.handle_header_cqe(result) {
            HeaderCqeOutcome::PayloadReady => {
                s.post_payload_sqe(&mut self.ring.submission(), self.dc_buf_index);
            }
            HeaderCqeOutcome::NeedHeaderSqe => {
                s.post_header_sqe(&mut self.ring.submission());
            }
            HeaderCqeOutcome::Idle => { /* will be re-armed in poll_with sweep */ }
            HeaderCqeOutcome::Disconnect => {
                handler(PollEvent::Disconnect { token });
                self.disconnect_at_index(i);
            }
        }
    }

    fn handle_payload_cqe<F>(&mut self, token: Token, result: i32, handler: &mut F)
    where
        F: for<'a> FnMut(PollEvent<'a>),
    {
        let Some(i) = self.conns.iter().position(|(t, _)| *t == token) else {
            safe_panic!("payload CQE for unknown token {token:?}");
            return;
        };
        let s = match &mut self.conns[i].1 {
            ConnectionVariant::Outbound(s) | ConnectionVariant::Inbound(s) => s,
            ConnectionVariant::Listener(_) => {
                safe_panic!("payload CQE for listener token {token:?}");
                return;
            }
        };

        // Transmute to 'static so we can call the handler without holding a
        // borrow on conns. Safe because:
        // - Cached variant holds only DCacheRef (indices, no slice ptr).
        // - Raw variant's slice is into the stream's heap buf; conns[i] is not
        //   removed (handler can only write to other tokens), so the buf lives.
        let outcome: PayloadCqeOutcome<'static> =
            unsafe { std::mem::transmute(s.handle_payload_cqe(result)) };

        match outcome {
            PayloadCqeOutcome::Done { payload, send_ts } => {
                handler(PollEvent::Message { token, payload, send_ts });
                // Re-arm for the next frame.
                if let ConnectionVariant::Outbound(s) | ConnectionVariant::Inbound(s) =
                    &mut self.conns[i].1
                {
                    s.post_header_sqe(&mut self.ring.submission());
                }
            }
            PayloadCqeOutcome::NeedPayloadSqe => {
                if let ConnectionVariant::Outbound(s) | ConnectionVariant::Inbound(s) =
                    &mut self.conns[i].1
                {
                    s.post_payload_sqe(&mut self.ring.submission(), self.dc_buf_index);
                }
            }
            PayloadCqeOutcome::Disconnect => {
                handler(PollEvent::Disconnect { token });
                self.disconnect_at_index(i);
            }
        }
    }
}

/// Non-blocking TCP connector/acceptor built on io_uring.
///
/// Manages outbound (auto-reconnecting) connections and inbound connections
/// accepted from registered listeners. Drive all IO by calling [`poll_with`]
/// in a tight loop.
///
/// When a [`DCache`] is attached via [`with_dcache`], its backing memory is
/// registered as a fixed io_uring buffer. Payload bytes are then written
/// directly into reserved DCache slots via `READ_FIXED` SQEs — no copy.
pub struct TcpConnector {
    conn_mgr: ConnectionManager,
}

impl Default for TcpConnector {
    fn default() -> Self {
        Self { conn_mgr: ConnectionManager::default() }
    }
}

impl TcpConnector {
    pub fn with_reconnect_interval(mut self, interval: FluxDuration) -> Self {
        self.conn_mgr.reconnector = Repeater::every(interval);
        self
    }

    pub fn with_on_connect_msg(mut self, msg: Vec<u8>) -> Self {
        assert!(msg.len() <= TcpStream::SEND_BUF_SIZE, "on_connect_msg exceeds send buffer size");
        self.conn_mgr.on_connect_msg = Some(msg);
        self
    }

    pub fn with_dcache(mut self, dcache: Arc<DCache>) -> Self {
        self.conn_mgr.register_dcache(&dcache);
        self.conn_mgr.dcache = Some(dcache);
        self
    }

    pub fn with_telemetry(mut self, telemetry: TcpTelemetry) -> Self {
        self.conn_mgr.telemetry = telemetry;
        self
    }

    pub fn with_socket_buf_size(mut self, size: usize) -> Self {
        self.conn_mgr.socket_buf_size = Some(size);
        self
    }

    /// Poll io_uring once (non-blocking) and dispatch events via `handler`.
    #[inline]
    pub fn poll_with<F>(&mut self, mut handler: F) -> bool
    where
        F: for<'a> FnMut(PollEvent<'a>),
    {
        self.conn_mgr.maybe_reconnect();
        for token in self.conn_mgr.reconnected_to.drain(..) {
            handler(PollEvent::Reconnect { token });
            // Post initial header SQE for the reconnected stream.
            if let Some(i) = self.conn_mgr.conns.iter().position(|(t, _)| *t == token) {
                if let ConnectionVariant::Outbound(s) = &mut self.conn_mgr.conns[i].1 {
                    if matches!(s.io_state, IoState::Idle) {
                        s.post_header_sqe(&mut self.conn_mgr.ring.submission());
                    }
                }
            }
        }
        let had_cqes = !self.conn_mgr.ring.completion().is_empty();
        self.conn_mgr.poll_with(&mut handler);
        had_cqes
    }

    #[inline]
    pub fn write_or_enqueue_with<F>(&mut self, where_to: SendBehavior, serialise: F)
    where
        F: Fn(&mut Vec<u8>),
    {
        self.conn_mgr.write_or_enqueue_with(serialise, where_to);
    }

    pub fn disconnect_outbound(&mut self) {
        self.conn_mgr.disconnect_all_outbound();
    }

    pub fn disconnect(&mut self, token: Token) {
        self.conn_mgr.disconnect_token(token);
    }

    pub fn connect(&mut self, addr: SocketAddr) -> Option<Token> {
        self.conn_mgr.connect(addr)
    }

    pub fn listen_at(&mut self, addr: SocketAddr) -> Option<Token> {
        self.conn_mgr.listen_at(addr)
    }

    pub fn currently_disconnected(&self) -> impl Iterator<Item = Token> {
        self.conn_mgr.currently_disconnected()
    }

    pub fn force_reconnect(&mut self) {
        self.conn_mgr.force_reconnect();
    }
}
