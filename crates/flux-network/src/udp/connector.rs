//! UDP side of [`crate::NetworkDriver`]: sockets, peers, and the per-poll
//! drive.
//!
//! One socket per `listen_at` (shared by every peer that dials it) and one per
//! `connect` (owned by that single peer). Peers hold all reliability state,
//! see [`UdpPeer`]. Message bytes live once in a [`MsgStore`] shared by every
//! peer sending them, and unsent fragments of all peers on a socket go out in
//! one `sendmmsg` batch.

use std::{
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    os::fd::AsRawFd,
};

use flux::spine::{SpineProducerWithDCache, SpineProducers};
use flux_communication::Timer;
use flux_timing::{Duration, Instant, Nanos};
use flux_utils::{DCache, DCachePtr, safe_panic};
use mio::{Events, Interest, Poll, Registry, Token, event::Event, net::UdpSocket};
use tracing::{debug, info, warn};

use super::{
    UdpConfig,
    peer::{MsgStore, PushOutcome, RxPayload, SendOutcome, Staged, UdpPeer, send_batch},
    sys::{BATCH, RecvBatch, SendBatch},
    wire::{HEADER_SIZE, Header, Kind},
};
use crate::{
    network_driver::{Config, PollEvent, SendBehavior},
    tcp::{TcpTelemetry, set_socket_buf_size},
};

const EVENTS_CAPACITY: usize = 128;

struct Endpoint {
    token: Token,
    socket: UdpSocket,
    listener: bool,
    writable_armed: bool,
}

/// One decoded datagram and where it came from.
struct Datagram<'a> {
    header: Header,
    payload: &'a [u8],
    from: SocketAddr,
    now: Instant,
}

/// Session id for a new connection attempt: mixes the TSC, the pid and the
/// peer token. Called once per connect, accept and reconnect.
fn new_session(salt: usize) -> u32 {
    let t = Instant::now().0;
    let mut x = t ^
        (t >> 32) ^
        (u64::from(std::process::id()) << 20) ^
        (salt as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x as u32
}

fn latency_timer(telemetry: TcpTelemetry, peer: SocketAddr) -> Option<Timer> {
    let TcpTelemetry::Enabled { app_name } = telemetry else { return None };
    Some(Timer::new(app_name, format!("udp_latency_{peer}")))
}

#[inline]
fn arm_writable(registry: &Registry, entry: &mut Endpoint) {
    if entry.writable_armed {
        return;
    }
    if let Err(err) =
        registry.reregister(&mut entry.socket, entry.token, Interest::READABLE | Interest::WRITABLE)
    {
        debug!(?err, "udp: reregister writable");
        return;
    }
    entry.writable_armed = true;
}

/// Queues the on-connect message for a peer whose session was just set up.
fn push_on_connect(store: &mut MsgStore, cfg: &Config, peer: &mut UdpPeer, now: Instant) {
    if let Some(msg) = &cfg.on_connect_msg {
        let slot = store.insert(&mut msg.clone());
        peer.push_message(store, slot, Nanos::now(), now);
        store.release(slot);
    }
}

#[inline]
fn socket_index(sockets: &[Endpoint], token: Token) -> usize {
    sockets.iter().position(|s| s.token == token).expect("udp peer without socket")
}

pub(crate) struct UdpManager {
    pub(crate) config: Config,
    pub(crate) dcache: Option<DCachePtr>,
    udp: UdpConfig,
    poll: Poll,
    events: Events,
    registry: Registry,
    sockets: Vec<Endpoint>,
    peers: Vec<UdpPeer>,
    store: MsgStore,
    batch: SendBatch,
    staged: [Staged; BATCH],
    /// Taken out while datagrams are dispatched so peers can be borrowed.
    recv: Option<RecvBatch>,
    pending_disconnects: Vec<Token>,
    next_token: usize,
    /// Peer maintenance runs at most this often; half the minimum RTO keeps
    /// recovery timing within tolerance while idle polls stay cheap.
    tick_interval: Duration,
    next_tick: Instant,
}

impl UdpManager {
    pub(crate) fn new(config: Config, udp: UdpConfig) -> Self {
        udp.validate();
        let poll = Poll::new().expect("couldn't set up a poll for connector");
        let registry = poll.registry().try_clone().expect("couldn't clone poll registry");
        Self {
            config,
            dcache: None,
            udp,
            poll,
            events: Events::with_capacity(EVENTS_CAPACITY),
            registry,
            sockets: Vec::new(),
            peers: Vec::new(),
            store: MsgStore::new(),
            batch: SendBatch::new(),
            staged: [Staged { peer: 0, seq: 0 }; BATCH],
            recv: Some(RecvBatch::new(udp.max_datagram_size)),
            pending_disconnects: Vec::new(),
            next_token: 0,
            tick_interval: udp.min_rto / 2_u32,
            next_tick: Instant::ZERO,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.sockets.is_empty()
    }

    fn next_token(&mut self) -> Token {
        let token = Token(self.next_token);
        self.next_token += 1;
        token
    }

    fn bind(&mut self, bind: SocketAddr, listener: bool) -> io::Result<Token> {
        let mut socket = UdpSocket::bind(bind)?;
        #[cfg(target_os = "linux")]
        if let Err(err) = self.batch.enable_gso(socket.as_raw_fd()) {
            debug!(?err, "UDP GSO unavailable");
        }
        if let Some(size) = self.config.socket_buf_size {
            set_socket_buf_size(&socket, size);
        }
        let token = self.next_token();
        self.registry.register(&mut socket, token, Interest::READABLE)?;
        self.sockets.push(Endpoint { token, socket, listener, writable_armed: false });
        Ok(token)
    }

    pub(crate) fn connect(&mut self, addr: SocketAddr) -> Option<Token> {
        let bind = match addr {
            SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
            SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
        };
        let token = self
            .bind(bind, false)
            .inspect_err(|e| warn!("couldn't open udp socket for {addr}: {e}"))
            .ok()?;
        let entry = self.sockets.last().unwrap();
        let mut peer = UdpPeer::new(
            addr,
            token,
            token,
            new_session(token.0),
            self.udp,
            latency_timer(self.config.telemetry, addr),
        );
        let now = Instant::now();
        peer.send_hello(&entry.socket, now);
        // First in the queue; nothing goes out before the handshake anyway.
        push_on_connect(&mut self.store, &self.config, &mut peer, now);
        self.peers.push(peer);
        Some(token)
    }

    pub(crate) fn listen_at(&mut self, addr: SocketAddr) -> Option<Token> {
        self.bind(addr, true)
            .inspect_err(|e| warn!("couldn't start listening at {addr:?}: {e}"))
            .ok()
    }

    /// Resets an outbound peer to a fresh session and starts dialling again.
    /// The on-connect message is queued behind any retained backlog; UDP
    /// delivery is unordered regardless.
    fn reset_outbound(&mut self, index: usize, now: Instant) {
        let peer = &mut self.peers[index];
        let session = new_session(peer.token.0);
        peer.mark_disconnected(
            self.config.drop_outbound_backlog_on_disconnect,
            session,
            &mut self.store,
        );
        push_on_connect(&mut self.store, &self.config, peer, now);
        let entry = &self.sockets[socket_index(&self.sockets, peer.token)];
        peer.send_hello(&entry.socket, now);
    }

    /// Removes an accepted peer, returning its store references.
    fn remove_peer(&mut self, index: usize) -> Token {
        let mut peer = self.peers.swap_remove(index);
        peer.release_all(&mut self.store);
        peer.token
    }

    /// Outbound peers renegotiate; accepted peers are dropped.
    fn drop_peer(&mut self, index: usize, now: Instant) {
        if self.peers[index].is_outbound() {
            self.reset_outbound(index, now);
        } else {
            self.remove_peer(index);
        }
    }

    fn drop_peer_pending(&mut self, index: usize, now: Instant) {
        self.pending_disconnects.push(self.peers[index].token);
        self.drop_peer(index, now);
    }

    pub(crate) fn disconnect(&mut self, token: Token) {
        let now = Instant::now();
        if let Some(i) = self.peers.iter().position(|p| p.token == token) {
            self.drop_peer(i, now);
            return;
        }
        let Some(k) = self.sockets.iter().position(|s| s.token == token && s.listener) else {
            return;
        };
        let mut i = self.peers.len();
        while i != 0 {
            i -= 1;
            if self.peers[i].socket_token == token {
                let dropped = self.remove_peer(i);
                self.pending_disconnects.push(dropped);
            }
        }
        let mut entry = self.sockets.swap_remove(k);
        let _ = self.registry.deregister(&mut entry.socket);
    }

    pub(crate) fn disconnect_outbound(&mut self) {
        let now = Instant::now();
        for i in 0..self.peers.len() {
            if self.peers[i].is_outbound() {
                self.reset_outbound(i, now);
            }
        }
    }

    /// Takes `payload` into the store and stages it for one peer or every
    /// peer, then flushes the sockets touched. `payload` comes back as a
    /// recycled buffer for the caller's next message.
    pub(crate) fn write(&mut self, where_to: SendBehavior, payload: &mut Vec<u8>) {
        let now = Instant::now();
        let ts = Nanos::now();
        let slot = self.store.insert(payload);
        match where_to {
            SendBehavior::Broadcast => {
                let mut i = self.peers.len();
                while i != 0 {
                    i -= 1;
                    if !self.stage_message(i, slot, ts, now) {
                        self.drop_peer_pending(i, now);
                    }
                }
                self.store.release(slot);
                for k in 0..self.sockets.len() {
                    self.flush_socket(k, now);
                }
            }
            SendBehavior::Single(token) => {
                if let Some(i) = self.peers.iter().position(|p| p.token == token) {
                    let k = socket_index(&self.sockets, self.peers[i].socket_token);
                    if !self.stage_message(i, slot, ts, now) {
                        self.drop_peer_pending(i, now);
                    }
                    self.store.release(slot);
                    self.flush_socket(k, now);
                } else {
                    self.store.release(slot);
                    if self.sockets.iter().any(|s| s.token == token) {
                        tracing::error!("cannot write to listener bound to token {token:?}");
                    } else {
                        tracing::error!("udp sending: unknown token {token:?}");
                    }
                }
            }
        }
    }

    /// Queues the stored message for one peer. `false` when the peer must be
    /// dropped: it violated the backlog limit or cannot hold the message.
    #[inline]
    fn stage_message(&mut self, index: usize, slot: u32, ts: Nanos, now: Instant) -> bool {
        let peer = &mut self.peers[index];
        if peer.is_outbound() &&
            !peer.is_connected() &&
            self.config.drop_outbound_backlog_on_disconnect
        {
            return true;
        }
        match peer.push_message(&mut self.store, slot, ts, now) {
            PushOutcome::Queued => !peer.backlog_exceeded(self.config.max_backlog),
            PushOutcome::WindowFull => false,
            PushOutcome::TooLarge => true,
        }
    }

    /// Sends every unsent fragment of every peer on socket `k`, batched across
    /// peers. Arms WRITABLE if the kernel stopped accepting.
    fn flush_socket(&mut self, k: usize, now: Instant) {
        let entry = &mut self.sockets[k];
        let fd = entry.socket.as_raw_fd();
        let mut n = 0;
        for i in 0..self.peers.len() {
            if self.peers[i].socket_token != entry.token {
                continue;
            }
            for seq in self.peers[i].unsent() {
                self.peers[i].stage(seq, &self.store, &mut self.batch);
                self.staged[n] = Staged { peer: i, seq };
                n += 1;
                if n == BATCH {
                    if !Self::dispatch(&mut self.batch, &self.staged, &mut self.peers, fd, n, now) {
                        arm_writable(&self.registry, entry);
                        return;
                    }
                    n = 0;
                }
            }
        }
        if n != 0 && !Self::dispatch(&mut self.batch, &self.staged, &mut self.peers, fd, n, now) {
            arm_writable(&self.registry, entry);
        }
    }

    /// Sends the staged batch and marks the accepted prefix. `false` if the
    /// kernel took less than everything.
    fn dispatch(
        batch: &mut SendBatch,
        staged: &[Staged; BATCH],
        peers: &mut [UdpPeer],
        fd: i32,
        n: usize,
        now: Instant,
    ) -> bool {
        let accepted = send_batch(batch, fd, n);
        for s in &staged[..accepted] {
            peers[s.peer].mark_sent(s.seq, now);
        }
        accepted == n
    }

    pub(crate) fn currently_disconnected(&self) -> impl Iterator<Item = Token> {
        self.peers.iter().filter(|p| p.is_outbound() && !p.is_connected()).map(|p| p.token)
    }

    pub(crate) fn force_reconnect(&mut self) {
        let now = Instant::now();
        for peer in self.peers.iter_mut().filter(|p| p.is_outbound() && !p.is_connected()) {
            let entry = &self.sockets[socket_index(&self.sockets, peer.token)];
            peer.send_hello(&entry.socket, now);
        }
    }

    /// Hello retries, retransmits, heartbeats and peer timeouts.
    fn tick(&mut self, now: Instant) {
        let peer_timeout = self.config.user_timeout;
        let mut i = self.peers.len();
        while i != 0 {
            i -= 1;
            let peer = &mut self.peers[i];
            let k = socket_index(&self.sockets, peer.socket_token);
            let entry = &mut self.sockets[k];
            match peer.tick(&self.store, &entry.socket, &mut self.batch, now, peer_timeout) {
                Ok(SendOutcome::Done) => {}
                Ok(SendOutcome::WouldBlock) => arm_writable(&self.registry, entry),
                Err(()) => {
                    warn!(addr = %peer.addr, "udp peer timed out");
                    self.drop_peer_pending(i, now);
                }
            }
        }
    }

    /// Creates the accepted peer for a hello from `from`.
    fn accept<F>(&mut self, k: usize, dgram: &Datagram<'_>, deliver: &mut F)
    where
        F: for<'a> FnMut(PollEvent<RxPayload<'a>>),
    {
        let token = self.next_token();
        let entry = &self.sockets[k];
        let mut peer = UdpPeer::new(
            dgram.from,
            token,
            entry.token,
            new_session(token.0),
            self.udp,
            latency_timer(self.config.telemetry, dgram.from),
        );
        let listener = entry.token;
        peer.on_hello(&dgram.header, &entry.socket, dgram.now);
        push_on_connect(&mut self.store, &self.config, &mut peer, dgram.now);
        info!(addr = %dgram.from, "udp client connected");
        deliver(PollEvent::Accept { listener, stream: token, peer_addr: dgram.from });
        self.peers.push(peer);
        self.flush_socket(k, dgram.now);
    }

    /// One received datagram.
    fn on_datagram<F>(
        &mut self,
        k: usize,
        dgram: &Datagram<'_>,
        dcache: Option<&DCache>,
        deliver: &mut F,
    ) where
        F: for<'a> FnMut(PollEvent<RxPayload<'a>>),
    {
        let Datagram { header, payload, from, now } = *dgram;
        let header = &header;
        let socket_token = self.sockets[k].token;
        let peer_index =
            self.peers.iter().position(|p| p.socket_token == socket_token && p.addr == from);

        match header.kind {
            Kind::Hello => {
                if !self.sockets[k].listener {
                    return;
                }
                if let Some(i) = peer_index {
                    if self.peers[i].on_hello(header, &self.sockets[k].socket, now) {
                        return;
                    }
                    let old = self.remove_peer(i);
                    deliver(PollEvent::Disconnect { token: old });
                }
                self.accept(k, dgram, deliver);
            }
            Kind::HelloAck => {
                let Some(i) = peer_index else { return };
                let peer = &mut self.peers[i];
                if !peer.is_outbound() {
                    return;
                }
                let Some(was_connected) = peer.on_hello_ack(header, now) else { return };
                let token = peer.token;
                debug!(addr = %from, "udp connected");
                if was_connected {
                    deliver(PollEvent::Reconnect { token });
                }
                self.flush_socket(k, now);
            }
            Kind::Reset => {
                let Some(i) = peer_index else { return };
                let peer = &self.peers[i];
                if peer.is_outbound() && peer.on_reset(header) {
                    warn!(addr = %from, "udp peer reset us, reconnecting");
                    let token = peer.token;
                    deliver(PollEvent::Disconnect { token });
                    self.drop_peer(i, now);
                }
            }
            Kind::Data | Kind::Ack => {
                let entry = &mut self.sockets[k];
                let Some(i) = peer_index else {
                    // Unknown sender on a listener: a client from before our
                    // restart, or one we dropped. Tell it to renegotiate.
                    if entry.listener {
                        UdpPeer::send_reset(&entry.socket, from, header.session);
                    }
                    return;
                };
                let peer = &mut self.peers[i];
                let token = peer.token;
                if header.kind == Kind::Data {
                    peer.on_data(header, payload, dcache, now, &mut |payload, send_ts| {
                        deliver(PollEvent::Message { token, payload, send_ts });
                    });
                } else if peer.on_ack(
                    header,
                    payload,
                    &mut self.store,
                    &entry.socket,
                    &mut self.batch,
                    now,
                ) == SendOutcome::WouldBlock
                {
                    arm_writable(&self.registry, entry);
                }
            }
        }
    }

    /// Readable/writable event on the socket at `k`.
    fn handle_event<F>(&mut self, k: usize, event: &Event, dcache: Option<&DCache>, deliver: &mut F)
    where
        F: for<'a> FnMut(PollEvent<RxPayload<'a>>),
    {
        let now = Instant::now();
        if event.is_readable() {
            let fd = self.sockets[k].socket.as_raw_fd();
            let mut recv = self.recv.take().expect("recv batch in use");
            loop {
                let n = match recv.recv(fd) {
                    Ok(n) => n,
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                    Err(err) => {
                        debug!(?err, "udp recv failed");
                        break;
                    }
                };
                for i in 0..n {
                    let Some((bytes, from)) = recv.datagram(i) else { continue };
                    let Some(header) = Header::decode(bytes) else { continue };
                    let dgram = Datagram { header, payload: &bytes[HEADER_SIZE..], from, now };
                    self.on_datagram(k, &dgram, dcache, deliver);
                }
            }
            self.recv = Some(recv);
        }

        if event.is_writable() {
            self.sockets[k].writable_armed = false;
            self.flush_socket(k, now);
        }
        let entry = &mut self.sockets[k];
        let token = entry.token;
        for peer in self.peers.iter_mut().filter(|p| p.socket_token == token) {
            if peer.take_ack_due() && peer.send_ack(&entry.socket, now) == SendOutcome::WouldBlock {
                arm_writable(&self.registry, entry);
            }
        }
        if event.is_writable() && !entry.writable_armed {
            if let Err(err) =
                self.registry.reregister(&mut entry.socket, entry.token, Interest::READABLE)
            {
                debug!(?err, "udp: reregister drop writable");
            }
        }
    }

    #[inline]
    fn drain_pending_disconnects<F>(&mut self, deliver: &mut F) -> bool
    where
        F: for<'a> FnMut(PollEvent<RxPayload<'a>>),
    {
        let had_pending = !self.pending_disconnects.is_empty();
        for token in self.pending_disconnects.drain(..) {
            deliver(PollEvent::Disconnect { token });
        }
        had_pending
    }

    /// One non-blocking poll pass with all housekeeping around it.
    fn drive<F>(&mut self, dcache: Option<&DCache>, deliver: &mut F) -> bool
    where
        F: for<'a> FnMut(PollEvent<RxPayload<'a>>),
    {
        let mut o = self.drain_pending_disconnects(deliver);
        let now = Instant::now();
        if now >= self.next_tick {
            self.next_tick = now + self.tick_interval;
            self.tick(now);
        }
        // Taken out so `handle_event` can borrow `self`; put back below.
        let mut events = std::mem::replace(&mut self.events, Events::with_capacity(0));
        if let Err(e) = self.poll.poll(&mut events, Some(std::time::Duration::ZERO)) {
            safe_panic!("got error polling {e}");
            self.events = events;
            return false;
        }
        for event in &events {
            o = true;
            let Some(k) = self.sockets.iter().position(|s| s.token == event.token()) else {
                debug!(token = ?event.token(), "ignoring stale udp readiness event");
                continue;
            };
            self.handle_event(k, event, dcache, deliver);
        }
        self.events = events;
        o |= self.drain_pending_disconnects(deliver);
        o
    }

    pub(crate) fn poll_with<F>(&mut self, mut handler: F) -> bool
    where
        F: for<'a> FnMut(PollEvent<&'a [u8]>),
    {
        let dcache = self.dcache;
        self.drive(dcache.as_deref(), &mut |event| match event {
            PollEvent::Message { token, payload: RxPayload::Raw(bytes), send_ts } => {
                handler(PollEvent::Message { token, payload: bytes, send_ts });
            }
            PollEvent::Message { payload: RxPayload::DCache(_), .. } => {
                safe_panic!("poll_with called on dcache connector; use poll_with_produce");
            }
            PollEvent::Accept { listener, stream, peer_addr } => {
                handler(PollEvent::Accept { listener, stream, peer_addr });
            }
            PollEvent::Reconnect { token } => handler(PollEvent::Reconnect { token }),
            PollEvent::Disconnect { token } => handler(PollEvent::Disconnect { token }),
        })
    }

    pub(crate) fn poll_with_produce<T, P, F>(&mut self, produce: &P, mut on_msg: F) -> bool
    where
        T: 'static + Copy,
        P: SpineProducers + AsRef<SpineProducerWithDCache<T>>,
        F: for<'a> FnMut(PollEvent<&'a [u8]>) -> Option<T>,
    {
        let dcache = self.dcache.expect("dcache required for poll_with_produce");
        self.drive(Some(&dcache), &mut |event| match event {
            PollEvent::Message { token, payload: RxPayload::DCache(dref), send_ts } => match dcache
                .map(dref, |bytes| on_msg(PollEvent::Message { token, payload: bytes, send_ts }))
            {
                Ok(Some(t)) => produce.produce_with_dref(t, dref, send_ts),
                Ok(None) => {}
                Err(e) => warn!("dcache map failed: {e}"),
            },
            PollEvent::Message { payload: RxPayload::Raw(_), .. } => {
                safe_panic!("poll_with_produce called on non-dcache connector; use poll_with");
            }
            PollEvent::Accept { listener, stream, peer_addr } => {
                let _ = on_msg(PollEvent::Accept { listener, stream, peer_addr });
            }
            PollEvent::Reconnect { token } => {
                let _ = on_msg(PollEvent::Reconnect { token });
            }
            PollEvent::Disconnect { token } => {
                let _ = on_msg(PollEvent::Disconnect { token });
            }
        })
    }
}
