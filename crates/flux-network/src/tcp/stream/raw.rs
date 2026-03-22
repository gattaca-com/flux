use std::{
    io::{self, Read, Write},
    net::SocketAddr,
};

use flux_timing::Nanos;
use mio::{Registry, Token, event::Event};
use tracing::{debug, warn};

use super::{ConnState, MessagePayload, ReadOutcome, SendBacklog, TcpTelemetry, TcpTimers};

const RX_BUF_SIZE: usize = 32 * 1024;

/// Single mio-backed TCP connection with no framing.
///
/// Delivers raw byte slices to the caller on each readable event.
/// The caller is responsible for protocol parsing and buffer accumulation.
pub(super) struct RawStream {
    stream: mio::net::TcpStream,
    peer_addr: SocketAddr,
    rx_buf: Vec<u8>,
    send_buf: Vec<u8>,
    backlog: SendBacklog,
    timers: Option<TcpTimers>,
}

impl RawStream {
    pub(super) const SEND_BUF_SIZE: usize = 32 * 1024;

    #[inline(never)]
    pub(super) fn new(
        stream: mio::net::TcpStream,
        token: Token,
        peer_addr: SocketAddr,
        telemetry: TcpTelemetry,
    ) -> Self {
        let timers = match telemetry {
            TcpTelemetry::Disabled => None,
            TcpTelemetry::Enabled { app_name } => {
                let local_port = stream.local_addr().map(|a| a.port()).unwrap_or(0);
                let peer = peer_addr.to_string();
                Some(TcpTimers::new(app_name, &format!("{local_port}-{peer}")))
            }
        };
        Self {
            backlog: SendBacklog::new(token),
            stream,
            peer_addr,
            rx_buf: vec![0; RX_BUF_SIZE],
            send_buf: vec![0; Self::SEND_BUF_SIZE],
            timers,
        }
    }

    #[inline]
    pub(super) fn reset_with_new_stream(
        &mut self,
        registry: &Registry,
        stream: mio::net::TcpStream,
        on_connect_msg: Option<&Vec<u8>>,
    ) -> ConnState {
        self.send_buf.clear();
        self.stream = stream;
        if !self.backlog.is_empty() {
            self.backlog.disarm();
            if let Some(message) = on_connect_msg {
                return self.backlog.enqueue_front(registry, &mut self.stream, message.clone());
            }
            self.backlog.arm_writable(registry, &mut self.stream)
        } else if let Some(message) = on_connect_msg {
            self.write_or_enqueue_with(registry, |bytes| bytes.extend_from_slice(message))
        } else {
            ConnState::Alive
        }
    }

    #[inline]
    pub(super) fn poll_with<F>(
        &mut self,
        registry: &Registry,
        ev: &Event,
        on_msg: &mut F,
    ) -> ConnState
    where
        F: for<'a> FnMut(Token, MessagePayload<'a>, Nanos),
    {
        if ev.is_readable() {
            loop {
                match self.read_raw() {
                    ReadOutcome::PayloadDone { payload, send_ts } => {
                        on_msg(ev.token(), payload, send_ts);
                    }
                    ReadOutcome::WouldBlock => break,
                    ReadOutcome::Disconnected => return ConnState::Disconnected,
                }
            }
        }

        if ev.is_writable() &&
            self.backlog.drain(registry, &mut self.stream) == ConnState::Disconnected
        {
            return ConnState::Disconnected;
        }

        ConnState::Alive
    }

    #[inline]
    pub(super) fn write_or_enqueue_with<F>(
        &mut self,
        registry: &Registry,
        serialise: F,
    ) -> ConnState
    where
        F: Fn(&mut Vec<u8>),
    {
        self.send_buf.clear();
        serialise(&mut self.send_buf);
        if self.send_buf.is_empty() {
            return ConnState::Alive;
        }

        if !self.backlog.is_empty() {
            if self.backlog.drain(registry, &mut self.stream) == ConnState::Disconnected {
                return ConnState::Disconnected;
            }
            if !self.backlog.is_empty() {
                let data = self.send_buf.clone();
                return self.backlog.enqueue_back(registry, &mut self.stream, data);
            }
        }

        match self.stream.write(&self.send_buf) {
            Ok(0) => {
                warn!("tcp: stream failed to write, disconnecting");
                ConnState::Disconnected
            }
            Ok(n) if n == self.send_buf.len() => ConnState::Alive,
            Ok(n) => {
                let data = self.send_buf[n..].to_vec();
                self.backlog.enqueue_back(registry, &mut self.stream, data)
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let data = self.send_buf.clone();
                self.backlog.enqueue_back(registry, &mut self.stream, data)
            }
            Err(err) => {
                warn!(?err, "tcp: stream write fail");
                ConnState::Disconnected
            }
        }
    }

    #[inline]
    pub(super) fn has_backlog(&self) -> bool {
        !self.backlog.is_empty()
    }

    #[inline]
    pub(super) fn drain_backlog(&mut self, registry: &Registry) -> ConnState {
        self.backlog.drain(registry, &mut self.stream)
    }

    pub(super) fn close(&mut self, registry: &Registry) {
        debug!("terminating connection");
        let _ = registry.deregister(&mut self.stream);
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }

    pub(super) fn peer(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Read available bytes into rx_buf, returning everything received.
    ///
    /// Loops until WouldBlock. If the buffer fills, returns what was read so
    /// far — mio will re-fire a readable event while kernel data remains.
    #[inline]
    fn read_raw(&mut self) -> ReadOutcome<'_> {
        let mut total = 0;
        loop {
            match self.stream.read(&mut self.rx_buf[total..]) {
                Ok(0) => {
                    // Peer closed. If we already accumulated bytes, deliver
                    // them first — the next call will see Ok(0) again and
                    // return Disconnected.
                    if total > 0 {
                        return ReadOutcome::PayloadDone {
                            payload: MessagePayload::Raw(&self.rx_buf[..total]),
                            send_ts: Nanos(0),
                        };
                    }
                    return ReadOutcome::Disconnected;
                }
                Ok(n) => {
                    total += n;
                    if total == self.rx_buf.len() {
                        // Buffer full; emit now. Caller must loop — mio is
                        // edge-triggered and won't re-fire until new data arrives.
                        let _ = &mut self.timers; // suppress unused warning
                        return ReadOutcome::PayloadDone {
                            payload: MessagePayload::Raw(&self.rx_buf[..total]),
                            send_ts: Nanos(0),
                        };
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if total > 0 {
                        return ReadOutcome::PayloadDone {
                            payload: MessagePayload::Raw(&self.rx_buf[..total]),
                            send_ts: Nanos(0),
                        };
                    }
                    return ReadOutcome::WouldBlock;
                }
                Err(err) => {
                    debug!(?err, "tcp: read raw");
                    return ReadOutcome::Disconnected;
                }
            }
        }
    }
}
