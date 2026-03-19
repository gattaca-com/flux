use std::{
    collections::VecDeque,
    io::{self, Write},
};

use mio::{Interest, Registry, Token};
use tracing::debug;

use super::ConnState;

pub(super) struct SendBacklog {
    backlog: VecDeque<Vec<u8>>,
    /// Byte offset into the front backlog entry (partially written head).
    pub(super) cursor: usize,
    writable_armed: bool,
    token: Token,
}

impl SendBacklog {
    pub(super) fn new(token: Token) -> Self {
        Self { backlog: VecDeque::with_capacity(64), cursor: 0, writable_armed: false, token }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.backlog.is_empty()
    }

    /// Disarm WRITABLE interest without clearing the backlog.
    ///
    /// Used during reconnect: the new stream is not yet registered for
    /// WRITABLE, so arm_writable must reregister unconditionally.
    pub(super) fn disarm(&mut self) {
        self.writable_armed = false;
    }

    pub(super) fn enqueue_front(
        &mut self,
        registry: &Registry,
        stream: &mut mio::net::TcpStream,
        data: Vec<u8>,
    ) -> ConnState {
        self.backlog.push_front(data);
        self.arm_writable(registry, stream)
    }

    pub(super) fn enqueue_back(
        &mut self,
        registry: &Registry,
        stream: &mut mio::net::TcpStream,
        data: Vec<u8>,
    ) -> ConnState {
        self.backlog.push_back(data);
        self.arm_writable(registry, stream)
    }

    /// Flush queued data until kernel blocks or queue empty.
    pub(super) fn drain(
        &mut self,
        registry: &Registry,
        stream: &mut mio::net::TcpStream,
    ) -> ConnState {
        while let Some(front) = self.backlog.front() {
            match stream.write(&front[self.cursor..]) {
                Ok(0) => return ConnState::Disconnected,
                Ok(n) => {
                    self.cursor += n;
                    if self.cursor == front.len() {
                        self.backlog.pop_front();
                        self.cursor = 0;
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => {
                    debug!(?err, "tcp: write from backlog");
                    return ConnState::Disconnected;
                }
            }
        }

        // Drop WRITABLE interest only when fully drained
        if self.backlog.is_empty() && self.writable_armed {
            if let Err(err) = registry.reregister(stream, self.token, Interest::READABLE) {
                debug!(?err, "tcp: reregister drop writable");
                return ConnState::Disconnected;
            }
            self.writable_armed = false;
        }

        ConnState::Alive
    }

    pub(super) fn arm_writable(
        &mut self,
        registry: &Registry,
        stream: &mut mio::net::TcpStream,
    ) -> ConnState {
        if !self.writable_armed {
            if let Err(err) =
                registry.reregister(stream, self.token, Interest::READABLE | Interest::WRITABLE)
            {
                debug!(?err, "tcp: poll reregister");
                return ConnState::Disconnected;
            }
            self.writable_armed = true;
        }
        ConnState::Alive
    }
}
