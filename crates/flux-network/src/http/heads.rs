//! Requests still arriving on an accepted connection, and their heads
//! delivered early; see [`HttpNetwork::with_request_heads`].

use super::{Conn, HttpNetwork, Role, State, chunked};
use crate::NetworkCore;

#[derive(Clone, Copy, Default)]
pub(super) struct Partial {
    /// (Encoded offset, decoded length) after the last complete chunk.
    pub(super) chunk_scan: (usize, usize),
    /// A delivered head waiting for its body.
    pub(super) head: Option<HeadSeen>,
    /// Bytes of an incomplete head already searched for its end.
    pub(super) head_scan: usize,
}

#[derive(Clone, Copy)]
pub(super) struct HeadSeen {
    pub(super) end: usize,
    /// `None` when chunked.
    pub(super) length: Option<usize>,
    pub(super) expects_continue: bool,
}

impl HttpNetwork {
    /// Whether request `i` is incomplete, found without parsing its head again.
    pub(super) fn incomplete(&mut self, net: &mut NetworkCore, i: usize, over_limit: bool) -> bool {
        let (max_body_bytes, max_headers) = (self.max_body_bytes, self.max_headers);
        let Conn { buf, role, .. } = &mut self.conns[i];
        let partial = role.partial();
        if let Some(seen) = partial.head {
            let body = &buf[seen.end..];
            let complete = match seen.length {
                Some(length) => Ok(body.len() >= length),
                None => chunked::end(body, max_body_bytes, max_headers, &mut partial.chunk_scan)
                    .map(|end| end.is_some()),
            };
            match complete {
                Ok(true) => return false,
                Ok(false) if over_limit => self.error(net, i, 413),
                Ok(false) if seen.expects_continue => self.send_continue(net, i),
                Ok(false) => {}
                Err(status) => self.error(net, i, status),
            }
            return true
        }
        // A blank line may end the head, a malformed one with a bare LF included.
        let scanned = std::mem::replace(&mut partial.head_scan, buf.len());
        let new = &buf[scanned.saturating_sub(2)..];
        if scanned == 0 || new.windows(2).any(|w| w == b"\n\n" || w == b"\n\r") {
            return false
        }
        if over_limit || buf.len() > self.max_head_bytes {
            self.error(net, i, 431);
        }
        true
    }
}

impl Role {
    pub(super) fn chunk_scan(&self) -> (usize, usize) {
        match self {
            Self::Accepted { partial, .. } => partial.chunk_scan,
            _ => (0, 0),
        }
    }
    /// Holds a delivered head until its body is in; answering it closes.
    pub(super) fn announce(&mut self, seen: HeadSeen, chunk_scan: (usize, usize), head: bool) {
        self.set_accepted_state(State::Head);
        self.set_accepted_close(true);
        self.set_accepted_head_request(head);
        *self.partial() = Partial { chunk_scan, head: Some(seen), head_scan: 0 };
    }
}
