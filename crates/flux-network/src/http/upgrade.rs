//! WebSocket upgrades on [`HttpNetwork`] connections; the framing is in
//! [`crate::websocket`].

use mio::Token;

use super::{
    Conn, HttpEvent, HttpNetwork, HttpRequest, Role, STACK_HEADERS, State, has_token, header_slots,
    valid_request, write_head_fields,
};
use crate::{
    NetworkCore,
    websocket::{Ws, WsKind, accept_key, release},
};

/// A frame header, the read crossing the poll budget and spilled TLS plaintext.
const WS_SLACK_BYTES: usize = 64 * 1024;

impl HttpNetwork {
    /// Allows WebSocket upgrades, taking messages up to `max_message_bytes`.
    pub fn with_websockets(mut self, max_message_bytes: usize) -> Self {
        assert!(self.group.is_none(), "configure before listen or connect");
        self.max_ws_message_bytes = Some(max_message_bytes);
        self
    }
    /// Answers a pending upgrade request with `101`; `key` is its
    /// `Sec-WebSocket-Key`. The idle sweep then skips it, so ping it.
    pub fn accept_websocket(&mut self, net: &mut NetworkCore, token: Token, key: &[u8]) -> bool {
        let Some(max) = self.max_ws_message_bytes else { return false };
        let Some(conn) = self
            .conns
            .iter_mut()
            .find(|c| c.token == token && matches!(c.role.accepted_state(), State::Pending))
        else {
            return false
        };
        let accept = accept_key(key);
        let ok = net.send_with(token, |out| {
            out.extend_from_slice(
                b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ",
            );
            out.extend_from_slice(&accept);
            out.extend_from_slice(b"\r\n\r\n");
        });
        if ok {
            conn.role = Role::Ws { ws: Ws::server(max), addr: None };
            conn.dirty = !conn.buf.is_empty();
        }
        ok
    }
    /// Sends a WebSocket handshake on a connected, unpooled endpoint;
    /// [`HttpEvent::WsOpen`] follows, or it redials as plain HTTP.
    pub fn websocket(
        &mut self,
        net: &mut NetworkCore,
        token: Token,
        path: &str,
        headers: &[(&str, &str)],
    ) -> bool {
        if !valid_request("GET", path, headers) ||
            self.pools.iter().any(|pool| pool.tokens.contains(&token))
        {
            return false
        }
        let Some(max) = self.max_ws_message_bytes else { return false };
        let Some(c) =
            self.conns.iter_mut().find(|c| c.token == token && c.role.outbound_head().is_none())
        else {
            return false
        };
        let Role::Outbound { addr, .. } = &c.role else { return false };
        let addr = *addr;
        let (ws, key) = Ws::client(max);
        let sent = net.send_with(token, |out| {
            write_head_fields(out, "GET", path, headers, &addr);
            out.extend_from_slice(
                b"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ",
            );
            out.extend_from_slice(&key);
            out.extend_from_slice(b"\r\n\r\n");
        });
        if sent {
            c.role = Role::Ws { ws, addr: Some(addr) };
        }
        sent
    }
    /// Sends one unfragmented frame; pings and pongs take at most 125 bytes.
    pub fn ws_send(
        &mut self,
        net: &mut NetworkCore,
        token: Token,
        kind: WsKind,
        payload: &[u8],
    ) -> bool {
        self.conns.iter().find(|c| c.token == token).is_some_and(|c| match &c.role {
            Role::Ws { ws, .. } => ws.send(net, token, kind, payload),
            _ => false,
        })
    }
    /// Sends a close frame and drops the connection once it is written. A
    /// client endpoint then redials; [`Self::remove`] stops it.
    pub fn ws_close(&mut self, net: &mut NetworkCore, token: Token, code: u16) -> bool {
        self.conns.iter_mut().find(|c| c.token == token).is_some_and(|c| match &mut c.role {
            Role::Ws { ws, .. } => ws.is_open() && ws.close(net, token, code),
            _ => false,
        })
    }
    pub(super) fn parse_ws<F>(&mut self, net: &mut NetworkCore, i: usize, handler: &mut F)
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        self.conns[i].dirty = false;
        if let Role::Ws { ws: Ws { expect: Some(expect), .. }, .. } = self.conns[i].role &&
            !self.open_ws(net, i, expect, handler)
        {
            return
        }
        let Conn { token, buf, role: Role::Ws { ws, .. }, .. } = &mut self.conns[i] else { return };
        let held = buf.len();
        ws.parse(net, *token, buf, handler);
        release(buf, held);
    }
    /// Opens a client WebSocket on a valid `101`; anything else drops it.
    fn open_ws<F>(
        &mut self,
        net: &mut NetworkCore,
        i: usize,
        expect: [u8; 28],
        handler: &mut F,
    ) -> bool
    where
        F: for<'a> FnMut(HttpEvent<'a>),
    {
        let buf = &self.conns[i].buf;
        let (mut stack_slots, mut heap_slots) =
            ([httparse::EMPTY_HEADER; STACK_HEADERS], Vec::new());
        let hs = header_slots(&mut stack_slots, &mut heap_slots, self.max_headers);
        let mut response = httparse::Response::new(hs);
        let head = match response.parse(buf) {
            Ok(httparse::Status::Partial) if buf.len() <= self.max_head_bytes => return false,
            Ok(httparse::Status::Complete(head)) if head <= self.max_head_bytes => Some(head),
            _ => None,
        };
        let header = |name: &str| {
            response.headers.iter().find(|h| h.name.eq_ignore_ascii_case(name)).map(|h| h.value)
        };
        let accepted = response.code == Some(101) &&
            has_token(response.headers, "upgrade", b"websocket") &&
            has_token(response.headers, "connection", b"upgrade") &&
            header("sec-websocket-accept") == Some(&expect[..]) &&
            header("sec-websocket-extensions").is_none();
        let Some(head) = head.filter(|_| accepted) else {
            self.fail_outbound(net, i);
            return false
        };
        if let Role::Ws { ws, .. } = &mut self.conns[i].role {
            ws.expect = None;
        }
        self.conns[i].buf.drain(..head);
        handler(HttpEvent::WsOpen { token: self.conns[i].token });
        true
    }
}

impl Role {
    /// A WebSocket buffers an unfinished frame plus one poll's reads.
    pub(super) fn rx_limit(&self, limit: usize) -> usize {
        match self {
            Self::Ws { ws, .. } => {
                ws.max.saturating_mul(2).saturating_add(limit).saturating_add(WS_SLACK_BYTES)
            }
            _ => limit,
        }
    }
}

impl HttpRequest<'_> {
    /// A WebSocket opening handshake; answer it with
    /// [`HttpNetwork::accept_websocket`].
    pub fn is_websocket_upgrade(&self) -> bool {
        self.method == "GET" &&
            has_token(self.headers, "upgrade", b"websocket") &&
            has_token(self.headers, "connection", b"upgrade") &&
            self.header("sec-websocket-version") == Some(b"13") &&
            self.header("sec-websocket-key").is_some_and(|key| key.len() == 24)
    }
}
