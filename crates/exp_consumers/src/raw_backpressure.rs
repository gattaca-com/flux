//! The overseer's headless `PostgreSQL` server: a raw byte stream, hard
//! backpressure, fairness by round robin, and both close flavours.
//!
//! Raw framing hands the tile read chunks without message boundaries; the
//! protocol layer reassembles newline-terminated queries per connection. The
//! backpressure is exactly the original's: a per-connection input cap the
//! tile enforces (fatal `54000`), an output cap that cuts the connection
//! immediately, and the group's hard `max_backlog_bytes` the *stack* enforces
//! by disconnecting a peer that stops reading. At most one query is processed
//! per iteration, round robin from the last served, and a reply chooses its
//! close: `Immediate` cuts the connection where it stands, `Drained` lets the
//! reply flush first. The leaf is `StreamService`; the connection table, the
//! cursor and the caps are all tile state.

use flux_network::{
    Token,
    stream::{StreamEvent, StreamNetwork, StreamService},
};
use flux_timing::Duration;

/// How a reply wants its connection closed, if at all.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CloseMode {
    Immediate,
    Drained,
}

/// Most simultaneous connections before new ones are refused politely. A
/// refused connection still counts until its drained close completes, as the
/// original counts everything in its map.
pub const MAX_CONNECTIONS: usize = 2;
/// Most buffered input per connection before the fatal `54000`.
pub const MAX_INPUT: usize = 4096;
/// Most pending output per connection before the connection is cut
/// immediately, reply and all.
pub const MAX_OUTPUT: usize = 96 * 1024;

struct Conn {
    token: Token,
    input: Vec<u8>,
    output: Vec<u8>,
    close: Option<CloseMode>,
    /// A failed connection stops accumulating input and only flushes.
    failed: bool,
}

/// The server tile: per-connection reassembly, replies and closes flushed
/// after processing, one query per iteration.
pub struct PgTile {
    pub service: StreamService,
    conns: Vec<Conn>,
    cursor: usize,
    pub served: Vec<Token>,
}

impl PgTile {
    pub fn new(service: StreamService) -> Self {
        Self { service, conns: Vec::new(), cursor: 0, served: Vec::new() }
    }

    pub fn connections(&self) -> usize {
        self.conns.len()
    }

    /// Pulls the iteration's transport events into the connection table.
    fn pull(&mut self) {
        while let Some(event) = self.service.next_event() {
            match event {
                StreamEvent::Accepted { token, .. } => {
                    let over_capacity = self.conns.len() >= MAX_CONNECTIONS;
                    let mut conn = Conn {
                        token,
                        input: Vec::new(),
                        output: Vec::new(),
                        close: None,
                        failed: false,
                    };
                    if over_capacity {
                        conn.output.extend_from_slice(b"53300 too many connections\n");
                        conn.close = Some(CloseMode::Drained);
                        conn.failed = true;
                    }
                    self.conns.push(conn);
                }
                StreamEvent::Message { token, payload, .. } => {
                    let Some(conn) = self.conns.iter_mut().find(|conn| conn.token == token) else {
                        continue;
                    };
                    if conn.failed {
                        continue;
                    }
                    if conn.input.len() + payload.len() > MAX_INPUT {
                        conn.output.extend_from_slice(b"54000 program limit exceeded\n");
                        conn.close = Some(CloseMode::Drained);
                        conn.failed = true;
                        conn.input.clear();
                    } else {
                        conn.input.extend_from_slice(payload);
                    }
                }
                StreamEvent::Disconnected { token, .. } => {
                    self.conns.retain(|conn| conn.token != token);
                }
                StreamEvent::Connected { .. } => unreachable!("this group only listens"),
            }
        }
    }

    /// Processes at most one complete query, round robin from the connection
    /// after the last one served.
    fn process_one(&mut self, execute: impl FnOnce(&[u8]) -> (Vec<u8>, Option<CloseMode>)) {
        let count = self.conns.len();
        if count == 0 {
            return;
        }
        for offset in 0..count {
            let index = (self.cursor + offset) % count;
            let conn = &mut self.conns[index];
            if conn.failed || conn.close.is_some() {
                continue;
            }
            let Some(line_end) = conn.input.iter().position(|byte| *byte == b'\n') else {
                continue;
            };
            let query: Vec<u8> = conn.input.drain(..=line_end).take(line_end).collect();
            let (reply, close) = execute(&query);
            conn.output.extend_from_slice(&reply);
            conn.close = close;
            self.served.push(conn.token);
            self.cursor = (index + 1) % count;
            return;
        }
    }

    /// Flushes every pending reply and applies the closes: output beyond
    /// [`MAX_OUTPUT`] makes the close `Immediate` first, a refused send
    /// becomes `Immediate`, then `Immediate` disconnects where the connection
    /// stands and `Drained` lets the reply out first.
    fn flush(&mut self) {
        for conn in &mut self.conns {
            if conn.output.len() > MAX_OUTPUT {
                conn.close = Some(CloseMode::Immediate);
            }
            if !conn.output.is_empty() {
                let output = std::mem::take(&mut conn.output);
                if !self.service.send_with(conn.token, |buf| buf.extend_from_slice(&output)) {
                    conn.close = Some(CloseMode::Immediate);
                }
            }
            match conn.close.take() {
                Some(CloseMode::Immediate) => {
                    let _ = self.service.disconnect(conn.token);
                    conn.failed = true;
                }
                Some(CloseMode::Drained) => {
                    let _ = self.service.disconnect_when_drained(conn.token);
                    conn.failed = true;
                }
                None => {}
            }
        }
    }

    /// One server iteration: the network pass, the pull, one query, the
    /// flush.
    pub fn step(
        &mut self,
        net: &mut StreamNetwork,
        execute: impl FnOnce(&[u8]) -> (Vec<u8>, Option<CloseMode>),
    ) {
        let _ = net.drive(Some(Duration::ZERO), std::slice::from_mut(&mut self.service));
        self.pull();
        self.process_one(execute);
        self.flush();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{ErrorKind, Read, Write},
        net::TcpStream,
    };

    use flux_network::stream::{ConnectionGroupConfig, Framing, StreamNetwork, StreamService};

    use super::{CloseMode, PgTile};
    use crate::harness::{bound_addr, ephemeral, expired};

    /// The test's `execute`: ping/quit/die/flood, as the close and
    /// backpressure scenarios need them.
    fn execute(query: &[u8]) -> (Vec<u8>, Option<CloseMode>) {
        match query {
            b"ping" => (b"pong\n".to_vec(), None),
            b"quit" => (b"bye\n".to_vec(), Some(CloseMode::Drained)),
            b"die" => (Vec::new(), Some(CloseMode::Immediate)),
            b"flood" => (vec![b'x'; 64 * 1024], None),
            b"megaflood" => (vec![b'x'; 100 * 1024], None),
            other => panic!("an unexpected query: {other:?}"),
        }
    }

    fn raw_server(max_backlog: Option<usize>) -> (StreamNetwork, PgTile, std::net::SocketAddr) {
        let mut net = StreamNetwork::default();
        let mut service = StreamService::new(net.add_group(ConnectionGroupConfig {
            name: "postgres",
            framing: Framing::Raw,
            max_frame_size: 128 * 1024,
            backlog_warn_bytes: Some(4096),
            max_backlog_bytes: max_backlog,
            socket_buf_size: Some(4096),
            ..ConnectionGroupConfig::default()
        }));
        let addr = bound_addr(&service.listen(ephemeral()).unwrap());
        (net, PgTile::new(service), addr)
    }

    fn read_available(stream: &mut TcpStream, into: &mut Vec<u8>) -> bool {
        let mut chunk = [0u8; 4096];
        match stream.read(&mut chunk) {
            Ok(0) => true,
            Ok(n) => {
                into.extend_from_slice(&chunk[..n]);
                false
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => false,
            Err(err) => panic!("the client read failed: {err}"),
        }
    }

    fn client(addr: std::net::SocketAddr) -> TcpStream {
        let stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(std::time::Duration::from_millis(5))).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
    }

    #[test]
    fn round_robin_fairness_and_both_close_flavours() {
        let started = std::time::Instant::now();
        let (mut net, mut server, addr) = raw_server(None);
        let mut alice = client(addr);
        let mut bob = client(addr);

        // Both clients queue a query before any is served: one query per
        // iteration, round robin, so the two are served on consecutive
        // iterations, one each.
        alice.write_all(b"ping\n").unwrap();
        bob.write_all(b"ping\n").unwrap();
        let (mut alice_read, mut bob_read) = (Vec::new(), Vec::new());
        while server.served.len() < 2 {
            assert!(!expired(started), "both pings were never served");
            server.step(&mut net, execute);
            let _ = read_available(&mut alice, &mut alice_read);
            let _ = read_available(&mut bob, &mut bob_read);
        }
        assert_ne!(server.served[0], server.served[1], "round robin served each client once");
        while alice_read.len() < 5 || bob_read.len() < 5 {
            assert!(!expired(started), "a pong never arrived");
            server.step(&mut net, execute);
            let _ = read_available(&mut alice, &mut alice_read);
            let _ = read_available(&mut bob, &mut bob_read);
        }
        assert_eq!((alice_read.as_slice(), bob_read.as_slice()), (&b"pong\n"[..], &b"pong\n"[..]));

        // Drained: the goodbye arrives, then the close. Immediate: the close
        // arrives and nothing else.
        alice_read.clear();
        alice.write_all(b"quit\n").unwrap();
        bob_read.clear();
        bob.write_all(b"die\n").unwrap();
        let (mut alice_closed, mut bob_closed) = (false, false);
        while !(alice_closed && bob_closed) {
            assert!(!expired(started), "the closes never landed");
            server.step(&mut net, execute);
            alice_closed |= read_available(&mut alice, &mut alice_read);
            bob_closed |= read_available(&mut bob, &mut bob_read);
        }
        assert_eq!(alice_read, b"bye\n", "a drained close lets the reply out first");
        assert_eq!(bob_read, b"", "an immediate close sends nothing");
    }

    #[test]
    fn the_connection_cap_refuses_politely_and_the_input_cap_kills() {
        let started = std::time::Instant::now();
        let (mut net, mut server, addr) = raw_server(None);
        let mut alice = client(addr);
        let bob = client(addr);
        // Both connect before the third arrives, so the cap is real.
        while server.connections() < 2 {
            assert!(!expired(started), "the first two never connected");
            server.step(&mut net, execute);
        }
        let mut carol = client(addr);
        let mut carol_read = Vec::new();
        let mut carol_closed = false;
        while !carol_closed {
            assert!(!expired(started), "the third client was never refused");
            server.step(&mut net, execute);
            carol_closed |= read_available(&mut carol, &mut carol_read);
        }
        assert_eq!(carol_read, b"53300 too many connections\n");
        assert_eq!(server.connections(), 2, "the refused connection is gone");

        // A client that streams 5 KiB without a newline hits the input cap:
        // the fatal reply, then the close.
        alice.write_all(&vec![b'q'; 5 * 1024]).unwrap();
        let mut alice_read = Vec::new();
        let mut alice_closed = false;
        while !alice_closed {
            assert!(!expired(started), "the input cap never fired");
            server.step(&mut net, execute);
            alice_closed |= read_available(&mut alice, &mut alice_read);
        }
        assert_eq!(alice_read, b"54000 program limit exceeded\n");
        drop(bob);
    }

    #[test]
    fn the_output_cap_cuts_the_connection_immediately() {
        let started = std::time::Instant::now();
        let (mut net, mut server, addr) = raw_server(None);
        let mut alice = client(addr);

        // The megaflood reply exceeds MAX_OUTPUT, so the flush marks the
        // close Immediate before anything else: the connection is cut and the
        // tile forgets it.
        alice.write_all(b"megaflood\n").unwrap();
        while server.connections() > 0 || server.served.is_empty() {
            assert!(!expired(started), "the output cap never fired");
            server.step(&mut net, execute);
        }
        let mut sink = Vec::new();
        let mut closed = false;
        while !closed {
            assert!(!expired(started), "the client never saw the cut");
            closed = read_available(&mut alice, &mut sink);
        }
        assert!(sink.len() < 100 * 1024, "the reply never fully arrived");
    }

    #[test]
    fn the_hard_backlog_cap_disconnects_a_peer_that_stops_reading() {
        let started = std::time::Instant::now();
        let (mut net, mut server, addr) = raw_server(Some(8 * 1024));
        let mut alice = client(addr);

        // The flood reply is 64 KiB against an 8 KiB backlog cap and a client
        // that never reads: the stack disconnects the peer rather than queue
        // past the cap, and the tile sees the Disconnected like any other.
        alice.write_all(b"flood\n").unwrap();
        while server.connections() > 0 || server.served.is_empty() {
            assert!(!expired(started), "the backlog cap never fired");
            server.step(&mut net, execute);
        }
        let mut sink = Vec::new();
        let mut closed = false;
        while !closed {
            assert!(!expired(started), "the client never saw the disconnect");
            closed = read_available(&mut alice, &mut sink);
        }
        assert!(sink.len() < 64 * 1024, "the flood never fully arrived");
    }
}
