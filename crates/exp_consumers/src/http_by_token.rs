//! The overseer's headless `POST /sql` server: every request is answered
//! strictly on a later iteration, by token, behind a generation guard.
//!
//! The event's [`Responder`](flux_network::http::Responder) is deliberately
//! dropped unanswered: the request is parsed into a pending entry and the SQL
//! "worker" — here, the next loop iteration — answers with
//! `HttpService::respond(token, ..)`. The generation guard is the original's
//! defence against token reuse: a response computed for a connection that has
//! since closed is refused by the tile before it reaches the wire.
//!
//! One simplification against the original: the 405/404/400 classifications
//! ride the same later-iteration queue as SQL results, where the original
//! answers them in the same iteration's drain. The by-token path and the
//! guard — the load-bearing parts — are identical.

use std::collections::VecDeque;

use flux_network::{
    Token,
    http::{HttpEvent, HttpService},
    stream::StreamNetwork,
};
use flux_timing::Duration;

/// A parsed request waiting for its worker, with the identity it must still
/// match at response time.
pub struct Pending {
    pub token: Token,
    pub generation: u64,
    pub status: u16,
    pub body: Vec<u8>,
}

/// The headless HTTP server around one [`HttpService`].
pub struct SqlServer {
    pub http: HttpService,
    generations: Vec<(Token, u64)>,
    next_generation: u64,
    pub pending: VecDeque<Pending>,
    pub refused_stale: usize,
}

impl SqlServer {
    pub fn new(http: HttpService) -> Self {
        Self {
            http,
            generations: Vec::new(),
            next_generation: 0,
            pending: VecDeque::new(),
            refused_stale: 0,
        }
    }

    /// One iteration's network pass and event pull: requests are classified
    /// and queued — never answered from inside the event.
    pub fn step(&mut self, net: &mut StreamNetwork) {
        let _ = net.drive(Some(Duration::ZERO), std::slice::from_mut(&mut self.http));
        while let Some(event) = self.http.next_event() {
            match event {
                HttpEvent::Accepted { token, .. } => {
                    self.next_generation += 1;
                    self.generations.push((token, self.next_generation));
                }
                HttpEvent::Request { token, request, responder } => {
                    // Dropped unanswered: the reply goes out by token later.
                    drop(responder);
                    let (status, body): (u16, Vec<u8>) = match (request.method, request.path) {
                        ("POST", "/sql") if request.body.starts_with(b"{") => {
                            (200, request.body.to_vec())
                        }
                        ("POST", "/sql") => (400, b"{\"error\":\"bad json\"}".to_vec()),
                        (_, "/sql") => (405, Vec::new()),
                        _ => (404, Vec::new()),
                    };
                    let generation = self
                        .generations
                        .iter()
                        .find(|(owner, _)| *owner == token)
                        .map_or(0, |(_, generation)| *generation);
                    self.pending.push_back(Pending { token, generation, status, body });
                }
                HttpEvent::Disconnected { token } => {
                    self.generations.retain(|(owner, _)| *owner != token);
                    self.pending.retain(|pending| pending.token != token);
                }
                HttpEvent::Connected { .. } |
                HttpEvent::Response { .. } |
                HttpEvent::RequestFailed { .. } => {
                    unreachable!("this service never dials out")
                }
            }
        }
    }

    /// Answers one pending request — the worker completing on a later
    /// iteration than the one that queued it.
    pub fn respond_one(&mut self) {
        if let Some(pending) = self.pending.pop_front() {
            let _ =
                self.respond_sql(pending.token, pending.generation, pending.status, &pending.body);
        }
    }

    /// The by-token response path, refused when the token's generation no
    /// longer matches — the connection the answer was computed for is gone.
    pub fn respond_sql(&mut self, token: Token, generation: u64, status: u16, body: &[u8]) -> bool {
        let current = self.generations.iter().find(|(owner, _)| *owner == token);
        if current.map(|(_, generation)| *generation) != Some(generation) {
            self.refused_stale += 1;
            return false;
        }
        self.http.respond(token, status, &[("content-type", "application/json")], body)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpStream,
    };

    use flux_network::{
        Token,
        http::{HttpConfig, HttpService},
        stream::StreamNetwork,
    };

    use super::SqlServer;
    use crate::harness::{bound_addr, ephemeral, expired, raw_group};

    /// A raw HTTP/1.1 client: writes bytes, reads one whole response.
    struct RawClient {
        stream: TcpStream,
        buf: Vec<u8>,
    }

    impl RawClient {
        fn connect(addr: std::net::SocketAddr) -> Self {
            let stream = TcpStream::connect(addr).unwrap();
            stream.set_read_timeout(Some(std::time::Duration::from_millis(10))).unwrap();
            stream.set_nodelay(true).unwrap();
            Self { stream, buf: Vec::new() }
        }

        fn send(&mut self, request: &str) {
            self.stream.write_all(request.as_bytes()).unwrap();
        }

        /// The status and body of the next complete response, if buffered.
        fn try_response(&mut self) -> Option<(u16, Vec<u8>)> {
            let mut chunk = [0u8; 4096];
            match self.stream.read(&mut chunk) {
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(err) => panic!("the client read failed: {err}"),
            }
            let head_end = self.buf.windows(4).position(|w| w == b"\r\n\r\n")? + 4;
            let head = std::str::from_utf8(&self.buf[..head_end]).unwrap().to_ascii_lowercase();
            let status: u16 = head[9..12].parse().unwrap();
            let length: usize = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .map_or(0, |value| value.trim().parse().unwrap());
            if self.buf.len() < head_end + length {
                return None;
            }
            let body = self.buf[head_end..head_end + length].to_vec();
            self.buf.drain(..head_end + length);
            Some((status, body))
        }
    }

    #[test]
    fn requests_are_answered_later_by_token_behind_a_generation_guard() {
        let started = std::time::Instant::now();
        let mut net = StreamNetwork::default();
        let mut http = HttpService::new(net.add_group(raw_group("sql")), HttpConfig::default());
        let addr = bound_addr(&http.listen(ephemeral()).unwrap());
        let mut server = SqlServer::new(http);

        // A well-formed POST /sql is parsed, queued, and answered on a later
        // iteration: respond_one runs at the top of the loop, so an answer
        // always postdates the iteration that queued its request.
        let mut client = RawClient::connect(addr);
        client.send(
            "POST /sql HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\n\
             content-length: 9\r\n\r\n{\"q\":\"1\"}",
        );
        let (status, body) = loop {
            assert!(!expired(started), "the SQL response never arrived");
            server.respond_one();
            server.step(&mut net);
            if let Some(response) = client.try_response() {
                break response;
            }
        };
        assert_eq!((status, body.as_slice()), (200, b"{\"q\":\"1\"}".as_slice()));

        // The wrong method on the right path is a 405, through the same
        // queue-then-answer path.
        client.send("GET /sql HTTP/1.1\r\nhost: x\r\n\r\n");
        let (status, _) = loop {
            assert!(!expired(started), "the 405 never arrived");
            server.respond_one();
            server.step(&mut net);
            if let Some(response) = client.try_response() {
                break response;
            }
        };
        assert_eq!(status, 405);

        // The generation guard: an answer carrying a stale generation is
        // refused by the tile before it reaches the service.
        let token = server.generations.first().expect("the client is known").0;
        let stale = server.generations.first().unwrap().1 + 1;
        assert!(!server.respond_sql(token, stale, 200, b"{}"), "a stale answer is refused");
        assert_eq!(server.refused_stale, 1);

        // And a token the server has forgotten refuses too.
        assert!(!server.respond_sql(Token(usize::MAX), 1, 200, b"{}"));
        assert_eq!(server.refused_stale, 2);
    }
}
