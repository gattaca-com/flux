//! The rpc simulation client pool: bounded in-flight requests over persistent
//! endpoints that are never removed.
//!
//! `fetch_all` is one blocking round trip with a deadline, exactly as the
//! original: fill the pool to the concurrency bound with `connect`, put a
//! request on every idle connection, drive with the 100 µs pacing the sleep
//! used to provide, and collect. A body whose connection drops resolves to
//! an error, never blocking the rest; only a refused `request` send retries
//! a body, on the next idle token. Dead tokens are dropped from the pool's
//! sets but never `remove`d from the service, so the stack keeps
//! reconnecting them; a later `Connected` for a token the pool forgot is
//! re-adopted into the idle set — the pool self-heals through the transport's
//! own persistence.

use flux_network::{
    Token,
    http::{HttpEvent, HttpService},
    stream::StreamNetwork,
};
use flux_timing::Duration;

/// The pool around one client-mode [`HttpService`].
pub struct ClientPool {
    pub http: HttpService,
    idle: Vec<Token>,
    connecting: Vec<Token>,
    in_flight: Vec<(Token, usize)>,
}

impl ClientPool {
    pub fn new(http: HttpService) -> Self {
        Self { http, idle: Vec::new(), connecting: Vec::new(), in_flight: Vec::new() }
    }

    fn forget(&mut self, token: Token) -> Option<usize> {
        self.idle.retain(|idle| *idle != token);
        self.connecting.retain(|connecting| *connecting != token);
        let index = self.in_flight.iter().position(|(owner, _)| *owner == token)?;
        Some(self.in_flight.swap_remove(index).1)
    }

    /// Sends every body and returns each answer, `Err` for the ones the
    /// deadline caught unresolved.
    pub fn fetch_all(
        &mut self,
        net: &mut StreamNetwork,
        addr: std::net::SocketAddr,
        bodies: &[&[u8]],
        bound: usize,
        deadline: std::time::Instant,
    ) -> Vec<Result<Vec<u8>, &'static str>> {
        let mut results: Vec<Option<Result<Vec<u8>, &'static str>>> = vec![None; bodies.len()];
        let mut queue: Vec<usize> = (0..bodies.len()).collect();

        while results.iter().any(Option::is_none) {
            if std::time::Instant::now() > deadline {
                for result in &mut results {
                    result.get_or_insert(Err("deadline"));
                }
                break;
            }

            // Fill to the bound, counting every connection whatever its
            // state, capped by what is left to do.
            let outstanding = queue.len() + self.in_flight.len();
            while self.idle.len() + self.connecting.len() + self.in_flight.len() <
                bound.min(outstanding)
            {
                let token = self.http.connect(flux_network::stream::Endpoint::Tcp(addr));
                self.connecting.push(token);
            }

            // A request on every idle connection; a refused send is a dead
            // token, dropped — never removed — and the same body tries the
            // next idle token.
            'assign: while !self.idle.is_empty() {
                let Some(index) = queue.pop() else { break };
                loop {
                    let Some(token) = self.idle.pop() else {
                        queue.push(index);
                        break 'assign;
                    };
                    let sent = self.http.request(
                        token,
                        "POST",
                        "/",
                        &[("content-type", "application/json")],
                        bodies[index],
                    );
                    if sent {
                        self.in_flight.push((token, index));
                        break;
                    }
                    self.forget(token);
                }
            }

            // The 100 µs idle pacing the original's sleep provided.
            let _ =
                net.drive(Some(Duration::from_micros(100)), std::slice::from_mut(&mut self.http));
            while let Some(event) = self.http.next_event() {
                match event {
                    // Re-adoption: a Connected for any token the pool is not
                    // already tracking joins the idle set, forgotten tokens
                    // included.
                    HttpEvent::Connected { token } => {
                        self.connecting.retain(|connecting| *connecting != token);
                        if !self.idle.contains(&token) &&
                            !self.in_flight.iter().any(|(owner, _)| *owner == token)
                        {
                            self.idle.push(token);
                        }
                    }
                    HttpEvent::Response { token, response } => {
                        // An answer for a request the pool no longer tracks
                        // is ignored, as the original ignores it.
                        let Some(index) =
                            self.in_flight.iter().position(|(owner, _)| *owner == token)
                        else {
                            continue;
                        };
                        let (_, body_index) = self.in_flight.swap_remove(index);
                        results[body_index] = Some(if (200..300).contains(&response.status) {
                            Ok(response.body.to_vec())
                        } else {
                            Err("http error")
                        });
                        self.idle.push(token);
                    }
                    // A body whose connection drops resolves to an error and
                    // never blocks the rest.
                    HttpEvent::RequestFailed { token, .. } | HttpEvent::Disconnected { token } => {
                        if let Some(body_index) = self.forget(token) {
                            results[body_index] = Some(Err("connection closed"));
                        }
                    }
                    HttpEvent::Accepted { .. } | HttpEvent::Request { .. } => {
                        unreachable!("this service never listens")
                    }
                }
            }
        }
        results.into_iter().map(|result| result.expect("every body resolved")).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    };

    use flux_network::{
        Token,
        http::{HttpConfig, HttpEvent, HttpService},
        stream::StreamNetwork,
    };
    use flux_timing::Duration;

    use super::ClientPool;
    use crate::harness::{bound_addr, ephemeral, raw_group};

    /// What the test thread can ask the server thread to do.
    enum Command {
        DropAllClients,
        Stop,
    }

    /// The peer: an inline echo server on its own thread, counting distinct
    /// accepts, dropping clients on command.
    fn spawn_echo_server(
        accepted: Arc<AtomicUsize>,
        stopped: Arc<AtomicBool>,
        commands: mpsc::Receiver<Command>,
    ) -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
        let mut net = StreamNetwork::default();
        let mut http = HttpService::new(net.add_group(raw_group("echo")), HttpConfig::default());
        let addr = bound_addr(&http.listen(ephemeral()).unwrap());
        let handle = std::thread::spawn(move || {
            let mut clients: Vec<Token> = Vec::new();
            while !stopped.load(Ordering::Relaxed) {
                match commands.try_recv() {
                    Ok(Command::DropAllClients) => {
                        for token in std::mem::take(&mut clients) {
                            let _ = http.disconnect(token);
                        }
                    }
                    Ok(Command::Stop) => break,
                    Err(_) => {}
                }
                let _ = net.drive(Some(Duration::from_millis(1)), std::slice::from_mut(&mut http));
                while let Some(event) = http.next_event() {
                    match event {
                        HttpEvent::Accepted { token, .. } => {
                            accepted.fetch_add(1, Ordering::Relaxed);
                            clients.push(token);
                        }
                        HttpEvent::Request { request, responder, .. } => {
                            let _ = responder.respond(
                                200,
                                &[("content-type", "application/json")],
                                request.body,
                            );
                        }
                        HttpEvent::Disconnected { token } => {
                            clients.retain(|client| *client != token);
                        }
                        _ => {}
                    }
                }
            }
        });
        (addr, handle)
    }

    #[test]
    fn a_bounded_pool_heals_through_reconnect_without_remove() {
        let accepted = Arc::new(AtomicUsize::new(0));
        let stopped = Arc::new(AtomicBool::new(false));
        let (commands, receiver) = mpsc::channel();
        let (addr, server) = spawn_echo_server(accepted.clone(), stopped.clone(), receiver);

        let mut net = StreamNetwork::default();
        let group = net.add_group(raw_group("pool"));
        let mut pool =
            ClientPool::new(HttpService::new(group, HttpConfig::default().without_idle_timeout()));

        // Ten bodies through at most three connections, inside the
        // original's 3 s deadline.
        let bodies: Vec<Vec<u8>> = (0..10).map(|i| format!("{{\"i\":{i}}}").into_bytes()).collect();
        let borrowed: Vec<&[u8]> = bodies.iter().map(Vec::as_slice).collect();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        let results = pool.fetch_all(&mut net, addr, &borrowed, 3, deadline);
        for (i, result) in results.iter().enumerate() {
            assert_eq!(
                result.as_deref().expect("every body answered"),
                format!("{{\"i\":{i}}}").as_bytes()
            );
        }
        let first_round = accepted.load(Ordering::Relaxed);
        assert!(first_round <= 3, "the bound held: {first_round} accepts");

        // The server drops every connection; the next round succeeds through
        // the endpoints' own reconnect — the pool never removed a token, and
        // a Connected for a forgotten token is re-adopted into idle.
        commands.send(Command::DropAllClients).unwrap();
        // A body in flight when its connection dies resolves to an error, so
        // the caller retries failed bodies — the healing the test pins is the
        // POOL's: connections come back without a remove, and within the
        // overall deadline every body eventually lands.
        let overall = std::time::Instant::now() + std::time::Duration::from_secs(3);
        let mut unresolved: Vec<&[u8]> = borrowed[..4].to_vec();
        while !unresolved.is_empty() {
            assert!(std::time::Instant::now() < overall, "the pool never healed");
            let slice = std::time::Instant::now() + std::time::Duration::from_millis(250);
            let results = pool.fetch_all(&mut net, addr, &unresolved, 3, slice);
            unresolved = unresolved
                .iter()
                .zip(&results)
                .filter(|(_, result)| result.is_err())
                .map(|(body, _)| *body)
                .collect();
        }
        assert!(accepted.load(Ordering::Relaxed) > first_round, "the healing was a real reconnect");

        stopped.store(true, Ordering::Relaxed);
        let _ = commands.send(Command::Stop);
        server.join().unwrap();
    }
}
