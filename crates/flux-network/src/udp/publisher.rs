use std::{
    io,
    net::{IpAddr, SocketAddr, UdpSocket},
};

use flux_timing::Nanos;
use rustc_hash::FxHashMap;
use tracing::info;

use super::wire::{DEFAULT_MAX_DATAGRAM_SIZE, SUBSCRIBE, encode_fragments};
use crate::tcp::set_socket_buf_size;

const SUBSCRIPTION_TTL_NANOS: u64 = 5_000_000_000;
const CONTROL_BUF_SIZE: usize = 64;

pub struct UdpPublisher {
    socket: UdpSocket,
    session_id: u32,
    next_seq: u64,
    targets: FxHashMap<SocketAddr, u64>,
    buf: Vec<u8>,
}

impl UdpPublisher {
    pub fn bind(bind: SocketAddr, socket_buf_size: Option<usize>) -> io::Result<Self> {
        let socket = UdpSocket::bind(bind)?;
        socket.set_nonblocking(true)?;
        if let Some(size) = socket_buf_size {
            set_socket_buf_size(&socket, size);
        }
        Ok(Self {
            socket,
            // Random, changes per restart vs wrap
            session_id: (Nanos::now().0 as u32) ^ std::process::id(),
            next_seq: 0,
            targets: FxHashMap::default(),
            buf: Vec::with_capacity(DEFAULT_MAX_DATAGRAM_SIZE),
        })
    }

    /// Takes pending subscribes, drops lapsed leases, returns how many were
    /// added.
    pub fn poll_subscriptions<F>(&mut self, mut accept: F) -> usize
    where
        F: FnMut(IpAddr) -> bool,
    {
        let now = Nanos::now().0;
        let mut buf = [0u8; CONTROL_BUF_SIZE];
        let mut added = 0;
        loop {
            let (n, from) = match self.socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                // Nothing more is waiting: this socket only gets control.
                Err(_) => break,
            };
            if buf[..n] != SUBSCRIBE {
                continue;
            }
            if !accept(from.ip()) {
                continue;
            }
            if self.targets.insert(from, now + SUBSCRIPTION_TTL_NANOS).is_none() {
                info!(?from, "udp subscriber added");
                added += 1;
            }
        }

        self.targets.retain(|addr, expires| {
            let live = *expires > now;
            if !live {
                info!(?addr, "udp subscription expired");
            }
            live
        });
        added
    }

    pub fn publish(&mut self, message: &[u8]) -> Option<u64> {
        if self.targets.is_empty() {
            return None;
        }
        let seq: u64 = self.next_seq;
        self.next_seq += 1;

        let Self { socket, targets, buf, .. } = self;
        encode_fragments(
            DEFAULT_MAX_DATAGRAM_SIZE,
            self.session_id,
            seq,
            Nanos::now().0,
            message,
            buf,
            |datagram| {
                for addr in targets.keys() {
                    let _ = socket.send_to(datagram, addr);
                }
            },
        );
        Some(seq)
    }
}
