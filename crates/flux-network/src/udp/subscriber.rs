use std::{
    io,
    net::{SocketAddr, UdpSocket},
};

use flux_communication::Timer;
use flux_timing::Nanos;
use rustc_hash::FxHashMap;

use super::wire::{
    DEFAULT_MAX_DATAGRAM_SIZE, Fragment, MAX_DATAGRAM_SIZE, SUBSCRIBE, UDP_HEADER_SIZE,
    fragment_count,
};
use crate::tcp::set_socket_buf_size;

/// mem-guard, prevent unbounded UDP msg partials that never arrive
const MAX_PARTIALS_PER_SOURCE: usize = 256;

const FRAGMENT_PAYLOAD_SIZE: usize = DEFAULT_MAX_DATAGRAM_SIZE - UDP_HEADER_SIZE;

/// Controls emission of network latency telemetry.
#[derive(Clone, Copy)]
pub enum UdpTelemetry {
    Disabled,
    Enabled { app_name: &'static str },
}

/// One publisher's receive state.
#[derive(Default)]
struct Source {
    session_id: u32,
    /// Oldest is the smallest key: `seq` only grows within a session.
    partials: FxHashMap<u64, Partial>,
    latency: Option<Timer>,
}

struct Partial {
    buf: Vec<u8>,
    received: Vec<bool>,
    remaining: usize,
}

pub struct UdpMessage<'a> {
    pub from: SocketAddr,
    pub session_id: u32,
    pub seq: u64,
    /// Publisher clock at framing.
    pub send_ns: Nanos,
    pub payload: &'a [u8],
}

pub struct UdpSubscriber {
    socket: UdpSocket,
    telemetry: UdpTelemetry,
    /// Held for timer labels: the bound port cannot change.
    local_port: u16,
    sources: FxHashMap<SocketAddr, Source>,
    recv_buf: Vec<u8>,
}

impl UdpSubscriber {
    pub fn bind(bind: SocketAddr, socket_buf_size: Option<usize>) -> io::Result<Self> {
        let socket = UdpSocket::bind(bind)?;
        socket.set_nonblocking(true)?;
        if let Some(size) = socket_buf_size {
            set_socket_buf_size(&socket, size);
        }
        Ok(Self {
            local_port: socket.local_addr().map_or(0, |addr| addr.port()),
            socket,
            telemetry: UdpTelemetry::Disabled,
            sources: FxHashMap::default(),
            recv_buf: vec![0u8; MAX_DATAGRAM_SIZE],
        })
    }

    pub fn with_telemetry(mut self, telemetry: UdpTelemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Sent from the receiving socket, so replies come back here.
    pub fn dial(&self, publisher: SocketAddr) -> io::Result<()> {
        self.socket.send_to(&SUBSCRIBE, publisher).map(|_| ())
    }

    pub fn poll_with<F>(&mut self, mut handler: F) -> usize
    where
        F: FnMut(UdpMessage<'_>),
    {
        let mut delivered = 0;
        loop {
            let (n, from) = match self.socket.recv_from(&mut self.recv_buf) {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                // ICMP surfaces here only on a connected socket; ours is not.
                Err(_) => break,
            };

            let Self { sources, recv_buf, telemetry, local_port, .. } = self;
            let source = sources.entry(from).or_insert_with(|| {
                let mut source = Source::default();
                if let UdpTelemetry::Enabled { app_name } = telemetry {
                    let label = format!("udp_latency_{local_port}-{from}");
                    source.latency = Some(Timer::new(app_name, label));
                }
                source
            });

            let Some(fragment) = Fragment::decode(&recv_buf[..n], FRAGMENT_PAYLOAD_SIZE) else {
                continue;
            };
            let header = fragment.header;
            // A new session is a restarted publisher: numbering starts over, so
            // anything half-assembled from the old one can never complete.
            if header.session_id != source.session_id {
                source.session_id = header.session_id;
                source.partials.clear();
            }

            let complete;
            let total = fragment_count(header.len as usize, FRAGMENT_PAYLOAD_SIZE);
            let payload = if total == 1 {
                fragment.payload
            } else {
                let Some(buf) = source.assemble(&fragment, total) else { continue };
                complete = buf;
                &complete[..]
            };
            delivered += 1;
            if let Some(latency) = &mut source.latency {
                latency.emit_latency_from_nanos(Nanos(header.send_ns), Nanos::now());
            }
            handler(UdpMessage {
                from,
                session_id: header.session_id,
                seq: header.seq,
                send_ns: Nanos(header.send_ns),
                payload,
            });
        }
        delivered
    }
}

impl Source {
    /// Returns the message on the fragment that completes it.
    fn assemble(&mut self, fragment: &Fragment<'_>, total: usize) -> Option<Vec<u8>> {
        let seq = fragment.header.seq;
        if self.partials.len() >= MAX_PARTIALS_PER_SOURCE && !self.partials.contains_key(&seq) {
            let oldest = *self.partials.keys().min().unwrap();
            self.partials.remove(&oldest);
        }
        let partial = self.partials.entry(seq).or_insert_with(|| Partial {
            buf: vec![0u8; fragment.header.len as usize],
            received: vec![false; total],
            remaining: total,
        });
        if partial.received[fragment.index] {
            return None; // duplicate fragment
        }
        partial.received[fragment.index] = true;
        partial.remaining -= 1;
        let offset = fragment.header.offset as usize;
        partial.buf[offset..offset + fragment.payload.len()].copy_from_slice(fragment.payload);

        if partial.remaining > 0 {
            return None;
        }
        self.partials.remove(&seq).map(|p| p.buf)
    }
}
