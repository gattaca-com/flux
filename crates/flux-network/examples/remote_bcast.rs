//! Two-box broadcast benchmark: UDP pub/sub or a TCP group. Start the
//! receivers first:
//!
//! ```text
//! remote_bcast receiver <sender_ip> <port> <core> <secs>
//! remote_bcast sender   <bind_ip> <port> <clients> <core> <secs>
//! ```
//!
//! Env, same on both ends:
//!
//! ```text
//! TRANSPORT=udp|tcp   MODE=grouped|packed
//! BATCH_N=<n>         messages per send call (default 100)
//! SIZE=<lo>-<hi>      message size range (default 512-2048)
//! PACE_MBPS=<n>       cap payload at n MB/s aggregate; unset floods
//! ```

mod common;

use std::{
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant},
};

use common::{Mode, Sink, Source, mode, pack};
use flux_network::{
    Group, Network, NetworkEvent, Publisher, PublisherEvent, Subscriber, SubscriberEvent,
};

fn main() {
    common::init_tracing();
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("sender") => sender(&args),
        Some("receiver") => receiver(&args),
        _ => {
            eprintln!("usage: remote_bcast sender|receiver ...");
            std::process::exit(2);
        }
    }
}

struct TcpSink {
    net: Network,
    group: Group,
    packed: Vec<u8>,
}

impl Sink for TcpSink {
    fn send(&mut self, msgs: &[Vec<u8>]) -> usize {
        match mode() {
            Mode::Grouped => {
                self.net.broadcast_many_with(self.group, msgs, |b, m| b.extend_from_slice(m))
            }
            Mode::Packed => {
                pack(msgs, &mut self.packed);
                self.net.broadcast_with(self.group, |b| b.extend_from_slice(&self.packed))
            }
        }
    }

    fn poll(&mut self) {
        self.net.poll_with(|_| {});
    }
}

struct UdpSink {
    publisher: Publisher,
    packed: Vec<u8>,
}

impl Sink for UdpSink {
    fn send(&mut self, msgs: &[Vec<u8>]) -> usize {
        match mode() {
            Mode::Grouped => {
                self.publisher.broadcast_many_with(msgs, |b, m| b.extend_from_slice(m))
            }
            Mode::Packed => {
                pack(msgs, &mut self.packed);
                self.publisher.broadcast_with(|b| b.extend_from_slice(&self.packed))
            }
        }
    }

    fn poll(&mut self) {
        self.publisher.poll_with(|_| {});
    }
}

fn sender(args: &[String]) {
    let bind: IpAddr = args[2].parse().expect("bind_ip");
    let port: u16 = args[3].parse().unwrap_or(common::PORT_FALLBACK);
    let clients: usize = args[4].parse().expect("clients");
    let core: usize = args[5].parse().expect("core");
    let secs: u64 = args.get(6).and_then(|s| s.parse().ok()).unwrap_or(10);
    common::pin(core);
    common::spawn_echo(bind, port + 1);
    let addr = SocketAddr::new(bind, port);
    eprintln!("sender ({}): on {addr}, waiting for {clients} receivers", common::kind());
    let deadline = Instant::now() + Duration::from_secs(40);
    let mut joined = 0;
    let mut msgs = common::messages();
    if common::tcp() {
        let mut net = Network::default();
        let group = net.add_group(common::tcp_config());
        net.listen(group, addr).expect("listen");
        while joined < clients {
            assert!(Instant::now() < deadline, "only {joined}/{clients} connected");
            net.poll_with(|e| joined += usize::from(matches!(e, NetworkEvent::Accepted { .. })));
        }
        eprintln!("sender: all {clients} connected, running {secs}s");
        let mut sink = TcpSink { net, group, packed: Vec::new() };
        common::run_sender(&mut sink, &mut msgs, clients, secs);
    } else {
        let mut publisher = Publisher::new(common::udp_config());
        publisher.listen(addr).expect("listen");
        while joined < clients {
            assert!(Instant::now() < deadline, "only {joined}/{clients} subscribed");
            publisher.poll_with(|e| {
                joined += usize::from(matches!(e, PublisherEvent::Subscribed { .. }));
            });
        }
        eprintln!("sender: all {clients} subscribed, running {secs}s");
        let mut sink = UdpSink { publisher, packed: Vec::new() };
        common::run_sender(&mut sink, &mut msgs, clients, secs);
    }
}

impl Source for Network {
    fn poll(&mut self, on_message: &mut dyn FnMut(&[u8])) {
        self.poll_with(|e| {
            if let NetworkEvent::Message { payload, .. } = e {
                on_message(payload);
            }
        });
    }
}

impl Source for Subscriber {
    fn poll(&mut self, on_message: &mut dyn FnMut(&[u8])) {
        self.poll_with(|e| {
            if let SubscriberEvent::Message { payload, .. } = e {
                on_message(payload);
            }
        });
    }
}

fn receiver(args: &[String]) {
    let sender_ip: IpAddr = args[2].parse().expect("sender_ip");
    let port: u16 = args[3].parse().unwrap_or(common::PORT_FALLBACK);
    let core: usize = args[4].parse().expect("core");
    let secs: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(12);
    common::pin(core);
    let theta = common::measure_offset(sender_ip, port + 1);
    let addr = SocketAddr::new(sender_ip, port);
    eprintln!("receiver core {core} ({}): connecting to {addr}", common::kind());
    if common::tcp() {
        let mut net = Network::default();
        let group = net.add_group(common::tcp_config());
        let _ = net.connect(group, addr);
        common::run_receiver(&mut net, core, secs, theta);
    } else {
        let mut subscriber = Subscriber::new(common::udp_config());
        subscriber.subscribe(addr).expect("subscribe");
        common::run_receiver(&mut subscriber, core, secs, theta);
    }
}
