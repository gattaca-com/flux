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
use flux_network::{Group, GroupConfig, Network, NetworkEvent};

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

fn sender_group() -> GroupConfig {
    if common::tcp() {
        common::tcp_config().into()
    } else {
        GroupConfig::UdpPublisher(common::udp_config())
    }
}

fn receiver_group() -> GroupConfig {
    if common::tcp() {
        common::tcp_config().into()
    } else {
        GroupConfig::UdpSubscriber(common::udp_config())
    }
}

struct NetSink {
    net: Network,
    group: Group,
    packed: Vec<u8>,
}

impl Sink for NetSink {
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
    let mut net = Network::default();
    let group = net.add_group(sender_group());
    net.listen(group, addr).expect("listen");
    while joined < clients {
        assert!(Instant::now() < deadline, "only {joined}/{clients} connected");
        net.poll_with(|e| joined += usize::from(matches!(e, NetworkEvent::Accepted { .. })));
    }
    eprintln!("sender: all {clients} connected, running {secs}s");
    let mut sink = NetSink { net, group, packed: Vec::new() };
    common::run_sender(&mut sink, &mut msgs, clients, secs);
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

fn receiver(args: &[String]) {
    let sender_ip: IpAddr = args[2].parse().expect("sender_ip");
    let port: u16 = args[3].parse().unwrap_or(common::PORT_FALLBACK);
    let core: usize = args[4].parse().expect("core");
    let secs: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(12);
    common::pin(core);
    let theta = common::measure_offset(sender_ip, port + 1);
    let addr = SocketAddr::new(sender_ip, port);
    eprintln!("receiver core {core} ({}): connecting to {addr}", common::kind());
    let mut net = Network::default();
    let group = net.add_group(receiver_group());
    let _ = net.connect(group, addr);
    common::run_receiver(&mut net, core, secs, theta);
}
