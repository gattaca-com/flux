//! Two-box single-connection benchmark: a UDP or TCP connection in a
//! `Network` group. Start the receiver first:
//!
//! ```text
//! remote_conn receiver <sender_ip> <port> <core> <secs>
//! remote_conn sender   <bind_ip> <port> <core> <secs>
//! ```
//!
//! Env, same on both ends:
//!
//! ```text
//! TRANSPORT=udp|tcp   MODE=grouped|packed
//! BATCH_N=<n>         messages per send call (default 100)
//! SIZE=<lo>-<hi>      message size range (default 512-2048)
//! PACE_MBPS=<n>       cap payload at n MB/s; unset floods
//! ```

mod common;

use std::{
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant},
};

use common::{Mode, Sink, Source, mode, pack};
use flux_network::{GroupConfig, Network, NetworkEvent, Token};

fn main() {
    common::init_tracing();
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("sender") => sender(&args),
        Some("receiver") => receiver(&args),
        _ => {
            eprintln!("usage: remote_conn sender|receiver ...");
            std::process::exit(2);
        }
    }
}

fn config() -> GroupConfig {
    if common::tcp() { common::tcp_config().into() } else { common::udp_config().into() }
}

struct Conn {
    net: Network,
    token: Token,
    packed: Vec<u8>,
}

impl Sink for Conn {
    fn send(&mut self, msgs: &[Vec<u8>]) -> usize {
        let sent = match mode() {
            Mode::Grouped => {
                self.net.send_many_with(self.token, msgs, |b, m| b.extend_from_slice(m))
            }
            Mode::Packed => {
                pack(msgs, &mut self.packed);
                self.net.send_with(self.token, |b| b.extend_from_slice(&self.packed))
            }
        };
        usize::from(sent)
    }

    fn poll(&mut self) {
        self.net.poll_with(|_| {});
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

fn sender(args: &[String]) {
    let bind: IpAddr = args[2].parse().expect("bind_ip");
    let port: u16 = args[3].parse().unwrap_or(common::PORT_FALLBACK);
    let core: usize = args[4].parse().expect("core");
    let secs: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(10);
    common::pin(core);
    common::spawn_echo(bind, port + 1);

    let mut net = Network::default();
    let group = net.add_group(config());
    net.listen(group, SocketAddr::new(bind, port)).expect("listen");
    eprintln!("sender ({}): on {bind}:{port}, waiting for the receiver", common::kind());
    let mut token = None;
    let deadline = Instant::now() + Duration::from_secs(40);
    while token.is_none() {
        assert!(Instant::now() < deadline, "no receiver connected");
        net.poll_with(|e| {
            if let NetworkEvent::Accepted { token: t, .. } = e {
                token = Some(t);
            }
        });
    }
    eprintln!("sender: connected, running {secs}s");
    let mut conn = Conn { net, token: token.unwrap(), packed: Vec::new() };
    common::run_sender(&mut conn, &mut common::messages(), 1, secs);
}

fn receiver(args: &[String]) {
    let sender_ip: IpAddr = args[2].parse().expect("sender_ip");
    let port: u16 = args[3].parse().unwrap_or(common::PORT_FALLBACK);
    let core: usize = args[4].parse().expect("core");
    let secs: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(12);
    common::pin(core);
    let theta = common::measure_offset(sender_ip, port + 1);

    let mut net = Network::default();
    let group = net.add_group(config());
    let _ = net.connect(group, SocketAddr::new(sender_ip, port));
    eprintln!("receiver core {core} ({}): connecting to {sender_ip}:{port}", common::kind());
    common::run_receiver(&mut net, core, secs, theta);
}
