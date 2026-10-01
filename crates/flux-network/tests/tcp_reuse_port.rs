//! Listeners in separate networks share an address with `reuse_port`.

use std::{
    io::ErrorKind,
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    time::{Duration, Instant},
};

use flux_network::{Network, NetworkEvent, TcpGroupConfig};

fn unused_addr() -> SocketAddr {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap()
}

fn listen(addr: SocketAddr, reuse_port: bool) -> std::io::Result<Network> {
    let mut net = Network::default();
    let group = net.add_group(TcpGroupConfig { reuse_port, ..TcpGroupConfig::default() });
    net.listen(group, addr)?;
    Ok(net)
}

#[test]
fn reuse_port_spreads_connections_across_networks() {
    let addr = unused_addr();
    let mut networks = [listen(addr, true).unwrap(), listen(addr, true).unwrap()];
    let clients: Vec<_> = (0..64).map(|_| TcpStream::connect(addr).unwrap()).collect();
    let mut accepted = [0; 2];
    let end = Instant::now() + Duration::from_secs(10);
    while accepted.iter().sum::<usize>() < clients.len() {
        assert!(Instant::now() < end, "accepted {accepted:?} of {}", clients.len());
        for (net, count) in networks.iter_mut().zip(&mut accepted) {
            net.poll_with(|event| {
                if let NetworkEvent::Accepted { .. } = event {
                    *count += 1;
                }
            });
        }
    }
    assert!(accepted.iter().all(|&count| count > 0), "both listeners accept: {accepted:?}");
}

#[test]
fn without_reuse_port_a_second_listener_is_refused() {
    let addr = unused_addr();
    let _first = listen(addr, false).unwrap();
    assert_eq!(listen(addr, false).err().map(|e| e.kind()), Some(ErrorKind::AddrInUse));
}
