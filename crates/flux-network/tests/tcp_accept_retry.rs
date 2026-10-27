//! Its own test binary: it lowers the process's descriptor limit.

use std::{
    net::{Ipv4Addr, TcpStream},
    thread,
    time::{Duration, Instant},
};

use flux_network::{Framing, Network, NetworkEvent, ReplayPolicy, TcpGroupConfig};

#[test]
fn accepts_resume_after_descriptor_exhaustion_without_a_new_connection() {
    let mut network = Network::default();
    let group = network.add_group(TcpGroupConfig {
        framing: Framing::Raw,
        replay: ReplayPolicy::Drop,
        ..TcpGroupConfig::default()
    });
    let listener = network.listen(group, (Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let addr = network.listener_addr(listener).unwrap();

    // Use up the descriptor table once the clients are connected.
    let mut fillers = Vec::new();
    let clients: Vec<_> = (0..4).map(|_| TcpStream::connect(addr).unwrap()).collect();
    while let Ok(file) = std::fs::File::open("/dev/null") {
        fillers.push(file);
    }
    let mut accepted = 0;
    let deadline = Instant::now() + Duration::from_millis(300);
    while Instant::now() < deadline {
        network.poll_with(|event| {
            accepted += usize::from(matches!(event, NetworkEvent::Accepted { .. }));
        });
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(accepted, 0, "nothing can be accepted without descriptors");

    // Free descriptors; the queued clients are accepted with no new arrival.
    drop(fillers);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && accepted < clients.len() {
        network.poll_with(|event| {
            accepted += usize::from(matches!(event, NetworkEvent::Accepted { .. }));
        });
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(accepted, clients.len());
}
