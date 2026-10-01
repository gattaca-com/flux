//! Sends still reach the right peer after connections leave from the middle.

use std::{
    collections::HashMap,
    io::Read,
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    time::{Duration, Instant},
};

use flux_network::{Framing, Network, NetworkEvent, ReplayPolicy, TcpGroupConfig, Token};

#[test]
fn sends_follow_connections_moved_by_removals() {
    let addr: SocketAddr =
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap().local_addr().unwrap();
    let mut net = Network::default();
    let group = net.add_group(TcpGroupConfig {
        framing: Framing::Raw,
        replay: ReplayPolicy::Drop,
        ..TcpGroupConfig::default()
    });
    net.listen(group, addr).unwrap();

    let mut clients: Vec<TcpStream> = (0..8).map(|_| TcpStream::connect(addr).unwrap()).collect();
    let mut tokens: HashMap<SocketAddr, Token> = HashMap::new();
    let end = Instant::now() + Duration::from_secs(10);
    while tokens.len() < clients.len() {
        assert!(Instant::now() < end, "accepted {} of {}", tokens.len(), clients.len());
        net.poll_with(|event| {
            if let NetworkEvent::Accepted { token, peer_addr, .. } = event {
                tokens.insert(peer_addr, token);
            }
        });
    }

    // Removing from the front, middle and back moves later connections into
    // the freed slots.
    for index in [7, 3, 0] {
        let client = clients.remove(index);
        assert!(net.disconnect(tokens[&client.local_addr().unwrap()]));
    }
    assert!(!net.disconnect(Token(usize::MAX)), "unknown tokens find nothing");

    for client in &clients {
        let token = tokens[&client.local_addr().unwrap()];
        assert!(net.send_with(token, |out| out.extend_from_slice(&token.0.to_le_bytes())));
    }
    for client in &mut clients {
        client.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut received = [0; 8];
        client.read_exact(&mut received).unwrap();
        let expected = tokens[&client.local_addr().unwrap()];
        assert_eq!(usize::from_le_bytes(received), expected.0, "each peer gets its own message");
    }
}
