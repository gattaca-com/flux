use std::{
    net::{Ipv4Addr, SocketAddr},
    thread,
    time::{Duration, Instant},
};

use flux_communication::cleanup_shmem;
use flux_network::tcp::{TcpEvent, TcpGroup, TcpGroupConfig, TcpNetwork, TcpTelemetry};
use flux_utils::directories::shmem_dir;
use mio::Token;

const APP_NAME: &str = "tcp-network-telemetry-reconnect-test";

fn unused_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn process_mapping_count() -> usize {
    std::fs::read_to_string("/proc/self/maps").unwrap().lines().count()
}

fn wait_for_connection(
    network: &mut TcpNetwork,
    server_group: TcpGroup,
    client_group: TcpGroup,
    client_token: Token,
) -> Token {
    let mut connected = false;
    let mut server_token = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (!connected || server_token.is_none()) {
        network.poll_with(|event| match event {
            TcpEvent::Accepted { group, token, .. } if group == server_group => {
                server_token = Some(token);
            }
            TcpEvent::Connected { group, token, .. } if group == client_group => {
                assert_eq!(token, client_token);
                connected = true;
            }
            _ => {}
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(connected, "client did not connect");
    server_token.expect("server did not accept connection")
}

#[test]
fn outbound_endpoint_reuses_telemetry_mappings_across_reconnects() {
    let shmem = shmem_dir(APP_NAME);
    cleanup_shmem(&shmem);

    let addr = unused_addr();
    let mut network = TcpNetwork::default();
    let server_group = network.add_group(TcpGroupConfig { name: "server", ..Default::default() });
    let client_group = network.add_group(TcpGroupConfig {
        name: "client",
        reconnect_interval: flux_timing::Duration::from_millis(1),
        telemetry: TcpTelemetry::Enabled { app_name: APP_NAME },
        ..Default::default()
    });
    network.listen(server_group, addr).unwrap();
    let client_token = network.connect(client_group, addr);

    let mut server_token =
        wait_for_connection(&mut network, server_group, client_group, client_token);
    let mappings_after_first_connect = process_mapping_count();

    for _ in 0..5 {
        assert!(network.disconnect(server_token));

        let mut disconnected = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline && !disconnected {
            network.poll_with(|event| {
                if let TcpEvent::Disconnected { group, token, .. } = event &&
                    group == client_group
                {
                    assert_eq!(token, client_token);
                    disconnected = true;
                }
            });
            thread::sleep(Duration::from_millis(1));
        }
        assert!(disconnected, "client did not observe disconnect");

        server_token = wait_for_connection(&mut network, server_group, client_group, client_token);
    }

    assert_eq!(process_mapping_count(), mappings_after_first_connect);
    cleanup_shmem(&shmem);
}
