mod common;

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use common::{RawService, Record};
use flux_communication::cleanup_shmem;
use flux_network::{
    Token,
    stream::{ConnectionGroupConfig, Endpoint, StreamNetwork, TcpTelemetry},
};
use flux_utils::directories::shmem_dir;

const APP_NAME: &str = "tcp-network-telemetry-reconnect-test";

/// How long one iteration of a test loop is allowed to wait in the poll.
fn poll_slice() -> flux_timing::Duration {
    flux_timing::Duration::from_millis(1)
}

/// A loopback endpoint whose port the kernel picks when the listener binds,
/// so no address is handed out before something holds it.
fn ephemeral() -> Endpoint {
    Endpoint::Tcp((Ipv4Addr::LOCALHOST, 0).into())
}

/// The TCP address a listener bound, port included.
fn bound_addr(bound: Endpoint) -> SocketAddr {
    match bound {
        Endpoint::Tcp(addr) => addr,
        Endpoint::Unix(path) => panic!("a TCP listener bound {}", path.display()),
    }
}

fn process_mapping_count() -> usize {
    std::fs::read_to_string("/proc/self/maps").unwrap().lines().count()
}

/// Drives both services until the client has connected and the server has
/// accepted, reporting the token the server accepted on.
fn wait_for_connection(
    network: &mut StreamNetwork,
    server: &mut RawService,
    client: &mut RawService,
    client_token: Token,
) -> Token {
    let mut connected = false;
    let mut server_token = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (!connected || server_token.is_none()) {
        network.drive(Some(poll_slice()), &mut [&mut *server, &mut *client]);
        for record in server.take_records() {
            if let Record::Accepted { token, .. } = record {
                server_token = Some(token);
            }
        }
        for record in client.take_records() {
            if let Record::Connected { token, .. } = record {
                assert_eq!(token, client_token);
                connected = true;
            }
        }
    }
    assert!(connected, "client did not connect");
    server_token.expect("server did not accept connection")
}

#[test]
fn outbound_endpoint_reuses_telemetry_mappings_across_reconnects() {
    let shmem = shmem_dir(APP_NAME);
    cleanup_shmem(&shmem);

    let mut network = StreamNetwork::default();
    let mut server = RawService::new(
        network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() }),
    );
    let mut client = RawService::new(network.add_group(ConnectionGroupConfig {
        name: "client",
        reconnect_interval: flux_timing::Duration::from_millis(1),
        telemetry: TcpTelemetry::Enabled { app_name: APP_NAME },
        ..Default::default()
    }));
    let addr = bound_addr(server.listen(ephemeral()).unwrap());
    let client_token = client.connect(Endpoint::Tcp(addr));

    let mut server_token =
        wait_for_connection(&mut network, &mut server, &mut client, client_token);
    let mappings_after_first_connect = process_mapping_count();

    for _ in 0..5 {
        assert!(server.disconnect(server_token));

        let mut disconnected = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline && !disconnected {
            network.drive(Some(poll_slice()), &mut [&mut server, &mut client]);
            for record in client.take_records() {
                if let Record::Disconnected { token, .. } = record {
                    assert_eq!(token, client_token);
                    disconnected = true;
                }
            }
        }
        assert!(disconnected, "client did not observe disconnect");

        server_token = wait_for_connection(&mut network, &mut server, &mut client, client_token);
    }

    assert_eq!(process_mapping_count(), mappings_after_first_connect);
    cleanup_shmem(&shmem);
}
