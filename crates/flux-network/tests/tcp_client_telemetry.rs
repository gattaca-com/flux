use std::{
    net::{Ipv4Addr, SocketAddr},
    thread,
    time::{Duration, Instant},
};

use flux_communication::cleanup_shmem;
use flux_network::tcp::{ClientEvent, ServerEvent, TcpClient, TcpServer, TcpTelemetry};
use flux_utils::directories::shmem_dir;
use mio::Token;

const APP_NAME: &str = "tcp-client-telemetry-reconnect-test";

fn unused_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn process_mapping_count() -> usize {
    std::fs::read_to_string("/proc/self/maps").unwrap().lines().count()
}

fn wait_for_connection(client: &mut TcpClient, server: &mut TcpServer) -> Token {
    let mut client_connected = false;
    let mut server_stream = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (!client_connected || server_stream.is_none()) {
        client.poll_with(|event| {
            if let ClientEvent::Connected { .. } = event {
                client_connected = true;
            }
        });
        server.poll_with(|event| {
            if let ServerEvent::Accept { stream, .. } = event {
                server_stream = Some(stream);
            }
        });
        thread::sleep(Duration::from_millis(1));
    }
    assert!(client_connected, "client did not connect");
    server_stream.expect("server did not accept connection")
}

#[test]
fn client_reuses_telemetry_mappings_across_reconnects() {
    let shmem = shmem_dir(APP_NAME);
    cleanup_shmem(&shmem);

    let addr = unused_addr();
    let mut server = TcpServer::default();
    server.listen_at(addr).expect("server failed to listen");
    let mut client = TcpClient::default()
        .with_reconnect_interval(flux_timing::Duration::from_millis(1))
        .with_telemetry(TcpTelemetry::Enabled { app_name: APP_NAME });
    let _token = client.connect(addr);

    let mut server_stream = wait_for_connection(&mut client, &mut server);
    let mappings_after_first_connect = process_mapping_count();

    for _ in 0..5 {
        server.disconnect(server_stream);

        let mut disconnected = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline && !disconnected {
            client.poll_with(|event| {
                if let ClientEvent::Disconnect { .. } = event {
                    disconnected = true;
                }
            });
            server.poll_with(|_| {});
            thread::sleep(Duration::from_millis(1));
        }
        assert!(disconnected, "client did not observe disconnect");

        client.force_reconnect();
        server_stream = wait_for_connection(&mut client, &mut server);
    }

    assert_eq!(process_mapping_count(), mappings_after_first_connect);
    cleanup_shmem(&shmem);
}
