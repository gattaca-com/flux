use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use flux_network::tcp::{PollEvent, SendBehavior, TcpConnector};

#[test]
fn tcp_raw_roundtrip() {
    // Bind to port 0 so the OS picks a free port (avoids TIME_WAIT conflicts).
    let probe =
        std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("probe");
    let bind_addr = probe.local_addr().unwrap();
    drop(probe);

    // Signal so the server doesn't exit (and close sockets) before the client
    // has read its response.
    let client_done = Arc::new(AtomicBool::new(false));
    let client_done2 = client_done.clone();

    let mut listener = TcpConnector::default();
    let _listening_token = listener.bind_raw(bind_addr).unwrap();

    let server = thread::spawn(move || {
        let mut accepted_stream = None;

        while accepted_stream.is_none() {
            listener.poll_with(|event| {
                if let PollEvent::Accept { stream, .. } = event {
                    accepted_stream = Some(stream);
                }
            });
            thread::sleep(Duration::from_micros(50));
        }

        let stream_token = accepted_stream.unwrap();

        // Wait for raw message from client
        let mut recv = None;
        loop {
            listener.poll_with(|event| {
                if let PollEvent::RawMessage { token, data } = event {
                    assert_eq!(token, stream_token);
                    recv = Some(data.to_vec());
                }
                // Ensure we never get a framed Message on a raw connection
                if let PollEvent::Message { .. } = event {
                    panic!("raw connection should not produce PollEvent::Message");
                }
            });
            if recv.is_some() {
                break;
            }
            thread::sleep(Duration::from_micros(50));
        }
        assert_eq!(recv.as_deref(), Some(b"hello from client".as_slice()));

        // Send response
        listener.write_or_enqueue_with(SendBehavior::Single(stream_token), |buf| {
            buf.extend_from_slice(b"hello from server");
        });
        // Flush
        listener.poll_with(|_| {});

        // Keep sockets alive until the client has received the response.
        while !client_done.load(Ordering::Relaxed) {
            listener.poll_with(|_| {});
            thread::sleep(Duration::from_micros(100));
        }
    });

    let client = thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        let mut conn = TcpConnector::default();
        let tok = conn.connect_raw(bind_addr).unwrap();

        // Send raw bytes
        conn.write_or_enqueue_with(SendBehavior::Single(tok), |buf| {
            buf.extend_from_slice(b"hello from client");
        });

        // Wait for server response
        let mut recv = None;
        loop {
            conn.poll_with(|event| {
                if let PollEvent::RawMessage { data, .. } = event {
                    recv = Some(data.to_vec());
                }
                if let PollEvent::Message { .. } = event {
                    panic!("raw connection should not produce PollEvent::Message");
                }
            });
            if recv.is_some() {
                break;
            }
            thread::sleep(Duration::from_micros(50));
        }
        assert_eq!(recv.as_deref(), Some(b"hello from server".as_slice()));

        // Signal the server we're done
        client_done2.store(true, Ordering::Relaxed);
    });

    server.join().unwrap();
    client.join().unwrap();
}
