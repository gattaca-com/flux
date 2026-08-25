//! ADR 0003: what binding a Unix-domain listener does to an occupied path,
//! and what closing one leaves behind.

use std::{
    io::{self, Read, Write},
    os::{
        fd::AsRawFd,
        unix::{
            fs::FileTypeExt as _,
            net::{UnixDatagram, UnixListener, UnixStream},
        },
    },
    path::Path,
    thread,
    time::{Duration, Instant},
};

use flux_network::stream::{ConnectionGroupConfig, Endpoint, StreamEvent, StreamNetwork};

/// A network with one group, ready to listen.
fn network() -> (StreamNetwork, flux_network::stream::ConnectionGroup) {
    let mut network = StreamNetwork::default();
    let group = network.add_group(ConnectionGroupConfig { name: "server", ..Default::default() });
    (network, group)
}

fn accepts_a_connection(network: &mut StreamNetwork, path: &Path) -> bool {
    let _client = UnixStream::connect(path).unwrap();
    let mut accepted = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !accepted {
        network.poll_with(|event| accepted |= matches!(event, StreamEvent::Accepted { .. }));
        thread::sleep(Duration::from_millis(1));
    }
    accepted
}

#[test]
fn stale_socket_file_is_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");

    // Closing a socket does not unlink its file, so a process that exits
    // without cleaning up leaves exactly this behind.
    drop(UnixListener::bind(&path).unwrap());
    assert!(path.exists());

    let (mut network, group) = network();
    network.listen(group, Endpoint::Unix(path.clone())).unwrap();
    assert!(accepts_a_connection(&mut network, &path));
}

#[test]
fn live_socket_file_is_left_to_its_owner() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let live = UnixListener::bind(&path).unwrap();

    let (mut network, group) = network();
    let err = network.listen(group, Endpoint::Unix(path.clone())).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AddrInUse);
    assert!(err.to_string().contains(path.to_str().unwrap()), "{err}");
    assert!(path.exists());

    let mut client = UnixStream::connect(&path).unwrap();
    client.write_all(b"ping").unwrap();
    // The refused bind's probe connection is queued ahead of this one and
    // reads as an immediate end of stream.
    let mut delivered = Vec::new();
    for _ in 0..2 {
        let (mut stream, _) = live.accept().unwrap();
        let mut buf = [0; 4];
        let read = stream.read(&mut buf).unwrap();
        delivered.push(buf[..read].to_vec());
    }
    assert!(delivered.contains(&b"ping".to_vec()), "{delivered:?}");
}

#[test]
fn regular_file_at_the_path_is_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    std::fs::write(&path, b"not a socket").unwrap();

    let (mut network, group) = network();
    let err = network.listen(group, Endpoint::Unix(path.clone())).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    assert!(err.to_string().contains(path.to_str().unwrap()), "{err}");
    assert_eq!(std::fs::read(&path).unwrap(), b"not a socket");
}

#[test]
fn symlink_to_a_socket_is_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("real");
    let link = dir.path().join("link");
    let _live = UnixListener::bind(&target).unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();

    let (mut network, group) = network();
    // The link resolves to a live socket, but the bind must not follow it.
    let err = network.listen(group, Endpoint::Unix(link.clone())).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    assert!(err.to_string().contains(link.to_str().unwrap()), "{err}");
    assert!(std::fs::symlink_metadata(&link).unwrap().file_type().is_symlink());
    assert_eq!(std::fs::read_link(&link).unwrap(), target);
    assert!(target.exists());
}

#[test]
fn closing_the_listener_unlinks_its_socket_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");

    let (mut network, group) = network();
    network.listen(group, Endpoint::Unix(path.clone())).unwrap();
    assert!(path.exists());

    drop(network);
    assert!(!path.exists());
}

#[test]
fn saturated_live_socket_is_still_left_to_its_owner() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let live = UnixListener::bind(&path).unwrap();
    // An owner that has stopped accepting is the case ADR 0003 is about, and
    // the kernel neither completes nor refuses a connection to it.
    assert_eq!(unsafe { libc::listen(live.as_raw_fd(), 1) }, 0);
    let mut queued = Vec::new();
    loop {
        match mio::net::UnixStream::connect(&path) {
            Ok(stream) => queued.push(stream),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => panic!("unexpected connect error: {err}"),
        }
        assert!(queued.len() < 64, "the accept queue never filled");
    }

    let (mut network, group) = network();
    let started = Instant::now();
    let err = network.listen(group, Endpoint::Unix(path.clone())).unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(1), "the probe waited for the owner");
    assert_eq!(err.kind(), io::ErrorKind::AddrInUse);
    assert!(err.to_string().contains(path.to_str().unwrap()), "{err}");
    assert!(path.exists());

    // The owner is unharmed: drained, it serves again.
    drop(queued);
    live.set_nonblocking(true).unwrap();
    while live.accept().is_ok() {}
    live.set_nonblocking(false).unwrap();
    let mut client = UnixStream::connect(&path).unwrap();
    client.write_all(b"ping").unwrap();
    let (mut stream, _) = live.accept().unwrap();
    let mut buf = [0; 4];
    stream.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"ping");
}

#[test]
fn non_refused_probe_error_leaves_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    // A datagram socket passes the `lstat`, but a stream connect to it fails
    // with something other than a refusal, which says nothing about whether
    // the file is stale.
    let _datagram = UnixDatagram::bind(&path).unwrap();

    let (mut network, group) = network();
    let err = network.listen(group, Endpoint::Unix(path.clone())).unwrap_err();
    assert_ne!(err.kind(), io::ErrorKind::AddrInUse);
    assert!(err.to_string().contains(path.to_str().unwrap()), "{err}");
    assert!(std::fs::symlink_metadata(&path).unwrap().file_type().is_socket());
}

#[test]
fn directory_at_the_path_is_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("keep"), b"contents").unwrap();

    let (mut network, group) = network();
    let err = network.listen(group, Endpoint::Unix(path.clone())).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    assert!(err.to_string().contains(path.to_str().unwrap()), "{err}");
    assert!(path.is_dir());
    assert_eq!(std::fs::read(path.join("keep")).unwrap(), b"contents");
}
