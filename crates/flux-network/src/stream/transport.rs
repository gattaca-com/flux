use std::{
    io::{self, IoSlice, Read, Write},
    net::Shutdown,
    os::fd::{AsRawFd, RawFd},
    path::PathBuf,
};

use mio::{Interest, Registry, Token, event::Source, net};

use super::{
    Endpoint, Peer,
    tcp_stream::{set_keepalive, set_user_timeout},
};

/// A bound listening socket, one variant per transport.
pub(crate) enum ListenSocket {
    Tcp(net::TcpListener),
    Unix(UnixListenSocket),
}

impl ListenSocket {
    /// Binds `endpoint`.
    pub(crate) fn bind(endpoint: Endpoint) -> io::Result<Self> {
        match endpoint {
            Endpoint::Tcp(addr) => net::TcpListener::bind(addr).map(Self::Tcp),
            Endpoint::Unix(path) => UnixListenSocket::bind(path).map(Self::Unix),
        }
    }

    /// Accepts one pending connection, with the identity of its remote end.
    pub(crate) fn accept(&self) -> io::Result<(TransportStream, Peer)> {
        match self {
            Self::Tcp(listener) => listener
                .accept()
                .map(|(socket, addr)| (TransportStream::Tcp(socket), Peer::Tcp(addr))),
            // The accepted address of a Unix-domain client is unnamed, so it
            // carries no identity to report.
            Self::Unix(listener) => listener
                .socket
                .accept()
                .map(|(socket, _)| (TransportStream::Unix(socket), Peer::Unix)),
        }
    }
}

impl Source for ListenSocket {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        match self {
            Self::Tcp(listener) => listener.register(registry, token, interests),
            Self::Unix(listener) => listener.socket.register(registry, token, interests),
        }
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        match self {
            Self::Tcp(listener) => listener.reregister(registry, token, interests),
            Self::Unix(listener) => listener.socket.reregister(registry, token, interests),
        }
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        match self {
            Self::Tcp(listener) => listener.deregister(registry),
            Self::Unix(listener) => listener.socket.deregister(registry),
        }
    }
}

/// A Unix-domain listener together with the socket file it created.
///
/// Dropping it unlinks that file, so the path is free for the next bind.
pub(crate) struct UnixListenSocket {
    socket: net::UnixListener,
    path: PathBuf,
}

impl UnixListenSocket {
    /// Binds a listener at `path`.
    fn bind(path: PathBuf) -> io::Result<Self> {
        let socket = net::UnixListener::bind(&path)?;
        Ok(Self { socket, path })
    }
}

impl Drop for UnixListenSocket {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A connected or connecting stream, one variant per transport.
pub(crate) enum TransportStream {
    Tcp(net::TcpStream),
    Unix(net::UnixStream),
}

impl TransportStream {
    /// Starts a nonblocking connection to `endpoint`.
    ///
    /// The returned stream may still be connecting; [`Self::is_connected`]
    /// reports when the attempt has finished.
    pub(crate) fn connect(endpoint: &Endpoint) -> io::Result<Self> {
        match endpoint {
            Endpoint::Tcp(addr) => net::TcpStream::connect(*addr).map(Self::Tcp),
            Endpoint::Unix(path) => net::UnixStream::connect(path).map(Self::Unix),
        }
    }

    /// Whether a connection attempt has completed, erroring once it has
    /// failed.
    pub(crate) fn is_connected(&self) -> io::Result<bool> {
        if let Some(err) = self.take_error()? {
            return Err(err);
        }
        let connected = match self {
            Self::Tcp(socket) => socket.peer_addr().map(|_| ()),
            Self::Unix(socket) => socket.peer_addr().map(|_| ()),
        };
        match connected {
            Ok(()) => Ok(true),
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::NotConnected | io::ErrorKind::WouldBlock
                ) || matches!(err.raw_os_error(), Some(libc::EINPROGRESS | libc::EALREADY)) =>
            {
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    fn take_error(&self) -> io::Result<Option<io::Error>> {
        match self {
            Self::Tcp(socket) => socket.take_error(),
            Self::Unix(socket) => socket.take_error(),
        }
    }

    pub(crate) fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        match self {
            Self::Tcp(socket) => socket.shutdown(how),
            Self::Unix(socket) => socket.shutdown(how),
        }
    }

    /// Enables `TCP_NODELAY`, which Unix-domain sockets do not have.
    pub(crate) fn set_nodelay(&self) -> io::Result<()> {
        match self {
            Self::Tcp(socket) => socket.set_nodelay(true),
            Self::Unix(_) => Ok(()),
        }
    }

    /// Enables TCP keepalive, which Unix-domain sockets do not have.
    pub(crate) fn set_keepalive(&self) -> io::Result<()> {
        match self {
            Self::Tcp(socket) => set_keepalive(socket),
            Self::Unix(_) => Ok(()),
        }
    }

    /// Sets `TCP_USER_TIMEOUT`, which Unix-domain sockets do not have.
    pub(crate) fn set_user_timeout(&self, timeout_ms: u32) {
        if let Self::Tcp(socket) = self {
            set_user_timeout(socket, timeout_ms);
        }
    }
}

impl AsRawFd for TransportStream {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            Self::Tcp(socket) => socket.as_raw_fd(),
            Self::Unix(socket) => socket.as_raw_fd(),
        }
    }
}

impl Read for TransportStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(socket) => socket.read(buf),
            Self::Unix(socket) => socket.read(buf),
        }
    }
}

impl Write for TransportStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(socket) => socket.write(buf),
            Self::Unix(socket) => socket.write(buf),
        }
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        match self {
            Self::Tcp(socket) => socket.write_vectored(bufs),
            Self::Unix(socket) => socket.write_vectored(bufs),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Tcp(socket) => socket.flush(),
            Self::Unix(socket) => socket.flush(),
        }
    }
}

impl Source for TransportStream {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        match self {
            Self::Tcp(socket) => socket.register(registry, token, interests),
            Self::Unix(socket) => socket.register(registry, token, interests),
        }
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        match self {
            Self::Tcp(socket) => socket.reregister(registry, token, interests),
            Self::Unix(socket) => socket.reregister(registry, token, interests),
        }
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        match self {
            Self::Tcp(socket) => socket.deregister(registry),
            Self::Unix(socket) => socket.deregister(registry),
        }
    }
}
