use std::{fmt, net::SocketAddr, path::PathBuf};

/// The address a listener binds or an outbound connection targets.
///
/// The set of transports is closed: a caller that accepts addresses as text
/// parses them itself and builds the variant it wants.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Endpoint {
    /// A TCP socket address.
    Tcp(SocketAddr),
    /// A Unix-domain socket path.
    Unix(PathBuf),
}

impl Endpoint {
    /// The identity every connection to this endpoint reports.
    pub(crate) fn peer(&self) -> Peer {
        match self {
            Self::Tcp(addr) => Peer::Tcp(*addr),
            Self::Unix(_) => Peer::Unix,
        }
    }
}

impl From<SocketAddr> for Endpoint {
    fn from(addr: SocketAddr) -> Self {
        Self::Tcp(addr)
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(addr) => write!(f, "{addr}"),
            Self::Unix(path) => write!(f, "{}", path.display()),
        }
    }
}

/// The identity of the remote end of a connection.
///
/// A Unix-domain client is anonymous: it binds no path of its own, so the
/// kernel reports an unnamed address for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Peer {
    /// The remote TCP socket address.
    Tcp(SocketAddr),
    /// The unnamed remote end of a Unix-domain connection.
    Unix,
}

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(addr) => write!(f, "{addr}"),
            Self::Unix => f.write_str("unix"),
        }
    }
}
