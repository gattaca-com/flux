---
status: accepted
---

# Transports are a closed set: `Endpoint { Tcp, Unix }`

`StreamNetwork` listens and connects on an `Endpoint` enum — a TCP socket address or a
Unix-domain socket path — and reports accepted connections with a `Peer` enum (TCP address,
or anonymous for Unix-domain sockets), rather than being generic over a stream type. A tile
must be able to hold TCP and Unix-domain listeners in the same poll and the same group, which
a type parameter forbids; the set of transports is fixed and small, and the two mio stream
types differ only in construction, so an enum costs one match per operation and no
monomorphisation. TCP-only socket options (`TCP_NODELAY`, keepalive, `TCP_USER_TIMEOUT`) do
not exist for Unix-domain sockets; socket buffer sizes apply to both. Half-close
(`shutdown(Write)`) is required of both transports because the Lingering state depends on it.
Parsing user-facing address strings is the caller's job; the enum is the only form flux
accepts.

## Considered options

- **`StreamNetwork<S: Stream>`.** Rejected: one network could not mix transports, so a tile
  serving a TCP and a Unix-domain bind would need two polls.
- **Trait object per connection.** Rejected: a virtual call per read/write on the hot path
  for a set that will not grow.
