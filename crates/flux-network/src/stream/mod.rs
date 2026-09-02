//! Poll-driven byte-stream networking: TCP and Unix-domain sockets in one
//! [`StreamNetwork`], with protocol layers as services inside it.
//!
//! [`StreamService`] is the service for a group whose messages the tile
//! handles itself; [`crate::http::HttpService`] speaks HTTP over one.
//!
//! # Poll ownership
//! A tile hosting sockets owns exactly one poll, so which side holds it is a
//! construction-time mode of the network.
//!
//! **Owned** — [`StreamNetwork::default`] — creates a poll of its own and
//! drives it: one call per iteration, [`StreamNetwork::drive`], folds every
//! deadline, polls once, routes each event to the service owning its group
//! and ticks each service once. [`StreamNetwork::waker`] hands out a
//! [`mio::Waker`] for that poll, on a reserved token whose wakes the loop
//! swallows.
//!
//! **External** — [`StreamNetwork::with_registry`] — is built over a registry
//! cloned from the caller's poll and a token base, and never polls. The
//! caller polls, and makes the three calls a caller-held poll requires:
//! [`StreamNetwork::next_deadline`] to fold into its own timeout,
//! [`StreamNetwork::handle_event`] per readiness event, and
//! [`StreamNetwork::tick`] once per iteration. Sources of the caller's own
//! live in the same poll on tokens below the base — its waker included — and
//! `handle_event` hands their events back untouched. Deadlines are folded as
//! [`flux_timing::Instant`]s, which the caller turns into the timeout of its
//! poll.
//!
//! Each mode has exactly one way of being driven — an Owned network by
//! [`StreamNetwork::drive`], an External one by the three calls — and asking a
//! network for the other's panics.
//!
//! Both modes run one tick per iteration, after readiness: a service runs its
//! group's due transport work at the start of its tick and routes what that
//! produces before its own timers, so protocol state never lags transport
//! state by an iteration. Delivery order across different connections may
//! differ between the modes and is not a correctness guarantee.
//!
//! ```no_run
//! use flux_network::{
//!     Token, mio,
//!     http::{HttpConfig, HttpEvent, HttpService},
//!     stream::{Endpoint, Framing, ConnectionGroupConfig, StreamNetwork},
//! };
//! use flux_timing::Instant;
//!
//! // The tile's own poll, which every source it hosts shares.
//! let mut poll = mio::Poll::new()?;
//! let mut net = StreamNetwork::with_registry(poll.registry().try_clone()?, Token(1024));
//! // The tile registers this waker with flux's park Signal; its token is the
//! // caller's, below the base, so the network never takes it for its own.
//! let _waker = mio::Waker::new(poll.registry(), Token(0))?;
//!
//! let group = net.add_group(ConnectionGroupConfig {
//!     name: "api",
//!     framing: Framing::Raw,
//!     ..ConnectionGroupConfig::default()
//! });
//! let mut api = HttpService::new(group, HttpConfig::default());
//! api.listen(Endpoint::Unix("/run/flux/api.sock".into()))?;
//!
//! let mut events = mio::Events::with_capacity(128);
//! loop {
//!     let mut services = [&mut api];
//!     let timeout = net
//!         .next_deadline(&services)
//!         .map(|deadline| deadline.saturating_sub(Instant::now()).into());
//!     poll.poll(&mut events, timeout)?;
//!
//!     let mut worked = false;
//!     for event in &events {
//!         let ours = net.handle_event(event, &mut services);
//!         worked |= ours;
//!         if !ours {
//!             // A source of the caller's own: the waker, or a socket it
//!             // registered on this poll itself.
//!         }
//!     }
//!     worked |= net.tick(&mut services);
//!     drop(services);
//!
//!     while let Some(event) = api.next_event() {
//!         if let HttpEvent::Request { request, responder, .. } = event {
//!             responder.respond(200, &[], request.path.as_bytes());
//!         }
//!         worked = true;
//!     }
//!     let _ = worked; // the tile ORs this into SpineAdapter::mark_work
//! #   break;
//! }
//! # Ok::<(), std::io::Error>(())
//! ```

mod connector;
mod endpoint;
mod network;
mod payload_buf;
mod service;
mod stream_service;
mod tcp_stream;
mod transport;

pub use connector::{PollEvent, SendBehavior, TcpConnector};
pub use endpoint::{Endpoint, Peer};
pub use network::{
    ConnectionGroup, ConnectionGroupConfig, ConnectionGroupId, Deadline, Framing, ReadinessOutcome,
    StreamEvent, StreamNetwork, TcpOptions, TickOutcome,
};
pub use payload_buf::PayloadBuf;
pub use service::Service;
pub use stream_service::{Retained, StreamService, StreamSink};
pub(crate) use tcp_stream::set_socket_buf_size;
pub use tcp_stream::{ConnState, TcpStream, TcpTelemetry};
