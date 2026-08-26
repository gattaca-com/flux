pub mod http;

pub mod stream;
/// The mio a network is built on. An External-mode caller reaches `Poll`,
/// `Registry`, `Waker`, `Events` and `event::Event` through here, so the poll
/// it drives and the sockets flux registers on it are the same mio.
pub use mio;
pub use mio::Token;
