//! Client-side TLS sessions for outbound connections.
//!
//! A [`Session`] wraps one rustls client connection. The transport hands it
//! socket bytes to decrypt, plaintext to encrypt, and flushes whatever the
//! session wants to send. Certificates verify against the Mozilla roots; the
//! crypto provider is explicit (ring) and never installed process-wide, so a
//! host using another provider is unaffected.
//!
//! Without the `tls` feature `Session` is uninhabited: the transport keeps
//! its `Option<Session>` fields, they are always `None`, and no TLS
//! dependency enters the build.

#[cfg(not(feature = "tls"))]
pub use disabled::Session;
#[cfg(feature = "tls")]
pub use enabled::{ClientConfig, Session};
/// The rustls build these sessions use, so callers configuring TLS cannot
/// drift onto a second version of the crate.
#[cfg(feature = "tls")]
pub use rustls;

#[cfg(feature = "tls")]
mod enabled {
    use std::{
        io,
        sync::{Arc, OnceLock},
    };

    use flux_timing::{Duration, Instant};
    pub use rustls::ClientConfig;

    /// Socket read size; one syscall feeds many TLS records.
    const CIPHER_CHUNK: usize = 32 * 1024;
    use rustls::{ClientConnection, RootCertStore, pki_types::ServerName};

    /// The shared default: Mozilla roots over the ring provider.
    fn default_config() -> Arc<ClientConfig> {
        static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
        CONFIG
            .get_or_init(|| {
                let roots = RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
                let provider = Arc::new(rustls::crypto::ring::default_provider());
                Arc::new(
                    ClientConfig::builder_with_provider(provider)
                        .with_safe_default_protocol_versions()
                        .expect("ring supports standard TLS")
                        .with_root_certificates(roots)
                        .with_no_client_auth(),
                )
            })
            .clone()
    }

    pub struct Session {
        server: ServerName<'static>,
        config: Option<Arc<ClientConfig>>,
        conn: Option<ClientConnection>,
        started: Option<Instant>,
        /// Whether the completed handshake has been reported once.
        announced: bool,
        cipher: Vec<u8>,
        /// Plaintext that did not fit the caller's buffer, and how much of it
        /// has been handed back.
        plain: Vec<u8>,
        served: usize,
    }

    impl Session {
        /// Verifies against the Mozilla roots and sends `server` as SNI.
        /// Panics if `server` is not a valid DNS name or IP.
        pub fn new(server: &str) -> Self {
            Self {
                server: ServerName::try_from(server.to_owned()).expect("valid TLS server name"),
                config: None,
                conn: None,
                started: None,
                announced: false,
                cipher: Vec::new(),
                plain: Vec::new(),
                served: 0,
            }
        }
        /// Trusts `config` instead of the Mozilla roots.
        #[must_use]
        pub fn with_config(mut self, config: Arc<ClientConfig>) -> Self {
            self.config = Some(config);
            self
        }
        /// Starts the handshake, writing the opening flight into `out`. Called
        /// again after a reconnect, which discards the previous session.
        pub fn start(&mut self, out: &mut Vec<u8>) {
            let config = self.config.clone().unwrap_or_else(default_config);
            let mut conn =
                ClientConnection::new(config, self.server.clone()).expect("valid TLS config");
            // Sends are bounded by the caller's frame limits, not by rustls.
            conn.set_buffer_limit(None);
            self.conn = Some(conn);
            self.started = Some(Instant::now());
            self.announced = false;
            self.plain.clear();
            self.served = 0;
            // Allocated once here rather than checked on every read.
            self.cipher.resize(CIPHER_CHUNK, 0);
            self.take_pending(out);
        }
        pub fn is_handshaking(&self) -> bool {
            self.conn.as_ref().is_none_or(|conn| conn.is_handshaking())
        }
        /// Whether the completed handshake has already been reported.
        pub fn announced(&self) -> bool {
            self.announced
        }
        /// Whether the handshake finished but has not been announced yet.
        pub fn handshake_completed(&self) -> bool {
            !self.is_handshaking() && !self.announced
        }
        /// Records that the completed handshake has been reported.
        pub fn mark_announced(&mut self) {
            self.announced = true;
        }
        /// True when the handshake has been stuck longer than `timeout`.
        pub fn handshake_stalled(&self, timeout: Duration) -> bool {
            self.started.is_some_and(|since| self.is_handshaking() && since.elapsed() >= timeout)
        }
        /// Encrypts one write into `out`; false once the session is dead.
        pub fn encrypt(&mut self, plain: &[u8], out: &mut Vec<u8>) -> bool {
            use std::io::Write as _;
            let Some(conn) = self.conn.as_mut() else { return false };
            if conn.writer().write_all(plain).is_err() {
                return false
            }
            self.take_pending(out);
            true
        }
        /// Appends the session's pending bytes to `out`: handshake flights,
        /// alerts, and records produced by [`Self::encrypt`].
        pub fn take_pending(&mut self, out: &mut Vec<u8>) {
            // One write_tls emits a single record; drain like complete_io.
            if let Some(conn) = self.conn.as_mut() {
                while conn.wants_write() {
                    let _ = conn.write_tls(out);
                }
            }
        }
        /// Reads decrypted bytes into `buf`, pulling ciphertext from `socket`.
        /// Mirrors socket semantics: `Ok(0)` at end of stream, `WouldBlock`
        /// when no plaintext is ready yet.
        pub fn read_plain<R: io::Read>(
            &mut self,
            socket: &mut R,
            buf: &mut [u8],
        ) -> io::Result<usize> {
            use std::io::Read as _;
            // Whatever a previous read could not hand over comes first.
            if self.served < self.plain.len() {
                let taken = (&self.plain[self.served..]).read(buf)?;
                self.served += taken;
                if self.served == self.plain.len() {
                    self.plain.clear();
                    self.served = 0;
                }
                return Ok(taken)
            }
            self.fill(socket, buf)
        }
        /// Pulls socket reads through the session until it yields plaintext,
        /// decrypting straight into `buf` and spilling only what will not fit.
        fn fill<R: io::Read>(&mut self, socket: &mut R, buf: &mut [u8]) -> io::Result<usize> {
            use std::io::Read as _;
            loop {
                let Self { conn, cipher, plain, .. } = self;
                let Some(conn) = conn.as_mut() else {
                    return Err(io::Error::other("TLS bytes before the handshake started"))
                };
                // One syscall per fill: rustls reads at most 4KB per
                // read_tls, so feeding it the socket directly would cost a
                // syscall per record.
                let read = socket.read(cipher)?;
                if read == 0 {
                    return Ok(0)
                }
                // read_tls also refuses entry once ~16KB of plaintext is
                // undrained, so process and drain after every slice read.
                let mut pending = &cipher[..read];
                let mut filled = 0;
                while !pending.is_empty() {
                    conn.read_tls(&mut pending)?;
                    conn.process_new_packets().map_err(io::Error::other)?;
                    // Decrypt into the caller's buffer, which a whole fill
                    // normally fits, so nothing is copied twice.
                    while filled < buf.len() {
                        match conn.reader().read(&mut buf[filled..]) {
                            Ok(0) => break,
                            Ok(taken) => filled += taken,
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                            Err(error) => return Err(error),
                        }
                    }
                    if filled == buf.len() {
                        match conn.reader().read_to_end(plain) {
                            Ok(_) => {}
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                            Err(error) => return Err(error),
                        }
                    }
                }
                // A read that carried only handshake records yields nothing
                // to hand back; the next one may, or it blocks and says so.
                if filled > 0 {
                    return Ok(filled)
                }
            }
        }
    }
}

#[cfg(not(feature = "tls"))]
mod disabled {
    use std::io;

    use flux_timing::Duration;

    /// Uninhabited: the `tls` feature is off, so no session can be built.
    pub enum Session {}

    impl Session {
        pub fn is_handshaking(&self) -> bool {
            unreachable!("the tls feature is off, so no session exists")
        }
        pub fn announced(&self) -> bool {
            unreachable!("the tls feature is off, so no session exists")
        }
        pub fn start(&mut self, _out: &mut Vec<u8>) {
            unreachable!("the tls feature is off, so no session exists")
        }
        pub fn handshake_completed(&self) -> bool {
            unreachable!("the tls feature is off, so no session exists")
        }
        pub fn mark_announced(&mut self) {
            unreachable!("the tls feature is off, so no session exists")
        }
        pub fn handshake_stalled(&self, _timeout: Duration) -> bool {
            unreachable!("the tls feature is off, so no session exists")
        }
        pub fn encrypt(&mut self, _plain: &[u8], _out: &mut Vec<u8>) -> bool {
            unreachable!("the tls feature is off, so no session exists")
        }
        pub fn take_pending(&mut self, _out: &mut Vec<u8>) {
            unreachable!("the tls feature is off, so no session exists")
        }
        pub fn read_plain<R: io::Read>(
            &mut self,
            _socket: &mut R,
            _buf: &mut [u8],
        ) -> io::Result<usize> {
            unreachable!("the tls feature is off, so no session exists")
        }
    }
}
