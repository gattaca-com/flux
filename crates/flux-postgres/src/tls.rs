//! Client TLS configuration beyond the transport's verifying default.

use std::sync::{Arc, OnceLock};

use flux_network::tls::rustls::{
    self, ClientConfig, DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{CryptoProvider, WebPkiSupportedAlgorithms},
    pki_types::{CertificateDer, ServerName, UnixTime},
};

pub(crate) fn check_server_name(name: &str) {
    ServerName::try_from(name).expect("valid TLS server name");
}

/// Accepts any certificate, as libpq's `sslmode=require` does. Handshake
/// signatures are still checked, so the peer must hold the key it presents.
#[derive(Debug)]
struct AnyCertificate(WebPkiSupportedAlgorithms);

impl ServerCertVerifier for AnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

/// The shared unverified config, over the same ring provider the transport
/// uses.
pub(crate) fn unverified() -> Arc<ClientConfig> {
    static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let provider: Arc<CryptoProvider> = Arc::new(rustls::crypto::ring::default_provider());
            let verifier = Arc::new(AnyCertificate(provider.signature_verification_algorithms));
            Arc::new(
                ClientConfig::builder_with_provider(provider)
                    .with_safe_default_protocol_versions()
                    .expect("ring supports standard TLS")
                    .dangerous()
                    .with_custom_certificate_verifier(verifier)
                    .with_no_client_auth(),
            )
        })
        .clone()
}
