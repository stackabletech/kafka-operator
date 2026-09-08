//! mTLS client-config construction (no tokio-zookeeper analog beyond its transport wiring).

use std::{path::Path, sync::Arc};

use rustls::ClientConfig;
use snafu::ResultExt;

use crate::error::{
    AddRootCertSnafu, BuildTlsConfigSnafu, Error, ParsePemSnafu, ReadCredentialFileSnafu,
};

/// Builds a rustls [`ClientConfig`] for mutual TLS.
///
/// Client identity (`tls.crt` chain + `tls.key`) comes from `cert_dir`; the root store used to
/// verify the *server* comes from `server_ca_dir/ca.crt`, which may be a different CA than the client
/// credential's own (cross-CA mTLS). Uses the explicit `ring` crypto provider so no process-wide
/// default `CryptoProvider` needs installing (and to avoid aws-lc-rs' C toolchain — this is what keeps
/// the crate building in a bare sandbox).
pub(crate) fn build_client_config(
    cert_dir: &Path,
    server_ca_dir: &Path,
) -> Result<Arc<ClientConfig>, Error> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

    let ca_path = server_ca_dir.join("ca.crt");
    let cert_path = cert_dir.join("tls.crt");
    let key_path = cert_dir.join("tls.key");

    let ca_bytes = std::fs::read(&ca_path).context(ReadCredentialFileSnafu { path: &ca_path })?;
    let mut roots = rustls::RootCertStore::empty();
    for ca in CertificateDer::pem_slice_iter(&ca_bytes) {
        let ca = ca.context(ParsePemSnafu { path: &ca_path })?;
        roots.add(ca).context(AddRootCertSnafu)?;
    }

    let cert_bytes =
        std::fs::read(&cert_path).context(ReadCredentialFileSnafu { path: &cert_path })?;
    let cert_chain = CertificateDer::pem_slice_iter(&cert_bytes)
        .collect::<Result<Vec<_>, _>>()
        .context(ParsePemSnafu { path: &cert_path })?;
    let key_bytes = std::fs::read(&key_path).context(ReadCredentialFileSnafu { path: &key_path })?;
    let key =
        PrivateKeyDer::from_pem_slice(&key_bytes).context(ParsePemSnafu { path: &key_path })?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context(BuildTlsConfigSnafu)?
        .with_root_certificates(roots)
        .with_client_auth_cert(cert_chain, key)
        .context(BuildTlsConfigSnafu)?;

    Ok(Arc::new(config))
}
