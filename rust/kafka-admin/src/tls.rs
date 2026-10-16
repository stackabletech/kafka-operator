//! TLS client configuration from PEM files.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use snafu::{ResultExt, Snafu, ensure};

use crate::connection::Security;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to read the PEM file {path:?}"))]
    ReadPem {
        source: rustls::pki_types::pem::Error,
        path: PathBuf,
    },

    #[snafu(display("the PEM file {path:?} contains no certificate"))]
    NoCertificate { path: PathBuf },

    #[snafu(display("failed to add the CA certificate from {path:?}"))]
    AddCaCertificate {
        source: rustls::Error,
        path: PathBuf,
    },

    #[snafu(display("failed to build the TLS configuration"))]
    BuildConfig { source: rustls::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Returns `None` for plaintext connections. The PEM files are read on every call.
pub(crate) fn client_config(security: &Security) -> Result<Option<ClientConfig>> {
    let (ca, client_certificate) = match security {
        Security::Plaintext => return Ok(None),
        Security::Tls { ca } => (ca, None),
        Security::MutualTls {
            ca,
            certificate,
            key,
        } => (ca, Some((certificate, key))),
    };

    let builder =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .context(BuildConfigSnafu)?
            .with_root_certificates(root_certificates(ca)?);

    let config = match client_certificate {
        None => builder.with_no_client_auth(),
        Some((certificate, key)) => {
            let key = PrivateKeyDer::from_pem_file(key).context(ReadPemSnafu { path: key })?;
            builder
                .with_client_auth_cert(certificate_chain(certificate)?, key)
                .context(BuildConfigSnafu)?
        }
    };
    Ok(Some(config))
}

fn root_certificates(ca: &Path) -> Result<RootCertStore> {
    let mut root_certificates = RootCertStore::empty();
    for certificate in CertificateDer::pem_file_iter(ca).context(ReadPemSnafu { path: ca })? {
        root_certificates
            .add(certificate.context(ReadPemSnafu { path: ca })?)
            .context(AddCaCertificateSnafu { path: ca })?;
    }
    ensure!(
        !root_certificates.is_empty(),
        NoCertificateSnafu { path: ca }
    );
    Ok(root_certificates)
}

fn certificate_chain(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let certificate_chain = CertificateDer::pem_file_iter(path)
        .context(ReadPemSnafu { path })?
        .collect::<Result<Vec<_>, _>>()
        .context(ReadPemSnafu { path })?;
    ensure!(!certificate_chain.is_empty(), NoCertificateSnafu { path });
    Ok(certificate_chain)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_ca_file_without_certificate() {
        let ca = std::env::temp_dir().join(format!("empty-ca-{}.crt", std::process::id()));
        std::fs::write(&ca, "").expect("the CA file should be writable");

        let error = root_certificates(&ca).expect_err("the CA file should be rejected");
        std::fs::remove_file(&ca).expect("the CA file should be removable");

        assert!(matches!(error, Error::NoCertificate { .. }));
    }
}
