//! The crate's error type.
//!
//! One flat `snafu` enum covers the whole client: connection/TLS setup, the wire round-trip
//! (framing, correlation, version negotiation) and Kafka protocol-level errors (non-zero response
//! codes). Kept flat rather than per-operation (tokio-zookeeper splits `error::Create`,
//! `error::Delete`, ...) because the admin surface is small and callers mostly map the whole thing to
//! one CRD condition.

use std::path::PathBuf;

use snafu::Snafu;

/// Everything that can go wrong talking to Kafka.
#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("no bootstrap server in {bootstrap_servers:?}"))]
    NoBootstrapServer { bootstrap_servers: String },

    #[snafu(display("invalid bootstrap host:port {addr:?}"))]
    InvalidBootstrapAddr { addr: String },

    #[snafu(display("failed to connect to the Kafka cluster at {addr:?}"))]
    Connect {
        source: std::io::Error,
        addr: String,
    },

    #[snafu(display("TLS handshake with {addr:?} failed"))]
    TlsHandshake {
        source: std::io::Error,
        addr: String,
    },

    #[snafu(display("invalid TLS server name {host:?}"))]
    InvalidServerName {
        source: rustls::pki_types::InvalidDnsNameError,
        host: String,
    },

    #[snafu(display("failed to read the credential file {path:?}"))]
    ReadCredentialFile {
        source: std::io::Error,
        path: PathBuf,
    },

    #[snafu(display("failed to parse PEM from {path:?}"))]
    ParsePem {
        source: rustls::pki_types::pem::Error,
        path: PathBuf,
    },

    #[snafu(display("failed to add the trust anchor to the root certificate store"))]
    AddRootCert { source: rustls::Error },

    #[snafu(display("failed to build the mTLS client config"))]
    BuildTlsConfig { source: rustls::Error },

    #[snafu(display("an mTLS credential is required but no cert dir was configured"))]
    MissingCredential,

    #[snafu(display("failed to write the {request} request to the wire"))]
    WriteRequest {
        source: std::io::Error,
        request: &'static str,
    },

    #[snafu(display("failed to read the {request} response from the wire"))]
    ReadResponse {
        source: std::io::Error,
        request: &'static str,
    },

    #[snafu(display("failed to encode the {request} request"))]
    Encode {
        source: anyhow::Error,
        request: &'static str,
    },

    #[snafu(display("failed to decode the {request} response"))]
    Decode {
        source: anyhow::Error,
        request: &'static str,
    },

    #[snafu(display(
        "correlation id mismatch on {request} response: expected {expected}, got {actual}"
    ))]
    CorrelationMismatch {
        request: &'static str,
        expected: i32,
        actual: i32,
    },

    #[snafu(display("the broker does not support any version of the {request} api we can speak"))]
    UnsupportedApi { request: &'static str },

    #[snafu(display("the Kafka {request} request returned error code {code} ({name}){detail}"))]
    Kafka {
        request: &'static str,
        code: i16,
        name: String,
        detail: String,
    },

    #[snafu(display(
        "cannot drain: {blocked} partition(s) would drop below their replication factor \
         (too few surviving brokers) — refusing to remove the broker"
    ))]
    CannotDrainBelowReplicationFactor { blocked: u32 },
}

impl Error {
    /// Builds a [`Error::Kafka`] from a Kafka error code + optional broker error message.
    ///
    /// `ResponseError::try_from_code` gives the symbolic name; unknown codes stringify as `None`.
    pub(crate) fn from_code(request: &'static str, code: i16, message: Option<&str>) -> Self {
        let name = kafka_protocol::ResponseError::try_from_code(code)
            .map(|e| e.to_string())
            .unwrap_or_else(|| "None".to_string());
        let detail = message
            .filter(|m| !m.is_empty())
            .map(|m| format!(": {m}"))
            .unwrap_or_default();
        Error::Kafka {
            request,
            code,
            name,
            detail,
        }
    }

    /// Whether this error is worth retrying (a transient blip vs. a permanent failure).
    ///
    /// Connection/IO errors are transient (the broker may be restarting, the socket may have
    /// dropped). Kafka protocol errors defer to [`ResponseError::is_retriable`], which is
    /// protocol-accurate (e.g. `RequestTimedOut`, `NotController`, `LeaderNotAvailable`,
    /// `CoordinatorLoadInProgress` are retriable; `InvalidConfig`, auth failures are not).
    /// Everything else — bad config/credentials, codec bugs, correlation desync, an unsupported api —
    /// will not fix itself on retry.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Connect { .. }
            | Error::TlsHandshake { .. }
            | Error::WriteRequest { .. }
            | Error::ReadResponse { .. } => true,
            Error::Kafka { code, .. } => kafka_protocol::ResponseError::try_from_code(*code)
                .is_some_and(|e| e.is_retriable()),
            _ => false,
        }
    }

    /// Whether this error means the reused connection should be dropped and re-dialled before the
    /// next attempt — either the socket is poisoned (IO error) or we reached the wrong node
    /// (`NotController`, so a re-dial + fresh `Metadata` will re-route to the current controller).
    pub(crate) fn is_connection_fatal(&self) -> bool {
        match self {
            Error::Connect { .. }
            | Error::TlsHandshake { .. }
            | Error::WriteRequest { .. }
            | Error::ReadResponse { .. } => true,
            Error::Kafka { code, .. } => {
                *code == kafka_protocol::ResponseError::NotController.code()
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Error;

    fn kafka(code: i16) -> Error {
        Error::from_code("Test", code, None)
    }

    #[test]
    fn kafka_codes_classified_by_protocol_flag() {
        // Retriable per KIP/`ResponseError::is_retriable`.
        assert!(kafka(7).is_retryable(), "RequestTimedOut"); // 7
        assert!(kafka(41).is_retryable(), "NotController"); // 41
        assert!(kafka(5).is_retryable(), "LeaderNotAvailable"); // 5
        // Not retriable.
        assert!(!kafka(40).is_retryable(), "InvalidConfig"); // 40
        assert!(!kafka(37).is_retryable(), "TopicAlreadyExists"); // 37 (also handled idempotently upstream)
    }

    #[test]
    fn not_controller_is_connection_fatal_but_timeout_is_not() {
        assert!(kafka(41).is_connection_fatal(), "NotController re-routes");
        assert!(
            !kafka(7).is_connection_fatal(),
            "RequestTimedOut retries on the same connection"
        );
    }

    #[test]
    fn config_errors_are_terminal() {
        assert!(!Error::MissingCredential.is_retryable());
        assert!(!Error::MissingCredential.is_connection_fatal());
    }
}
