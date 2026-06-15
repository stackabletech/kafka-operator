use std::str::FromStr;

use serde::{Deserialize, Serialize};
use stackable_operator::{
    schemars::{self, JsonSchema},
    v2::types::kubernetes::SecretClassName,
};

const TLS_DEFAULT_SECRET_CLASS: &str = "tls";

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTls {
    /// The [SecretClass](DOCS_BASE_URL_PLACEHOLDER/secret-operator/secretclass.html) to use for
    /// internal broker communication. Use mutual verification between brokers (mandatory).
    /// This setting controls:
    /// - Which cert the brokers should use to authenticate themselves against other brokers
    /// - Which ca.crt to use when validating the other brokers
    ///
    /// Defaults to `tls`
    /// Set to `null` to disable internal TLS (resulting in a plaintext cluster).
    #[serde(default = "internal_tls_default")]
    pub internal_secret_class: Option<SecretClassName>,
    /// The [SecretClass](DOCS_BASE_URL_PLACEHOLDER/secret-operator/secretclass.html) to use for
    /// client connections. This setting controls:
    /// - If TLS encryption is used at all
    /// - Which cert the servers should use to authenticate themselves against the client
    ///
    /// Defaults to `tls`.
    #[serde(
        default = "server_tls_default",
        skip_serializing_if = "Option::is_none"
    )]
    pub server_secret_class: Option<SecretClassName>,
}

/// Default TLS settings.
/// Internal and server communication default to `tls` secret class.
pub fn default_kafka_tls() -> Option<KafkaTls> {
    Some(KafkaTls {
        internal_secret_class: internal_tls_default(),
        server_secret_class: server_tls_default(),
    })
}

/// The `tls` default secret class as a typed name.
fn default_secret_class() -> SecretClassName {
    SecretClassName::from_str(TLS_DEFAULT_SECRET_CLASS)
        .expect("the default secret class name is valid")
}

/// Helper methods to provide defaults in the CRDs and tests
pub fn internal_tls_default() -> Option<SecretClassName> {
    Some(default_secret_class())
}

/// Helper methods to provide defaults in the CRDs and tests
pub fn server_tls_default() -> Option<SecretClassName> {
    Some(default_secret_class())
}
