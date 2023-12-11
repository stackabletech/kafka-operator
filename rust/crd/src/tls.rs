use serde::{Deserialize, Serialize};
use stackable_operator::schemars::{self, JsonSchema};

const TLS_DEFAULT_SECRET_CLASS: &str = "tls";

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTls {
    /// The [SecretClass](DOCS_BASE_URL_PLACEHOLDER/secret-operator/secretclass.html) to use for
    /// internal broker communication. Use mutual verification between brokers (mandatory).
    /// This setting controls:
    /// - Which cert the brokers should use to authenticate themselves against other brokers
    /// - Which ca.crt to use when validating the other brokers
    /// Defaults to `tls`
    #[serde(default = "internal_tls_default")]
    pub internal_secret_class: String,
    /// The [SecretClass](DOCS_BASE_URL_PLACEHOLDER/secret-operator/secretclass.html) to use for
    /// client connections. This setting controls:
    /// - If TLS encryption is used at all
    /// - Which cert the servers should use to authenticate themselves against the client
    /// Defaults to `tls`.
    #[serde(
        default = "server_tls_default",
        skip_serializing_if = "Option::is_none"
    )]
    pub server_secret_class: Option<String>,
}

/// Default TLS settings.
/// Internal and server communication default to `tls` secret class.
pub fn default_kafka_tls() -> Option<KafkaTls> {
    Some(KafkaTls {
        internal_secret_class: internal_tls_default(),
        server_secret_class: server_tls_default(),
    })
}

/// Helper methods to provide defaults in the CRDs and tests
pub fn internal_tls_default() -> String {
    TLS_DEFAULT_SECRET_CLASS.into()
}

/// Helper methods to provide defaults in the CRDs and tests
pub fn server_tls_default() -> Option<String> {
    Some(TLS_DEFAULT_SECRET_CLASS.into())
}
