//! Paths and environment variables through which the operator configures the agent.

use std::str::FromStr;

use stackable_operator::{constant, v2::builder::pod::container::EnvVarName};
use strum::{Display, EnumString};

/// Directory of the CA certificate the Kafka brokers are verified with.
pub const KAFKA_TLS_DIR: &str = "/stackable/secrets/tls-kafka";

constant!(pub ENV_CLUSTER_NAME: EnvVarName = "KAFKA_CLUSTER_NAME");
constant!(pub ENV_CLUSTER_NAMESPACE: EnvVarName = "KAFKA_CLUSTER_NAMESPACE");
constant!(pub ENV_CLIENT_SECURITY: EnvVarName = "KAFKA_CLIENT_SECURITY");
// Comma-separated `host:port` list
constant!(pub ENV_BOOTSTRAP_SERVERS: EnvVarName = "KAFKA_BOOTSTRAP_SERVERS");

/// How the agent connects to the brokers.
#[derive(Clone, Copy, Debug, Display, EnumString, Eq, PartialEq)]
#[strum(serialize_all = "kebab-case")]
pub enum ClientSecurity {
    Plaintext,
    Tls,
    MutualTls,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::plaintext(ClientSecurity::Plaintext, "plaintext")]
    #[case::tls(ClientSecurity::Tls, "tls")]
    #[case::mutual_tls(ClientSecurity::MutualTls, "mutual-tls")]
    fn roundtrip(#[case] security: ClientSecurity, #[case] value: &str) {
        assert_eq!(value, security.to_string());
        assert_eq!(Ok(security), ClientSecurity::from_str(value));
    }
}
