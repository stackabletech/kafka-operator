//! Paths and environment variables through which the operator configures the agent.

use strum::{Display, EnumString};

pub const DISCOVERY_DIR: &str = "/stackable/discovery";

/// Directory of the CA certificate the Kafka brokers are verified with.
pub const SERVER_CA_DIR: &str = "/stackable/secrets/tls-server-ca";

pub const ENV_CLUSTER_NAME: &str = "KAFKA_CLUSTER_NAME";

pub const ENV_CLUSTER_NAMESPACE: &str = "KAFKA_CLUSTER_NAMESPACE";

pub const ENV_CLIENT_SECURITY: &str = "KAFKA_CLIENT_SECURITY";

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
