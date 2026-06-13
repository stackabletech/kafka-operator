//! Property-file builders for Kafka rolegroup ConfigMaps.

pub mod broker_properties;
pub mod controller_properties;
pub mod listener;
pub mod logging;
pub mod security_properties;

use crate::crd::{
    KafkaPodDescriptor,
    role::{AnyConfig, KafkaRole},
};

/// The names of the config files assembled into the rolegroup `ConfigMap`.
///
/// A single source of truth for the on-disk file names, used by the config-map
/// builder, the per-file property builders and the JVM/command builders.
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::Display)]
pub enum ConfigFileName {
    #[strum(serialize = "broker.properties")]
    BrokerProperties,
    #[strum(serialize = "controller.properties")]
    ControllerProperties,
    #[strum(serialize = "security.properties")]
    Security,
    #[strum(serialize = "client.properties")]
    Client,
    /// JAAS configuration for Kerberos authentication. It has the `.properties`
    /// extension but is not a Java properties file.
    #[strum(serialize = "jaas.properties")]
    Jaas,
    /// Used by Kafka 3.x.
    #[strum(serialize = "log4j.properties")]
    Log4j,
    /// Used by Kafka 4.0 and later.
    #[strum(serialize = "log4j2.properties")]
    Log4j2,
}

/// The product config-file name for a role group, derived from its role
/// (`broker.properties` for brokers, `controller.properties` for controllers).
pub fn config_file_name(config: &AnyConfig) -> ConfigFileName {
    match config {
        AnyConfig::Broker(_) => ConfigFileName::BrokerProperties,
        AnyConfig::Controller(_) => ConfigFileName::ControllerProperties,
    }
}

/// Whether the given Kafka version uses the legacy log4j logging framework.
///
/// Kafka 3.x uses log4j ([`ConfigFileName::Log4j`]); Kafka 4.0 and later use log4j2
/// ([`ConfigFileName::Log4j2`]). This is the single source of truth for that decision,
/// used both when rendering the log config file and when selecting the JVM option that
/// points at it.
pub fn uses_legacy_log4j(product_version: &str) -> bool {
    product_version.starts_with("3.")
}

pub(crate) fn kraft_controllers(pod_descriptors: &[KafkaPodDescriptor]) -> Vec<String> {
    pod_descriptors
        .iter()
        .filter(|pd| pd.role == KafkaRole::Controller.to_string())
        .map(|desc| {
            format!(
                "{fqdn}:{client_port}",
                fqdn = desc.fqdn(),
                client_port = desc.client_port
            )
        })
        .collect::<Vec<String>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_match_the_kafka_on_disk_names() {
        assert_eq!(
            ConfigFileName::BrokerProperties.to_string(),
            "broker.properties"
        );
        assert_eq!(
            ConfigFileName::ControllerProperties.to_string(),
            "controller.properties"
        );
        assert_eq!(ConfigFileName::Security.to_string(), "security.properties");
        assert_eq!(ConfigFileName::Client.to_string(), "client.properties");
        assert_eq!(ConfigFileName::Jaas.to_string(), "jaas.properties");
        assert_eq!(ConfigFileName::Log4j.to_string(), "log4j.properties");
        assert_eq!(ConfigFileName::Log4j2.to_string(), "log4j2.properties");
    }
}
