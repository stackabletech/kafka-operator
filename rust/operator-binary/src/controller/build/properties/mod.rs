//! Property-file builders for Kafka rolegroup ConfigMaps.

pub mod broker_properties;
pub mod controller_properties;
pub mod listener;
pub mod product_logging;
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
    /// Client-side (unprefixed `security.protocol`/`ssl.*`) properties for an admin CLI tool
    /// (e.g. `kafka-metadata-quorum.sh`) running inside a controller pod. Only written to
    /// controller rolegroup `ConfigMap`s.
    #[strum(serialize = "admin-client.properties")]
    AdminClient,
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

/// The config file name for a role group, derived from its role (`broker.properties` for brokers,
/// `controller.properties` for controllers).
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

/// `controller.quorum.bootstrap.servers` addresses, one per distinct controller role group,
/// pointing at each role group's own headless Service DNS name rather than individual pod
/// FQDNs.
///
/// Only adding or removing a whole role group changes this list.
pub(crate) fn kraft_controllers(pod_descriptors: &[KafkaPodDescriptor]) -> Vec<String> {
    let mut role_group_addresses: Vec<String> = pod_descriptors
        .iter()
        .filter(|pd| pd.role == KafkaRole::Controller)
        .map(|desc| {
            format!(
                "{service}.{namespace}.svc.{cluster_domain}:{client_port}",
                service = desc.role_group_service_name,
                namespace = desc.namespace,
                cluster_domain = desc.cluster_domain,
                client_port = desc.client_port,
            )
        })
        .collect();
    role_group_addresses.sort();
    role_group_addresses.dedup();
    role_group_addresses
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
        assert_eq!(
            ConfigFileName::AdminClient.to_string(),
            "admin-client.properties"
        );
        assert_eq!(ConfigFileName::Jaas.to_string(), "jaas.properties");
        assert_eq!(ConfigFileName::Log4j.to_string(), "log4j.properties");
        assert_eq!(ConfigFileName::Log4j2.to_string(), "log4j2.properties");
    }

    /// Builds a minimal [`KafkaPodDescriptor`] for the given role, replica and client port.
    ///
    /// `KafkaPodDescriptor`'s fields are `pub(crate)`, which is crate-wide (not
    /// module-scoped) visibility in Rust, so this direct construction is legal from any
    /// module inside `stackable-kafka-operator`, including this one.
    fn pod_descriptor(role: KafkaRole, replica: u16, client_port: u16) -> KafkaPodDescriptor {
        KafkaPodDescriptor {
            namespace: "default".parse().expect("valid namespace name"),
            role_group_statefulset_name: "kafka-controller-default"
                .parse()
                .expect("valid statefulset name"),
            role_group_service_name: "kafka-controller-default-headless"
                .parse()
                .expect("valid service name"),
            replica,
            cluster_domain: stackable_operator::commons::networking::DomainName::try_from(
                "cluster.local",
            )
            .expect("valid domain"),
            node_id: replica.into(),
            role,
            client_port: client_port.into(),
        }
    }

    /// `kraft_controllers` points at the controller role group's *headless Service* DNS name
    /// (no pod prefix), not individual pod FQDNs. A headless Service's own DNS name resolves
    /// to every backing pod's IP (Kafka's own AdminClient default,
    /// `client.dns.lookup=use_all_dns_ips`, already expects exactly this), and the
    /// operator's headless Service sets `publishNotReadyAddresses: true`, so this also works
    /// during initial cluster formation before any pod is Ready. This is what makes
    /// `controller.quorum.bootstrap.servers` invariant to the controller role group's
    /// replica count: adding or removing replicas within an existing role group never
    /// changes the role group's own Service name.
    #[test]
    fn kraft_controllers_points_at_the_role_group_headless_service_not_individual_pods() {
        let pod_descriptors = vec![
            pod_descriptor(KafkaRole::Controller, 0, 9093),
            pod_descriptor(KafkaRole::Controller, 1, 9093),
            pod_descriptor(KafkaRole::Controller, 2, 9093),
            // Brokers must be filtered out of the controller quorum bootstrap servers list.
            pod_descriptor(KafkaRole::Broker, 0, 9092),
        ];

        let quorum_bootstrap_servers = kraft_controllers(&pod_descriptors).join(",");

        assert_eq!(
            quorum_bootstrap_servers,
            "kafka-controller-default-headless.default.svc.cluster.local:9093"
        );
    }

    /// The whole point: scaling an existing controller role group up or down must not change
    /// `kraft_controllers`'s output at all, since it no longer depends on which replicas
    /// currently exist — the role group's Service name is stable regardless.
    #[test]
    fn kraft_controllers_is_stable_across_replica_count_changes() {
        let three_replicas = vec![
            pod_descriptor(KafkaRole::Controller, 0, 9093),
            pod_descriptor(KafkaRole::Controller, 1, 9093),
            pod_descriptor(KafkaRole::Controller, 2, 9093),
        ];
        let five_replicas = vec![
            pod_descriptor(KafkaRole::Controller, 0, 9093),
            pod_descriptor(KafkaRole::Controller, 1, 9093),
            pod_descriptor(KafkaRole::Controller, 2, 9093),
            pod_descriptor(KafkaRole::Controller, 3, 9093),
            pod_descriptor(KafkaRole::Controller, 4, 9093),
        ];

        assert_eq!(
            kraft_controllers(&three_replicas),
            kraft_controllers(&five_replicas)
        );
    }

    /// Multiple controller role groups each have their own headless Service, so each must
    /// still get its own bootstrap-servers entry — deduplication is per-Service, not a
    /// blanket "collapse everything to one entry".
    #[test]
    fn kraft_controllers_lists_every_distinct_role_groups_service_once() {
        let mut default_group_pod = pod_descriptor(KafkaRole::Controller, 0, 9093);
        let mut other_group_pod = pod_descriptor(KafkaRole::Controller, 0, 9093);
        other_group_pod.role_group_statefulset_name = "kafka-controller-other"
            .parse()
            .expect("valid statefulset name");
        other_group_pod.role_group_service_name = "kafka-controller-other-headless"
            .parse()
            .expect("valid service name");
        // Second replica of the *same* role group as `default_group_pod` — must not produce
        // a second entry for that Service.
        let default_group_pod_replica_1 = {
            let mut pod = pod_descriptor(KafkaRole::Controller, 1, 9093);
            pod.node_id = 1;
            pod
        };
        default_group_pod.node_id = 0;

        let pod_descriptors = vec![
            default_group_pod,
            default_group_pod_replica_1,
            other_group_pod,
        ];

        let mut quorum_bootstrap_servers = kraft_controllers(&pod_descriptors);
        quorum_bootstrap_servers.sort();

        assert_eq!(
            quorum_bootstrap_servers,
            vec![
                "kafka-controller-default-headless.default.svc.cluster.local:9093".to_string(),
                "kafka-controller-other-headless.default.svc.cluster.local:9093".to_string(),
            ]
        );
    }
}
