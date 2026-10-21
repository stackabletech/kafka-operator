//! Property-file builders for Kafka rolegroup ConfigMaps.

pub mod broker_properties;
pub mod controller_properties;
pub mod listener;
pub mod product_logging;
pub mod security_properties;

use std::collections::BTreeSet;

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

/// Builds the contents of the `controller.quorum.bootstrap.servers` property.
///
/// When Kerberos is enabled, the list contains the FQDN pod names of the KRaft controllers.
///
/// For non-kerberized clusters, this list contains the headless service names
/// of controller role groups.
///
/// # Why pod FQDNs under Kerberos
///
/// The CONTROLLER listener's acceptor offers the principal `kafka/<pod-fqdn>`, which
/// is also what the Raft voter endpoints advertise.
/// When a GSSAPI client uses the headless Service to ask for `kafka/<service>`,
/// the authentication fails for every peer, so Kerberos-enabled clusters must list
/// individual pod FQDNs.
///
/// That has a cost: the pod-FQDN list changes on every scaling operation (replica count change),
/// so scaling a controller role group rolls *all* controller pods.
/// Non-Kerberos clusters don't need pod-level addressing, so they keep the headless
/// Service form and avoid that churn.
pub(crate) fn kraft_controllers(
    pod_descriptors: &[KafkaPodDescriptor],
    kerberos_enabled: bool,
) -> Vec<String> {
    let controllers = pod_descriptors
        .iter()
        .filter(|pd| pd.role == KafkaRole::Controller);

    controllers
        .map(|desc| {
            if kerberos_enabled {
                format!(
                    "{sts}-{replica}.{service}.{namespace}.svc.{cluster_domain}:{client_port}",
                    sts = desc.role_group_statefulset_name,
                    replica = desc.replica,
                    service = desc.role_group_service_name,
                    namespace = desc.namespace,
                    cluster_domain = desc.cluster_domain,
                    client_port = desc.client_port,
                )
            } else {
                format!(
                    "{service}.{namespace}.svc.{cluster_domain}:{client_port}",
                    service = desc.role_group_service_name,
                    namespace = desc.namespace,
                    cluster_domain = desc.cluster_domain,
                    client_port = desc.client_port,
                )
            }
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
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

    #[test]
    fn kraft_controllers_lists_individual_pod_fqdns_under_kerberos() {
        let pod_descriptors = vec![
            pod_descriptor(KafkaRole::Controller, 0, 9093),
            pod_descriptor(KafkaRole::Controller, 1, 9093),
            pod_descriptor(KafkaRole::Controller, 2, 9093),
            // Brokers must be filtered out of the controller quorum bootstrap servers list.
            pod_descriptor(KafkaRole::Broker, 0, 9092),
        ];

        let quorum_bootstrap_servers = kraft_controllers(&pod_descriptors, true).join(",");

        // Individual pod FQDNs, *not* the role group's headless Service. Under Kerberos the
        // GSSAPI service principal is derived from the hostname the peer dials, and the
        // CONTROLLER listener's acceptor can only offer one SPN -- the pod's own. Dialling
        // the headless Service asks for `kafka/<service>` instead and is rejected.
        assert_eq!(
            quorum_bootstrap_servers,
            "kafka-controller-default-0.kafka-controller-default-headless.default.svc.cluster.local:9093,\
             kafka-controller-default-1.kafka-controller-default-headless.default.svc.cluster.local:9093,\
             kafka-controller-default-2.kafka-controller-default-headless.default.svc.cluster.local:9093"
        );
    }

    #[test]
    fn kraft_controllers_lists_headless_services_without_kerberos() {
        let mut other_group_pod = pod_descriptor(KafkaRole::Controller, 0, 9093);
        other_group_pod.role_group_statefulset_name = "kafka-controller-other"
            .parse()
            .expect("valid statefulset name");
        other_group_pod.role_group_service_name = "kafka-controller-other-headless"
            .parse()
            .expect("valid service name");

        // Two replicas of the *default* role group and one of an *other* role group - the
        // default group's Service entry must appear only once, regardless of replica count.
        let pod_descriptors = vec![
            pod_descriptor(KafkaRole::Controller, 0, 9093),
            pod_descriptor(KafkaRole::Controller, 1, 9093),
            other_group_pod,
            // Brokers must be filtered out of the controller quorum bootstrap servers list.
            pod_descriptor(KafkaRole::Broker, 0, 9092),
        ];

        let quorum_bootstrap_servers = kraft_controllers(&pod_descriptors, false);

        assert_eq!(
            quorum_bootstrap_servers,
            vec![
                "kafka-controller-default-headless.default.svc.cluster.local:9093".to_string(),
                "kafka-controller-other-headless.default.svc.cluster.local:9093".to_string(),
            ]
        );
    }

    #[test]
    fn kraft_controllers_grows_with_the_replica_count_under_kerberos() {
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

        // Deliberate consequence of per-pod addressing: unlike the headless-Service form used
        // without Kerberos, this list changes with the replica count, so scaling a controller
        // role group rolls the controller pods.
        assert_eq!(kraft_controllers(&three_replicas, true).len(), 3);
        assert_eq!(kraft_controllers(&five_replicas, true).len(), 5);
        assert_ne!(
            kraft_controllers(&three_replicas, true),
            kraft_controllers(&five_replicas, true)
        );
    }

    #[test]
    fn kraft_controllers_is_stable_across_replica_count_changes_without_kerberos() {
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

        // Without Kerberos, only adding or removing a whole role group changes this list, so
        // scaling replicas within a role group does not roll the controller pods.
        assert_eq!(
            kraft_controllers(&three_replicas, false),
            kraft_controllers(&five_replicas, false)
        );
    }
}
