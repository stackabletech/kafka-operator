//! Property-file builders for Kafka rolegroup ConfigMaps.

pub mod broker_properties;
pub mod controller_properties;
pub mod listener;
pub mod product_logging;
pub mod security_properties;

use std::collections::{BTreeMap, BTreeSet};

use self::listener::headless_service_address;
use crate::{
    controller::{ValidatedCluster, ValidatedClusterConfig},
    crd::{KafkaPodDescriptor, role::KafkaRole},
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

/// Whether the given Kafka version uses the legacy log4j logging framework.
///
/// Kafka 3.x uses log4j ([`ConfigFileName::Log4j`]); Kafka 4.0 and later use log4j2
/// ([`ConfigFileName::Log4j2`]). This is the single source of truth for that decision,
/// used both when rendering the log config file and when selecting the JVM option that
/// points at it.
pub fn uses_legacy_log4j(product_version: &str) -> bool {
    product_version.starts_with("3.")
}

/// Builds the controllers' `controller.quorum.bootstrap.servers` property: the FQDNs of all KRaft
/// controller pods.
///
/// # Reason for NOT using the role group's headless service
///
/// When a new controller pod starts (without metadata) it finds the quorum leader by fetching
/// from a bootstrap server. Kafka treats every entry of this list as one bootstrap node and picks
/// a random entry for each attempt. The headless Service publishes not-ready addresses, so a
/// single Service entry can resolve to the starting pod itself. That connection succeeds, so
/// Kafka NEVER tries another address and the pod never learns the leader (see
/// <https://github.com/stackabletech/kafka-operator/issues/1062>).
///
/// This has a cost: the list changes on every scaling operation of a controller role group, so
/// scaling rolls all controller pods. Brokers use [`kraft_controller_quorum_service`] instead and
/// are not affected.
pub(crate) fn kraft_controller_pods(pod_descriptors: &[KafkaPodDescriptor]) -> Vec<String> {
    pod_descriptors
        .iter()
        .filter(|pd| pd.role == KafkaRole::Controller)
        .map(|desc| {
            format!(
                "{host}:{port}",
                host = desc.pod_fqdn(),
                port = desc.client_port
            )
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Builds the brokers' `controller.quorum.bootstrap.servers` property: the address of the
/// headless Service of all KRaft controllers, see
/// [`build_controller_quorum_service`][bcqs].
///
/// Unlike controllers (see [`kraft_controller_pods`]), a broker is never part of this Service,
/// so it cannot resolve it to its own address. The address depends on neither the controller role
/// groups nor their replicas, so changing the controllers never restarts the brokers.
///
/// [bcqs]: crate::controller::build::resource::service::build_controller_quorum_service
pub(crate) fn kraft_controller_quorum_service(cluster: &ValidatedCluster) -> String {
    headless_service_address(
        &cluster.controller_quorum_service_name(),
        &cluster.namespace,
        &cluster.cluster_domain,
        &cluster.cluster_config.kafka_security.client_port(),
    )
}

/// The OPA authorizer properties shared by brokers and controllers.
///
/// In KRaft mode, brokers forward requests such as `CreateTopics` to the controllers without
/// authorizing them, so the controllers must run the authorizer too. Empty if OPA is not
/// configured.
///
/// # Why `super.users` is not set
///
/// Every request is authorized by OPA, including the traffic between the Kafka nodes, so the
/// Rego rules must allow the nodes. Their principal is the subject DN of their internal pod
/// certificate, e.g. `DC=local,...,DC=<pod>,CN=generated certificate for pod`, see the
/// documentation on internal traffic in the security usage guide.
///
/// Listing the subjects of all pods in `super.users` instead would tie every node's config to the
/// set of pods: on a controller scale-up, the running controllers would not know the subject of
/// the new controller and deny its requests until they are restarted, while the `OrderedReady`
/// rollout waits for the new controller to become ready first.
pub(crate) fn opa_authorizer_properties(
    cluster_config: &ValidatedClusterConfig,
) -> BTreeMap<String, String> {
    let Some(opa_connect_string) = cluster_config.opa_connect() else {
        return BTreeMap::new();
    };

    BTreeMap::from([
        (
            "authorizer.class.name".to_string(),
            "org.openpolicyagent.kafka.OpaAuthorizer".to_string(),
        ),
        (
            "opa.authorizer.metrics.enabled".to_string(),
            "true".to_string(),
        ),
        (
            "opa.authorizer.url".to_string(),
            opa_connect_string.to_string(),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

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
    fn opa_authorizer_properties_do_not_set_super_users() {
        let cluster_config = ValidatedClusterConfig {
            kafka_security: crate::controller::test_support::kerberos(),
            authorization_config: Some(crate::crd::authorization::KafkaAuthorizationConfig {
                opa_connect: "http://opa.default.svc.cluster.local:8081/v1/data/kafka".to_owned(),
                secret_class: None,
            }),
            metadata_manager: crate::crd::MetadataManager::KRaft,
            zookeeper_config_map_name: None,
            broker_id_pod_config_map_name: None,
        };

        let properties = opa_authorizer_properties(&cluster_config);

        assert_eq!(
            properties.get("authorizer.class.name").map(String::as_str),
            Some("org.openpolicyagent.kafka.OpaAuthorizer")
        );
        assert_eq!(properties.get("super.users"), None);
    }

    /// Two replicas of the `default` role group (in reverse order), one replica of the `other`
    /// role group and a broker, which must be filtered out.
    fn controllers_of_two_role_groups_and_a_broker() -> Vec<KafkaPodDescriptor> {
        let mut other_group_pod = pod_descriptor(KafkaRole::Controller, 0, 9093);
        other_group_pod.role_group_statefulset_name = "kafka-controller-other"
            .parse()
            .expect("should be a valid statefulset name");
        other_group_pod.role_group_service_name = "kafka-controller-other-headless"
            .parse()
            .expect("should be a valid service name");

        vec![
            pod_descriptor(KafkaRole::Controller, 1, 9093),
            pod_descriptor(KafkaRole::Controller, 0, 9093),
            other_group_pod,
            pod_descriptor(KafkaRole::Broker, 0, 9092),
        ]
    }

    #[test]
    fn kraft_controller_pods_lists_individual_controller_pod_fqdns() {
        // One entry per controller pod, *not* the role group's headless Service: a single
        // Service entry can resolve to the starting pod itself, which then never finds the
        // leader (#1062).
        assert_eq!(
            kraft_controller_pods(&controllers_of_two_role_groups_and_a_broker()),
            vec![
                "kafka-controller-default-0.kafka-controller-default-headless.default.svc.cluster.local:9093"
                    .to_string(),
                "kafka-controller-default-1.kafka-controller-default-headless.default.svc.cluster.local:9093"
                    .to_string(),
                "kafka-controller-other-0.kafka-controller-other-headless.default.svc.cluster.local:9093"
                    .to_string(),
            ]
        );
    }

    /// A KRaft cluster with the given `controllers.roleGroups`, as YAML.
    fn kraft_cluster_with_controller_role_groups(role_groups: &str) -> ValidatedCluster {
        let kafka = crate::controller::test_support::minimal_kafka(&format!(
            r#"
            apiVersion: kafka.stackable.tech/v1alpha1
            kind: KafkaCluster
            metadata:
              name: simple-kafka
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              image:
                productVersion: 3.9.2
              clusterConfig:
                metadataManager: KRaft
              controllers:
                roleGroups: {role_groups}
              brokers:
                roleGroups:
                  default:
                    replicas: 1
            "#
        ));
        crate::controller::test_support::validated_cluster(&kafka)
    }

    /// The brokers' address must not depend on the controller role groups or their replicas, so
    /// that changing the controllers never restarts the brokers.
    #[rstest]
    #[case::three_replicas("{default: {replicas: 3}}")]
    #[case::five_replicas("{default: {replicas: 5}}")]
    #[case::two_role_groups("{default: {replicas: 3}, other: {replicas: 2}}")]
    fn kraft_controller_quorum_service_is_independent_of_the_controllers(
        #[case] controller_role_groups: &str,
    ) {
        let cluster = kraft_cluster_with_controller_role_groups(controller_role_groups);

        assert_eq!(
            kraft_controller_quorum_service(&cluster),
            format!(
                "simple-kafka-controller-headless.default.svc.cluster.local:{port}",
                port = cluster.cluster_config.kafka_security.client_port()
            )
        );
    }

    #[test]
    fn kraft_controller_pods_change_with_the_replica_count() {
        let three_replicas: Vec<_> = (0..3)
            .map(|replica| pod_descriptor(KafkaRole::Controller, replica, 9093))
            .collect();
        let five_replicas: Vec<_> = (0..5)
            .map(|replica| pod_descriptor(KafkaRole::Controller, replica, 9093))
            .collect();

        // Scaling a controller role group rolls the controllers.
        assert_eq!(kraft_controller_pods(&three_replicas).len(), 3);
        assert_eq!(kraft_controller_pods(&five_replicas).len(), 5);
        assert_ne!(
            kraft_controller_pods(&three_replicas),
            kraft_controller_pods(&five_replicas)
        );
    }
}
