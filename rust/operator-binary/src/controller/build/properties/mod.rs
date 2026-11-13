//! Property-file builders for Kafka rolegroup ConfigMaps.

pub mod broker_properties;
pub mod controller_properties;
pub mod listener;
pub mod product_logging;
pub mod security_properties;

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    controller::ValidatedClusterConfig,
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

/// Builds the contents of the `controller.quorum.bootstrap.servers` property: the FQDNs of all
/// KRaft controller pods.
///
/// # Reasons for NOT using the role group's headless service
///
/// * When a new controller pod starts (without metadata) it finds the quorum leader
///   by fetching from a bootstrap server. Kafka treats every entry of this list as
///   one bootstrap node and picks a random entry for each attempt.
///   The headless Service publishes not-ready addresses, so a single Service entry
///   can resolve to the starting pod itself. That connection succeeds, so
///   Kafka NEVER tries another address and the pod never learns the leader (see
///   <https://github.com/stackabletech/kafka-operator/issues/1062>).
/// * With Kerberos, the CONTROLLER listener's acceptor offers the principal `kafka/<pod-fqdn>`,
///   which is also what the Raft voter endpoints advertise. Dialling the headless Service asks for
///   `kafka/<service>` and fails authentication.
///
/// This has a cost: the list changes on every scaling operation of a controller role group, so
/// scaling rolls all controller and broker pods. With OPA, `super.users` lists every pod as well,
/// so those clusters already roll on any scaling operation.
pub(crate) fn kraft_controllers(pod_descriptors: &[KafkaPodDescriptor]) -> Vec<String> {
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

/// The OPA authorizer properties shared by brokers and controllers.
///
/// In KRaft mode, brokers forward requests such as `CreateTopics` to the controllers without
/// authorizing them, so the controllers must run the authorizer too. Empty if OPA is not
/// configured.
///
/// The Kafka nodes themselves are super users, so that traffic between them does not
/// hit OPA. Forwarded requests are still authorized against the original client principal.
pub(crate) fn opa_authorizer_properties(
    cluster_config: &ValidatedClusterConfig,
    pod_descriptors: &[KafkaPodDescriptor],
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
        (
            "super.users".to_string(),
            super_users(
                pod_descriptors,
                cluster_config.kafka_security.has_kerberos_enabled(),
            ),
        ),
    ])
}

/// The `super.users` value: the principals of all Kafka nodes.
///
/// Internal traffic is authenticated with the pod certificates of the internal `SecretClass`
/// (mTLS), whose subject DN is `CN=generated certificate for pod,DC=<pod>,DC=<service>,
/// DC=<namespace>,DC=svc,DC=<cluster domain parts>`, see the secret-operator.
/// With Kerberos, the node principals are mapped to the short name of the service.
///
/// # Why the `CN=generated certificate for pod` entry is required
///
/// The secret-operator always adds this common name to the subject of the pod certificates,
/// regardless of whether domain components are added as well. Kafka's default principal for an
/// mTLS client is the complete subject DN in RFC 2253 notation, which lists the RDNs in reverse
/// order, so the CN is the last component: `DC=local,...,DC=<pod>,CN=generated certificate for pod`.
///
/// Kafka compares each `super.users` entry for exact equality with the principal; there is no
/// support for wildcards or prefixes (the only wildcard Kafka knows is `User:*` in ACLs, which
/// matches all users). An entry without the CN, or with a `*` in place of it, therefore never
/// matches, and the brokers and controllers would be denied access to each other.
///
/// This could only be avoided by rewriting the principal with `ssl.principal.mapping.rules`,
/// which is not done here.
///
/// See the Kafka documentation:
/// - [Authorization and ACLs](https://kafka.apache.org/44/security/authorization-and-acls/)
/// - [`ssl.principal.mapping.rules`](https://kafka.apache.org/44/security/authorization-and-acls/#customizing-ssl-user-name)
fn super_users(pod_descriptors: &[KafkaPodDescriptor], kerberos_enabled: bool) -> String {
    let mut principals: BTreeSet<String> = pod_descriptors
        .iter()
        .map(|desc| {
            // The certificate encodes `CN`, then the `DC`s from the pod up to the cluster domain
            // suffix. Java prints the subject DN in RFC 2253 order, which is reversed.
            let pod_fqdn = desc.pod_fqdn();
            let mut rdns: Vec<String> = vec!["CN=generated certificate for pod".to_string()];
            rdns.extend(pod_fqdn.split('.').map(|dc| format!("DC={dc}")));
            rdns.reverse();
            format!("User:{}", rdns.join(","))
        })
        .collect();

    if kerberos_enabled {
        principals.extend(
            [KafkaRole::Broker, KafkaRole::Controller]
                .iter()
                .map(|role| format!("User:{}", role.kerberos_service_name())),
        );
    }

    principals.into_iter().collect::<Vec<_>>().join(";")
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
    fn super_users_contains_the_pod_certificate_subject_of_every_node() {
        let pod_descriptors = vec![
            pod_descriptor(KafkaRole::Controller, 0, 9093),
            pod_descriptor(KafkaRole::Broker, 1, 9092),
        ];

        assert_eq!(
            super_users(&pod_descriptors, false),
            "User:DC=local,DC=cluster,DC=svc,DC=default,DC=kafka-controller-default-headless,DC=kafka-controller-default-0,CN=generated certificate for pod;\
             User:DC=local,DC=cluster,DC=svc,DC=default,DC=kafka-controller-default-headless,DC=kafka-controller-default-1,CN=generated certificate for pod"
        );
    }

    #[test]
    fn super_users_contains_the_service_name_with_kerberos() {
        let pod_descriptors = vec![pod_descriptor(KafkaRole::Controller, 0, 9093)];

        assert!(super_users(&pod_descriptors, true).ends_with(";User:kafka"));
        assert!(!super_users(&pod_descriptors, false).contains("User:kafka"));
    }

    #[test]
    fn kraft_controllers_lists_individual_controller_pod_fqdns() {
        let mut other_group_pod = pod_descriptor(KafkaRole::Controller, 0, 9093);
        other_group_pod.role_group_statefulset_name = "kafka-controller-other"
            .parse()
            .expect("valid statefulset name");
        other_group_pod.role_group_service_name = "kafka-controller-other-headless"
            .parse()
            .expect("valid service name");

        let pod_descriptors = vec![
            pod_descriptor(KafkaRole::Controller, 1, 9093),
            pod_descriptor(KafkaRole::Controller, 0, 9093),
            other_group_pod,
            // Brokers must be filtered out of the controller quorum bootstrap servers list.
            pod_descriptor(KafkaRole::Broker, 0, 9092),
        ];

        // One entry per controller pod, *not* the role group's headless Service: a single
        // Service entry can resolve to the starting pod itself, which then never finds the
        // leader (#1062). Under Kerberos the pod FQDN is also the only acceptable GSSAPI
        // service principal.
        assert_eq!(
            kraft_controllers(&pod_descriptors),
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

    #[test]
    fn kraft_controllers_grows_with_the_replica_count() {
        let three_replicas: Vec<_> = (0..3)
            .map(|replica| pod_descriptor(KafkaRole::Controller, replica, 9093))
            .collect();
        let five_replicas: Vec<_> = (0..5)
            .map(|replica| pod_descriptor(KafkaRole::Controller, replica, 9093))
            .collect();

        // Deliberate consequence of per-pod addressing: the list changes with the replica count,
        // so scaling a controller role group rolls the controller and broker pods.
        assert_eq!(kraft_controllers(&three_replicas).len(), 3);
        assert_eq!(kraft_controllers(&five_replicas).len(), 5);
    }
}
