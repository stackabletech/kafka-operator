pub mod affinity;
pub mod authentication;
pub mod authorization;
pub mod listener;
pub mod role;
pub mod security;
pub mod tls;

use std::collections::{BTreeMap, HashMap};

use authentication::KafkaAuthentication;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, Snafu};
use stackable_operator::{
    commons::{
        cluster_operation::ClusterOperation, networking::DomainName,
        product_image_selection::ProductImage,
    },
    kube::{CustomResource, runtime::reflector::ObjectRef},
    role_utils::{GenericRoleConfig, JavaCommonConfig, Role, RoleGroupRef},
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
    utils::cluster_info::KubernetesClusterInfo,
    versioned::versioned,
};
use strum::{Display, EnumIter, EnumString};

use crate::{
    config::node_id_hasher::node_id_hash32_offset,
    crd::{
        authorization::KafkaAuthorization,
        role::{KafkaRole, broker::BrokerConfigFragment, controller::ControllerConfigFragment},
        tls::KafkaTls,
    },
};

pub const DOCKER_IMAGE_BASE_NAME: &str = "kafka";
pub const APP_NAME: &str = "kafka";
pub const OPERATOR_NAME: &str = "kafka.stackable.tech";
// metrics
pub const METRICS_PORT_NAME: &str = "metrics";
pub const METRICS_PORT: u16 = 9606;
// config files
pub const JVM_SECURITY_PROPERTIES_FILE: &str = "security.properties";
// env vars
pub const KAFKA_HEAP_OPTS: &str = "KAFKA_HEAP_OPTS";
// server_properties
pub const LOG_DIRS_VOLUME_NAME: &str = "log-dirs";
// directories
pub const LISTENER_BROKER_VOLUME_NAME: &str = "listener-broker";
pub const LISTENER_BOOTSTRAP_VOLUME_NAME: &str = "listener-bootstrap";
pub const STACKABLE_LISTENER_BROKER_DIR: &str = "/stackable/listener-broker";
pub const STACKABLE_LISTENER_BOOTSTRAP_DIR: &str = "/stackable/listener-bootstrap";
pub const STACKABLE_DATA_DIR: &str = "/stackable/data";
pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";
// kerberos
pub const STACKABLE_KERBEROS_DIR: &str = "/stackable/kerberos";
pub const STACKABLE_KERBEROS_KRB5_PATH: &str = "/stackable/kerberos/krb5.conf";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("The Kafka role [{role}] is missing from spec"))]
    MissingRole { role: String },

    #[snafu(display("Object has no namespace associated"))]
    NoNamespace,

    #[snafu(display(
        "Kafka version 4 and higher requires a Kraft controller (configured via `spec.controller`)"
    ))]
    Kafka4RequiresKraft,

    #[snafu(display(
        "Kraft controller (`spec.controller`) and ZooKeeper (`spec.clusterConfig.zookeeperConfigMapName`) are configured. Please only choose one"
    ))]
    KraftAndZookeeperConfigured,

    #[snafu(display(
        "Could not calculate 'node.id' hash offset for role '{role}' and rolegroup '{rolegroup}' which collides with role '{coliding_role}' and rolegroup '{colliding_rolegroup}'. Please try to rename one of the rolegroups."
    ))]
    KafkaNodeIdHashCollision {
        role: KafkaRole,
        rolegroup: String,
        coliding_role: KafkaRole,
        colliding_rolegroup: String,
    },
}

#[versioned(
    version(name = "v1alpha1"),
    crates(
        kube_core = "stackable_operator::kube::core",
        kube_client = "stackable_operator::kube::client",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars",
        versioned = "stackable_operator::versioned"
    )
)]
pub mod versioned {
    /// A Kafka cluster stacklet. This resource is managed by the Stackable operator for Apache Kafka.
    /// Find more information on how to use it and the resources that the operator generates in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/kafka/).
    #[versioned(crd(
        group = "kafka.stackable.tech",
        plural = "kafkaclusters",
        status = "KafkaClusterStatus",
        shortname = "kafka",
        namespaced
    ))]
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct KafkaClusterSpec {
        // no doc - docs in ProductImage struct.
        pub image: ProductImage,

        // no doc - docs in Role struct.
        pub brokers: Option<Role<BrokerConfigFragment, GenericRoleConfig, JavaCommonConfig>>,

        // no doc - docs in Role struct.
        pub controllers:
            Option<Role<ControllerConfigFragment, GenericRoleConfig, JavaCommonConfig>>,

        /// Kafka settings that affect all roles and role groups.
        ///
        /// The settings in the `clusterConfig` are cluster wide settings that do not need to be configurable at role or role group level.
        #[serde(default)]
        pub cluster_config: v1alpha1::KafkaClusterConfig,

        // no doc - docs in ClusterOperation struct.
        #[serde(default)]
        pub cluster_operation: ClusterOperation,
    }

    #[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct KafkaClusterConfig {
        /// Authentication class settings for Kafka like mTLS authentication.
        #[serde(default)]
        pub authentication: Vec<KafkaAuthentication>,

        /// Authorization settings for Kafka like OPA.
        #[serde(default)]
        pub authorization: KafkaAuthorization,

        /// TLS encryption settings for Kafka (server, internal).
        #[serde(
            default = "tls::default_kafka_tls",
            skip_serializing_if = "Option::is_none"
        )]
        pub tls: Option<KafkaTls>,

        /// Name of the Vector aggregator [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery).
        /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
        /// Follow the [logging tutorial](DOCS_BASE_URL_PLACEHOLDER/tutorials/logging-vector-aggregator)
        /// to learn how to configure log aggregation with Vector.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub vector_aggregator_config_map_name: Option<String>,

        /// Provide the name of the ZooKeeper [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery)
        /// here. When using the [Stackable operator for Apache ZooKeeper](DOCS_BASE_URL_PLACEHOLDER/zookeeper/)
        /// to deploy a ZooKeeper cluster, this will simply be the name of your ZookeeperCluster resource.
        /// This can only be used up to Kafka version 3.9.x. Since Kafka 4.0.0, ZooKeeper support was dropped.
        /// Please use the 'controller' role instead.
        pub zookeeper_config_map_name: Option<String>,

        #[serde(default = "default_metadata_manager")]
        pub metadata_manager: MetadataManager,
    }
}

impl Default for v1alpha1::KafkaClusterConfig {
    fn default() -> Self {
        Self {
            authentication: vec![],
            authorization: KafkaAuthorization::default(),
            tls: tls::default_kafka_tls(),
            vector_aggregator_config_map_name: None,
            zookeeper_config_map_name: None,
            metadata_manager: default_metadata_manager(),
        }
    }
}

impl HasStatusCondition for v1alpha1::KafkaCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

impl v1alpha1::KafkaCluster {
    pub fn is_kraft_mode(&self) -> bool {
        self.spec.cluster_config.metadata_manager == MetadataManager::KRaft
    }

    /// The Kafka cluster id when running in Kraft mode.
    ///
    /// In ZooKeeper mode the cluster id is a UUID generated by Kafka its self and users typically
    /// do not need to deal with it.
    ///
    /// When in Kraft mode, the cluster id is passed on an as the environment variable `KAFKA_CLUSTER_ID`.
    ///
    /// When migrating to Kraft mode, users *must* set this variable via `envOverrides` to the value
    /// found in the `cluster/id` ZooKeeper node or in the `meta.properties` file.
    ///
    /// For freshly installed clusters, users do not need to deal with the cluster id.
    pub fn cluster_id(&self) -> Option<&str> {
        match self.spec.cluster_config.metadata_manager {
            MetadataManager::KRaft => self.metadata.name.as_deref(),
            _ => None,
        }
    }

    /// The name of the load-balanced Kubernetes Service providing the bootstrap address. Kafka clients will use this
    /// to get a list of broker addresses and will use those to transmit data to the correct broker.
    pub fn bootstrap_service_name(&self, rolegroup: &RoleGroupRef<Self>) -> String {
        format!("{}-bootstrap", rolegroup.object_name())
    }

    /// Metadata about a rolegroup
    pub fn rolegroup_ref(
        &self,
        role: &KafkaRole,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<Self> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: role.to_string(),
            role_group: group_name.into(),
        }
    }

    pub fn role_config(&self, role: &KafkaRole) -> Option<&GenericRoleConfig> {
        match role {
            KafkaRole::Broker => self.spec.brokers.as_ref().map(|b| &b.role_config),
            KafkaRole::Controller => self.spec.controllers.as_ref().map(|b| &b.role_config),
        }
    }

    pub fn broker_role(
        &self,
    ) -> Result<&Role<BrokerConfigFragment, GenericRoleConfig, JavaCommonConfig>, Error> {
        self.spec.brokers.as_ref().context(MissingRoleSnafu {
            role: KafkaRole::Broker.to_string(),
        })
    }

    pub fn controller_role(
        &self,
    ) -> Result<&Role<ControllerConfigFragment, GenericRoleConfig, JavaCommonConfig>, Error> {
        self.spec.controllers.as_ref().context(MissingRoleSnafu {
            role: KafkaRole::Controller.to_string(),
        })
    }

    /// List pod descriptors for a given role and all its rolegroups.
    /// If no role is provided, pod descriptors for all roles (and all groups) are listed.
    /// We try to predict the pods here rather than looking at the current cluster state in order to
    /// avoid instance churn.
    pub fn pod_descriptors(
        &self,
        requested_kafka_role: Option<&KafkaRole>,
        cluster_info: &KubernetesClusterInfo,
        client_port: u16,
    ) -> Result<Vec<KafkaPodDescriptor>, Error> {
        let namespace = self.metadata.namespace.clone().context(NoNamespaceSnafu)?;
        let mut pod_descriptors = Vec::new();
        let mut seen_hashes = HashMap::<u32, (KafkaRole, String)>::new();

        for current_role in KafkaRole::roles() {
            let rolegroup_replicas = self.extract_rolegroup_replicas(&current_role)?;
            for (rolegroup, replicas) in rolegroup_replicas {
                let rolegroup_ref = self.rolegroup_ref(&current_role, &rolegroup);
                let node_id_hash_offset = node_id_hash32_offset(&rolegroup_ref);

                // check collisions
                match seen_hashes.get(&node_id_hash_offset) {
                    Some((coliding_role, coliding_rolegroup)) => {
                        return KafkaNodeIdHashCollisionSnafu {
                            role: current_role.clone(),
                            rolegroup: rolegroup.clone(),
                            coliding_role: coliding_role.clone(),
                            colliding_rolegroup: coliding_rolegroup.to_string(),
                        }
                        .fail();
                    }
                    None => {
                        seen_hashes.insert(node_id_hash_offset, (current_role.clone(), rolegroup))
                    }
                };

                // If no specific role is requested, or the current role matches the requested one, add pod descriptors
                if requested_kafka_role.is_none() || Some(&current_role) == requested_kafka_role {
                    for replica in 0..replicas {
                        pod_descriptors.push(KafkaPodDescriptor {
                            namespace: namespace.clone(),
                            role: current_role.to_string(),
                            role_group_service_name: rolegroup_ref
                                .rolegroup_headless_service_name(),
                            role_group_statefulset_name: rolegroup_ref.object_name(),
                            replica,
                            cluster_domain: cluster_info.cluster_domain.clone(),
                            node_id: node_id_hash_offset + u32::from(replica),
                            client_port,
                        });
                    }
                }
            }
        }

        Ok(pod_descriptors)
    }

    fn extract_rolegroup_replicas(
        &self,
        kafka_role: &KafkaRole,
    ) -> Result<BTreeMap<String, u16>, Error> {
        Ok(match kafka_role {
            KafkaRole::Broker => self
                .broker_role()
                .iter()
                .flat_map(|role| &role.role_groups)
                .flat_map(|(rolegroup_name, rolegroup)| {
                    std::iter::once((rolegroup_name.to_string(), rolegroup.replicas.unwrap_or(0)))
                })
                // Order rolegroups consistently, to avoid spurious downstream rewrites
                .collect::<BTreeMap<_, _>>(),

            KafkaRole::Controller => self
                .controller_role()
                .iter()
                .flat_map(|role| &role.role_groups)
                .flat_map(|(rolegroup_name, rolegroup)| {
                    std::iter::once((rolegroup_name.to_string(), rolegroup.replicas.unwrap_or(0)))
                })
                // Order rolegroups consistently, to avoid spurious downstream rewrites
                .collect::<BTreeMap<_, _>>(),
        })
    }
}

/// Reference to a single `Pod` that is a component of a [`KafkaCluster`]
///
/// Used for service discovery.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct KafkaPodDescriptor {
    namespace: String,
    role_group_statefulset_name: String,
    role_group_service_name: String,
    replica: u16,
    cluster_domain: DomainName,
    node_id: u32,
    pub role: String,
    pub client_port: u16,
}

impl KafkaPodDescriptor {
    /// Return the fully qualified domain name
    /// Format: `<pod-name>.<service>.<namespace>.svc.<cluster-domain>`
    pub fn fqdn(&self) -> String {
        format!(
            "{pod_name}.{service_name}.{namespace}.svc.{cluster_domain}",
            pod_name = self.pod_name(),
            service_name = self.role_group_service_name,
            namespace = self.namespace,
            cluster_domain = self.cluster_domain
        )
    }

    pub fn pod_name(&self) -> String {
        format!("{}-{}", self.role_group_statefulset_name, self.replica)
    }

    /// Build the Kraft voter String
    /// See: <https://kafka.apache.org/40/documentation.html#kraft_storage_voters>
    /// Example: 0@controller-0:1234:0000000000-00000000000
    ///   * 0 is the replica id
    ///   * 0000000000-00000000000 is the replica directory id (even though the used Uuid states to be type 4 it does not work)
    ///     See: <https://github.com/apache/kafka/blob/c5169ca805bd03d870a5bcd49744dcc34891cf15/clients/src/main/java/org/apache/kafka/common/Uuid.java#L29>
    ///   * controller-0 is the replica's host,
    ///   * 1234 is the replica's port.
    // NOTE(@maltesander): Even though the used Uuid states to be type 4 it does not work... 0000000000-00000000000 works...
    pub fn as_voter(&self) -> String {
        format!(
            "{node_id}@{fqdn}:{port}:0000000000-{node_id:0>11}",
            node_id = self.node_id,
            port = self.client_port,
            fqdn = self.fqdn(),
        )
    }

    pub fn as_quorum_voter(&self) -> String {
        format!(
            "{node_id}@{fqdn}:{port}",
            node_id = self.node_id,
            port = self.client_port,
            fqdn = self.fqdn(),
        )
    }
}

#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    JsonSchema,
    PartialEq,
    Serialize,
    EnumString,
)]
#[serde(rename_all = "lowercase")]
pub enum MetadataManager {
    ZooKeeper,
    KRaft,
}

fn default_metadata_manager() -> MetadataManager {
    MetadataManager::ZooKeeper
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_server_secret_class(kafka: &v1alpha1::KafkaCluster) -> Option<String> {
        kafka
            .spec
            .cluster_config
            .tls
            .as_ref()
            .and_then(|tls| tls.server_secret_class.clone())
    }

    fn get_internal_secret_class(kafka: &v1alpha1::KafkaCluster) -> String {
        kafka
            .spec
            .cluster_config
            .tls
            .as_ref()
            .unwrap()
            .internal_secret_class
            .clone()
    }

    #[test]
    fn test_client_tls() {
        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          image:
            productVersion: 3.9.1
          clusterConfig:
            zookeeperConfigMapName: xyz
        "#;
        let kafka: v1alpha1::KafkaCluster =
            serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(get_server_secret_class(&kafka), tls::server_tls_default());
        assert_eq!(
            get_internal_secret_class(&kafka),
            tls::internal_tls_default()
        );

        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          image:
            productVersion: 3.9.1
          clusterConfig:
            tls:
              serverSecretClass: simple-kafka-server-tls
            zookeeperConfigMapName: xyz

        "#;
        let kafka: v1alpha1::KafkaCluster =
            serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            get_server_secret_class(&kafka).unwrap(),
            "simple-kafka-server-tls".to_string()
        );
        assert_eq!(
            get_internal_secret_class(&kafka),
            tls::internal_tls_default()
        );

        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          image:
            productVersion: 3.9.1
          clusterConfig:
            tls:
              serverSecretClass: null
            zookeeperConfigMapName: xyz
        "#;
        let kafka: v1alpha1::KafkaCluster =
            serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(get_server_secret_class(&kafka), None);
        assert_eq!(
            get_internal_secret_class(&kafka),
            tls::internal_tls_default()
        );

        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          image:
            productVersion: 3.9.1
          zookeeperConfigMapName: xyz
          clusterConfig:
            tls:
              internalSecretClass: simple-kafka-internal-tls
            zookeeperConfigMapName: xyz
        "#;
        let kafka: v1alpha1::KafkaCluster =
            serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(get_server_secret_class(&kafka), tls::server_tls_default());
        assert_eq!(
            get_internal_secret_class(&kafka),
            "simple-kafka-internal-tls".to_string()
        );
    }

    #[test]
    fn test_internal_tls() {
        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          image:
            productVersion: 3.9.1
          clusterConfig:
            zookeeperConfigMapName: xyz
        "#;
        let kafka: v1alpha1::KafkaCluster =
            serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(get_server_secret_class(&kafka), tls::server_tls_default());
        assert_eq!(
            get_internal_secret_class(&kafka),
            tls::internal_tls_default()
        );

        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          image:
            productVersion: 3.9.1
          clusterConfig:
            tls:
              internalSecretClass: simple-kafka-internal-tls
            zookeeperConfigMapName: xyz
        "#;
        let kafka: v1alpha1::KafkaCluster =
            serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(get_server_secret_class(&kafka), tls::server_tls_default());
        assert_eq!(
            get_internal_secret_class(&kafka),
            "simple-kafka-internal-tls".to_string()
        );

        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          image:
            productVersion: 3.9.1
          clusterConfig:
            tls:
              serverSecretClass: simple-kafka-server-tls
            zookeeperConfigMapName: xyz
        "#;
        let kafka: v1alpha1::KafkaCluster =
            serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            get_server_secret_class(&kafka),
            Some("simple-kafka-server-tls".to_string())
        );
        assert_eq!(
            get_internal_secret_class(&kafka),
            tls::internal_tls_default()
        );
    }
}
