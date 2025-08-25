pub mod affinity;
pub mod authentication;
pub mod authorization;
pub mod listener;
pub mod role;
pub mod security;
pub mod tls;

use std::{collections::BTreeMap, str::FromStr};

use authentication::KafkaAuthentication;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::{cluster_operation::ClusterOperation, product_image_selection::ProductImage},
    config::{
        fragment::{self, ValidationError},
        merge::Merge,
    },
    kube::{CustomResource, ResourceExt, runtime::reflector::ObjectRef},
    role_utils::{GenericRoleConfig, JavaCommonConfig, Role, RoleGroup, RoleGroupRef},
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
    utils::cluster_info::KubernetesClusterInfo,
    versioned::versioned,
};

use crate::crd::{
    authorization::KafkaAuthorization,
    role::{
        KafkaRole,
        broker::{BrokerConfig, BrokerConfigFragment},
        controller::ControllerConfigFragment,
    },
    tls::KafkaTls,
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
pub const STACKABLE_LOG_CONFIG_DIR: &str = "/stackable/log_config";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
// kerberos
pub const STACKABLE_KERBEROS_DIR: &str = "/stackable/kerberos";
pub const STACKABLE_KERBEROS_KRB5_PATH: &str = "/stackable/kerberos/krb5.conf";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,

    #[snafu(display("failed to validate config of rolegroup {rolegroup}"))]
    RoleGroupValidation {
        rolegroup: RoleGroupRef<v1alpha1::KafkaCluster>,
        source: ValidationError,
    },

    #[snafu(display("the Kafka role [{role}] is missing from spec"))]
    MissingKafkaRole { role: String },

    #[snafu(display("the role {role} is not defined"))]
    CannotRetrieveKafkaRole { role: String },

    #[snafu(display("the Kafka node role group [{role_group}] is missing from spec"))]
    MissingKafkaRoleGroup { role_group: String },

    #[snafu(display("the role group {role_group} is not defined"))]
    CannotRetrieveKafkaRoleGroup { role_group: String },

    #[snafu(display("unknown role {role}. Should be one of {roles:?}"))]
    UnknownKafkaRole {
        source: strum::ParseError,
        role: String,
        roles: Vec<String>,
    },

    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },
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
        /// This can only be used up to Kafka version 3.9.x. Since Kafka 4.0.0, ZooKeeper suppport was dropped.
        /// Please use the 'controller' role instead.
        pub zookeeper_config_map_name: Option<String>,
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
    /// The name of the load-balanced Kubernetes Service providing the bootstrap address. Kafka clients will use this
    /// to get a list of broker addresses and will use those to transmit data to the correct broker.
    pub fn bootstrap_service_name(&self, rolegroup: &RoleGroupRef<Self>) -> String {
        format!("{}-bootstrap", rolegroup.object_name())
    }

    /// Metadata about a broker rolegroup
    pub fn broker_rolegroup_ref(&self, group_name: impl Into<String>) -> RoleGroupRef<Self> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: KafkaRole::Broker.to_string(),
            role_group: group_name.into(),
        }
    }

    pub fn role(
        &self,
        role: &KafkaRole,
    ) -> Result<&Role<BrokerConfigFragment, GenericRoleConfig, JavaCommonConfig>, Error> {
        match role {
            KafkaRole::Broker => self.spec.brokers.as_ref(),
            KafkaRole::Controller => todo!(),
        }
        .with_context(|| CannotRetrieveKafkaRoleSnafu {
            role: role.to_string(),
        })
    }

    pub fn rolegroup(
        &self,
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> Result<&RoleGroup<BrokerConfigFragment, JavaCommonConfig>, Error> {
        let role_variant =
            KafkaRole::from_str(&rolegroup_ref.role).with_context(|_| UnknownKafkaRoleSnafu {
                role: rolegroup_ref.role.to_owned(),
                roles: KafkaRole::roles(),
            })?;

        let role = self.role(&role_variant)?;
        role.role_groups
            .get(&rolegroup_ref.role_group)
            .with_context(|| CannotRetrieveKafkaRoleGroupSnafu {
                role_group: rolegroup_ref.role_group.to_owned(),
            })
    }

    pub fn role_config(&self, role: &KafkaRole) -> Option<&GenericRoleConfig> {
        match role {
            KafkaRole::Broker => self.spec.brokers.as_ref().map(|b| &b.role_config),
            KafkaRole::Controller => self.spec.controllers.as_ref().map(|b| &b.role_config),
        }
    }

    /// List all pods expected to form the cluster
    ///
    /// We try to predict the pods here rather than looking at the current cluster state in order to
    /// avoid instance churn.
    pub fn pods(&self) -> Result<impl Iterator<Item = KafkaPodRef> + '_, Error> {
        let ns = self.metadata.namespace.clone().context(NoNamespaceSnafu)?;
        Ok(self
            .spec
            .brokers
            .iter()
            .flat_map(|role| &role.role_groups)
            // Order rolegroups consistently, to avoid spurious downstream rewrites
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .flat_map(move |(rolegroup_name, rolegroup)| {
                let rolegroup_ref = self.broker_rolegroup_ref(rolegroup_name);
                let ns = ns.clone();
                (0..rolegroup.replicas.unwrap_or(0)).map(move |i| KafkaPodRef {
                    namespace: ns.clone(),
                    role_group_service_name: rolegroup_ref.object_name(),
                    pod_name: format!("{}-{}", rolegroup_ref.object_name(), i),
                })
            }))
    }

    /// Retrieve and merge resource configs for role and role groups
    pub fn merged_config(
        &self,
        role: &KafkaRole,
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> Result<BrokerConfig, Error> {
        // Initialize the result with all default values as baseline
        let conf_defaults = BrokerConfig::default_config(&self.name_any(), &role.to_string());

        // Retrieve role resource config
        let role = self.role(role)?;
        let mut conf_role = role.config.config.to_owned();

        // Retrieve rolegroup specific resource config
        let role_group = self.rolegroup(rolegroup_ref)?;
        let mut conf_role_group = role_group.config.config.to_owned();

        // Merge more specific configs into default config
        // Hierarchy is:
        // 1. RoleGroup
        // 2. Role
        // 3. Default
        conf_role.merge(&conf_defaults);
        conf_role_group.merge(&conf_role);

        tracing::debug!("Merged config: {:?}", conf_role_group);
        fragment::validate(conf_role_group).context(FragmentValidationFailureSnafu)
    }
}

/// Reference to a single `Pod` that is a component of a [`KafkaCluster`]
///
/// Used for service discovery.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct KafkaPodRef {
    pub namespace: String,
    pub role_group_service_name: String,
    pub pod_name: String,
}

impl KafkaPodRef {
    pub fn fqdn(&self, cluster_info: &KubernetesClusterInfo) -> String {
        format!(
            "{pod_name}.{service_name}.{namespace}.svc.{cluster_domain}",
            pod_name = self.pod_name,
            service_name = self.role_group_service_name,
            namespace = self.namespace,
            cluster_domain = cluster_info.cluster_domain
        )
    }
}

#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
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
            productVersion: 3.7.2
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
            productVersion: 3.7.2
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
            productVersion: 3.7.2
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
            productVersion: 3.7.2
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
            productVersion: 3.7.2
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
            productVersion: 3.7.2
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
            productVersion: 3.7.2
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
