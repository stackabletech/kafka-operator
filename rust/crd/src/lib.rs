pub mod authentication;
pub mod authorization;
pub mod listener;
pub mod security;
pub mod tls;

use crate::authentication::KafkaAuthentication;
use crate::authorization::KafkaAuthorization;
use crate::tls::KafkaTls;

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, Snafu};
use stackable_operator::{
    commons::{
        product_image_selection::ProductImage,
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            PvcConfig, PvcConfigFragment, Resources, ResourcesFragment,
        },
    },
    config::fragment::{Fragment, ValidationError},
    config::merge::Merge,
    k8s_openapi::{
        api::core::v1::PersistentVolumeClaim, apimachinery::pkg::api::resource::Quantity,
    },
    kube::{runtime::reflector::ObjectRef, CustomResource},
    product_config_utils::{ConfigError, Configuration},
    role_utils::{Role, RoleGroup, RoleGroupRef},
    schemars::{self, JsonSchema},
};
use std::collections::BTreeMap;
use strum::{Display, EnumIter, EnumString};

pub const DOCKER_IMAGE_BASE_NAME: &str = "kafka";
pub const APP_NAME: &str = "kafka";
pub const OPERATOR_NAME: &str = "kafka.stackable.tech";
// metrics
pub const METRICS_PORT_NAME: &str = "metrics";
pub const METRICS_PORT: u16 = 9606;
// config files
pub const SERVER_PROPERTIES_FILE: &str = "server.properties";
// env vars
pub const KAFKA_HEAP_OPTS: &str = "KAFKA_HEAP_OPTS";
// server_properties
pub const LOG_DIRS_VOLUME_NAME: &str = "log-dirs";
// directories
pub const STACKABLE_TMP_DIR: &str = "/stackable/tmp";
pub const STACKABLE_DATA_DIR: &str = "/stackable/data";
pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,
    #[snafu(display("failed to validate config of rolegroup {rolegroup}"))]
    RoleGroupValidation {
        rolegroup: RoleGroupRef<KafkaCluster>,
        source: ValidationError,
    },
}

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "kafka.stackable.tech",
    version = "v1alpha1",
    kind = "KafkaCluster",
    plural = "kafkaclusters",
    shortname = "kafka",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterSpec {
    pub image: ProductImage,
    pub brokers: Option<Role<KafkaConfigFragment>>,
    pub cluster_config: KafkaClusterConfig,
    pub stopped: Option<bool>,
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
    /// Log4j configuration
    pub log4j: Option<String>,
    /// TLS encryption settings for Kafka (server, internal).
    #[serde(
        default = "tls::default_kafka_tls",
        skip_serializing_if = "Option::is_none"
    )]
    pub tls: Option<KafkaTls>,
    /// ZooKeeper discovery config map name.
    pub zookeeper_config_map_name: String,
}

impl KafkaCluster {
    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn broker_role_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }

    /// Metadata about a broker rolegroup
    pub fn broker_rolegroup_ref(
        &self,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<KafkaCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: KafkaRole::Broker.to_string(),
            role_group: group_name.into(),
        }
    }

    pub fn rolegroup(
        &self,
        rolegroup_ref: &RoleGroupRef<KafkaCluster>,
    ) -> Option<(&Role<KafkaConfigFragment>, &RoleGroup<KafkaConfigFragment>)> {
        match rolegroup_ref.role.parse().ok()? {
            KafkaRole::Broker => {
                let role = &self.spec.brokers.as_ref()?;
                let rg = role.role_groups.get(&rolegroup_ref.role_group)?;
                Some((role, rg))
            }
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

    pub fn broker_default_config() -> KafkaConfigFragment {
        KafkaConfigFragment {
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("500m".to_owned())),
                    max: Some(Quantity("4".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("2Gi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: StorageFragment {
                    log_dirs: PvcConfigFragment {
                        capacity: Some(Quantity("1Gi".to_owned())),
                        storage_class: None,
                        selectors: None,
                    },
                },
            },
        }
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
    pub fn fqdn(&self) -> String {
        format!(
            "{}.{}.{}.svc.cluster.local",
            self.pod_name, self.role_group_service_name, self.namespace
        )
    }
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
pub enum KafkaRole {
    #[strum(serialize = "broker")]
    Broker,
}

#[derive(Clone, Debug, Default, PartialEq, Fragment, JsonSchema)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        JsonSchema,
        Merge,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct Storage {
    #[fragment_attrs(serde(default))]
    pub log_dirs: PvcConfig,
}

impl Storage {
    pub fn build_pvcs(&self) -> Vec<PersistentVolumeClaim> {
        let data_pvc = self
            .log_dirs
            .build_pvc(LOG_DIRS_VOLUME_NAME, Some(vec!["ReadWriteOnce"]));
        vec![data_pvc]
    }
}

#[derive(Debug, Default, PartialEq, Fragment, JsonSchema)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        JsonSchema,
        Merge,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct KafkaConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<Storage, NoRuntimeLimits>,
}

impl Configuration for KafkaConfigFragment {
    type Configurable = KafkaCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        resource: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut config = BTreeMap::new();

        if file == SERVER_PROPERTIES_FILE {
            // OPA
            if resource.spec.cluster_config.authorization.opa.is_some() {
                config.insert(
                    "authorizer.class.name".to_string(),
                    Some("org.openpolicyagent.kafka.OpaAuthorizer".to_string()),
                );
                config.insert(
                    "opa.authorizer.metrics.enabled".to_string(),
                    Some("true".to_string()),
                );
            }
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_server_secret_class(kafka: &KafkaCluster) -> Option<String> {
        kafka
            .spec
            .cluster_config
            .tls
            .as_ref()
            .and_then(|tls| tls.server_secret_class.clone())
    }

    fn get_internal_secret_class(kafka: &KafkaCluster) -> String {
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
            productVersion: 3.3.1
            stackableVersion: "23.4.0-rc1"
          clusterConfig:  
            zookeeperConfigMapName: xyz
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
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
            productVersion: 3.3.1
            stackableVersion: "23.4.0-rc1"
          clusterConfig:
            tls:
              serverSecretClass: simple-kafka-server-tls  
            zookeeperConfigMapName: xyz
          
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
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
            productVersion: 3.3.1
            stackableVersion: "23.4.0-rc1"
          clusterConfig:
            tls:
              serverSecretClass: null  
            zookeeperConfigMapName: xyz            
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
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
            productVersion: 3.3.1
            stackableVersion: "23.4.0-rc1"
          zookeeperConfigMapName: xyz
          clusterConfig:
            tls:
              internalSecretClass: simple-kafka-internal-tls  
            zookeeperConfigMapName: xyz          
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
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
            productVersion: 3.3.1
            stackableVersion: "23.4.0-rc1"
          clusterConfig:
            zookeeperConfigMapName: xyz              
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
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
            productVersion: 3.3.1
            stackableVersion: "23.4.0-rc1"
          clusterConfig:
            tls:
              internalSecretClass: simple-kafka-internal-tls  
            zookeeperConfigMapName: xyz              
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
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
            productVersion: 3.3.1
            stackableVersion: "23.4.0-rc1"
          clusterConfig:
            tls:
              serverSecretClass: simple-kafka-server-tls  
            zookeeperConfigMapName: xyz              
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
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
