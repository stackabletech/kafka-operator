pub mod listener;

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, Snafu};
use stackable_operator::commons::product_image_selection::ProductImageSelection;
use stackable_operator::memory::to_java_heap;
use stackable_operator::{
    commons::{
        opa::OpaConfig,
        resources::{CpuLimits, MemoryLimits, NoRuntimeLimits, PvcConfig, Resources},
    },
    config::merge::Merge,
    error::OperatorResult,
    k8s_openapi::{
        api::core::v1::{PersistentVolumeClaim, ResourceRequirements},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::{runtime::reflector::ObjectRef, CustomResource},
    product_config_utils::{ConfigError, Configuration},
    role_utils::{Role, RoleGroupRef},
    schemars::{self, JsonSchema},
};
use std::collections::BTreeMap;
use strum::{Display, EnumIter, EnumString};

pub const DOCKER_IMAGE_BASE_NAME: &str = "kafka";
pub const APP_NAME: &str = "kafka";
// ports
pub const CLIENT_PORT_NAME: &str = "kafka";
pub const CLIENT_PORT: u16 = 9092;
pub const SECURE_CLIENT_PORT_NAME: &str = "kafka-tls";
pub const SECURE_CLIENT_PORT: u16 = 9093;
pub const INTERNAL_PORT: u16 = 19092;
pub const SECURE_INTERNAL_PORT: u16 = 19093;
pub const METRICS_PORT_NAME: &str = "metrics";
pub const METRICS_PORT: u16 = 9606;
// config files
pub const SERVER_PROPERTIES_FILE: &str = "server.properties";
// env vars
pub const KAFKA_HEAP_OPTS: &str = "KAFKA_HEAP_OPTS";
// server_properties
pub const LOG_DIRS_VOLUME_NAME: &str = "log-dirs";
// - TLS global
pub const TLS_DEFAULT_SECRET_CLASS: &str = "tls";
pub const SSL_KEYSTORE_LOCATION: &str = "ssl.keystore.location";
pub const SSL_KEYSTORE_PASSWORD: &str = "ssl.keystore.password";
pub const SSL_KEYSTORE_TYPE: &str = "ssl.keystore.type";
pub const SSL_TRUSTSTORE_LOCATION: &str = "ssl.truststore.location";
pub const SSL_TRUSTSTORE_PASSWORD: &str = "ssl.truststore.password";
pub const SSL_TRUSTSTORE_TYPE: &str = "ssl.truststore.type";
pub const SSL_STORE_PASSWORD: &str = "changeit";
// - TLS client
pub const CLIENT_SSL_KEYSTORE_LOCATION: &str = "listener.name.client.ssl.keystore.location";
pub const CLIENT_SSL_KEYSTORE_PASSWORD: &str = "listener.name.client.ssl.keystore.password";
pub const CLIENT_SSL_KEYSTORE_TYPE: &str = "listener.name.client.ssl.keystore.type";
pub const CLIENT_SSL_TRUSTSTORE_LOCATION: &str = "listener.name.client.ssl.truststore.location";
pub const CLIENT_SSL_TRUSTSTORE_PASSWORD: &str = "listener.name.client.ssl.truststore.password";
pub const CLIENT_SSL_TRUSTSTORE_TYPE: &str = "listener.name.client.ssl.truststore.type";
// - TLS client authentication
pub const CLIENT_AUTH_SSL_KEYSTORE_LOCATION: &str =
    "listener.name.client_auth.ssl.keystore.location";
pub const CLIENT_AUTH_SSL_KEYSTORE_PASSWORD: &str =
    "listener.name.client_auth.ssl.keystore.password";
pub const CLIENT_AUTH_SSL_KEYSTORE_TYPE: &str = "listener.name.client_auth.ssl.keystore.type";
pub const CLIENT_AUTH_SSL_TRUSTSTORE_LOCATION: &str =
    "listener.name.client_auth.ssl.truststore.location";
pub const CLIENT_AUTH_SSL_TRUSTSTORE_PASSWORD: &str =
    "listener.name.client_auth.ssl.truststore.password";
pub const CLIENT_AUTH_SSL_TRUSTSTORE_TYPE: &str = "listener.name.client_auth.ssl.truststore.type";
pub const CLIENT_AUTH_SSL_CLIENT_AUTH: &str = "listener.name.client_auth.ssl.client.auth";
// - TLS internal
pub const SECURITY_INTER_BROKER_PROTOCOL: &str = "security.inter.broker.protocol";
pub const INTER_BROKER_LISTENER_NAME: &str = "inter.broker.listener.name";
pub const INTER_SSL_KEYSTORE_LOCATION: &str = "listener.name.internal.ssl.keystore.location";
pub const INTER_SSL_KEYSTORE_PASSWORD: &str = "listener.name.internal.ssl.keystore.password";
pub const INTER_SSL_KEYSTORE_TYPE: &str = "listener.name.internal.ssl.keystore.type";
pub const INTER_SSL_TRUSTSTORE_LOCATION: &str = "listener.name.internal.ssl.truststore.location";
pub const INTER_SSL_TRUSTSTORE_PASSWORD: &str = "listener.name.internal.ssl.truststore.password";
pub const INTER_SSL_TRUSTSTORE_TYPE: &str = "listener.name.internal.ssl.truststore.type";
pub const INTER_SSL_CLIENT_AUTH: &str = "listener.name.internal.ssl.client.auth";
// directories
pub const STACKABLE_TMP_DIR: &str = "/stackable/tmp";
pub const STACKABLE_DATA_DIR: &str = "/stackable/data";
pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";
pub const STACKABLE_TLS_CLIENT_DIR: &str = "/stackable/tls_client";
pub const STACKABLE_TLS_CLIENT_AUTH_DIR: &str = "/stackable/tls_client_auth";
pub const STACKABLE_TLS_INTERNAL_DIR: &str = "/stackable/tls_internal";
pub const SYSTEM_TRUST_STORE_DIR: &str = "/etc/pki/java/cacerts";

const JVM_HEAP_FACTOR: f32 = 0.8;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("could not parse product version from image: [{image_version}]. Expected format e.g. [2.8.0-stackable0.1.0]"))]
    KafkaProductVersion { image_version: String },
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,
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
    pub image: ProductImageSelection,
    pub brokers: Option<Role<KafkaConfig>>,
    pub zookeeper_config_map_name: String,
    pub opa: Option<OpaConfig>,
    pub log4j: Option<String>,
    #[serde(default)]
    pub config: GlobalKafkaConfig,
    pub stopped: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GlobalKafkaConfig {
    /// Only affects client connections. This setting controls:
    /// - If TLS encryption is used at all
    /// - Which cert the servers should use to authenticate themselves against the client
    /// Defaults to `TlsSecretClass` { secret_class: "tls".to_string() }.
    #[serde(
        default = "tls_secret_class_default",
        skip_serializing_if = "Option::is_none"
    )]
    pub tls: Option<TlsSecretClass>,
    /// Only affects client connections. This setting controls:
    /// - If clients need to authenticate themselves against the server via TLS
    /// - Which ca.crt to use when validating the provided client certs
    /// Defaults to `None`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_authentication: Option<ClientAuthenticationClass>,
    /// Only affects internal communication. Use mutual verification between Kafka nodes
    /// This setting controls:
    /// - Which cert the servers should use to authenticate themselves against other servers
    /// - Which ca.crt to use when validating the other server
    #[serde(
        default = "tls_secret_class_default",
        skip_serializing_if = "Option::is_none"
    )]
    pub internal_tls: Option<TlsSecretClass>,
}

impl Default for GlobalKafkaConfig {
    fn default() -> Self {
        GlobalKafkaConfig {
            tls: tls_secret_class_default(),
            client_authentication: None,
            internal_tls: tls_secret_class_default(),
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientAuthenticationClass {
    pub authentication_class: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsSecretClass {
    pub secret_class: String,
}

fn tls_secret_class_default() -> Option<TlsSecretClass> {
    Some(TlsSecretClass {
        secret_class: TLS_DEFAULT_SECRET_CLASS.to_string(),
    })
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

    /// Build the [`PersistentVolumeClaim`]s and [`ResourceRequirements`] for the given `rolegroup_ref`.
    /// These can be defined at the role or rolegroup level and as usual, the
    /// following precedence rules are implemented:
    /// 1. group pvc
    /// 2. role pvc
    /// 3. a default PVC with 1Gi capacity
    pub fn resources(
        &self,
        rolegroup_ref: &RoleGroupRef<KafkaCluster>,
    ) -> (Vec<PersistentVolumeClaim>, ResourceRequirements) {
        let mut role_resources = self.role_resources();
        role_resources.merge(&Self::default_resources());
        let mut resources = self.rolegroup_resources(rolegroup_ref);
        resources.merge(&role_resources);

        let data_pvc = resources
            .storage
            .log_dirs
            .build_pvc(LOG_DIRS_VOLUME_NAME, Some(vec!["ReadWriteOnce"]));
        let pod_resources = resources.clone().into();

        (vec![data_pvc], pod_resources)
    }

    fn rolegroup_resources(
        &self,
        rolegroup_ref: &RoleGroupRef<KafkaCluster>,
    ) -> Resources<Storage, NoRuntimeLimits> {
        let spec: &KafkaClusterSpec = &self.spec;

        spec.brokers
            .as_ref()
            .map(|brokers| &brokers.role_groups)
            .and_then(|role_groups| role_groups.get(&rolegroup_ref.role_group))
            .map(|role_group| role_group.config.config.resources.clone())
            .unwrap_or_default()
    }

    fn role_resources(&self) -> Resources<Storage, NoRuntimeLimits> {
        let spec: &KafkaClusterSpec = &self.spec;
        spec.brokers
            .as_ref()
            .map(|brokers| brokers.config.config.resources.clone())
            .unwrap_or_default()
    }

    fn default_resources() -> Resources<Storage, NoRuntimeLimits> {
        Resources {
            cpu: CpuLimits {
                min: None,
                max: None,
            },
            memory: MemoryLimits {
                limit: None,
                runtime_limits: NoRuntimeLimits {},
            },
            storage: Storage {
                log_dirs: PvcConfig {
                    capacity: Some(Quantity("1Gi".to_owned())),
                    storage_class: None,
                    selectors: None,
                },
            },
        }
    }

    pub fn heap_limits(&self, resources: &ResourceRequirements) -> OperatorResult<Option<String>> {
        resources
            .limits
            .as_ref()
            .and_then(|limits| limits.get("memory"))
            .map(|memory_limit| to_java_heap(memory_limit, JVM_HEAP_FACTOR))
            .transpose()
    }

    /// Returns the product version, e.g. `2.1.0`
    pub fn product_version(&self) -> String {
        self.spec
            .image
            .resolve(DOCKER_IMAGE_BASE_NAME)
            .product_version
    }

    /// Returns the full image name e.g. `docker.stackable.tech/stackable/superset:1.4.1-stackable2.1.0`
    pub fn image(&self) -> String {
        self.spec.image.resolve(DOCKER_IMAGE_BASE_NAME).image
    }

    /// Returns the secret class for client connection encryption. Defaults to `tls`.
    pub fn client_tls_secret_class(&self) -> Option<&TlsSecretClass> {
        let spec: &KafkaClusterSpec = &self.spec;
        spec.config.tls.as_ref()
    }

    /// Returns the authentication class used for client authentication
    pub fn client_authentication_class(&self) -> Option<&str> {
        let spec: &KafkaClusterSpec = &self.spec;
        spec.config
            .client_authentication
            .as_ref()
            .map(|tls| tls.authentication_class.as_ref())
    }

    /// Returns the secret class for internal server encryption.
    pub fn internal_tls_secret_class(&self) -> Option<&TlsSecretClass> {
        let spec: &KafkaClusterSpec = &self.spec;
        spec.config.internal_tls.as_ref()
    }

    /// Returns the client port based on the security (tls) settings.
    pub fn client_port(&self) -> u16 {
        if self.client_tls_secret_class().is_some() || self.client_authentication_class().is_some()
        {
            SECURE_CLIENT_PORT
        } else {
            CLIENT_PORT
        }
    }

    /// Returns the client port name based on the security (tls) settings.
    pub fn client_port_name(&self) -> &str {
        if self.client_tls_secret_class().is_some() || self.client_authentication_class().is_some()
        {
            SECURE_CLIENT_PORT_NAME
        } else {
            CLIENT_PORT_NAME
        }
    }
}

/// Reference to a single `Pod` that is a component of a [`KafkaCluster`]
///
/// Used for service discovery.
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

#[derive(Clone, Debug, Default, Deserialize, Merge, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Storage {
    #[serde(default)]
    pub log_dirs: PvcConfig,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConfig {
    #[serde(default)]
    pub resources: Resources<Storage, NoRuntimeLimits>,
}

impl Configuration for KafkaConfig {
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
            if resource.spec.opa.is_some() {
                config.insert(
                    "authorizer.class.name".to_string(),
                    Some("org.openpolicyagent.kafka.OpaAuthorizer".to_string()),
                );
                config.insert(
                    "opa.authorizer.metrics.enabled".to_string(),
                    Some("true".to_string()),
                );
            }

            // We set either client tls with authentication or client tls without authentication
            // If authentication is explicitly required we do not want to have any other CAs to
            // be trusted.
            if resource.client_authentication_class().is_some() {
                config.insert(
                    CLIENT_AUTH_SSL_KEYSTORE_LOCATION.to_string(),
                    Some(format!("{}/keystore.p12", STACKABLE_TLS_CLIENT_AUTH_DIR)),
                );
                config.insert(
                    CLIENT_AUTH_SSL_KEYSTORE_PASSWORD.to_string(),
                    Some(SSL_STORE_PASSWORD.to_string()),
                );
                config.insert(
                    CLIENT_AUTH_SSL_KEYSTORE_TYPE.to_string(),
                    Some("PKCS12".to_string()),
                );
                config.insert(
                    CLIENT_AUTH_SSL_TRUSTSTORE_LOCATION.to_string(),
                    Some(format!("{}/truststore.p12", STACKABLE_TLS_CLIENT_AUTH_DIR)),
                );
                config.insert(
                    CLIENT_AUTH_SSL_TRUSTSTORE_PASSWORD.to_string(),
                    Some(SSL_STORE_PASSWORD.to_string()),
                );
                config.insert(
                    CLIENT_AUTH_SSL_TRUSTSTORE_TYPE.to_string(),
                    Some("PKCS12".to_string()),
                );
                // client auth required
                config.insert(
                    CLIENT_AUTH_SSL_CLIENT_AUTH.to_string(),
                    Some("required".to_string()),
                );
            } else if resource.client_tls_secret_class().is_some() {
                config.insert(
                    CLIENT_SSL_KEYSTORE_LOCATION.to_string(),
                    Some(format!("{}/keystore.p12", STACKABLE_TLS_CLIENT_DIR)),
                );
                config.insert(
                    CLIENT_SSL_KEYSTORE_PASSWORD.to_string(),
                    Some(SSL_STORE_PASSWORD.to_string()),
                );
                config.insert(
                    CLIENT_SSL_KEYSTORE_TYPE.to_string(),
                    Some("PKCS12".to_string()),
                );
                config.insert(
                    CLIENT_SSL_TRUSTSTORE_LOCATION.to_string(),
                    Some(format!("{}/truststore.p12", STACKABLE_TLS_CLIENT_DIR)),
                );
                config.insert(
                    CLIENT_SSL_TRUSTSTORE_PASSWORD.to_string(),
                    Some(SSL_STORE_PASSWORD.to_string()),
                );
                config.insert(
                    CLIENT_SSL_TRUSTSTORE_TYPE.to_string(),
                    Some("PKCS12".to_string()),
                );
            }

            // Internal TLS
            if resource.internal_tls_secret_class().is_some() {
                config.insert(
                    INTER_SSL_KEYSTORE_LOCATION.to_string(),
                    Some(format!("{}/keystore.p12", STACKABLE_TLS_INTERNAL_DIR)),
                );
                config.insert(
                    INTER_SSL_KEYSTORE_PASSWORD.to_string(),
                    Some(SSL_STORE_PASSWORD.to_string()),
                );
                config.insert(
                    INTER_SSL_KEYSTORE_TYPE.to_string(),
                    Some("PKCS12".to_string()),
                );
                config.insert(
                    INTER_SSL_TRUSTSTORE_LOCATION.to_string(),
                    Some(format!("{}/truststore.p12", STACKABLE_TLS_INTERNAL_DIR)),
                );
                config.insert(
                    INTER_SSL_TRUSTSTORE_PASSWORD.to_string(),
                    Some(SSL_STORE_PASSWORD.to_string()),
                );
                config.insert(
                    INTER_SSL_TRUSTSTORE_TYPE.to_string(),
                    Some("PKCS12".to_string()),
                );
                config.insert(
                    INTER_SSL_CLIENT_AUTH.to_string(),
                    Some("required".to_string()),
                );
            }

            // common
            config.insert(
                INTER_BROKER_LISTENER_NAME.to_string(),
                Some(listener::KafkaListenerName::Internal.to_string()),
            );
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_tls() {
        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          version: abc
          zookeeperConfigMapName: xyz
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            kafka.client_tls_secret_class().unwrap().secret_class,
            TLS_DEFAULT_SECRET_CLASS.to_string()
        );
        assert_eq!(
            kafka.internal_tls_secret_class().unwrap().secret_class,
            TLS_DEFAULT_SECRET_CLASS.to_string()
        );

        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          version: abc
          zookeeperConfigMapName: xyz
          config:
            tls:
              secretClass: simple-kafka-client-tls
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            kafka.client_tls_secret_class().unwrap().secret_class,
            "simple-kafka-client-tls".to_string()
        );
        assert_eq!(
            kafka.internal_tls_secret_class().unwrap().secret_class,
            TLS_DEFAULT_SECRET_CLASS
        );

        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          version: abc
          zookeeperConfigMapName: xyz
          config:
            tls: null
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(kafka.client_tls_secret_class(), None);
        assert_eq!(
            kafka.internal_tls_secret_class().unwrap().secret_class,
            TLS_DEFAULT_SECRET_CLASS.to_string()
        );

        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          version: abc
          zookeeperConfigMapName: xyz
          config:
            internalTls:
              secretClass: simple-kafka-internal-tls
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            kafka.client_tls_secret_class().unwrap().secret_class,
            TLS_DEFAULT_SECRET_CLASS.to_string()
        );
        assert_eq!(
            kafka.internal_tls_secret_class().unwrap().secret_class,
            "simple-kafka-internal-tls"
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
          version: abc
          zookeeperConfigMapName: xyz
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            kafka.internal_tls_secret_class().unwrap().secret_class,
            TLS_DEFAULT_SECRET_CLASS.to_string()
        );
        assert_eq!(
            kafka.client_tls_secret_class().unwrap().secret_class,
            TLS_DEFAULT_SECRET_CLASS
        );

        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          version: abc
          zookeeperConfigMapName: xyz
          config:
            internalTls:
              secretClass: simple-kafka-internal-tls
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            kafka.internal_tls_secret_class().unwrap().secret_class,
            "simple-kafka-internal-tls".to_string()
        );
        assert_eq!(
            kafka.client_tls_secret_class().unwrap().secret_class,
            TLS_DEFAULT_SECRET_CLASS
        );

        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          version: abc
          zookeeperConfigMapName: xyz
          config:
            tls:
              secretClass: simple-kafka-client-tls
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        assert_eq!(
            kafka.internal_tls_secret_class().unwrap().secret_class,
            TLS_DEFAULT_SECRET_CLASS.to_string()
        );
        assert_eq!(
            kafka.client_tls_secret_class().unwrap().secret_class,
            "simple-kafka-client-tls"
        );
    }
}
