use serde::{Deserialize, Serialize};
use snafu::{OptionExt, Snafu};
use stackable_operator::error::OperatorResult;
use stackable_operator::memory::to_java_heap;
use stackable_operator::{
    commons::{
        opa::OpaConfig,
        resources::{CpuLimits, MemoryLimits, NoRuntimeLimits, PvcConfig, Resources},
    },
    config::merge::Merge,
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

pub const APP_NAME: &str = "kafka";
// ports
pub const CLIENT_PORT_NAME: &str = "http";
pub const CLIENT_PORT: u16 = 9092;
pub const SECURE_CLIENT_PORT_NAME: &str = "https";
pub const SECURE_CLIENT_PORT: u16 = 9093;
pub const METRICS_PORT_NAME: &str = "metrics";
pub const METRICS_PORT: u16 = 9606;
// config files
pub const SERVER_PROPERTIES_FILE: &str = "server.properties";
// env vars
pub const KAFKA_HEAP_OPTS: &str = "KAFKA_HEAP_OPTS";
// server_properties
pub const LOG_DIRS_VOLUME_NAME: &str = "log-dirs";
// TLS
pub const TLS_DEFAULT_SECRET_CLASS: &str = "tls";
pub const SSL_KEYSTORE_LOCATION: &str = "ssl.keystore.location";
pub const SSL_KEYSTORE_PASSWORD: &str = "ssl.keystore.password";
pub const SSL_KEYSTORE_TYPE: &str = "ssl.keystore.type";
pub const SSL_TRUSTSTORE_LOCATION: &str = "ssl.truststore.location";
pub const SSL_TRUSTSTORE_PASSWORD: &str = "ssl.truststore.password";
pub const SSL_TRUSTSTORE_TYPE: &str = "ssl.truststore.type";
pub const SSL_STORE_PASSWORD: &str = "changeit";
pub const SSL_CLIENT_AUTH: &str = "ssl.client.auth";
// TLS internal
pub const SECURITY_INTER_BROKER_PROTOCOL: &str = "security.inter.broker.protocol";
// directories
pub const STACKABLE_TMP_DIR: &str = "/stackable/tmp";
pub const STACKABLE_DATA_DIR: &str = "/stackable/data";
pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";
pub const STACKABLE_TLS_CERTS_DIR: &str = "/stackable/certificates";
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
    pub version: Option<String>,
    pub brokers: Option<Role<KafkaConfig>>,
    pub zookeeper_config_map_name: String,
    pub opa: Option<OpaConfig>,
    pub log4j: Option<String>,
    #[serde(
        default = "global_config_default",
        skip_serializing_if = "Option::is_none"
    )]
    pub config: Option<GlobalKafkaConfig>,
    pub stopped: Option<bool>,
}

#[derive(Clone, Default, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
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
    /// Only affects internal communication. Use mutual verification between Kafka Broker Nodes
    /// (mandatory). This setting controls:
    /// - Which cert the brokers should use to authenticate themselves against other brokers
    /// - Which ca.crt to use when validating the other server
    #[serde(
        default = "tls_secret_class_default",
        skip_serializing_if = "Option::is_none"
    )]
    pub internal_tls: Option<TlsSecretClass>,
}

fn global_config_default() -> Option<GlobalKafkaConfig> {
    Some(GlobalKafkaConfig {
        tls: Some(TlsSecretClass {
            secret_class: TLS_DEFAULT_SECRET_CLASS.to_string(),
        }),
        client_authentication: None,
        internal_tls: Some(TlsSecretClass {
            secret_class: TLS_DEFAULT_SECRET_CLASS.to_string(),
        }),
    })
}

#[derive(Clone, Default, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientAuthenticationClass {
    pub authentication_class: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
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

    /// Returns the provided docker image e.g. 2.8.1-stackable0.1.0
    pub fn image_version(&self) -> Result<&str, Error> {
        self.spec
            .version
            .as_deref()
            .context(ObjectHasNoVersionSnafu)
    }

    /// Returns our semver representation for product config e.g. 2.8.1
    pub fn product_version(&self) -> Result<&str, Error> {
        let image_version = self.image_version()?;
        image_version
            .split('-')
            .next()
            .with_context(|| KafkaProductVersionSnafu {
                image_version: image_version.to_string(),
            })
    }

    pub fn client_port(&self) -> u16 {
        if self.is_client_secure() {
            SECURE_CLIENT_PORT
        } else {
            CLIENT_PORT
        }
    }

    /// Returns the secret class for client connection encryption. Defaults to `tls`.
    pub fn client_tls_secret_class(&self) -> Option<&TlsSecretClass> {
        let spec: &KafkaClusterSpec = &self.spec;
        spec.config.as_ref().and_then(|c| c.tls.as_ref())
    }

    /// Checks if we should use TLS to encrypt client connections.
    pub fn is_client_secure(&self) -> bool {
        self.client_tls_secret_class().is_some() || self.client_authentication_class().is_some()
    }

    /// Returns the authentication class used for client authentication
    pub fn client_authentication_class(&self) -> Option<String> {
        let spec: &KafkaClusterSpec = &self.spec;
        spec.config
            .as_ref()
            .and_then(|c| c.client_authentication.as_ref())
            .map(|tls| tls.authentication_class.clone())
    }

    /// Returns the secret class for internal server encryption
    pub fn internal_tls_secret_class(&self) -> Option<&TlsSecretClass> {
        let spec: &KafkaClusterSpec = &self.spec;
        spec.config.as_ref().and_then(|c| c.internal_tls.as_ref())
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
            // Client TLS
            if resource.client_tls_secret_class().is_some() {
                config.insert(
                    SSL_KEYSTORE_LOCATION.to_string(),
                    Some(format!("{}/keystore.p12", STACKABLE_TLS_CERTS_DIR)),
                );
                config.insert(
                    SSL_KEYSTORE_PASSWORD.to_string(),
                    Some(SSL_STORE_PASSWORD.to_string()),
                );
                config.insert(SSL_KEYSTORE_TYPE.to_string(), Some("PKCS12".to_string()));
                config.insert(
                    SSL_TRUSTSTORE_LOCATION.to_string(),
                    Some(format!("{}/truststore.p12", STACKABLE_TLS_CERTS_DIR)),
                );
                config.insert(
                    SSL_TRUSTSTORE_PASSWORD.to_string(),
                    Some(SSL_STORE_PASSWORD.to_string()),
                );
                config.insert(SSL_TRUSTSTORE_TYPE.to_string(), Some("PKCS12".to_string()));

                // Authentication
                if resource.client_authentication_class().is_some() {
                    config.insert(SSL_CLIENT_AUTH.to_string(), Some("required".to_string()));
                }
            }

            // We require authentication
            if resource.client_authentication_class().is_some() {
                config.insert(SSL_CLIENT_AUTH.to_string(), Some("required".to_string()));
            }

            // Internal TLS
            if resource.internal_tls_secret_class().is_some() {
                config.insert(
                    SECURITY_INTER_BROKER_PROTOCOL.to_string(),
                    Some("SSL".to_string()),
                );
            }
        }

        Ok(config)
    }
}
