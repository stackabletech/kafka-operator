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
pub const APP_PORT: u16 = 9092;
pub const METRICS_PORT: u16 = 9606;

pub const SERVER_PROPERTIES_FILE: &str = "server.properties";

pub const KAFKA_HEAP_OPTS: &str = "KAFKA_HEAP_OPTS";
pub const LOG_DIRS_VOLUME_NAME: &str = "log-dirs";

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
    pub stopped: Option<bool>,
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

        if resource.spec.opa.is_some() && file == SERVER_PROPERTIES_FILE {
            config.insert(
                "authorizer.class.name".to_string(),
                Some("org.openpolicyagent.kafka.OpaAuthorizer".to_string()),
            );
            config.insert(
                "opa.authorizer.metrics.enabled".to_string(),
                Some("true".to_string()),
            );
        }

        Ok(config)
    }
}
