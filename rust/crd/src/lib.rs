use serde::{Deserialize, Serialize};
use snafu::{OptionExt, Snafu};
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::CustomResource;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::Role;
use stackable_operator::role_utils::RoleGroupRef;
use stackable_operator::schemars::{self, JsonSchema};
use std::collections::BTreeMap;
use strum_macros::{Display, EnumIter, EnumString};

pub const APP_NAME: &str = "kafka";
pub const APP_PORT: u16 = 9092;
pub const MANAGED_BY: &str = "kafka-operator";

pub const SERVER_PROPERTIES_FILE: &str = "server.properties";

pub const ZOOKEEPER_CONNECT: &str = "zookeeper.connect";
pub const OPA_AUTHORIZER_URL: &str = "opa.authorizer.url";
pub const METRICS_PORT: &str = "metricsPort";

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
    pub version: String,
    pub brokers: Option<Role<KafkaConfig>>,
    pub zookeeper_config_map_name: String,
    pub opa: Option<OpaConfig>,
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
    pub fn pods(&self) -> Result<impl Iterator<Item = KafkaPodRef> + '_, NoNamespaceError> {
        let ns = self
            .metadata
            .namespace
            .clone()
            .context(NoNamespaceContext)?;
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
}

#[derive(Debug, Snafu)]
#[snafu(display("object has no namespace associated"))]
pub struct NoNamespaceError;

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

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
/// Contains all data to combine with OPA. The "opa.authorizer.url" is set dynamically in
/// the controller (local nodes first, random otherwise).
pub struct OpaConfig {
    pub config_map_name: String,
    pub authorizer_class_name: String,
    //pub authorizer_url: Option<String>,
    pub authorizer_cache_initial_capacity: Option<usize>,
    pub authorizer_cache_maximum_size: Option<usize>,
    pub authorizer_cache_expire_after_seconds: Option<usize>,
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

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Default)]
#[serde(rename_all = "camelCase")]
/// In order for compute_files from the Configuration trait to work, we cannot pass an empty or
/// "None" config. Therefore we need at least one required property.
// TODO: Does "log.dirs" make sense in that case? If we make it an option in can happen that the
//    config will be parsed as None.
pub struct KafkaConfig {
    pub metrics_port: Option<u16>,
}

impl Configuration for KafkaConfig {
    type Configurable = KafkaCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();
        if let Some(metrics_port) = self.metrics_port {
            result.insert(METRICS_PORT.to_string(), Some(metrics_port.to_string()));
        }
        Ok(result)
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
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut config = BTreeMap::new();
        // TODO: How to work with zookeeper reference or opa reference?
        //   The ZooKeeper reference is queried at the start of reconcile and stored in the state
        //   (which we do not have access here).
        //   Similar, retrieving the OPA reference requires a node_name, which we do not have here
        //   either.
        if let Some(opa_config) = &resource.spec.opa {
            config.insert(
                "authorizer.class.name".to_string(),
                Some(opa_config.authorizer_class_name.clone()),
            );
            config.insert(
                "opa.authorizer.cache.initial.capacity".to_string(),
                opa_config
                    .authorizer_cache_initial_capacity
                    .map(|auth| auth.to_string()),
            );
            config.insert(
                "opa.authorizer.cache.maximum.size".to_string(),
                opa_config
                    .authorizer_cache_maximum_size
                    .map(|auth| auth.to_string()),
            );
            config.insert(
                "opa.authorizer.cache.expire.after.seconds".to_string(),
                opa_config
                    .authorizer_cache_expire_after_seconds
                    .map(|auth| auth.to_string()),
            );
        }

        Ok(config)
    }
}
