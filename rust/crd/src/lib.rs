use serde::{Deserialize, Serialize};
use stackable_operator::crd::HasApplication;
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
pub const LOG_DIR: &str = "log.dir";

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

impl HasApplication for KafkaCluster {
    fn get_application_name() -> &'static str {
        APP_NAME
    }
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
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
/// Contains all data to combine with OPA. The "opa.authorizer.url" is set dynamically in
/// the controller (local nodes first, random otherwise).
pub struct OpaConfig {
    // pub reference: OpaReference,
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

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
/// In order for compute_files from the Configuration trait to work, we cannot pass an empty or
/// "None" config. Therefore we need at least one required property.
// TODO: Does "log.dirs" make sense in that case? If we make it an option in can happen that the
//    config will be parsed as None.
pub struct KafkaConfig {
    pub log_dirs: String,
    pub metrics_port: Option<u16>,
    pub port: Option<u16>,
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

        if let Some(port) = self.port {
            config.insert(
                "listeners".to_string(),
                Some(format!("PLAINTEXT://:{}", port)),
            );
        }

        config.insert(LOG_DIR.to_string(), Some(self.log_dirs.clone()));

        Ok(config)
    }
}
