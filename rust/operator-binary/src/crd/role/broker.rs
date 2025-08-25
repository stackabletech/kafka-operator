use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use stackable_operator::{
    commons::resources::{
        CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
        PvcConfigFragment, Resources, ResourcesFragment,
    },
    config::{fragment::Fragment, merge::Merge},
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    product_config_utils::Configuration,
    product_logging::{self, spec::Logging},
    schemars::{self, JsonSchema},
};
use strum::{Display, EnumIter};

use crate::crd::{
    SERVER_PROPERTIES_FILE,
    role::commons::{CommonConfig, Storage, StorageFragment},
    v1alpha1,
};

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    Eq,
    EnumIter,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum BrokerContainer {
    Vector,
    KcatProber,
    GetService,
    Kafka,
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
pub struct BrokerConfig {
    #[fragment_attrs(serde(flatten))]
    pub common_config: CommonConfig,

    /// The ListenerClass used for bootstrapping new clients. Should use a stable ListenerClass to avoid unnecessary client restarts (such as `cluster-internal` or `external-stable`).
    pub bootstrap_listener_class: String,

    /// The ListenerClass used for connecting to brokers. Should use a direct connection ListenerClass to minimize cost and minimize performance overhead (such as `cluster-internal` or `external-unstable`).
    pub broker_listener_class: String,

    #[fragment_attrs(serde(default))]
    pub logging: Logging<BrokerContainer>,

    #[fragment_attrs(serde(default))]
    pub resources: Resources<Storage, NoRuntimeLimits>,
}

impl BrokerConfig {
    pub fn default_config(cluster_name: &str, role: &str) -> BrokerConfigFragment {
        BrokerConfigFragment {
            common_config: CommonConfig::default_config(cluster_name, role),
            bootstrap_listener_class: Some("cluster-internal".to_string()),
            broker_listener_class: Some("cluster-internal".to_string()),
            logging: product_logging::spec::default_logging(),
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("250m".to_owned())),
                    max: Some(Quantity("1000m".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("1Gi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: StorageFragment {
                    log_dirs: PvcConfigFragment {
                        capacity: Some(Quantity("2Gi".to_owned())),
                        storage_class: None,
                        selectors: None,
                    },
                },
            },
        }
    }
}

impl Configuration for BrokerConfigFragment {
    type Configurable = v1alpha1::KafkaCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        resource: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
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
