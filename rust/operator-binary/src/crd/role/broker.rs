use serde::{Deserialize, Serialize};
use stackable_operator::{
    commons::resources::{
        CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
        PvcConfigFragment, Resources, ResourcesFragment,
    },
    config::{fragment::Fragment, merge::Merge},
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    product_logging::{self, spec::Logging},
    schemars::{self, JsonSchema},
};
use strum::{Display, EnumIter};

use crate::crd::role::commons::{CommonConfig, Storage, StorageFragment};

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
                    limit: Some(Quantity("2Gi".to_owned())),
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

