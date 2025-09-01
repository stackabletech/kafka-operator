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
    listener::KafkaListenerName,
    role::{
        KAFKA_LOG_DIRS, KAFKA_PROCESS_ROLES, KafkaRole,
        commons::{CommonConfig, Storage, StorageFragment},
    },
    v1alpha1,
};

pub const CONTROLLER_PROPERTIES_FILE: &str = "controller.properties";

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
pub enum ControllerContainer {
    Vector,
    // TODO: Kafka, Kraft, Controller?
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
pub struct ControllerConfig {
    #[fragment_attrs(serde(flatten))]
    pub common_config: CommonConfig,

    #[fragment_attrs(serde(default))]
    pub logging: Logging<ControllerContainer>,

    #[fragment_attrs(serde(default))]
    pub resources: Resources<Storage, NoRuntimeLimits>,
}

impl ControllerConfig {
    pub fn default_config(cluster_name: &str, role: &str) -> ControllerConfigFragment {
        ControllerConfigFragment {
            common_config: CommonConfig::default_config(cluster_name, role),
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

impl Configuration for ControllerConfigFragment {
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
        _resource: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        let mut config = BTreeMap::new();

        if file == CONTROLLER_PROPERTIES_FILE {
            config.insert(
                KAFKA_LOG_DIRS.to_string(),
                Some("/stackable/data/kraft".to_string()),
            );

            // KRAFT
            config.insert(
                KAFKA_PROCESS_ROLES.to_string(),
                Some(KafkaRole::Controller.to_string()),
            );

            config.insert(
                "controller.listener.names".to_string(),
                Some(KafkaListenerName::Controller.to_string()),
            );
        }

        Ok(config)
    }
}
