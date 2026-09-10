use std::{ops::Deref, str::FromStr};

use serde::{Deserialize, Serialize};
use stackable_operator::{
    commons::resources::{
        CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
        PvcConfigFragment, Resources, ResourcesFragment,
    },
    config::{fragment::Fragment, merge::Merge},
    constant,
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    product_logging::{self, spec::Logging},
    schemars::{self, JsonSchema},
    v2::types::kubernetes::ContainerName,
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
pub enum ControllerContainer {
    Vector,
    Kafka,
}

// Typed container names. They must match the strum `Display` (kebab-case) of the variants above,
// which is pinned by a unit test.
constant!(VECTOR_CONTAINER_NAME: ContainerName = "vector");
constant!(KAFKA_CONTAINER_NAME: ContainerName = "kafka");

impl Deref for ControllerContainer {
    type Target = ContainerName;

    fn deref(&self) -> &Self::Target {
        match self {
            ControllerContainer::Vector => &VECTOR_CONTAINER_NAME,
            ControllerContainer::Kafka => &KAFKA_CONTAINER_NAME,
        }
    }
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

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *VECTOR_CONTAINER_NAME;
        let _ = *KAFKA_CONTAINER_NAME;
    }

    /// The typed container names behind `ControllerContainer`'s `Deref` must agree with its strum
    /// `Display`, which the logging configuration still uses as the per-container key.
    #[test]
    fn container_names_match_display() {
        for container in ControllerContainer::iter() {
            let container_name: &ContainerName = &container;
            assert_eq!(container_name.to_string(), container.to_string());
        }
    }
}
