pub mod broker;
pub mod commons;
pub mod controller;

use std::{borrow::Cow, ops::Deref};

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::resources::{NoRuntimeLimits, Resources},
    config::{
        fragment::{self, ValidationError},
        merge::Merge,
    },
    k8s_openapi::api::core::v1::PodTemplateSpec,
    kube::{ResourceExt, runtime::reflector::ObjectRef},
    product_logging::spec::ContainerLogConfig,
    role_utils::RoleGroupRef,
    schemars::{self, JsonSchema},
};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

use crate::{
    config::jvm::{construct_heap_jvm_args, construct_non_heap_jvm_args},
    crd::role::{
        broker::{BrokerConfig, BrokerConfigFragment},
        commons::{CommonConfig, Storage},
        controller::ControllerConfig,
    },
    v1alpha1,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },

    #[snafu(display("the Kafka role [{role}] is missing from spec"))]
    MissingRole { role: String },

    #[snafu(display("missing role group {rolegroup:?} for role {role:?}"))]
    MissingRoleGroup { role: String, rolegroup: String },

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArguments { source: crate::config::jvm::Error },
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
    #[strum(serialize = "controller")]
    Controller,
}

impl KafkaRole {
    /// Return all available roles
    pub fn roles() -> Vec<String> {
        let mut roles = vec![];
        for role in Self::iter() {
            roles.push(role.to_string())
        }
        roles
    }

    /// Metadata about a rolegroup
    pub fn rolegroup_ref(
        &self,
        kafka: &v1alpha1::KafkaCluster,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<v1alpha1::KafkaCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(kafka),
            role: self.to_string(),
            role_group: group_name.into(),
        }
    }

    /// A Kerberos principal has three parts, with the form username/fully.qualified.domain.name@YOUR-REALM.COM.
    /// We only have one role and will use "kafka" everywhere (which e.g. differs from the current hdfs implementation,
    /// but is similar to HBase).
    pub fn kerberos_service_name(&self) -> &'static str {
        "kafka"
    }

    /// Merge the [Broker|Controller]ConfigFragment defaults, role and role group settings.
    /// The priority is: default < role config < role_group config
    pub fn merged_config(
        &self,
        kafka: &v1alpha1::KafkaCluster,
        rolegroup: &str,
    ) -> Result<AnyConfig, Error> {
        match self {
            Self::Broker => {
                // Initialize the result with all default values as baseline
                let default_config =
                    BrokerConfig::default_config(&kafka.name_any(), &self.to_string());

                // Retrieve role resource config
                let role = kafka
                    .spec
                    .brokers
                    .as_ref()
                    .with_context(|| MissingRoleSnafu {
                        role: self.to_string(),
                    })?;

                let mut role_config = role.config.config.clone();
                // Retrieve rolegroup specific resource config
                let mut role_group_config = role
                    .role_groups
                    .get(rolegroup)
                    .with_context(|| MissingRoleGroupSnafu {
                        role: self.to_string(),
                        rolegroup: rolegroup.to_string(),
                    })?
                    .config
                    .config
                    .clone();

                // Merge more specific configs into default config
                // Hierarchy is:
                // 1. RoleGroup
                // 2. Role
                // 3. Default
                role_config.merge(&default_config);
                role_group_config.merge(&role_config);
                Ok(AnyConfig::Broker(
                    fragment::validate::<BrokerConfig>(role_group_config)
                        .context(FragmentValidationFailureSnafu)?,
                ))
            }
            Self::Controller => {
                // Initialize the result with all default values as baseline
                let default_config =
                    ControllerConfig::default_config(&kafka.name_any(), &self.to_string());

                // Retrieve role resource config
                let role = kafka
                    .spec
                    .controllers
                    .as_ref()
                    .with_context(|| MissingRoleSnafu {
                        role: self.to_string(),
                    })?;

                let mut role_config = role.config.config.clone();
                // Retrieve rolegroup specific resource config
                let mut role_group_config = role
                    .role_groups
                    .get(rolegroup)
                    .with_context(|| MissingRoleGroupSnafu {
                        role: self.to_string(),
                        rolegroup: rolegroup.to_string(),
                    })?
                    .config
                    .config
                    .clone();

                // Merge more specific configs into default config
                // Hierarchy is:
                // 1. RoleGroup
                // 2. Role
                // 3. Default
                role_config.merge(&default_config);
                role_group_config.merge(&role_config);
                Ok(AnyConfig::Controller(
                    fragment::validate::<ControllerConfig>(role_group_config)
                        .context(FragmentValidationFailureSnafu)?,
                ))
            }
        }
    }

    pub fn construct_non_heap_jvm_args(
        &self,
        merged_config: &AnyConfig,
        kafka: &v1alpha1::KafkaCluster,
        rolegroup: &str,
    ) -> Result<String, Error> {
        match self {
            Self::Broker => construct_non_heap_jvm_args::<BrokerConfigFragment>(
                merged_config,
                &kafka
                    .spec
                    .brokers
                    .clone()
                    .with_context(|| MissingRoleSnafu {
                        role: self.to_string(),
                    })?,
                rolegroup,
            )
            .context(ConstructJvmArgumentsSnafu),
            Self::Controller => construct_non_heap_jvm_args(
                merged_config,
                &kafka
                    .spec
                    .controllers
                    .clone()
                    .with_context(|| MissingRoleSnafu {
                        role: self.to_string(),
                    })?,
                rolegroup,
            )
            .context(ConstructJvmArgumentsSnafu),
        }
    }

    pub fn construct_heap_jvm_args(
        &self,
        merged_config: &AnyConfig,
        kafka: &v1alpha1::KafkaCluster,
        rolegroup: &str,
    ) -> Result<String, Error> {
        match self {
            Self::Broker => construct_heap_jvm_args::<BrokerConfigFragment>(
                merged_config,
                &kafka
                    .spec
                    .brokers
                    .clone()
                    .with_context(|| MissingRoleSnafu {
                        role: self.to_string(),
                    })?,
                rolegroup,
            )
            .context(ConstructJvmArgumentsSnafu),
            Self::Controller => construct_heap_jvm_args(
                merged_config,
                &kafka
                    .spec
                    .controllers
                    .clone()
                    .with_context(|| MissingRoleSnafu {
                        role: self.to_string(),
                    })?,
                rolegroup,
            )
            .context(ConstructJvmArgumentsSnafu),
        }
    }

    pub fn role_pod_overrides(
        &self,
        kafka: &v1alpha1::KafkaCluster,
    ) -> Result<PodTemplateSpec, Error> {
        let pod_overrides = match self {
            Self::Broker => {
                kafka
                    .spec
                    .brokers
                    .clone()
                    .with_context(|| MissingRoleSnafu {
                        role: self.to_string(),
                    })?
                    .config
                    .pod_overrides
            }
            Self::Controller => {
                kafka
                    .spec
                    .controllers
                    .clone()
                    .with_context(|| MissingRoleSnafu {
                        role: self.to_string(),
                    })?
                    .config
                    .pod_overrides
            }
        };

        Ok(pod_overrides)
    }

    pub fn role_group_pod_overrides(
        &self,
        kafka: &v1alpha1::KafkaCluster,
        rolegroup: &str,
    ) -> Result<PodTemplateSpec, Error> {
        let pod_overrides = match self {
            Self::Broker => kafka
                .spec
                .brokers
                .clone()
                .with_context(|| MissingRoleSnafu {
                    role: self.to_string(),
                })?
                .role_groups
                .get(rolegroup)
                .with_context(|| MissingRoleGroupSnafu {
                    role: self.to_string(),
                    rolegroup: rolegroup.to_string(),
                })?
                .config
                .pod_overrides
                .clone(),
            Self::Controller => kafka
                .spec
                .controllers
                .clone()
                .with_context(|| MissingRoleSnafu {
                    role: self.to_string(),
                })?
                .role_groups
                .get(rolegroup)
                .with_context(|| MissingRoleGroupSnafu {
                    role: self.to_string(),
                    rolegroup: rolegroup.to_string(),
                })?
                .config
                .pod_overrides
                .clone(),
        };

        Ok(pod_overrides)
    }

    pub fn replicas(
        &self,
        kafka: &v1alpha1::KafkaCluster,
        rolegroup: &str,
    ) -> Result<Option<u16>, Error> {
        let replicas = match self {
            Self::Broker => {
                kafka
                    .spec
                    .brokers
                    .clone()
                    .with_context(|| MissingRoleSnafu {
                        role: self.to_string(),
                    })?
                    .role_groups
                    .get(rolegroup)
                    .with_context(|| MissingRoleGroupSnafu {
                        role: self.to_string(),
                        rolegroup: rolegroup.to_string(),
                    })?
                    .replicas
            }
            Self::Controller => {
                kafka
                    .spec
                    .controllers
                    .clone()
                    .with_context(|| MissingRoleSnafu {
                        role: self.to_string(),
                    })?
                    .role_groups
                    .get(rolegroup)
                    .with_context(|| MissingRoleGroupSnafu {
                        role: self.to_string(),
                        rolegroup: rolegroup.to_string(),
                    })?
                    .replicas
            }
        };

        Ok(replicas)
    }
}

/// Configuration for a role and rolegroup of an unknown type.
#[derive(Debug)]
pub enum AnyConfig {
    Broker(BrokerConfig),
    Controller(ControllerConfig),
}

impl Deref for AnyConfig {
    type Target = CommonConfig;

    fn deref(&self) -> &Self::Target {
        match self {
            AnyConfig::Broker(broker_config) => &broker_config.common_config,
            AnyConfig::Controller(controller_config) => &controller_config.common_config,
        }
    }
}

impl AnyConfig {
    pub fn resources(&self) -> &Resources<Storage, NoRuntimeLimits> {
        match self {
            AnyConfig::Broker(broker_config) => &broker_config.resources,
            AnyConfig::Controller(controller_config) => &controller_config.resources,
        }
    }

    // Logging config is distinct between each role, due to the different enum types,
    // so provide helpers for containers that are common between all roles.
    pub fn kafka_logging(&self) -> Cow<ContainerLogConfig> {
        match self {
            AnyConfig::Broker(node) => node.logging.for_container(&broker::BrokerContainer::Kafka),
            AnyConfig::Controller(node) => node
                .logging
                .for_container(&controller::ControllerContainer::Kafka),
        }
    }

    pub fn vector_logging(&self) -> Cow<ContainerLogConfig> {
        match &self {
            AnyConfig::Broker(broker_config) => broker_config
                .logging
                .for_container(&broker::BrokerContainer::Vector),
            AnyConfig::Controller(controller_config) => controller_config
                .logging
                .for_container(&controller::ControllerContainer::Vector),
        }
    }

    pub fn vector_logging_enabled(&self) -> bool {
        match self {
            AnyConfig::Broker(broker_config) => broker_config.logging.enable_vector_agent,
            AnyConfig::Controller(controller_config) => {
                controller_config.logging.enable_vector_agent
            }
        }
    }

    pub fn listener_class(&self) -> Option<&String> {
        match self {
            AnyConfig::Broker(broker_config) => Some(&broker_config.broker_listener_class),
            AnyConfig::Controller(_) => None,
        }
    }
}
